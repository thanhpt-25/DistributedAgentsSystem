-- create_request: persist a new Pending Request (Req 1.1, 6.1, 15.11), idempotent and
-- quota-checked for Phase 4 clients (Phase 4 Req 3.1, 3.2, 4.2-4.5, 5.4, 6.1, 8.1).
-- KEYS[1] request hash, KEYS[2] das:requests:pending, KEYS[3] das:requests:index,
-- KEYS[4] das:requests:by_status:Pending
-- ARGV[1] request_id, ARGV[2] prompt, ARGV[3] trace_context JSON or '' (optional)
-- Optional ARGV[4..12] ('' or '0' = not set): ARGV[4] client_fp, ARGV[5] budget JSON,
-- ARGV[6] webhook_url, ARGV[7] webhook_secret_name (the target is stored only when both
-- are set), ARGV[8] idem_hash, ARGV[9] body_hash (required with idem_hash),
-- ARGV[10] idem_ttl seconds, ARGV[11] max_active (0 = no quota), ARGV[12] events_max_len.
-- Phase 5 (Req 3.1, 3.5, 3.7, 6.1, 7.3), also optional: ARGV[13] tenant (stored on the Request; absent
-- means the Default_Tenant), ARGV[14] tenant_max_active (Tenant_Quota, 0 = none), ARGV[15] priority
-- ('high' is stored, '' is not). After the per-key quota, a full tenant set returns
-- {'TENANT_QUOTA_EXCEEDED', tenant_max_active}. The Request also joins the tenant's index twins and
-- active set, and counts as 'submitted' in the tenant's Usage_Rollup of the day.
-- Order: (1) idempotency record of (client_fp, idem_hash) whose Request still exists ->
-- {'REPLAYED', rid} when its body_hash matches, else {'IDEM_CONFLICT'}; a record naming a
-- Request that no longer exists is treated as absent and overwritten in (4) (Req 4.8);
-- (2) {'EXISTS'} on Request_ID collision; (3) {'QUOTA_EXCEEDED', max_active} when the
-- Active_Set is full; (4) writes, SADD to the Active_Set, the idempotency record
-- (rid, body_hash) with a refreshed TTL, and the request.created Event.
-- Nothing is written unless the reply is {'OK', created_at}.
local function opt(i)
  local v = ARGV[i]
  if v == nil or v == '' then return nil end
  return v
end

local rid = ARGV[1]
local fp = opt(4)
local budget_json = opt(5)
local webhook_url = opt(6)
local webhook_secret = opt(7)
local idem_hash = opt(8)
local body_hash = opt(9)
local idem_ttl = tonumber(opt(10) or '0')
local max_active = tonumber(opt(11) or '0')
local events_max_len = opt(12)
local tenant = opt(13)
local tenant_max = tonumber(opt(14) or '0')
local priority = opt(15)

-- Validate every input before the first write (no rollback on runtime errors).
if tenant and (#tenant > 32 or not string.match(tenant, '^[a-z0-9][a-z0-9%-]*$')) then
  error('create_request: tenant is not a valid Tenant_ID')
end
if not tenant_max or tenant_max < 0 or tenant_max ~= math.floor(tenant_max) then
  error('create_request: tenant_max_active must be an integer >= 0')
end
if priority and priority ~= 'high' then
  error('create_request: priority must be high or absent')
end
if budget_json and type(cjson.decode(budget_json)) ~= 'table' then
  error('create_request: budget must be a JSON object')
end
if not max_active or max_active < 0 or max_active ~= math.floor(max_active) then
  error('create_request: max_active must be an integer >= 0')
end
if events_max_len then
  local n = tonumber(events_max_len)
  if not n or n < 1 or n ~= math.floor(n) then
    error('create_request: events_max_len must be an integer >= 1')
  end
end
if idem_hash then
  if not fp then error('create_request: idem_hash requires client_fp') end
  if not body_hash then error('create_request: idem_hash requires body_hash') end
  if not idem_ttl or idem_ttl < 1 or idem_ttl ~= math.floor(idem_ttl) then
    error('create_request: idem_ttl must be an integer >= 1')
  end
end

local idem
if idem_hash then
  idem = idem_key(KEYS[1], fp, idem_hash)
  local rec = redis.call('HMGET', idem, 'rid', 'body_hash')
  -- A record whose Request is gone (retention or deletion) counts as absent: the
  -- Request is created below and the record overwritten (Req 4.8). Same key layout
  -- as das.state.keys.request: <prefix>:req:<rid>.
  if rec[1] and redis.call('EXISTS', key_prefix(KEYS[1]) .. ':req:' .. rec[1]) == 1 then
    if rec[2] == body_hash then
      return { 'REPLAYED', rec[1] }
    end
    return { 'IDEM_CONFLICT' }
  end
end

if redis.call('EXISTS', KEYS[1]) == 1 then
  return { 'EXISTS' }
end

local active
if fp then
  active = active_key(KEYS[1], fp)
  if max_active > 0 and redis.call('SCARD', active) >= max_active then
    return { 'QUOTA_EXCEEDED', max_active }
  end
end
-- The Tenant_Quota counts every non-terminal Request of the tenant, whichever key created it (Phase 5 Req 3.7).
if tenant_max > 0 and redis.call('SCARD', tenant_active_key(KEYS[1], tenant or 'default')) >= tenant_max then
  return { 'TENANT_QUOTA_EXCEEDED', tenant_max }
end

local now, ms = server_time()
local created = fmt_seconds(now)
redis.call('HSET', KEYS[1], 'status', 'Pending', 'prompt', ARGV[2], 'created_at', created, 'inbox_cursor', '0-0')
if ARGV[3] and ARGV[3] ~= '' then
  redis.call('HSET', KEYS[1], 'trace_context', ARGV[3])
end
if fp then
  redis.call('HSET', KEYS[1], 'client_fp', fp)
end
if budget_json then
  redis.call('HSET', KEYS[1], 'budget', budget_json)
end
if webhook_url and webhook_secret then
  redis.call('HSET', KEYS[1], 'webhook_url', webhook_url, 'webhook_secret_name', webhook_secret)
end
if events_max_len then
  redis.call('HSET', KEYS[1], 'events_max_len', events_max_len)
end
-- The tenant and priority are on the hash before the helpers below read the tenant from it.
if tenant then
  redis.call('HSET', KEYS[1], 'tenant', tenant)
end
if priority then
  redis.call('HSET', KEYS[1], 'priority', priority)
end
redis.call('ZADD', KEYS[2], created, rid)
index_move(rid, nil, KEYS[3], ms, KEYS[1])
index_move(rid, nil, KEYS[4], ms, KEYS[1])
client_active_add(KEYS[1], rid)  -- the tenant's set always, the client's set when it has a client_fp
if idem then
  redis.call('HSET', idem, 'rid', rid, 'body_hash', body_hash)
  redis.call('EXPIRE', idem, idem_ttl)
end
append_event(KEYS[1], 'request.created', '{"status":"Pending"}')
usage_bump(KEYS[1], 'submitted', 1)
return { 'OK', created }
