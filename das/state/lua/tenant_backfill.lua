-- tenant_backfill: file existing Requests and Dead_Letter_Records in their tenant's index twins (Phase 5
-- Req 9.2; design §3 "Tenant_Backfill"). Batched and idempotent (ZADD, SADD, SREM, ZREM, HSETNX), so
-- repeated runs and concurrent writers converge: every write mirrors what the Phase 5 scripts do for the
-- item's CURRENT state, read in this one atomic call. Single Redis primary only (the twin names are derived
-- from the index key, as in index_move).
-- KEYS[1] das:requests:index (kind 'requests') or das:dlq:index (kind 'dlq')
-- ARGV[1] kind, ARGV[2..] the request_ids or dead_letter_ids of one batch
-- 'requests': for each Request that exists and is in the global index: ZADD the tenant's index and its current
--   status set (same score as the global index), ZREM the other four status sets, SADD the tenant's active
--   set when non-terminal and SREM it when terminal. The tenant is the 'tenant' field, 'default' when absent.
-- 'dlq': for each record in the global index whose tenant is known (its 'tenant' field, else the tenant of
--   its Request when that exists, stored with HSETNX): ZADD the tenant's DLQ index, its current by_status
--   set and its by_reason set, ZREM the other two by_status sets. A record without a Request has no tenant
--   and is left alone.
-- Returns {'OK', n} with n the number of items filed; an id that is missing or not indexed is skipped.
local REQUEST_STATUSES = { 'Pending', 'Running', 'Completed', 'Failed', 'Cancelled' }
local RECORD_STATUSES = { 'Pending', 'Replayed', 'Discarded' }
local kind = ARGV[1]
local n = 0

if kind == 'requests' then
  local prefix = string.match(KEYS[1], '^(.*):requests:index$')
  if not prefix then
    return redis.error_reply('tenant_backfill: KEYS[1] must be the request index')
  end
  for i = 2, #ARGV do
    local rid = ARGV[i]
    local req_key = prefix .. ':req:' .. rid
    local status = redis.call('HGET', req_key, 'status')
    local score = redis.call('ZSCORE', KEYS[1], rid)
    if status and score then
      local t = tenant_of(req_key)
      redis.call('ZADD', tenant_twin(KEYS[1], t), score, rid)
      for _, s in ipairs(REQUEST_STATUSES) do
        local twin = prefix .. ':tenant:' .. t .. ':requests:by_status:' .. s
        if s == status then
          redis.call('ZADD', twin, score, rid)
        else
          redis.call('ZREM', twin, rid)
        end
      end
      if is_terminal(status) then
        redis.call('SREM', tenant_active_key(req_key, t), rid)
      else
        redis.call('SADD', tenant_active_key(req_key, t), rid)
      end
      n = n + 1
    end
  end
elseif kind == 'dlq' then
  local prefix = string.match(KEYS[1], '^(.*):dlq:index$')
  if not prefix then
    return redis.error_reply('tenant_backfill: KEYS[1] must be the dead-letter index')
  end
  for i = 2, #ARGV do
    local dlid = ARGV[i]
    local rec_key = prefix .. ':dlq:' .. dlid
    local v = redis.call('HMGET', rec_key, 'request_id', 'status', 'reason', 'tenant')
    local score = redis.call('ZSCORE', KEYS[1], dlid)
    if score then
      local t = v[4]
      if not t or t == '' then
        t = nil
        if v[1] and v[1] ~= '' then
          local req_key = prefix .. ':req:' .. v[1]
          if redis.call('EXISTS', req_key) == 1 then
            t = tenant_of(req_key)
            redis.call('HSETNX', rec_key, 'tenant', t)
          end
        end
      end
      if t then
        redis.call('ZADD', tenant_twin(KEYS[1], t), score, dlid)
        for _, s in ipairs(RECORD_STATUSES) do
          local twin = prefix .. ':tenant:' .. t .. ':dlq:by_status:' .. s
          if s == v[2] then
            redis.call('ZADD', twin, score, dlid)
          else
            redis.call('ZREM', twin, dlid)
          end
        end
        if v[3] and v[3] ~= '' then
          redis.call('ZADD', prefix .. ':tenant:' .. t .. ':dlq:by_reason:' .. v[3], score, dlid)
        end
        n = n + 1
      end
    end
  end
else
  return redis.error_reply('tenant_backfill: kind must be requests or dlq')
end
return { 'OK', n }
