-- purge_dead_letter: retention of one Dead_Letter_Record (Req 14.8). Only Replayed or
-- Discarded records with status_changed_at + ttl <= now (script TIME) are removed;
-- a Pending record is never removed, even when its Request is gone.
-- KEYS[1] das:dlq:{dlid}, KEYS[2] das:dlq:index, KEYS[3] das:dlq:by_status:Replayed,
-- KEYS[4] das:dlq:by_status:Discarded, KEYS[5] das:dlq:by_reason:{reason},
-- KEYS[6] das:dlq:terminal
-- ARGV[1] dead_letter_id, ARGV[2] dlq_record_ttl in ms, ARGV[3] reason read by the
-- caller (KEYS[5] is checked against the record: compare-and-set)
-- Returns {'DELETED'} | {'KEEP'} | {'CONFLICT'} (reason differs; re-read and retry).
local dlid = ARGV[1]
local ttl = tonumber(ARGV[2])
if not ttl or ttl < 0 then
  return redis.error_reply('purge_dead_letter: ttl_ms must be a non-negative number')
end
local _, now = server_time()
local v = redis.call('HMGET', KEYS[1], 'status', 'status_changed_at', 'reason')
local status = v[1]
if status ~= 'Replayed' and status ~= 'Discarded' then
  -- Missing, or back to Pending (Req 13.15): not a retention candidate.
  redis.call('ZREM', KEYS[6], dlid)
  return { 'KEEP' }
end
local changed = tonumber(v[2] or '')
if not changed then
  redis.call('ZADD', KEYS[6], now, dlid)
  redis.call('HSET', KEYS[1], 'status_changed_at', tostring(now))
  return { 'KEEP' }
end
if changed + ttl > now then
  redis.call('ZADD', KEYS[6], changed, dlid)
  return { 'KEEP' }
end
if (v[3] or '') ~= ARGV[3] then
  return { 'CONFLICT' }
end
local tenant = redis.call('HGET', KEYS[1], 'tenant')  -- Phase 5 Req 5.1: read before the record goes
redis.call('DEL', KEYS[1])
for i = 2, 6 do
  redis.call('ZREM', KEYS[i], dlid)
  if tenant and tenant ~= '' then
    local twin = tenant_twin(KEYS[i], tenant)
    if twin then redis.call('ZREM', twin, dlid) end
  end
end
return { 'DELETED' }
