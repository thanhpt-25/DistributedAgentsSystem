-- purge_request: retention of one Request (Req 14.3, 14.4, 14.5, 14.9). Mirrors
-- das.ops.retention.retention_decision with the script's own TIME, so the safety
-- condition never depends on the caller's clock:
--   missing or non-terminal -> KEEP (stale retention candidates are removed)
--   terminal without finished_at -> STAMPED (finished_at = now; never deleted now)
--   finished_at + ttl > now -> KEEP
--   otherwise -> every key of the Request and every index entry is removed: DELETED
--   (including its Active_Set member, Phase 4 Req 3.5, and its outbox entry, Req 8.2)
-- KEYS[1..11] request_keys(rid): request, jobs, results, inbox, timers, transitions,
--   lease, usage, pending_replays, events, webhook (Phase 4)
-- KEYS[12] das:requests:index, KEYS[13..17] das:requests:by_status:{Pending, Running,
--   Completed, Failed, Cancelled}, KEYS[18] das:requests:finished,
-- KEYS[19] das:requests:inbox_purge, KEYS[20] das:requests:pending, KEYS[21] das:leases:expiry
-- ARGV[1] request_id, ARGV[2] terminal_request_ttl in ms
-- Returns {'DELETED'} | {'KEEP'} | {'STAMPED'}.
local rid = ARGV[1]
local ttl = tonumber(ARGV[2])
if not ttl or ttl < 0 then
  return redis.error_reply('purge_request: ttl_ms must be a non-negative number')
end
local _, now = server_time()
local v = redis.call('HMGET', KEYS[1], 'status', 'finished_at')
local status = v[1]
if not is_terminal(status) then
  redis.call('ZREM', KEYS[18], rid)
  if not status then
    redis.call('ZREM', KEYS[19], rid)
  end
  return { 'KEEP' }
end
local finished = tonumber(v[2] or '')
if not finished then
  mark_finished(KEYS[1], KEYS[18], KEYS[19], rid, now)
  return { 'STAMPED' }
end
if finished + ttl > now then
  -- Keep the candidate score in line with finished_at (e.g. after a later re-finish).
  redis.call('ZADD', KEYS[18], finished, rid)
  return { 'KEEP' }
end
-- Read client_fp before the DEL removes the request hash (Phase 4 Req 3.5).
client_active_remove(KEYS[1], rid)
local tenant = tenant_of(KEYS[1])  -- Phase 5 Req 3.9: read before the DEL removes the request hash
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7],
  KEYS[8], KEYS[9], KEYS[10], KEYS[11])
for i = 12, 21 do
  redis.call('ZREM', KEYS[i], rid)
  local twin = tenant_twin(KEYS[i], tenant)  -- the index and by_status keys have one; the others none
  if twin then redis.call('ZREM', twin, rid) end
end
redis.call('ZREM', outbox_key(KEYS[1]), rid)
return { 'DELETED' }
