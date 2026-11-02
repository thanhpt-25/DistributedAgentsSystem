-- index_request: add one (possibly v1) Request to the Request_Index (Req 15.10, 15.11).
-- Idempotent: ZADD das:requests:index and by_status:{status} scored by created_at ms,
-- ZREM the other by_status sets. A terminal Request without finished_at is stamped
-- with the script's TIME and added to finished / inbox_purge (Req 14.9).
-- KEYS[1] request hash, KEYS[2] das:requests:index,
-- KEYS[3..7] das:requests:by_status:{Pending, Running, Completed, Failed, Cancelled},
-- KEYS[8] das:requests:finished, KEYS[9] das:requests:inbox_purge
-- ARGV[1] request_id
-- Returns {'OK'} | {'NOT_FOUND'}.
local rid = ARGV[1]
local v = redis.call('HMGET', KEYS[1], 'status', 'finished_at')
local status = v[1]
if not status then
  return { 'NOT_FOUND' }
end
local ORDER = { 'Pending', 'Running', 'Completed', 'Failed', 'Cancelled' }
local score = created_ms(KEYS[1], KEYS[2], rid)
redis.call('ZADD', KEYS[2], score, rid)
for i, s in ipairs(ORDER) do
  if s == status then
    redis.call('ZADD', KEYS[2 + i], score, rid)
  else
    redis.call('ZREM', KEYS[2 + i], rid)
  end
end
if is_terminal(status) and not tonumber(v[2] or '') then
  local _, now = server_time()
  mark_finished(KEYS[1], KEYS[8], KEYS[9], rid, now)
end
return { 'OK' }
