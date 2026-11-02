-- discard_dead_letter: Pending -> Discarded (Req 13.11), idempotent on Discarded
-- (Req 13.12), rejected on Replayed (Req 13.13). Same order as das.ops.replay.decide_discard.
-- Request, Job and Job_Result state are never touched.
-- KEYS[1] das:dlq:{dlid}, KEYS[2] das:dlq:by_status:Pending,
-- KEYS[3] das:dlq:by_status:Discarded, KEYS[4] das:dlq:terminal
-- ARGV[1] dead_letter_id
-- Returns {'OK', discarded_at_ms} | {'ALREADY', discarded_at_ms}
--   | {'RECORD_NOT_PENDING', status} | {'NOT_FOUND'}.
local v = redis.call('HMGET', KEYS[1], 'status', 'discarded_at', 'dead_lettered_at')
local status = v[1]
if not status then
  return { 'NOT_FOUND' }
end
if status == 'Discarded' then
  return { 'ALREADY', v[2] or '' }
end
if status ~= 'Pending' then
  return { 'RECORD_NOT_PENDING', status }
end
local _, ms = server_time()
local dlid = ARGV[1]
redis.call('HSET', KEYS[1], 'status', 'Discarded', 'discarded_at', tostring(ms),
  'status_changed_at', tostring(ms))
redis.call('ZREM', KEYS[2], dlid)
redis.call('ZADD', KEYS[3], tonumber(v[3]) or ms, dlid)
redis.call('ZADD', KEYS[4], ms, dlid)
return { 'OK', tostring(ms) }
