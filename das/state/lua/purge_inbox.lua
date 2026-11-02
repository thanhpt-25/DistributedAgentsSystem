-- purge_inbox: drop the inbox stream of a terminal Request after inbox_ttl (Req 14.7).
-- Every other key of the Request is kept until purge_request applies. Uses the
-- script's own TIME.
-- KEYS[1] request hash, KEYS[2] inbox stream, KEYS[3] das:requests:inbox_purge
-- ARGV[1] request_id, ARGV[2] inbox_ttl in ms
-- Returns {'DELETED'} when an inbox stream was removed, else {'KEEP'}. A candidate
-- that is missing, non-terminal or already without an inbox leaves inbox_purge.
local rid = ARGV[1]
local ttl = tonumber(ARGV[2])
if not ttl or ttl < 0 then
  return redis.error_reply('purge_inbox: ttl_ms must be a non-negative number')
end
local _, now = server_time()
local v = redis.call('HMGET', KEYS[1], 'status', 'finished_at')
if not is_terminal(v[1]) then
  redis.call('ZREM', KEYS[3], rid)
  return { 'KEEP' }
end
local finished = tonumber(v[2] or '')
if not finished then
  return { 'KEEP' }  -- purge_request stamps it first (Req 14.9)
end
if finished + ttl > now then
  redis.call('ZADD', KEYS[3], finished, rid)
  return { 'KEEP' }
end
redis.call('ZREM', KEYS[3], rid)
if redis.call('DEL', KEYS[2]) == 1 then
  return { 'DELETED' }
end
return { 'KEEP' }
