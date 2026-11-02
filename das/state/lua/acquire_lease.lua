-- acquire_lease: single-winner claim of a Request (Req 2.2, 2.3, 3.3).
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] das:leases:expiry, KEYS[4] das:requests:pending
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] lease duration (seconds)
-- Returns {'OK', epoch, expires_at, kind} | {'HELD'} | {'REQUEST_TERMINAL'} | {'NOT_FOUND'}.
-- kind is 'takeover' when the previous acquire (last_owner) was by another agent,
-- otherwise 'initial'. last_owner survives release so takeovers stay detectable.
-- The epoch is never reset (release keeps it), so it is a monotonic fencing token.
local rid = ARGV[1]
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  redis.call('ZREM', KEYS[3], rid)
  redis.call('ZREM', KEYS[4], rid)
  return { 'NOT_FOUND' }
end
if is_terminal(status) then
  redis.call('ZREM', KEYS[3], rid)
  redis.call('ZREM', KEYS[4], rid)
  return { 'REQUEST_TERMINAL' }
end
local now = server_time()
local cur = redis.call('HMGET', KEYS[2], 'owner', 'expires_at', 'last_owner')
if cur[1] and cur[2] and tonumber(cur[2]) > now then
  return { 'HELD' }
end
local kind = 'initial'
if cur[3] and cur[3] ~= ARGV[2] then
  kind = 'takeover'
end
local epoch = redis.call('HINCRBY', KEYS[2], 'epoch', 1)
local expires = fmt_seconds(now + tonumber(ARGV[3]))
redis.call('HSET', KEYS[2], 'owner', ARGV[2], 'expires_at', expires, 'last_owner', ARGV[2])
redis.call('ZADD', KEYS[3], expires, rid)
redis.call('ZREM', KEYS[4], rid)
return { 'OK', tostring(epoch), expires, kind }
