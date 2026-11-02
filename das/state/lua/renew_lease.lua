-- renew_lease: extend an owned, unexpired lease to now + duration (Req 2.4).
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] das:leases:expiry
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] epoch, ARGV[4] duration (seconds)
-- Returns {'OK', expires_at} | {'LEASE_LOST'} | {'REQUEST_TERMINAL'}.
local now = server_time()
if not lease_ok(KEYS[2], ARGV[2], ARGV[3], now) then
  return { 'LEASE_LOST' }
end
if is_terminal(redis.call('HGET', KEYS[1], 'status')) then
  return { 'REQUEST_TERMINAL' }
end
local expires = fmt_seconds(now + tonumber(ARGV[4]))
redis.call('HSET', KEYS[2], 'expires_at', expires)
redis.call('ZADD', KEYS[3], expires, ARGV[1])
return { 'OK', expires }
