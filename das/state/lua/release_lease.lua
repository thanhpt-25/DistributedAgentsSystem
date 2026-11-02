-- release_lease: give up a lease held by (owner, epoch).
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] das:leases:expiry, KEYS[4] das:requests:pending
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] epoch
-- Returns {'OK'} | {'LEASE_LOST'}.
-- The epoch field is kept so fencing tokens stay monotonic. A non-terminal Request
-- becomes claimable immediately (expiry score = now; Pending also re-enters the pending set).
local rid = ARGV[1]
local v = redis.call('HMGET', KEYS[2], 'owner', 'epoch')
if not v[1] or v[1] ~= ARGV[2] or tonumber(v[2]) ~= tonumber(ARGV[3]) then
  return { 'LEASE_LOST' }
end
redis.call('HDEL', KEYS[2], 'owner', 'expires_at')
local status = redis.call('HGET', KEYS[1], 'status')
if status and not is_terminal(status) then
  local now = server_time()
  redis.call('ZADD', KEYS[3], fmt_seconds(now), rid)
  if status == 'Pending' then
    redis.call('ZADD', KEYS[4], redis.call('HGET', KEYS[1], 'created_at') or fmt_seconds(now), rid)
  end
else
  redis.call('ZREM', KEYS[3], rid)
end
return { 'OK' }
