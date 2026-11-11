-- stream_acquire: register one open Event_Stream connection of a client (Phase 4 Req 7.7, 7.8).
-- A client's open connections are a zset: member = connection id, score = its expiry in Redis
-- time (seconds). A live stream renews its member, so the members of a crashed Gateway expire
-- on their own instead of counting against the client's limit forever.
--   expired members are pruned first
--   ZCARD >= max -> LIMIT, nothing registered
--   otherwise    -> member added with score now + ttl
-- The key expires 60 s after the longest possible member, so an idle client leaves no key behind.
-- KEYS[1] das:client:{fp}:streams
-- ARGV[1] member (unique per connection), ARGV[2] max_streams (integer >= 1),
-- ARGV[3] ttl in seconds (%.17g text, > 0)
-- Returns {'OK'} | {'LIMIT'}.
local max = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
if not max or max < 1 or max ~= math.floor(max) then
  return redis.error_reply('stream_acquire: max_streams must be an integer >= 1')
end
if not ttl or ttl ~= ttl or ttl <= 0 or ttl == math.huge then
  return redis.error_reply('stream_acquire: ttl must be a finite number > 0')
end
local now = server_time()
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', fmt_seconds(now))
if redis.call('ZCARD', KEYS[1]) >= max then
  return { 'LIMIT' }
end
redis.call('ZADD', KEYS[1], fmt_seconds(now + ttl), ARGV[1])
redis.call('PEXPIRE', KEYS[1], math.ceil(ttl * 1000) + 60000)
return { 'OK' }
