-- rate_check: take one token from a client's rate-limit bucket (Phase 4 Req 2.1-2.4).
-- Mirrors das.client.bucket.take with the script's own TIME, so every Gateway
-- replica shares one bucket and one clock:
--   rate == 0 -> limit disabled: OK, nothing read or written
--   missing hash -> full bucket (burst tokens)
--   refill: tokens = min(burst, tokens + rate * max(0, now - updated)),
--           updated = max(now, updated)
--   tokens >= 1 -> store tokens - 1: OK; otherwise store the refilled tokens: LIMITED
-- tokens and updated are stored as %.17g text so Python and Lua see the same doubles.
-- The key expires ceil(burst / rate * 1000) + 60000 ms after each call, longer than a
-- full refill, so an expired key is equivalent to a full bucket.
-- KEYS[1] das:client:{fp}:bucket
-- ARGV[1] rate (tokens per second, %.17g text), ARGV[2] burst (integer >= 1)
-- Returns {'OK'} | {'LIMITED', retry_after_seconds (%.17g text)}.
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
if not rate or rate ~= rate or rate < 0 or rate == math.huge then
  return redis.error_reply('rate_check: rate must be a finite number >= 0')
end
if not burst or burst < 1 or burst ~= math.floor(burst) then
  return redis.error_reply('rate_check: burst must be an integer >= 1')
end
if rate == 0 then
  return { 'OK' }
end
local now = server_time()
local v = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local old_tokens = tonumber(v[1] or '')
local old_updated = tonumber(v[2] or '')
local tokens, updated
if not old_tokens or not old_updated then
  tokens, updated = burst, now
else
  local elapsed = math.max(0, now - old_updated)
  tokens = math.min(burst, old_tokens + rate * elapsed)
  updated = math.max(now, old_updated)
end
local allowed = tokens >= 1
local stored = tokens
if allowed then stored = tokens - 1 end
redis.call('HSET', KEYS[1], 'tokens', fmt_double(stored), 'updated', fmt_double(updated))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 60000)
if allowed then
  return { 'OK' }
end
return { 'LIMITED', fmt_double((1 - tokens) / rate) }
