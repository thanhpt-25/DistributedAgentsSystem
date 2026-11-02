-- complete_webhook: record the outcome of one Webhook_Delivery attempt
-- (Phase 4 Req 8.3, 8.5-8.7).
-- Fenced by the claim counter and the event_id: applied only when the hash exists, is
-- pending, its claim equals ARGV[2] and its event_id equals ARGV[6]. A sender whose
-- lease ran out and whose delivery was claimed again fails the claim check; a sender
-- whose delivery was reset by a later terminal write (which restarts claim at 0 with a
-- new event_id, so the counter can match again) fails the event_id check. Either gets
-- STALE and changes nothing. Inputs are validated before any read or write.
--   delivered -> state delivered, ZREM outbox
--   failed    -> state failed, ZREM outbox
--   retry     -> stays pending, ZADD outbox next_due
-- Every applied outcome increments attempts and stores last_status.
-- KEYS[1] das:webhooks:due, KEYS[2] das:req:{rid}:webhook
-- ARGV[1] rid, ARGV[2] claim, ARGV[3] outcome, ARGV[4] last_status,
-- ARGV[5] next_due (Redis seconds, %.17g text; '' unless retry),
-- ARGV[6] event_id of the claimed delivery (required, non-empty)
-- Returns {'OK', attempts} | {'STALE'}.
local rid, claim, outcome, last_status = ARGV[1], ARGV[2], ARGV[3], ARGV[4] or ''
local event_id = ARGV[6]
if not event_id or event_id == '' then
  return redis.error_reply('complete_webhook: event_id required')
end
if outcome ~= 'delivered' and outcome ~= 'retry' and outcome ~= 'failed' then
  return redis.error_reply('complete_webhook: unknown outcome ' .. tostring(outcome))
end
local next_due
if outcome == 'retry' then
  next_due = tonumber(ARGV[5] or '')
  if not next_due or next_due ~= next_due or next_due == math.huge or next_due == -math.huge then
    return redis.error_reply('complete_webhook: retry needs a finite numeric next_due')
  end
end
local v = redis.call('HMGET', KEYS[2], 'state', 'claim', 'event_id')
if v[1] ~= 'pending' or v[2] ~= claim or v[3] ~= event_id then
  return { 'STALE' }
end
local attempts = redis.call('HINCRBY', KEYS[2], 'attempts', 1)
redis.call('HSET', KEYS[2], 'last_status', last_status)
if outcome == 'retry' then
  redis.call('ZADD', KEYS[1], fmt_double(next_due), rid)
else
  redis.call('HSET', KEYS[2], 'state', outcome)
  redis.call('ZREM', KEYS[1], rid)
end
return { 'OK', attempts }
