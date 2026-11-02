-- claim_webhook: claim the next due Webhook_Delivery (Phase 4 Req 8.3).
-- Scans at most 10 rids with score <= now (Redis TIME). A rid whose delivery hash is
-- missing or not pending is removed from the outbox. The first pending one is
-- re-scored to now + lease, so no other sender sees it as due until the lease ends,
-- and its claim counter is incremented; complete_webhook applies only with that
-- claim value and the delivery's event_id. The hash key is built from ARGV[2] (single Redis primary, OD 4.1).
-- KEYS[1] das:webhooks:due
-- ARGV[1] lease seconds (> 0), ARGV[2] request key prefix 'das:req:'
-- Returns {rid, claim, attempts, url, secret_name, event_id, type, data, now} or {};
-- now is the Redis time of the claim (fmt_double), so the sender computes next_due
-- from Redis time, never from its host clock.
local lease = tonumber(ARGV[1])
if not lease or lease ~= lease or lease <= 0 or lease == math.huge then
  return redis.error_reply('claim_webhook: lease_seconds must be a finite number > 0')
end
local prefix = ARGV[2]
if not prefix or prefix == '' then
  return redis.error_reply('claim_webhook: request key prefix required')
end
local now = server_time()
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', fmt_double(now), 'LIMIT', 0, 10)
for _, rid in ipairs(due) do
  local wk = prefix .. rid .. ':webhook'
  local v = redis.call('HMGET', wk, 'state', 'attempts', 'url', 'secret_name', 'event_id', 'type', 'data')
  if v[1] ~= 'pending' then
    redis.call('ZREM', KEYS[1], rid)
  else
    redis.call('ZADD', KEYS[1], fmt_double(now + lease), rid)
    local claim = redis.call('HINCRBY', wk, 'claim', 1)
    return { rid, tostring(claim), v[2] or '0', v[3] or '', v[4] or '', v[5] or '', v[6] or '', v[7] or '{}',
      fmt_double(now) }
  end
end
return {}
