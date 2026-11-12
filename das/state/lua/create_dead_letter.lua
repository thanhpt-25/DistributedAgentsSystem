-- create_dead_letter: persist exactly one Dead_Letter_Record per dead-lettered Message
-- (Req 12.1, 12.2; design D6: the Dead_Letter_ID is deterministic, so a redelivery to
-- any Manager hits EXISTS and writes nothing).
-- KEYS[1] das:dlq:{dlid}, KEYS[2] das:dlq:index, KEYS[3] das:dlq:by_status:Pending,
-- KEYS[4] das:dlq:by_reason:{reason}
-- ARGV[1] dead_letter_id, ARGV[2..] alternating field, value of the record (strings;
-- absent fields are omitted). status and dead_lettered_at are set by the script.
-- Returns {'CREATED', dead_lettered_at_ms} | {'EXISTS'}.
if redis.call('EXISTS', KEYS[1]) == 1 then
  return { 'EXISTS' }
end
if (#ARGV - 1) % 2 ~= 0 then
  return redis.error_reply('create_dead_letter: field without a value')
end
local _, ms = server_time()
local dlid = ARGV[1]
for i = 2, #ARGV, 2 do
  local f = ARGV[i]
  if f ~= 'status' and f ~= 'dead_lettered_at' and f ~= 'dead_letter_id' then
    redis.call('HSET', KEYS[1], f, ARGV[i + 1])
  end
end
redis.call('HSET', KEYS[1], 'dead_letter_id', dlid, 'status', 'Pending',
  'dead_lettered_at', tostring(ms))
redis.call('ZADD', KEYS[2], ms, dlid)
redis.call('ZADD', KEYS[3], ms, dlid)
redis.call('ZADD', KEYS[4], ms, dlid)
-- Phase 5 Req 5.1, 5.2: a record whose Request exists belongs to that Request's tenant ('default' when the
-- Request has none): the tenant is stored on the record and the record joins the tenant's index twins. A
-- record without a request_id, or whose Request is gone, belongs to no tenant and has no twins.
local request_id
for i = 2, #ARGV, 2 do
  if ARGV[i] == 'request_id' then request_id = ARGV[i + 1] end
end
if request_id and request_id ~= '' then
  local at = string.find(KEYS[1], ':dlq:', 1, true)
  local request_key = string.sub(KEYS[1], 1, at - 1) .. ':req:' .. request_id
  if redis.call('EXISTS', request_key) == 1 then
    local t = tenant_of(request_key)
    redis.call('HSET', KEYS[1], 'tenant', t)
    for i = 2, 4 do
      local twin = tenant_twin(KEYS[i], t)
      if twin then redis.call('ZADD', twin, ms, dlid) end
    end
  end
end
return { 'CREATED', tostring(ms) }
