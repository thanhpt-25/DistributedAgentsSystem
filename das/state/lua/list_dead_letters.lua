-- list_dead_letters: one page of Dead_Letter_IDs in (dead_lettered_at desc, dlid desc)
-- order, filtered on every given filter (Req 12.4, 12.5, 12.6).
-- KEYS[1] source zset scored by dead_lettered_at ms (das:dlq:by_status:{s},
-- das:dlq:by_reason:{r} or das:dlq:index)
-- ARGV[1] after_score ('' = from the start), ARGV[2] after_member (with ARGV[1]),
-- ARGV[3] filters JSON {status?, reason?, request_id?, capability?}, ARGV[4] n,
-- ARGV[5] record key prefix ('das:dlq:'). Record hashes are read by name: Phase 2
-- supports a single Redis instance only (Req 20.7).
-- Walks the zset downward from the cursor: members with score == after_score and
-- member < after_member, then members with score < after_score, reading
-- status/reason/request_id/capabilities of each record until n matched or the zset ends.
-- Returns [dlid, score, dlid, score, ...] (at most n pairs).
local n = tonumber(ARGV[4])
if not n or n < 1 then
  return redis.error_reply('list_dead_letters: n must be >= 1')
end
local filters = cjson.decode(ARGV[3] ~= '' and ARGV[3] or '{}')
local function want(v)
  if v == nil or v == cjson.null or v == '' then return nil end
  return v
end
local f_status = want(filters.status)
local f_reason = want(filters.reason)
local f_rid = want(filters.request_id)
local f_cap = want(filters.capability)

local out = {}
local found = 0
local function consider(dlid, score)
  local v = redis.call('HMGET', ARGV[5] .. dlid, 'status', 'reason', 'request_id',
    'capabilities')
  if not v[1] then return end  -- index entry of a record removed concurrently
  if f_status and v[1] ~= f_status then return end
  if f_reason and v[2] ~= f_reason then return end
  if f_rid and v[3] ~= f_rid then return end
  if f_cap then
    if not v[4] or v[4] == '' then return end
    local caps = cjson.decode(v[4])
    local hit = false
    if type(caps) == 'table' then
      for _, c in ipairs(caps) do
        if c == f_cap then hit = true break end
      end
    end
    if not hit then return end
  end
  out[#out + 1] = dlid
  out[#out + 1] = score
  found = found + 1
end

local max = '+inf'
if ARGV[1] ~= '' then
  -- Ties on the cursor score: members below after_member, in descending order.
  local ties = redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[1])
  for _, m in ipairs(ties) do
    if found >= n then break end
    if m < ARGV[2] then consider(m, ARGV[1]) end
  end
  max = '(' .. ARGV[1]
end
local chunk = 200
local offset = 0
while found < n do
  local batch = redis.call('ZREVRANGEBYSCORE', KEYS[1], max, '-inf', 'WITHSCORES',
    'LIMIT', offset, chunk)
  if #batch == 0 then break end
  for i = 1, #batch, 2 do
    if found >= n then break end
    consider(batch[i], batch[i + 1])
  end
  offset = offset + chunk
end
return out
