-- list_index: one page of the Request_Index in (created_at desc, Request_ID desc) order
-- (Req 15.1, 15.2, 15.7, 15.12; design D9).
-- KEYS[1..k] 1 to 5 source zsets scored by created_at ms (das:requests:by_status:{s}
-- for the selected statuses, or das:requests:index when no status filter is given)
-- ARGV[1] after_score ('' = from the start), ARGV[2] after_member (with ARGV[1]),
-- ARGV[3] min_score inclusive ('' = -inf), ARGV[4] max_score ('' = +inf),
-- ARGV[5] '1' when max_score is exclusive, ARGV[6] n
-- Per zset: members with score == after_score and member < after_member, then up to
-- n members with score < after_score, all within [min, max]. The per-zset lists are
-- merged k-way by (score desc, member desc) and cut to n. The cursor position itself
-- need not exist any more (a deleted Request resumes at the next one, Req 15.12).
-- Returns [member, score, member, score, ...] (at most n pairs).
local n = tonumber(ARGV[6])
if not n or n < 1 then
  return redis.error_reply('list_index: n must be >= 1')
end
if #KEYS < 1 or #KEYS > 5 then
  return redis.error_reply('list_index: expected 1 to 5 source zsets')
end
local has_after = ARGV[1] ~= ''
local after = tonumber(ARGV[1])
if has_after and not after then
  return redis.error_reply('list_index: after_score must be a number')
end
local min_spec = '-inf'
local min_v = -math.huge
if ARGV[3] ~= '' then
  min_v = tonumber(ARGV[3])
  min_spec = ARGV[3]
end
local max_v = math.huge
local max_excl = ARGV[5] == '1'
local max_spec = '+inf'
if ARGV[4] ~= '' then
  max_v = tonumber(ARGV[4])
  max_spec = ARGV[4]
  if max_excl then max_spec = '(' .. ARGV[4] end
end

local function in_range(score)
  if score < min_v then return false end
  if max_excl then return score < max_v end
  return score <= max_v
end

-- Upper bound of the "strictly below the cursor" part.
local upper = max_spec
local ties_ok = false
if has_after then
  if after < max_v or (after == max_v and not max_excl) then
    upper = '(' .. ARGV[1]
    ties_ok = in_range(after)
  end
end

local lists = {}
for k = 1, #KEYS do
  local items = {}
  if ties_ok then
    local ties = redis.call('ZREVRANGEBYSCORE', KEYS[k], ARGV[1], ARGV[1], 'WITHSCORES')
    for i = 1, #ties, 2 do
      if #items >= n then break end
      if ties[i] < ARGV[2] then
        items[#items + 1] = { ties[i], tonumber(ties[i + 1]), ties[i + 1] }
      end
    end
  end
  if #items < n then
    local rest = redis.call('ZREVRANGEBYSCORE', KEYS[k], upper, min_spec, 'WITHSCORES',
      'LIMIT', 0, n - #items)
    for i = 1, #rest, 2 do
      items[#items + 1] = { rest[i], tonumber(rest[i + 1]), rest[i + 1] }
    end
  end
  lists[k] = items
end

-- k-way merge by (score desc, member desc).
local heads = {}
for k = 1, #lists do heads[k] = 1 end
local out = {}
local seen = {}
local count = 0
while count < n do
  local best = nil
  for k = 1, #lists do
    local item = lists[k][heads[k]]
    if item then
      if not best then
        best = k
      else
        local b = lists[best][heads[best]]
        if item[2] > b[2] or (item[2] == b[2] and item[1] > b[1]) then
          best = k
        end
      end
    end
  end
  if not best then break end
  local item = lists[best][heads[best]]
  heads[best] = heads[best] + 1
  if not seen[item[1]] then
    seen[item[1]] = true
    out[#out + 1] = item[1]
    out[#out + 1] = item[3]
    count = count + 1
  end
end
return out
