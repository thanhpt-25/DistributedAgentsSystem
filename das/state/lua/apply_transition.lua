-- apply_transition: atomically apply one RequestMachine decision (Req 1.7, 2.7, 13.7;
-- Phase 2: 9.2, 9.5, 13.4, 13.15; Phase 4 Req 6.1, 6.3: one job.transition Event per
-- transition entry, in the same write).
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] jobs hash, KEYS[4] results hash,
-- KEYS[5] transitions list, KEYS[6] timers zset, KEYS[7] usage list,
-- KEYS[8] pending_replays set, KEYS[9] das:dlq:by_status:Pending,
-- KEYS[10] das:dlq:by_status:Replayed, KEYS[11] das:dlq:terminal,
-- KEYS[12..] das:dlq:{dlid} for each dead_letter_reverts entry, in order
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] epoch, ARGV[4] batch JSON:
--   { jobs = [[job_id, record_json]...], results = [[field, result_json]...],
--     timers_add = [[member, score]...], timers_remove = [member...],
--     inbox_cursor = string|null, request_fields = [[field, value]...],
--     usage = [[result_field, usage_json]...], tag = dlid|null,
--     replays_applied = [dlid...], dead_letter_reverts = [dlid...], job_count = int|null }
-- Job_Results are written with HSETNX, so the first stored result for an
-- (job_id, attempt) is never replaced (Req 2.8, 6.5). A usage entry is appended only
-- when the HSETNX of the result with the same field stored a new result (Req 9.2, 9.5).
-- tag (Dead_Letter_ID) is written on every transition entry of the batch (Req 13.4).
-- replays_applied are removed from pending_replays; dead_letter_reverts move their
-- Dead_Letter_Record Replayed -> Pending (compare-and-set, Req 13.15).
-- Returns {'OK', at, transitions_written} | {'LEASE_LOST'} | {'REQUEST_TERMINAL'} | {'NOT_FOUND'}.
local now, ms = server_time()
if not lease_ok(KEYS[2], ARGV[2], ARGV[3], now) then
  return { 'LEASE_LOST' }
end
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  return { 'NOT_FOUND' }
end
if is_terminal(status) then
  return { 'REQUEST_TERMINAL' }
end

-- Redis does not roll back a script on a runtime error: decode and check every
-- input before the first write, so malformed input aborts with nothing written.
local batch = cjson.decode(ARGV[4])
if type(batch) ~= 'table' then
  return redis.error_reply('apply_transition: batch must be an object')
end
local function list(name)
  local v = batch[name]
  if v == nil or v == cjson.null then return {} end
  if type(v) ~= 'table' then error('apply_transition: ' .. name .. ' must be an array') end
  return v
end
local function pairs_of(name)
  local v = list(name)
  for _, pair in ipairs(v) do
    if type(pair) ~= 'table' or pair[1] == nil or pair[2] == nil then
      error('apply_transition: ' .. name .. ' entries must be pairs')
    end
  end
  return v
end
local jobs = pairs_of('jobs')
local results = pairs_of('results')
local timers_add = pairs_of('timers_add')
local timers_remove = list('timers_remove')
local request_fields = pairs_of('request_fields')
local usage = pairs_of('usage')
local replays_applied = list('replays_applied')
local reverts = list('dead_letter_reverts')
for _, pair in ipairs(jobs) do
  cjson.decode(pair[2])
end
for _, pair in ipairs(timers_add) do
  if not tonumber(pair[2]) then error('apply_transition: timer score must be a number') end
end
for _, pair in ipairs(usage) do
  check_usage('[' .. cjson.encode(pair[2]) .. ']')
end
if #KEYS ~= 11 + #reverts then
  return redis.error_reply('apply_transition: one dlq record key per dead_letter_revert')
end
local tag = batch.tag
if tag == cjson.null or tag == '' then tag = nil end
local job_count = batch.job_count
if job_count == cjson.null then job_count = nil end
if job_count ~= nil and not tonumber(job_count) then
  error('apply_transition: job_count must be a number')
end

local rid = ARGV[1]
local at = iso_ms(ms)
local written = 0
for _, pair in ipairs(jobs) do
  written = written + put_job(KEYS[3], KEYS[5], pair[1], pair[2], at, tag, KEYS[1])
end
local stored = {}
for _, pair in ipairs(results) do
  if redis.call('HSETNX', KEYS[4], pair[1], pair[2]) == 1 then
    stored[pair[1]] = true
  end
end
local accepted = {}
for _, pair in ipairs(usage) do
  if stored[pair[1]] then
    accepted[#accepted + 1] = pair[2]
  end
end
if #accepted > 0 then
  append_usage(KEYS[7], KEYS[1], cjson.encode(accepted))
end
for _, member in ipairs(timers_remove) do
  redis.call('ZREM', KEYS[6], member)
end
for _, pair in ipairs(timers_add) do
  redis.call('ZADD', KEYS[6], pair[2], pair[1])
end
for _, pair in ipairs(request_fields) do
  redis.call('HSET', KEYS[1], pair[1], pair[2])
end
if batch.inbox_cursor and batch.inbox_cursor ~= cjson.null then
  redis.call('HSET', KEYS[1], 'inbox_cursor', batch.inbox_cursor)
end
if job_count ~= nil then
  redis.call('HSET', KEYS[1], 'job_count', tostring(math.floor(tonumber(job_count))))
end
for _, dlid in ipairs(replays_applied) do
  redis.call('SREM', KEYS[8], dlid)
end
for i, dlid in ipairs(reverts) do
  dlq_revert(KEYS[11 + i], KEYS[9], KEYS[10], KEYS[11], dlid, rid, ms)
end
return { 'OK', at, tostring(written) }
