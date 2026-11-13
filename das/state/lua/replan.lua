-- replan: swap the unfinished part of a Running Request's plan for a new one, atomically
-- (das-smarter-agents Req 3.4). Every Succeeded Job and its result stay; every Failed or Skipped Job leaves the plan
-- (its results stay as history); the new Jobs are written Waiting with their transitions. The Request stays Running.
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] jobs hash, KEYS[4] transitions list, KEYS[5] usage list
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] epoch,
-- ARGV[4] the new plan JSON (every Job, kept and new),
-- ARGV[5] JSON array of the planning UsageRecords ('' or '[]' for none),
-- ARGV[6] request.replanned Event data JSON (stored verbatim),
-- ARGV[7] the replans count the caller read: refused when the stored count differs (a stale caller),
-- ARGV[8..] alternating job_id, JobRecord JSON for every NEW Job.
-- Returns {'OK', at} | {'LEASE_LOST'} | {'REQUEST_TERMINAL'} | {'INVALID_STATE', reason}
-- where reason is the status, 'replans', 'jobs_active' or 'invalid_plan: ...'. Nothing is written unless it is OK.
local now, ms = server_time()
if not lease_ok(KEYS[2], ARGV[2], ARGV[3], now) then
  return { 'LEASE_LOST' }
end
local status = redis.call('HGET', KEYS[1], 'status')
if is_terminal(status) then
  return { 'REQUEST_TERMINAL' }
end
if status ~= 'Running' then
  return { 'INVALID_STATE', status or '' }
end
local previous = tonumber(redis.call('HGET', KEYS[1], 'replans')) or 0
if previous ~= tonumber(ARGV[7]) then
  return { 'INVALID_STATE', 'replans' }
end
-- Redis does not roll back a script on a runtime error, so decode every input before the first write.
if (#ARGV - 7) % 2 ~= 0 then
  return redis.error_reply('replan: job_id without a record')
end
local plan = cjson.decode(ARGV[4])
if type(plan) ~= 'table' or type(plan.jobs) ~= 'table' then
  error('replan: the plan must be an object with jobs')
end
local new_ids, new_count = {}, 0
for i = 8, #ARGV, 2 do
  cjson.decode(ARGV[i + 1])
  new_ids[ARGV[i]] = true
  new_count = new_count + 1
end
if type(cjson.decode(ARGV[6])) ~= 'table' then
  error('replan: the replanned event must be a JSON object')
end
check_usage(ARGV[5])

-- Sort the stored Jobs: Succeeded are kept, Failed and Skipped are replaced, anything else is still in flight.
local kept, kept_count, removed = {}, 0, {}
local stored = redis.call('HGETALL', KEYS[3])
for i = 1, #stored, 2 do
  local job_id = stored[i]
  local state = cjson.decode(stored[i + 1]).state
  if state == 'Succeeded' then
    kept[job_id] = true
    kept_count = kept_count + 1
  elseif state == 'Failed' or state == 'Skipped' then
    removed[#removed + 1] = job_id
  else
    return { 'INVALID_STATE', 'jobs_active' }
  end
  -- Results are keyed by (job_id, attempt): a new Job under a stored id would meet that Job's results.
  if new_ids[job_id] then
    return { 'INVALID_STATE', 'invalid_plan: job ' .. job_id .. ' already exists' }
  end
end
if new_count < 1 then
  return { 'INVALID_STATE', 'invalid_plan: no new job' }
end
if #plan.jobs ~= kept_count + new_count then
  return { 'INVALID_STATE', 'invalid_plan: the plan does not list exactly the kept and the new jobs' }
end
for _, planned in ipairs(plan.jobs) do
  if not kept[planned.job_id] and not new_ids[planned.job_id] then
    return { 'INVALID_STATE', 'invalid_plan: job ' .. tostring(planned.job_id) .. ' is neither kept nor new' }
  end
end
for job_id in pairs(kept) do
  local listed = false
  for _, planned in ipairs(plan.jobs) do
    if planned.job_id == job_id then listed = true end
  end
  if not listed then
    return { 'INVALID_STATE', 'invalid_plan: succeeded job ' .. job_id .. ' is missing from the plan' }
  end
end

local at = iso_ms(ms)
-- The Event first, then one job.transition per new Job (the transition log and the stream stay one-for-one).
append_event(KEYS[1], 'request.replanned', ARGV[6])
for _, job_id in ipairs(removed) do
  redis.call('HDEL', KEYS[3], job_id)
end
for i = 8, #ARGV, 2 do
  put_job(KEYS[3], KEYS[4], ARGV[i], ARGV[i + 1], at, nil, KEYS[1])
end
redis.call('HSET', KEYS[1], 'plan', ARGV[4], 'job_count', tostring(#plan.jobs))
redis.call('HINCRBY', KEYS[1], 'replans', 1)
append_usage(KEYS[5], KEYS[1], ARGV[5])
return { 'OK', at }
