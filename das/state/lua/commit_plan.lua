-- commit_plan: persist the Job_Plan, all Jobs as Waiting, their transitions, the
-- planning Usage_Records, and status Pending -> Running in one atomic update
-- (Req 3.9, 3.12, 13.7; Phase 2: 9.1, 15.3, 15.11), with the request.planned Event and
-- one job.transition Event per Job in the same write (Phase 4 Req 6.1, 6.3).
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] jobs hash, KEYS[4] transitions list,
-- KEYS[5] das:requests:pending, KEYS[6] usage list, KEYS[7] das:requests:index,
-- KEYS[8] das:requests:by_status:Pending, KEYS[9] das:requests:by_status:Running
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] epoch, ARGV[4] plan JSON,
-- ARGV[5] JSON array of planning UsageRecords ('' or '[]' for none),
-- ARGV[6] request.planned Event data JSON ('' for none: no event),
-- ARGV[7..] alternating job_id, JobRecord JSON
-- Returns {'OK', at} | {'LEASE_LOST'} | {'REQUEST_TERMINAL'} | {'INVALID_STATE', status}.
local now, ms = server_time()
if not lease_ok(KEYS[2], ARGV[2], ARGV[3], now) then
  return { 'LEASE_LOST' }
end
local status = redis.call('HGET', KEYS[1], 'status')
if is_terminal(status) then
  return { 'REQUEST_TERMINAL' }
end
if status ~= 'Pending' then
  return { 'INVALID_STATE', status or '' }
end
-- Redis does not roll back a script on a runtime error, so decode every input
-- before the first write: malformed input aborts with nothing written.
if (#ARGV - 6) % 2 ~= 0 then
  return redis.error_reply('commit_plan: job_id without a record')
end
for i = 8, #ARGV, 2 do
  cjson.decode(ARGV[i])
end
if ARGV[6] ~= '' and type(cjson.decode(ARGV[6])) ~= 'table' then
  error('commit_plan: planned event must be a JSON object')
end
check_usage(ARGV[5])
local rid = ARGV[1]
local score = created_ms(KEYS[1], KEYS[7], rid)
local at = iso_ms(ms)
redis.call('DEL', KEYS[3])
-- Events are written before the single plan+status HSET below, so they stay one-for-one
-- with the transition log, which put_job writes in the same loop as the job.transition Events.
if ARGV[6] ~= '' then
  -- Stored verbatim: Python builds it, so an empty depends_on stays [] (Phase 4 Req 6.2).
  append_event(KEYS[1], 'request.planned', ARGV[6])
end
local job_count = 0
for i = 7, #ARGV, 2 do
  put_job(KEYS[3], KEYS[4], ARGV[i], ARGV[i + 1], at, nil, KEYS[1])
  job_count = job_count + 1
end
redis.call('HSET', KEYS[1], 'plan', ARGV[4], 'status', 'Running', 'job_count', tostring(job_count))
redis.call('ZREM', KEYS[5], rid)
index_move(rid, KEYS[8], KEYS[9], score)
append_usage(KEYS[6], KEYS[1], ARGV[5])
return { 'OK', at }
