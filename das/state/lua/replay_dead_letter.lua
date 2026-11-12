-- replay_dead_letter: accept or reject a Replay of one Dead_Letter_Record (Req 13.1-13.3,
-- 13.6-13.8; design D7, D8). Checks follow das.ops.replay.decide_replay, first violated
-- condition wins: NOT_FOUND, RECORD_NOT_PENDING, NOT_REPLAYABLE, REQUEST_GONE,
-- REQUEST_STATE, BUDGET_EXCEEDED (Failed Request whose error is budget_exceeded or
-- budget_unenforceable), JOB_STATE, STALE_ATTEMPT, LEASE_HELD (Failed Request only).
-- A rejection writes nothing.
-- KEYS[1] das:dlq:{dlid}, KEYS[2] request hash, KEYS[3] jobs hash, KEYS[4] lease hash,
-- KEYS[5] inbox stream, KEYS[6] transitions list, KEYS[7] pending_replays set,
-- KEYS[8] das:requests:pending, KEYS[9] das:requests:index,
-- KEYS[10] das:requests:by_status:Failed, KEYS[11] das:requests:by_status:Pending,
-- KEYS[12] das:requests:finished, KEYS[13] das:requests:inbox_purge,
-- KEYS[14] das:dlq:by_status:Pending, KEYS[15] das:dlq:by_status:Replayed,
-- KEYS[16] das:dlq:terminal
-- ARGV[1] dead_letter_id, ARGV[2] request_id the shell read from the record ('' for none)
-- Running Request: record -> Replayed, XADD inbox replay event, SADD pending_replays;
-- the Lease owner applies it through the RequestMachine (fenced).
-- Failed Request (no unexpired Lease): additionally status Failed -> Pending with a
-- request transition entry tagged with the dlid, error/final_result/finished_at moved
-- into replay_history, Finished_At cleared, index Failed -> Pending, removed from the
-- retention zsets, queued on das:requests:pending, added back to the client's
-- Active_Set regardless of the Concurrent_Quota (Phase 4 Req 3.4), and request.reopened
-- {"status":"Pending"} appended to the Event_Stream (Phase 4 Req 6.1, 6.4). A Replay of
-- a Running Request appends no Event.
-- Returns {'ACCEPTED', request_status_before, new_attempt} | {code, current_state}
--   | {'NOT_FOUND'} | {'REQUEST_GONE'} | {'CONFLICT'} (record names another Request).
local rec = redis.call('HMGET', KEYS[1], 'status', 'reason', 'request_id', 'job_id',
  'attempt', 'dead_lettered_at')
if not rec[1] then
  return { 'NOT_FOUND' }
end
if rec[1] ~= 'Pending' then
  return { 'RECORD_NOT_PENDING', rec[1] }
end
if rec[2] == 'malformed' then
  return { 'NOT_REPLAYABLE', rec[2] }
end
local rid = rec[3] or ''
if rid ~= ARGV[2] then
  return { 'CONFLICT' }
end
if rid == '' then
  return { 'REQUEST_GONE' }
end
local status = redis.call('HGET', KEYS[2], 'status')
if not status then
  return { 'REQUEST_GONE' }
end
if status ~= 'Running' and status ~= 'Failed' then
  return { 'REQUEST_STATE', status }
end
-- A Request stopped by its budget would spend it again (Phase 4 Req 5.10); the same list as
-- das.ops.replay.BUDGET_FAILURES.
if status == 'Failed' then
  local err = redis.call('HGET', KEYS[2], 'error')
  if err == 'budget_exceeded' or err == 'budget_unenforceable' then
    return { 'BUDGET_EXCEEDED' }
  end
end
local job_id = rec[4]
local job_json = nil
if job_id and job_id ~= '' then
  job_json = redis.call('HGET', KEYS[3], job_id)
end
if not job_json then
  return { 'JOB_STATE', '' }
end
local job = cjson.decode(job_json)
if job.state ~= 'Failed' then
  return { 'JOB_STATE', tostring(job.state) }
end
local job_attempt = tonumber(job.attempt) or 0
local rec_attempt = tonumber(rec[5] or '')
if rec_attempt == nil or rec_attempt ~= job_attempt then
  return { 'STALE_ATTEMPT', tostring(job_attempt) }
end
local now, ms = server_time()
if status == 'Failed' then
  local l = redis.call('HMGET', KEYS[4], 'owner', 'expires_at')
  if l[1] and l[2] and tonumber(l[2]) > now then
    return { 'LEASE_HELD', status }
  end
end

-- Accepted: every check passed, start writing.
local dlid = ARGV[1]
local at = iso_ms(ms)
if status == 'Failed' then
  local cur = redis.call('HMGET', KEYS[2], 'error', 'final_result', 'finished_at',
    'replay_history', 'created_at')
  local function json_or_null(x, encode)
    if not x or x == '' then return 'null' end
    return encode(x)
  end
  -- Built as text so the stored Final_Result JSON is embedded verbatim (cjson would
  -- turn empty arrays into objects).
  local entry = '{"dead_letter_id":' .. cjson.encode(dlid)
    .. ',"at":' .. cjson.encode(at)
    .. ',"error":' .. json_or_null(cur[1], cjson.encode)
    .. ',"final_result":' .. json_or_null(cur[2], function(x) return x end)
    .. ',"finished_at":' .. json_or_null(cur[3], function(x)
      return cjson.encode(iso_ms(math.floor(tonumber(x))))
    end)
    .. '}'
  local history = cur[4]
  if not history or history == '' or string.match(history, '^%s*%[%s*%]%s*$') then
    history = '[' .. entry .. ']'
  else
    history = string.gsub(history, '%]%s*$', '') .. ',' .. entry .. ']'
  end
  redis.call('HSET', KEYS[2], 'replay_history', history, 'status', 'Pending')
  redis.call('HDEL', KEYS[2], 'error', 'final_result', 'finished_at')
  -- The reopen is recorded here, in the same write, so a cancel before a Manager
  -- resumes the Request still follows request.reopened (Phase 4 Req 6.1, 6.4).
  append_event(KEYS[2], 'request.reopened', '{"status":"Pending"}')
  -- Reopened: back in the Active_Set even when it is full (Phase 4 Req 3.4).
  client_active_add(KEYS[2], rid)
  redis.call('RPUSH', KEYS[6], cjson.encode({
    scope = 'request', from_status = 'Failed', to_status = 'Pending',
    dead_letter_id = dlid, at = at,
  }))
  local created = created_ms(KEYS[2], KEYS[9], rid)
  index_move(rid, KEYS[10], KEYS[11], created, KEYS[2])  -- and the tenant's twins (Phase 5 Req 3.5)
  redis.call('ZREM', KEYS[12], rid)
  redis.call('ZREM', KEYS[13], rid)
  local pending_score = tonumber(cur[5] or '') or (created / 1000)
  redis.call('ZADD', KEYS[8], pending_score, rid)
end
redis.call('XADD', KEYS[5], '*', 'kind', 'replay', 'data', cjson.encode({
  dead_letter_id = dlid, job_id = job_id, attempt = rec_attempt,
}))
redis.call('SADD', KEYS[7], dlid)
redis.call('HSET', KEYS[1], 'status', 'Replayed', 'status_changed_at', tostring(ms))
redis.call('ZREM', KEYS[14], dlid)
redis.call('ZADD', KEYS[15], tonumber(rec[6] or '') or ms, dlid)
redis.call('ZADD', KEYS[16], ms, dlid)
dlq_twin_move(KEYS[1], dlid, KEYS[14], KEYS[15], tonumber(rec[6] or '') or ms)  -- Phase 5 Req 5.1
return { 'ACCEPTED', status, tostring(job_attempt + 1) }
