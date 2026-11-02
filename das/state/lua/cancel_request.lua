-- cancel_request: Gateway cancellation (Req 1.6, 1.8; Phase 2: 2.3, 13.15, 14.2, 15.11;
-- Phase 4 Req 3.3: leaves the client's Active_Set; Phase 4 Req 6.1, 6.3, 6.4, 8.2:
-- job.transition Events, then the Terminal_Event and, with a webhook target, the
-- Webhook_Delivery, after the writes above).
-- KEYS[1] request hash, KEYS[2] jobs hash, KEYS[3] transitions list, KEYS[4] das:requests:pending,
-- KEYS[5] pending_replays set, KEYS[6] das:requests:index, KEYS[7] das:requests:finished,
-- KEYS[8] das:requests:inbox_purge, KEYS[9] das:requests:by_status:Pending,
-- KEYS[10] das:requests:by_status:Running, KEYS[11] das:requests:by_status:Cancelled,
-- KEYS[12] das:dlq:by_status:Pending, KEYS[13] das:dlq:by_status:Replayed,
-- KEYS[14] das:dlq:terminal, KEYS[15..] das:dlq:{dlid} for each dlid of ARGV[2], in order
-- ARGV[1] request_id, ARGV[2] JSON array of the Dead_Letter_IDs read from pending_replays,
-- ARGV[3] budget view JSON spliced into the Terminal_Event ('' for none),
-- ARGV[4..] triples: job_id, expected current JobRecord JSON, new JobRecord JSON
-- The caller computes the Cancelled records; this script applies them only if every
-- listed Job still has the expected JSON, no other Job is non-terminal-and-unlisted,
-- and pending_replays holds exactly the listed Dead_Letter_IDs (compare-and-set).
-- Otherwise it returns {'CONFLICT'} and the caller re-reads and retries.
-- Replays accepted but never applied are returned to Pending and the set is deleted.
-- Returns {'OK', at, finished_at_ms, created_at_ms} | {'TERMINAL', status} | {'NOT_FOUND'}
--   | {'CONFLICT'}.
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  return { 'NOT_FOUND' }
end
if is_terminal(status) then
  return { 'TERMINAL', status }
end
local dlids = cjson.decode(ARGV[2] or '[]')
if type(dlids) ~= 'table' then
  return redis.error_reply('cancel_request: ARGV[2] must be a JSON array')
end
if #KEYS ~= 14 + #dlids then
  return redis.error_reply('cancel_request: one dlq record key per Dead_Letter_ID')
end
check_budget_view(ARGV[3])
if (#ARGV - 3) % 3 ~= 0 then
  return redis.error_reply('cancel_request: job triples are incomplete')
end
local expected = {}
for _, dlid in ipairs(dlids) do expected[dlid] = true end
local members = redis.call('SMEMBERS', KEYS[5])
if #members ~= #dlids then
  return { 'CONFLICT' }
end
for _, m in ipairs(members) do
  if not expected[m] then return { 'CONFLICT' } end
end
local listed = {}
for i = 4, #ARGV, 3 do
  if redis.call('HGET', KEYS[2], ARGV[i]) ~= ARGV[i + 1] then
    return { 'CONFLICT' }
  end
  cjson.decode(ARGV[i + 2])
  listed[ARGV[i]] = true
end
local TERMINAL_JOB = { Succeeded = true, Failed = true, Skipped = true, Cancelled = true }
local all = redis.call('HGETALL', KEYS[2])
for i = 1, #all, 2 do
  if not listed[all[i]] and not TERMINAL_JOB[cjson.decode(all[i + 1]).state] then
    return { 'CONFLICT' }
  end
end
local rid = ARGV[1]
local _, ms = server_time()
local at = iso_ms(ms)
local created = created_ms(KEYS[1], KEYS[6], rid)
for i = 4, #ARGV, 3 do
  put_job(KEYS[2], KEYS[3], ARGV[i], ARGV[i + 2], at, nil, KEYS[1])
end
redis.call('HSET', KEYS[1], 'status', 'Cancelled')
client_active_remove(KEYS[1], rid)  -- Phase 4 Req 3.3
mark_finished(KEYS[1], KEYS[7], KEYS[8], rid, ms)
local from_key = KEYS[10]
if status == 'Pending' then from_key = KEYS[9] end
index_move(rid, from_key, KEYS[11], created)
redis.call('ZREM', KEYS[4], rid)
for i, dlid in ipairs(dlids) do
  dlq_revert(KEYS[14 + i], KEYS[12], KEYS[13], KEYS[14], dlid, rid, ms)
end
redis.call('DEL', KEYS[5])
terminal_event(KEYS[1], rid, 'Cancelled', nil, ARGV[3], ms)
return { 'OK', at, tostring(ms), tostring(created) }
