-- finalize: write the Final_Result and Usage_Records, then the terminal status, then
-- release the lease, all in one script (Req 3.6, 3.10, 3.11, 7.3, 7.4, 7.6;
-- Phase 2: 2.2, 9.1, 9.3, 14.2, 15.11; Phase 4 Req 3.3: leaves the client's Active_Set;
-- Phase 4 Req 6.1, 6.4, 8.2: appends the Terminal_Event and enqueues the Webhook_Delivery
-- when the Request has a webhook target, after the writes above).
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] das:leases:expiry, KEYS[4] das:requests:pending,
-- KEYS[5] usage list, KEYS[6] pending_replays set, KEYS[7] das:requests:index,
-- KEYS[8] das:requests:finished, KEYS[9] das:requests:inbox_purge,
-- KEYS[10] das:requests:by_status:Pending, KEYS[11] das:requests:by_status:Running,
-- KEYS[12] das:requests:by_status:{target}
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] epoch, ARGV[4] status (Completed|Failed),
-- ARGV[5] Final_Result JSON ('' for none), ARGV[6] error ('' for none),
-- ARGV[7] JSON array of UsageRecords (summarize and/or planning; '' for none),
-- ARGV[8] optional budget view JSON spliced into the Terminal_Event ('' or absent: none)
-- Completed requires status Running and a non-empty Final_Result. A Replay accepted
-- but not yet applied by the owner (pending_replays non-empty) blocks finalization.
-- Returns {'OK', finished_at_ms, created_at_ms} | {'LEASE_LOST'} | {'REQUEST_TERMINAL'}
--   | {'INVALID_STATE', status} | {'REPLAY_PENDING'}.
local now, ms = server_time()
if not lease_ok(KEYS[2], ARGV[2], ARGV[3], now) then
  return { 'LEASE_LOST' }
end
local status = redis.call('HGET', KEYS[1], 'status')
if is_terminal(status) then
  return { 'REQUEST_TERMINAL' }
end
local target = ARGV[4]
if target ~= 'Completed' and target ~= 'Failed' then
  return { 'INVALID_STATE', status or '' }
end
if target == 'Completed' and (status ~= 'Running' or ARGV[5] == '') then
  return { 'INVALID_STATE', status or '' }
end
if redis.call('SCARD', KEYS[6]) > 0 then
  return { 'REPLAY_PENDING' }
end
-- Decode every input before the first write (no rollback on runtime errors).
check_usage(ARGV[7])
local budget_json = ARGV[8] or ''
check_budget_view(budget_json)
local rid = ARGV[1]
local created = created_ms(KEYS[1], KEYS[7], rid)
if ARGV[5] ~= '' then
  redis.call('HSET', KEYS[1], 'final_result', ARGV[5])
end
if ARGV[6] ~= '' then
  redis.call('HSET', KEYS[1], 'error', ARGV[6])
end
append_usage(KEYS[5], KEYS[1], ARGV[7])
redis.call('HSET', KEYS[1], 'status', target)
client_active_remove(KEYS[1], rid)  -- Phase 4 Req 3.3
mark_finished(KEYS[1], KEYS[8], KEYS[9], rid, ms)
local from_key = KEYS[11]
if status == 'Pending' then from_key = KEYS[10] end
index_move(rid, from_key, KEYS[12], created, KEYS[1])  -- and the tenant's twins (Phase 5 Req 3.5)
usage_bump(KEYS[1], 'finished/' .. target, 1)  -- Phase 5 Req 6.1
redis.call('HDEL', KEYS[2], 'owner', 'expires_at')
redis.call('ZREM', KEYS[3], rid)
redis.call('ZREM', KEYS[4], rid)
local err = nil
if ARGV[6] ~= '' then err = ARGV[6] end
terminal_event(KEYS[1], rid, target, err, budget_json, ms)
return { 'OK', tostring(ms), tostring(created) }
