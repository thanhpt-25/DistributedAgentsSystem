-- reopen_request: the Lease owner resumes a Request reopened by a Replay (design D7):
-- a Pending Request that already has a Job_Plan goes Pending -> Running without
-- planning again (Req 13.2, 13.4). It appends no Event: replay_dead_letter already
-- wrote request.reopened when it reopened the Request (Phase 4 Req 6.4).
-- KEYS[1] request hash, KEYS[2] lease hash, KEYS[3] transitions list,
-- KEYS[4] das:requests:pending, KEYS[5] das:requests:index,
-- KEYS[6] das:requests:by_status:Pending, KEYS[7] das:requests:by_status:Running
-- ARGV[1] request_id, ARGV[2] manager_id, ARGV[3] epoch
-- Returns {'OK', at} | {'LEASE_LOST'} | {'INVALID_STATE', status}.
local now, ms = server_time()
if not lease_ok(KEYS[2], ARGV[2], ARGV[3], now) then
  return { 'LEASE_LOST' }
end
local v = redis.call('HMGET', KEYS[1], 'status', 'plan')
local status = v[1]
if status ~= 'Pending' or not v[2] or v[2] == '' then
  return { 'INVALID_STATE', status or '' }
end
local rid = ARGV[1]
local at = iso_ms(ms)
local created = created_ms(KEYS[1], KEYS[5], rid)
redis.call('HSET', KEYS[1], 'status', 'Running')
redis.call('RPUSH', KEYS[3], cjson.encode({
  scope = 'request', from_status = 'Pending', to_status = 'Running', at = at,
}))
redis.call('ZREM', KEYS[4], rid)
index_move(rid, KEYS[6], KEYS[7], created, KEYS[1])
return { 'OK', at }
