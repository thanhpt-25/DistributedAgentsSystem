-- record_inbox: append an event to a Request's inbox stream, only if the Request exists
-- (avoids orphan streams for unknown Request_IDs).
-- KEYS[1] request hash, KEYS[2] inbox stream
-- ARGV[1] kind, ARGV[2] data JSON, ARGV[3] optional meta JSON (provider/model of a
-- Job_Result taken from the Message headers, Req 9.2); written as the entry's meta field.
-- Returns {'OK', entry_id} | {'NOT_FOUND'}.
if redis.call('EXISTS', KEYS[1]) == 0 then
  return { 'NOT_FOUND' }
end
local id
if ARGV[3] and ARGV[3] ~= '' then
  id = redis.call('XADD', KEYS[2], '*', 'kind', ARGV[1], 'data', ARGV[2], 'meta', ARGV[3])
else
  id = redis.call('XADD', KEYS[2], '*', 'kind', ARGV[1], 'data', ARGV[2])
end
return { 'OK', id }
