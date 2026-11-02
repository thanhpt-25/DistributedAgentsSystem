-- Shared helpers, prepended to every State_Store script by das/state/scripts.py.
-- All time comes from Redis TIME so lease expiry and transition timestamps are
-- independent of client clocks.

local function server_time()
  local t = redis.call('TIME')
  local sec = tonumber(t[1])
  local usec = tonumber(t[2])
  local now = sec + usec / 1000000
  local ms = sec * 1000 + math.floor(usec / 1000)
  return now, ms
end

local function fmt_seconds(x)
  return string.format('%.6f', x)
end

-- UTC ISO 8601 with milliseconds from epoch milliseconds (civil-from-days).
local function iso_ms(ms)
  local secs = math.floor(ms / 1000)
  local msr = ms - secs * 1000
  local days = math.floor(secs / 86400)
  local rem = secs - days * 86400
  local hh = math.floor(rem / 3600)
  local mi = math.floor((rem - hh * 3600) / 60)
  local ss = rem - hh * 3600 - mi * 60
  local z = days + 719468
  local era = math.floor(z / 146097)
  local doe = z - era * 146097
  local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524) - math.floor(doe / 146096)) / 365)
  local y = yoe + era * 400
  local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
  local mp = math.floor((5 * doy + 2) / 153)
  local d = doy - math.floor((153 * mp + 2) / 5) + 1
  local m
  if mp < 10 then m = mp + 3 else m = mp - 9 end
  if m <= 2 then y = y + 1 end
  return string.format('%04d-%02d-%02dT%02d:%02d:%02d.%03dZ', y, m, d, hh, mi, ss, msr)
end

local TERMINAL_REQUEST = { Completed = true, Failed = true, Cancelled = true }

local function is_terminal(status)
  return status and TERMINAL_REQUEST[status] == true
end

-- True when (owner, epoch) holds the unexpired lease stored at lease_key.
local function lease_ok(lease_key, owner, epoch, now)
  local v = redis.call('HMGET', lease_key, 'owner', 'epoch', 'expires_at')
  if not v[1] or v[1] ~= owner then return false end
  if not v[2] or tonumber(v[2]) ~= tonumber(epoch) then return false end
  if not v[3] or tonumber(v[3]) <= now then return false end
  return true
end

-- Move rid between Request index zsets (Phase 2, Req 15.11): ZREM from from_key
-- (skipped when nil/'' or equal to to_key), then ZADD to_key with score (created_at ms).
-- Both key names must come from KEYS.
local function index_move(rid, from_key, to_key, score)
  if from_key and from_key ~= '' and from_key ~= to_key then
    redis.call('ZREM', from_key, rid)
  end
  if to_key and to_key ~= '' then
    redis.call('ZADD', to_key, score, rid)
  end
end

-- Stamp a terminal Request (Req 14.2): finished_at = ms on the request hash and
-- rid added to the finished and inbox_purge retention zsets, both scored by ms.
local function mark_finished(req_key, finished_key, inbox_purge_key, rid, ms)
  redis.call('HSET', req_key, 'finished_at', tostring(ms))
  redis.call('ZADD', finished_key, ms, rid)
  redis.call('ZADD', inbox_purge_key, ms, rid)
end

-- Append UsageRecords to the per-Request usage list and add them to the three
-- running totals of the request hash (Req 9). records_json is a JSON array whose
-- elements are UsageRecord objects or UsageRecord JSON strings (stored verbatim).
-- Returns the number of records appended.
local function append_usage(usage_key, req_key, records_json)
  if not records_json or records_json == '' then return 0 end
  local records = cjson.decode(records_json)
  local n, sum_in, sum_out, sum_total = 0, 0, 0, 0
  for _, item in ipairs(records) do
    local raw, rec
    if type(item) == 'string' then
      raw = item
      rec = cjson.decode(item)
    else
      raw = cjson.encode(item)
      rec = item
    end
    local u = rec.usage or {}
    sum_in = sum_in + (tonumber(u.input_tokens) or 0)
    sum_out = sum_out + (tonumber(u.output_tokens) or 0)
    sum_total = sum_total + (tonumber(u.total_tokens) or 0)
    redis.call('RPUSH', usage_key, raw)
    n = n + 1
  end
  if n > 0 then
    redis.call('HINCRBY', req_key, 'usage_input', sum_in)
    redis.call('HINCRBY', req_key, 'usage_output', sum_out)
    redis.call('HINCRBY', req_key, 'usage_total', sum_total)
  end
  return n
end

-- Decode and validate a UsageRecord JSON array before any write (Redis does not
-- roll back a script on a runtime error). Raises on malformed input; returns the
-- number of records. '' or nil means no records.
local function check_usage(records_json)
  if not records_json or records_json == '' then return 0 end
  local records = cjson.decode(records_json)
  if type(records) ~= 'table' then
    error('usage: expected a JSON array of UsageRecords')
  end
  local n = 0
  for _, item in ipairs(records) do
    local rec = item
    if type(item) == 'string' then rec = cjson.decode(item) end
    if type(rec) ~= 'table' then
      error('usage: UsageRecord must be an object')
    end
    n = n + 1
  end
  return n
end

-- created_at of a Request in epoch ms: the requests:index score when present,
-- otherwise parsed exactly from the stored created_at seconds string (v1 Requests).
local function created_ms(req_key, index_key, rid)
  local score = redis.call('ZSCORE', index_key, rid)
  if score then return math.floor(tonumber(score)) end
  local c = redis.call('HGET', req_key, 'created_at')
  if not c then return 0 end
  local s, f = string.match(c, '^(%d+)%.?(%d*)$')
  if not s then return math.floor((tonumber(c) or 0) * 1000) end
  return tonumber(s) * 1000 + tonumber(string.sub(f .. '000', 1, 3))
end

-- Revert a Dead_Letter_Record Replayed -> Pending (Req 13.15) with compare-and-set:
-- only when the record exists, is Replayed, and belongs to rid (when it names one).
-- Moves dlid from by_status:Replayed and terminal to by_status:Pending (scored by
-- dead_lettered_at ms). Returns 1 when reverted, 0 when left alone.
local function dlq_revert(record_key, pending_idx, replayed_idx, terminal_idx, dlid, rid, ms)
  local v = redis.call('HMGET', record_key, 'status', 'request_id', 'dead_lettered_at')
  if v[1] ~= 'Replayed' then return 0 end
  if v[2] and v[2] ~= '' and v[2] ~= rid then return 0 end
  redis.call('HSET', record_key, 'status', 'Pending', 'status_changed_at', tostring(ms))
  redis.call('ZREM', replayed_idx, dlid)
  redis.call('ZREM', terminal_idx, dlid)
  redis.call('ZADD', pending_idx, tonumber(v[3]) or ms, dlid)
  return 1
end

-- Phase 4 key names, derived from the request key das:req:{rid} (KEYS[1] of the
-- per-Request scripts) so callers' KEYS lists stay unchanged. They must match
-- das/state/keys.py. Building key names inside a script that are not in KEYS is
-- only safe because DAS runs on a single Redis primary (Phase 1 OD 4.1); it would
-- break under Redis Cluster.
local function key_prefix(req_key)
  local i = string.find(req_key, ':req:', 1, true)
  if not i then error('key_prefix: not a request key: ' .. tostring(req_key)) end
  return string.sub(req_key, 1, i - 1)
end

local function events_key(req_key)
  return req_key .. ':events'
end

local function webhook_key(req_key)
  return req_key .. ':webhook'
end

local function outbox_key(req_key)
  return key_prefix(req_key) .. ':webhooks:due'
end

local function active_key(req_key, fp)
  return key_prefix(req_key) .. ':client:' .. fp .. ':active'
end

local function idem_key(req_key, fp, h)
  return key_prefix(req_key) .. ':client:' .. fp .. ':idem:' .. h
end

-- Doubles as text that tonumber() and Python float() read back exactly.
local function fmt_double(x)
  return string.format('%.17g', x)
end

-- Append one Event to the Request's Event_Stream (Phase 4 Req 6.1, 6.5), capped at
-- the stored events_max_len (1000 when absent). data_json is stored verbatim.
-- Returns the new stream ID.
local function append_event(req_key, event_type, data_json)
  local n = tonumber(redis.call('HGET', req_key, 'events_max_len')) or 1000
  return redis.call('XADD', events_key(req_key), 'MAXLEN', n, '*', 'type', event_type, 'data', data_json)
end

-- Active_Set membership of a Request (Phase 4 Req 3.3-3.7): add or remove rid in
-- das:client:{client_fp}:active. No-op for a Request without client_fp (created
-- before Phase 4 or through create_request), so those touch no Active_Set.
local function client_active_add(req_key, rid)
  local fp = redis.call('HGET', req_key, 'client_fp')
  if fp and fp ~= '' then redis.call('SADD', active_key(req_key, fp), rid) end
end

local function client_active_remove(req_key, rid)
  local fp = redis.call('HGET', req_key, 'client_fp')
  if fp and fp ~= '' then redis.call('SREM', active_key(req_key, fp), rid) end
end

-- Write one JobRecord JSON and, when its state changed, append a transition entry and
-- the matching job.transition Event (Phase 4 Req 6.1, 6.3), built from the same entry.
-- Record JSON is stored verbatim; it is only decoded to read state/attempt/worker_id.
-- tag (optional Dead_Letter_ID) is written as dead_letter_id on the entry (Req 13.4).
-- req_key is the request hash (KEYS[1] of every caller); it locates the Event_Stream.
local function put_job(jobs_key, transitions_key, job_id, new_json, at_iso, tag, req_key)
  local old_json = redis.call('HGET', jobs_key, job_id)
  local new_rec = cjson.decode(new_json)
  local from_state = cjson.null
  if old_json then
    from_state = cjson.decode(old_json).state
  end
  redis.call('HSET', jobs_key, job_id, new_json)
  if from_state ~= new_rec.state then
    local entry = {
      job_id = job_id,
      from_state = from_state,
      to_state = new_rec.state,
      attempt = new_rec.attempt or 0,
      worker_id = new_rec.worker_id or cjson.null,
      at = at_iso,
    }
    if tag and tag ~= '' and tag ~= cjson.null then
      entry.dead_letter_id = tag
    end
    redis.call('RPUSH', transitions_key, cjson.encode(entry))
    -- Same shape as das.client.events.transition_event_data(entry).
    append_event(req_key, 'job.transition', cjson.encode({
      job_id = entry.job_id,
      ['from'] = entry.from_state,
      to = entry.to_state,
      attempt = entry.attempt,
      worker_id = entry.worker_id,
      at = entry.at,
    }))
    return 1
  end
  return 0
end

-- Decode and check an optional budget view JSON object before any write ('' or nil
-- means none). Raises on malformed input. cjson decodes arrays and objects to tables
-- alike, so a JSON array is rejected by its first non-space character.
local function check_budget_view(budget_json)
  if not budget_json or budget_json == '' then return end
  if type(cjson.decode(budget_json)) ~= 'table' or not string.match(budget_json, '^%s*{') then
    error('budget view must be a JSON object')
  end
end

local function int_text(v)
  if v and string.match(v, '^%-?%d+$') then return v end
  return '0'
end

-- Append the Terminal_Event of a Request that just reached status (Completed, Failed
-- or Cancelled) and, when it has a webhook target, (re)start its Webhook_Delivery
-- (Phase 4 Req 6.1, 6.4, 8.2). Data is built as text, equal to
-- das.client.events.terminal_event_data: {status, error|null, usage totals of the
-- request hash}, plus "budget":<budget_json> (verbatim) when budget_json is not ''.
-- The delivery hash is replaced (DEL, then HSET) and rid is put on the outbox scored
-- by ms in Redis seconds. Returns the Terminal_Event's stream ID.
local function terminal_event(req_key, rid, status, error_or_nil, budget_json, ms)
  local v = redis.call('HMGET', req_key, 'usage_input', 'usage_output', 'usage_total',
    'webhook_url', 'webhook_secret_name')
  local err = 'null'
  if error_or_nil and error_or_nil ~= '' then err = cjson.encode(error_or_nil) end
  local data = '{"status":' .. cjson.encode(status)
    .. ',"error":' .. err
    .. ',"usage":{"input_tokens":' .. int_text(v[1])
    .. ',"output_tokens":' .. int_text(v[2])
    .. ',"total_tokens":' .. int_text(v[3]) .. '}'
  if budget_json and budget_json ~= '' then
    data = data .. ',"budget":' .. budget_json
  end
  data = data .. '}'
  local event_type = 'request.' .. string.lower(status)
  local id = append_event(req_key, event_type, data)
  if v[4] and v[4] ~= '' then
    local wk = webhook_key(req_key)
    redis.call('DEL', wk)
    redis.call('HSET', wk, 'url', v[4], 'secret_name', v[5] or '', 'state', 'pending',
      'attempts', '0', 'last_status', '', 'claim', '0', 'event_id', id,
      'type', event_type, 'data', data)
    redis.call('ZADD', outbox_key(req_key), fmt_double(ms / 1000), rid)
  end
  return id
end
