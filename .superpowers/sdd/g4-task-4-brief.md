### Task 4: Event_Stream and webhook enqueue in state-changing scripts (rest of spec task 4.5; spec task 4.7)

**Files:**
- Modify: `das/state/lua/_common.lua` (`put_job`, `terminal_event`), `commit_plan.lua`, `apply_transition.lua`, `reopen_request.lua`, `finalize.lua`, `cancel_request.lua`, `purge_request.lua`, `das/state/store.py`
- Modify tests that call scripts with raw ARGV: `tests/unit/test_state_atomic.py` (`commit_args`) and `tests/unit/test_state_phase2_writes.py` (the direct `commit_plan` and `cancel_request` calls). Change only their argument lists, to the new layout.
- Test: `tests/unit/test_state_phase4_events.py`, `tests/property/test_event_stream.py`

**Interfaces:**
- Consumes: Tasks 1–3; `das.client.events` builders (`planned_event_data`, `transition_event_data`, `terminal_event_data`); `das.client.budget.budget_view`.
- Produces:
  - `put_job(jobs_key, transitions_key, job_id, new_json, at_iso, tag, req_key)`: when it writes a transition entry, it also appends `job.transition` with data `{job_id, from, to, attempt, worker_id, at}`. That is the `transition_event_data` shape built from the same entry: `from` is JSON null for a new Job, and `worker_id` is null when absent. All three callers pass `KEYS[1]`.
  - `commit_plan.lua`: new ARGV[6] = planned event JSON (`''` for none), and job pairs move to ARGV[7..]. It appends `request.planned` before the first `put_job`. `StateStore.commit_plan` passes `json.dumps(planned_event_data(plan))`, so callers don't change.
  - `reopen_request.lua`: appends `request.reopened` with data `{"status":"Running"}` (equals `reopened_event_data()`).
  - `terminal_event(req_key, rid, status, error_or_nil, budget_json, ms)` in `_common.lua`:
    - Builds the data as text: `{"status":…,"error":…|null,"usage":{"input_tokens":n,"output_tokens":n,"total_tokens":n}}` from the request hash totals. When `budget_json` is not `''`, it splices in `,"budget":<budget_json>`.
    - Appends the Terminal_Event with type `request.completed`, `request.failed` or `request.cancelled`.
    - When the request hash has `webhook_url`: `DEL` and then `HSET` the webhook hash with `url`, `secret_name`, `state=pending`, `attempts=0`, `last_status=''`, `claim=0`, `event_id`, `type` and `data`, then `ZADD outbox ms/1000 rid` (score in Redis seconds, as `fmt_double`) (Req 8.2).
  - `finalize.lua`: new optional ARGV[8] = budget view JSON. It calls `terminal_event` after its existing writes, using the error it stored (`ARGV[6]` or none).
  - `cancel_request.lua`: new ARGV[3] = budget view JSON, and job triples move to ARGV[4..]. Its CONFLICT and arity checks are adjusted to the new layout. It calls `terminal_event` with no error after its existing writes.
  - `StateStore.finalize(..., budget_view: Mapping[str, Any] | None = None)` and `StateStore.cancel_request(rid, max_retries=16, budget_view: Mapping[str, Any] | None = None)` pass `json.dumps(budget_view)` or `''`. Existing calls keep working. `fail_request` gains the same optional parameter and forwards it.
  - `purge_request.lua` already deletes the events and webhook keys (Task 1). Add `ZREM outbox rid`.
  - `StateStore.get_webhook(rid) -> WebhookState | None`. `WebhookState` is a frozen dataclass with `state: str`, `attempts: int`, `last_status: str | None` (`''` becomes `None`). Group 7 uses it for the query view. Export it from `das.state`.

- [ ] **Step 1: Write the failing unit tests** (`tests/unit/test_state_phase4_events.py`). Read events with `client.xrange(keys.events(rid))` and decode `data` with `json.loads`.
  - Commit plan: entries are `request.created`, then `request.planned` (data equals `planned_event_data(plan)`), then one `job.transition` per Job, from null to `Waiting`. Each equals `transition_event_data(entry)` for the matching entry of `load_snapshot(rid, include_transitions=True)`.
  - `apply_transition` that moves two Jobs appends two `job.transition` events, in transition-log order. A batch with no state change appends none.
  - A batch rejected with `LEASE_LOST` appends nothing.
  - Finalize Completed:
    - The last event is `request.completed`, with data equal to `terminal_event_data(COMPLETED, None, usage_records, None, None)`, where `usage_records` comes from `load_usage(rid)`.
    - With `budget_view=V`, `data["budget"] == V`.
  - Finalize Failed with error `"budget_exceeded"` gives `request.failed` with `error == "budget_exceeded"`.
  - Cancel gives `request.cancelled`, preceded by one `job.transition` per Job it cancelled.
  - Replay of a Failed Request, then `reopen_request`: the stream continues `… request.failed, request.reopened`. A second finalize adds a second Terminal_Event at the end.
  - No event `data` contains the prompt or any instruction text (use distinctive strings).
  - Webhook:
    - A Request created with a webhook target and then finalized has `get_webhook(rid) == WebhookState("pending", 0, None)`, its rid in `WEBHOOKS_DUE` with score ≈ the Redis time of the finalize, and a webhook hash whose `event_id` is the Terminal_Event's stream id.
    - Without a target: no webhook hash and no outbox entry.
    - Finalizing a reopened Request resets the delivery.
  - Stream trimming: with `events_max_len=100`, 150 events leave `XLEN == 100`.
  - Existing direct-ARGV tests still pass after their argument lists are updated.

- [ ] **Step 2: Run, confirm the tests fail.** **Step 3: Implement** (decode and validate the budget JSON before the first write, then update each script's header comment).

- [ ] **Step 4: Write the property test** `tests/property/test_event_stream.py`, **Property 6: Event_Stream mirrors persisted changes** (**Validates: Requirements 6.1, 6.2, 6.3, 6.4**):
  - Draw a plan with `tests.strategies.job_plans(max_jobs=5)` and a sequence of operations: transition batches that move random Jobs to random states, an optional stale-lease batch, then finalize or cancel, and optionally a Replay of a Failed Request followed by `reopen_request` and a second finalize.
  - Assert the event types follow the sequence:
    - one `request.created`, then one `request.planned` per commit;
    - one `job.transition` per transition-log entry, in the same order and with equal data;
    - one `request.reopened` per reopen;
    - exactly one Terminal_Event after each terminal write, and it is the last event while the Request is terminal.
  - Assert operations answered `LEASE_LOST` or `REQUEST_TERMINAL` added nothing.
  - Assert no `data` contains the prompt or an instruction.

- [ ] **Step 5: Run** the new tests, then the full state and manager suites: `uv run pytest tests/unit -k "state or manager or gateway" -q` and `uv run pytest tests/property -q`.

- [ ] **Step 6: Lint, type-check, contracts.** **Step 7: Tick** 4.5 and 4.7.

---


## Shared context for this group (from the plan header)

- Global Constraints and "How to read this plan": see the top of `docs/superpowers/plans/2026-10-30-das-client-api-state-store.md` (read only the header up to the first `### Task`).
- Reference: `.kiro/specs/das-client-api/design.md` Components §3 (key table, `_common.lua` helpers, script changes) and the Correctness Properties section.
