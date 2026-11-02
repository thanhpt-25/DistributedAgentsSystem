# DAS Client API (Phase 4) State_Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spec task group 4 in `.kiro/specs/das-client-api/tasks.md`: the Redis side of Phase 4. That is the rate-limit bucket, idempotent and quota-checked create, the Active_Set, the per-Request Event_Stream, and the Webhook_Delivery outbox. Each is written by Lua in the same atomic script as the state change it belongs to.

**Architecture:** Every new key is derived from the request key `das:req:{rid}` (KEYS[1] of the per-Request scripts) by helpers in `_common.lua`, so callers' KEYS lists stay as they are. Python builds event payloads with `das.client.events` where it already has the data, and Lua builds the rest. Tests check the Lua output against the pure-core builders. Spec: `requirements.md` Req 2–4, 6, 8; `design.md` Components §3 (key table, helpers, script changes) and Correctness Properties 1, 2, 3, 6, 7.

**Tech Stack:** Python 3.12, redis-py 8.1.0 async client, Lua 5.1 (Redis scripting), fakeredis[lua] 2.40.0, Hypothesis 6.170.8, pytest 9.1.1, uv.

**How to read this plan:** Each task gives exact interfaces, script contracts (KEYS, ARGV, replies) and the test cases to write as input → expected output. Implementers write the code test-first. Earlier groups embedded complete code because they were small. For this group, `design.md` §3 is the reference wherever a task says so.

## Global Constraints

- Python `>=3.12,<3.13`; run everything through `uv run`. No new dependencies.
- Redis is the source of truth. Every new write happens inside a Lua script, in the same script as the state change it belongs to. Time inside scripts comes from Redis `TIME` (the `server_time()` helper), never from the caller.
- Redis does not roll back a script on a runtime error. Decode and validate every input before the first write, as existing scripts do (`check_usage`, `cjson.decode` up front).
- Single Redis primary (Phase 1 OD 4.1). Deriving key names inside Lua from KEYS[1] is allowed only because of this. A comment in `_common.lua` must say so.
- Existing behaviour is unchanged when a Request has no Phase 4 fields (no `client_fp`, no budget, no webhook). That covers Requests created before the upgrade and every existing caller of `StateStore.create_request(prompt)`.
- `das.manager` and `das.worker` keep the import-linter contract. This group changes only `das/state/` and tests.
- Property tests: in `tests/property/`. First line `# Feature: das-client-api, Property {n}: {title}`, docstring ending with `**Validates: Requirements …**`, default Hypothesis profile (≥ 100 examples). Run the real `das/state/lua/*.lua` scripts on a fresh `fakeredis.FakeServer` per example, as `tests/property/test_state_cancel.py` does. Drive Redis time with `tests/fixtures/redis_time.FakeRedisTime`.
- Lint: `uv run ruff check` (E, F; line length 120). Types: `uv run mypy`. Contracts: `uv run lint-imports`.
- Docstrings and script header comments cite requirements as `Phase 4 Req X.Y`.
- Not a git repository. Each task ends by ticking its checkboxes in `.kiro/specs/das-client-api/tasks.md` instead of committing.

---

### Task 1: Keys, Lua helpers and the rate-limit script (spec tasks 4.1, 4.2)

**Files:**
- Modify: `das/state/keys.py`, `das/state/lua/_common.lua`, `das/state/scripts.py`, `das/state/store.py`, `das/state/__init__.py`
- Create: `das/state/lua/rate_check.lua`
- Test: `tests/unit/test_state_phase4_keys.py`, `tests/property/test_rate_limit_bucket.py`

**Interfaces:**
- Consumes: `das.client.bucket.BucketState`, `take`, `TakeResult`.
- Produces:
  - `das/state/keys.py`:
    - `client_bucket(fp)` → `das:client:{fp}:bucket`
    - `client_active(fp)` → `das:client:{fp}:active`
    - `client_idem(fp, h)` → `das:client:{fp}:idem:{h}`
    - `client_streams(fp)` → `das:client:{fp}:streams` (used in group 7)
    - `events(rid)` → `das:req:{rid}:events`
    - `webhook(rid)` → `das:req:{rid}:webhook`
    - `WEBHOOKS_DUE = "das:webhooks:due"`
    - new request-hash field constants `F_CLIENT_FP`, `F_BUDGET`, `F_WEBHOOK_URL`, `F_WEBHOOK_SECRET_NAME`, `F_EVENTS_MAX_LEN`, all added to `RESERVED_REQUEST_FIELDS`
    - `request_keys(rid)` gains `events(rid)` and `webhook(rid)` at the end, so it returns 11 keys
    - the key table in the module docstring is updated
  - `_common.lua` helpers used by Tasks 2–5. Each takes the request key; the prefix is the text before `:req:`.
    - `key_prefix(req_key)`, `events_key(req_key)`, `webhook_key(req_key)`, `outbox_key(req_key)`
    - `active_key(req_key, fp)`, `idem_key(req_key, fp, h)`
    - `fmt_double(x)`: `string.format('%.17g', x)`
  - `StateStore.rate_check(fingerprint: str, rate: float, burst: int) -> float | None`: `None` when the call is allowed, otherwise `retry_after` seconds as a float.
  - `DEFAULT_EVENTS_MAX_LEN = 1000` in `das/state/store.py`, exported from `das.state`.

**`rate_check.lua` contract:**
- KEYS[1] is the bucket hash. ARGV[1] is `rate` and ARGV[2] is `burst`, both as `%.17g` / int text.
- `rate == 0`: return `{'OK'}` without reading or writing anything.
- Otherwise, with `now = sec + usec / 1000000` taken from `TIME` (the same expression as `server_time()`), follow `das.client.bucket.take` exactly:
  - A missing hash is a full bucket.
  - `elapsed = max(0, now - updated)`, `tokens = min(burst, tokens + rate * elapsed)`, `updated = max(now, updated)`.
  - Allowed: write `tokens - 1`. Denied: write the refilled tokens.
  - In both cases store `tokens` and `updated` with `fmt_double`, and `PEXPIRE` the key `ceil(burst / rate * 1000) + 60000` ms. A key that expires is equivalent to a full bucket, because the TTL is longer than a full refill takes.
- Reply `{'OK'}` or `{'LIMITED', fmt_double(retry_after)}`, where `retry_after = (1 - tokens) / rate`.

- [ ] **Step 1: Write the failing unit tests** (`tests/unit/test_state_phase4_keys.py`)

  Key helpers: each new builder returns the exact string above, and `request_keys("r")[-2:] == ["das:req:r:events", "das:req:r:webhook"]`.

  Lua helpers: register `script_source`-style source made of `_common.lua` plus a one-line return, as `tests/unit/test_state_store.py:394` does. Check:
  - `key_prefix("das:req:r1") == "das"`
  - `events_key("das:req:r1") == "das:req:r1:events"`
  - `outbox_key("das:req:r1") == "das:webhooks:due"`
  - `active_key("das:req:r1", "cbabecbd") == "das:client:cbabecbd:active"`
  - `fmt_double(0.1 + 0.2) == "0.30000000000000004"`

  `rate_check` (with `fake_redis_time`), with rate 1 and burst 2 at t = 1000.000000:
  - three calls in a row return `None`, `None`, then about 1.0;
  - after `advance(0.5)` the next call returns about 0.5;
  - after a further `advance(0.5)` it returns `None`.
  - `rate == 0` returns `None` 1000 times, and the bucket key does not exist afterwards.
  - The key's PTTL is positive and at most `ceil(2 / 1 * 1000) + 60000`.

- [ ] **Step 2: Run, confirm the tests fail** (missing attributes, `KeyError: unknown State_Store script: rate_check`).

- [ ] **Step 3: Implement** the keys, the helpers (with the single-primary comment), `rate_check.lua` (header comment in the existing style: purpose, `Phase 4 Req 2.1-2.4`, KEYS, ARGV, replies), `"rate_check"` in `SCRIPT_NAMES`, and `StateStore.rate_check`.

- [ ] **Step 4: Write the property test** `tests/property/test_rate_limit_bucket.py`, **Property 1: Token bucket bound** (**Validates: Requirements 2.3, 2.4**):
  - Draw `rate` from `st.floats(0.001, 1000)` or 0, `burst` from 1..50, and 1..40 non-decreasing call times. Draw the times as integer microseconds starting at 1_700_000_000_000_000, so Redis `TIME` and the model see identical values.
  - For each call, set Redis time to that microsecond, run `StateStore.rate_check`, and run `bucket.take` on the model with `now = sec + usec / 1e6` computed the same way.
  - Assert the allow/deny decisions are equal at every step.
  - Assert that for every window `[t_i, t_j]` the number of admitted calls is at most `floor(burst + rate * (t_j - t_i))` (skip this check when `rate == 0`).
  - Two `StateStore` instances on one `FakeServer` represent two Gateway replicas. Alternate calls between them; the decisions must equal a single model (Req 2.4).

- [ ] **Step 5: Run** `uv run pytest tests/unit/test_state_phase4_keys.py tests/property/test_rate_limit_bucket.py -q -W error`, then the state suites `uv run pytest tests/unit -k state -q` and `tests/property/test_retention_safety.py`. `request_keys` grew, and retention deletes these keys too. Task 3 changes `purge_request.lua`; until then, `purge_request.lua` must keep working. Its KEYS indexes shift by two, so update the `DEL` list and the `ZREM` loop in this task.

- [ ] **Step 6: Lint, type-check, contracts**: `uv run ruff check && uv run mypy && uv run lint-imports`.

- [ ] **Step 7: Tick** 4.1 and 4.2 in `.kiro/specs/das-client-api/tasks.md`.

---

### Task 2: Idempotent, quota-checked create (spec tasks 4.3, 4.4)

**Files:**
- Modify: `das/state/lua/create_request.lua`, `das/state/store.py`, `das/state/__init__.py`, `das/state/lua/_common.lua` (only if a helper is needed)
- Test: `tests/unit/test_state_phase4_create.py`, `tests/property/test_idempotent_create.py`

**Interfaces:**
- Consumes: Task 1 keys and helpers; `das.client.budget.Budget.to_json_obj`; `das.client.events.created_event_data`.
- Produces (used by the Gateway in group 7):

```python
@dataclass(frozen=True, slots=True)
class ClientRequestSpec:
    """Phase 4 fields of a new Request (Req 3.1, 4.2, 5.4, 6.5, 8.1)."""
    fingerprint: str
    budget: Budget | None = None
    webhook_url: str | None = None
    webhook_secret_name: str | None = None
    idempotency_key_hash: str | None = None   # das.client.idempotency.idem_key_hash
    body_hash: str | None = None              # required when idempotency_key_hash is set
    idempotency_ttl: int = 86_400
    max_active_requests: int = 0              # 0 = no quota
    events_max_len: int = DEFAULT_EVENTS_MAX_LEN

CreateOutcome = Literal["created", "replayed", "idempotency_conflict", "quota_exceeded"]

@dataclass(frozen=True, slots=True)
class CreateResult:
    outcome: CreateOutcome
    request_id: str | None = None   # created / replayed
    limit: int | None = None        # quota_exceeded

async def submit_request(self, prompt: str, client: ClientRequestSpec,
                         trace_context: TraceContext | None = None) -> CreateResult
```

  - `create_request(prompt, trace_context=None) -> str` keeps its signature and behaviour, and writes `request.created` too (Req 6.1).
  - Both methods share one private helper that loops over Request_ID allocation as today.
  - `submit_request` raises `ValueError` when `idempotency_key_hash` is set without `body_hash`.
  - Export `ClientRequestSpec`, `CreateResult` and `DEFAULT_EVENTS_MAX_LEN` from `das.state`.

**`create_request.lua` contract:**
- KEYS are unchanged (4).
- ARGV[1..3] are unchanged. Optional ARGV[4..12] are `client_fp`, `budget_json`, `webhook_url`, `webhook_secret_name`, `idem_hash`, `body_hash`, `idem_ttl` (seconds), `max_active`, `events_max_len`. Each defaults to `''` (or `'0'`), meaning "not set", so a 3-argument call behaves exactly as before plus the event.
- Order:
  1. When `client_fp` and `idem_hash` are set and `idem_key(KEYS[1], fp, h)` exists: reply `{'REPLAYED', rid}` if its `body_hash` matches, else `{'IDEM_CONFLICT'}`. Nothing is written in either case.
  2. Existing `EXISTS` check on KEYS[1].
  3. When `client_fp` is set and `max_active > 0` and `SCARD active_key >= max_active`: reply `{'QUOTA_EXCEEDED', max_active}`.
  4. Existing writes, plus these fields when set: `client_fp`, `budget`, `webhook_url` with `webhook_secret_name`, and `events_max_len`. Then `SADD active_key rid` (when `client_fp` is set), the idem hash `rid`/`body_hash` with `EXPIRE idem_ttl` (when `idem_hash` is set), and `append_event(KEYS[1], 'request.created', '{"status":"Pending"}')`.
- Reply `{'OK', created}`.
- `append_event` reads `events_max_len` from the request hash (1000 when absent) and runs `XADD events_key MAXLEN <n> * type <type> data <json>`. Task 1 doesn't need it, so add it here.

- [ ] **Step 1: Write the failing unit tests** (`tests/unit/test_state_phase4_create.py`):
  - `create_request("p")` still returns a rid. Its hash has no `client_fp`, and its Event_Stream holds one entry, `type == "request.created"`, with `json.loads(data) == created_event_data()`.
  - `submit_request` with a fingerprint returns `created`. Afterwards `client_fp` and `budget` (equal to `Budget.to_json_obj()` JSON) are stored, the rid is in `client_active(fp)`, and `events_max_len` is stored.
  - Same idempotency key and body hash: the second call returns `replayed` with the same rid. Exactly one request hash exists, and the Active_Set still has 1 member.
  - Same key with a different body hash: `idempotency_conflict`, and nothing new written (compare `client.keys("*")` before and after).
  - The same key value under two fingerprints creates two Requests (Req 4.7).
  - The idem key's TTL is between 1 and `idempotency_ttl`.
  - Quota: `max_active_requests=2`. Two creates succeed; the third returns `quota_exceeded` with `limit == 2`. A replay of the first (same key and body) still returns `replayed` (Req 4.5).
  - `max_active_requests=0` never limits.
  - A webhook target is stored only when both fields are given.
  - `ValueError` when `idempotency_key_hash` is set without `body_hash`.

- [ ] **Step 2: Run, confirm the tests fail.**

- [ ] **Step 3: Implement** the script, `append_event`, `ClientRequestSpec`, `CreateResult`, `submit_request` and the shared helper. Update the script's header comment.

- [ ] **Step 4: Write the property test** `tests/property/test_idempotent_create.py`, **Property 2: Idempotent create** (**Validates: Requirements 4.2, 4.3, 4.4, 4.6, 4.7**):
  - Draw 2..8 submits. Each has a fingerprint from a pool of 2, the idempotency key `"k"`, and a body hash from a pool of 2.
  - Run them concurrently with `asyncio.gather` on one store.
  - Group by fingerprint and assert:
    - exactly one Request is created per fingerprint that appears;
    - every submit with the creator's body hash returns the creator's rid (`created` or `replayed`);
    - every other submit returns `idempotency_conflict`;
    - the Active_Set of each fingerprint has exactly 1 member.

- [ ] **Step 5: Run** the new tests with `-W error`, then `uv run pytest tests/unit -k "state or gateway" -q` and `tests/property/test_submission_uniqueness.py`. Existing callers of `create_request` must not change.

- [ ] **Step 6: Lint, type-check, contracts.**

- [ ] **Step 7: Tick** 4.3 and 4.4.

---

### Task 3: Active_Set release and re-add (part of spec task 4.5; spec task 4.6)

**Files:**
- Modify: `das/state/lua/_common.lua`, `finalize.lua`, `cancel_request.lua`, `replay_dead_letter.lua`, `purge_request.lua`, `das/state/store.py` (`delete_request`)
- Test: `tests/unit/test_state_phase4_quota.py`, `tests/property/test_active_set.py`

**Interfaces:**
- Consumes: Tasks 1–2.
- Produces:
  - `_common.lua`: `client_active_add(req_key, rid)` and `client_active_remove(req_key, rid)`. Each reads `client_fp` from the request hash and is a no-op without one.
  - Script changes, with no KEYS or ARGV changes in this task:
    - `finalize.lua` and `cancel_request.lua` call `client_active_remove` after setting the terminal status.
    - `replay_dead_letter.lua` calls `client_active_add` inside its `status == 'Failed'` branch (Req 3.4).
    - `purge_request.lua` reads `client_fp` before its `DEL` and removes the rid from the Active_Set.
    - `StateStore.delete_request` also does `SREM client_active(fp) rid` when the Request has a `client_fp` (read it first). Retention and tests must never leave a dangling member.

- [ ] **Step 1: Write the failing unit tests** (`tests/unit/test_state_phase4_quota.py`). Build Requests with `submit_request` and drive them through the existing store methods (see `tests/unit/test_state_phase2_writes.py` for lease and plan helpers).
  - Finalize Completed or Failed removes the rid from the Active_Set; cancel does too.
  - A Replay of a Failed Request (see `tests/property/test_replay_discard_exclusivity.py` `_setup` for a dead letter on a Failed Request) re-adds it, even when the set already holds `max_active` members.
  - `purge_request` of an expired terminal Request leaves no Active_Set member and does not touch other members.
  - `delete_request` removes the member.
  - Requests from `create_request(prompt)` touch no Active_Set.

- [ ] **Step 2: Run, confirm the tests fail.** **Step 3: Implement.**

- [ ] **Step 4: Write the property test** `tests/property/test_active_set.py`, **Property 3: Active_Set equals non-terminal Requests** (**Validates: Requirements 3.1, 3.3, 3.4, 3.5, 3.6, 3.7**):
  - Draw a sequence of 1..20 operations: `create(fp)` (fp from a pool of 2, or `None` for a pre-upgrade Request through `create_request`), `finalize(i, Completed|Failed)`, `cancel(i)`, `replay(i)` (only applies to a Failed Request with a Failed Job; otherwise skipped), and `purge(i)` (with Redis time advanced past the TTL).
  - After every operation, assert that for each fp `SMEMBERS client_active(fp)` equals the set of that fp's Requests whose status is non-terminal and which still exist.

- [ ] **Step 5: Run** the new tests, then the existing retention, replay and cancel suites: `tests/property/test_retention_safety.py`, `tests/property/test_replay_discard_exclusivity.py`, `tests/property/test_state_cancel.py`, `uv run pytest tests/unit -k "state or retention or dlq or replay" -q`.

- [ ] **Step 6: Lint, type-check, contracts.** **Step 7: Tick** 4.6 (4.5 is ticked in Task 4).

---

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

### Task 5: Webhook claim and completion (spec tasks 4.8, 4.9)

**Files:**
- Create: `das/state/lua/claim_webhook.lua`, `das/state/lua/complete_webhook.lua`
- Modify: `das/state/scripts.py`, `das/state/store.py`, `das/state/__init__.py`
- Test: `tests/unit/test_state_phase4_webhook.py`, `tests/property/test_webhook_outbox.py`

**Interfaces:**
- Consumes: Task 4 (deliveries enqueued by `finalize` and `cancel_request`).
- Produces:

```python
@dataclass(frozen=True, slots=True)
class WebhookClaim:
    request_id: str
    claim: int          # pass back to complete_webhook
    attempts: int       # attempts made before this one
    url: str
    secret_name: str
    event_id: str
    event_type: str
    data: dict[str, Any]

WebhookOutcome = Literal["delivered", "retry", "failed"]

async def claim_webhook(self, lease_seconds: float) -> WebhookClaim | None
async def complete_webhook(self, claim: WebhookClaim, outcome: WebhookOutcome,
                           last_status: str, next_due: float | None = None) -> bool
    # False when stale; next_due (Redis seconds) is required for "retry"
```

  Export `WebhookClaim` and `WebhookOutcome` from `das.state`.

**`claim_webhook.lua` contract** (Req 8.3):
- KEYS[1] is the outbox. ARGV[1] is `lease_seconds` and ARGV[2] is the request key prefix `das:req:`.
- `ZRANGEBYSCORE outbox -inf now LIMIT 0 10`. For each rid, the hash is `ARGV[2] .. rid .. ':webhook'`. If it is missing or its `state ~= 'pending'`, `ZREM` the rid and continue. Otherwise:
  - `ZADD outbox now + lease rid`
  - `HINCRBY claim 1`
  - reply `{rid, claim, attempts, url, secret_name, event_id, type, data}`
- Reply `{}` when nothing is due.

**`complete_webhook.lua` contract** (Req 8.5–8.7):
- KEYS[1] is the outbox and KEYS[2] the webhook hash. ARGV[1] is `rid`, ARGV[2] `claim`, ARGV[3] `outcome`, ARGV[4] `last_status`, ARGV[5] `next_due` (`''` unless retry).
- An unknown outcome, or retry without a numeric `next_due`, is an error reply, checked before any write.
- If the hash is missing, `state ~= 'pending'`, or `claim ~= ARGV[2]`: reply `{'STALE'}`.
- Otherwise `HINCRBY attempts 1`, `HSET last_status`, then:
  - `delivered`: `HSET state delivered`, `ZREM`.
  - `failed`: `HSET state failed`, `ZREM`.
  - `retry`: `ZADD outbox next_due rid`.
- Reply `{'OK', attempts}`.

- [ ] **Step 1: Write the failing unit tests** (`tests/unit/test_state_phase4_webhook.py`), using `fake_redis_time` and enqueuing through `submit_request` with a webhook target and then `cancel_request`:
  - Nothing due → `None`.
  - A due delivery is claimed once. A second claim at the same time returns `None` (re-scored), and `claim == 1`.
  - After `advance(lease + 1)`, the delivery is claimable again with `claim == 2`, and `complete_webhook` with the first claim returns `False` and changes nothing.
  - `delivered` sets state and attempts and removes it from the outbox, and `get_webhook` agrees.
  - `retry` keeps it pending and due at `next_due`.
  - `failed` removes it.
  - An entry whose hash was deleted is dropped from the outbox on the next claim.
  - `retry` without `next_due` raises `ValueError` in the Python method before calling Redis.

- [ ] **Step 2: Run, confirm the tests fail.** **Step 3: Implement.**

- [ ] **Step 4: Write the property test** `tests/property/test_webhook_outbox.py`, **Property 7: Webhook outbox delivery** (**Validates: Requirements 8.2, 8.3, 8.5, 8.6, 8.7**):
  - Draw 1..5 Requests, each with or without a webhook target, and terminate each with finalize or cancel.
  - Assert each terminal write enqueued exactly one delivery for Requests with a target and none for the others.
  - Then simulate 2..3 senders with a step list. Each step picks a sender and an action:
    - claim
    - crash (forget the claim)
    - advance Redis time by 0..`2 * lease`
    - complete the held claim with a random outcome
  - The sender chooses `failed` once `attempts + 1 == max_attempts` (3), otherwise `retry` at `now + 1`.
  - After the steps, drain: advance time and let one well-behaved sender claim and complete until the outbox is empty.
  - Assert:
    - every delivery ends `delivered` or `failed` with `1 <= attempts <= max_attempts`;
    - while a claim's lease is live, no other claim of the same rid succeeds (track lease expiry in the model);
    - every `complete_webhook` with a superseded claim returned `False` and changed nothing.

- [ ] **Step 5: Run** the new tests with `-W error`. **Step 6: Lint, type-check, contracts.** **Step 7: Tick** 4.8, 4.9 and the parent `4. State_Store: Lua scripts`.

---

### Task 6: Verify the task group

- [ ] **Step 1:** `uv run pytest -m "not integration" -q -p no:cacheprovider`. Expect `0 failed`, with the same 3 pre-existing pytest-asyncio warnings and no new warnings.
- [ ] **Step 2:** `uv run ruff check && uv run mypy && uv run lint-imports`.
- [ ] **Step 3:** `uv run pytest tests/unit/test_phase3_baseline.py -q`. The contract baseline must not change in this group: no config, metric, route or exit-code changes. If it fails, stop and report.
