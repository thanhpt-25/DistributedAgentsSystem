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


## Shared context for this group (from the plan header)

- Global Constraints and "How to read this plan": see the top of `docs/superpowers/plans/2026-10-30-das-client-api-state-store.md` (read only the header up to the first `### Task`).
- Reference: `.kiro/specs/das-client-api/design.md` Components §3 (key table, `_common.lua` helpers, script changes) and the Correctness Properties section.
