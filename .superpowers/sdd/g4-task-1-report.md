# Group 4, Task 1 report: keys, Lua helpers, rate_check (spec 4.1, 4.2)

Status: DONE_WITH_CONCERNS (minor, see the end)

## Implemented
- `das/state/keys.py`: `client_bucket`, `client_active`, `client_idem`, `client_streams`, `events`, `webhook`, `WEBHOOKS_DUE`; `F_CLIENT_FP`, `F_BUDGET`, `F_WEBHOOK_URL`, `F_WEBHOOK_SECRET_NAME`, `F_EVENTS_MAX_LEN`, all added to `RESERVED_REQUEST_FIELDS`. `request_keys` now ends with `events(rid)`, `webhook(rid)` (11 keys). Module docstring key table updated.
- `das/state/lua/_common.lua`: `key_prefix` (errors on a non-request key), `events_key`, `webhook_key`, `outbox_key`, `active_key`, `idem_key`, `fmt_double`, plus a comment saying that building key names outside KEYS relies on the single Redis primary (Phase 1 OD 4.1).
- `das/state/lua/rate_check.lua` (new): header comment in the existing style. It validates rate (finite, >= 0) and burst (integer >= 1) before any read or write. `rate == 0` returns OK and touches nothing. Otherwise it follows `bucket.take` using `server_time()`, stores both values with `fmt_double`, sets `PEXPIRE ceil(burst/rate*1000)+60000`, and replies `{'OK'}` or `{'LIMITED', fmt_double(retry_after)}`.
- `das/state/scripts.py`: `"rate_check"` added to `SCRIPT_NAMES`.
- `das/state/store.py`: `DEFAULT_EVENTS_MAX_LEN = 1000` (in `__all__`) and `StateStore.rate_check(fingerprint, rate, burst) -> float | None`, which sends rate as `%.17g`. `das/state/__init__.py` exports `DEFAULT_EVENTS_MAX_LEN`.
- `das/state/lua/purge_request.lua`: KEYS layout comment updated. `DEL` now covers KEYS[1..11], and the index `ZREM` loop and the finished/inbox_purge references moved up by two (KEYS[12..21], 18, 19).

## TDD evidence
RED: `uv run pytest tests/unit/test_state_phase4_keys.py -q` gave 16 failed. The failures were AttributeError for `keys.client_bucket` and `F_CLIENT_FP`, the request_keys tail mismatch, ImportError for `DEFAULT_EVENTS_MAX_LEN`, Lua `attempt to call global 'key_prefix' (a nil value)` (and the same for each helper), `'StateStore' object has no attribute 'rate_check'`, and `KeyError: 'unknown State_Store script: rate_check'`.
GREEN: `uv run pytest tests/unit/test_state_phase4_keys.py tests/property/test_rate_limit_bucket.py -q -W error` gave 17 passed.

## Property test (Property 1, tests/property/test_rate_limit_bucket.py)
- default profile: 100 passing, 0 failing (14 invalid); 1 passed
- `HYPOTHESIS_PROFILE=ci`: 200 passing, 0 failing (41 invalid); 1 passed
It checks that Lua and model decisions are equal at every step, that retry_after matches within 1e-12, that every window satisfies the bound (a 1e-9 slack covers float rounding), and that two StateStores on one FakeServer, called alternately, match one model.

## Regressions and checks
- `uv run pytest tests/unit -k state -q`: 188 passed
- retention_safety, prop_retention_liveness, state_cancel, manager_retention, phase2_keys: 31 passed
- Full `uv run pytest -m "not integration"`: 2161 passed
- `ruff check`: All checks passed. `mypy`: no issues (107 files). `lint-imports`: 1 kept, 0 broken.

## Files changed
das/state/keys.py, das/state/lua/_common.lua, das/state/lua/rate_check.lua (new), das/state/lua/purge_request.lua, das/state/scripts.py, das/state/store.py, das/state/__init__.py, tests/unit/test_state_phase4_keys.py (new), tests/property/test_rate_limit_bucket.py (new), tests/unit/test_state_phase2_keys.py, tests/property/test_retention_safety.py, .kiro/specs/das-client-api/tasks.md (4.1 and 4.2 ticked)

## Self-review, deviations, concerns
- Two existing tests hardcoded 9 request keys. `test_state_phase2_keys.py` now expects 11. In `test_retention_safety.py`, `assert_request_gone` sliced `names[:9]`; it now slices by `len(keys.request_keys(rid))`. Without this fix the events/webhook keys would have been treated as zsets.
- Additions beyond the brief: one unit test that rejects bad rate_check args and writes nothing, one test that checks the Lua helpers against the Python builders, and a `webhook_key`/`idem_key` case.
- tasks.md 4.1 lists `_common.lua` helpers `append_event`, `active_remove` and `webhook_enqueue`. Per the brief, those belong to Tasks 2-5 and are not implemented here. I ticked 4.1 as instructed, but its sub-bullet is only partly done until those tasks land.
- Docstring citations for the new key builders use the requirements numbering: quota is Req 3, idempotency Req 4, SSE Req 7.
