# Group 4 Task 2 report: idempotent, quota-checked create (spec 4.3, 4.4)

Status: DONE

## Implemented

- `das/state/lua/_common.lua`: `append_event(req_key, event_type, data_json)` reads `events_max_len` from the request hash (1000 when absent), runs `XADD events_key MAXLEN n * type .. data ..`, returns the stream ID.
- `das/state/lua/create_request.lua`: KEYS unchanged; optional ARGV[4..12] (`client_fp`, `budget_json`, `webhook_url`, `webhook_secret_name`, `idem_hash`, `body_hash`, `idem_ttl`, `max_active`, `events_max_len`), '' / '0' = not set. All inputs are decoded and validated before any write: budget must decode to a JSON object, `max_active` an integer >= 0, `events_max_len` an integer >= 1, and `idem_hash` requires `client_fp`, `body_hash` and an integer `idem_ttl` >= 1. Order follows the contract: idempotency (REPLAYED / IDEM_CONFLICT) -> EXISTS -> QUOTA_EXCEEDED -> writes (client fields; webhook target only when both fields are set), SADD to the Active_Set, idem hash `rid`/`body_hash` + EXPIRE, `request.created` event. Header comment updated with Phase 4 Req citations.
- `das/state/store.py`: `ClientRequestSpec`, `CreateOutcome`, `CreateResult`, `StateStore.submit_request` (ValueError when the key hash is set without `body_hash`; budget serialized as compact sorted JSON of `Budget.to_json_obj()`), and a shared private `_create(prompt, trace_context, extra)` that loops over Request_ID allocation. `create_request` keeps its signature and now delegates to `_create` with no extra args, so it also writes `request.created`.
- `das/state/__init__.py`: exports `ClientRequestSpec`, `CreateResult` (`DEFAULT_EVENTS_MAX_LEN` was already exported).

## TDD evidence

RED: `uv run pytest tests/unit/test_state_phase4_create.py -q`
-> `ImportError: cannot import name 'ClientRequestSpec' from 'das.state'` (collection error; expected, nothing implemented yet).

GREEN: `uv run pytest tests/unit/test_state_phase4_create.py -q -W error` -> `14 passed`.

## Property test (Property 2)

- `uv run pytest tests/property/test_idempotent_create.py -q -W error` (default profile, 100 examples) -> 1 passed
- `HYPOTHESIS_PROFILE=ci uv run pytest tests/property/test_idempotent_create.py -q -W error` (200 examples) -> 1 passed

It also asserts that the only request hashes present are the creators' ones.

## Regression and checks

- `uv run pytest tests/unit -k "state or gateway" -q` -> 393 passed
- `uv run pytest tests/property/test_submission_uniqueness.py -q` -> 1 passed
- `uv run pytest tests/unit -m "not integration" -q` -> 1774 passed, 3 warnings. The warnings come from the existing `tests/unit/test_require_docker_hook.py` and are unrelated to this task.
- `uv run ruff check` -> All checks passed
- `uv run mypy` -> no issues in 107 source files
- `uv run lint-imports` -> 1 kept, 0 broken

## Files changed

- das/state/lua/_common.lua
- das/state/lua/create_request.lua
- das/state/store.py
- das/state/__init__.py
- tests/unit/test_state_phase4_create.py (new)
- tests/property/test_idempotent_create.py (new)
- .kiro/specs/das-client-api/tasks.md (ticked 4.3 and 4.4 only)

## Self-review / notes

- No caller of `create_request` changed. A 3-arg script call behaves as before, plus the event.
- `submit_request` always sends `events_max_len` (default 1000), so Phase 4 Requests store it explicitly. Requests from `create_request` don't store it and fall back to 1000 in `append_event`.
- An empty `Budget()` passed in the spec is stored as `budget = "{}"`. The brief doesn't say whether an empty Budget counts as "set". The Gateway (group 7) can pass `None` for an empty budget if it shouldn't be stored.
- `store.py` now imports `das.client.budget` (pure module, no cycle; mypy and lint-imports are clean).
