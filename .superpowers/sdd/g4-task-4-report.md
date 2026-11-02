# Group 4, Task 4 report: Event_Stream and webhook enqueue (spec tasks 4.5, 4.7)

Status: DONE

## Implemented
- `das/state/lua/_common.lua`
  - `put_job(..., tag, req_key)`: moved below `append_event` (Lua locals must be defined before use). When it writes a transition entry, it also appends `job.transition` built from the same `entry` table: `{job_id, from, to, attempt, worker_id, at}`. `from` and `worker_id` are cjson.null when absent.
  - `check_budget_view(json)`: decodes and checks an optional budget view object, so callers can validate before the first write.
  - `terminal_event(req_key, rid, status, error_or_nil, budget_json, ms)`: builds the data as text from the hash usage totals, adding `,"budget":<budget_json>` verbatim when set, then appends `request.{completed|failed|cancelled}`. With `webhook_url` it runs `DEL` and then `HSET` on the webhook hash (url, secret_name, state=pending, attempts=0, last_status='', claim=0, event_id, type, data), followed by `ZADD webhooks:due fmt_double(ms/1000) rid`. It returns the stream ID.
- `commit_plan.lua`: adds ARGV[6] for the planned event JSON (`''` means no event); job pairs move to ARGV[7..]. The arity check is now `(#ARGV-6)%2`, and ARGV[6] is decoded before the first write. `request.planned` is appended after `DEL jobs` and before the first `put_job`, verbatim, so an empty `depends_on` stays `[]`.
- `apply_transition.lua`: passes `KEYS[1]` to `put_job`.
- `reopen_request.lua`: appends `request.reopened` `{"status":"Running"}`.
- `finalize.lua`: adds optional ARGV[8] for the budget view, checked after `check_usage` and before any write. `terminal_event` runs last, with the error from ARGV[6] or nil.
- `cancel_request.lua`: adds ARGV[3] for the budget view (checked before writes); triples move to ARGV[4..] and the arity check is now `(#ARGV-3)%3`. `put_job` gets `KEYS[1]`, and `terminal_event(..., 'Cancelled', nil, ARGV[3], ms)` runs last.
- `purge_request.lua`: `ZREM outbox rid` on DELETED.
- `das/state/store.py`:
  - New frozen `WebhookState` dataclass and `get_webhook(rid)`.
  - `commit_plan` passes `json.dumps(planned_event_data(plan))`.
  - `finalize`, `fail_request` and `cancel_request` take `budget_view: Mapping | None = None`, sent as `json.dumps` or `''` through the `_budget_view_json` helper.
  - Header comments and docstrings cite Phase 4 Req 6.1, 6.3, 6.4, 8.2.
- `das/state/__init__.py`: exports `WebhookState`.

## TDD evidence
- RED 1: `uv run pytest tests/unit/test_state_phase4_events.py` failed with ImportError: cannot import name 'WebhookState'.
- RED 2, after adding only the dataclass: 17 failed, 2 passed. The failures were the expected missing behaviour, for example `assert 'request.created' == 'request.failed'` and `get_webhook` missing.
- GREEN: 19 passed. Before that, 3 tests failed with REPLAY_PENDING because of my own test setup, not the code: the owner has to apply the Replay before it finalizes. I added an `apply_replay` helper.
- Property test, first run: failed because a batch after finalize returns LEASE_LOST, since finalize releases the Lease. REQUEST_TERMINAL only happens after a cancel. I fixed the test to accept either and assert that nothing is appended.

## Property 6 (`tests/property/test_event_stream.py`)
- Default profile: 100 passing, 0 failing (11 invalid).
- `HYPOTHESIS_PROFILE=ci`: 200 passing, 0 failing (25 invalid).
- After every step it checks the whole stream against the expected sequence built from the pure builders: planned_event_data, transition_event_data per log entry, reopened_event_data, and terminal_event_data from load_usage.

## Existing tests edited (argument lists only, no assertion changed)
- `tests/unit/test_state_atomic.py`: `commit_args` gains a sixth arg, `json.dumps(planned_event_data(plan))`. Added imports for `json` and `planned_event_data`.
- `tests/unit/test_state_phase2_writes.py`: the direct `commit_plan` args gain `""` as ARGV[6]; the direct `cancel_request` args become `[rid, json.dumps([DL1]), ""]`.
- `tests/unit/test_state_phase2_keys.py::test_put_job_tag_sets_dead_letter_id`: **not listed in the brief**. It calls `put_job` directly, which now needs `req_key`, so I added `KEYS[3]` to the call and `keys.request("r")` to its keys list. No assertion changed.

## Fault-injection tests (test_state_atomic.py)
Both still pass.
- commit_plan: the Running invariant rests on one `HSET plan+status` after every `put_job`. The new `XADD`s (planned, then one per Job) come before that HSET. A fault at any of them leaves the Request Pending with no plan, and the existing assertion holds.
- finalize: `terminal_event` runs after `HSET final_result` and after `HSET status`. A fault inside it leaves the Request Completed with its Final_Result already written. A Terminal_Event or webhook may then be missing, but that partial state is the same kind the test already accepts for later writes such as the lease release.

## Regression and checks
- `uv run pytest tests/unit -k "state or manager or gateway"`: 615 passed.
- `uv run pytest tests/property`: 404 passed.
- Full fast suite (`-m "not integration"`): 2207 passed, 24 deselected, 3 warnings. The warnings are an existing pytest-asyncio deprecation in `tests/unit/test_require_docker_hook.py` and are unrelated.
- `uv run ruff check`: All checks passed. `uv run mypy`: no issues in 107 files. `uv run lint-imports`: 1 kept, 0 broken.
- Ticked 4.5 and 4.7 in `.kiro/specs/das-client-api/tasks.md`.

## Files changed
- das/state/lua/_common.lua, commit_plan.lua, apply_transition.lua, reopen_request.lua, finalize.lua, cancel_request.lua, purge_request.lua
- das/state/store.py, das/state/__init__.py
- tests/unit/test_state_phase4_events.py (new), tests/property/test_event_stream.py (new)
- tests/unit/test_state_atomic.py, tests/unit/test_state_phase2_writes.py, tests/unit/test_state_phase2_keys.py (argument lists only)
- .kiro/specs/das-client-api/tasks.md (two checkboxes)

## Self-review and concerns
- Every new input (planned JSON, budget view) is decoded before the first write. The malformed-budget tests for finalize and cancel check that nothing changes.
- The tests compare event data against the pure builders, including `worker_id`/`from` null, the usage totals and the spliced budget.
- `terminal_event` reads usage from the hash counters, which `append_usage` keeps equal to the sums over `load_usage`. The tests confirm they match.
- Deviation 1: I edited `test_state_phase2_keys.py`, which the brief does not list. It was an argument-list change only.
- Deviation 2: `put_job` moved lower in `_common.lua` so it can call `append_event`. Its body is unchanged apart from the event append.
- The design.md signature `terminal_event(req_key, rid, status, budget_json, now)` omits the error parameter. I followed the brief's signature, which includes `error_or_nil`.
