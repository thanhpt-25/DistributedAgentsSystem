# Group 4 Task 3 report: Active_Set release and re-add (spec 4.5 part, 4.6)

Status: DONE

## Implemented
- `das/state/lua/_common.lua`: `client_active_add(req_key, rid)` / `client_active_remove(req_key, rid)`, appended after `append_event`. Each does `HGET req_key client_fp` and SADD/SREM on `active_key(req_key, fp)`; no-op when `client_fp` is missing or empty (Phase 4 Req 3.7).
- `finalize.lua`: `client_active_remove(KEYS[1], rid)` right after `HSET status target` (Req 3.3). Header cites Phase 4 Req 3.3.
- `cancel_request.lua`: `client_active_remove(KEYS[1], rid)` right after `HSET status Cancelled` (Req 3.3).
- `replay_dead_letter.lua`: `client_active_add(KEYS[2], rid)` inside the `status == 'Failed'` branch only, after the HDEL; no quota check (Req 3.4). A Running Replay leaves the set alone.
- `purge_request.lua`: `client_active_remove(KEYS[1], rid)` immediately before the `DEL` (reads `client_fp` first), only on the DELETED path (Req 3.5).
- `das/state/store.py` `delete_request`: reads `client_fp` before the pipeline and adds `SREM client_active(fp) rid` to the MULTI when present (docstring cites Phase 4 Req 3.6).
- No KEYS/ARGV changes, no events. All additions sit after validation, alongside the existing writes.

## TDD evidence
RED: `uv run pytest tests/unit/test_state_phase4_quota.py -q` -> `7 failed, 3 passed` (finalize Completed/Failed, cancel, replay re-add, replay at full quota, purge, delete_request all failed with Active_Set still holding the rid / missing the rid). The 3 that passed before implementation were the guard cases: rejected finalize keeps member, Running replay leaves set alone, create_request touches no set.
GREEN: same command -> `10 passed in 0.17s`.

## Property test (tests/property/test_active_set.py, Property 3)
- Default profile: `1 passed in 0.71s`. Statistics: ~10.7% of examples purge a client Request, ~5.7% have a Replay reopen a Failed Request.
- `HYPOTHESIS_PROFILE=ci`: `1 passed in 0.75s`.
- Mutation check: removing `client_active_add` from replay_dead_letter.lua makes the property fail (`AssertionError: aaaaaaaa`). Removing the purge SREM is not caught by the property (finalize/cancel already remove the member before purge, so in a reachable sequence there is nothing for purge to remove); the unit test `test_purge_of_expired_request_leaves_no_member` covers that path by seeding a dangling member. Both scripts restored and verified afterward.

## Regression suites (Step 5)
- `tests/property/test_retention_safety.py test_replay_discard_exclusivity.py test_state_cancel.py`: 4 passed.
- `uv run pytest tests/unit -k "state or retention or dlq or replay" -q`: 325 passed.
- Additionally `uv run pytest -m "not integration" -q`: 2187 passed (3 warnings, pre-existing; new tests pass with `-W error`).

## Lint / types / contracts
- `uv run ruff check`: All checks passed.
- `uv run mypy`: no issues in 107 source files.
- `uv run lint-imports`: 1 kept, 0 broken.

## Files changed
- das/state/lua/_common.lua, finalize.lua, cancel_request.lua, replay_dead_letter.lua, purge_request.lua
- das/state/store.py (delete_request)
- tests/unit/test_state_phase4_quota.py (new)
- tests/property/test_active_set.py (new)
- .kiro/specs/das-client-api/tasks.md (4.6 ticked only)

## Self-review / notes
- No write before validation: every new call is placed after the scripts' existing checks and after their first status write.
- `delete_request` reads `client_fp` outside the MULTI (read then pipeline), as the brief specifies; a concurrent create cannot reuse the rid, so no race of concern.
- Property test model: finalize acquires a lease, commits the plan (or reopens a replayed Request, clearing `pending_replays` via `replays_applied` so finalize is not blocked by REPLAY_PENDING); Failed finalize leaves Job `a` Failed with an increasing attempt so later Replays pass STALE_ATTEMPT. Each purge advances Redis time past the TTL.
- No deviations from the brief.
