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


## Shared context for this group (from the plan header)

- Global Constraints and "How to read this plan": see the top of `docs/superpowers/plans/2026-10-30-das-client-api-state-store.md` (read only the header up to the first `### Task`).
- Reference: `.kiro/specs/das-client-api/design.md` Components §3 (key table, `_common.lua` helpers, script changes) and the Correctness Properties section.
