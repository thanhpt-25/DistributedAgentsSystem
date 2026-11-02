# Group 4 final-review fix brief (approved by the user)

The whole-group review of task group 4 returned "ready to build on, with fixes". The user approved the fixes below and chose option (a) for both spec decisions. The spec is already updated: read requirements.md Req 4.8, 6.1, 6.4, 8.2 (last sentence) and design.md §3 (key table, `replay_dead_letter.lua`, `reopen_request.lua`, `create_request.lua`, `claim_webhook.lua` bullets, Property 6).

Keep every change test-first: add the test, watch it fail on the current code, then fix.

## Fix 1: `request.reopened` is written by the Replay (Req 6.1, 6.4)

Problem: `replay_dead_letter.lua` reopens a Failed Request (Failed → Pending) but writes no Event. `reopen_request.lua` writes `request.reopened` later, when a Manager resumes the Request. A cancel in between leaves the stream `... request.failed, request.cancelled`.

- `das/client/events.py`: `reopened_event_data()` returns `{"status": "Pending"}`, and its docstring says it is written by the Replay. Update `tests/unit/test_client_events.py` to match.
- `das/state/lua/replay_dead_letter.lua`: in the `status == 'Failed'` branch, call `append_event(KEYS[2], 'request.reopened', '{"status":"Pending"}')` after the status write. Check which KEYS index is the request hash. A Replay of a Running Request appends nothing.
- `das/state/lua/reopen_request.lua`: remove the `append_event` call. Update both header comments.
- Tests:
  - Update the existing event tests in `tests/unit/test_state_phase4_events.py` and `tests/property/test_event_stream.py` to the new position and payload. Change positions and payloads only; keep each test's intent.
  - New unit test for Replay then cancel before resumption: the stream ends `request.failed, request.reopened, request.cancelled`, and there is exactly one Terminal_Event after the reopen.
  - Extend the Property 6 strategy so that after a Replay it either cancels before resumption or resumes and finalizes again, matching the updated Property 6 text in design.md.

## Fix 2: an Idempotency_Record whose Request is gone counts as absent (Req 4.8)

- `das/state/lua/create_request.lua`: when the idempotency record exists, check whether the Request it names still exists. Build the key from `key_prefix(KEYS[1])` as `<prefix>:req:<rid>`, the same layout `keys.request` uses.
  - If that Request is gone, continue as if no record existed: create a new Request and overwrite the record's `rid` and `body_hash`, then refresh its TTL.
  - If the Request still exists, keep the current `REPLAYED` and `IDEM_CONFLICT` replies.
  - Update the header comment.
- Unit tests:
  - Create with key K, `delete_request` the Request, then submit again with K and the same body: the reply is `created` with a new rid, and the record points at the new rid.
  - Same as above but with a different body: still `created`, not `idempotency_conflict`.

## Fix 3: cancel recomputes Budget_Used on every retry

Problem: `StateStore.cancel_request` (das/state/store.py ~637) serialises `budget_view` once, before its compare-and-set retry loop. A retry after CONFLICT sends a stale view next to fresh usage totals.

- Replace the `budget_view` parameter of `cancel_request` with `prices: PriceTable | None = None`.
- In each loop iteration, read the Request's stored budget and its UsageRecords. You may use the same pipeline read the loop already does, or add `HGET budget` and `LRANGE usage` to it.
  - If the Request has a budget, compute `budget_view(budget, budget_status(usage, budget, prices))` with `das.client.budget`.
  - If it has none, use `''`.
- `finalize` and `fail_request` keep their `budget_view` parameter. Only the lease owner adds usage, so their view cannot go stale.
- Update the Task 4 tests that pass `budget_view=` to `cancel_request`.
- New unit test that forces one CONFLICT. For example, use a thin `StateStore` subclass whose first `_run("cancel_request", ...)` adds a UsageRecord and changes a Job record via a direct store write before delegating. Assert that the `request.cancelled` event's `budget.used.total_tokens` equals its `usage.total_tokens`.
- `_budget_view_json`: use `json.dumps(..., allow_nan=False)`.
- `check_budget_view` in `_common.lua`: reject a JSON array, so only an object or `''` is accepted. Add a unit case.

## Fix 4: webhook retry must not need the host clock

- `das/state/lua/claim_webhook.lua`: append the Redis time of the claim, `fmt_double(now)`, as the last reply element. Update the header comment.
- `WebhookClaim` gains `claimed_at: float` (Redis seconds), documented as the `now` to pass to `das.client.webhook.next_due`.
- New `StateStore.next_webhook_due() -> float | None`: the lowest score in `WEBHOOKS_DUE` (`ZRANGE 0 0 WITHSCORES`), or `None` when the outbox is empty. Document it as the input for the sender's idle wait.
- Unit tests:
  - `claimed_at` equals the `FakeRedisTime` time of the claim.
  - `next_webhook_due` is `None` when the outbox is empty, equals the enqueue time after a cancel, and equals `claimed_at + lease` after a claim.

## Fix 5: `delete_request` leaves no outbox entry

- Add `pipe.zrem(keys.WEBHOOKS_DUE, rid)` to `delete_request`'s transaction. Extend the existing delete test to assert the entry is gone.

## Fix 6: regression tests for rejected calls (review Minor 6)

A rejected call must leave the Event_Stream, the Active_Set and the outbox unchanged. Add unit tests (compare `XLEN`/`XRANGE`, `SMEMBERS` and `ZRANGE ... WITHSCORES` before and after) for:
- `cancel_request` returning TERMINAL (on a Completed Request)
- `cancel_request` returning NOT_FOUND
- a direct `cancel_request.lua` call returning CONFLICT
- `finalize` returning REPLAY_PENDING
- `finalize` returning INVALID_STATE (Completed without a Final_Result)

## Fix 7: small cleanups from the review

- `StateStore._create`: only `EXISTS` loops. Any other unrecognised reply raises `StoreError` with the reply code.
- `commit_plan.lua`: add a two-line comment at the event writes. It should say that events are written before the single plan+status `HSET` so they stay one-for-one with the transition log, which `put_job` writes in the same loop.

## Do not change

- `terminal_event`'s reset behaviour.
- The `complete_webhook` fence.
- The `rate_check` script.
- Config.
- The Phase 3 contract baseline: `tests/unit/test_phase3_baseline.py` must still pass with no regeneration.

## Verify

- Focused runs with `-W error`: every test file you touched. Run the property tests (`test_event_stream.py`, `test_webhook_outbox.py`, `test_active_set.py`, `test_idempotent_create.py`) with the default profile and with `HYPOTHESIS_PROFILE=ci`.
- Mutation checks:
  - Remove the new `append_event` in `replay_dead_letter.lua` and confirm the new unit test and Property 6 fail. Restore it.
  - Remove the "Request gone" check in `create_request.lua` and confirm the Fix 2 test fails. Restore it.
- Full fast suite once: `uv run pytest -m "not integration" -q -p no:cacheprovider`. The expected warnings are the 3 pre-existing ones from `tests/unit/test_require_docker_hook.py`.
- `uv run ruff check && uv run mypy && uv run lint-imports`.
- `tasks.md`: do not change any ticks.
