# Group 4 final-review fixes: report

Status: DONE_WITH_CONCERNS (minor; see Concerns)

Baseline before any change: the touched test files passed (70 passed).

## Fix 1: `request.reopened` is written by the Replay (Req 6.1, 6.4)

Changes:
- `das/client/events.py`: `reopened_event_data()` returns `{"status": "Pending"}`. New docstring says the Replay writes it and that `reopen_request` writes no Event.
- `das/state/lua/replay_dead_letter.lua`: in the `status == 'Failed'` branch, right after the status `HSET`/`HDEL`, it calls `append_event(KEYS[2], 'request.reopened', '{"status":"Pending"}')`. KEYS[2] is the request hash, which matches the header. The header comment now documents the Event and says that a Replay of a Running Request appends none.
- `das/state/lua/reopen_request.lua`: the `append_event` call is gone, and the header says the Replay already recorded the reopen. The script has only one header block, so I updated that one. I took "both header comments" to mean the headers of both scripts.
- Tests:
  - `tests/unit/test_client_events.py::test_created_and_reopened`: the expected value changed from `{"status": "Running"}` to `{"status": "Pending"}`.
  - `tests/unit/test_state_phase4_events.py::test_reopen_appends_reopened_and_second_finalize_adds_terminal`: the `["request.failed", "request.reopened"]` assertion moved to straight after `replay_dead_letter`. The payload is now `{"status": "Pending"}`. A new assertion checks that `reopen_request` leaves the stream unchanged. The final `ts[-4:]` assertion is unchanged.
  - New `test_replay_then_cancel_before_resumption`: the stream ends `request.failed, request.reopened, request.cancelled`, there is exactly one Terminal_Event after the reopen, and there is exactly one `request.reopened`.
  - New `test_replay_of_running_request_appends_no_event`.
  - `tests/property/test_event_stream.py`: the docstring is updated. `Scenario` gains `after_replay: "cancel" | "resume"`. The expected `request.reopened` (Pending) is now appended right after the Replay. Two branches follow it:
    - cancel before resumption: expects the `job.transition`s and `request.cancelled`;
    - resume: `reopen_request` is now checked to add nothing, then apply and finalize as before.
  - A final check now asserts exactly one Terminal_Event after `request.reopened`.

RED (old code): 4 failed.
- `test_reopen_appends_reopened_and_second_finalize_adds_terminal`: `['job.transition', 'request.failed'] != ['request.failed', 'request.reopened']`
- `test_replay_then_cancel_before_resumption`: same diff at index 0
- `test_created_and_reopened`
- `test_event_stream_mirrors_persisted_changes`

GREEN: `test_state_phase4_events.py`, `test_client_events.py` and `test_event_stream.py`: 36 passed.

## Fix 2: an Idempotency_Record whose Request is gone counts as absent (Req 4.8)

Changes:
- `das/state/lua/create_request.lua`: REPLAYED and IDEM_CONFLICT now apply only when `EXISTS key_prefix(KEYS[1]) .. ':req:' .. rec[1]` returns 1. Otherwise the script falls through to the existing create path, which already `HSET`s the record's `rid` and `body_hash` and `EXPIRE`s it, so the record is overwritten and its TTL refreshed. The header comment is updated.
- `tests/unit/test_state_phase4_create.py`: new `test_record_naming_a_deleted_request_counts_as_absent[b1|b2]` covers both the same body and a different body:
  - the reply is `created` with a new rid;
  - the record holds `{rid: new, body_hash}` and its TTL is refreshed;
  - a later submit with the same key replays the new rid.

RED: 2 failed (both parametrizations). GREEN: 17 passed, including `tests/property/test_idempotent_create.py`.

## Fix 3: cancel recomputes Budget_Used on every retry

Changes:
- `das/state/store.py`:
  - `cancel_request(rid, max_retries=16, prices: PriceTable | None = None)`.
  - Each iteration reads the jobs hash, `pending_replays`, `HGET budget` and `LRANGE usage` in one MULTI pipeline. When a budget is stored, it computes `budget_view(budget, budget_status(usage, budget, prices))`; otherwise it sends `''`.
  - Imports added: `budget_status`, `budget_view`, `PriceTable`.
  - `_budget_view_json` uses `allow_nan=False`.
  - `finalize` and `fail_request` are unchanged.
- `das/state/lua/_common.lua`: `check_budget_view` also requires the text to start with `{` (after whitespace), so a JSON array is rejected.
- Tests in `tests/unit/test_state_phase4_events.py`:
  - `test_cancel_appends_transitions_then_cancelled`: the Request is now created with `Budget(max_total_tokens=5)`, and `cancel_request(rid, budget_view=view)` became `cancel_request(rid, prices=None)`. The expected `view` dict and the assertions are unchanged. The view now comes from the stored budget, so the Request has to carry one.
  - The `submitted()` and `running()` helpers gain an optional `budget` parameter (default `None`).
  - New `RacingStore` subclass and `test_cancel_recomputes_budget_used_after_conflict`. Before the first `cancel_request` run, the subclass adds a UsageRecord (`RPUSH` plus `HINCRBY` totals) and changes Job `a`. The test asserts two script runs, and that `budget.used.total_tokens == usage.total_tokens == 7`.
  - New `test_budget_view_json_rejects_nan`.
  - `test_cancel_rejects_malformed_budget_json_before_writing` is parametrized over `"{bad"` (the original case), `"[]"`, `"[1]"`, `"  [ ]"` and `"7"`. The body is unchanged apart from using the parameter.

RED: 6 failed.
- `cancel_appends...` (unexpected keyword `prices`)
- `recomputes_budget_used_after_conflict`
- `budget_view_json_rejects_nan`
- the three array cases

GREEN: 27 passed.

## Fix 4: webhook retry does not need the host clock

Changes:
- `das/state/lua/claim_webhook.lua`: `fmt_double(now)` is the last reply element, and the header comment is updated.
- `das/state/store.py`:
  - `WebhookClaim.claimed_at: float` is documented as the `now` for `das.client.webhook.next_due`.
  - `claim_webhook` parses it.
  - New `next_webhook_due() -> float | None` (`ZRANGE 0 0 WITHSCORES`) is documented as the sender's idle-wait input.
- `tests/unit/test_state_phase4_webhook.py`:
  - `test_due_delivery_is_claimed_once`: the expected `WebhookClaim(...)` gains `claimed_at=c.claimed_at`, which the new required field forces.
  - New `test_claimed_at_is_redis_time_of_the_claim`: `claimed_at == fake_redis_time.now()`.
  - New `test_next_webhook_due`: `None` when the outbox is empty, the enqueue time after a cancel, `claimed_at + LEASE` after a claim, and `None` after delivery.

RED: 3 failed. GREEN: `test_state_phase4_webhook.py` plus `test_webhook_outbox.py`: 15 passed.

## Fix 5: `delete_request` removes the outbox entry

Changes:
- `das/state/store.py`: `delete_request`'s transaction adds `pipe.zrem(keys.WEBHOOKS_DUE, rid)`, and the docstring mentions the outbox.
- `tests/unit/test_state_phase4_events.py`: new `test_delete_request_removes_outbox_entry`. I did not extend an existing delete test: none of them (`test_state_store.py`, `test_state_phase2_keys.py`, `test_state_phase4_quota.py`) creates a webhook target, so none has an outbox entry to assert on.

RED: `assert 1793350555.206 is None`. GREEN: passed.

## Fix 6: regression tests for rejected calls

New tests in `tests/unit/test_state_phase4_events.py`. Each compares `XLEN`, `XRANGE`, `SMEMBERS` of the Active_Set and `ZRANGE WITHSCORES` of the outbox, before and after:
- `test_cancel_terminal_changes_nothing`
- `test_cancel_not_found_changes_nothing`
- `test_cancel_script_conflict_changes_nothing` (direct `cancel_request.lua` call with stale expected Job JSON)
- `test_finalize_replay_pending_changes_nothing`
- `test_finalize_completed_without_final_result_changes_nothing`

All five pass on the current code, as expected for regression guards. There is no RED step.

## Fix 7: cleanups

Changes:
- `das/state/store.py` `_create`: an unrecognised reply raises `StoreError(f"create_request returned an unexpected reply: {status}")`. Only `EXISTS` loops.
- `das/state/lua/commit_plan.lua`: two-line comment at the event writes, as the brief specifies.
- `tests/unit/test_state_phase4_create.py`: new `test_unknown_create_reply_raises_store_error`, with a stubbed `_run` that returns `SURPRISE`.

RED: the test raised "could not allocate a unique request_id" instead. GREEN: 17 passed.

## Mutation checks

- `append_event` removed from `replay_dead_letter.lua`: 3 failed.
  - `test_reopen_appends_reopened_and_second_finalize_adds_terminal`
  - `test_replay_then_cancel_before_resumption`
  - Property 6 `test_event_stream_mirrors_persisted_changes`

  Restored, and confirmed by grep.
- "Request gone" check removed from `create_request.lua`: 2 failed (`test_record_naming_a_deleted_request_counts_as_absent[b1|b2]`). Restored, and confirmed by grep.

## Property runs (`-W error`)

The runs cover `test_event_stream.py`, `test_webhook_outbox.py`, `test_active_set.py` and `test_idempotent_create.py`.

- default profile: 4 passed (3.16 s)
- `HYPOTHESIS_PROFILE=ci`: 4 passed (6.25 s)

## Focused unit runs (`-W error`)

The run covers the `test_state_phase4_{events,create,webhook,quota,keys}.py` files and `test_client_events.py`: 104 passed.

## Full fast suite

`uv run pytest -m "not integration" -q -p no:cacheprovider`: 2239 passed, 24 deselected, 3 warnings (166.9 s).

The 3 warnings are the pre-existing ones from `tests/unit/test_require_docker_hook.py`. `tests/unit/test_phase3_baseline.py` passes and was not regenerated.

## Static checks

- `uv run ruff check`: All checks passed!
- `uv run mypy`: Success: no issues found in 107 source files
- `uv run lint-imports`: Contracts: 1 kept, 0 broken.

## Existing tests edited

1. `tests/unit/test_client_events.py::test_created_and_reopened`: the reopened payload changed from Running to Pending.
2. `tests/unit/test_state_phase4_events.py`:
   - The import of `das.client.events` became a parenthesised list and gained `TERMINAL_EVENT_TYPES`.
   - `from das.client.budget import Budget` was added.
   - `test_reopen_appends_reopened_and_second_finalize_adds_terminal`: event position (after the Replay instead of after the reopen) and payload (Pending), plus an assertion that `reopen_request` adds nothing.
   - `submitted()` and `running()` helpers: new optional `budget` parameter.
   - `test_cancel_appends_transitions_then_cancelled`: `budget_view=view` became `prices=None`, and the Request is created with `Budget(max_total_tokens=5)` so the stored budget produces the same `view`.
   - `test_cancel_rejects_malformed_budget_json_before_writing`: parametrized with the array and non-object cases. The `"{bad"` case is kept.
3. `tests/unit/test_state_phase4_webhook.py::test_due_delivery_is_claimed_once`: `claimed_at=c.claimed_at` added to the expected `WebhookClaim`.
4. `tests/property/test_event_stream.py`: the docstring, the `Scenario.after_replay` field and its draw, the expected position and payload of `request.reopened`, the new cancel-before-resumption branch, and the one-Terminal_Event-after-reopen assertion.

No existing test failed for any other reason. `tasks.md` was not touched.

## Concerns

- Fix 5: the brief said to extend the existing delete test. I added a new test instead, because no existing delete test has an outbox entry to check.
- Fix 3: `test_cancel_appends_transitions_then_cancelled` needed more than a parameter rename. The Request now needs a stored budget, because the view is computed from it.
- Fix 3: `cancel_request` now raises `ValueError` from `Budget.from_json_obj` or `UsageRecord` validation if the stored budget or usage is corrupt. Before this change it never read them. A corrupt stored value cannot come from `submit_request`, so I left that as is.
- Fix 1: `reopen_request.lua` has a single header comment, so I updated that one. I read "both header comments" as one header in each of the two scripts.
