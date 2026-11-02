# Group 4 Task 5 fix brief

The task review found, and the controller reproduced on the real scripts, a fencing bug:

1. A Failed Request with a webhook target gets a Webhook_Delivery (claim 0, event_id E1). Sender A claims it: claim 1.
2. A Replay reopens the Request and a later finalize writes a new Terminal_Event. `terminal_event` (`das/state/lua/_common.lua`) DELs and re-creates the hash with `claim=0`, `event_id=E2`, and re-scores the rid to now.
3. Sender B claims the new delivery: claim 1 again.
4. A's `complete_webhook(claim=1, "delivered")` passes the fence: the E2 delivery is marked delivered without being sent, and B's completion returns STALE.

The spec now requires the completion fence to check `event_id` as well (requirements.md Req 8.3, design.md §3 `complete_webhook.lua`). Do not change `terminal_event`'s reset behaviour.

## Fix

1. `das/state/lua/complete_webhook.lua`: new ARGV[6] = `event_id` of the claimed delivery (required, non-empty; validate before any read or write). Read `event_id` together with `state` and `claim` and return `{'STALE'}` unless all three match (`state == 'pending'`, `claim == ARGV[2]`, `event_id == ARGV[6]`). Update the header comment (ARGV list, fencing rule, `Phase 4 Req 8.3, 8.7`).
2. `StateStore.complete_webhook` passes `claim.event_id`. No public signature change.
3. Unit test in `tests/unit/test_state_phase4_webhook.py` for the exact sequence above using real store calls:
   - Make a Failed Request with a webhook target and a dead letter on a Failed Job. Follow `tests/property/test_active_set.py` / `tests/property/test_replay_discard_exclusivity.py` for how a Failed Request with a replayable dead letter is built.
   - Claim it as A.
   - `replay_dead_letter`, then `acquire_lease` / `reopen_request`, apply the replay so `pending_replays` is empty, then `fail_request` or `finalize` again.
   - Claim it as B.
   - Assert A's completion returns `False` and changes nothing (hash and outbox score unchanged), then B's completion returns `True`.
4. Unit test for the Lua error replies: call the script directly (`store._run("complete_webhook", ...)`, or register `script_source("complete_webhook")`), and check that an unknown outcome, a retry with non-numeric `next_due`, and an empty `event_id` each raise and write nothing.
5. `tests/property/test_webhook_outbox.py`: add an action that performs a second terminal write on a delivery's Request, which resets the delivery. Use the replay + re-finalize path if the property's Requests can reach it cheaply. Otherwise end some Requests Failed via `fail_request` and give them a dead letter so replay is possible.
   - The model must then reset that Delivery (new event_id, claim 0, attempts 0, due now).
   - Every claim held before the reset must be kept as stale and replayed later as a completion, which must return False and change nothing.
   - Keep the existing assertions. Update the docstring to say resets are covered.
6. Docstring note on `StateStore.claim_webhook`: it inspects at most 10 due entries per call, so `None` can mean "nothing claimable among the first 10" rather than "nothing due".

## Verify

- Test-first: add the reset unit test and the new property action, run them on the current code, and record that they FAIL (the reset test: A's completion returns True). Then fix.
- `uv run pytest tests/unit/test_state_phase4_webhook.py tests/property/test_webhook_outbox.py -q -W error` (and the property with `HYPOTHESIS_PROFILE=ci`).
- Mutation check: temporarily remove the `event_id` comparison and confirm both the unit test and the property fail. Restore it.
- Full fast suite once: `uv run pytest -m "not integration" -q -p no:cacheprovider -W error::DeprecationWarning:das`. Then run it plainly, and identify where the 3 pre-existing warnings come from (`-rw` shows them). Report the source. Do not fix them unless they come from this task's files.
- `uv run ruff check && uv run mypy && uv run lint-imports`.
- `tasks.md`: already ticked; do not change ticks.
