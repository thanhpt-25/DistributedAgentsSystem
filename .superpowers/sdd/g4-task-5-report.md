# g4 Task 5 report: webhook claim and completion (spec tasks 4.8, 4.9)

Status: DONE

## Implemented

- `das/state/lua/claim_webhook.lua` (new). KEYS[1] outbox, ARGV[1] lease seconds, ARGV[2] `das:req:`. Validates the lease (finite, > 0) and the prefix before any write. `ZRANGEBYSCORE -inf now LIMIT 0 10`. A rid whose hash is missing or not `pending` is `ZREM`ed. The first pending one is re-scored to `now + lease`, `HINCRBY claim 1`, and the reply is `{rid, claim, attempts, url, secret_name, event_id, type, data}`. Returns `{}` when nothing is due. The header notes that the hash key is built from ARGV and is safe only on a single Redis primary (OD 4.1).
- `das/state/lua/complete_webhook.lua` (new). Validates `outcome` and, for `retry`, a finite numeric `next_due`, both as error replies before any read or write. Returns `{'STALE'}` when the hash is missing, not pending, or its `claim ~= ARGV[2]`. Otherwise it does `HINCRBY attempts`, `HSET last_status`, then delivered/failed: `HSET state` + `ZREM`; retry: `ZADD next_due`. Returns `{'OK', attempts}`.
- `das/state/scripts.py`: both scripts registered.
- `das/state/store.py`: `WebhookClaim` dataclass, `WebhookOutcome` Literal, `StateStore.claim_webhook(lease_seconds)` and `StateStore.complete_webhook(claim, outcome, last_status, next_due=None) -> bool`. The Python method raises `ValueError` for an unknown outcome or a retry without `next_due`, before calling Redis. Both are added to `__all__`.
- `das/state/__init__.py`: exports `WebhookClaim` and `WebhookOutcome`.
- `.kiro/specs/das-client-api/tasks.md`: ticked 4, 4.8 and 4.9 (only those three lines).

## TDD evidence

- RED: `uv run pytest tests/unit/test_state_phase4_webhook.py -q -p no:cacheprovider` failed with `ImportError: cannot import name 'WebhookClaim' from 'das.state'` (collection error, as expected).
- GREEN: same command with `-W error`: `8 passed`.

Unit tests (`tests/unit/test_state_phase4_webhook.py`, 8 tests): nothing due gives None; claimed once with claim == 1, re-scored to now + lease, second claim None; after `advance(lease + 1)` the delivery is re-claimed with claim == 2, and completing the old claim returns False with the hash and score unchanged; delivered (state, attempts, last_status, removed, and a repeat completion is stale); retry (pending, due at next_due, not claimable before, claimable after with claim 2 / attempts 1); failed is removed; an entry whose hash was deleted is dropped; retry without next_due raises ValueError and nothing changes.

## Property 7 (`tests/property/test_webhook_outbox.py`)

- default profile, `-W error`: 100 passing (17 invalid). Events: 65% ended failed, 32% stale: superseded claim, 7% ended delivered.
- `HYPOTHESIS_PROFILE=ci`, `-W error`: 200 passing (34 invalid). 66% failed, 23% superseded claim, 7% delivered.

Model: a `Delivery` per rid holds `state, attempts, claim, due, lease_until`. On every `claim_webhook` call the model computes the due set. A `None` reply asserts nothing was due. A claim asserts the rid was due and pending, that `lease_until` is None or `<= now` (no claim during a live lease), that `claim == model.claim + 1`, and that `attempts` matches. It then sets `lease_until = due = now + lease`. On `complete`, the call is stale iff the model's claim differs or the delivery is no longer pending. Stale calls must return False, and a hash + full outbox snapshot taken before the call must equal the one taken after. Applied calls update attempts/state/due. Steps: 2..3 senders (drawn) with claim / crash / advance(0..2*lease) / complete. The outcome is delivered when the step's boolean is true, otherwise failed at `attempts + 1 == 3`, otherwise retry at now + 1. The drain loop claims and fails/retries until the outbox is empty, advancing time to the next due score. Claims senders still held when the steps ended are completed during the drain, right after the drain sender re-claims the same rid, so superseded-claim fencing is exercised often. Without that, the first version produced almost no superseded claims. At the end every delivery is `delivered|failed`, matches the model, has `1 <= attempts <= 3`, and the outbox is empty. Each terminal write (finalize via lease/commit_plan/apply_transition/finalize, or cancel) is checked to enqueue exactly one pending delivery due at the Redis time for a target, and none otherwise (outbox ZCARD equals the number of targets).

Mutation checks (reverted afterwards): removing the `claim` fence from `complete_webhook.lua` fails both the property and unit tests. Not re-scoring on claim (`now` instead of `now + lease`) fails the property.

## Checks

- `uv run pytest -m "not integration" -q -p no:cacheprovider`: 2216 passed, 24 deselected, 3 warnings in 184s. The warnings were in the trimmed output, so I did not see their source. The new tests pass under `-W error`, so they are not the source.
- `uv run ruff check`: All checks passed.
- `uv run mypy`: no issues in 107 source files (the two new test files also pass mypy).
- `uv run lint-imports`: 1 kept, 0 broken.

## Files changed

- das/state/lua/claim_webhook.lua (new)
- das/state/lua/complete_webhook.lua (new)
- das/state/scripts.py
- das/state/store.py
- das/state/__init__.py
- tests/unit/test_state_phase4_webhook.py (new)
- tests/property/test_webhook_outbox.py (new)
- .kiro/specs/das-client-api/tasks.md (3 ticks)

## Self-review / deviations

- No write before validation in either script. `claim_webhook` validates its ARGV, which the brief did not require. It is a small guard in the same style as `rate_check.lua`.
- Python `complete_webhook` also rejects an unknown outcome with ValueError (the brief only mandates the retry check). Lua still validates both.
- `claim_webhook` passes `keys.request("")` (= `das:req:`) as ARGV[2], so the prefix stays tied to `keys.py`.
- In the property, "max_attempts" lives in the sender policy, since the scripts do not enforce a cap. The sender chooses failed on the 3rd attempt, and the property checks that bound.

## Fix (review round 1)

Bug: a claim taken before a delivery reset (Replay + second terminal write, which re-creates the hash with `claim=0` and a new `event_id`) could complete the new delivery once the new claim counter reached the same value. Fixed by fencing on `event_id` too (Req 8.3, design.md §3). `terminal_event` is unchanged.

### Changes per file

- `das/state/lua/complete_webhook.lua`: new ARGV[6] `event_id`. It is required and non-empty, and it is validated first, before the outcome/next_due checks and before any read or write. `HMGET state, claim, event_id` returns `{'STALE'}` unless `state == 'pending'`, `claim == ARGV[2]` and `event_id == ARGV[6]`. The header now covers the ARGV list, the fencing rule and both stale causes, and cites `Phase 4 Req 8.3, 8.5-8.7`.
- `das/state/store.py`: `complete_webhook` passes `claim.event_id` as ARGV[6]. The public signature is unchanged. Its docstring adds "reset by a later terminal write" as a stale cause. The `claim_webhook` docstring notes that at most 10 due entries are inspected per call, so `None` can mean "nothing claimable among the first 10" rather than "nothing due".
- `tests/unit/test_state_phase4_webhook.py` (now 12 tests):
  - `test_claim_from_before_a_delivery_reset_is_stale` runs the exact sequence on real store calls: a Failed Request with a target (Job `a` Failed), A claims (claim 1), then a dead letter, `replay_dead_letter`, `acquire_lease`, `reopen_request`, `apply_transition(replays_applied=(dlid,))` with Job `a` Failed attempt 2, and `fail_request`. B then claims (claim 1, new event_id). A's completion returns False and leaves the hash and outbox score unchanged. B's completion returns True, delivered/1/"204".
  - `test_lua_rejects_bad_arguments_without_writing` (parametrized x3) calls `store._run("complete_webhook", ...)` directly with an unknown outcome, a retry with non-numeric `next_due` (`"soon"`), and an empty `event_id`. Each raises `redis.exceptions.ResponseError` and leaves the hash and score unchanged.
- `tests/property/test_webhook_outbox.py`:
  - Requests can now also end through `fail_request` (Job `a` Failed), so a replay is possible.
  - New step `reset`: Replay of a dead letter for the Failed Job, `reopen_request`, apply the replay, then `fail_request` again. It targets the sender's held delivery when that one is resettable, else the `arg`-th Failed Request.
  - The model then replaces the delivery with a fresh one: new event_id (asserted different from the old one), claim 0, attempts 0, pending, due at Redis now. The outbox ZCARD must equal the number of pending deliveries.
  - Every claim senders held on the reset delivery moves to a `stale` list. Those claims are completed right after the delivery is next claimed (so the counters can collide), and any left over are completed at the end. Each completion must return False and leave the snapshot unchanged.
  - The model tracks `event_id`. A claim asserts `event_id` matches, and staleness now includes an `event_id` mismatch. All existing assertions are kept, and the docstring describes resets.

### RED (new tests on the old code)

- Unit: `test_claim_from_before_a_delivery_reset_is_stale` failed at `assert await store.complete_webhook(a, "delivered", "200") is False` with `E assert True is False`, so A's completion was accepted. `test_lua_rejects_bad_arguments_without_writing[delivered--]` failed with `DID NOT RAISE ResponseError`. The run ended `2 failed, 10 passed`.
- Property: failed with minimal example `reqs=[(True, 'fail')], senders=2, step_list=[(0,'claim',None), (0,'reset',0)]`: `E assert True is not True` from the stale pre-reset completion being applied.

### Mutation check

With ` or v[3] ~= event_id` removed from the fence, under both the default and ci profiles: `FAILED test_claim_from_before_a_delivery_reset_is_stale` (`assert True is False`) and `FAILED test_webhook_outbox_delivery` (`assert True is not True`), `2 failed, 11 passed`. The comparison was then restored.

The first version of the reset step picked a random Request, so resets were rare (about 1-2% of examples) and the property did not fail under this mutation with the default profile. I changed it to target the held delivery or a Failed Request. After that, the default profile shows 24% "reset a pending delivery", 6% "stale: claim from before a reset" and 4% "pre-reset claim with the current counter", and the mutation fails under both profiles.

### GREEN

- `uv run pytest tests/unit/test_state_phase4_webhook.py tests/property/test_webhook_outbox.py -q -W error`: 13 passed (default) and 13 passed (`HYPOTHESIS_PROFILE=ci`).
- Property statistics:
  - default: 100 passing / 12 invalid. 72% ended failed, 29% superseded claim, 24% reset a pending delivery, 8% ended delivered, 6% claim from before a reset, 4% pre-reset claim with the current counter.
  - ci: 200 passing / 13 invalid. 80% ended failed, 22% superseded, 11% reset pending, 2.4% delivered, 2.4% pre-reset, 2.4% pre-reset with equal counter.

### Full fast suite

- `uv run pytest -m "not integration" -q -p no:cacheprovider -W error::DeprecationWarning:das`: 2220 passed, 24 deselected, 3 warnings in 180s.
- Plain run: 2220 passed, 24 deselected, 3 warnings.
- Source of the 3 warnings (`-rw`): `PytestDeprecationWarning: The configuration option "asyncio_default_fixture_loop_scope" is unset`, from `pytest_asyncio/plugin.py:299`. It fires once in each of the three inner `pytester` sessions in `tests/unit/test_require_docker_hook.py` (`test_docker_skip_becomes_error_when_required`, `test_docker_skip_stays_skipped_without_variable`, `test_busy_port_skip_stays_skipped_when_required`). Those sessions run in a temp dir without the repo's `pyproject.toml`, where `asyncio_default_fixture_loop_scope = "function"` is set. They are not from this task's files, so they are not fixed.

### Static checks

- `uv run ruff check`: All checks passed!
- `uv run mypy`: Success: no issues found in 107 source files. The two test files also pass mypy.
- `uv run lint-imports`: Contracts: 1 kept, 0 broken.

tasks.md ticks unchanged.
