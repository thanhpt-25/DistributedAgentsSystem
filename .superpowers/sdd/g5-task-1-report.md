# Group 5, Task 1 report: budget stop in the reducer (spec 5.1, 5.2)

Status: DONE

## Implementation

### das/state/store.py
- `StoredRequest` gains `budget: Budget | None = None` and `client_fp: str | None = None`.
- `load_snapshot` parses `F_BUDGET` with `Budget.from_json_obj(json.loads(...))` and reads `F_CLIENT_FP`. Both stay `None` when the field is absent. No extra Redis round trip: the fields come from the existing request `HGETALL`.

### das/manager/machine.py
- Constants `BUDGET_EXCEEDED = "budget_exceeded"` and `BUDGET_UNENFORCEABLE = "budget_unenforceable"`, both exported.
- `StepContext.price_table: PriceTable | None = None`.
- `RequestSnapshot.budget: Budget | None = None`. `from_stored` copies `budget` and `usage` (`tuple(stored.usage or ())`). `apply_decision` carries `budget` over unchanged.
- `FinalizeFailed.budget_kind: ExceededKind | None = None`. It is set only on a budget stop.
- `budget_exceeded(snapshot, ctx)` is a pure helper over `snapshot.usage`.
- `_Work.budget_kind()` returns `budget_status(snap.usage + new_usage, budget, ctx.price_table).exceeded`, or `None` without a budget. It is computed on every call, so it includes usage accepted earlier in the same step.

How each rule maps to code:

| Rule | Code |
|---|---|
| Accepted Failed/TimedOut result | `_on_result`: the result and its usage are recorded first, then `w.budget_kind()` is checked. If it is set, the Job goes to `FAILED`, reason `budget_exceeded`. No retry timer, no `PublishDeadLetter`. |
| Retry timer | `_on_timer`: the timer is removed (existing code). In stop mode a Failed/TimedOut Job goes to `FAILED`, reason `budget_exceeded`. `enter_ready` is never called. |
| Publish timer | `_on_timer`: the timer is removed and the handler returns without dispatching. `_settle` then skips the Job. |
| Publish failure | `_on_publish_failed`: the Job goes to `SKIPPED`, reason `budget_exceeded`, and its timers are removed. |
| Replay | `_on_replay`: the violation is `"budget exceeded"` and the existing `_revert_replay` runs. |
| `_settle` in stop mode | `_settle_budget_stop` replaces the capability-wait, propagation and readiness passes. It runs in plan order: WAITING goes to SKIPPED. A READY Job with a `publish`/`capwait` timer for its attempt goes to SKIPPED and its timers are removed. A Job awaiting a retry goes to FAILED and its retry timer is removed. When every Job `is_settled`, it emits `FinalizeFailed(build_final_result(plan, jobs, results, None, error), budget_kind=kind)`. `RunSummarizer` is never emitted. |
| No `PublishDispatch` | Every dispatch site (`enter_ready`, `try_dispatch` from timers and capability wait) is bypassed in stop mode. |

- Pending-timer check: `_never_published` looks at the step's current timers and also the snapshot's timers. A publish timer that `_on_timer` removed in the same step still marks the Job as never published, matching the brief's "remove the timer only; `_settle` then skips the Job".
- Single change per Job per step: each handler branch makes one change. `_settle_budget_stop` only touches Jobs still WAITING, READY-with-timer, or awaiting retry. A Job changed by a handler in this step is no longer in one of those states, so `_Work.put` never fires.
- `CancelObserved` and non-Running snapshots are untouched. With `budget=None`, `budget_kind()` returns `None` and every original path runs unchanged.

## TDD evidence
- RED: `uv run pytest tests/unit/test_manager_machine_budget.py -q` gave `ImportError: cannot import name 'BUDGET_EXCEEDED' from 'das.manager.machine'` (collection error, expected).
- GREEN: `uv run pytest tests/unit/test_manager_machine_budget.py -q -p no:cacheprovider -W error` gave 18 passed.
- The unit tests cover every case the brief lists. Extra cases: a final-attempt Failed result is not dead-lettered; a retry timer firing during the stop; a pending retry of another Job when a Succeeded result crosses the limit; a publish timer firing; `PublishFailed`; cost reaching the limit (`kind == "cost"`).

## Property test (tests/property/test_budget_stop.py, Property 5)
- default profile: 100 passing (35 invalid from job-id uniqueness filters), 1 passed in 0.78s.
- `HYPOTHESIS_PROFILE=ci`: 200 passing, 1 passed in 1.24s. Both runs used `-W error`.
- Coverage from `hypothesis.event` labels (ci): stop kind cost 27%, tokens 19%, unenforceable 10%. Finalize at the stop step 43%, finalize after in-flight Jobs finished 11%, finished within budget 15%.
- How the model tracks the stop: it calls `budget_exceeded(post, ctx)` after each `apply_decision`. The first non-`None` step is the stop step. At that step it records the token count, the length of `post.usage`, and the In_Flight_Jobs as `{job_id: attempt}`, using the glossary definition: Ready or Dispatched, attempt ≥ 1, no result for that attempt, and not Ready with a `publish`/`capwait` timer.
- Assertions:
  - From the stop step on: no `PublishDispatch`, no `ScheduleTimer`, and empty `timers_add` (so no retry timer). No `RunSummarizer`.
  - Every Usage_Record appended after the stop belongs to a Job/attempt in flight at the stop, at most one per Job.
  - Tokens gained since the stop are at most the usage of those Jobs' results.
  - The in-flight set only shrinks.
  - A budget `FinalizeFailed` is present exactly when nothing is in flight, with the correct `budget_kind` and error. No non-budget `FinalizeFailed` appears.
  - Before the stop: each event is also stepped on a parallel `budget=None` snapshot. The `PublishDispatch` lists and the whole `Decision` must be equal.
- Events are drawn from: results (random status and usage) for In_Flight_Jobs, `PublishConfirmed`, `PublishFailed`, firing a persisted timer (time advances to its due time), and `Tick`. Workers are sometimes absent so capwait timers occur. Up to 40 events.

## Regression and checks
- `uv run pytest tests/unit -k "machine or manager" -q`: 213 passed.
- Reducer property suites (dispatch_readiness, attempt_progression, failure_propagation, machine_replay, machine_cancelled, final_result): 7 passed.
- Full fast suite `uv run pytest -m "not integration" -q -p no:cacheprovider`: 2258 passed, 24 deselected. 3 warnings: the existing pytest-asyncio fixture-loop-scope deprecation, not from the new tests.
- `uv run ruff check`: all checks passed. `uv run mypy`: no issues in 107 files. `uv run lint-imports`: 1 kept, 0 broken.
- No existing test was edited or failed.

## Files changed
- das/manager/machine.py
- das/state/store.py
- tests/unit/test_manager_machine_budget.py (new)
- tests/property/test_budget_stop.py (new)
- .kiro/specs/das-client-api/tasks.md (ticked 5.1 and 5.2 only)

## Self-review and concerns
- The reducer is still pure. `budget_status` and `build_final_result` are pure, and `das.client`/`das.ops` imports are allowed by the plan.
- In stop mode the Job's `reason` is `budget_exceeded`. `final_result.failures[].reason` still shows the Worker's `error` first when a result has one, because `build_final_result` prefers it. I did not change that module.
- `UnackedDeliveryExhausted` during a stop keeps its existing `fail(REASON_UNACKED_DELIVERY)` path. The brief does not cover it, and it causes no dispatch or retry. The Request then finalizes through the budget pass.
- `tests/property/test_budget_stop.py` changed on disk during the run. I checked the current content (header, `hevent` labels) and it passes under both profiles.
- Driver wiring (passing `price_table`, `include_usage=True`, the planning check, the metric) is task 5.3 and was not done here.
