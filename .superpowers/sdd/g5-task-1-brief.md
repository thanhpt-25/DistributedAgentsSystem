### Task 1: Budget stop in the reducer (spec tasks 5.1, 5.2)

**Files:**
- Modify: `das/manager/machine.py`, `das/state/store.py` (`StoredRequest`, `load_snapshot`)
- Test: `tests/unit/test_manager_machine_budget.py`, `tests/property/test_budget_stop.py`

**Interfaces:**
- Consumes:
  - `das.client.budget.Budget`, `BudgetStatus`, `ExceededKind`, `budget_status(usage, budget, prices)`
  - `das.ops.cost.PriceTable`
  - `das.manager.final_result.build_final_result`
- Produces:
  - `StoredRequest` gains `budget: Budget | None = None` and `client_fp: str | None = None`. `load_snapshot` parses them from the request hash fields `F_BUDGET` (via `Budget.from_json_obj(json.loads(...))`) and `F_CLIENT_FP`. Both stay `None` when absent.
  - `RequestSnapshot` gains `budget: Budget | None = None`. `RequestSnapshot.from_stored` copies `budget` and `usage` (`tuple(stored.usage or ())`). `apply_decision` keeps `budget` unchanged.
  - `StepContext` gains `price_table: PriceTable | None = None`.
  - Constants: `BUDGET_EXCEEDED: Final = "budget_exceeded"` and `BUDGET_UNENFORCEABLE: Final = "budget_unenforceable"`.
  - `FinalizeFailed` gains `budget_kind: ExceededKind | None = None`. It is set only for a budget stop.
  - `budget_exceeded(snapshot, ctx) -> ExceededKind | None`: a pure helper over `snapshot.usage`, used by tests and the driver.

**Rules** (design §4 and its "Budget-stop details"; In_Flight_Job is as defined in the requirements glossary):
- `_Work` gets a method that returns `budget_status(snap.usage + new_usage, budget, ctx.price_table).exceeded`.
  - It returns `None` when `snap.budget` is `None`.
  - It must reflect usage accepted earlier in the same step.
- While exceeded (budget-stop mode):
  - **Accepted result.** `_on_result` records it and its usage as today. If the result is Failed or TimedOut, the Job becomes `FAILED` with reason `budget_exceeded`: no retry timer and no `PublishDeadLetter`. The check runs after this result's usage is added, so the result that crosses the limit is itself not retried.
  - **Retry timer.** `_on_timer` with `TIMER_RETRY` on a Failed or TimedOut Job: remove the timer and set the Job `FAILED` with reason `budget_exceeded`. Do not call `enter_ready`.
  - **Publish timer.** `_on_timer` with `TIMER_PUBLISH`: remove the timer only. `_settle` then skips the Job.
  - **Publish failure.** `_on_publish_failed`: set the Job `SKIPPED` with reason `budget_exceeded` and remove its timers.
  - **Replay.** `_on_replay`: revert with violation `budget exceeded`.
  - **`_settle` in budget-stop mode.** It replaces the capability-wait, failure-propagation and readiness passes with this pass, in plan order:
    - a `WAITING` Job becomes `SKIPPED` with reason `budget_exceeded`;
    - a `READY` Job that is not an In_Flight_Job (pending `publish` or `capwait` timer) becomes `SKIPPED` with reason `budget_exceeded`, and its timers are removed;
    - a `FAILED` or `TIMED_OUT` Job awaiting a retry becomes `FAILED` with reason `budget_exceeded`, and its retry timer is removed.
  - Then, when every Job `is_settled`, emit `FinalizeFailed(final_result=build_final_result(plan, jobs, results, None, error), budget_kind=kind)`, where `error` is `budget_unenforceable` for kind `unenforceable` and `budget_exceeded` otherwise. Never emit `RunSummarizer`.
  - No `PublishDispatch` is emitted anywhere.
- A Job may still change state at most once per step (`_Work.put` enforces this). The rules above keep each path to one change.
- `CancelObserved` and non-Running snapshots are unchanged.

- [ ] **Step 1: Write the failing unit tests** in `tests/unit/test_manager_machine_budget.py`. Use the `step`/`apply_decision` harness style of `tests/property/test_dispatch_readiness.py`. Build `UsageRecord`s with `das.models.usage.UsageRecord` and results with a `TokenUsage`. Each case:
  - **No budget.** A two-Job chain runs to `RunSummarizer` exactly as before.
  - **Planning usage alone exceeds the budget.** The snapshot has usage of 100 tokens and the budget is `max_total_tokens=50`. On `PlanCommitted`: no dispatch, every Job is `SKIPPED` with reason `budget_exceeded`, and the decision has `FinalizeFailed` with `budget_kind == "tokens"` and `final_result.error == "budget_exceeded"`.
  - **Mid-run stop.** Jobs `a` and `b` run in parallel, and `c` depends on both. A Succeeded result for `a` whose usage crosses the limit: `b`, which is Dispatched, stays in flight, and `c` becomes `SKIPPED`. There is no FinalizeFailed yet. Then `b`'s result: FinalizeFailed, never RunSummarizer.
  - **Failed result that crosses the limit.** It becomes `FAILED` with reason `budget_exceeded`. There is no retry `ScheduleTimer` and no `PublishDeadLetter`, even when attempts remain.
  - **Retry pending when the stop starts.** The Job becomes `FAILED` with reason `budget_exceeded` on the next step, and the retry timer is in `timers_remove`.
  - **Ready Job with a capwait timer.** It becomes `SKIPPED`, and its timer is removed.
  - **Replay during the stop.** It is reverted. The decision has `dead_letter_reverts`, and the warning mentions `budget exceeded`.
  - **Unenforceable.** `max_cost` with `ctx.price_table=None` gives `budget_kind == "unenforceable"` and error `budget_unenforceable`. So does `max_cost` with a Price_Table that lacks the result's provider and model.
  - **Cost within budget.** `max_cost` with a matching Price_Table and usage below the limit runs normally.
  - **`from_stored`.** It copies `budget` and `usage`.
  - **`load_snapshot`.** On fakeredis, `load_snapshot` of a Request created with `submit_request(..., ClientRequestSpec(fingerprint=..., budget=Budget(max_total_tokens=5)))` has `budget == Budget(max_total_tokens=5)` and `client_fp` set. A Request from `create_request` has both `None`.

- [ ] **Step 2: Run the tests and confirm they fail.** Then **Step 3: implement.**

- [ ] **Step 4: Write the property test** `tests/property/test_budget_stop.py`, for **Property 5: Budget stop** (**Validates: Requirements 5.6, 5.7, 5.8**).
  - Draw:
    - a plan from `tests.strategies.job_plans(max_jobs=6)`;
    - a budget: `max_total_tokens` from 1..300, or `max_cost` with a one-entry Price_Table, sometimes with a `None` Price_Table;
    - optional planning usage;
    - a sequence of up to 40 reducer events driven by a model Worker:
      - results with random status and random token usage for In_Flight_Jobs;
      - `PublishConfirmed` and `PublishFailed`;
      - firing due timers;
      - `Tick`.
  - Apply each decision with `apply_decision`, and track the step at which `budget_exceeded` first becomes non-`None`, together with the set of In_Flight_Jobs at that moment.
  - Assert:
    - after that step, no decision contains `PublishDispatch`, and no `ScheduleTimer` of kind `retry`;
    - final Budget_Used minus Budget_Used at that step is at most the usage carried by results of Jobs that were In_Flight_Jobs at that step;
    - `FinalizeFailed` with `budget_kind` set is emitted exactly when no Job is in flight any more;
    - `RunSummarizer` is never emitted after the stop;
    - every `PublishDispatch` before the stop is unchanged from today's behaviour. Check this by running the same events with `budget=None` up to the stop step and comparing the dispatch sets.

- [ ] **Step 5: Run** the new tests with `-W error` and the property test under both profiles. Then run every reducer suite: `uv run pytest tests/unit -k "machine or manager" -q`, plus `tests/property/test_dispatch_readiness.py`, `test_attempt_progression.py`, `test_failure_propagation.py`, `test_machine_replay.py`, `test_machine_cancelled.py` and `test_final_result.py`.

- [ ] **Step 6: Lint, type-check, contracts.** **Step 7: Tick** 5.1 and 5.2.

---


## Shared context for this group (from the plan header)

- Global Constraints and "How to read this plan": see the top of `docs/superpowers/plans/2026-10-30-das-client-api-budget.md` (read only the header up to the first `### Task`).
- Reference: `.kiro/specs/das-client-api/design.md` Components §4 "Reducer" (including "Budget-stop details") and Property 5; `requirements.md` Req 5.5-5.9 and the In_Flight_Job glossary entry.
