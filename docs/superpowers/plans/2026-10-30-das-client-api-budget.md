# DAS Client API (Phase 4) Manager Budget Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spec task group 5 in `.kiro/specs/das-client-api/tasks.md`: the Manager enforces per-Request budgets. Once Budget_Used reaches a limit, it stops dispatching, lets in-flight Jobs finish, and fails the Request with `budget_exceeded` or `budget_unenforceable`. The terminal event carries Budget_Used.

**Architecture:** Enforcement lives in the pure reducer `das/manager/machine.py`. It computes `das.client.budget.budget_status` from the snapshot's persisted UsageRecords plus the usage accepted in the current step. The decision is recomputed from persisted state on every step, so a Manager that takes over the Lease reaches the same decision (v1 Req 2.5). The driver supplies the Price_Table, loads snapshots with their usage, runs the pre-planning check, passes the budget view to `finalize`, and counts the metric.

Spec references:
- `requirements.md` Req 5.5–5.9 (In_Flight_Job glossary entry), Req 6.1, Req 10.2.
- `design.md` Components §4 "Reducer", including the "Budget-stop details" list, and Property 5.

**Tech Stack:** Python 3.12, pydantic 2.14.1, pytest 9.1.1, Hypothesis 6.170.8, fakeredis[lua] 2.40.0, uv.

**How to read this plan:** Tasks give exact interfaces, rules and test cases. Implementers write the code test-first. `design.md` §4 is the reference wherever a task points to it.

## Global Constraints

- Python `>=3.12,<3.13`; run everything through `uv run`. No new dependencies.
- The reducer stays pure: `step(snapshot, event, ctx) -> Decision`, with no I/O, clock or randomness. Time comes from `StepContext.now`.
- Requests without a budget behave exactly as before. Every existing reducer and driver test must pass unchanged.
- `das.manager` and `das.worker` keep the import-linter contract. `das.manager` may import `das.client` (pure) and `das.ops`.
- Property tests:
  - live in `tests/property/`;
  - first line is `# Feature: das-client-api, Property {n}: {title}`;
  - the docstring ends with `**Validates: Requirements …**`;
  - use the default Hypothesis profile (at least 100 examples).
- Lint with `uv run ruff check` (E, F; line length 120), type-check with `uv run mypy`, and check contracts with `uv run lint-imports`.
- Docstrings cite requirements as `Phase 4 Req X.Y`.
- This is not a git repository. Each task ends by ticking its checkboxes in `.kiro/specs/das-client-api/tasks.md` instead of committing.

---

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

### Task 2: Driver wiring, planning check and metric (spec task 5.3)

**Files:**
- Modify: `das/manager/driver.py`, `das/manager/agent.py`, `das/observability/metrics.py`
- Modify: `tests/fixtures/phase3_baseline/contract.json` (regenerated; metrics catalog only)
- Test: `tests/unit/test_manager_driver_budget.py`

**Interfaces:**
- Consumes: everything from Task 1, `das.client.budget.budget_view`, `StateStore.finalize(..., budget_view=...)` and `fail_request(..., budget_view=...)`.
- Produces:
  - `DriverDeps` gains `price_table: PriceTable | None = None`. `das/manager/agent.py` passes `settings.price_table`. `_step_context()` passes it into `StepContext`.
  - Every `self._store.load_snapshot(self.rid)` call in the driver passes `include_usage=True`, so `RequestSnapshot.usage` holds every persisted UsageRecord. There are four call sites today.
  - **Planning check (Req 5.5).** At the start of `_plan`, and only when `stored.plan is None`: if `stored.budget` has `max_cost` and `deps.price_table is None`, fail the Request with error `budget_unenforceable`, budget kind `unenforceable`. This happens before the Planner is called.
  - **Budget view on every terminal write (Req 6.1).** In `_write_final`, when the snapshot (or `stored`, for the planning path) has a budget, pass `budget_view(budget, budget_status((*snap.usage, *records), budget, deps.price_table))` to `finalize`. Requests without a budget pass nothing.
  - **Metric (Req 10.2).**
    - `ManagerMetrics.budget_exceeded(kind: ExceededKind)` increments `das_budget_exceeded_total{kind}`.
    - Add the catalog entry with label `kind` and allowed values `{"tokens", "cost", "unenforceable"}`.
    - The driver calls it once, only when the terminal write for a budget stop or the planning check answers OK. A `REPLAY_PENDING` or retried finalize must not double-count it.
    - `_finalize` reads `FinalizeFailed.budget_kind`.

- [ ] **Step 1: Write the failing tests** in `tests/unit/test_manager_driver_budget.py`. Follow `tests/unit/test_manager_driver.py` (`make_env`, `FakeWorker`, `wait_until`) and create budgeted Requests with `StateStore.submit_request`. Cases:
  - **Pre-planning check.** A `max_cost` budget with no Price_Table: the Request ends `Failed` with error `budget_unenforceable`, the Planner is never called, and the metric shows `kind="unenforceable"` exactly once.
  - **Budget hit mid-run.** A token budget hit after the first Job's result, with workers that return usage: the Request ends `Failed` with error `budget_exceeded`. The Summarizer is never called. Succeeded Job results stay queryable in `load_snapshot`. The last event in `das:req:{rid}:events` is `request.failed`, and its `data["budget"]` has `exceeded: true` and `used.total_tokens` equal to its `usage.total_tokens`. The metric shows `kind="tokens"` once.
  - **No budget.** The Request completes normally. Its Terminal_Event has no `budget` key, and the metric is not incremented.
  - **Budgeted Request that completes within budget.** Its `request.completed` event has `budget.exceeded == false`.
  - **Takeover.** Run until the budget stop starts with one Job still in flight, then steal the Lease (`steal_lease`) and start a second driver. The new owner reaches the same budget-stop state, dispatches nothing, and finalizes `Failed` with `budget_exceeded` after the in-flight result arrives.
  - **Metric not double-counted.** A finalize that first answers `REPLAY_PENDING` and then OK increments the metric once.

- [ ] **Step 2: Run the tests and confirm they fail.** Then **Step 3: implement.**

- [ ] **Step 4: Regenerate the Phase 3 contract baseline.** The metrics catalog gained one entry. Copy `contract.json` and `codec_corpus.jsonl` to `/tmp`, then run `uv run python -m tests.fixtures.phase3_baseline.capture`. Compare against the copies:
  - only `metrics` changed;
  - its only change is the added `das_budget_exceeded_total` entry;
  - the corpus is byte-identical.

  Delete the `/tmp` copies. If anything else changed, stop and report it.

- [ ] **Step 5: Run** the new tests with `-W error`, then all manager and agent suites: `uv run pytest tests/unit -k "manager or agent or driver or metrics or observability" -q` and `uv run pytest tests/unit/test_phase3_baseline.py -q`.

- [ ] **Step 6: Lint, type-check, contracts.** **Step 7: Tick** 5.3 and the parent `5. Manager budget enforcement`.

---

### Task 3: Checkpoint (spec task 6)

- [ ] **Step 1:** Run `uv run pytest -m "not integration" -q -p no:cacheprovider`. Expect `0 failed`, with only the 3 known warnings from `tests/unit/test_require_docker_hook.py`.
- [ ] **Step 2:** Run `uv run ruff check && uv run mypy && uv run lint-imports`.
- [ ] **Step 3:** Tick `6. Checkpoint - Ensure all tests pass`.
