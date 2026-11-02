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


## Shared context for this group (from the plan header)

- Global Constraints and "How to read this plan": see the top of `docs/superpowers/plans/2026-10-30-das-client-api-budget.md` (read only the header up to the first `### Task`).
- Reference: `.kiro/specs/das-client-api/design.md` Components §4 "Reducer" (including "Budget-stop details") and Property 5; `requirements.md` Req 5.5-5.9 and the In_Flight_Job glossary entry.
