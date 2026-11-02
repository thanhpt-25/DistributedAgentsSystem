# G5 Task 2 report: driver wiring, planning check and metric (spec task 5.3)

Status: DONE_WITH_CONCERNS (one existing test's hard-coded catalog count was changed; see Concerns)

## Files changed

- `das/manager/driver.py`
  - `DriverDeps.price_table: PriceTable | None = None`. `_step_context()` passes it into `StepContext`.
  - New `RequestDriver._load()` = `load_snapshot(self.rid, include_usage=True)`. All four former `load_snapshot` call sites (`_run`, `_plan` reload, `_reopen` reload, `_resync`) go through it.
  - Planning check (Phase 4 Req 5.5): at the start of `_plan`, after the `stored.plan is not None` reopen branch and before the registry snapshot and the Planner, a budget with `max_cost` and `deps.price_table is None` calls `_fail(BUDGET_UNENFORCEABLE, budget_kind="unenforceable")`. That goes through `_write_final` -> `finalize(Failed)`, the same path the other planning failures use. `_fail_code` is set to `budget_unenforceable` for the `manager.request` Span, the same way other planning failures set it.
  - `_write_final(..., budget_kind=None)`: computes `view = budget_view(budget, budget_status((*snap.usage, *records), budget, price_table))` once when `self._snap.budget` is set (otherwise `None`) and passes `budget_view=view` to `finalize`. On OK, after `_record_finished`, it calls `metrics.budget_exceeded(budget_kind)` when `budget_kind` is set. REPLAY_PENDING, exceptions, LEASE_LOST and REQUEST_TERMINAL never reach that line, so retries and REPLAY_PENDING cannot double-count.
  - `_finalize` passes `FinalizeFailed.budget_kind` through. `_fail` gained an optional `budget_kind`.
  - The module docstring gained a "Budget (Phase 4 Req 5.5, 5.7, 6.1, 10.2)" paragraph.
- `das/manager/agent.py`: `DriverDeps(..., price_table=settings.price_table)`. The `ManagerSettings.price_table` comment now mentions budgets.
- `das/observability/metrics.py`: `BudgetExceededKind` Literal (same values as `das.client.budget.ExceededKind`; defined locally so `das.observability` does not import `das.client`), `BUDGET_EXCEEDED_KINDS`, a catalog entry `das_budget_exceeded_total` (counter, label `kind`, allowed `{tokens, cost, unenforceable}`) appended at the end of `CATALOG`, and `ManagerMetrics.budget_exceeded(kind)`.
- `tests/unit/test_manager_driver_budget.py` (new, 9 tests).
- `tests/unit/test_metrics_recorder.py`: the catalog size assertion went from 26 to 27, with a comment.
- `tests/fixtures/phase3_baseline/contract.json`: regenerated (metrics only, see below).
- `.kiro/specs/das-client-api/tasks.md`: ticked 5.3 and the parent 5.

## Design decisions

- The planning-check budget view comes from `_write_final`, not from a separate call. `self._snap` is adopted from `stored` (with usage) before `_plan` runs, so the view is `budget_view(budget, budget_status((), budget, None))`, which gives `exceeded: true`, `used.total_tokens: 0` and `cost: null`. This matches the controller's instruction.
- Every terminal write of a budgeted Request carries the view: Completed, no-success Failed, summarization Failed, planning failures, and budget stops. Usage is the snapshot's persisted usage plus the records written by the same `finalize`. The view therefore agrees with the script's `usage.total_tokens` in the Terminal_Event, and the tests assert this.
- Takeover: `resume()` is unchanged, as the controller decided. The new owner may re-publish the Job that was in flight (same attempt). The test asserts that every dispatch after the takeover is for `c` with its original attempt, and a test comment explains why.

## Tests (`tests/unit/test_manager_driver_budget.py`)

1. The catalog entry exists (counter, `kind`, allowed set).
2. Pre-planning check: `max_cost` with no Price_Table ends Failed with `budget_unenforceable`. The Planner is never called and nothing is dispatched. The metric is `unenforceable == 1`. The last event is `request.failed` with the exact budget view.
3. Budget hit mid-run: limit = planner 2 + job 5 tokens. Only `a` is dispatched, `b` is Skipped with `budget_exceeded`, and the Summarizer is never called. The `a` result is still in `load_snapshot`. The last event is `request.failed` with `budget.exceeded` true and `used.total_tokens == usage.total_tokens`. The metric is `tokens == 1`.
4. No budget: Completed, no `budget` key in the Terminal_Event, metric 0.
5. Within budget: `request.completed` has `budget.exceeded == false` and used tokens equal to the usage total.
6. Extra: `max_cost` with a Price_Table covering the pairs completes, and `used.cost` is set. This proves the Price_Table reaches the reducer.
7. Takeover: a fan-in plan (`a`, `c` independent, `b` depends on both) with the worker holding. The `a` result hits the limit, so `b` is Skipped while `c` is still in flight. Then the Lease is stolen and a second `RequestDrivers` starts. The old driver ends LEASE_LOST. The new driver stays Running and does not finish until the `c` result arrives, then finalizes Failed with `budget_exceeded`. No dispatch other than a re-publish of `c` at the same attempt, no Summarizer, metric `tokens == 1`.
8. REPLAY_PENDING then OK: finalize is called 2 times and the metric counts once.
9. `ManagerAgent` passes `settings.price_table` into `DriverDeps`.

TDD: all 9 tests failed before the implementation (KeyError on the catalog, then `DriverDeps` had no `price_table`). Mutation check: dropping `include_usage=True` makes 3 tests fail (mid-run, within-budget, takeover). I restored it afterwards.

## Commands and results

- `uv run pytest tests/unit/test_manager_driver_budget.py -q -W error -p no:cacheprovider`: `9 passed in 0.77s` (3 more runs: 9 passed each).
- `uv run pytest tests/unit -k "manager or agent or driver or metrics or observability" -q -p no:cacheprovider`: `367 passed, 1494 deselected in 12.64s`.
- `uv run pytest tests/unit/test_phase3_baseline.py -q -p no:cacheprovider`: `6 passed in 0.28s`.
- `uv run pytest -m "not integration" -q -p no:cacheprovider`: `2267 passed, 24 deselected, 3 warnings in 172.07s`. The 3 warnings are the known ones from `tests/unit/test_require_docker_hook.py` (that file alone gives `3 passed, 3 warnings`).
- `uv run ruff check`: `All checks passed!`
- `uv run mypy`: `Success: no issues found in 107 source files`
- `uv run lint-imports`: `Contracts: 1 kept, 0 broken.`

## Baseline diff (brief Step 4)

The copies went in `/tmp/das-g5t2-baseline` and were deleted afterwards.
- `codec_corpus.jsonl`: byte-identical (`cmp` clean).
- `contract.json`: same top-level keys. Only `metrics` changed: no entries removed, every existing entry unchanged, existing order unchanged. The one addition is `{"allowed": {"kind": ["cost","tokens","unenforceable"]}, "buckets_key": null, "help": "Requests failed by a budget stop, by exceeded kind.", "labels": ["kind"], "name": "das_budget_exceeded_total", "type": "counter"}`, appended at the end.

## Concerns

- `tests/unit/test_metrics_recorder.py::test_catalog_names_and_labels_are_allowed` hard-codes the catalog size. Adding the metric the brief requires makes it fail, so I changed 26 to 27. No other existing test was modified.
- The metric help text does not cite the requirement (the other catalog help strings don't either). The citation is in the `ManagerMetrics.budget_exceeded` docstring.
