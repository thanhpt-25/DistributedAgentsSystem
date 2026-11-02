"""Unit tests for das.plan.dag (Req 4.3, 4.4, 7.1)."""

from __future__ import annotations

import pytest

from das.models.job import JobState
from das.models.plan import JobPlan, PlannedJob
from das.plan.dag import (
    CycleError,
    all_terminal,
    ready_jobs,
    topo_order,
    transitive_dependents,
)

S = JobState


def plan(**deps: list[str]) -> JobPlan:
    """Build a plan from job_id=[dependency ids], preserving keyword order."""
    return JobPlan(
        jobs=[
            PlannedJob(job_id=j, instruction=f"do {j}", capabilities=["research"], depends_on=d)
            for j, d in deps.items()
        ]
    )


# a -> b -> d, a -> c -> d, e independent
DIAMOND = plan(d=["b", "c"], b=["a"], c=["a"], a=[], e=[])


class TestTopoOrder:
    def test_dependencies_precede_dependents(self) -> None:
        order = topo_order(DIAMOND)
        assert sorted(order) == ["a", "b", "c", "d", "e"]
        pos = {j: i for i, j in enumerate(order)}
        for job in DIAMOND.jobs:
            for dep in job.depends_on:
                assert pos[dep] < pos[job.job_id]

    def test_deterministic_ties_follow_plan_order(self) -> None:
        # Lowest plan index among available Jobs goes first (plan order: d, b, c, a, e).
        assert topo_order(DIAMOND) == ["a", "b", "c", "d", "e"]
        assert topo_order(plan(z=[], y=[], x=[])) == ["z", "y", "x"]

    def test_empty_plan(self) -> None:
        assert topo_order(JobPlan(jobs=[])) == []

    def test_unknown_dependency_ignored(self) -> None:
        assert topo_order(plan(a=["ghost"], b=["a"])) == ["a", "b"]

    def test_cycle_raises_with_members(self) -> None:
        with pytest.raises(CycleError) as exc:
            topo_order(plan(a=[], b=["a", "d"], c=["b"], d=["c"]))
        cycle = exc.value.cycle
        assert cycle[0] == cycle[-1]
        assert set(cycle) == {"b", "c", "d"}
        assert "->" in str(exc.value)

    def test_self_dependency_is_cycle(self) -> None:
        with pytest.raises(CycleError) as exc:
            topo_order(plan(a=["a"]))
        assert exc.value.cycle == ["a", "a"]


class TestReadyJobs:
    def test_roots_ready_initially(self) -> None:
        states = {j: S.WAITING for j in DIAMOND.job_ids()}
        assert ready_jobs(DIAMOND, states) == {"a", "e"}

    def test_requires_all_dependencies_succeeded(self) -> None:
        states = {"a": S.SUCCEEDED, "b": S.SUCCEEDED, "c": S.DISPATCHED, "d": S.WAITING,
                  "e": S.READY}
        assert ready_jobs(DIAMOND, states) == set()
        states["c"] = S.SUCCEEDED
        assert ready_jobs(DIAMOND, states) == {"d"}

    def test_only_waiting_jobs_returned(self) -> None:
        states = {"a": S.READY, "b": S.WAITING, "c": S.WAITING, "d": S.WAITING, "e": S.SKIPPED}
        assert ready_jobs(DIAMOND, states) == set()

    @pytest.mark.parametrize("dep_state", [S.FAILED, S.TIMED_OUT, S.SKIPPED, S.CANCELLED])
    def test_non_succeeded_dependency_blocks(self, dep_state: JobState) -> None:
        p = plan(a=[], b=["a"])
        assert ready_jobs(p, {"a": dep_state, "b": S.WAITING}) == set()

    def test_missing_states_not_ready(self) -> None:
        p = plan(a=[], b=["a"], c=["ghost"])
        assert ready_jobs(p, {"b": S.WAITING, "c": S.WAITING}) == set()


class TestTransitiveDependents:
    def test_diamond(self) -> None:
        assert transitive_dependents(DIAMOND, "a") == {"b", "c", "d"}
        assert transitive_dependents(DIAMOND, "b") == {"d"}
        assert transitive_dependents(DIAMOND, "d") == set()
        assert transitive_dependents(DIAMOND, "e") == set()

    def test_chain(self) -> None:
        p = plan(a=[], b=["a"], c=["b"], d=["c"], x=[])
        assert transitive_dependents(p, "a") == {"b", "c", "d"}

    def test_unknown_job(self) -> None:
        assert transitive_dependents(DIAMOND, "ghost") == set()

    def test_excludes_self_on_cycle(self) -> None:
        p = plan(a=["b"], b=["a"])
        assert transitive_dependents(p, "a") == {"b"}


class TestAllTerminal:
    def test_all_terminal_states(self) -> None:
        assert all_terminal({"a": S.SUCCEEDED, "b": S.FAILED, "c": S.SKIPPED, "d": S.CANCELLED})

    @pytest.mark.parametrize("state", [S.WAITING, S.READY, S.DISPATCHED, S.TIMED_OUT])
    def test_non_terminal_state(self, state: JobState) -> None:
        assert not all_terminal({"a": S.SUCCEEDED, "b": state})

    def test_empty_is_vacuously_true(self) -> None:
        assert all_terminal({})
