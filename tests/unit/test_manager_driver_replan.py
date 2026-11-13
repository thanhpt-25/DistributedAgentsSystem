"""The driver replans instead of finishing a Request that lost Jobs (das-smarter-agents task 3.4; Phase 6 Req 3.2, 3.5,
3.6, 3.8). Real driver, State_Store and in-memory broker, a scripted Planner, and a Worker that fails chosen Jobs."""

from __future__ import annotations

import asyncio
import dataclasses
import json

import pytest

from das.client.budget import Budget
from das.manager.driver import DriverDeps, DriverOutcome, RequestDriver
from das.manager.machine import BUDGET_EXCEEDED, MachineConfig
from das.models import JobState, RequestStatus, TokenUsage
from das.models.usage import UsageRecord
from das.state import StateStore, StoreStatus, WriteResult
from tests.unit.test_manager_driver import ONE_JOB, TIMEOUT, TWO_JOBS, make_env
from tests.unit.test_manager_driver_budget import PLANNER_TOKENS, WORKER_TOKENS, _events, _submit_and_claim

A = {"job_id": "a", "instruction": "find facts", "capabilities": ["search"]}
B = {"job_id": "b", "instruction": "write", "capabilities": ["write"], "depends_on": ["a"]}


def plan(*jobs: dict) -> str:
    return json.dumps({"jobs": list(jobs)})


def c(job_id: str, *deps: str) -> dict:
    return {"job_id": job_id, "instruction": f"write {job_id} differently", "capabilities": ["write"],
            "depends_on": list(deps)}


REPLAN_AC = plan(A, c("c", "a"))
REPLAN_AD = plan(A, c("d", "a"))
BAD = plan({"job_id": "x", "instruction": "paint", "capabilities": ["paint"]})  # a Capability nobody has
ONE_ATTEMPT = MachineConfig(max_attempts=1)


def with_replans(n: int, planning_retries: int = 0):  # noqa: ANN201
    """The harness plans with no retries; a test may give the Planner more attempts."""

    def tweak(deps: DriverDeps) -> DriverDeps:
        config = dataclasses.replace(deps.planner_config, max_replans=n, planning_retries=planning_retries)
        return dataclasses.replace(deps, planner_config=config)

    return tweak


async def run(  # noqa: ANN201
    store: StateStore, redis, plans: tuple[str, ...], fails: dict[str, str], replans: int, retries: int = 0, **kw  # noqa: ANN001
):
    tweak = with_replans(replans, retries)
    env = await make_env(store, redis, plan_outcomes=plans, machine_config=ONE_ATTEMPT, tweak=tweak, **kw)
    env.worker.statuses = fails
    rid, task = await env.submit_and_claim()
    outcome = await asyncio.wait_for(task, TIMEOUT)
    stored = await store.load_snapshot(rid, include_usage=True)
    assert stored is not None
    return env, rid, outcome, stored


def plan_usages(stored) -> int:  # noqa: ANN001
    return sum(1 for r in (stored.usage or []) if r.operation == "plan")


# --------------------------------------------------------------------------- the replan (Req 3.2, 3.4, 3.6)


async def test_a_partial_failure_is_replanned_and_the_request_completes(state_store: StateStore, fake_redis) -> None:
    env, rid, outcome, stored = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert outcome is DriverOutcome.COMPLETED and stored.status is RequestStatus.COMPLETED
    assert stored.replans == 1 and stored.plan is not None and stored.plan.job_ids() == ["a", "c"]
    assert {j: r.state for j, r in stored.jobs.items()} == {"a": JobState.SUCCEEDED, "c": JobState.SUCCEEDED}
    assert [m.payload.job_id for m in env.worker.dispatches] == ["a", "b", "c"]  # a ran once; its result was kept
    assert stored.final_result is not None and [j.job_id for j in stored.final_result.jobs] == ["a", "c"]


async def test_the_new_job_receives_the_kept_jobs_result(state_store: StateStore, fake_redis) -> None:
    env, _, _, _ = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert env.worker.dispatches[-1].payload.dependency_results == {"a": "out-a"}


async def test_the_planner_is_told_what_succeeded_and_what_failed(state_store: StateStore, fake_redis) -> None:
    env, _, _, _ = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert len(env.planner_llm.calls) == 2
    prompt = env.planner_llm.calls[1].messages[0].content
    assert "out-a" in prompt and "boom" in prompt and '"b"' in prompt


async def test_the_summary_covers_the_replanned_plan_only(state_store: StateStore, fake_redis) -> None:
    env, _, _, _ = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    prompt = "\n".join(m.content for m in env.summary_llm.calls[0].messages)
    assert "out-a" in prompt and "out-c" in prompt


async def test_a_replanned_event_is_in_the_stream_once(state_store: StateStore, fake_redis) -> None:
    _, rid, _, _ = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    replanned = [data for kind, data in await _events(state_store, rid) if kind == "request.replanned"]
    assert replanned == [{"replans": 1, "failed_jobs": ["b"], "new_jobs": ["c"]}]
    assert (await _events(state_store, rid))[-1][0] == "request.completed"  # and the Request still ends normally


async def test_both_planning_calls_are_recorded_as_usage(state_store: StateStore, fake_redis) -> None:
    _, _, _, stored = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert plan_usages(stored) == 2


async def test_usage_that_was_still_unpersisted_is_written_with_the_replan_and_only_once(
    state_store: StateStore, fake_redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A finalize answered REPLAY_PENDING leaves usage in the driver until the next write (Phase 2 Req 9.3). The replan
    is that write; the final write after it must not write the same records again."""
    carried = UsageRecord(
        operation="summarize", provider="p", model="carry-marker",
        usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2),
    )
    original = RequestDriver._adopt

    def seeded(self: RequestDriver, stored) -> None:  # noqa: ANN001
        original(self, stored)
        if not getattr(self, "_seeded", False):
            self._seeded = True  # type: ignore[attr-defined]
            self._carry_usage = [carried]

    monkeypatch.setattr(RequestDriver, "_adopt", seeded)
    _, _, outcome, stored = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 1
    assert sum(1 for r in (stored.usage or []) if r.model == "carry-marker") == 1


async def test_a_request_where_nothing_succeeded_is_replanned_too(state_store: StateStore, fake_redis) -> None:
    env, rid, outcome, stored = await run(
        state_store, fake_redis, (ONE_JOB, plan({"job_id": "d", "instruction": "again", "capabilities": ["search"]})),
        {"a": "Failed"}, 1,
    )
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 1
    assert stored.plan is not None and stored.plan.job_ids() == ["d"]
    assert [m.payload.job_id for m in env.worker.dispatches] == ["a", "d"]
    replanned = [data for kind, data in await _events(state_store, rid) if kind == "request.replanned"]
    assert replanned == [{"replans": 1, "failed_jobs": ["a"], "new_jobs": ["d"]}]


# --------------------------------------------------------------------------- off by default, and bounded (Req 3.1)


async def test_without_max_replans_the_request_finishes_exactly_as_before(state_store: StateStore, fake_redis) -> None:
    env, rid, outcome, stored = await run(state_store, fake_redis, (TWO_JOBS,), {"b": "Failed"}, 0)
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 0
    assert len(env.planner_llm.calls) == 1
    assert stored.jobs["b"].state is JobState.FAILED  # the partial result stands
    assert "request.replanned" not in [kind for kind, _ in await _events(state_store, rid)]


async def test_a_request_where_every_job_succeeded_is_never_replanned(state_store: StateStore, fake_redis) -> None:
    env, _, outcome, stored = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {}, 3)
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 0 and len(env.planner_llm.calls) == 1


async def test_the_limit_stops_the_replanning(state_store: StateStore, fake_redis) -> None:
    """One replan allowed; the replacement fails too, so the Request ends with what it has."""
    env, _, outcome, stored = await run(
        state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed", "c": "Failed"}, 1
    )
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 1 and len(env.planner_llm.calls) == 2
    assert stored.jobs["c"].state is JobState.FAILED and stored.jobs["a"].state is JobState.SUCCEEDED


async def test_a_second_replan_is_made_when_allowed(state_store: StateStore, fake_redis) -> None:
    env, rid, outcome, stored = await run(
        state_store, fake_redis, (TWO_JOBS, REPLAN_AC, REPLAN_AD), {"b": "Failed", "c": "Failed"}, 2
    )
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 2
    assert stored.plan is not None and stored.plan.job_ids() == ["a", "d"]
    counts = [data["replans"] for kind, data in await _events(state_store, rid) if kind == "request.replanned"]
    assert counts == [1, 2]


# ------------------------------------------------------------------------- when replanning itself fails (Req 3.5, 3.8)


async def test_a_replan_that_cannot_be_made_leaves_the_request_ending_as_it_would_have(
    state_store: StateStore, fake_redis
) -> None:
    env, _, outcome, stored = await run(state_store, fake_redis, (TWO_JOBS, BAD, BAD, BAD), {"b": "Failed"}, 1)
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 0
    assert stored.plan is not None and stored.plan.job_ids() == ["a", "b"]  # untouched
    assert stored.jobs["b"].state is JobState.FAILED


async def test_the_tokens_of_a_failed_replan_still_count(state_store: StateStore, fake_redis) -> None:
    env, _, _, stored = await run(state_store, fake_redis, (TWO_JOBS, BAD, BAD, BAD), {"b": "Failed"}, 1, retries=2)
    assert len(env.planner_llm.calls) == 1 + 3  # the plan, and three attempts at a replan (two retries)
    assert plan_usages(stored) == 1 + 3  # every one of them is recorded, though the replan was never made


async def test_a_request_that_failed_and_cannot_be_replanned_fails_with_its_own_error(
    state_store: StateStore, fake_redis
) -> None:
    _, _, outcome, stored = await run(state_store, fake_redis, (ONE_JOB, BAD, BAD, BAD), {"a": "Failed"}, 1, retries=2)
    assert outcome is DriverOutcome.FAILED and stored.status is RequestStatus.FAILED
    assert stored.replans == 0 and stored.error is not None and "replan" not in stored.error.lower()
    assert plan_usages(stored) == 4


# ------------------------------------------------------------------------- when the replan cannot be written (Req 3.5)


def store_answers(monkeypatch: pytest.MonkeyPatch, status: StoreStatus, detail: str | None = None) -> None:
    async def answer(self, lease, plan, usage=(), **kw):  # noqa: ANN001, ANN202
        return WriteResult(status, detail=detail)

    monkeypatch.setattr(StateStore, "replan", answer)


async def test_a_replan_the_script_refuses_leaves_the_request_ending_as_it_would_have(
    state_store: StateStore, fake_redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_answers(monkeypatch, StoreStatus.INVALID_STATE, "jobs_active")
    env, _, outcome, stored = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert outcome is DriverOutcome.COMPLETED and stored.replans == 0
    assert stored.plan is not None and stored.plan.job_ids() == ["a", "b"] and len(env.planner_llm.calls) == 2
    assert plan_usages(stored) == 2  # the replan's tokens were spent, so they are recorded with the final write


async def test_a_lease_lost_while_replanning_stops_the_driver_without_finishing_the_request(
    state_store: StateStore, fake_redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_answers(monkeypatch, StoreStatus.LEASE_LOST)
    _, _, outcome, stored = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert outcome is DriverOutcome.LEASE_LOST
    assert stored.status is RequestStatus.RUNNING  # the new owner decides, not this one


async def test_a_request_cancelled_while_replanning_ends_cancelled(
    state_store: StateStore, fake_redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_answers(monkeypatch, StoreStatus.REQUEST_TERMINAL)
    _, _, outcome, _ = await run(state_store, fake_redis, (TWO_JOBS, REPLAN_AC), {"b": "Failed"}, 1)
    assert outcome is DriverOutcome.CANCELLED


# --------------------------------------------------------------------------- a budget stop is never replanned (Req 3.8)


async def test_a_budget_stop_is_never_replanned(state_store: StateStore, fake_redis) -> None:
    env = await make_env(state_store, fake_redis, tweak=with_replans(2))
    rid, task = await _submit_and_claim(env, Budget(max_total_tokens=PLANNER_TOKENS + WORKER_TOKENS))
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.error == BUDGET_EXCEEDED and stored.replans == 0
    assert len(env.planner_llm.calls) == 1  # the Planner was never asked again
