# Feature: distributed-agent-system, Property 11: Cancellation semantics
"""Property 11: Cancellation semantics.

For any Request with any Job state map:

- Pending or Running: ``cancel_request`` returns OK, sets the status to Cancelled,
  sets every Job not in {Succeeded, Failed, Skipped, Cancelled} to Cancelled (with
  one transition-log entry each), and leaves terminal Jobs unchanged (Req 1.6).
- Completed, Failed, or Cancelled: it returns ``TERMINAL`` with the current status
  and leaves the Request, its Jobs, and its Final_Result unchanged (Req 1.8).

Runs the real Lua scripts on a fresh in-process fakeredis server per example.

**Validates: Requirements 1.6, 1.8**
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given
from hypothesis import strategies as st

from das.models import (
    TERMINAL_JOB,
    FinalResult,
    JobPlan,
    JobReport,
    JobState,
    RequestStatus,
)
from das.state import CANCEL_REASON, StateStore, StoreStatus, TransitionBatch, keys
from tests.strategies import job_plans

ACTIVE = (RequestStatus.PENDING, RequestStatus.RUNNING)


@dataclass(frozen=True)
class Scenario:
    status: RequestStatus
    plan: JobPlan | None  # None: Request never got a plan
    job_states: tuple[JobState, ...]  # one per plan Job, in plan order
    keep_lease: bool  # Pending/Running only: whether a Manager still holds the Lease


@st.composite
def scenarios(draw: st.DrawFn) -> Scenario:
    status = draw(st.sampled_from(list(RequestStatus)))
    # A Pending Request has no plan (commit_plan moves it to Running); Failed may
    # happen before or after planning.
    has_plan = status is not RequestStatus.PENDING and (
        status is not RequestStatus.FAILED or draw(st.booleans())
    )
    plan = draw(job_plans(max_jobs=6)) if has_plan else None
    n = len(plan.jobs) if plan else 0
    states = tuple(draw(st.lists(st.sampled_from(list(JobState)), min_size=n, max_size=n)))
    return Scenario(status, plan, states, draw(st.booleans()))


def _final_result(plan: JobPlan | None) -> FinalResult:
    jobs = [JobReport(job_id=j.job_id, state=JobState.SUCCEEDED) for j in plan.jobs] if plan else []
    return FinalResult(summary="done", jobs=jobs)


async def _setup(store: StateStore, sc: Scenario) -> str:
    """Drive a fresh Request through the real store API into the scenario's state."""
    rid = await store.create_request("prompt")
    if sc.status is RequestStatus.PENDING and not sc.keep_lease:
        return rid
    lease = (await store.acquire_lease(rid, "m1", 300)).lease
    assert lease is not None
    if sc.plan is not None:
        assert (await store.commit_plan(lease, sc.plan)).ok
        records = (await store.load_snapshot(rid)).jobs
        updated = {
            jid: rec.model_copy(
                update={"state": state, "attempt": 1, "worker_id": "w1", "reason": f"r-{state}"}
            )
            for (jid, rec), state in zip(records.items(), sc.job_states, strict=True)
        }
        assert (await store.apply_transition(lease, TransitionBatch(jobs=updated))).ok

    if sc.status in ACTIVE:
        if not sc.keep_lease:
            assert (await store.release_lease(lease)).ok
    elif sc.status is RequestStatus.COMPLETED:
        assert (await store.finalize(lease, _final_result(sc.plan))).ok
    elif sc.status is RequestStatus.FAILED:
        assert (await store.fail_request(lease, "boom", _final_result(sc.plan))).ok
    else:  # Cancelled
        assert (await store.cancel_request(rid)).ok

    assert await store.get_status(rid) is sc.status
    return rid


async def _check(sc: Scenario) -> None:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        store = StateStore(client)
        rid = await _setup(store, sc)
        before = await store.load_snapshot(rid, include_transitions=True)

        res = await store.cancel_request(rid)
        after = await store.load_snapshot(rid, include_transitions=True)

        if sc.status not in ACTIVE:
            # Req 1.8: rejected as terminal; nothing changes.
            assert res.status is StoreStatus.TERMINAL
            assert res.request_status is sc.status
            assert after == before
            return

        # Req 1.6: Request Cancelled; non-terminal Jobs Cancelled; terminal Jobs untouched.
        assert res.status is StoreStatus.OK
        assert after.status is RequestStatus.CANCELLED
        assert after.final_result is None
        assert list(after.jobs) == list(before.jobs)
        expected_log = []
        for jid, old in before.jobs.items():
            new = after.jobs[jid]
            if old.state in TERMINAL_JOB:
                assert new == old
            else:
                assert new == old.model_copy(
                    update={"state": JobState.CANCELLED, "reason": CANCEL_REASON}
                )
                expected_log.append((jid, old.state, JobState.CANCELLED))
        assert all(r.state in TERMINAL_JOB for r in after.jobs.values())
        assert after.transitions[: len(before.transitions)] == before.transitions
        new_entries = after.transitions[len(before.transitions) :]
        assert sorted((t.job_id, t.from_state, t.to_state) for t in new_entries) == sorted(
            expected_log
        )
        # A cancelled Request is no longer waiting to be claimed.
        assert await client.zscore(keys.REQUESTS_PENDING, rid) is None

        # Cancelling again is now a terminal rejection that changes nothing (Req 1.8).
        again = await store.cancel_request(rid)
        assert again.status is StoreStatus.TERMINAL
        assert again.request_status is RequestStatus.CANCELLED
        assert await store.load_snapshot(rid, include_transitions=True) == after
    finally:
        await client.aclose()


@given(scenarios())
def test_cancellation_semantics(sc: Scenario) -> None:
    """**Validates: Requirements 1.6, 1.8**"""
    asyncio.run(_check(sc))


def test_cancel_unknown_request_is_not_found() -> None:
    async def run() -> None:
        client = FakeRedis(server=FakeServer(), decode_responses=True)
        try:
            store = StateStore(client)
            assert (await store.cancel_request("missing")).status is StoreStatus.NOT_FOUND
            assert await client.exists(*keys.request_keys("missing")) == 0
        finally:
            await client.aclose()

    asyncio.run(run())
