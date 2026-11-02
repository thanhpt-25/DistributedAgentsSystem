# Feature: distributed-agent-system, Property 10: Query response shape
"""Property 10: Query response shape.

For any stored Request (any status, any Job set, any transitions), ``GET
/v1/requests/{id}`` returns the Request status, one ``(job_id, state)`` entry per
Job in plan order (empty when no plan exists), and the transition entries in
chronological order. It contains ``final_result`` if and only if the status is
Completed (Req 1.2, 13.9).

Requests are driven into each state through the real StateStore API (real Lua
scripts) on a fresh in-process fakeredis server per example, then queried through
the real FastAPI app over ``httpx.ASGITransport``.

**Validates: Requirements 1.2, 13.9**
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given
from hypothesis import strategies as st

from das.gateway.app import create_app
from das.models import FinalResult, JobPlan, JobReport, JobState, RequestStatus
from das.registry import AgentRegistry
from das.state import StateStore, TransitionBatch
from tests.strategies import job_plans

KEY = "test-api-key-123"
AUTH = {"X-API-Key": KEY}
ACTIVE = (RequestStatus.PENDING, RequestStatus.RUNNING)


@dataclass(frozen=True)
class Scenario:
    status: RequestStatus
    plan: JobPlan | None  # None: Request never got a plan
    rounds: tuple[tuple[JobState, ...], ...]  # transition batches, one state per plan Job
    keep_lease: bool  # Pending only: whether a Manager has claimed it
    summary: str


@st.composite
def scenarios(draw: st.DrawFn) -> Scenario:
    status = draw(st.sampled_from(list(RequestStatus)))
    # Pending has no plan (commit_plan moves it to Running); Failed may fail before planning.
    has_plan = status is not RequestStatus.PENDING and (
        status is not RequestStatus.FAILED or draw(st.booleans())
    )
    plan = draw(job_plans(max_jobs=6)) if has_plan else None
    n = len(plan.jobs) if plan else 0
    state_row = st.lists(st.sampled_from(list(JobState)), min_size=n, max_size=n).map(tuple)
    rounds = tuple(draw(st.lists(state_row, max_size=3))) if plan else ()
    summary = draw(st.text(min_size=1, max_size=30))
    return Scenario(status, plan, rounds, draw(st.booleans()), summary)


def _final_result(sc: Scenario) -> FinalResult:
    jobs = [JobReport(job_id=j.job_id, state=JobState.SUCCEEDED) for j in sc.plan.jobs] if sc.plan else []
    return FinalResult(summary=sc.summary, jobs=jobs)


async def _setup(store: StateStore, sc: Scenario) -> str:
    """Drive a fresh Request through the real store API into the scenario's state."""
    rid = await store.create_request("prompt")
    if sc.status is RequestStatus.PENDING and not sc.keep_lease:
        return rid
    lease = (await store.acquire_lease(rid, "m1", 300)).lease
    assert lease is not None
    if sc.plan is not None:
        assert (await store.commit_plan(lease, sc.plan)).ok
        for i, row in enumerate(sc.rounds):
            records = (await store.load_snapshot(rid)).jobs
            updated = {
                jid: rec.model_copy(
                    update={"state": state, "attempt": i + 1, "worker_id": f"w{i}", "reason": f"r-{state}"}
                )
                for (jid, rec), state in zip(records.items(), row, strict=True)
            }
            assert (await store.apply_transition(lease, TransitionBatch(jobs=updated))).ok

    if sc.status in ACTIVE:
        assert (await store.release_lease(lease)).ok
    elif sc.status is RequestStatus.COMPLETED:
        assert (await store.finalize(lease, _final_result(sc))).ok
    elif sc.status is RequestStatus.FAILED:
        assert (await store.fail_request(lease, "boom", _final_result(sc))).ok
    else:  # Cancelled
        assert (await store.cancel_request(rid)).ok

    assert await store.get_status(rid) is sc.status
    return rid


async def _check(sc: Scenario) -> None:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        store = StateStore(client)
        rid = await _setup(store, sc)
        snap = await store.load_snapshot(rid, include_transitions=True)

        app = create_app(store, AgentRegistry(client), None, [KEY], max_prompt_length=100)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as http:
            r = await http.get(f"/v1/requests/{rid}", headers=AUTH)
        assert r.status_code == 200
        body = r.json()

        assert body["request_id"] == rid
        assert body["status"] == sc.status.value == snap.status.value

        # One (job_id, state) entry per Job, in plan order; empty when no plan exists.
        plan_ids = [j.job_id for j in sc.plan.jobs] if sc.plan else []
        assert [j["job_id"] for j in body["jobs"]] == plan_ids
        assert body["jobs"] == [
            {"job_id": jid, "state": rec.state.value} for jid, rec in snap.jobs.items()
        ]
        if sc.plan is None:
            assert body["jobs"] == []

        # Transitions match the stored log exactly and are chronologically ordered.
        expected = [t.model_dump(mode="json") for t in (snap.transitions or [])]
        assert body["transitions"] == expected
        times = [datetime.fromisoformat(t["at"].replace("Z", "+00:00")) for t in body["transitions"]]
        assert times == sorted(times)

        # final_result present iff Completed.
        if sc.status is RequestStatus.COMPLETED:
            assert body["final_result"] == _final_result(sc).model_dump(mode="json")
        else:
            assert "final_result" not in body

        if snap.error:
            assert body["error"] == snap.error
        else:
            assert "error" not in body
    finally:
        await client.aclose()


@given(scenarios())
def test_query_response_shape(sc: Scenario) -> None:
    """**Validates: Requirements 1.2, 13.9**"""
    asyncio.run(_check(sc))
