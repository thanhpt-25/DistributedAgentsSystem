"""Property test for ``RequestMachine`` resume (task 12.6, Req 2.6).

``resume(snapshot, agents)`` is what a new lease owner runs before stepping a Request.
Per its contract it re-arms every persisted timer and re-emits ``PublishDispatch``
exactly for Jobs in Ready or Dispatched with no Job_Result for the current attempt,
except Ready Jobs holding a pending ``publish`` or ``capwait`` timer for that attempt
(those are re-driven by the timer or the next Tick). Waiting and terminal Jobs are never
re-dispatched. Each dispatch carries the persisted Succeeded contents of its
dependencies and routes to ``jobs/min(C)``, falling back to ``jobs/min(required)`` when
no routing Capability exists. Requests that are not Running yield nothing.

Snapshots are generated consistently: Jobs are assigned states in topological order, a
Job leaves Waiting only once all its dependencies Succeeded, every Succeeded/Failed/
TimedOut Job has a matching current-attempt result, and timers only exist where the
machine would have persisted them.
"""

# Feature: distributed-agent-system, Property 14: Resume re-dispatches exactly the unfinished in-flight Jobs

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    TIMER_CAPABILITY_WAIT,
    TIMER_PUBLISH,
    TIMER_RETRY,
    PublishDispatch,
    RequestSnapshot,
    ScheduleTimer,
    resume,
)
from das.manager.routing import choose_routing_capability
from das.models import (
    AgentRecord,
    JobRecord,
    JobResultPayload,
    JobState,
    RequestStatus,
    TokenUsage,
)
from das.plan.dag import topo_order
from das.state import Timer
from tests.strategies import capability_names, job_plans

S = JobState
RID = "r-resume"
IN_FLIGHT = (S.READY, S.DISPATCHED)
DEP_BLOCKED = (S.WAITING, S.SKIPPED, S.CANCELLED)
ANY_STATE = tuple(S)


def _result(job_id: str, attempt: int, status: str) -> JobResultPayload:
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}",
        status=status,  # type: ignore[arg-type]
        content=f"out {job_id}@{attempt}:{status}",
        error=None if status == "Succeeded" else "boom",
        usage=TokenUsage(),
        duration_ms=1,
    )


def _worker(agent_id: str, caps: list[str]) -> AgentRecord:
    return AgentRecord(
        agent_id=agent_id,
        role="worker",
        host="h",
        capabilities=frozenset(caps),
        status="Available",
        last_heartbeat=0.0,
    )


@st.composite
def resume_cases(draw: st.DrawFn) -> tuple[RequestSnapshot, tuple[AgentRecord, ...]]:
    pool = draw(st.lists(capability_names, min_size=1, max_size=4, unique=True))
    plan = draw(job_plans(max_jobs=8, capabilities=frozenset(pool)))
    by_id = {j.job_id: j for j in plan.jobs}
    order = topo_order(plan)

    jobs: dict[str, JobRecord] = {}
    results: dict[tuple[str, int], JobResultPayload] = {}
    timers: list[Timer] = []
    due = iter(range(10_000))

    for job_id in order:
        job = by_id[job_id]
        deps_ok = all(jobs[d].state is S.SUCCEEDED for d in job.depends_on)
        state = draw(st.sampled_from(ANY_STATE if deps_ok else DEP_BLOCKED))
        if state is S.WAITING:
            attempt = 0
        elif state in (S.SKIPPED, S.CANCELLED):
            attempt = draw(st.integers(0, 3))
        else:
            attempt = draw(st.integers(1, 3))

        # Earlier attempts failed or timed out (they led to the current attempt).
        for prev in range(1, attempt):
            results[(job_id, prev)] = _result(
                job_id, prev, draw(st.sampled_from(["Failed", "TimedOut"]))
            )

        if state is S.SUCCEEDED:
            results[(job_id, attempt)] = _result(job_id, attempt, "Succeeded")
        elif state in (S.FAILED, S.TIMED_OUT):
            status = "Failed" if state is S.FAILED else "TimedOut"
            results[(job_id, attempt)] = _result(job_id, attempt, status)
            if draw(st.booleans()):  # retry pending
                timers.append(Timer(TIMER_RETRY, job_id, attempt, float(next(due))))
        elif state in IN_FLIGHT and draw(st.booleans()):
            # A result recorded for the current attempt but not yet applied.
            status = draw(st.sampled_from(["Succeeded", "Failed", "TimedOut", "Cancelled"]))
            results[(job_id, attempt)] = _result(job_id, attempt, status)
        elif state is S.READY:
            kind = draw(st.sampled_from([None, TIMER_PUBLISH, TIMER_CAPABILITY_WAIT]))
            if kind is not None:
                timers.append(Timer(kind, job_id, attempt, float(next(due))))

        jobs[job_id] = JobRecord(
            job=job,
            state=state,
            attempt=attempt,
            ready_since=0.0 if state is S.READY else None,
        )

    status = draw(
        st.sampled_from(
            [RequestStatus.RUNNING] * 6
            + [s for s in RequestStatus if s is not RequestStatus.RUNNING]
        )
    )
    snapshot = RequestSnapshot(
        request_id=RID,
        status=status,
        prompt="the prompt",
        plan=plan,
        jobs=jobs,
        results=results,
        timers=tuple(draw(st.permutations(timers))),
    )
    worker_caps = draw(
        st.lists(st.lists(st.sampled_from(pool), min_size=1, unique=True), max_size=4)
    )
    agents = tuple(_worker(f"w{i}", caps) for i, caps in enumerate(worker_caps))
    return snapshot, agents


def _expected_dispatch_ids(snapshot: RequestSnapshot) -> set[str]:
    pending = {(t.kind, t.job_id, t.attempt) for t in snapshot.timers}
    expected = set()
    for job_id, rec in snapshot.jobs.items():
        if rec.state not in IN_FLIGHT or (job_id, rec.attempt) in snapshot.results:
            continue
        if rec.state is S.READY and (
            (TIMER_PUBLISH, job_id, rec.attempt) in pending
            or (TIMER_CAPABILITY_WAIT, job_id, rec.attempt) in pending
        ):
            continue
        expected.add(job_id)
    return expected


@settings(max_examples=200, deadline=None)
@given(resume_cases())
def test_resume_redispatches_exactly_unfinished_in_flight_jobs(case) -> None:
    """**Validates: Requirements 2.6**"""
    snapshot, agents = case
    effects = resume(snapshot, agents)

    if snapshot.status is not RequestStatus.RUNNING:
        assert effects == []
        return

    # Every persisted timer is re-armed, and nothing else is scheduled.
    armed = sorted(
        (e.kind, e.job_id, e.attempt, e.at) for e in effects if isinstance(e, ScheduleTimer)
    )
    assert armed == sorted((t.kind, t.job_id, t.attempt, t.due) for t in snapshot.timers)

    pubs = [e for e in effects if isinstance(e, PublishDispatch)]
    assert all(isinstance(e, (ScheduleTimer, PublishDispatch)) for e in effects)

    # Exactly the unfinished in-flight Jobs, each once.
    ids = [p.job_id for p in pubs]
    assert len(ids) == len(set(ids))
    assert set(ids) == _expected_dispatch_ids(snapshot)

    for pub in pubs:
        rec = snapshot.jobs[pub.job_id]
        # Never Waiting or terminal.
        assert rec.state in IN_FLIGHT
        assert pub.attempt == rec.attempt
        assert pub.instruction == rec.job.instruction
        assert pub.required_capabilities == tuple(rec.job.capabilities)
        # Dependency inputs are the persisted Succeeded contents, keyed by dep Job_ID.
        expected_inputs = {}
        for dep in rec.job.depends_on:
            dep_result = snapshot.results[(dep, snapshot.jobs[dep].attempt)]
            assert dep_result.status == "Succeeded"
            expected_inputs[dep] = dep_result.content
        assert dict(pub.dependency_results) == expected_inputs
        # Routing: jobs/min(C), or jobs/min(required) when no routing Capability exists.
        routed = choose_routing_capability(rec.job.capabilities, agents)
        assert pub.routing_cap == (routed if routed is not None else min(rec.job.capabilities))
        assert pub.routing_cap in rec.job.capabilities
        assert pub.destination == f"jobs/{pub.routing_cap}"
