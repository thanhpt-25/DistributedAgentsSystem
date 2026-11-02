# Feature: distributed-agent-system, Property 12: Cancelled Requests are inert
"""Property 12: Cancelled Requests are inert.

For any snapshot whose Request status is Cancelled (arbitrary plan, Job state map,
attempts, stored results, persisted timers, registry agents, and time) and any event
(including Job_Results of every status, publish confirmations/failures, fired timers,
unacked-delivery dead-letters, ticks, and cancel observations), ``step`` returns an
empty Decision: no Job updates, no stored results, no timer changes, and no effects.
``resume`` likewise re-dispatches nothing and re-arms no timers.

Events are drawn mostly against the snapshot's own Job_IDs, current attempts, and
persisted timers so that the same event on a Running Request would usually act.

**Validates: Requirements 1.7**
"""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    TIMER_CAPABILITY_WAIT,
    TIMER_PUBLISH,
    TIMER_RETRY,
    CancelObserved,
    Decision,
    Event,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishFailed,
    RequestSnapshot,
    StepContext,
    Tick,
    TimerFired,
    UnackedDeliveryExhausted,
    resume,
    step,
)
from das.models import (
    AgentRecord,
    JobRecord,
    JobResultPayload,
    JobState,
    RequestStatus,
    TokenUsage,
)
from das.state.store import Timer
from tests.strategies import job_plans

RID = "req-cancelled"
TIMER_KINDS = (TIMER_RETRY, TIMER_PUBLISH, TIMER_CAPABILITY_WAIT)
RESULT_STATUSES = ("Succeeded", "Failed", "TimedOut", "Cancelled")
CAPS = frozenset({"research", "summarize", "code"})

times = st.floats(min_value=0, max_value=1e7, allow_nan=False, allow_infinity=False)
attempts = st.integers(min_value=0, max_value=5)
short_text = st.text(max_size=20)


def _result(draw: st.DrawFn, job_id: str, attempt: int) -> JobResultPayload:
    return JobResultPayload(
        request_id=draw(st.sampled_from([RID, RID, "other-request"])),
        job_id=job_id,
        attempt=max(attempt, 1),
        worker_id=draw(st.sampled_from(["w1", "w2"])),
        status=draw(st.sampled_from(RESULT_STATUSES)),
        content=draw(short_text),
        error=draw(st.none() | short_text.filter(bool)),
        usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2),
        duration_ms=draw(st.integers(min_value=0, max_value=10_000)),
    )


@st.composite
def cancelled_snapshots(draw: st.DrawFn) -> RequestSnapshot:
    plan = draw(job_plans(max_jobs=6, capabilities=CAPS))
    jobs: dict[str, JobRecord] = {}
    results: dict[tuple[str, int], JobResultPayload] = {}
    timers: dict[tuple[str, str, int], Timer] = {}
    for job in plan.jobs:
        state = draw(st.sampled_from(list(JobState)))
        attempt = draw(attempts)
        jobs[job.job_id] = JobRecord(
            job=job,
            state=state,
            attempt=attempt,
            ready_since=draw(st.none() | times),
            publish_failures=draw(st.integers(min_value=0, max_value=4)),
            worker_id=draw(st.none() | st.just("w1")),
            reason=draw(st.none() | short_text),
        )
        if attempt >= 1 and draw(st.booleans()):
            r = _result(draw, job.job_id, attempt)
            results[(r.job_id, r.attempt)] = r
        for kind in draw(st.lists(st.sampled_from(TIMER_KINDS), unique=True, max_size=2)):
            timers[(kind, job.job_id, attempt)] = Timer(
                kind=kind, job_id=job.job_id, attempt=attempt, due=draw(times)
            )
    return RequestSnapshot(
        request_id=RID,
        status=RequestStatus.CANCELLED,
        prompt=draw(short_text),
        plan=plan,
        jobs=jobs,
        results=results,
        timers=tuple(timers.values()),
    )


@st.composite
def events_for(draw: st.DrawFn, snap: RequestSnapshot) -> Event:
    job_ids = list(snap.jobs) + ["unknown-job"]
    job_id = draw(st.sampled_from(job_ids))
    record = snap.jobs.get(job_id)
    current = record.attempt if record is not None else 1
    attempt = draw(st.sampled_from([current, current + 1, max(current - 1, 1)]))
    kinds = ["plan", "tick", "result", "confirmed", "failed", "unacked", "cancel", "timer"]
    if snap.timers:
        kinds.append("persisted_timer")
    kind = draw(st.sampled_from(kinds))
    match kind:
        case "plan":
            return PlanCommitted()
        case "tick":
            return Tick()
        case "result":
            return JobResultReceived(result=_result(draw, job_id, attempt))
        case "confirmed":
            return PublishConfirmed(job_id=job_id, attempt=attempt)
        case "failed":
            return PublishFailed(
                job_id=job_id,
                attempt=attempt,
                n=draw(st.none() | st.integers(min_value=1, max_value=5)),
                error=draw(st.none() | short_text),
            )
        case "unacked":
            return UnackedDeliveryExhausted(job_id=job_id)
        case "cancel":
            return CancelObserved()
        case "timer":
            timer_kind = draw(st.sampled_from(TIMER_KINDS))
            return TimerFired(kind=timer_kind, job_id=job_id, attempt=attempt)
        case _:
            t = draw(st.sampled_from(snap.timers))
            return TimerFired(kind=t.kind, job_id=t.job_id, attempt=t.attempt)


@st.composite
def agent_sets(draw: st.DrawFn) -> tuple[AgentRecord, ...]:
    caps = draw(st.lists(st.sampled_from(sorted(CAPS)), unique=True, max_size=len(CAPS)))
    if not caps:
        return ()
    return (
        AgentRecord(
            agent_id="w1",
            role="worker",
            host="h",
            capabilities=frozenset(caps),
            status="Available",
            last_heartbeat=0.0,
        ),
    )


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[RequestSnapshot, Event, StepContext]:
    snap = draw(cancelled_snapshots())
    event = draw(events_for(snap))
    config = MachineConfig(
        max_attempts=draw(st.integers(min_value=1, max_value=4)),
        capability_wait=draw(st.floats(min_value=0.1, max_value=120)),
    )
    ctx = StepContext(now=draw(times), config=config, agents=draw(agent_sets()))
    return snap, event, ctx


@settings(max_examples=300)
@given(scenarios())
def test_cancelled_request_step_is_inert(
    scenario: tuple[RequestSnapshot, Event, StepContext],
) -> None:
    snap, event, ctx = scenario
    decision = step(snap, event, ctx)

    assert decision.is_empty
    assert decision == Decision()
    u = decision.updates
    assert not u.jobs
    assert not u.results
    assert not u.timers_add and not u.timers_remove
    assert not u.request_fields
    assert decision.effects == ()
    assert decision.changes == ()


@settings(max_examples=200)
@given(cancelled_snapshots(), agent_sets())
def test_cancelled_request_resume_is_inert(
    snap: RequestSnapshot, agents: tuple[AgentRecord, ...]
) -> None:
    assert resume(snap, agents) == []
