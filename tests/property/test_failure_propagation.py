"""Property test for failure propagation (task 12.8).

The Request is driven through the real reducer (``step`` + ``apply_decision``) to a
reachable mid-execution state: a random DAG, a random number of Jobs Succeeded. Then one
in-flight Job ``X`` is made to fail in one of the ways that end a Job in terminal Failed:

- a Failed/TimedOut Job_Result on the final attempt (Req 6.3),
- ``UnackedDeliveryExhausted`` (Req 6.6),
- the 4th unconfirmed publish (Req 4.8),
- the capability-wait timeout (Req 4.6).

Two contrast cases are also drawn: a Failed result below ``max_attempts`` (a retry is
pending, so ``X`` is not terminal and nothing is Skipped) and a Cancelled result (the
documented extension: dependents are Skipped with ``skipped: dependency X cancelled``).

Oracle: ``das.plan.dag.transitive_dependents``.
"""

# Feature: distributed-agent-system, Property 16: Failure propagation skips exactly the transitive dependents

from __future__ import annotations

import hypothesis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    TIMER_PUBLISH,
    Decision,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishDispatch,
    PublishFailed,
    RequestSnapshot,
    StepContext,
    Tick,
    TimerFired,
    UnackedDeliveryExhausted,
    apply_decision,
    is_settled,
    step,
)
from das.models import (
    TERMINAL_JOB,
    AgentRecord,
    JobPlan,
    JobRecord,
    JobResultPayload,
    JobState,
    RequestStatus,
    TokenUsage,
)
from das.plan.dag import transitive_dependents
from tests.strategies import job_plans

S = JobState
RID = "r1"
CAP = "research"
CFG = MachineConfig(max_attempts=3, base_backoff=2, max_backoff=60, capability_wait=60)
WORKERS = (
    AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=frozenset({CAP}),
        status="Available",
        last_heartbeat=0.0,
    ),
)

TERMINAL_KINDS = ("result_final", "unacked", "publish_exhausted", "capability_timeout")
KINDS = (*TERMINAL_KINDS, "result_retryable", "cancelled")


class Driver:
    """Applies events through the reducer, tracking the snapshot and the clock."""

    def __init__(self, plan: JobPlan) -> None:
        self.now = 0.0
        self.snap = RequestSnapshot(
            request_id=RID,
            status=RequestStatus.RUNNING,
            prompt="the prompt",
            plan=plan,
            jobs={j.job_id: JobRecord(job=j, state=S.WAITING, attempt=0) for j in plan.jobs},
        )

    def ctx(self, *, agents=WORKERS, config: MachineConfig = CFG) -> StepContext:
        return StepContext(now=self.now, config=config, agents=tuple(agents))

    def run(self, event, **ctx_kw) -> Decision:
        decision = step(self.snap, event, self.ctx(**ctx_kw))
        self.snap = apply_decision(self.snap, decision)
        return decision

    def record(self, job_id: str) -> JobRecord:
        return self.snap.jobs[job_id]

    def in_flight(self) -> list[str]:
        return sorted(j for j, r in self.snap.jobs.items() if r.state in (S.READY, S.DISPATCHED))


def result(job_id: str, attempt: int, status: str, error: str | None = None) -> JobResultPayload:
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}",
        status=status,  # type: ignore[arg-type]
        content=f"out {job_id}@{attempt}",
        error=error,
        usage=TokenUsage(),
        duration_ms=1,
    )


def fail_event(drv: Driver, x: str, kind: str, confirm: bool, status: str):
    """Prepare ``x`` for the failure ``kind`` and return ``(event, ctx kwargs)`` of the
    single step under test."""
    rec = drv.record(x)
    if kind in ("result_final", "result_retryable", "cancelled", "unacked") and confirm:
        drv.run(PublishConfirmed(x, rec.attempt))
        rec = drv.record(x)
    if kind == "result_final":
        # The current attempt is the last one allowed.
        return JobResultReceived(result(x, rec.attempt, status, "boom")), {
            "config": MachineConfig(max_attempts=rec.attempt, capability_wait=CFG.capability_wait)
        }
    if kind == "result_retryable":
        return JobResultReceived(result(x, rec.attempt, status, "boom")), {}
    if kind == "cancelled":
        return JobResultReceived(result(x, rec.attempt, "Cancelled", "stop")), {}
    if kind == "unacked":
        return UnackedDeliveryExhausted(x), {}
    if kind == "publish_exhausted":
        for n in range(1, 4):
            drv.run(PublishFailed(x, rec.attempt, n))
            drv.now += CFG.publish_retry_delay
            drv.run(TimerFired(TIMER_PUBLISH, x, rec.attempt))
        return PublishFailed(x, rec.attempt, 4), {}
    if kind == "capability_timeout":
        # One failed publish, then the retry finds no capable worker: capability wait.
        drv.run(PublishFailed(x, rec.attempt, 1))
        drv.now += CFG.publish_retry_delay
        drv.run(TimerFired(TIMER_PUBLISH, x, rec.attempt), agents=())
        drv.now += CFG.capability_wait
        return Tick(), {"agents": ()}
    raise AssertionError(kind)


@st.composite
def scenarios(draw):
    plan = draw(job_plans(max_jobs=8, capabilities=frozenset({CAP})))
    drv = Driver(plan)
    drv.run(PlanCommitted())
    # Succeed up to n-1 Jobs: some Job always stays unfinished, and the earliest such
    # Job in topological order has all dependencies Succeeded, so it is in flight.
    for _ in range(draw(st.integers(0, len(plan.jobs) - 1))):
        job_id = draw(st.sampled_from(drv.in_flight()))
        rec = drv.record(job_id)
        if draw(st.booleans()):
            drv.run(PublishConfirmed(job_id, rec.attempt))
        drv.now += 1.0
        drv.run(JobResultReceived(result(job_id, rec.attempt, "Succeeded")))
    x = draw(st.sampled_from(drv.in_flight()))
    kind = draw(st.sampled_from(KINDS))
    event, ctx_kw = fail_event(
        drv, x, kind, draw(st.booleans()), draw(st.sampled_from(["Failed", "TimedOut"]))
    )
    return plan, drv, x, kind, event, ctx_kw


@settings(max_examples=300, deadline=None)
@given(scenarios())
def test_failure_propagation_skips_exactly_the_transitive_dependents(case):
    """**Validates: Requirements 4.4**"""
    plan, drv, x, kind, event, ctx_kw = case
    before = drv.snap
    decision = drv.run(event, **ctx_kw)
    after = drv.snap

    newly_skipped = {
        j for j, r in after.jobs.items() if r.state is S.SKIPPED and before.jobs[j].state is not S.SKIPPED
    }
    dependents = transitive_dependents(plan, x)
    dispatched = {e.job_id for e in decision.effects if isinstance(e, PublishDispatch)}

    if kind == "result_retryable":
        # Failed/TimedOut awaiting a retry timer is not terminal: nothing is skipped.
        assert after.jobs[x].state in (S.FAILED, S.TIMED_OUT)
        assert not is_settled(after.jobs[x], after.timer_keys())
        assert newly_skipped == set()
        expected: set[str] = set()
    else:
        terminal = S.CANCELLED if kind == "cancelled" else S.FAILED
        assert after.jobs[x].state is terminal
        assert is_settled(after.jobs[x], after.timer_keys())
        expected = {d for d in dependents if before.jobs[d].state not in TERMINAL_JOB}
        assert newly_skipped == expected
        word = "cancelled" if kind == "cancelled" else "failed"
        for j in expected:
            assert before.jobs[j].state is S.WAITING
            assert after.jobs[j].reason == f"skipped: dependency {x} {word}"

    hypothesis.event(f"kind={kind}, skipped={'some' if expected else 'none'}")

    # No Skipped Job receives a dispatch.
    assert not dispatched & {j for j, r in after.jobs.items() if r.state is S.SKIPPED}
    assert not dispatched & dependents
    # Every Job outside the propagated set (and other than X) keeps its prior state.
    for j in plan.job_ids():
        if j != x and j not in expected:
            assert after.jobs[j].state is before.jobs[j].state, j
