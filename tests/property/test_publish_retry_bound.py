"""Property test for the publish retry bound (task 12.10).

A one-Job Request is driven through the real reducer (``step`` + ``apply_decision``) by
a random sequence of publish outcomes: failures (with arbitrary informational ``n``),
confirmations, publish-retry timer firings, duplicate failures while a retry is pending,
stale or future-attempt events, ticks, and Failed Job_Results that start a new attempt.

A small reference model tracks, per attempt, the consecutive unconfirmed publishes. After
every step the reducer must agree with the model:

- the Job stays Ready while an attempt has fewer than 4 consecutive publish failures,
- it becomes Failed with ``REASON_DISPATCH_FAILURE`` exactly at the 4th (1 + 3 retries),
- it becomes Dispatched only after a confirmed publish of its current attempt,
- at most 4 ``PublishDispatch`` effects are emitted per attempt (1 + one per retry).
"""

# Feature: distributed-agent-system, Property 19: Publish retry bound

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

import hypothesis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    MAX_PUBLISH_ATTEMPTS,
    REASON_DISPATCH_FAILURE,
    TIMER_PUBLISH,
    TIMER_RETRY,
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
    apply_decision,
    step,
)
from das.models import (
    AgentRecord,
    PlannedJob,
    JobPlan,
    JobRecord,
    JobResultPayload,
    JobState,
    RequestStatus,
    TokenUsage,
)
from das.state import TimerKey

S = JobState
RID = "r1"
JOB = "a"
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


class Driver:
    """Applies events through the reducer, tracking the snapshot and the clock."""

    def __init__(self) -> None:
        self.now = 0.0
        plan = JobPlan(jobs=[PlannedJob(job_id=JOB, instruction="do a", capabilities=[CAP], depends_on=[])])
        self.snap = RequestSnapshot(
            request_id=RID,
            status=RequestStatus.RUNNING,
            prompt="the prompt",
            plan=plan,
            jobs={JOB: JobRecord(job=plan.jobs[0], state=S.WAITING, attempt=0)},
        )

    def run(self, event) -> Decision:
        decision = step(self.snap, event, StepContext(now=self.now, config=CFG, agents=WORKERS))
        self.snap = apply_decision(self.snap, decision)
        return decision

    @property
    def record(self) -> JobRecord:
        return self.snap.jobs[JOB]


def failed_result(attempt: int) -> JobResultPayload:
    return JobResultPayload(
        request_id=RID,
        job_id=JOB,
        attempt=attempt,
        worker_id="w1",
        status="Failed",
        content="",
        error="boom",
        usage=TokenUsage(),
        duration_ms=1,
    )


@dataclass
class Model:
    """Reference model of one Job's publish lifecycle."""

    state: JobState = S.READY  # after PlanCommitted
    attempt: int = 1
    failures: int = 0  # consecutive unconfirmed publishes of the current attempt
    publish_pending: bool = False  # a publish retry timer is armed
    retry_pending: bool = False  # a Failed attempt awaits its retry timer
    dispatch_failed: bool = False

    def fail(self, attempt: int) -> None:
        if self.state is not S.READY or attempt != self.attempt or self.publish_pending:
            return  # stale, duplicate, or not awaiting a publish outcome: ignored
        self.failures += 1
        if self.failures >= MAX_PUBLISH_ATTEMPTS:
            self.state, self.dispatch_failed = S.FAILED, True
        else:
            self.publish_pending = True

    def confirm(self, attempt: int) -> None:
        if self.state is S.READY and attempt == self.attempt:
            self.state, self.failures, self.publish_pending = S.DISPATCHED, 0, False

    def fire_publish(self, attempt: int) -> int:
        """Returns the number of dispatches expected."""
        if self.publish_pending and attempt == self.attempt and self.state is S.READY:
            self.publish_pending = False
            return 1
        return 0

    def result_failed(self) -> None:
        if self.attempt >= CFG.max_attempts:
            self.state = S.FAILED  # final attempt: dead-lettered, no retry
        else:
            self.state, self.retry_pending = S.FAILED, True

    def fire_retry(self) -> int:
        if not self.retry_pending:
            return 0
        self.retry_pending = False
        self.state, self.attempt, self.failures = S.READY, self.attempt + 1, 0
        return 1


DELTA = st.sampled_from([0, 0, 0, 0, -1, 1])  # mostly the current attempt; some stale/future
# Weighted toward fail/fire so runs of 4 unconfirmed publishes are common; confirms and
# attempt changes are rarer so both outcomes (Dispatched, dispatch failure) appear.
KINDS = st.sampled_from(
    ["fail"] * 8 + ["fire_publish"] * 8 + ["confirm", "tick", "result_failed", "fire_retry"]
)
ACTIONS = st.tuples(KINDS, DELTA, st.one_of(st.none(), st.integers(0, 10)))


@settings(max_examples=300, deadline=None)
@given(st.lists(ACTIONS, min_size=10, max_size=60))
def test_publish_retry_bound(actions):
    """**Validates: Requirements 4.8**"""
    drv, model = Driver(), Model()
    dispatches: Counter[int] = Counter()
    expected_dispatches: Counter[int] = Counter({1: 1})

    def count(decision: Decision) -> None:
        for e in decision.effects:
            if isinstance(e, PublishDispatch):
                assert e.job_id == JOB
                dispatches[e.attempt] += 1

    count(drv.run(PlanCommitted()))
    confirmed: set[int] = set()

    for kind, delta, n in actions:
        if kind in ("tick", "result_failed", "fire_retry"):
            delta = 0
        attempt = model.attempt + delta
        before = drv.record.state
        drv.now += CFG.publish_retry_delay
        if kind == "fail":
            model.fail(attempt)
            decision = drv.run(PublishFailed(JOB, attempt, n))
        elif kind == "confirm":
            if model.state is S.READY and attempt == model.attempt:
                confirmed.add(attempt)
            model.confirm(attempt)
            decision = drv.run(PublishConfirmed(JOB, attempt))
        elif kind == "fire_publish":
            expected_dispatches[attempt] += model.fire_publish(attempt)
            decision = drv.run(TimerFired(TIMER_PUBLISH, JOB, attempt))
        elif kind == "tick":
            decision = drv.run(Tick())
        elif kind == "result_failed":
            if model.state is not S.DISPATCHED:
                continue  # only Dispatched attempts report results in this scenario
            model.result_failed()
            decision = drv.run(JobResultReceived(failed_result(model.attempt)))
        else:  # fire_retry
            prev = model.attempt
            if model.fire_retry():
                expected_dispatches[prev + 1] += 1
            decision = drv.run(TimerFired(TIMER_RETRY, JOB, prev))
        count(decision)

        rec = drv.record
        assert rec.state is model.state, (kind, delta)
        assert rec.attempt == model.attempt
        if rec.state is S.READY:
            assert rec.publish_failures == model.failures < MAX_PUBLISH_ATTEMPTS
            assert (TimerKey(TIMER_PUBLISH, JOB, rec.attempt) in drv.snap.timer_keys()) == (
                model.publish_pending
            )
        if rec.state is S.DISPATCHED and before is not S.DISPATCHED:
            assert rec.attempt in confirmed  # Dispatched only after a confirmed publish
        if model.dispatch_failed:
            assert rec.state is S.FAILED and rec.reason == REASON_DISPATCH_FAILURE
            assert rec.publish_failures == MAX_PUBLISH_ATTEMPTS
            assert drv.snap.timers == ()
        # Per-attempt publish bound: 1 initial + at most 3 retries.
        assert dispatches == expected_dispatches
        assert all(c <= MAX_PUBLISH_ATTEMPTS for c in dispatches.values())

    hypothesis.event(
        f"final={model.state.value}, dispatch_failed={model.dispatch_failed}, "
        f"attempts={model.attempt}"
    )
