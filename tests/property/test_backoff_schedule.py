# Feature: distributed-agent-system, Property 21: Backoff schedule
"""Property 21: Backoff schedule.

For any base delay ``b`` in [0.1, 60], max delay ``m`` in [1, 600] with ``b <= m``,
and failed attempt ``N >= 1``, the scheduled re-dispatch time minus the result time
is at least ``min(b * 2^(N-1), m)`` and at most that value plus the driver tick
(250 ms).

Checked on the pure ``backoff`` function and on the RequestMachine: a Failed or
TimedOut result for attempt N < max_attempts schedules the retry timer at
``now + backoff(N)``.

**Validates: Requirements 6.2**
"""
from __future__ import annotations

from fractions import Fraction

from hypothesis import assume, given
from hypothesis import strategies as st

from das.manager.backoff import backoff
from das.manager.machine import (
    TIMER_RETRY,
    JobResultReceived,
    MachineConfig,
    RequestSnapshot,
    ScheduleTimer,
    StepContext,
    step,
)
from das.models import (
    AgentRecord,
    JobPlan,
    JobRecord,
    JobResultPayload,
    JobState,
    PlannedJob,
    RequestStatus,
    TokenUsage,
)

DRIVER_TICK = 0.25
EPS = 1e-6  # float rounding of (now + delay) - now; ulp(2e9) is ~2.4e-7

bases = st.floats(min_value=0.1, max_value=60, allow_nan=False, allow_infinity=False)
maxes = st.floats(min_value=1, max_value=600, allow_nan=False, allow_infinity=False)
attempts = st.integers(min_value=1, max_value=10)
nows = st.floats(min_value=0, max_value=2e9, allow_nan=False, allow_infinity=False)


def expected_delay(n: int, b: float, m: float) -> float:
    """min(b * 2^(n-1), m), compared exactly so huge n cannot overflow."""
    if Fraction(b) * 2 ** (n - 1) >= Fraction(m):
        return m
    return b * 2.0 ** (n - 1)


@given(n=st.integers(min_value=1, max_value=2000), b=bases, m=maxes)
def test_backoff_formula(n: int, b: float, m: float) -> None:
    assume(b <= m)
    delay = backoff(n, b, m)
    assert delay == expected_delay(n, b, m)
    assert b <= delay <= m
    if n > 1:
        assert backoff(n - 1, b, m) <= delay  # monotone non-decreasing


def _snapshot(attempt: int) -> RequestSnapshot:
    job = PlannedJob(job_id="a", instruction="do a", capabilities=["research"], depends_on=[])
    return RequestSnapshot(
        request_id="r1",
        status=RequestStatus.RUNNING,
        prompt="p",
        plan=JobPlan(jobs=[job]),
        jobs={"a": JobRecord(job=job, state=JobState.DISPATCHED, attempt=attempt)},
        results={},
        timers=(),
    )


WORKERS = (
    AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=frozenset({"research"}),
        status="Available",
        last_heartbeat=0.0,
    ),
)


@given(
    b=bases,
    m=maxes,
    n=attempts,
    extra=st.integers(min_value=1, max_value=10),
    now=nows,
    status=st.sampled_from(["Failed", "TimedOut"]),
)
def test_machine_schedules_retry_at_backoff(
    b: float, m: float, n: int, extra: int, now: float, status: str
) -> None:
    assume(b <= m)
    max_attempts = min(n + extra, 10)
    assume(n < max_attempts)
    cfg = MachineConfig(max_attempts=max_attempts, base_backoff=b, max_backoff=m)
    result = JobResultPayload(
        request_id="r1",
        job_id="a",
        attempt=n,
        worker_id="w1",
        status=status,  # type: ignore[arg-type]
        content="",
        error="boom",
        usage=TokenUsage(),
        duration_ms=1,
    )
    d = step(_snapshot(n), JobResultReceived(result), StepContext(now=now, config=cfg, agents=WORKERS))
    timers = [e for e in d.effects if isinstance(e, ScheduleTimer)]
    assert len(timers) == 1
    (timer,) = timers
    assert (timer.kind, timer.job_id, timer.attempt) == (TIMER_RETRY, "a", n)
    want = expected_delay(n, b, m)
    gap = timer.at - now
    assert want - EPS <= gap <= want + DRIVER_TICK
