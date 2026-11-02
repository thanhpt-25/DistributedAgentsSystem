# Feature: distributed-agent-system, Property 20: Attempt progression, retry, and dead-lettering
"""Property 20: Attempt progression, retry, and dead-lettering.

For any ``max_attempts`` in 1..10 and any sequence of current-attempt Job_Results with
status in {Succeeded, Failed, TimedOut, Cancelled}, driven against a single Job (with
0..3 already-Succeeded dependencies):

- Failed/TimedOut at attempt ``N < max_attempts`` records that state and schedules
  exactly one retry timer at ``now + backoff(N)``; when it fires the Job is Ready at
  ``N+1`` with exactly one ``PublishDispatch`` carrying the same Job_ID, instruction,
  Capabilities and dependency results as the first dispatch.
- Failed/TimedOut at ``N == max_attempts`` sets Failed with the last error and emits
  exactly one ``PublishDeadLetter(request_id, job_id, N, last_error)``.
- Cancelled sets Cancelled with no re-dispatch and no dead-letter.
- Succeeded sets Succeeded.

**Validates: Requirements 6.1, 6.3, 6.7**
"""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.backoff import backoff
from das.manager.machine import (
    TIMER_RETRY,
    Decision,
    JobResultReceived,
    MachineConfig,
    PublishConfirmed,
    PublishDeadLetter,
    PublishDispatch,
    RequestSnapshot,
    ScheduleTimer,
    StepContext,
    Tick,
    TimerFired,
    apply_decision,
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
from das.state.store import TimerKey

S = JobState
RID = "req-p20"
TARGET = "target"
CAP = "research"
FAILURES = ("Failed", "TimedOut")
STATUSES = ("Succeeded", "Failed", "TimedOut", "Cancelled")

WORKERS = (
    AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=frozenset({CAP, "summarize"}),
        status="Available",
        last_heartbeat=0.0,
    ),
)


# ------------------------------------------------------------------ helpers (mirrored)


def res(
    job_id: str, attempt: int, status: str, error: str | None = None, content: str | None = None
) -> JobResultPayload:
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}",
        status=status,  # type: ignore[arg-type]
        content=content if content is not None else f"out {job_id}@{attempt}",
        error=error,
        usage=TokenUsage(),
        duration_ms=1,
    )


def of(d: Decision, kind: type) -> list:
    return [e for e in d.effects if isinstance(e, kind)]


def run(s: RequestSnapshot, event, c: StepContext) -> tuple[RequestSnapshot, Decision]:
    d = step(s, event, c)
    return apply_decision(s, d), d


def initial_snapshot(dep_contents: list[str]) -> RequestSnapshot:
    deps = [f"dep{i}" for i in range(len(dep_contents))]
    planned = [
        PlannedJob(job_id=d, instruction=f"do {d}", capabilities=[CAP], depends_on=[])
        for d in deps
    ]
    planned.append(
        PlannedJob(
            job_id=TARGET,
            instruction="do the target thing",
            capabilities=[CAP],
            depends_on=deps,
        )
    )
    plan = JobPlan(jobs=planned)
    jobs = {
        j.job_id: JobRecord(
            job=j,
            state=S.SUCCEEDED if j.job_id != TARGET else S.WAITING,
            attempt=1 if j.job_id != TARGET else 0,
        )
        for j in plan.jobs
    }
    results = {
        (d, 1): res(d, 1, "Succeeded", content=c) for d, c in zip(deps, dep_contents)
    }
    return RequestSnapshot(
        request_id=RID,
        status=RequestStatus.RUNNING,
        prompt="the prompt",
        plan=plan,
        jobs=jobs,
        results=results,
    )


# ------------------------------------------------------------------ strategies

_status_seq = st.one_of(
    st.lists(st.sampled_from(STATUSES), min_size=1, max_size=11),
    st.lists(st.sampled_from(FAILURES), min_size=10, max_size=11),  # reach exhaustion
)


@st.composite
def scenarios(draw: st.DrawFn):
    max_attempts = draw(st.integers(min_value=1, max_value=10))
    statuses = draw(_status_seq)
    # (status, error, seconds elapsed before the result arrives)
    outcomes = [
        (
            s,
            draw(st.none() | st.text(min_size=1, max_size=20)),
            draw(st.floats(min_value=0.0, max_value=1000.0, allow_nan=False)),
        )
        for s in statuses
    ]
    dep_contents = draw(st.lists(st.text(max_size=20), max_size=3))
    base = draw(st.floats(min_value=0.1, max_value=60.0, allow_nan=False))
    cap = draw(st.floats(min_value=base, max_value=600.0, allow_nan=False))
    return max_attempts, outcomes, dep_contents, base, cap


# ------------------------------------------------------------------ property


@settings(max_examples=200, deadline=None)
@given(scenarios())
def test_attempt_progression_retry_and_dead_lettering(scenario) -> None:
    max_attempts, outcomes, dep_contents, base, cap = scenario
    cfg = MachineConfig(max_attempts=max_attempts, base_backoff=base, max_backoff=cap)
    now = 100.0

    def ctx() -> StepContext:
        return StepContext(now=now, config=cfg, agents=WORKERS)

    s = initial_snapshot(dep_contents)
    s, d = run(s, Tick(), ctx())
    first = of(d, PublishDispatch)
    assert len(first) == 1 and first[0].attempt == 1
    first_dispatch = first[0]
    assert first_dispatch.job_id == TARGET
    assert dict(first_dispatch.dependency_results) == {
        f"dep{i}": c for i, c in enumerate(dep_contents)
    }

    for status, error, advance in outcomes:
        n = s.jobs[TARGET].attempt
        assert s.jobs[TARGET].state is S.READY
        s, _ = run(s, PublishConfirmed(job_id=TARGET, attempt=n), ctx())
        assert s.jobs[TARGET].state is S.DISPATCHED

        now += advance
        s, d = run(s, JobResultReceived(res(TARGET, n, status, error=error)), ctx())
        rec = s.jobs[TARGET]
        dead = of(d, PublishDeadLetter)
        retry_timers = [t for t in of(d, ScheduleTimer) if t.kind == TIMER_RETRY]
        assert of(d, PublishDispatch) == []
        assert rec.attempt == n

        if status == "Succeeded":
            assert rec.state is S.SUCCEEDED
            assert dead == [] and retry_timers == []
            return

        if status == "Cancelled":
            # Req 6.7: no retry, no dead-letter.
            assert rec.state is S.CANCELLED
            assert dead == [] and retry_timers == []
            assert TimerKey(TIMER_RETRY, TARGET, n) not in s.timer_keys()
            return

        reason = error or f"job {status}"
        if n >= max_attempts:
            # Req 6.3: exhausted — Failed with last error, exactly one dead-letter.
            assert n == max_attempts
            assert rec.state is S.FAILED
            assert rec.reason == reason
            assert retry_timers == []
            assert dead == [
                PublishDeadLetter(
                    request_id=RID, job_id=TARGET, attempts=n, last_error=reason
                )
            ]
            assert not any(k.kind == TIMER_RETRY for k in s.timer_keys())
            return

        # Req 6.1: record the attempt's state and schedule exactly one retry.
        assert rec.state is S(status)
        assert dead == []
        assert len(retry_timers) == 1
        timer = retry_timers[0]
        assert (timer.job_id, timer.attempt) == (TARGET, n)
        assert timer.at == now + backoff(n, base, cap)
        assert TimerKey(TIMER_RETRY, TARGET, n) in s.timer_keys()

        now = timer.at
        s, d = run(s, TimerFired(kind=TIMER_RETRY, job_id=TARGET, attempt=n), ctx())
        assert s.jobs[TARGET].state is S.READY
        assert s.jobs[TARGET].attempt == n + 1
        assert of(d, PublishDeadLetter) == []
        redispatch = of(d, PublishDispatch)
        assert len(redispatch) == 1
        p = redispatch[0]
        assert p.attempt == n + 1
        assert p.job_id == first_dispatch.job_id
        assert p.instruction == first_dispatch.instruction
        assert p.required_capabilities == first_dispatch.required_capabilities
        assert dict(p.dependency_results) == dict(first_dispatch.dependency_results)
        assert TimerKey(TIMER_RETRY, TARGET, n) not in s.timer_keys()
