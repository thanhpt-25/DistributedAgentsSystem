"""Property test for result idempotency (task 12.12).

A Running Request with independent Jobs (no dependencies, so the Jobs never influence
one another's states) is built directly. Each Job is at some current attempt ``cur`` and
is either in flight (Ready/Dispatched, no result for ``cur`` yet) or already Succeeded
with a stored Succeeded result for ``cur``. Earlier attempts may have stored Failed
results.

Candidate Job_Results are drawn per Job: any attempt in ``1..cur+1`` (stale, current,
future) with any status, plus results carrying another Request's ID. Each candidate has
a unique payload. The candidates are delivered through ``step`` + ``apply_decision``,
each one ``k >= 1`` times, in a shuffled order. The oracle is delivering only the valid
results (this Request, in-flight Job, ``attempt == cur``) once each.
"""

# Feature: distributed-agent-system, Property 22: Result handling is idempotent

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    Decision,
    JobResultReceived,
    MachineConfig,
    RequestSnapshot,
    StepContext,
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

S = JobState
RID = "r1"
OTHER_RID = "r2"
CAP = "research"
STATUSES = ("Succeeded", "Failed", "TimedOut", "Cancelled")
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


def result(
    job_id: str, attempt: int, status: str, tag: str, request_id: str = RID
) -> JobResultPayload:
    return JobResultPayload(
        request_id=request_id,
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}-{tag}",
        status=status,  # type: ignore[arg-type]
        content=f"out {job_id}@{attempt} {tag}",
        error=None if status == "Succeeded" else f"err {tag}",
        usage=TokenUsage(),
        duration_ms=1,
    )


def deliver(
    snap: RequestSnapshot, results: list[JobResultPayload], ctx: StepContext
) -> RequestSnapshot:
    for r in results:
        snap = apply_decision(snap, step(snap, JobResultReceived(r), ctx))
    return snap


def no_store_updates(d: Decision) -> bool:
    u = d.updates
    return not (u.jobs or u.results or u.timers_add or u.timers_remove) and not d.changes


@st.composite
def scenarios(draw):
    max_attempts = draw(st.integers(1, 4))
    n_jobs = draw(st.integers(1, 4))
    planned, jobs, stored, candidates, valid = [], {}, {}, [], []
    for i in range(n_jobs):
        job_id = f"j{i}"
        job = PlannedJob(job_id=job_id, instruction=f"do {job_id}", capabilities=[CAP])
        planned.append(job)
        cur = draw(st.integers(1, max_attempts))
        # Earlier attempts' stored (Failed) results, a random subset.
        for a in range(1, cur):
            if draw(st.booleans()):
                stored[(job_id, a)] = result(job_id, a, "Failed", "stored")
        if draw(st.booleans()):
            state = S.SUCCEEDED
            stored[(job_id, cur)] = result(job_id, cur, "Succeeded", "first")
        else:
            state = draw(st.sampled_from([S.READY, S.DISPATCHED]))
        jobs[job_id] = JobRecord(job=job, state=state, attempt=cur)

        # Candidate results: one per (request, attempt) at most, unique payloads.
        for a in range(1, cur + 2):
            if draw(st.booleans()):
                r = result(job_id, a, draw(st.sampled_from(STATUSES)), f"cand{a}")
                candidates.append(r)
                if a == cur and state is not S.SUCCEEDED:
                    valid.append(r)
            if draw(st.integers(0, 3)) == 0:
                candidates.append(
                    result(job_id, a, draw(st.sampled_from(STATUSES)), f"x{a}", OTHER_RID)
                )

    snap = RequestSnapshot(
        request_id=RID,
        status=RequestStatus.RUNNING,
        prompt="the prompt",
        plan=JobPlan(jobs=planned),
        jobs=jobs,
        results=stored,
    )
    copies = draw(st.lists(st.integers(1, 3), min_size=len(candidates), max_size=len(candidates)))
    sequence = [r for r, k in zip(candidates, copies) for _ in range(k)]
    sequence = draw(st.permutations(sequence))
    ctx = StepContext(
        now=100.0,
        config=MachineConfig(max_attempts=max_attempts, base_backoff=2, max_backoff=60),
        agents=WORKERS,
    )
    return snap, candidates, valid, list(sequence), ctx


@settings(max_examples=300, deadline=None)
@given(scenarios())
def test_result_handling_is_idempotent(case):
    """**Validates: Requirements 2.8, 6.5**"""
    snap, candidates, valid, sequence, ctx = case

    reference = deliver(snap, valid, ctx)

    # Deliver the noisy sequence step by step; every non-first or invalid delivery
    # must produce no Job/result/timer updates.
    current, seen = snap, set()
    valid_ids = {id(r) for r in valid}
    for r in sequence:
        d = step(current, JobResultReceived(r), ctx)
        if id(r) not in valid_ids or id(r) in seen:
            assert no_store_updates(d), r
        seen.add(id(r))
        current = apply_decision(current, d)

    assert current.jobs == reference.jobs
    assert current.results == reference.results
    assert current.timer_keys() == reference.timer_keys()

    # Previously stored results (including the first Succeeded one) are never replaced.
    for key, stored in snap.results.items():
        assert current.results[key] == stored
    # Each valid result was recorded exactly as delivered; nothing else was added.
    assert set(current.results) == set(snap.results) | {(r.job_id, r.attempt) for r in valid}
    for r in valid:
        assert current.results[(r.job_id, r.attempt)] == r
    # Jobs without a valid result keep their state and attempt.
    valid_jobs = {r.job_id for r in valid}
    for job_id, rec in snap.jobs.items():
        if job_id not in valid_jobs:
            assert current.jobs[job_id] == rec
        else:
            assert current.jobs[job_id].attempt == rec.attempt

    # Re-delivering anything once more is a no-op for the store.
    for r in candidates:
        assert no_store_updates(step(current, JobResultReceived(r), ctx)), r
