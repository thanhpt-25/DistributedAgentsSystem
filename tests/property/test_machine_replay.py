# Feature: das-observability-operations, Property 22: Áp dụng Replay trong RequestMachine
"""Property 22: applying a Replay in the RequestMachine.

For any Job_Plan DAG, any Job state map and any ``ReplayRequested(dlid, job, attempt)``:

- If the Job is Failed (no pending ``retry`` timer), ``attempt == job.attempt`` and the
  Request is Running, the replayed Job goes to Ready (dispatched or in capability wait)
  with ``attempt + 1`` and ``attempt_limit = attempt + max_attempts``. Exactly the Jobs
  in ``skipped_to_reset`` (checked against a reference ancestor-walk oracle) go back to
  Waiting. Every other Job keeps its state, except changes made by ``_settle``.
- Otherwise no Job changes state and ``dead_letter_reverts == [dlid]``.
- In both cases ``replays_applied == [dlid]``.

Snapshots are drawn with reachable Job state maps (a Job left Waiting/Skipped/Cancelled
unless all its dependencies Succeeded, each Succeeded Job with its stored result; the
reducer requires those results to dispatch), then settled with ``Tick`` until a
fixpoint, so any state change in the decision is attributable to the replay. With a
settled snapshot, ``_settle`` can only change descendants of the replayed Job.

**Validates: Requirements 13.4, 13.15**
"""
from __future__ import annotations

from hypothesis import assume, event, given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    TIMER_CAPABILITY_WAIT,
    TIMER_PUBLISH,
    TIMER_RETRY,
    MachineConfig,
    PublishDispatch,
    ReplayRequested,
    RequestSnapshot,
    StepContext,
    Tick,
    apply_decision,
    attempt_limit,
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
from das.ops.replay import skipped_to_reset
from das.state.store import Timer
from tests.strategies import job_plans

RID = "req-replay"
DLID = "cd" * 16
CAPS = frozenset({"research", "summarize", "code"})
TIMER_KINDS = (TIMER_RETRY, TIMER_PUBLISH, TIMER_CAPABILITY_WAIT)
NOW = 1000.0

# Weighted toward the states a Replay cares about (Failed roots, Skipped dependents).
state_choices = st.sampled_from(
    [JobState.FAILED] * 3 + [JobState.SKIPPED] * 3 + list(JobState)
)


# --------------------------------------------------------------------------- oracle


def oracle_reset(plan: JobPlan, jobs: dict[str, JobRecord], replayed: str) -> frozenset[str]:
    """Reference: Skipped Jobs whose root causes (Failed/Cancelled ancestors reached
    only through Skipped intermediates) are exactly ``{replayed}``."""
    deps = {pj.job_id: list(pj.depends_on) for pj in plan.jobs}
    out: set[str] = set()
    for jid, rec in jobs.items():
        if rec.state is not JobState.SKIPPED:
            continue
        roots: set[str] = set()
        seen: set[str] = set()
        stack = list(deps.get(jid, []))
        while stack:
            dep = stack.pop()
            if dep in seen:
                continue
            seen.add(dep)
            d = jobs.get(dep)
            if d is None:
                continue
            if d.state in (JobState.FAILED, JobState.CANCELLED):
                roots.add(dep)
            elif d.state is JobState.SKIPPED:
                stack.extend(deps.get(dep, []))
        if roots == {replayed}:
            out.add(jid)
    return frozenset(out)


def descendants(plan: JobPlan, root: str) -> set[str]:
    children: dict[str, list[str]] = {}
    for pj in plan.jobs:
        for d in pj.depends_on:
            children.setdefault(d, []).append(pj.job_id)
    out: set[str] = set()
    stack = list(children.get(root, []))
    while stack:
        j = stack.pop()
        if j not in out:
            out.add(j)
            stack.extend(children.get(j, []))
    return out


# --------------------------------------------------------------------------- strategies


def topo(plan: JobPlan) -> list[PlannedJob]:
    done: set[str] = set()
    order: list[PlannedJob] = []
    while len(order) < len(plan.jobs):
        for pj in plan.jobs:
            if pj.job_id not in done and all(d in done for d in pj.depends_on):
                done.add(pj.job_id)
                order.append(pj)
    return order


def _result(job_id: str, attempt: int) -> JobResultPayload:
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id="w1",
        status="Succeeded",
        content=f"out {job_id}",
        error=None,
        usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2),
        duration_ms=1,
    )


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


def settle(snap: RequestSnapshot, ctx: StepContext) -> RequestSnapshot | None:
    """Apply ``Tick`` until no Job state changes (``None`` if no fixpoint is reached)."""
    for _ in range(5):
        d = step(snap, Tick(), ctx)
        after = apply_decision(snap, d)
        if {j: r.state for j, r in after.jobs.items()} == {
            j: r.state for j, r in snap.jobs.items()
        } and not d.updates.jobs:
            return snap
        snap = after
    return None


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[RequestSnapshot, ReplayRequested, StepContext]:
    plan = draw(job_plans(max_jobs=7, capabilities=CAPS))
    max_attempts = draw(st.integers(min_value=1, max_value=4))
    config = MachineConfig(
        max_attempts=max_attempts,
        capability_wait=draw(st.floats(min_value=1.0, max_value=120)),
    )
    ctx = StepContext(now=NOW, config=config, agents=draw(agent_sets()))

    jobs: dict[str, JobRecord] = {}
    results: dict[tuple[str, int], JobResultPayload] = {}
    timers: dict[tuple[str, str, int], Timer] = {}
    for job in topo(plan):
        # Reachable states only: a Job ran only if every dependency Succeeded.
        if all(jobs[d].state is JobState.SUCCEEDED for d in job.depends_on):
            state = draw(state_choices)
        else:
            state = draw(st.sampled_from([JobState.SKIPPED] * 4 + [JobState.WAITING, JobState.CANCELLED]))
        attempt = 0 if state is JobState.WAITING else draw(st.integers(1, max_attempts + 2))
        jobs[job.job_id] = JobRecord(
            job=job,
            state=state,
            attempt=attempt,
            ready_since=NOW if state is JobState.READY else None,
            reason="r" if state in (JobState.FAILED, JobState.SKIPPED) else None,
            attempt_limit=draw(st.none() | st.integers(attempt, attempt + 4))
            if attempt
            else None,
        )
        if state is JobState.SUCCEEDED:
            results[(job.job_id, attempt)] = _result(job.job_id, attempt)
        if state is JobState.FAILED and draw(st.integers(0, 4)) == 0:
            timers[(TIMER_RETRY, job.job_id, attempt)] = Timer(
                kind=TIMER_RETRY, job_id=job.job_id, attempt=attempt, due=NOW + 10
            )
    status = draw(
        st.sampled_from([RequestStatus.RUNNING] * 12 + list(RequestStatus))
    )
    snap = RequestSnapshot(
        request_id=RID,
        status=status,
        prompt="p",
        plan=plan,
        jobs=jobs,
        results=results,
        timers=tuple(timers.values()),
        pending_replays=frozenset({DLID}),
    )
    settled = settle(snap, ctx)
    assume(settled is not None)
    assert settled is not None

    failed = [j for j, r in settled.jobs.items() if r.state is JobState.FAILED]
    job_id = draw(st.sampled_from(failed) if failed and draw(st.integers(0, 4)) > 0 else st.sampled_from(
        list(settled.jobs) + ["unknown-job"]
    ))
    rec = settled.jobs.get(job_id)
    current = rec.attempt if rec is not None else 1
    attempt = draw(st.sampled_from([current] * 4 + [current + 1, max(current - 1, 0)]))
    return settled, ReplayRequested(DLID, job_id, attempt), ctx


def is_applicable(snap: RequestSnapshot, ev: ReplayRequested) -> bool:
    rec = snap.jobs.get(ev.job_id)
    return (
        snap.status is RequestStatus.RUNNING
        and rec is not None
        and rec.state is JobState.FAILED
        and rec.attempt == ev.attempt
        and not any(
            t.kind == TIMER_RETRY and t.job_id == ev.job_id and t.attempt == rec.attempt
            for t in snap.timers
        )
    )


# --------------------------------------------------------------------------- property


@settings(max_examples=500, deadline=None)
@given(scenarios())
def test_replay_applied_or_reverted(
    scenario: tuple[RequestSnapshot, ReplayRequested, StepContext],
) -> None:
    snap, ev, ctx = scenario
    d = step(snap, ev, ctx)
    after = apply_decision(snap, d)

    assert list(d.updates.replays_applied) == [DLID]
    assert after.pending_replays == frozenset()

    applicable = is_applicable(snap, ev)
    event("applied" if applicable else "reverted")
    if not applicable:
        assert list(d.updates.dead_letter_reverts) == [DLID]
        assert d.changes == ()
        assert {j: r.state for j, r in after.jobs.items()} == {
            j: r.state for j, r in snap.jobs.items()
        }
        return

    assert list(d.updates.dead_letter_reverts) == []
    plan = snap.plan
    assert plan is not None
    before = snap.jobs[ev.job_id]

    # Replayed Job: Ready for attempt + 1 with the raised limit.
    replayed = after.jobs[ev.job_id]
    assert replayed.state is JobState.READY
    assert replayed.attempt == before.attempt + 1
    assert replayed.attempt_limit == before.attempt + ctx.config.max_attempts
    assert attempt_limit(replayed, ctx.config) == before.attempt + ctx.config.max_attempts
    dispatched = any(
        isinstance(e, PublishDispatch) and e.job_id == ev.job_id and e.attempt == replayed.attempt
        for e in d.effects
    )
    capwait = any(
        t.kind == TIMER_CAPABILITY_WAIT and t.job_id == ev.job_id and t.attempt == replayed.attempt
        for t in after.timers
    )
    assert dispatched or capwait

    # Exactly the oracle's Skipped Jobs return to Waiting.
    expected_reset = oracle_reset(plan, dict(snap.jobs), ev.job_id)
    assert skipped_to_reset(plan, snap.jobs, ev.job_id) == expected_reset
    event("resets skipped" if expected_reset else "no skipped reset")
    for j in expected_reset:
        assert after.jobs[j].state is JobState.WAITING
    reset_now = {
        j
        for j, r in after.jobs.items()
        if snap.jobs[j].state is JobState.SKIPPED and r.state is JobState.WAITING
    }
    assert reset_now == expected_reset

    # Every other Job keeps its state; only descendants may be moved by _settle.
    desc = descendants(plan, ev.job_id)
    for j, r in after.jobs.items():
        if j == ev.job_id or j in expected_reset:
            continue
        old = snap.jobs[j].state
        if j not in desc:
            assert r.state is old, (j, old, r.state)
        elif old is not JobState.WAITING:
            assert r.state is old, (j, old, r.state)
        else:
            assert r.state in (JobState.WAITING, JobState.SKIPPED), (j, r.state)
