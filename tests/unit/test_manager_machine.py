"""Unit tests for das.manager.machine: one or more tests per rule of the design's
RequestMachine table, plus resume and the store mapping (Req 1.7, 2.6, 2.8, 4.1-4.4,
4.6, 4.8, 6.1-6.3, 6.5-6.7, 7.1, 7.3, 13.7)."""
from __future__ import annotations

import pytest

from das.manager.backoff import backoff
from das.manager.machine import (
    REASON_DISPATCH_FAILURE,
    REASON_NO_CAPABLE_WORKER,
    REASON_UNACKED_DELIVERY,
    TIMER_CAPABILITY_WAIT,
    TIMER_PUBLISH,
    TIMER_RETRY,
    CancelObserved,
    Decision,
    FinalizeFailed,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishDeadLetter,
    PublishDispatch,
    PublishFailed,
    RequestSnapshot,
    RunSummarizer,
    ScheduleTimer,
    SendCancel,
    StepContext,
    Tick,
    TimerFired,
    UnackedDeliveryExhausted,
    apply_decision,
    resume,
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
from das.state import Timer, TimerKey

S = JobState
RID = "r1"
CFG = MachineConfig(max_attempts=3, base_backoff=2, max_backoff=60, capability_wait=60)


def worker(*caps: str, agent_id: str = "w1") -> AgentRecord:
    return AgentRecord(
        agent_id=agent_id,
        role="worker",
        host="h",
        capabilities=frozenset(caps),
        status="Available",
        last_heartbeat=0.0,
    )


WORKERS = (worker("research", "summarize"),)


def ctx(now: float = 100.0, agents=WORKERS, config: MachineConfig = CFG) -> StepContext:
    return StepContext(now=now, config=config, agents=tuple(agents))


def make_plan(caps: str = "research", **deps: list[str]) -> JobPlan:
    return JobPlan(
        jobs=[
            PlannedJob(job_id=j, instruction=f"do {j}", capabilities=[caps], depends_on=d)
            for j, d in deps.items()
        ]
    )


def snap(
    plan: JobPlan,
    states: dict[str, JobState] | None = None,
    *,
    attempts: dict[str, int] | None = None,
    results=(),
    timers=(),
    status: RequestStatus = RequestStatus.RUNNING,
    ready_since: float | None = None,
) -> RequestSnapshot:
    states = states or {}
    attempts = attempts or {}
    jobs = {}
    for job in plan.jobs:
        state = states.get(job.job_id, S.WAITING)
        attempt = attempts.get(job.job_id, 0 if state is S.WAITING else 1)
        jobs[job.job_id] = JobRecord(
            job=job,
            state=state,
            attempt=attempt,
            ready_since=ready_since if state is S.READY else None,
        )
    return RequestSnapshot(
        request_id=RID,
        status=status,
        prompt="the prompt",
        plan=plan,
        jobs=jobs,
        results={(r.job_id, r.attempt): r for r in results},
        timers=tuple(timers),
    )


def res(job_id: str, attempt: int = 1, status: str = "Succeeded", error: str | None = None):
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


def dispatches(d: Decision) -> list[PublishDispatch]:
    return [e for e in d.effects if isinstance(e, PublishDispatch)]


def of(d: Decision, kind: type) -> list:
    return [e for e in d.effects if isinstance(e, kind)]


def run(s: RequestSnapshot, event, c: StepContext) -> tuple[RequestSnapshot, Decision]:
    d = step(s, event, c)
    return apply_decision(s, d), d


def states(s: RequestSnapshot) -> dict[str, JobState]:
    return {j: r.state for j, r in s.jobs.items()}


# ------------------------------------------------------------- cancelled / inert


@pytest.mark.parametrize(
    "event",
    [
        JobResultReceived(res("a")),
        Tick(),
        PlanCommitted(),
        PublishConfirmed("a", 1),
        PublishFailed("a", 1),
        TimerFired(TIMER_RETRY, "a", 1),
        UnackedDeliveryExhausted("a"),
        CancelObserved(),
    ],
)
def test_cancelled_request_is_inert(event) -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(plan, {"a": S.CANCELLED, "b": S.CANCELLED}, status=RequestStatus.CANCELLED,
             timers=[Timer(TIMER_RETRY, "a", 1, 5.0)])
    d = step(s, event, ctx())
    assert d.is_empty and d.changes == ()


# ------------------------------------------------------------- readiness / dispatch


def test_plan_committed_readies_roots_and_dispatches() -> None:
    plan = make_plan(a=[], b=[], c=["a", "b"])
    new, d = run(snap(plan), PlanCommitted(), ctx(now=10.0))
    assert states(new) == {"a": S.READY, "b": S.READY, "c": S.WAITING}
    assert new.jobs["a"].attempt == 1 and new.jobs["a"].ready_since == 10.0
    pubs = dispatches(d)
    assert sorted(p.job_id for p in pubs) == ["a", "b"]
    assert all(p.attempt == 1 and p.destination == "jobs/research" for p in pubs)
    assert all(p.dependency_results == {} for p in pubs)
    assert {(c.job_id, c.from_state, c.to_state) for c in d.changes} == {
        ("a", S.WAITING, S.READY),
        ("b", S.WAITING, S.READY),
    }


def test_publish_confirmed_moves_ready_to_dispatched() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.READY})
    new, d = run(s, PublishConfirmed("a", 1), ctx())
    assert new.jobs["a"].state is S.DISPATCHED
    assert [(c.from_state, c.to_state) for c in d.changes] == [(S.READY, S.DISPATCHED)]
    # Stale attempt and repeated confirm are ignored.
    assert step(s, PublishConfirmed("a", 2), ctx()).is_empty
    assert step(new, PublishConfirmed("a", 1), ctx()).is_empty


def test_success_records_result_and_dispatches_dependent_with_inputs() -> None:
    plan = make_plan(a=[], b=[], c=["a", "b"])
    s = snap(plan, {"a": S.SUCCEEDED, "b": S.DISPATCHED}, results=[res("a")])
    new, d = run(s, JobResultReceived(res("b")), ctx())
    assert new.jobs["b"].state is S.SUCCEEDED and new.jobs["b"].worker_id == "w-b"
    assert list(d.updates.results) == [res("b")]
    (pub,) = dispatches(d)
    assert pub.job_id == "c" and pub.attempt == 1
    assert pub.dependency_results == {"a": "out a@1", "b": "out b@1"}
    assert pub.to_payload(RID).dependency_results == {"a": "out a@1", "b": "out b@1"}
    assert new.jobs["c"].state is S.READY


def test_waiting_job_with_unfinished_dependency_is_not_dispatched() -> None:
    plan = make_plan(a=[], b=["a"])
    d = step(snap(plan, {"a": S.DISPATCHED}), Tick(), ctx())
    assert d.is_empty


# ------------------------------------------------------------- publish retry (4.8)


def test_publish_failures_retry_then_fail_on_fourth() -> None:
    plan = make_plan(a=[], b=["a"])
    s, _ = run(snap(plan), PlanCommitted(), ctx(now=0.0))
    now = 0.0
    for n in range(1, 4):
        s, d = run(s, PublishFailed("a", 1, n), ctx(now=now))
        assert s.jobs["a"].state is S.READY and s.jobs["a"].publish_failures == n
        assert d.changes == ()
        (timer,) = of(d, ScheduleTimer)
        assert timer.kind == TIMER_PUBLISH and timer.at == now + CFG.publish_retry_delay
        now += CFG.publish_retry_delay
        s, d = run(s, TimerFired(TIMER_PUBLISH, "a", 1), ctx(now=now))
        assert [p.job_id for p in dispatches(d)] == ["a"]
        assert TimerKey(TIMER_PUBLISH, "a", 1) not in s.timer_keys()
    s, d = run(s, PublishFailed("a", 1, 4), ctx(now=now))
    assert s.jobs["a"].state is S.FAILED and s.jobs["a"].reason == REASON_DISPATCH_FAILURE
    assert s.jobs["b"].state is S.SKIPPED  # propagation
    assert dispatches(d) == []


def test_publish_failure_ignored_while_retry_pending() -> None:
    plan = make_plan(a=[])
    s, _ = run(snap(plan), PlanCommitted(), ctx())
    s, _ = run(s, PublishFailed("a", 1), ctx())
    assert step(s, PublishFailed("a", 1), ctx()).is_empty


# ------------------------------------------------------------- capability wait (4.6)


def test_capability_wait_then_no_capable_worker() -> None:
    plan = make_plan(a=[], b=["a"])
    s, d = run(snap(plan), PlanCommitted(), ctx(now=0.0, agents=()))
    assert s.jobs["a"].state is S.READY and dispatches(d) == []
    assert TimerKey(TIMER_CAPABILITY_WAIT, "a", 1) in s.timer_keys()
    assert step(s, Tick(), ctx(now=59.9, agents=())).is_empty
    s, d = run(s, Tick(), ctx(now=60.0, agents=()))
    assert s.jobs["a"].state is S.FAILED and s.jobs["a"].reason == REASON_NO_CAPABLE_WORKER
    assert s.jobs["b"].state is S.SKIPPED
    assert dispatches(d) == [] and s.timers == ()
    # Nothing succeeded and all terminal -> FinalizeFailed.
    (fin,) = of(d, FinalizeFailed)
    assert {e.job_id for e in fin.final_result.failures} == {"a", "b"}


def test_capability_appearing_during_wait_dispatches() -> None:
    plan = make_plan(a=[])
    s, _ = run(snap(plan), PlanCommitted(), ctx(now=0.0, agents=()))
    s, d = run(s, Tick(), ctx(now=30.0))
    assert [p.job_id for p in dispatches(d)] == ["a"]
    assert s.timers == () and s.jobs["a"].state is S.READY
    assert step(s, Tick(), ctx(now=100.0)).is_empty  # publish in flight: no repeat


def test_capability_wait_timer_fire_fails_job() -> None:
    plan = make_plan(a=[])
    s, _ = run(snap(plan), PlanCommitted(), ctx(now=0.0, agents=()))
    s, _ = run(s, TimerFired(TIMER_CAPABILITY_WAIT, "a", 1), ctx(now=60.0, agents=()))
    assert s.jobs["a"].state is S.FAILED and s.jobs["a"].reason == REASON_NO_CAPABLE_WORKER


# ------------------------------------------------------------- idempotency (6.5, 2.8)


def test_duplicate_and_stale_results_are_discarded() -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(plan, {"a": S.SUCCEEDED, "b": S.DISPATCHED}, results=[res("a")])
    assert step(s, JobResultReceived(res("a", status="Failed", error="x")), ctx()).is_empty
    s2 = snap(plan, {"a": S.DISPATCHED}, attempts={"a": 2}, results=[res("a", 1, "Failed")])
    assert step(s2, JobResultReceived(res("a", 1, "Succeeded")), ctx()).is_empty
    assert step(s2, JobResultReceived(res("a", 3, "Succeeded")), ctx()).is_empty
    other = res("a", 2).model_copy(update={"request_id": "other"})
    assert step(s2, JobResultReceived(other), ctx()).is_empty


# ------------------------------------------------------------- retries (6.1, 6.2, 6.3)


@pytest.mark.parametrize("status", ["Failed", "TimedOut"])
def test_failed_attempt_below_max_schedules_backoff_retry(status: str) -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(plan, {"a": S.DISPATCHED})
    s, d = run(s, JobResultReceived(res("a", 1, status, "boom")), ctx(now=100.0))
    assert s.jobs["a"].state is S(status) and s.jobs["a"].reason == "boom"
    assert s.jobs["b"].state is S.WAITING  # not terminal yet: no skip
    (timer,) = of(d, ScheduleTimer)
    assert (timer.kind, timer.attempt) == (TIMER_RETRY, 1)
    assert timer.at == 100.0 + backoff(1, CFG.base_backoff, CFG.max_backoff)
    assert of(d, RunSummarizer) == of(d, FinalizeFailed) == [] == dispatches(d)
    # Timer fires -> attempt 2, Ready, re-dispatched with the same Job contents.
    s, d = run(s, TimerFired(TIMER_RETRY, "a", 1), ctx(now=timer.at))
    assert s.jobs["a"].state is S.READY and s.jobs["a"].attempt == 2
    (pub,) = dispatches(d)
    assert (pub.job_id, pub.attempt, pub.instruction) == ("a", 2, "do a")
    s, _ = run(s, PublishConfirmed("a", 2), ctx())
    assert s.jobs["a"].state is S.DISPATCHED


def test_retry_reuses_dependency_inputs() -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(plan, {"a": S.SUCCEEDED, "b": S.DISPATCHED}, results=[res("a")])
    s, d = run(s, JobResultReceived(res("b", 1, "Failed")), ctx(now=0.0))
    (timer,) = of(d, ScheduleTimer)
    _, d = run(s, TimerFired(TIMER_RETRY, "b", 1), ctx(now=timer.at))
    assert dispatches(d)[0].dependency_results == {"a": "out a@1"}


def test_backoff_doubles_per_attempt() -> None:
    plan = make_plan(a=[])
    cfg = MachineConfig(max_attempts=5, base_backoff=2, max_backoff=60)
    s = snap(plan, {"a": S.DISPATCHED}, attempts={"a": 3})
    _, d = run(s, JobResultReceived(res("a", 3, "Failed")), ctx(now=0.0, config=cfg))
    assert of(d, ScheduleTimer)[0].at == 8.0


def test_max_attempts_fails_dead_letters_and_skips_dependents() -> None:
    plan = make_plan(a=[], b=["a"], c=["b"], d=[])
    s = snap(plan, {"a": S.DISPATCHED, "d": S.DISPATCHED}, attempts={"a": 3})
    s, d = run(s, JobResultReceived(res("a", 3, "TimedOut", "too slow")), ctx())
    assert s.jobs["a"].state is S.FAILED and s.jobs["a"].reason == "too slow"
    assert of(d, PublishDeadLetter) == [PublishDeadLetter(RID, "a", 3, "too slow")]
    assert s.jobs["b"].state is S.SKIPPED and s.jobs["b"].reason == "skipped: dependency a failed"
    assert s.jobs["c"].state is S.SKIPPED and s.jobs["c"].reason == "skipped: dependency a failed"
    assert s.jobs["d"].state is S.DISPATCHED  # independent Job untouched
    assert of(d, ScheduleTimer) == [] and dispatches(d) == []


def test_cancelled_result_no_retry_no_dead_letter() -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(plan, {"a": S.DISPATCHED})
    s, d = run(s, JobResultReceived(res("a", 1, "Cancelled")), ctx())
    assert s.jobs["a"].state is S.CANCELLED
    assert of(d, PublishDeadLetter) == of(d, ScheduleTimer) == []
    assert s.jobs["b"].state is S.SKIPPED
    assert s.jobs["b"].reason == "skipped: dependency a cancelled"


def test_unacked_delivery_exhausted_fails_job() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.DISPATCHED})
    s, d = run(s, UnackedDeliveryExhausted("a"), ctx())
    assert s.jobs["a"].state is S.FAILED and s.jobs["a"].reason == REASON_UNACKED_DELIVERY
    assert of(d, PublishDeadLetter) == []  # the broker already dead-lettered it
    again = step(s, UnackedDeliveryExhausted("a"), ctx())
    assert not again.updates.jobs and again.changes == ()


# ------------------------------------------------------------- summarization (7.1, 7.3)


def test_all_terminal_with_success_runs_summarizer_with_succeeded_contents_only() -> None:
    plan = make_plan(a=[], b=[], c=["b"])
    s = snap(plan, {"a": S.DISPATCHED, "b": S.FAILED, "c": S.SKIPPED}, attempts={"b": 3, "c": 0},
             results=[res("b", 3, "Failed", "bad")])
    _, d = run(s, JobResultReceived(res("a")), ctx())
    assert of(d, RunSummarizer) == [RunSummarizer("the prompt", {"a": "out a@1"})]
    assert of(d, FinalizeFailed) == []


def test_all_terminal_no_success_finalizes_failed() -> None:
    plan = make_plan(a=[], b=[])
    s = snap(plan, {"a": S.CANCELLED, "b": S.DISPATCHED}, attempts={"a": 1})
    _, d = run(s, JobResultReceived(res("b", 1, "Cancelled", "stop")), ctx())
    (fin,) = of(d, FinalizeFailed)
    assert [(r.job_id, r.state) for r in fin.final_result.jobs] == [
        ("a", S.CANCELLED),
        ("b", S.CANCELLED),
    ]
    assert of(d, RunSummarizer) == []


# ------------------------------------------------------------- cancel observed


def test_cancel_observed_sends_cancel_to_in_flight_jobs() -> None:
    plan = make_plan(a=[], b=[], c=[])
    s = snap(plan, {"a": S.DISPATCHED, "b": S.SUCCEEDED}, results=[res("b")])
    d = step(s, CancelObserved(), ctx())
    assert d.effects == (SendCancel("a", 1),) and not d.updates.jobs


# ------------------------------------------------------------- resume (2.6)


def test_resume_redispatches_exactly_unfinished_in_flight_jobs() -> None:
    plan = make_plan(a=[], b=["a"], c=["a"], d=["a"], e=[], f=[], g=[], h=[])
    s = snap(
        plan,
        {
            "a": S.SUCCEEDED,
            "b": S.READY,  # publish in flight
            "c": S.DISPATCHED,  # no result
            "d": S.DISPATCHED,  # result recorded but not yet applied
            "e": S.WAITING,
            "f": S.FAILED,
            "g": S.READY,  # waiting for capability
            "h": S.TIMED_OUT,
        },
        results=[res("a"), res("d"), res("h", 1, "TimedOut")],
        timers=[Timer(TIMER_CAPABILITY_WAIT, "g", 1, 50.0), Timer(TIMER_RETRY, "h", 1, 40.0)],
    )
    effects = resume(s, WORKERS)
    pubs = [e for e in effects if isinstance(e, PublishDispatch)]
    assert sorted(p.job_id for p in pubs) == ["b", "c"]
    assert all(p.dependency_results == {"a": "out a@1"} for p in pubs)
    timers = [e for e in effects if isinstance(e, ScheduleTimer)]
    assert timers == [ScheduleTimer(40.0, TIMER_RETRY, "h", 1),
                      ScheduleTimer(50.0, TIMER_CAPABILITY_WAIT, "g", 1)]


def test_resume_on_cancelled_request_does_nothing() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.DISPATCHED}, status=RequestStatus.CANCELLED)
    assert resume(s, WORKERS) == []


# ------------------------------------------------------------- store mapping (13.7)


async def test_decision_commits_through_apply_transition(state_store) -> None:
    """Each StateChange becomes exactly one transition entry; store == apply_decision."""
    plan = make_plan(a=[], b=["a"])
    rid = await state_store.create_request("the prompt")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.commit_plan(lease, plan)).ok

    def load_snap(stored) -> RequestSnapshot:
        return RequestSnapshot.from_stored(stored)

    events = [
        PlanCommitted(),
        PublishConfirmed("a", 1),
        JobResultReceived(res("a", 1, "Failed", "e1").model_copy(update={"request_id": rid})),
        TimerFired(TIMER_RETRY, "a", 1),
        PublishConfirmed("a", 2),
        JobResultReceived(res("a", 2).model_copy(update={"request_id": rid})),
    ]
    now = 0.0
    for event in events:
        current = load_snap(await state_store.load_snapshot(rid))
        if isinstance(event, TimerFired):
            now = next(t.due for t in current.timers)
        d = step(current, event, ctx(now=now))
        before = len((await state_store.load_snapshot(rid, include_transitions=True)).transitions)
        wr = await state_store.apply_transition(lease, d.updates)
        assert wr.ok and wr.transitions_written == len(d.changes)
        after = await state_store.load_snapshot(rid, include_transitions=True)
        assert len(after.transitions) == before + len(d.changes)
        expected = apply_decision(current, d)
        stored = load_snap(after)
        assert stored.jobs == expected.jobs
        assert stored.results == expected.results
        assert {t.key for t in stored.timers} == {t.key for t in expected.timers}
    final = load_snap(await state_store.load_snapshot(rid))
    assert states(final) == {"a": S.SUCCEEDED, "b": S.READY}
