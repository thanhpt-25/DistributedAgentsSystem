"""Unit tests for the Phase 2 RequestMachine extensions: Replay (Req 13.4, 13.5, 13.10,
13.15), the raised attempt limit, and job Usage_Records (Req 9.2, 9.5)."""
from __future__ import annotations

from das.manager.machine import (
    TIMER_RETRY,
    FinalizeFailed,
    JobResultReceived,
    MachineConfig,
    PublishDeadLetter,
    PublishDispatch,
    ReplayRequested,
    RequestSnapshot,
    RunSummarizer,
    StepContext,
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
from das.state import Timer, TransitionBatch

S = JobState
RID = "r1"
DLID = "ab" * 16
CFG = MachineConfig(max_attempts=3, base_backoff=2, max_backoff=60, capability_wait=60)
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
META = {"provider": "openai", "model": "gpt-x"}


def ctx(now: float = 100.0, agents=WORKERS) -> StepContext:
    return StepContext(now=now, config=CFG, agents=tuple(agents))


def make_plan(**deps: list[str]) -> JobPlan:
    return JobPlan(
        jobs=[
            PlannedJob(job_id=j, instruction=f"do {j}", capabilities=["research"], depends_on=d)
            for j, d in deps.items()
        ]
    )


def res(job_id: str, attempt: int = 1, status: str = "Succeeded", usage: int = 0):
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}",
        status=status,  # type: ignore[arg-type]
        content=f"out {job_id}@{attempt}",
        error=None if status == "Succeeded" else "boom",
        usage=TokenUsage(input_tokens=usage, output_tokens=usage, total_tokens=2 * usage),
        duration_ms=1,
    )


def snap(
    plan: JobPlan,
    states: dict[str, JobState],
    attempts: dict[str, int] | None = None,
    results=(),
    timers=(),
    status: RequestStatus = RequestStatus.RUNNING,
    limits: dict[str, int] | None = None,
) -> RequestSnapshot:
    attempts = attempts or {}
    limits = limits or {}
    jobs = {
        j.job_id: JobRecord(
            job=j,
            state=states.get(j.job_id, S.WAITING),
            attempt=attempts.get(j.job_id, 0 if states.get(j.job_id, S.WAITING) is S.WAITING else 1),
            reason="r" if states.get(j.job_id) in (S.FAILED, S.SKIPPED) else None,
            attempt_limit=limits.get(j.job_id),
        )
        for j in plan.jobs
    }
    return RequestSnapshot(
        request_id=RID,
        status=status,
        prompt="p",
        plan=plan,
        jobs=jobs,
        results={(r.job_id, r.attempt): r for r in results},
        timers=tuple(timers),
        pending_replays=frozenset({DLID}),
    )


# --------------------------------------------------------------------------- replay


def test_replay_applied_sets_ready_with_raised_limit_and_resets_skipped() -> None:
    # a -> b -> c, d independent and Succeeded; a failed at attempt 3, b and c Skipped.
    plan = make_plan(a=[], b=["a"], c=["b"], d=[])
    s = snap(
        plan,
        {"a": S.FAILED, "b": S.SKIPPED, "c": S.SKIPPED, "d": S.SUCCEEDED},
        attempts={"a": 3},
        results=[res("d")],
    )
    d = step(s, ReplayRequested(DLID, "a", 3), ctx())
    after = apply_decision(s, d)

    a = after.jobs["a"]
    assert a.state is S.READY and a.attempt == 4
    assert a.attempt_limit == 3 + CFG.max_attempts
    assert a.reason is None
    assert after.jobs["b"].state is S.WAITING and after.jobs["b"].reason is None
    assert after.jobs["c"].state is S.WAITING
    assert after.jobs["d"].state is S.SUCCEEDED
    assert [(e.job_id, e.attempt) for e in d.effects if isinstance(e, PublishDispatch)] == [
        ("a", 4)
    ]
    assert d.updates.tag == DLID
    assert list(d.updates.replays_applied) == [DLID]
    assert list(d.updates.dead_letter_reverts) == []
    assert d.warnings == ()
    assert after.pending_replays == frozenset()
    # Every change is one transition entry.
    assert {c.job_id for c in d.changes} == {"a", "b", "c"}


def test_replay_does_not_reset_skipped_with_other_root_cause() -> None:
    # c depends on a (replayed) and b (also Failed): its causes are {a, b}.
    plan = make_plan(a=[], b=[], c=["a", "b"])
    s = snap(plan, {"a": S.FAILED, "b": S.FAILED, "c": S.SKIPPED}, attempts={"a": 3, "b": 3})
    d = step(s, ReplayRequested(DLID, "a", 3), ctx())
    after = apply_decision(s, d)
    assert after.jobs["a"].state is S.READY
    assert after.jobs["b"].state is S.FAILED
    assert after.jobs["c"].state is S.SKIPPED


def test_replay_without_capable_worker_starts_capability_wait() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.FAILED}, attempts={"a": 3})
    d = step(s, ReplayRequested(DLID, "a", 3), ctx(agents=()))
    after = apply_decision(s, d)
    assert after.jobs["a"].state is S.READY
    assert any(t.kind == "capwait" and t.attempt == 4 for t in after.timers)
    assert not [e for e in d.effects if isinstance(e, PublishDispatch)]


def _assert_reverted(s: RequestSnapshot, ev: ReplayRequested) -> None:
    d = step(s, ev, ctx())
    assert d.updates.jobs == {}
    assert d.changes == ()
    assert list(d.updates.dead_letter_reverts) == [DLID]
    assert list(d.updates.replays_applied) == [DLID]
    assert d.updates.tag is None
    assert len(d.warnings) == 1 and DLID in d.warnings[0]


def test_replay_reverted_when_job_not_failed() -> None:
    plan = make_plan(a=[])
    _assert_reverted(snap(plan, {"a": S.DISPATCHED}, attempts={"a": 3}), ReplayRequested(DLID, "a", 3))


def test_replay_reverted_on_attempt_mismatch() -> None:
    plan = make_plan(a=[])
    _assert_reverted(snap(plan, {"a": S.FAILED}, attempts={"a": 3}), ReplayRequested(DLID, "a", 2))


def test_replay_reverted_when_job_awaiting_retry() -> None:
    plan = make_plan(a=[])
    s = snap(
        plan,
        {"a": S.FAILED},
        attempts={"a": 1},
        timers=[Timer(kind=TIMER_RETRY, job_id="a", attempt=1, due=200.0)],
    )
    _assert_reverted(s, ReplayRequested(DLID, "a", 1))


def test_replay_reverted_for_unknown_job_and_terminal_request() -> None:
    plan = make_plan(a=[])
    _assert_reverted(snap(plan, {"a": S.FAILED}, attempts={"a": 3}), ReplayRequested(DLID, "zz", 3))
    terminal = snap(plan, {"a": S.FAILED}, attempts={"a": 3}, status=RequestStatus.COMPLETED)
    _assert_reverted(terminal, ReplayRequested(DLID, "a", 3))


def test_replayed_job_uses_raised_limit_then_dead_letters() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.FAILED}, attempts={"a": 3})
    s = apply_decision(s, step(s, ReplayRequested(DLID, "a", 3), ctx()))
    assert attempt_limit(s.jobs["a"], CFG) == 6
    # Attempt 4 failing is below the raised limit: a retry is scheduled, no dead letter.
    d = step(s, JobResultReceived(res("a", 4, "Failed")), ctx())
    assert not [e for e in d.effects if isinstance(e, PublishDeadLetter)]
    assert any(t.kind == TIMER_RETRY for t in d.updates.timers_add)
    # Attempt 6 is the last one.
    s6 = snap(plan, {"a": S.DISPATCHED}, attempts={"a": 6}, limits={"a": 6})
    d6 = step(s6, JobResultReceived(res("a", 6, "Failed")), ctx())
    assert [e.attempts for e in d6.effects if isinstance(e, PublishDeadLetter)] == [6]
    assert any(isinstance(e, FinalizeFailed) for e in d6.effects)


def test_replay_then_success_triggers_summarizer_and_stale_result_ignored() -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(plan, {"a": S.FAILED, "b": S.SKIPPED}, attempts={"a": 3, "b": 0})
    s = apply_decision(s, step(s, ReplayRequested(DLID, "a", 3), ctx()))
    # A late result of the pre-replay attempt is discarded and produces no usage.
    d_old = step(s, JobResultReceived(res("a", 3, usage=5), META), ctx())
    assert d_old.updates.results == () and d_old.updates.usage == ()
    s = apply_decision(s, step(s, JobResultReceived(res("a", 4), META), ctx()))
    assert s.jobs["b"].state is S.READY
    d = step(s, JobResultReceived(res("b", 1), META), ctx())
    assert any(isinstance(e, RunSummarizer) for e in d.effects)


# --------------------------------------------------------------------------- usage


def test_accepted_result_produces_one_usage_record() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.DISPATCHED})
    d = step(s, JobResultReceived(res("a", 1, usage=7), META), ctx())
    (u,) = d.updates.usage
    assert (u.operation, u.provider, u.model, u.job_id, u.attempt) == (
        "job", "openai", "gpt-x", "a", 1,
    )
    assert u.usage.total_tokens == 14
    after = apply_decision(s, d)
    assert after.usage == (u,)


def test_missing_meta_records_unknown_provider_model() -> None:
    plan = make_plan(a=[])
    d = step(snap(plan, {"a": S.DISPATCHED}), JobResultReceived(res("a")), ctx())
    (u,) = d.updates.usage
    assert (u.provider, u.model) == ("unknown", "unknown")


def test_duplicate_and_future_results_produce_no_usage() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.DISPATCHED})
    s = apply_decision(s, step(s, JobResultReceived(res("a", 1, usage=3), META), ctx()))
    assert len(s.usage) == 1
    for ev in (JobResultReceived(res("a", 1, usage=3), META), JobResultReceived(res("a", 2), META)):
        d = step(s, ev, ctx())
        assert d.updates.usage == ()
        assert apply_decision(s, d).usage == s.usage


def test_apply_decision_appends_usage_only_for_new_result() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.DISPATCHED}, results=[res("a", 1)])
    d = step(snap(plan, {"a": S.DISPATCHED}), JobResultReceived(res("a", 1, usage=2), META), ctx())
    # Same decision committed against a store that already holds the result.
    assert apply_decision(s, d).usage == ()


def test_transition_batch_defaults() -> None:
    b = TransitionBatch()
    assert (b.usage, b.tag, b.replays_applied, b.dead_letter_reverts) == ((), None, (), ())
