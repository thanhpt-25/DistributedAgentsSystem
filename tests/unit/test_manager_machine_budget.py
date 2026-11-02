"""Unit tests for budget-stop mode in the RequestMachine (das-client-api task 5.1,
Phase 4 Req 5.6, 5.7, 5.8)."""
from __future__ import annotations

from decimal import Decimal

from das.client.budget import Budget
from das.manager.machine import (
    BUDGET_EXCEEDED,
    BUDGET_UNENFORCEABLE,
    TIMER_CAPABILITY_WAIT,
    TIMER_PUBLISH,
    TIMER_RETRY,
    FinalizeFailed,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishDeadLetter,
    PublishDispatch,
    PublishFailed,
    ReplayRequested,
    RequestSnapshot,
    RunSummarizer,
    ScheduleTimer,
    StepContext,
    Tick,
    TimerFired,
    apply_decision,
    budget_exceeded,
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
from das.models.usage import UsageRecord
from das.ops.cost import PriceEntry, PriceTable
from das.state import ClientRequestSpec, StateStore, Timer
from das.state.store import StoredRequest

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
# 1 per million input and output tokens: 2,000 tokens cost 0.002.
PRICES = PriceTable(
    currency="USD",
    models=[
        PriceEntry(
            provider="openai",
            model="gpt-x",
            input_per_million=Decimal("1"),
            output_per_million=Decimal("1"),
        )
    ],
)
OTHER_PRICES = PriceTable(
    currency="USD",
    models=[
        PriceEntry(
            provider="anthropic",
            model="m",
            input_per_million=Decimal("1"),
            output_per_million=Decimal("1"),
        )
    ],
)


def ctx(now: float = 100.0, agents=WORKERS, price_table: PriceTable | None = None) -> StepContext:
    return StepContext(now=now, config=CFG, agents=tuple(agents), price_table=price_table)


def make_plan(**deps: list[str]) -> JobPlan:
    return JobPlan(
        jobs=[
            PlannedJob(job_id=j, instruction=f"do {j}", capabilities=["research"], depends_on=d)
            for j, d in deps.items()
        ]
    )


def res(job_id: str, attempt: int = 1, status: str = "Succeeded", tokens: int = 0) -> JobResultPayload:
    """A Job_Result carrying ``tokens`` input and ``tokens`` output tokens."""
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}",
        status=status,  # type: ignore[arg-type]
        content=f"out {job_id}@{attempt}",
        error=None if status == "Succeeded" else "boom",
        usage=TokenUsage(input_tokens=tokens, output_tokens=tokens, total_tokens=2 * tokens),
        duration_ms=1,
    )


def usage_record(total: int) -> UsageRecord:
    """A planning Usage_Record of ``total`` tokens."""
    return UsageRecord(
        operation="plan",
        provider="openai",
        model="gpt-x",
        usage=TokenUsage(input_tokens=total, output_tokens=0, total_tokens=total),
    )


def snap(
    plan: JobPlan,
    states: dict[str, JobState] | None = None,
    attempts: dict[str, int] | None = None,
    results=(),
    timers=(),
    usage: tuple[UsageRecord, ...] = (),
    budget: Budget | None = None,
) -> RequestSnapshot:
    states = states or {}
    attempts = attempts or {}
    jobs = {
        j.job_id: JobRecord(
            job=j,
            state=states.get(j.job_id, S.WAITING),
            attempt=attempts.get(j.job_id, 0 if states.get(j.job_id, S.WAITING) is S.WAITING else 1),
        )
        for j in plan.jobs
    }
    return RequestSnapshot(
        request_id=RID,
        status=RequestStatus.RUNNING,
        prompt="p",
        plan=plan,
        jobs=jobs,
        results={(r.job_id, r.attempt): r for r in results},
        timers=tuple(timers),
        usage=usage,
        budget=budget,
    )


def dispatched(decision) -> list[tuple[str, int]]:
    return [(e.job_id, e.attempt) for e in decision.effects if isinstance(e, PublishDispatch)]


def of_type(decision, cls) -> list:
    return [e for e in decision.effects if isinstance(e, cls)]


def run(s: RequestSnapshot, event, c: StepContext | None = None):
    d = step(s, event, c or ctx())
    return d, apply_decision(s, d)


# --------------------------------------------------------------------------- no budget


def test_no_budget_chain_runs_to_summarizer() -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(plan, usage=(usage_record(10_000),))
    d, s = run(s, PlanCommitted())
    assert dispatched(d) == [("a", 1)]
    d, s = run(s, PublishConfirmed("a", 1))
    d, s = run(s, JobResultReceived(res("a", tokens=10_000), META))
    assert dispatched(d) == [("b", 1)]
    d, s = run(s, PublishConfirmed("b", 1))
    d, s = run(s, JobResultReceived(res("b", tokens=10_000), META))
    assert len(of_type(d, RunSummarizer)) == 1
    assert of_type(d, FinalizeFailed) == []
    assert budget_exceeded(s, ctx()) is None


# --------------------------------------------------------------------------- budget stop


def test_planning_usage_alone_exceeds_budget() -> None:
    plan = make_plan(a=[], b=["a"], c=[])
    s = snap(plan, usage=(usage_record(100),), budget=Budget(max_total_tokens=50))
    assert budget_exceeded(s, ctx()) == "tokens"
    d, after = run(s, PlanCommitted())
    assert dispatched(d) == []
    for job_id in ("a", "b", "c"):
        assert after.jobs[job_id].state is S.SKIPPED
        assert after.jobs[job_id].reason == BUDGET_EXCEEDED
    [final] = of_type(d, FinalizeFailed)
    assert final.budget_kind == "tokens"
    assert final.final_result.error == BUDGET_EXCEEDED
    assert of_type(d, RunSummarizer) == []
    assert after.budget == Budget(max_total_tokens=50)


def test_mid_run_stop_lets_in_flight_job_finish() -> None:
    plan = make_plan(a=[], b=[], c=["a", "b"])
    s = snap(plan, budget=Budget(max_total_tokens=50))
    d, s = run(s, PlanCommitted())
    assert sorted(dispatched(d)) == [("a", 1), ("b", 1)]
    _, s = run(s, PublishConfirmed("a", 1))
    _, s = run(s, PublishConfirmed("b", 1))

    d, s = run(s, JobResultReceived(res("a", tokens=30), META))  # 60 tokens >= 50
    assert dispatched(d) == []
    assert s.jobs["a"].state is S.SUCCEEDED
    assert s.jobs["b"].state is S.DISPATCHED
    assert s.jobs["c"].state is S.SKIPPED and s.jobs["c"].reason == BUDGET_EXCEEDED
    assert of_type(d, FinalizeFailed) == []
    assert of_type(d, RunSummarizer) == []

    d, s = run(s, Tick())
    assert d.is_empty

    d, s = run(s, JobResultReceived(res("b", tokens=5), META))
    assert s.jobs["b"].state is S.SUCCEEDED
    [final] = of_type(d, FinalizeFailed)
    assert final.budget_kind == "tokens"
    assert final.final_result.error == BUDGET_EXCEEDED
    assert of_type(d, RunSummarizer) == []
    assert dispatched(d) == []


def test_failed_result_crossing_limit_is_not_retried() -> None:
    plan = make_plan(a=[], b=[])
    for status in ("Failed", "TimedOut"):
        s = snap(plan, {"a": S.DISPATCHED, "b": S.DISPATCHED}, budget=Budget(max_total_tokens=10))
        d, after = run(s, JobResultReceived(res("a", 1, status, tokens=5), META))
        assert after.jobs["a"].state is S.FAILED
        assert after.jobs["a"].reason == BUDGET_EXCEEDED
        assert of_type(d, ScheduleTimer) == []
        assert of_type(d, PublishDeadLetter) == []
        assert d.updates.timers_add == ()
        assert len(d.updates.usage) == 1
        assert of_type(d, FinalizeFailed) == []  # b is still in flight


def test_failed_result_on_final_attempt_is_not_dead_lettered() -> None:
    plan = make_plan(a=[])
    s = snap(plan, {"a": S.DISPATCHED}, attempts={"a": 3}, budget=Budget(max_total_tokens=10))
    d, after = run(s, JobResultReceived(res("a", 3, "Failed", tokens=5), META))
    assert after.jobs["a"].state is S.FAILED and after.jobs["a"].reason == BUDGET_EXCEEDED
    assert of_type(d, PublishDeadLetter) == []
    [final] = of_type(d, FinalizeFailed)
    assert final.budget_kind == "tokens"


def test_retry_pending_when_stop_starts() -> None:
    plan = make_plan(a=[], b=[])
    for state in (S.FAILED, S.TIMED_OUT):
        retry = Timer(TIMER_RETRY, "a", 1, 150.0)
        s = snap(
            plan,
            {"a": state, "b": S.DISPATCHED},
            results=[res("a", 1, "Failed" if state is S.FAILED else "TimedOut")],
            timers=[retry],
            usage=(usage_record(100),),
            budget=Budget(max_total_tokens=50),
        )
        d, after = run(s, Tick())
        assert after.jobs["a"].state is S.FAILED
        assert after.jobs["a"].reason == BUDGET_EXCEEDED
        assert retry.key in d.updates.timers_remove
        assert after.timers == ()
        assert dispatched(d) == []
        assert of_type(d, FinalizeFailed) == []  # b in flight


def test_retry_timer_firing_during_stop_does_not_redispatch() -> None:
    plan = make_plan(a=[])
    retry = Timer(TIMER_RETRY, "a", 1, 150.0)
    s = snap(
        plan,
        {"a": S.TIMED_OUT},
        results=[res("a", 1, "TimedOut")],
        timers=[retry],
        usage=(usage_record(100),),
        budget=Budget(max_total_tokens=50),
    )
    d, after = run(s, TimerFired(TIMER_RETRY, "a", 1), ctx(now=150.0))
    assert after.jobs["a"].state is S.FAILED and after.jobs["a"].reason == BUDGET_EXCEEDED
    assert retry.key in d.updates.timers_remove
    assert dispatched(d) == []
    assert [c.job_id for c in d.changes] == ["a"]
    assert of_type(d, FinalizeFailed)[0].budget_kind == "tokens"


def test_result_crossing_limit_stops_pending_retry_of_other_job() -> None:
    plan = make_plan(a=[], b=[])
    retry = Timer(TIMER_RETRY, "a", 1, 150.0)
    s = snap(
        plan,
        {"a": S.FAILED, "b": S.DISPATCHED},
        results=[res("a", 1, "Failed")],
        timers=[retry],
        budget=Budget(max_total_tokens=50),
    )
    d, after = run(s, JobResultReceived(res("b", tokens=30), META))
    assert after.jobs["a"].state is S.FAILED and after.jobs["a"].reason == BUDGET_EXCEEDED
    assert retry.key in d.updates.timers_remove
    [final] = of_type(d, FinalizeFailed)
    assert final.budget_kind == "tokens"


def test_ready_job_with_capwait_timer_is_skipped() -> None:
    plan = make_plan(a=[], b=[])
    capwait = Timer(TIMER_CAPABILITY_WAIT, "a", 1, 160.0)
    s = snap(
        plan,
        {"a": S.READY, "b": S.DISPATCHED},
        timers=[capwait],
        usage=(usage_record(100),),
        budget=Budget(max_total_tokens=50),
    )
    d, after = run(s, Tick())
    assert after.jobs["a"].state is S.SKIPPED and after.jobs["a"].reason == BUDGET_EXCEEDED
    assert capwait.key in d.updates.timers_remove
    assert after.jobs["b"].state is S.DISPATCHED
    assert dispatched(d) == []


def test_publish_timer_firing_during_stop_skips_job() -> None:
    plan = make_plan(a=[])
    publish = Timer(TIMER_PUBLISH, "a", 1, 101.0)
    s = snap(plan, {"a": S.READY}, timers=[publish], usage=(usage_record(100),),
             budget=Budget(max_total_tokens=50))
    d, after = run(s, TimerFired(TIMER_PUBLISH, "a", 1), ctx(now=101.0))
    assert dispatched(d) == []
    assert after.jobs["a"].state is S.SKIPPED and after.jobs["a"].reason == BUDGET_EXCEEDED
    assert publish.key in d.updates.timers_remove
    assert of_type(d, FinalizeFailed)[0].budget_kind == "tokens"


def test_publish_failure_during_stop_skips_job() -> None:
    plan = make_plan(a=[], b=[])
    s = snap(plan, {"a": S.READY, "b": S.DISPATCHED}, usage=(usage_record(100),),
             budget=Budget(max_total_tokens=50))
    d, after = run(s, PublishFailed("a", 1, n=1))
    assert after.jobs["a"].state is S.SKIPPED and after.jobs["a"].reason == BUDGET_EXCEEDED
    assert d.updates.timers_add == ()
    assert of_type(d, ScheduleTimer) == []
    assert dispatched(d) == []


def test_replay_during_stop_is_reverted() -> None:
    plan = make_plan(a=[], b=["a"])
    s = snap(
        plan,
        {"a": S.FAILED, "b": S.SKIPPED},
        attempts={"a": 3},
        results=[res("a", 3, "Failed")],
        usage=(usage_record(100),),
        budget=Budget(max_total_tokens=50),
    )
    d, after = run(s, ReplayRequested(DLID, "a", 3))
    assert list(d.updates.dead_letter_reverts) == [DLID]
    assert list(d.updates.replays_applied) == [DLID]
    assert any("budget exceeded" in w for w in d.warnings)
    assert after.jobs["a"].state is S.FAILED and after.jobs["a"].attempt == 3
    assert dispatched(d) == []
    assert d.updates.tag is None


# --------------------------------------------------------------------------- cost


def test_max_cost_without_price_table_is_unenforceable() -> None:
    plan = make_plan(a=[])
    s = snap(plan, budget=Budget(max_cost=Decimal("1")))
    assert budget_exceeded(s, ctx()) == "unenforceable"
    d, after = run(s, PlanCommitted())
    assert dispatched(d) == []
    assert after.jobs["a"].state is S.SKIPPED
    [final] = of_type(d, FinalizeFailed)
    assert final.budget_kind == "unenforceable"
    assert final.final_result.error == BUDGET_UNENFORCEABLE


def test_max_cost_with_unpriced_result_is_unenforceable() -> None:
    plan = make_plan(a=[], b=["a"])
    c = ctx(price_table=OTHER_PRICES)
    s = snap(plan, budget=Budget(max_cost=Decimal("1")))
    d, s = run(s, PlanCommitted(), c)
    assert dispatched(d) == [("a", 1)]
    d, s = run(s, JobResultReceived(res("a", tokens=1), META), c)
    assert dispatched(d) == []
    assert s.jobs["b"].state is S.SKIPPED
    [final] = of_type(d, FinalizeFailed)
    assert final.budget_kind == "unenforceable"
    assert final.final_result.error == BUDGET_UNENFORCEABLE


def test_cost_within_budget_runs_normally() -> None:
    plan = make_plan(a=[], b=["a"])
    c = ctx(price_table=PRICES)
    s = snap(plan, budget=Budget(max_cost=Decimal("1")))
    d, s = run(s, PlanCommitted(), c)
    assert dispatched(d) == [("a", 1)]
    d, s = run(s, JobResultReceived(res("a", tokens=1000), META), c)
    assert dispatched(d) == [("b", 1)]
    d, s = run(s, JobResultReceived(res("b", tokens=1000), META), c)
    assert len(of_type(d, RunSummarizer)) == 1
    assert of_type(d, FinalizeFailed) == []
    assert budget_exceeded(s, c) is None


def test_cost_reaching_limit_stops() -> None:
    plan = make_plan(a=[], b=["a"])
    c = ctx(price_table=PRICES)
    s = snap(plan, budget=Budget(max_cost=Decimal("0.002")))
    _, s = run(s, PlanCommitted(), c)
    d, s = run(s, JobResultReceived(res("a", tokens=1000), META), c)  # 2,000 tokens cost 0.002
    assert dispatched(d) == []
    [final] = of_type(d, FinalizeFailed)
    assert final.budget_kind == "cost"
    assert final.final_result.error == BUDGET_EXCEEDED


# --------------------------------------------------------------------------- snapshots


def _stored(plan: JobPlan, **extra) -> StoredRequest:
    return StoredRequest(
        request_id=RID,
        status=RequestStatus.RUNNING,
        prompt="p",
        created_at=0.0,
        error=None,
        final_result=None,
        plan=plan,
        jobs={"a": JobRecord(job=plan.jobs[0], state=S.WAITING, attempt=0)},
        results={},
        timers=[],
        inbox_cursor="0-0",
        lease=None,
        **extra,
    )


def test_from_stored_copies_budget_and_usage() -> None:
    plan = make_plan(a=[])
    usage = [usage_record(7)]
    s = RequestSnapshot.from_stored(
        _stored(plan, usage=usage, budget=Budget(max_total_tokens=5), client_fp="fp")
    )
    assert s.budget == Budget(max_total_tokens=5)
    assert s.usage == tuple(usage)
    bare = RequestSnapshot.from_stored(_stored(plan))
    assert bare.budget is None and bare.usage == ()


async def test_load_snapshot_reads_budget_and_client_fp(state_store: StateStore) -> None:
    created = await state_store.submit_request(
        "p", ClientRequestSpec(fingerprint="cbabecbd", budget=Budget(max_total_tokens=5))
    )
    assert created.request_id is not None
    stored = await state_store.load_snapshot(created.request_id)
    assert stored is not None
    assert stored.budget == Budget(max_total_tokens=5)
    assert stored.client_fp == "cbabecbd"

    rid = await state_store.create_request("p")
    plain = await state_store.load_snapshot(rid)
    assert plain is not None
    assert plain.budget is None and plain.client_fp is None
