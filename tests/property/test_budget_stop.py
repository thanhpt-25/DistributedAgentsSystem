# Feature: das-client-api, Property 5: Budget stop
"""Property test for budget-stop mode in the reducer (das-client-api task 5.2).

A random Job_Plan, Budget and optional planning usage are driven through ``step`` /
``apply_decision`` by a model Worker: results with random status and token usage for
In_Flight_Jobs, ``PublishConfirmed`` / ``PublishFailed``, firing due timers, and
``Tick``. The model records the step at which ``budget_exceeded`` first reports a kind
(the stop step), Budget_Used then, and the In_Flight_Jobs then (glossary definition:
Ready or Dispatched with attempt >= 1 and no result for that attempt, excluding a Ready
Job with a pending ``publish`` or ``capwait`` timer). It checks:

- from the stop step on, no decision contains ``PublishDispatch`` or schedules a timer
  (in particular no ``retry`` timer);
- every Usage_Record added after the stop step belongs to the current attempt of a Job
  that was in flight at the stop step (one per Job), so final Budget_Used minus
  Budget_Used at the stop step is at most the usage of those Jobs' results;
- ``FinalizeFailed`` with ``budget_kind`` is emitted exactly on the steps after which no
  Job is in flight, with the matching error; ``RunSummarizer`` is never emitted;
- before the stop step, every decision (and so every ``PublishDispatch``) equals the one
  the same events produce with ``budget=None``.

**Validates: Requirements 5.6, 5.7, 5.8**
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

from hypothesis import event as hevent
from hypothesis import given
from hypothesis import strategies as st

from das.client.budget import Budget
from das.manager.machine import (
    BUDGET_EXCEEDED,
    BUDGET_UNENFORCEABLE,
    TIMER_CAPABILITY_WAIT,
    TIMER_PUBLISH,
    FinalizeFailed,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishDispatch,
    PublishFailed,
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
    RequestStatus,
    TokenUsage,
)
from das.models.usage import UsageRecord
from das.ops.cost import PriceEntry, PriceTable
from das.state import TimerKey
from tests.strategies import job_plans

S = JobState
RID = "r1"
CFG = MachineConfig(max_attempts=3, base_backoff=2, max_backoff=60, capability_wait=60)
META = {"provider": "openai", "model": "gpt-x"}
# 1,000 per million tokens: each token costs 0.001.
PRICES = PriceTable(
    currency="USD",
    models=[
        PriceEntry(
            provider="openai",
            model="gpt-x",
            input_per_million=Decimal("1000"),
            output_per_million=Decimal("1000"),
        )
    ],
)
MAX_EVENTS = 40


def _worker(caps: frozenset[str]) -> AgentRecord:
    return AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=caps,
        status="Available",
        last_heartbeat=0.0,
    )


@st.composite
def budgets(draw: st.DrawFn) -> tuple[Budget, PriceTable | None]:
    if draw(st.booleans()):
        return Budget(max_total_tokens=draw(st.integers(min_value=1, max_value=300))), None
    cost = Decimal(draw(st.integers(min_value=1, max_value=300))) / 1000
    table = draw(st.sampled_from([PRICES, PRICES, None]))
    return Budget(max_cost=cost), table


def _in_flight(snap: RequestSnapshot) -> set[str]:
    """In_Flight_Jobs (Phase 4 glossary)."""
    keys = snap.timer_keys()
    out = set()
    for job_id, rec in snap.jobs.items():
        if rec.state not in (S.READY, S.DISPATCHED) or rec.attempt < 1:
            continue
        if (job_id, rec.attempt) in snap.results:
            continue
        if rec.state is S.READY and (
            TimerKey(TIMER_PUBLISH, job_id, rec.attempt) in keys
            or TimerKey(TIMER_CAPABILITY_WAIT, job_id, rec.attempt) in keys
        ):
            continue
        out.add(job_id)
    return out


def _publish_outstanding(snap: RequestSnapshot) -> list[str]:
    """Ready Jobs with a publish awaiting confirmation (no publish/capwait timer)."""
    return sorted(j for j in _in_flight(snap) if snap.jobs[j].state is S.READY)


def _tokens(snap: RequestSnapshot) -> int:
    return sum(r.usage.total_tokens for r in snap.usage)


def _result(job_id: str, attempt: int, status: str, tokens_in: int, tokens_out: int) -> JobResultPayload:
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id="w1",
        status=status,  # type: ignore[arg-type]
        content=f"out {job_id}@{attempt}",
        error=None if status == "Succeeded" else "boom",
        usage=TokenUsage(input_tokens=tokens_in, output_tokens=tokens_out, total_tokens=tokens_in + tokens_out),
        duration_ms=1,
    )


@given(plan=job_plans(max_jobs=6), budget=budgets(), data=st.data())
def test_budget_stop(plan: JobPlan, budget: tuple[Budget, PriceTable | None], data: st.DataObject):
    """**Validates: Requirements 5.6, 5.7, 5.8**"""
    limit, prices = budget
    caps = frozenset(c for j in plan.jobs for c in j.capabilities)
    planning = data.draw(st.integers(min_value=0, max_value=200), label="planning tokens")
    usage: tuple[UsageRecord, ...] = ()
    if planning:
        usage = (
            UsageRecord(
                operation="plan",
                provider="openai",
                model="gpt-x",
                usage=TokenUsage(input_tokens=planning, output_tokens=0, total_tokens=planning),
            ),
        )
    snapshot = RequestSnapshot(
        request_id=RID,
        status=RequestStatus.RUNNING,
        prompt="the prompt",
        plan=plan,
        jobs={j.job_id: JobRecord(job=j, state=S.WAITING, attempt=0) for j in plan.jobs},
        usage=usage,
        budget=limit,
    )
    unbudgeted = dataclasses.replace(snapshot, budget=None)

    now = 100.0
    stop_step: int | None = None
    tokens_at_stop = 0
    in_flight_at_stop: dict[str, int] = {}  # Job_ID -> attempt at the stop step
    usage_len_at_stop = 0

    event: object = PlanCommitted()
    for index in range(MAX_EVENTS + 1):
        # Workers for every Capability most of the time; none sometimes (capwait timers).
        agents = (_worker(caps),) if data.draw(st.integers(0, 4), label="workers") else ()
        ctx = StepContext(now=now, config=CFG, agents=agents, price_table=prices)
        decision = step(snapshot, event, ctx)
        post = apply_decision(snapshot, decision)
        final = [e for e in decision.effects if isinstance(e, FinalizeFailed)]
        budget_final = [e for e in final if e.budget_kind is not None]

        if stop_step is None:
            reference = step(unbudgeted, event, ctx)
            unbudgeted = apply_decision(unbudgeted, reference)
            kind = budget_exceeded(post, ctx)
            if kind is None:
                # Before the stop: exactly today's behaviour.
                assert [e for e in decision.effects if isinstance(e, PublishDispatch)] == [
                    e for e in reference.effects if isinstance(e, PublishDispatch)
                ]
                assert decision == reference
                assert not budget_final
                if final or any(isinstance(e, RunSummarizer) for e in decision.effects):
                    hevent("finished within budget")
                    return
            else:
                stop_step = index
                hevent(f"stop: {kind}")
                tokens_at_stop = _tokens(post)
                usage_len_at_stop = len(post.usage)
                in_flight_at_stop = {j: post.jobs[j].attempt for j in _in_flight(post)}

        if stop_step is not None:
            kind = budget_exceeded(post, ctx)
            assert kind is not None  # usage only grows
            assert not any(isinstance(e, PublishDispatch) for e in decision.effects)
            assert not any(isinstance(e, ScheduleTimer) for e in decision.effects)
            assert decision.updates.timers_add == ()
            assert not any(isinstance(e, RunSummarizer) for e in decision.effects)
            # Only Jobs in flight at the stop step add usage, once, for that attempt.
            later = post.usage[usage_len_at_stop:]
            owners = [(r.job_id, r.attempt) for r in later]
            assert len(owners) == len(set(j for j, _ in owners))
            for job_id, attempt in owners:
                assert job_id in in_flight_at_stop and attempt == in_flight_at_stop[job_id]
            carried = sum(
                post.results[(j, a)].usage.total_tokens
                for j, a in in_flight_at_stop.items()
                if (j, a) in post.results
            )
            assert _tokens(post) - tokens_at_stop <= carried
            # Finalized exactly when nothing is in flight any more.
            in_flight = _in_flight(post)
            assert in_flight <= set(in_flight_at_stop)
            assert final == budget_final
            assert bool(budget_final) == (not in_flight)
            if budget_final:
                [effect] = budget_final
                assert effect.budget_kind == kind
                expected = BUDGET_UNENFORCEABLE if kind == "unenforceable" else BUDGET_EXCEEDED
                assert effect.final_result.error == expected
                for rec in post.jobs.values():
                    assert rec.state in (S.SUCCEEDED, S.FAILED, S.SKIPPED, S.CANCELLED, S.TIMED_OUT)
                hevent("budget finalize" + (" after in-flight finished" if index > stop_step else " at stop step"))
                return

        snapshot = post
        if index == MAX_EVENTS:
            hevent("event budget used up" + (" in stop mode" if stop_step is not None else ""))
            break

        # Next event from the model Worker.
        now += data.draw(st.floats(min_value=0.0, max_value=5.0), label="dt")
        running = sorted(_in_flight(snapshot))
        outstanding = _publish_outstanding(snapshot)
        timers = sorted(snapshot.timers, key=lambda t: (t.due, t.key.member))
        choices = ["tick"]
        if running:
            choices += ["result", "result", "result"]
        if outstanding:
            choices += ["confirm", "publish_failed"]
        if timers:
            choices += ["timer"]
        kind_ = data.draw(st.sampled_from(choices), label="event")
        if kind_ == "tick":
            event = Tick()
        elif kind_ == "result":
            j = data.draw(st.sampled_from(running), label="job")
            status = data.draw(st.sampled_from(["Succeeded", "Succeeded", "Failed", "TimedOut"]))
            tin = data.draw(st.integers(min_value=0, max_value=40), label="input tokens")
            tout = data.draw(st.integers(min_value=0, max_value=40), label="output tokens")
            event = JobResultReceived(_result(j, snapshot.jobs[j].attempt, status, tin, tout), META)
        elif kind_ == "confirm":
            j = data.draw(st.sampled_from(outstanding), label="job")
            event = PublishConfirmed(job_id=j, attempt=snapshot.jobs[j].attempt)
        elif kind_ == "publish_failed":
            j = data.draw(st.sampled_from(outstanding), label="job")
            event = PublishFailed(job_id=j, attempt=snapshot.jobs[j].attempt)
        else:
            t = data.draw(st.sampled_from(timers), label="timer")
            now = max(now, t.due)
            event = TimerFired(kind=t.kind, job_id=t.job_id, attempt=t.attempt)
