"""RequestDriver budget enforcement (das-client-api task 5.3; Phase 4 Req 5.5, 5.7,
5.9, 6.1, 10.2).

Same real pieces as ``test_manager_driver`` (fakeredis + Lua, MemoryTransport,
Planner/Summarizer over FakeAdapter), plus a real ``MetricsRecorder``. Budgeted
Requests are created with ``StateStore.submit_request``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from decimal import Decimal
from typing import Any

from das.client.budget import Budget
from das.clock import FakeClock
from das.codec import new_message
from das.llm.retry import LLMProvider
from das.manager.agent import ManagerAgent, ManagerSettings
from das.manager.driver import DriverDeps, DriverOutcome, RequestDrivers
from das.manager.machine import BUDGET_EXCEEDED, BUDGET_UNENFORCEABLE, UNKNOWN_LLM
from das.manager.planner import Planner, PlannerConfig
from das.manager.summarizer import Summarizer
from das.models import JobResultPayload, JobState, RequestStatus, TokenUsage
from das.observability.metrics import CATALOG_BY_NAME, ManagerMetrics, MetricsRecorder
from das.ops.cost import PriceEntry, PriceTable
from das.registry import AgentRegistry
from das.state import ClientRequestSpec, StateStore, StoreStatus, WriteResult, keys
from tests.fakes.llm_adapter import FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport
from tests.unit.test_manager_driver import (
    ONE_JOB,
    PARAMS,
    TIMEOUT,
    TWO_JOBS,
    Env,
    _status_of,
    _true,
    make_env,
    wait_until,
)

METRIC = "das_budget_exceeded_total"
#: Planner usage of the FakeAdapter's default completion: 1 input + 1 output token.
PLANNER_TOKENS = 2
#: FakeWorker usage per Job_Result.
WORKER_TOKENS = 5
#: Two independent Jobs plus one that depends on both.
FAN_IN = json.dumps(
    {
        "jobs": [
            {"job_id": "a", "instruction": "find facts", "capabilities": ["search"]},
            {"job_id": "c", "instruction": "find more", "capabilities": ["search"]},
            {"job_id": "b", "instruction": "write", "capabilities": ["write"],
             "depends_on": ["a", "c"]},
        ]
    }
)


class _Log:
    def warning(self, *_a, **_k) -> None: ...


def _recorder() -> MetricsRecorder:
    return MetricsRecorder(clock=FakeClock(), log=_Log())


def _wire(rec: MetricsRecorder, price_table: PriceTable | None = None):
    def tweak(deps: DriverDeps) -> DriverDeps:
        return dataclasses.replace(deps, metrics=ManagerMetrics(rec), price_table=price_table)

    return tweak


def _value(rec: MetricsRecorder, kind: str) -> float:
    return rec.registry.get_sample_value(METRIC, {"kind": kind}) or 0.0


def _total(rec: MetricsRecorder) -> float:
    return sum(_value(rec, k) for k in ("tokens", "cost", "unenforceable"))


async def _submit_and_claim(env: Env, budget: Budget | None, owner: str = "m1"):
    created = await env.store.submit_request(
        "hello", ClientRequestSpec(fingerprint="fp1", budget=budget)
    )
    rid = created.request_id
    assert rid is not None
    acquired = await env.store.acquire_lease(rid, owner, env.drivers.lease_duration)
    assert acquired.ok and acquired.lease is not None
    return rid, env.drivers.on_claimed(acquired.lease)


async def _events(store: StateStore, rid: str) -> list[tuple[str, dict[str, Any]]]:
    return [
        (f["type"], json.loads(f["data"])) for _, f in await store.client.xrange(keys.events(rid))
    ]


def _result(rid: str, job_id: str, attempt: int, tokens: int) -> JobResultPayload:
    return JobResultPayload(
        request_id=rid, job_id=job_id, attempt=attempt, worker_id="w1", status="Succeeded",
        content=f"out-{job_id}",
        usage=TokenUsage(input_tokens=tokens, output_tokens=0, total_tokens=tokens),
        duration_ms=1,
    )


# --------------------------------------------------------------------------- metric catalog


def test_catalog_has_budget_exceeded_metric():
    spec = CATALOG_BY_NAME[METRIC]
    assert spec.kind == "counter"
    assert spec.labels == ("kind",)
    assert spec.allowed == {"kind": frozenset({"tokens", "cost", "unenforceable"})}


# --------------------------------------------------------------------------- planning check


async def test_max_cost_without_price_table_fails_before_planning(
    state_store: StateStore, fake_redis
):
    """Phase 4 Req 5.5: ``max_cost`` and no Price_Table fail before the Planner runs."""
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    rid, task = await _submit_and_claim(env, Budget(max_cost=Decimal("1.5")))
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED

    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.error == BUDGET_UNENFORCEABLE
    assert stored.plan is None and stored.lease is None
    assert env.planner_llm.calls == []
    assert env.worker.dispatches == []
    assert _value(rec, "unenforceable") == 1 and _total(rec) == 1

    kind, data = (await _events(state_store, rid))[-1]
    assert kind == "request.failed"
    assert data["error"] == BUDGET_UNENFORCEABLE
    assert data["budget"] == {
        "limits": {"max_cost": "1.5"},
        "used": {"total_tokens": 0, "cost": None},
        "exceeded": True,
    }


# --------------------------------------------------------------------------- budget stop


async def test_token_budget_hit_mid_run_fails_without_summarizer(
    state_store: StateStore, fake_redis
):
    """Phase 4 Req 5.7, 5.9, 6.1: Job "a" reaches the limit; "b" is never dispatched."""
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    limit = PLANNER_TOKENS + WORKER_TOKENS
    rid, task = await _submit_and_claim(env, Budget(max_total_tokens=limit))
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED

    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.error == BUDGET_EXCEEDED
    assert env.summary_llm.calls == []
    assert [m.payload.job_id for m in env.worker.dispatches] == ["a"]
    assert stored.jobs["a"].state is JobState.SUCCEEDED
    assert stored.jobs["b"].state is JobState.SKIPPED
    assert stored.jobs["b"].reason == BUDGET_EXCEEDED
    # Succeeded Job results stay queryable (Req 5.9).
    assert stored.results[("a", 1)].content == "out-a"

    kind, data = (await _events(state_store, rid))[-1]
    assert kind == "request.failed"
    assert data["budget"]["exceeded"] is True
    assert data["budget"]["used"]["total_tokens"] == data["usage"]["total_tokens"] == limit
    assert _value(rec, "tokens") == 1 and _total(rec) == 1


async def test_no_budget_completes_without_budget_view_or_metric(
    state_store: StateStore, fake_redis
):
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    rid, task = await _submit_and_claim(env, None)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED

    kind, data = (await _events(state_store, rid))[-1]
    assert kind == "request.completed"
    assert "budget" not in data
    assert _total(rec) == 0


async def test_budget_within_limit_completes_with_budget_view(
    state_store: StateStore, fake_redis
):
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    rid, task = await _submit_and_claim(env, Budget(max_total_tokens=1000))
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED

    kind, data = (await _events(state_store, rid))[-1]
    assert kind == "request.completed"
    assert data["budget"]["exceeded"] is False
    # Planner + two Jobs + Summarizer.
    assert data["budget"]["used"]["total_tokens"] == data["usage"]["total_tokens"]
    assert data["usage"]["total_tokens"] == 2 * PLANNER_TOKENS + 2 * WORKER_TOKENS
    assert _total(rec) == 0


async def test_max_cost_with_price_table_is_enforceable(state_store: StateStore, fake_redis):
    """The driver passes the Price_Table to the reducer, so ``max_cost`` is priced."""
    table = PriceTable(
        currency="USD",
        models=[
            PriceEntry(provider="fake", model="m-1", input_per_million=Decimal(1),
                       output_per_million=Decimal(1)),
            PriceEntry(provider=UNKNOWN_LLM, model=UNKNOWN_LLM, input_per_million=Decimal(1),
                       output_per_million=Decimal(1)),
        ],
    )
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec, table))
    rid, task = await _submit_and_claim(env, Budget(max_cost=Decimal(100)))
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED

    kind, data = (await _events(state_store, rid))[-1]
    assert kind == "request.completed"
    assert data["budget"]["exceeded"] is False
    assert data["budget"]["used"]["cost"] is not None
    assert _total(rec) == 0


# --------------------------------------------------------------------------- takeover


async def test_takeover_during_budget_stop_finishes_in_flight_job(
    state_store: StateStore, fake_redis
):
    """Phase 4 Req 5.7, 5.8 with v1 Req 2.5: a new owner recomputes the stop from the
    persisted usage, dispatches nothing new and fails once the in-flight Job is done."""
    rec = _recorder()
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(FAN_IN,), lease_duration=1.0, tweak=_wire(rec)
    )
    env.worker.mode = "hold"
    limit = PLANNER_TOKENS + 10
    rid, task = await _submit_and_claim(env, Budget(max_total_tokens=limit))
    await wait_until(lambda: _true(len(env.worker.dispatches) == 2))
    await wait_until(lambda: _status_of(state_store, rid, "c", JobState.DISPATCHED))
    attempts = {m.payload.job_id: m.payload.attempt for m in env.worker.dispatches}

    # "a" reaches the limit while "c" is still in flight: "b" is Skipped, no finalize.
    late_a = _result(rid, "a", attempts["a"], 10)
    await env.worker.transport.publish("results", new_message(late_a, correlation_id=rid))
    await wait_until(lambda: _status_of(state_store, rid, "b", JobState.SKIPPED))
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.RUNNING
    assert stored.jobs["c"].state is JobState.DISPATCHED

    # Another Manager takes the Lease over and starts its own driver.
    sec, _ = await fake_redis.time()
    await fake_redis.hset(keys.lease(rid), "expires_at", f"{int(sec) - 1}.000000")
    acquired = await state_store.acquire_lease(rid, "m2", 30)
    assert acquired.ok and acquired.lease is not None
    before = len(env.worker.dispatches)
    m2 = RequestDrivers(env.drivers.deps, 30.0)
    task2 = m2.on_claimed(acquired.lease)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST

    # The new owner reached the same budget-stop state: nothing is Ready or re-planned.
    await asyncio.sleep(0.2)
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.RUNNING
    assert stored.jobs["b"].state is JobState.SKIPPED
    assert not task2.done()

    late_c = _result(rid, "c", attempts["c"], 1)
    await env.worker.transport.publish("results", new_message(late_c, correlation_id=rid))
    assert await asyncio.wait_for(task2, TIMEOUT) is DriverOutcome.FAILED

    # resume() may re-publish "c", which was in flight when the stop began (liveness
    # after a takeover, not a new dispatch); nothing else is dispatched, no retry.
    assert {m.payload.job_id for m in env.worker.dispatches[before:]} <= {"c"}
    assert all(m.payload.attempt == attempts["c"] for m in env.worker.dispatches[before:])
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.error == BUDGET_EXCEEDED
    assert stored.jobs["c"].state is JobState.SUCCEEDED
    assert env.summary_llm.calls == []
    kind, data = (await _events(state_store, rid))[-1]
    assert kind == "request.failed"
    assert data["budget"]["used"]["total_tokens"] == limit + 1
    assert _value(rec, "tokens") == 1 and _total(rec) == 1


# --------------------------------------------------------------------------- metric once


async def test_replay_pending_finalize_counts_metric_once(state_store: StateStore, fake_redis):
    rec = _recorder()
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,), tweak=_wire(rec))
    real_finalize = state_store.finalize
    calls: list[int] = []

    async def finalize(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            return WriteResult(StoreStatus.REPLAY_PENDING)
        return await real_finalize(*args, **kwargs)

    state_store.finalize = finalize  # type: ignore[method-assign]
    rid, task = await _submit_and_claim(env, Budget(max_total_tokens=PLANNER_TOKENS + 1))
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    assert len(calls) == 2
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.error == BUDGET_EXCEEDED
    assert _value(rec, "tokens") == 1 and _total(rec) == 1


# --------------------------------------------------------------------------- agent wiring


async def test_manager_agent_passes_price_table_to_drivers(state_store: StateStore, fake_redis):
    table = PriceTable(currency="EUR")
    broker = MemoryBroker()
    settings = ManagerSettings(
        agent_id="m1", host="mhost", grpc_endpoint="mhost:50051",
        planner_config=PlannerConfig(planning_retries=0), price_table=table,
    )
    agent = ManagerAgent(
        settings,
        queue=MemoryTransport(broker, agent_id="m1"),
        control=MemoryTransport(MemoryBroker(), agent_id="m1"),
        store=state_store,
        registry=AgentRegistry(fake_redis),
        planner=Planner(LLMProvider(FakeAdapter([TWO_JOBS]), PARAMS, retry_count=0)),
        summarizer=Summarizer(LLMProvider(FakeAdapter(["x"]), PARAMS, retry_count=0)),
    )
    assert agent.drivers.deps.price_table is table
