"""Unit tests for the RequestDriver (task 14.2).

Real pieces throughout: fakeredis running the State_Store Lua scripts, the Redis-backed
Agent_Registry, MemoryTransport/MemoryBroker, and the Planner/Summarizer over a scripted
FakeAdapter. A small fake Worker answers dispatches.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field

from das.codec import JobDispatchMessage, new_message
from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.manager.driver import (
    DLQ_ATTEMPTS_HEADER,
    DLQ_JOB_ID_HEADER,
    DLQ_REQUEST_ID_HEADER,
    DriverDeps,
    DriverOutcome,
    RequestDrivers,
)
from das.manager.ingest import ControlCommandHandler, DeadLetterIngest, ResultIngest
from das.manager.machine import REASON_UNACKED_DELIVERY, MachineConfig
from das.manager.planner import NO_CAPABILITIES_MESSAGE, Planner, PlannerConfig
from das.manager.summarizer import Summarizer
from das.models import ControlPayload, JobResultPayload, JobState, RequestStatus, TokenUsage
from das.registry import AgentInfo, AgentRegistry
from das.state import StateStore, keys
from das.transport.base import Delivery, Outcome
from tests.fakes.llm_adapter import FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="m-1", temperature=0.0, max_output_tokens=256)
CAPS = frozenset({"search", "write"})
TWO_JOBS = json.dumps(
    {
        "jobs": [
            {"job_id": "a", "instruction": "find facts", "capabilities": ["search"]},
            {"job_id": "b", "instruction": "write", "capabilities": ["write"], "depends_on": ["a"]},
        ]
    }
)
ONE_JOB = json.dumps(
    {"jobs": [{"job_id": "a", "instruction": "find facts", "capabilities": ["search"]}]}
)
TIMEOUT = 10.0


# --------------------------------------------------------------------------- fixtures


@dataclass
class FakeWorker:
    """Answers dispatches from ``jobs/<cap>``; ``behaviour`` picks the reply."""

    transport: MemoryTransport
    agent_id: str = "w1"
    status: str = "Succeeded"
    mode: str = "reply"  # reply | hold | requeue
    dispatches: list[JobDispatchMessage] = field(default_factory=list)

    async def start(self, caps=CAPS) -> None:
        await self.transport.start()
        for cap in sorted(caps):
            await self.transport.subscribe(f"jobs/{cap}", self.handle)

    async def handle(self, delivery: Delivery) -> None:
        msg = delivery.message
        assert isinstance(msg, JobDispatchMessage)
        self.dispatches.append(msg)
        if self.mode == "hold":
            return
        if self.mode == "requeue":
            await self.transport.acknowledge(delivery, Outcome.REQUEUE)
            return
        p = msg.payload
        result = JobResultPayload(
            request_id=p.request_id,
            job_id=p.job_id,
            attempt=p.attempt,
            worker_id=self.agent_id,
            status=self.status,  # type: ignore[arg-type]
            content=f"out-{p.job_id}" if self.status == "Succeeded" else "",
            error=None if self.status == "Succeeded" else "boom",
            usage=TokenUsage(input_tokens=2, output_tokens=3, total_tokens=5),
            duration_ms=1,
        )
        await self.transport.publish("results", new_message(result, correlation_id=p.request_id))
        await self.transport.acknowledge(delivery)


@dataclass
class Env:
    store: StateStore
    registry: AgentRegistry
    broker: MemoryBroker
    manager: MemoryTransport
    worker: FakeWorker
    drivers: RequestDrivers
    planner_llm: FakeAdapter
    summary_llm: FakeAdapter
    control_broker: MemoryBroker

    async def submit_and_claim(self, prompt: str = "hello", owner: str = "m1"):
        rid = await self.store.create_request(prompt)
        acquired = await self.store.acquire_lease(rid, owner, self.drivers.lease_duration)
        assert acquired.ok and acquired.lease is not None
        return rid, self.drivers.on_claimed(acquired.lease)


async def make_env(
    store: StateStore,
    fake_redis,
    *,
    plan_outcomes=(TWO_JOBS,),
    summary_outcomes=("final answer",),
    register_worker: bool = True,
    machine_config: MachineConfig | None = None,
    lease_duration: float = 30.0,
    dead_letter_destinations=("dlq",),
    tweak: Callable[[DriverDeps], DriverDeps] | None = None,
) -> Env:
    registry = AgentRegistry(fake_redis)
    if register_worker:
        await registry.register(AgentInfo(agent_id="w1", role="worker", host="h", capabilities=CAPS))
    broker = MemoryBroker(dead_letter_destinations)
    manager = MemoryTransport(broker, agent_id="m1")
    await manager.start()
    worker = FakeWorker(MemoryTransport(broker, agent_id="w1"))
    await worker.start()
    control_broker = MemoryBroker()
    control = MemoryTransport(control_broker, agent_id="m1")
    await control.start()
    planner_llm = FakeAdapter(list(plan_outcomes))
    summary_llm = FakeAdapter(list(summary_outcomes))
    deps = DriverDeps(
        store=store,
        registry=registry,
        transport=manager,
        planner=Planner(LLMProvider(planner_llm, PARAMS, retry_count=0)),
        summarizer=Summarizer(LLMProvider(summary_llm, PARAMS, retry_count=0)),
        planner_config=PlannerConfig(planning_retries=0),
        machine_config=machine_config or MachineConfig(),
        control_transport=control,
        tick_period=0.02,
        finalize_retry_delay=0.01,
    )
    if tweak is not None:
        deps = tweak(deps)
    drivers = RequestDrivers(deps, lease_duration)
    await ResultIngest(manager, store, on_ingested=drivers.notify).start()
    await DeadLetterIngest(manager, store, on_ingested=drivers.notify).start()
    await ControlCommandHandler(control, "m1", drivers).start()
    return Env(store, registry, broker, manager, worker, drivers, planner_llm, summary_llm,
               control_broker)


async def wait_until(predicate, timeout: float = TIMEOUT) -> None:
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.01)


async def steal_lease(fake_redis, store: StateStore, rid: str) -> None:
    """Expire the current Lease and let another Manager acquire it."""
    sec, _ = await fake_redis.time()
    await fake_redis.hset(keys.lease(rid), "expires_at", f"{int(sec) - 1}.000000")
    assert (await store.acquire_lease(rid, "m2", 30)).ok


# --------------------------------------------------------------------------- happy path


async def test_two_job_dag_runs_to_completed(state_store: StateStore, fake_redis):
    env = await make_env(state_store, fake_redis)
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED

    stored = await state_store.load_snapshot(rid, include_transitions=True)
    assert stored is not None
    assert stored.status is RequestStatus.COMPLETED
    assert stored.final_result is not None and stored.final_result.summary == "final answer"
    assert {r.job_id: r.state for r in stored.final_result.jobs} == {
        "a": JobState.SUCCEEDED,
        "b": JobState.SUCCEEDED,
    }
    assert all(r.worker_id == "w1" for r in stored.final_result.jobs)
    assert stored.lease is None  # released (Req 7.4)

    # Dependency inputs labelled by Job_ID (Req 4.2); dispatch carries the Request_ID.
    by_job = {m.payload.job_id: m for m in env.worker.dispatches}
    assert by_job["a"].payload.dependency_results == {}
    assert by_job["b"].payload.dependency_results == {"a": "out-a"}
    assert all(m.correlation_id == rid for m in env.worker.dispatches)

    # Ready is persisted before Dispatched (Req 4.1); every change is logged (13.7).
    a_states = [(t.from_state, t.to_state) for t in stored.transitions or [] if t.job_id == "a"]
    assert a_states == [
        (None, JobState.WAITING),
        (JobState.WAITING, JobState.READY),
        (JobState.READY, JobState.DISPATCHED),
        (JobState.DISPATCHED, JobState.SUCCEEDED),
    ]
    # The Summarizer saw only the prompt and Succeeded contents (Req 7.1).
    assert len(env.summary_llm.calls) == 1
    text = env.summary_llm.calls[0].messages[0].content
    assert "hello" in text and "out-a" in text and "out-b" in text


# --------------------------------------------------------------------------- planning failures


async def test_no_capabilities_fails_without_llm_call(state_store: StateStore, fake_redis):
    env = await make_env(state_store, fake_redis, register_worker=False)
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.error == NO_CAPABILITIES_MESSAGE
    assert env.planner_llm.calls == []  # Req 3.10
    assert stored.plan is None and stored.lease is None


async def test_rejected_plan_fails_with_checks(state_store: StateStore, fake_redis):
    bad = json.dumps({"jobs": [{"job_id": "a", "instruction": "x", "capabilities": ["paint"]}]})
    env = await make_env(state_store, fake_redis, plan_outcomes=(bad,))
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.error is not None and "unadvertised_capability" in stored.error.lower()
    assert stored.plan is None and stored.lease is None  # Req 3.6


async def test_provider_error_fails_with_provider_model_reason(
    state_store: StateStore, fake_redis
):
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(PermanentLLMError("invalid api key"),)
    )
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.error is not None
    assert "provider=fake" in stored.error and "model=m-1" in stored.error  # Req 3.11
    assert stored.plan is None and stored.lease is None


# --------------------------------------------------------------------------- lease loss


def gated() -> tuple[asyncio.Event, Callable]:
    gate = asyncio.Event()

    def responder(content: str):
        async def respond(_system, _messages, _params):
            await gate.wait()
            from tests.fakes.llm_adapter import completion

            return completion(content)

        return respond

    return gate, responder


async def test_lease_loss_discards_planner_output(state_store: StateStore, fake_redis):
    gate, responder = gated()
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(responder(TWO_JOBS),), lease_duration=1.0
    )
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _true(len(env.planner_llm.calls) == 1))
    await steal_lease(fake_redis, state_store, rid)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
    gate.set()
    stored = await state_store.load_snapshot(rid)
    assert stored is not None
    assert stored.status is RequestStatus.PENDING and stored.plan is None  # Req 3.12
    assert stored.lease is not None and stored.lease.owner == "m2"
    assert env.worker.dispatches == []


async def test_lease_loss_discards_summarizer_output(state_store: StateStore, fake_redis):
    gate, responder = gated()
    env = await make_env(
        state_store,
        fake_redis,
        plan_outcomes=(ONE_JOB,),
        summary_outcomes=(responder("late summary"),),
        lease_duration=1.0,
    )
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _true(len(env.summary_llm.calls) == 1))
    await steal_lease(fake_redis, state_store, rid)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
    gate.set()
    stored = await state_store.load_snapshot(rid)
    assert stored is not None
    assert stored.status is RequestStatus.RUNNING and stored.final_result is None  # Req 2.7


async def test_lease_loss_mid_execution_stops_writes(state_store: StateStore, fake_redis):
    env = await make_env(state_store, fake_redis, lease_duration=1.0)
    env.worker.mode = "hold"
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _true(len(env.worker.dispatches) == 1))
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))
    await steal_lease(fake_redis, state_store, rid)

    # A late result for "a" arrives after the Lease moved to m2 (Req 2.7).
    p = env.worker.dispatches[0].payload
    late = JobResultPayload(
        request_id=rid, job_id="a", attempt=p.attempt, worker_id="w1", status="Succeeded",
        content="out-a", usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2),
        duration_ms=1,
    )
    await env.worker.transport.publish("results", new_message(late, correlation_id=rid))
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
    await env.broker.settle()

    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.RUNNING
    assert stored.jobs["a"].state is JobState.DISPATCHED  # m1 wrote nothing more
    assert stored.jobs["b"].state is JobState.WAITING
    assert stored.lease is not None and stored.lease.owner == "m2"
    assert [m.payload.job_id for m in env.worker.dispatches] == ["a"]  # "b" never dispatched
    assert env.summary_llm.calls == []


async def _true(value: bool) -> bool:
    return value


# --------------------------------------------------------------------------- finalize retries


async def test_final_write_failures_stop_renewing(state_store: StateStore, fake_redis):
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,))
    calls: list[int] = []

    async def broken_finalize(*_args, **_kwargs):
        calls.append(1)
        raise ConnectionError("redis down")

    state_store.finalize = broken_finalize  # type: ignore[method-assign]
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.STOPPED_RENEWING
    assert len(calls) == 4  # 1 + 3 retries (Req 7.7)
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.RUNNING
    assert stored.lease is not None and stored.lease.owner == "m1"  # not released


async def test_final_write_retry_succeeds(state_store: StateStore, fake_redis):
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,))
    real_finalize = state_store.finalize
    calls: list[int] = []

    async def flaky_finalize(*args, **kwargs):
        calls.append(1)
        if len(calls) <= 2:
            raise ConnectionError("redis blip")
        return await real_finalize(*args, **kwargs)

    state_store.finalize = flaky_finalize  # type: ignore[method-assign]
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    assert len(calls) == 3  # two failures, then persisted on the second retry (Req 7.4)
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.COMPLETED
    assert stored.final_result is not None and stored.final_result.summary == "final answer"
    assert stored.lease is None
    assert len(env.summary_llm.calls) == 1  # summary not regenerated per retry


# --------------------------------------------------------------------------- dead letters


async def test_exhausted_attempts_publish_dead_letter(state_store: StateStore, fake_redis):
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(ONE_JOB,), machine_config=MachineConfig(max_attempts=1)
    )
    env.worker.status = "Failed"
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    [headers] = env.broker.pending_headers("dlq")
    assert headers[DLQ_REQUEST_ID_HEADER] == rid and headers[DLQ_JOB_ID_HEADER] == "a"
    assert headers[DLQ_ATTEMPTS_HEADER] == "1" and headers["x-last-error"] == "boom"  # Req 6.3
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.final_result is not None and stored.final_result.failures[0].reason == "boom"


async def test_unacked_delivery_exhaustion_fails_job(state_store: StateStore, fake_redis):
    env = await make_env(
        state_store, fake_redis, dead_letter_destinations=("dlq", "dlq-events")
    )
    env.broker.set_delivery_limit("jobs/search", 2)
    env.worker.mode = "requeue"
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.FAILED
    assert stored.jobs["a"].state is JobState.FAILED
    assert stored.jobs["a"].reason == REASON_UNACKED_DELIVERY  # Req 6.6
    assert stored.jobs["b"].state is JobState.SKIPPED


# --------------------------------------------------------------------------- cancellation


async def test_cancel_request_sends_cancel_job_and_releases(state_store: StateStore, fake_redis):
    env = await make_env(state_store, fake_redis)
    env.worker.mode = "hold"
    worker_control = MemoryTransport(env.control_broker, agent_id="w1")
    await worker_control.start()
    received: list = []

    async def on_control(delivery: Delivery) -> None:
        received.append(delivery.message)
        await worker_control.acknowledge(delivery)

    await worker_control.subscribe("w1", on_control)
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _true(len(env.worker.dispatches) == 1))
    await wait_until(
        lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED)
    )

    # Gateway: atomic store cancel, then the cancel_request command to the owner.
    assert (await state_store.cancel_request(rid)).ok
    gateway = MemoryTransport(env.control_broker, agent_id="gw")
    await gateway.start()
    await gateway.publish(
        "m1",
        new_message(ControlPayload(command="cancel_request", request_id=rid), correlation_id=rid),
    )
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.CANCELLED
    await env.control_broker.settle()
    assert [(m.payload.command, m.payload.job_id, m.correlation_id) for m in received] == [
        ("cancel_job", "a", rid)
    ]
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.CANCELLED
    assert stored.lease is None
    assert len(env.worker.dispatches) == 1  # no further dispatch (Req 1.7)


async def _status_of(store: StateStore, rid: str, job_id: str, state: JobState) -> bool:
    stored = await store.load_snapshot(rid)
    return stored is not None and job_id in stored.jobs and stored.jobs[job_id].state is state
