"""Unit tests for the Manager_Agent process wiring (task 14.3).

Real pieces: fakeredis with the State_Store Lua scripts, the Redis-backed
Agent_Registry, MemoryTransport/MemoryBroker, and the Planner/Summarizer over a
scripted FakeAdapter. A small fake Worker answers dispatches.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from dataclasses import dataclass, field

import pytest

from das.clock import SystemClock
from das.codec import (
    AgentControlPayload,
    AgentControlReplyMessage,
    JobDispatchMessage,
    new_message,
)
from das.config.schema import RetentionConfig
from das.errors import RegistrationFailed
from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.manager import agent as agent_module
from das.manager.agent import ManagerAgent, ManagerSettings
from das.manager.planner import Planner, PlannerConfig
from das.manager.summarizer import Summarizer
from das.models import JobResultPayload, RequestStatus, TokenUsage
from das.observability.metrics import MetricsRecorder
from das.registry import AgentInfo, AgentRegistry
from das.registry.registry import agent_key
from das.state import StateStore
from das.transport.base import Delivery, Outcome, RequestTimeout
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
TIMEOUT = 10.0
CRITICAL = frozenset({"claim", "sweeper", "heartbeat"})


@dataclass
class FakeWorker:
    transport: MemoryTransport
    agent_id: str = "w1"
    hold: bool = False
    dispatches: list[JobDispatchMessage] = field(default_factory=list)

    async def start(self) -> None:
        await self.transport.start()
        for cap in sorted(CAPS):
            await self.transport.subscribe(f"jobs/{cap}", self.handle)

    async def handle(self, delivery: Delivery) -> None:
        msg = delivery.message
        assert isinstance(msg, JobDispatchMessage)
        self.dispatches.append(msg)
        if self.hold:
            return
        p = msg.payload
        result = JobResultPayload(
            request_id=p.request_id,
            job_id=p.job_id,
            attempt=p.attempt,
            worker_id=self.agent_id,
            status="Succeeded",
            content=f"out-{p.job_id}",
            error=None,
            usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2),
            duration_ms=1,
        )
        await self.transport.publish("results", new_message(result, correlation_id=p.request_id))
        await self.transport.acknowledge(delivery, Outcome.ACK)


class RecordingRegistry:
    """AgentRegistry wrapper that records calls and what was subscribed at register time."""

    def __init__(self, inner: AgentRegistry, transports: list[MemoryTransport]) -> None:
        self.inner = inner
        self.transports = transports
        self.calls: list[str] = []
        self.subscriptions_at_register: list[int] = []
        self.heartbeats: list[tuple[str, int]] = []
        self.fail_register = False

    async def register(self, info: AgentInfo) -> float:
        self.calls.append("register")
        self.subscriptions_at_register.append(
            sum(len(t.registry.active()) for t in self.transports)
        )
        if self.fail_register:
            raise ConnectionError("registry down")
        return await self.inner.register(info)

    async def heartbeat(
        self, agent_id: str, desired_status: str = "Available", in_flight: int = 0
    ) -> bool:
        self.calls.append("heartbeat")
        self.heartbeats.append((desired_status, in_flight))
        return await self.inner.heartbeat(agent_id, desired_status, in_flight)

    async def set_status(self, agent_id: str, status: str, in_flight: int | None = None) -> bool:
        self.calls.append(f"set_status:{status}")
        return await self.inner.set_status(agent_id, status, in_flight)

    async def deregister(self, agent_id: str) -> bool:
        self.calls.append("deregister")
        return await self.inner.deregister(agent_id)

    async def sweep(self, now: float | None = None) -> list[str]:
        self.calls.append("sweep")
        return await self.inner.sweep(now)

    async def snapshot(self):
        return await self.inner.snapshot()


@dataclass
class Env:
    agent: ManagerAgent
    registry: RecordingRegistry
    inner: AgentRegistry
    queue: MemoryTransport
    control: MemoryTransport
    worker: FakeWorker
    store: StateStore


async def make_env(
    store: StateStore,
    fake_redis,
    *,
    heartbeat_interval: float = 5.0,
    registration_timeout: float = 5.0,
    hold: bool = False,
    recorder: MetricsRecorder | None = None,
    retention: RetentionConfig | None = None,
) -> Env:
    inner = AgentRegistry(fake_redis)
    await inner.register(AgentInfo(agent_id="w1", role="worker", host="h", capabilities=CAPS))
    broker = MemoryBroker()
    queue = MemoryTransport(broker, agent_id="m1")
    control = MemoryTransport(MemoryBroker(), agent_id="m1")
    await queue.start()
    await control.start()
    worker = FakeWorker(MemoryTransport(broker, agent_id="w1"), hold=hold)
    await worker.start()
    registry = RecordingRegistry(inner, [queue, control])
    settings = ManagerSettings(
        agent_id="m1",
        host="mhost",
        grpc_endpoint="mhost:50051",
        heartbeat_interval=heartbeat_interval,
        shutdown_timeout=5.0,
        registration_timeout=registration_timeout,
        planner_config=PlannerConfig(planning_retries=0),
        claim_period=0.05,
        queue_sample_interval=5.0,
        retention=retention or RetentionConfig(),
    )
    agent = ManagerAgent(
        settings,
        queue=queue,
        control=control,
        store=store,
        registry=registry,
        planner=Planner(LLMProvider(FakeAdapter([TWO_JOBS]), PARAMS, retry_count=0)),
        summarizer=Summarizer(LLMProvider(FakeAdapter(["final answer"]), PARAMS, retry_count=0)),
        tweak_deps=lambda d: dataclasses.replace(d, tick_period=0.02, finalize_retry_delay=0.01),
        recorder=recorder,
    )
    return Env(agent, registry, inner, queue, control, worker, store)


async def wait_until(predicate, timeout: float = TIMEOUT) -> None:
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.01)


async def record(inner: AgentRegistry, agent_id: str):
    return (await inner.snapshot()).get(agent_id)


# --------------------------------------------------------------------------- startup


async def test_registers_before_subscribing_then_starts_loops(state_store, fake_redis):
    env = await make_env(state_store, fake_redis)
    await env.agent.start()
    try:
        # Req 11.6: registered with nothing subscribed yet; subscriptions follow.
        assert env.registry.subscriptions_at_register == [0]
        assert env.agent.startup_steps == [
            "register",
            "subscribe:results",
            "subscribe:dlq-events",
            "subscribe:dlq",
            "subscribe:control",
            "loops",
        ]
        rec = await record(env.inner, "m1")
        assert rec is not None
        assert (rec.role, rec.status, rec.host, rec.grpc_endpoint) == (
            "manager", "Available", "mhost", "mhost:50051",
        )
        assert {s.destination for s in env.queue.registry.active()} == {
            "results", "dlq-events", "dlq",
        }
        assert [s.destination for s in env.control.registry.active()] == ["m1"]
        # Retention is disabled by default; the one-shot backfill may already be done.
        assert CRITICAL <= env.agent.loops
        assert env.agent.loops - CRITICAL <= {"queue_sampler", "registry_gauges", "index_backfill"}
        assert "retention" not in env.agent.loops
    finally:
        await env.agent.stop()


async def test_registration_failure_subscribes_nothing(state_store, fake_redis):
    env = await make_env(state_store, fake_redis, registration_timeout=0.3)
    env.registry.fail_register = True
    with pytest.raises(RegistrationFailed):
        await env.agent.start()
    assert not list(env.queue.registry.active())
    assert not list(env.control.registry.active())
    assert env.agent.loops == frozenset()


# --------------------------------------------------------------------------- loops


async def test_sweeper_marks_stale_agents_unavailable(state_store, fake_redis):
    env = await make_env(state_store, fake_redis)
    sec, _ = await fake_redis.time()
    await fake_redis.hset(agent_key("w1"), "last_heartbeat", f"{int(sec) - 60}.000000")
    await env.agent.start()
    try:
        async def stale() -> bool:
            rec = await record(env.inner, "w1")
            return rec is not None and rec.status == "Unavailable"

        await wait_until(stale)
        manager = await record(env.inner, "m1")
        assert manager is not None and manager.status == "Available"
    finally:
        await env.agent.stop()


async def test_heartbeat_re_registers_when_forgotten(state_store, fake_redis):
    env = await make_env(state_store, fake_redis, heartbeat_interval=1.0)
    await env.agent.start()
    try:
        await env.inner.deregister("m1")
        assert await record(env.inner, "m1") is None

        async def back() -> bool:
            return await record(env.inner, "m1") is not None

        await wait_until(back, timeout=5.0)
        assert "heartbeat" in env.registry.calls
        assert env.registry.calls.count("register") == 2
    finally:
        await env.agent.stop()


# --------------------------------------------------------------------------- end to end


async def test_claims_and_completes_two_job_dag(state_store, fake_redis):
    env = await make_env(state_store, fake_redis)
    await env.agent.start()
    try:
        rid = await state_store.create_request("hello")

        async def done() -> bool:
            return await state_store.get_status(rid) is RequestStatus.COMPLETED

        await wait_until(done)
        stored = await state_store.load_snapshot(rid)
        assert stored is not None and stored.final_result is not None
        assert stored.final_result.summary == "final answer"
        assert stored.lease is None
        assert {m.payload.job_id for m in env.worker.dispatches} == {"a", "b"}
    finally:
        await env.agent.stop()


# --------------------------------------------------------------------------- shutdown


async def test_graceful_shutdown_deregisters_and_closes(state_store, fake_redis):
    env = await make_env(state_store, fake_redis, hold=True)
    await env.agent.start()
    rid = await state_store.create_request("hello")

    async def dispatched() -> bool:
        return len(env.worker.dispatches) > 0

    await wait_until(dispatched)
    assert rid in env.agent.drivers.owned

    await env.agent.stop()
    await env.agent.stop()  # idempotent

    assert await record(env.inner, "m1") is None
    assert env.registry.calls[-1] == "deregister"
    assert env.agent.loops == frozenset()
    assert env.agent.drivers.owned == frozenset()
    assert not env.queue.connected and not env.control.connected
    assert not list(env.queue.registry.active()) and not list(env.control.registry.active())
    # The Request is left Running for another Manager; it no longer holds a Lease.
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.RUNNING
    assert stored.lease is None
    assert (await state_store.acquire_lease(rid, "m2", 30)).ok


async def test_run_stops_on_request_stop(state_store, fake_redis):
    env = await make_env(state_store, fake_redis)
    runner = asyncio.create_task(env.agent.run())
    await wait_until(lambda: _ready(env))
    env.agent.request_stop()
    await asyncio.wait_for(runner, TIMEOUT)
    assert await record(env.inner, "m1") is None


async def _ready(env: Env) -> bool:
    return CRITICAL <= env.agent.loops


# --------------------------------------------------------------------------- phase 2 loops


def _render(recorder: MetricsRecorder) -> str:
    body, _ = recorder.render()
    return body.decode()


async def test_metrics_wired_into_drivers_samplers_and_llm(state_store, fake_redis):
    recorder = MetricsRecorder(clock=SystemClock())
    env = await make_env(state_store, fake_redis, recorder=recorder)
    await env.agent.start()
    try:
        rid = await state_store.create_request("hello")

        async def done() -> bool:
            return await state_store.get_status(rid) is RequestStatus.COMPLETED

        await wait_until(done)
        text = _render(recorder)
        assert 'das_lease_acquisitions_total{kind="initial"} 1.0' in text
        assert "das_leases_held 0.0" in text
        assert 'das_requests_finished_total{status="Completed"} 1.0' in text
        # Planner and Summarizer providers got an LLMMetricsObserver.
        assert len(env.agent.llm_observers) == 2
        assert 'das_llm_calls_total{' in text and 'operation="plan"' in text
        # Samplers ran once right after start.
        assert 'das_registry_agents{role="worker",status="Available"} 1.0' in text
        assert 'das_queue_depth{queue="das.results"}' in text
    finally:
        await env.agent.stop()


async def test_index_backfill_runs_once_and_retention_only_when_enabled(state_store, fake_redis):
    env = await make_env(state_store, fake_redis, retention=RetentionConfig(enabled=True))
    await env.agent.start()
    try:
        assert "retention" in env.agent.loops

        async def ready() -> bool:
            return await state_store.index_ready() and "index_backfill" not in env.agent.loops

        await wait_until(ready)
    finally:
        await env.agent.stop()
    assert env.agent.loops == frozenset()


async def test_non_critical_loop_restarts_after_error(state_store, fake_redis, monkeypatch):
    monkeypatch.setattr(agent_module, "NON_CRITICAL_RESTART_DELAY", 0.01)
    env = await make_env(state_store, fake_redis)
    calls: list[int] = []
    original = env.agent.registry_gauges.run

    async def flaky() -> None:
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("boom")
        await original()

    env.agent.registry_gauges.run = flaky  # type: ignore[method-assign]
    await env.agent.start()
    try:
        await wait_until(lambda: _async(len(calls) >= 3))
        assert "registry_gauges" in env.agent.loops
        assert CRITICAL <= env.agent.loops
    finally:
        await env.agent.stop()


async def _async(value: bool) -> bool:
    return value


def test_settings_from_config():
    from das.config.schema import AgentConfig

    cfg = AgentConfig.model_validate(
        {
            "role": "manager",
            "agent_id": "m-9",
            "host": "box",
            "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "box", "advertise_port": 50051},
            "limits": {"lease_duration": 20, "heartbeat_interval": 2, "max_jobs": 5},
        }
    )
    s = ManagerSettings.from_config(cfg)
    assert (s.agent_id, s.host, s.grpc_endpoint) == ("m-9", "box", "box:50051")
    assert (s.lease_duration, s.heartbeat_interval, s.shutdown_timeout) == (20.0, 2.0, 30.0)
    assert s.planner_config.max_jobs == 5
    assert s.machine_config.max_attempts == 3
    assert s.queue_sample_interval == 15.0
    assert s.max_message_bytes == 1_048_576
    assert s.retention.enabled is False
    assert s.price_table is None


# --------------------------------------------------------------------------- drain (task 16.1)


async def _gateway(env: Env) -> MemoryTransport:
    gateway = MemoryTransport(env.control.broker, agent_id="gw")
    await gateway.start()
    return gateway


async def _agent_control(
    gateway: MemoryTransport, command: str, *, release_leases: bool = False
) -> AgentControlReplyMessage:
    payload = AgentControlPayload(command=command, agent_id="m1", release_leases=release_leases)
    reply = await gateway.request_reply("m1", new_message(payload), timeout=5.0)
    assert isinstance(reply, AgentControlReplyMessage)
    return reply


async def test_drain_and_undrain_over_agent_control(state_store, fake_redis):
    env = await make_env(
        state_store, fake_redis, heartbeat_interval=1.0, retention=RetentionConfig(enabled=True)
    )
    await env.agent.start()
    gateway = await _gateway(env)
    try:
        # Req 17.4: drain pauses claiming and writes Draining right away.
        reply = await _agent_control(gateway, "drain")
        assert reply.payload.agent_id == "m1"
        assert reply.payload.status == "Draining" and reply.payload.in_flight == 0
        assert env.agent.draining and env.agent.claim_loop.paused
        assert (await record(env.inner, "m1")).status == "Draining"
        assert "set_status:Draining" in env.registry.calls

        # Req 17.9: draining again only confirms.
        again = await _agent_control(gateway, "drain")
        assert again.payload.status == "Draining"
        assert env.registry.calls.count("set_status:Draining") == 1

        # No new Lease while draining; every other loop keeps running, retention too.
        rid = await state_store.create_request("hello")

        async def draining_heartbeat() -> bool:
            return ("Draining", 0) in env.registry.heartbeats

        await wait_until(draining_heartbeat, timeout=5.0)
        assert await state_store.get_status(rid) is RequestStatus.PENDING
        assert await state_store.get_lease(rid) is None
        assert CRITICAL | {"retention", "registry_gauges"} <= env.agent.loops
        assert (await record(env.inner, "m1")).status == "Draining"

        # Req 17.8: undrain -> Available and the Request is claimed and completed.
        back = await _agent_control(gateway, "undrain")
        assert back.payload.status == "Available"
        assert not env.agent.draining and not env.agent.claim_loop.paused
        assert (await record(env.inner, "m1")).status == "Available"

        async def done() -> bool:
            return await state_store.get_status(rid) is RequestStatus.COMPLETED

        await wait_until(done)
        # Req 17.9: undrain of an Available agent only confirms.
        assert (await _agent_control(gateway, "undrain")).payload.status == "Available"
        assert env.registry.calls.count("set_status:Available") == 1
    finally:
        await gateway.close()
        await env.agent.stop()


async def test_drain_keeps_owned_request_and_reports_in_flight(state_store, fake_redis):
    env = await make_env(state_store, fake_redis, hold=True)
    await env.agent.start()
    try:
        rid = await state_store.create_request("hello")
        await wait_until(lambda: _async(len(env.worker.dispatches) > 0))

        reply = await env.agent.drain()
        assert reply.status == "Draining" and reply.in_flight == 1
        assert (await record(env.inner, "m1")).in_flight == 1
        # Req 17.4: the Lease is kept and renewed.
        await asyncio.sleep(0.2)
        assert rid in env.agent.drivers.owned
        lease = await state_store.get_lease(rid)
        assert lease is not None and lease.owner == "m1"
    finally:
        await env.agent.stop()


async def test_drain_with_release_leases_hands_requests_over(state_store, fake_redis):
    env = await make_env(state_store, fake_redis, hold=True)
    await env.agent.start()
    gateway = await _gateway(env)
    try:
        rid = await state_store.create_request("hello")
        await wait_until(lambda: _async(len(env.worker.dispatches) > 0))
        assert rid in env.agent.drivers.owned

        loop = asyncio.get_running_loop()
        started = loop.time()
        reply = await _agent_control(gateway, "drain", release_leases=True)
        assert reply.payload.status == "Draining"

        # Req 17.5: every Lease is released (fenced) within 5 s; not re-claimed here.
        async def released() -> bool:
            return not env.agent.drivers.owned and await state_store.get_lease(rid) is None

        await wait_until(released, timeout=5.0)
        assert loop.time() - started < 5.0
        await asyncio.sleep(0.2)  # several claim periods: still not re-acquired
        assert await state_store.get_lease(rid) is None
        assert await state_store.get_status(rid) is RequestStatus.RUNNING
        assert (await state_store.acquire_lease(rid, "m2", 30)).ok
    finally:
        await gateway.close()
        await env.agent.stop()


async def test_misrouted_agent_control_gets_no_reply(state_store, fake_redis):
    env = await make_env(state_store, fake_redis)
    await env.agent.start()
    gateway = await _gateway(env)
    try:
        payload = AgentControlPayload(command="drain", agent_id="other")
        with pytest.raises(RequestTimeout):
            await gateway.request_reply("m1", new_message(payload), timeout=1.0)
        assert not env.agent.draining
    finally:
        await gateway.close()
        await env.agent.stop()


async def test_draining_agent_shuts_down_and_restarts_available(state_store, fake_redis):
    env = await make_env(state_store, fake_redis)
    await env.agent.start()
    await env.agent.drain()
    # Req 17.13: graceful shutdown as in v1.
    await env.agent.stop()
    assert await record(env.inner, "m1") is None
    assert env.agent.loops == frozenset()

    # Req 17.16: Draining is not kept across a restart.
    fresh = await make_env(state_store, fake_redis)
    await fresh.agent.start()
    try:
        assert not fresh.agent.draining
        assert (await record(fresh.inner, "m1")).status == "Available"
    finally:
        await fresh.agent.stop()


async def test_paused_claim_loop_acquires_nothing(state_store, fake_redis):
    env = await make_env(state_store, fake_redis)
    rid = await state_store.create_request("hello")
    loop = env.agent.claim_loop
    loop.pause()
    assert await loop.tick() == []
    assert await state_store.get_lease(rid) is None
    loop.resume()
    won = await loop.tick()
    assert [lease.request_id for lease in won] == [rid]
    await env.agent.drivers.stop_all()
