"""Worker process smoke tests over MemoryTransport + fakeredis registry (task 15.2).

Covers Req 4.5, 5.1, 5.2, 5.5, 5.7, 5.9, 5.10, 6.4, 11.1, 11.3, 13.5. Edge
paths are expanded in task 15.7; properties in 15.3-15.6.
"""

from __future__ import annotations

import asyncio

import pytest

from das.codec import MessageCodec, new_message
from das.errors import EXIT_REGISTRATION_FAILED, RegistrationFailed, exit_code_for
from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.models.results import ControlPayload, JobDispatchPayload
from das.registry import AgentRegistry
from das.worker.executor import JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import HANG, FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)
CODEC = MessageCodec()


class ManualClock:
    """Clock whose ``sleep`` blocks until the test calls ``tick``."""

    def __init__(self) -> None:
        self.sleeps: list[float] = []
        self._ticks: asyncio.Queue[None] = asyncio.Queue()

    def now(self) -> float:
        return 0.0

    def monotonic(self) -> float:
        return 0.0

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        await self._ticks.get()

    def tick(self) -> None:
        self._ticks.put_nowait(None)


class Harness:
    def __init__(self, fake_redis, adapter: FakeAdapter, *, caps=("research",), registry=None,
                 clock=None, **settings) -> None:
        self.broker = MemoryBroker()
        self.queue = MemoryTransport(self.broker, agent_id="worker-1")
        self.control = MemoryTransport(MemoryBroker(), agent_id="worker-1-control")
        self.manager_control = MemoryTransport(self.control.broker, agent_id="manager-1")
        self.manager = MemoryTransport(self.broker, agent_id="manager-1")
        self.registry = registry or AgentRegistry(fake_redis)
        self.adapter = adapter
        provider = LLMProvider(adapter, PARAMS, retry_count=0, timeout=10)
        self.worker = Worker(
            WorkerSettings(
                agent_id="worker-1",
                host="host-a",
                capabilities=frozenset(caps),
                concurrency_limit=settings.pop("concurrency_limit", 2),
                **settings,
            ),
            queue=self.queue,
            control=self.control,
            registry=self.registry,
            executor=JobExecutor(provider, "worker-1", job_timeout=30),
            clock=clock,
        )

    async def connect(self) -> None:
        for t in (self.queue, self.control, self.manager_control, self.manager):
            await t.start()

    async def dispatch(self, job_id: str = "job-1", caps=("research",), rid: str = "req-1",
                       attempt: int = 1) -> None:
        payload = JobDispatchPayload(
            request_id=rid, job_id=job_id, attempt=attempt, instruction="do it",
            required_capabilities=list(caps),
        )
        await self.manager.publish(f"jobs/{caps[0]}", new_message(payload, correlation_id=rid))

    def results(self):
        return self.broker.pending_messages("results")


async def _until(predicate, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_registers_then_subscribes_one_queue_per_capability(fake_redis):
    h = Harness(fake_redis, FakeAdapter(), caps=("research", "summarize"))
    await h.connect()
    await h.worker.start()
    try:
        snap = await h.registry.snapshot()
        rec = snap.get("worker-1")
        assert rec is not None and rec.status == "Available"
        assert rec.capabilities == frozenset({"research", "summarize"})
        assert rec.concurrency_limit == 2 and rec.host == "host-a"
        assert sorted(s.destination for s in h.queue.subscriptions) == [
            "jobs/research", "jobs/summarize",
        ]
        assert all(s.options.limiter is h.worker.limiter for s in h.queue.subscriptions)
        assert [s.destination for s in h.control.subscriptions] == ["worker-1"]
    finally:
        await h.worker.stop()


async def test_registration_failure_raises_and_subscribes_nothing(fake_redis):
    class Down:
        async def register(self, info):
            raise ConnectionError("redis down")

        async def heartbeat(self, agent_id, **_):
            return False

        async def deregister(self, agent_id):
            return False

    h = Harness(fake_redis, FakeAdapter(), registry=Down(), registration_timeout=0.3)
    await h.connect()
    with pytest.raises(RegistrationFailed) as info:
        await h.worker.start()
    assert exit_code_for(info.value) == EXIT_REGISTRATION_FAILED != 0
    assert h.queue.subscriptions == [] and h.control.subscriptions == []


async def test_job_result_published_then_dispatch_acked(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="answer"))
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch(attempt=2)
        await _until(lambda: h.results())
        await h.broker.settle()
        [result] = h.results()
        assert result.correlation_id == "req-1"  # Req 13.5
        p = result.payload
        assert (p.job_id, p.attempt, p.worker_id, p.status, p.content) == (
            "job-1", 2, "worker-1", "Succeeded", "answer",
        )
        assert h.broker.pending("jobs/research") == []
        assert h.broker.unacked_count("jobs/research") == 0
        assert len(h.adapter.calls) == 1
    finally:
        await h.worker.stop()


async def test_malformed_dispatch_rejected_and_consumption_continues(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="ok"))
    await h.connect()
    await h.worker.start()
    try:
        h.broker.publish_raw("jobs/research", b"{not json")
        # Valid envelope whose payload lacks an instruction (Req 5.9).
        good = CODEC.encode(new_message(JobDispatchPayload(
            request_id="r", job_id="j", attempt=1, instruction="x",
            required_capabilities=["research"],
        ), correlation_id="r"))
        h.broker.publish_raw("jobs/research", good.replace(b'"instruction":"x",', b""))
        await h.dispatch(job_id="job-ok")
        await _until(lambda: h.results())
        await h.broker.settle()
        assert len(h.broker.pending("dlq")) == 2
        assert [m.payload.job_id for m in h.results()] == ["job-ok"]
        assert len(h.adapter.calls) == 1
    finally:
        await h.worker.stop()


async def test_capability_guard_requeues_and_pauses_queue(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="ok"), guard_pause=0.2)
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch(caps=("research", "code-review"))
        await _until(lambda: "research" not in h.worker.subscriptions)
        await h.broker.settle()
        # Requeued untouched; nothing executed; queue paused.
        assert len(h.broker.pending("jobs/research")) == 1
        assert h.adapter.calls == [] and h.results() == []
        # After the pause the queue is subscribed again (and the Job bounces again).
        await _until(lambda: h.queue.events.count(("subscribe", "jobs/research")) >= 2)
    finally:
        await h.worker.stop()
    assert h.adapter.calls == []


async def test_failed_result_publish_leaves_dispatch_unacked_and_retries(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="ok"))
    await h.connect()
    h.queue.fail_next_publishes(1)
    await h.worker.start()
    try:
        await h.dispatch(attempt=1)
        await _until(lambda: h.results())
        await h.broker.settle()
        assert len(h.adapter.calls) == 2  # redelivered after the failed publish
        assert [m.payload.attempt for m in h.results()] == [1]  # attempt unchanged (6.4)
        assert h.broker.pending("jobs/research") == []
    finally:
        await h.worker.stop()


async def test_cancel_job_command_produces_cancelled_result(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default=HANG))
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch(job_id="job-x", rid="req-9")
        await _until(lambda: h.worker.in_flight == 1)
        unknown = ControlPayload(command="cancel_job", request_id="req-9", job_id="nope")
        await h.manager_control.publish("worker-1", new_message(unknown, correlation_id="req-9"))
        cmd = ControlPayload(command="cancel_job", request_id="req-9", job_id="job-x")
        await h.manager_control.publish("worker-1", new_message(cmd, correlation_id="req-9"))
        await _until(lambda: h.results())
        [result] = h.results()
        assert result.payload.status == "Cancelled"
        assert result.correlation_id == "req-9"
    finally:
        await h.worker.stop()


async def test_heartbeat_once_per_interval(fake_redis):
    calls: list[str] = []
    registry = AgentRegistry(fake_redis)
    real = registry.heartbeat

    async def heartbeat(agent_id, **kwargs):
        calls.append(agent_id)
        return await real(agent_id, **kwargs)

    registry.heartbeat = heartbeat  # type: ignore[method-assign]
    clock = ManualClock()
    h = Harness(fake_redis, FakeAdapter(), registry=registry, clock=clock, heartbeat_interval=3)
    await h.connect()
    await h.worker.start()
    try:
        await _until(lambda: clock.sleeps == [3])
        assert calls == []
        clock.tick()
        await _until(lambda: calls == ["worker-1"] and len(clock.sleeps) == 2)
        # A registry that forgot the agent gets a re-registration.
        await registry.deregister("worker-1")
        clock.tick()
        await _until(lambda: len(clock.sleeps) == 3)
        assert (await registry.snapshot()).get("worker-1") is not None
    finally:
        await h.worker.stop()


async def test_graceful_shutdown_requeues_unfinished_and_deregisters(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default=HANG), shutdown_timeout=1)
    await h.connect()
    await h.worker.start()
    await h.dispatch(job_id="slow", attempt=3)
    await _until(lambda: h.worker.in_flight == 1)
    loop = asyncio.get_running_loop()
    started = loop.time()
    await h.worker.stop()
    assert loop.time() - started <= 1.0 + 0.2
    pending = h.broker.pending_messages("jobs/research")
    assert [(m.payload.job_id, m.payload.attempt) for m in pending] == [("slow", 3)]
    assert h.results() == []
    assert (await h.registry.snapshot()).get("worker-1") is None
    assert not h.queue.connected and not h.control.connected
    assert h.queue.subscriptions == []
