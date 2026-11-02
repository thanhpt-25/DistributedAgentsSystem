"""Worker edge paths (task 15.7).

Timeout, cancellation and LLM-error results as seen end to end through the
Worker; registration failure exit; the capability guard requeue; and the
graceful shutdown sequence. Runs over MemoryTransport + fakeredis registry.

Requirements: 4.5, 5.6, 5.7, 5.8, 5.10, 11.3.
"""

from __future__ import annotations

import asyncio

import pytest

from das.codec import new_message
from das.errors import EXIT_REGISTRATION_FAILED, RegistrationFailed, exit_code_for
from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.models.results import ControlPayload, JobDispatchPayload
from das.registry import AgentRegistry
from das.worker.executor import JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import HANG, FakeAdapter, completion
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)


class RecordingTransport(MemoryTransport):
    """MemoryTransport that records ``close`` into a shared sequence log."""

    def __init__(self, broker: MemoryBroker, seq: list[str], **kw) -> None:
        super().__init__(broker, **kw)
        self._seq = seq

    async def close(self) -> None:
        self._seq.append(f"close:{self.agent_id}")
        await super().close()


class Harness:
    def __init__(self, fake_redis, adapter: FakeAdapter, *, caps=("research",), registry=None,
                 executor=None, job_timeout: float = 30, **settings) -> None:
        self.seq: list[str] = []
        self.broker = MemoryBroker()
        self.queue = RecordingTransport(self.broker, self.seq, agent_id="worker-1")
        self.control = RecordingTransport(MemoryBroker(), self.seq, agent_id="worker-1-control")
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
            executor=executor or JobExecutor(provider, "worker-1", job_timeout=job_timeout),
        )

    async def connect(self) -> None:
        for t in (self.queue, self.control, self.manager_control, self.manager):
            await t.start()

    async def dispatch(self, job_id: str = "job-1", caps=("research",), rid: str = "req-1",
                       attempt: int = 1, dest: str | None = None) -> None:
        payload = JobDispatchPayload(
            request_id=rid, job_id=job_id, attempt=attempt, instruction="do it",
            required_capabilities=list(caps),
        )
        await self.manager.publish(dest or f"jobs/{caps[0]}", new_message(payload, correlation_id=rid))

    async def cancel(self, job_id: str, rid: str = "req-1") -> None:
        cmd = ControlPayload(command="cancel_job", request_id=rid, job_id=job_id)
        await self.manager_control.publish("worker-1", new_message(cmd, correlation_id=rid))

    def results(self):
        return self.broker.pending_messages("results")


async def _until(predicate, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def _single_result(h: Harness):
    await _until(lambda: h.results())
    await h.broker.settle()
    [result] = h.results()
    return result


# --------------------------------------------------------------- result statuses


async def test_job_timeout_publishes_timed_out_result_and_acks(fake_redis):
    """Req 5.6: the call is aborted at job_timeout and a TimedOut result is sent."""
    h = Harness(fake_redis, FakeAdapter(default=HANG), job_timeout=0.05)
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch(attempt=2)
        result = await _single_result(h)
        p = result.payload
        assert (p.status, p.job_id, p.attempt, p.content) == ("TimedOut", "job-1", 2, "")
        assert p.error is not None and "0.05s" in p.error
        assert result.correlation_id == "req-1"
        # Dispatch is settled (ACKed), not redelivered.
        assert h.broker.pending("jobs/research") == []
        assert h.broker.unacked_count("jobs/research") == 0
        assert h.worker.in_flight == 0
    finally:
        await h.worker.stop()


async def test_llm_error_publishes_failed_result_with_provider_model_reason(fake_redis):
    """Req 5.8: provider failure yields Failed, empty content, provider/model/reason."""
    h = Harness(fake_redis, FakeAdapter([PermanentLLMError("quota exhausted")], default="ok"))
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch()
        result = await _single_result(h)
        p = result.payload
        assert p.status == "Failed" and p.content == ""
        assert p.error is not None
        for part in ("fake", "fake-model", "quota exhausted"):
            assert part in p.error
        assert h.broker.pending("jobs/research") == []
        assert h.broker.unacked_count("jobs/research") == 0
        assert len(h.adapter.calls) == 1  # not retried by redelivery
    finally:
        await h.worker.stop()


async def test_executor_crash_publishes_failed_result(fake_redis):
    class Broken:
        async def execute(self, job, cancel):
            raise RuntimeError("bug")

    h = Harness(fake_redis, FakeAdapter(), executor=Broken())
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch()
        p = (await _single_result(h)).payload
        assert p.status == "Failed" and p.content == "" and p.worker_id == "worker-1"
        assert p.error is not None and "RuntimeError" in p.error
        assert h.broker.unacked_count("jobs/research") == 0
    finally:
        await h.worker.stop()


async def test_cancel_job_only_matches_job_and_request(fake_redis):
    """Req 5.7: cancel_job for another Request's same Job_ID is ignored."""
    h = Harness(fake_redis, FakeAdapter(default=HANG))
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch(job_id="job-x", rid="req-1")
        await _until(lambda: h.worker.in_flight == 1)
        await h.cancel("job-x", rid="req-other")
        await asyncio.sleep(0.05)
        assert h.results() == [] and h.worker.in_flight == 1
        await h.cancel("job-x", rid="req-1")
        result = await _single_result(h)
        assert result.payload.status == "Cancelled"
        assert result.payload.content == ""
        assert h.broker.unacked_count("jobs/research") == 0
        # Control commands are acknowledged, not left pending.
        assert h.control.broker.unacked_count("worker-1") == 0
    finally:
        await h.worker.stop()


# --------------------------------------------------------------- registration


async def test_unresponsive_registry_fails_within_deadline_without_subscribing(fake_redis):
    """Req 5.10: no registration within the deadline -> RegistrationFailed (non-zero exit)."""

    class Hanging:
        async def register(self, info):
            await asyncio.Event().wait()

        async def heartbeat(self, agent_id, **_):
            return False

        async def deregister(self, agent_id):
            return False

    h = Harness(fake_redis, FakeAdapter(), registry=Hanging(), registration_timeout=0.2)
    await h.connect()
    await h.dispatch()
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(RegistrationFailed) as info:
        await h.worker.start()
    assert loop.time() - started < 1.0
    assert exit_code_for(info.value) == EXIT_REGISTRATION_FAILED != 0
    assert not h.worker.registered
    assert h.queue.subscriptions == [] and h.control.subscriptions == []
    # Nothing consumed: the dispatch is still waiting on its queue.
    assert len(h.broker.pending("jobs/research")) == 1
    assert h.adapter.calls == []


async def test_transient_registration_errors_are_retried_before_deadline(fake_redis, monkeypatch):
    monkeypatch.setattr("das.worker.worker.REGISTRATION_RETRY_INTERVAL", 0.01)
    real = AgentRegistry(fake_redis)
    attempts: list[int] = []

    class Flaky:
        async def register(self, info):
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("redis down")
            return await real.register(info)

        heartbeat = real.heartbeat
        deregister = real.deregister

    h = Harness(fake_redis, FakeAdapter(), registry=Flaky(), registration_timeout=2)
    await h.connect()
    await h.worker.start()
    try:
        assert len(attempts) == 3 and h.worker.registered
        assert [s.destination for s in h.queue.subscriptions] == ["jobs/research"]
    finally:
        await h.worker.stop()


# --------------------------------------------------------------- capability guard


async def test_capability_guard_pauses_only_that_queue(fake_redis):
    """Req 4.5: a Job needing an undeclared Capability is requeued untouched;
    other Capability queues keep flowing."""
    h = Harness(fake_redis, FakeAdapter(default="ok"), caps=("research", "summarize"),
                guard_pause=5)
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch(job_id="bad", caps=("research", "code-review"), attempt=4)
        await _until(lambda: "research" not in h.worker.subscriptions)
        await h.broker.settle()
        pending = h.broker.pending_messages("jobs/research")
        assert [(m.payload.job_id, m.payload.attempt) for m in pending] == [("bad", 4)]
        assert h.adapter.calls == []

        await h.dispatch(job_id="good", caps=("summarize",))
        result = await _single_result(h)
        assert result.payload.job_id == "good" and result.payload.status == "Succeeded"
        assert "summarize" in h.worker.subscriptions
        assert "research" not in h.worker.subscriptions  # still paused
    finally:
        await h.worker.stop()
    # Stopping cancels the pending resume; the requeued Job is left for others.
    assert h.queue.events.count(("subscribe", "jobs/research")) == 1
    assert len(h.broker.pending("jobs/research")) == 1


# --------------------------------------------------------------- graceful shutdown


async def test_shutdown_drains_in_flight_then_deregisters_then_closes(fake_redis):
    """Req 11.3: stop pulling, finish in-flight work, deregister, close transports."""
    release = asyncio.Event()

    async def slow(system, messages, params):
        await release.wait()
        return completion("late answer")

    seq_registry = AgentRegistry(fake_redis)
    h = Harness(fake_redis, FakeAdapter(default=slow), registry=seq_registry,
                shutdown_timeout=5)
    real_deregister = seq_registry.deregister

    async def deregister(agent_id):
        h.seq.append(
            f"deregister:subs={len(h.queue.subscriptions)}:results={len(h.results())}"
        )
        return await real_deregister(agent_id)

    seq_registry.deregister = deregister  # type: ignore[method-assign]

    await h.connect()
    await h.worker.start()
    await h.dispatch(job_id="in-flight")
    await _until(lambda: h.worker.in_flight == 1)

    stopping = asyncio.create_task(h.worker.stop())
    await _until(lambda: h.worker.stopping and h.queue.subscriptions == [])
    # No new Jobs are pulled once shutdown has begun.
    await h.dispatch(job_id="late")
    await asyncio.sleep(0.05)
    assert len(h.adapter.calls) == 1
    assert not stopping.done()

    release.set()
    await asyncio.wait_for(stopping, 5)

    [result] = h.results()
    assert result.payload.job_id == "in-flight" and result.payload.status == "Succeeded"
    # Drained job ACKed; the late one is untouched for another Worker.
    assert [m.payload.job_id for m in h.broker.pending_messages("jobs/research")] == ["late"]
    assert h.seq == [
        "deregister:subs=0:results=1",
        "close:worker-1",
        "close:worker-1-control",
    ]
    assert (await seq_registry.snapshot()).get("worker-1") is None
    assert not h.queue.connected and not h.control.connected


async def test_stop_is_idempotent_and_run_stops_on_event(fake_redis):
    h = Harness(fake_redis, FakeAdapter())
    await h.connect()
    stop_event = asyncio.Event()
    runner = asyncio.create_task(h.worker.run(stop_event))
    await _until(lambda: h.worker.registered)
    stop_event.set()
    await asyncio.wait_for(runner, 3)
    await h.worker.stop()  # second call returns without repeating the sequence
    assert h.seq == ["close:worker-1", "close:worker-1-control"]
    assert (await h.registry.snapshot()).get("worker-1") is None
