"""Worker metrics, ``worker.job`` Span and Job_Result LLM headers (task 12.1).

Covers Req 2.8, 2.9, 6.5, 9.2 (headers, design D2), 9.6, 20.5.
"""
from __future__ import annotations

import asyncio

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from das.clock import FakeClock
from das.codec import MessageCodec, new_message
from das.codec.envelope import TraceContextModel
from das.config.schema import TracingConfig
from das.llm.base import Completion, ModelParams
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload, TokenUsage
from das.observability.metrics import MetricsRecorder, WorkerMetrics
from das.observability.tracing import OtelTracer, build_tracer
from das.registry import AgentRegistry
from das.worker.executor import MODEL_HEADER, PROVIDER_HEADER, JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import HANG, FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)
CODEC = MessageCodec()
PARENT = TraceContextModel(traceparent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")


class StepClock:
    """``monotonic`` advances ``step`` seconds per call; ``sleep`` blocks (no heartbeat)."""

    def __init__(self, step: float = 1.5) -> None:
        self._t = 0.0
        self._step = step

    def now(self) -> float:
        return 0.0

    def monotonic(self) -> float:
        self._t += self._step
        return self._t

    async def sleep(self, seconds: float) -> None:
        await asyncio.Event().wait()


class Harness:
    def __init__(self, fake_redis, adapter: FakeAdapter, *, tracer=None, clock=None,
                 concurrency_limit: int = 3) -> None:
        self.broker = MemoryBroker()
        self.queue = MemoryTransport(self.broker, agent_id="worker-1")
        self.control = MemoryTransport(MemoryBroker(), agent_id="worker-1-control")
        self.manager = MemoryTransport(self.broker, agent_id="manager-1")
        self.recorder = MetricsRecorder(clock=FakeClock())
        provider = LLMProvider(adapter, PARAMS, retry_count=0, timeout=10, tracer=tracer)
        self.worker = Worker(
            WorkerSettings(
                agent_id="worker-1", host="h", capabilities=frozenset({"research"}),
                concurrency_limit=concurrency_limit,
            ),
            queue=self.queue,
            control=self.control,
            registry=AgentRegistry(fake_redis),
            executor=JobExecutor(provider, "worker-1", job_timeout=30),
            clock=clock or StepClock(),
            metrics=WorkerMetrics(self.recorder),
            tracer=tracer,
        )

    async def start(self) -> None:
        for t in (self.queue, self.control, self.manager):
            await t.start()
        await self.worker.start()

    async def dispatch(self, job_id: str = "job-1", trace_context=None) -> None:
        payload = JobDispatchPayload(
            request_id="req-1", job_id=job_id, attempt=1, instruction="do it",
            required_capabilities=["research"],
        )
        await self.manager.publish(
            "jobs/research",
            new_message(payload, correlation_id="req-1", trace_context=trace_context),
        )

    def sample(self, name: str, **labels: str) -> float | None:
        return self.recorder.registry.get_sample_value(name, labels)


async def _until(predicate, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def _tracer(exporter: InMemorySpanExporter) -> OtelTracer:
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317")
    t = build_tracer(cfg, "worker", "worker-1", FakeClock(), exporter=exporter, start_thread=False)
    assert isinstance(t, OtelTracer)
    return t


async def test_execution_seconds_observed_once_and_headers_published(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="answer"))
    await h.start()
    try:
        await h.dispatch()
        await _until(lambda: h.broker.pending("results"))
        await h.broker.settle()
        labels = {"capability": "research", "status": "Succeeded"}
        assert h.sample("das_job_execution_seconds_count", **labels) == 1
        assert h.sample("das_job_execution_seconds_sum", **labels) == 1.5
        [record] = h.broker.wire_for("results")
        assert record.headers[PROVIDER_HEADER] == "fake"
        assert record.headers[MODEL_HEADER] == "fake-model"
        # Tracing disabled: no Trace_Context on the wire (Req 6.13).
        [msg] = h.broker.pending_messages("results")
        assert msg.trace_context is None
    finally:
        await h.worker.stop()


async def test_failed_result_carries_headers_and_status_label(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default=ValueError("boom")))
    await h.start()
    try:
        await h.dispatch()
        await _until(lambda: h.broker.pending("results"))
        [msg] = h.broker.pending_messages("results")
        assert msg.payload.status == "Failed"
        assert h.sample(
            "das_job_execution_seconds_count", capability="research", status="Failed"
        ) == 1
        [record] = h.broker.wire_for("results")
        assert record.headers[PROVIDER_HEADER] == "fake"
        assert record.headers[MODEL_HEADER] == "fake-model"
    finally:
        await h.worker.stop()


async def test_publish_retry_does_not_record_again(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="answer"))
    await h.start()
    try:
        h.queue.fail_next_publishes(2)
        await h.dispatch()
        await _until(lambda: h.broker.pending("results"))
        await h.broker.settle()
        assert len(h.broker.wire_for("results")) == 1
        # Two failed publishes then a redelivered success: still one observation.
        assert h.sample(
            "das_job_execution_seconds_count", capability="research", status="Succeeded"
        ) == 1
    finally:
        await h.worker.stop()


async def test_in_flight_gauge_reads_at_scrape_time_and_limit_is_set(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default=HANG), concurrency_limit=3)
    await h.start()
    try:
        assert h.sample("das_worker_concurrency_limit") == 3
        assert h.sample("das_worker_jobs_in_flight") == 0
        await h.dispatch("job-1")
        await h.dispatch("job-2")
        await _until(lambda: h.worker.in_flight == 2)
        assert h.sample("das_worker_jobs_in_flight") == 2
        for entry in list(h.worker._inflight):
            entry.cancel.set()
        await _until(lambda: h.worker.in_flight == 0)
        assert h.sample("das_worker_jobs_in_flight") == 0
    finally:
        await h.worker.stop()


async def test_worker_job_span_child_of_dispatch_and_result_carries_its_context(fake_redis):
    mem = InMemorySpanExporter()
    tracer = _tracer(mem)
    h = Harness(fake_redis, FakeAdapter(default=Completion(content="ok", usage=TokenUsage(
        input_tokens=3, output_tokens=4, total_tokens=7))), tracer=tracer)
    await h.start()
    try:
        await h.dispatch(trace_context=PARENT)
        await _until(lambda: h.broker.pending("results"))
        await h.broker.settle()
    finally:
        await h.worker.stop()
    while tracer.processor.queued:
        assert tracer.processor.export_once()
    spans = {s.name: s for s in mem.get_finished_spans()}
    job_span = spans["worker.job"]
    assert format(job_span.context.trace_id, "032x") == "0af7651916cd43dd8448eb211c80319c"
    assert job_span.parent is not None and format(job_span.parent.span_id, "016x") == (
        "b7ad6b7169203331"
    )
    assert job_span.attributes["das.capability"] == "research"
    assert job_span.attributes["das.job_id"] == "job-1"
    llm = spans["llm.completion"]  # nested under worker.job via the copied asyncio context
    assert llm.parent is not None and llm.parent.span_id == job_span.context.span_id
    [msg] = h.broker.pending_messages("results")
    assert msg.trace_context is not None
    assert msg.trace_context.traceparent == (
        f"00-{job_span.context.trace_id:032x}-{job_span.context.span_id:016x}-01"
    )


async def test_cancelled_result_also_carries_worker_job_context(fake_redis):
    mem = InMemorySpanExporter()
    tracer = _tracer(mem)
    h = Harness(fake_redis, FakeAdapter(default=HANG), tracer=tracer)
    await h.start()
    try:
        await h.dispatch(trace_context=PARENT)
        await _until(lambda: h.worker.in_flight == 1)
        for entry in list(h.worker._inflight):
            entry.cancel.set()
        await _until(lambda: h.broker.pending("results"))
        [msg] = h.broker.pending_messages("results")
        assert msg.payload.status == "Cancelled"
        assert msg.trace_context is not None
        assert msg.trace_context.traceparent.split("-")[1] == "0af7651916cd43dd8448eb211c80319c"
        [record] = h.broker.wire_for("results")
        assert record.headers[MODEL_HEADER] == "fake-model"
    finally:
        await h.worker.stop()


async def test_failed_result_carries_worker_job_context_and_headers(fake_redis):
    """Req 6.5: a Failed Job_Result still carries the ``worker.job`` Trace_Context."""
    mem = InMemorySpanExporter()
    tracer = _tracer(mem)
    h = Harness(fake_redis, FakeAdapter(default=ValueError("boom")), tracer=tracer)
    await h.start()
    try:
        await h.dispatch(trace_context=PARENT)
        await _until(lambda: h.broker.pending("results"))
        await h.broker.settle()
    finally:
        await h.worker.stop()
    while tracer.processor.queued:
        assert tracer.processor.export_once()
    job_span = {s.name: s for s in mem.get_finished_spans()}["worker.job"]
    [msg] = h.broker.pending_messages("results")
    assert msg.payload.status == "Failed"
    assert msg.trace_context is not None
    assert msg.trace_context.traceparent == (
        f"00-{job_span.context.trace_id:032x}-{job_span.context.span_id:016x}-01"
    )
    [record] = h.broker.wire_for("results")
    assert record.headers[PROVIDER_HEADER] == "fake"
    assert record.headers[MODEL_HEADER] == "fake-model"


async def test_in_flight_gauge_bounded_by_concurrency_limit(fake_redis):
    """Req 2.9: gauge stays within 0..concurrency even when more Jobs are dispatched."""
    limit = 2
    h = Harness(fake_redis, FakeAdapter(default=HANG), concurrency_limit=limit)
    await h.start()
    try:
        for i in range(5):
            await h.dispatch(f"job-{i}")
        await _until(lambda: h.worker.in_flight == limit)
        await asyncio.sleep(0.05)  # give the worker a chance to over-admit if it would
        value = h.sample("das_worker_jobs_in_flight")
        assert value is not None and 0 <= value <= limit
        assert value == limit
        assert h.sample("das_worker_concurrency_limit") == limit
        # Drain everything; the gauge never exceeds the limit along the way.
        async with asyncio.timeout(5):
            while h.worker.in_flight or len(h.broker.pending_messages("results")) < 5:
                assert 0 <= h.sample("das_worker_jobs_in_flight") <= limit
                for entry in list(h.worker._inflight):
                    entry.cancel.set()
                await asyncio.sleep(0.01)
        assert h.sample("das_worker_jobs_in_flight") == 0
    finally:
        await h.worker.stop()


def test_executor_result_headers():
    provider = LLMProvider(FakeAdapter(), PARAMS, retry_count=0, timeout=10)
    assert JobExecutor(provider, "w").result_headers() == {
        PROVIDER_HEADER: "fake", MODEL_HEADER: "fake-model",
    }
