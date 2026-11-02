"""Unit tests for ``das.observability.tracing`` (Req 6.9, 6.10, 6.13, 8.1-8.12)."""
from __future__ import annotations

import socket
from collections.abc import Sequence
from typing import Any

import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from das.clock import FakeClock
from das.codec.envelope import TraceContextModel
from das.config.schema import TracingConfig
from das.observability import tracing
from das.observability.tracing import (
    ALLOWED_ATTRIBUTES,
    BoundedExportProcessor,
    NoopTracer,
    OtelTracer,
    TraceContext,
    build_tracer,
)

SAMPLED = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
UNSAMPLED = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"


class _Recorder:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **kw: Any) -> None:
        self.records.append((event, kw))

    debug = info = warning

    def events(self) -> list[str]:
        return [e for e, _ in self.records]


@pytest.fixture
def logs(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(tracing, "log", rec)
    return rec


def _cfg(**kw: Any) -> TracingConfig:
    base: dict[str, Any] = {"enabled": True, "otlp_endpoint": "http://otel:4317"}
    base.update(kw)
    return TracingConfig(**base)


def _tracer(exporter: SpanExporter, clock: FakeClock | None = None, **kw: Any) -> OtelTracer:
    t = build_tracer(_cfg(**kw), "manager", "m-1", clock or FakeClock(), exporter=exporter,
                     start_thread=False)
    assert isinstance(t, OtelTracer)
    return t


def _flush(t: OtelTracer) -> None:
    while t.processor.queued:
        assert t.processor.export_once()


class FakeExporter(SpanExporter):
    """Exporter whose results are scripted; records every exported span."""

    def __init__(self, results: Sequence[object] = ()) -> None:
        self.results = list(results)
        self.exported: list[ReadableSpan] = []
        self.calls = 0

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.calls += 1
        outcome = self.results.pop(0) if self.results else SpanExportResult.SUCCESS
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is SpanExportResult.SUCCESS:
            self.exported.extend(spans)
        return outcome  # type: ignore[return-value]

    def shutdown(self) -> None:
        return None


# ------------------------------------------------------------------- Noop / build


def test_disabled_builds_noop_without_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_socket(*a: object, **k: object) -> None:
        raise AssertionError("socket opened")

    monkeypatch.setattr(socket, "socket", no_socket)
    t = build_tracer(TracingConfig(), "worker", "w-1", FakeClock())
    assert isinstance(t, NoopTracer) and t.enabled is False
    with t.span("worker.job", attributes={"das.job_id": "j"}) as h:
        h.set_attributes({"das.attempt": 1})
        h.add_event("retry")
        h.fail("X")
        assert t.current_context() is None
    assert t.current_context() is None


async def test_noop_shutdown_returns_zero() -> None:
    assert await NoopTracer().shutdown(5) == 0


def test_trace_context_reexported() -> None:
    from das.observability.trace_context import TraceContext as Pure

    assert TraceContext is Pure


def test_exporter_factory_is_lazy_and_protocol_aware() -> None:
    grpc_cfg = _cfg(otlp_headers="x-api-key=abc")
    exp = tracing.make_exporter_factory(grpc_cfg)()
    assert type(exp).__module__.endswith("grpc.trace_exporter")
    exp.shutdown()
    http = tracing.make_exporter_factory(_cfg(otlp_protocol="http/protobuf",
                                              otlp_endpoint="http://otel:4318"))()
    assert type(http).__module__.endswith("http.trace_exporter")
    assert http._endpoint == "http://otel:4318/v1/traces"  # noqa: SLF001
    http.shutdown()


def test_resource_service_name_default_and_explicit() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with t.span("a"):
        pass
    _flush(t)
    assert mem.get_finished_spans()[0].resource.attributes["service.name"] == "das-manager"
    mem2 = InMemorySpanExporter()
    t2 = _tracer(mem2, service_name="custom")
    with t2.span("a"):
        pass
    _flush(t2)
    assert mem2.get_finished_spans()[0].resource.attributes["service.name"] == "custom"


# ------------------------------------------------------------------- spans


def test_child_parent_and_current_context() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with t.span("manager.request", parent=TraceContext.parse(SAMPLED)) as _:
        ctx = t.current_context()
        assert isinstance(ctx, TraceContextModel)
        assert ctx.traceparent.startswith("00-0af7651916cd43dd8448eb211c80319c-")
        assert ctx.traceparent.endswith("-01")
        with t.span("manager.plan"):
            with t.span("llm.completion"):
                pass
    assert t.current_context() is None
    _flush(t)
    spans = {s.name: s for s in mem.get_finished_spans()}
    req, plan, llm = spans["manager.request"], spans["manager.plan"], spans["llm.completion"]
    assert {s.context.trace_id for s in spans.values()} == {0x0AF7651916CD43DD8448EB211C80319C}
    assert req.parent is not None and req.parent.span_id == 0xB7AD6B7169203331
    assert plan.parent.span_id == req.context.span_id  # type: ignore[union-attr]
    assert llm.parent.span_id == plan.context.span_id  # type: ignore[union-attr]
    assert req.attributes["das.role"] == "manager" and req.attributes["das.agent_id"] == "m-1"


def test_parent_model_and_root_flag() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with t.span("outer"):
        with t.span("ingest", root=True):
            pass
        with t.span("w", parent=TraceContextModel(traceparent=SAMPLED)):
            pass
    _flush(t)
    spans = {s.name: s for s in mem.get_finished_spans()}
    assert spans["ingest"].parent is None
    assert spans["ingest"].context.trace_id != spans["outer"].context.trace_id
    assert spans["w"].context.trace_id == 0x0AF7651916CD43DD8448EB211C80319C


def test_unsampled_parent_not_exported_but_context_propagates() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem, sample_ratio=1.0)
    with t.span("worker.job", parent=TraceContext.parse(UNSAMPLED)):
        ctx = t.current_context()
        assert ctx is not None and ctx.traceparent.endswith("-00")
        assert ctx.traceparent[3:35] == "0af7651916cd43dd8448eb211c80319c"
    _flush(t)
    assert mem.get_finished_spans() == ()
    assert t.processor.ended == 0


def test_ratio_zero_root_unsampled_still_has_context() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem, sample_ratio=0.0)
    with t.span("gateway.submit"):
        ctx = t.current_context()
        assert ctx is not None
        parsed = TraceContext.parse(ctx.traceparent)
        assert parsed is not None and not parsed.sampled
    assert t.processor.queued == 0


def test_sampled_parent_overrides_ratio_zero() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem, sample_ratio=0.0)
    with t.span("manager.request", parent=TraceContext.parse(SAMPLED)):
        pass
    _flush(t)
    assert len(mem.get_finished_spans()) == 1


def test_tracestate_propagates() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with t.span("x", parent=TraceContext.parse(SAMPLED, "vendor=abc")):
        ctx = t.current_context()
        assert ctx is not None and ctx.tracestate == "vendor=abc"


def test_exception_sets_error_and_type() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with pytest.raises(KeyError), t.span("worker.job"):
        raise KeyError("boom")
    _flush(t)
    (span,) = mem.get_finished_spans()
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes["error.type"] == "KeyError"
    assert not span.events  # no exception event with message content


def test_fail_and_success() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with t.span("manager.request") as h:
        h.fail("LEASE_LOST")
    with t.span("ok"):
        pass
    _flush(t)
    spans = {s.name: s for s in mem.get_finished_spans()}
    assert spans["manager.request"].status.status_code is StatusCode.ERROR
    assert spans["manager.request"].attributes["error.type"] == "LEASE_LOST"
    assert spans["ok"].status.status_code is not StatusCode.ERROR
    assert "error.type" not in spans["ok"].attributes


def test_attribute_and_event_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    from das.observability.redaction import Redactor

    monkeypatch.setattr(tracing, "get_redactor", lambda: Redactor(["sk-supersecretkey"]))
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with t.span("llm.completion", attributes={"prompt": "hello", "llm.model": "m",
                                               "das.attempt": 2, "llm.provider": 1.5}) as h:
        h.set_attributes({"instruction": "do x", "llm.input_tokens": 3, "api_key": "k",
                          "das.job_id": "j-sk-supersecretkey"})
        h.add_event("retry", {"attempt": 1, "error.type": "Timeout", "content": "c"})
    _flush(t)
    (span,) = mem.get_finished_spans()
    assert set(span.attributes) <= ALLOWED_ATTRIBUTES
    assert span.attributes["llm.model"] == "m" and span.attributes["das.attempt"] == 2
    assert "llm.provider" not in span.attributes  # float rejected
    assert "sk-supersecretkey" not in str(span.attributes["das.job_id"])
    (event,) = span.events
    assert event.name == "retry" and dict(event.attributes or {}) == {"error.type": "Timeout"}


async def test_tracer_shutdown_flushes() -> None:
    mem = InMemorySpanExporter()
    t = _tracer(mem)
    with t.span("a"):
        pass
    assert await t.shutdown(10) == 0
    assert len(mem.get_finished_spans()) == 1


# ------------------------------------------------------------------- processor


def _ended_span(tracer: OtelTracer, name: str = "s") -> None:
    with tracer.span(name):
        pass


def test_queue_full_drops_and_counts(logs: _Recorder) -> None:
    fake = FakeExporter()
    t = _tracer(fake, max_queue_size=3)
    for _ in range(5):
        _ended_span(t)
    p = t.processor
    assert (p.queued, p.dropped, p.ended) == (3, 2, 5)
    assert p.export_once()
    assert (p.exported, p.queued) == (3, 0)
    assert "spans dropped" in logs.events()
    _, kw = logs.records[logs.events().index("spans dropped")]
    assert kw["dropped"] == 2


def test_drop_warning_rate_limited(logs: _Recorder) -> None:
    clock = FakeClock()
    fake = FakeExporter()
    t = _tracer(fake, clock=clock, max_queue_size=1)
    p = t.processor
    for _ in range(3):
        _ended_span(t)
    p.export_once()
    for _ in range(3):
        _ended_span(t)
    p.export_once()
    assert logs.events().count("spans dropped") == 1
    clock.advance(60)
    p.export_once()
    assert logs.events().count("spans dropped") == 2


def test_failure_requeues_at_head_with_backoff(logs: _Recorder) -> None:
    clock = FakeClock()
    fake = FakeExporter([SpanExportResult.FAILURE, ConnectionError("down"),
                         SpanExportResult.FAILURE])
    t = _tracer(fake, clock=clock, max_queue_size=10)
    p = t.processor
    for i in range(3):
        _ended_span(t, f"s{i}")
    assert not p.export_once()
    assert p.backoff == 1.0 and p.queued == 3
    assert logs.events().count("otlp_endpoint_unreachable") == 1
    assert not p.export_once()
    assert p.backoff == 2.0
    assert not p.export_once()
    assert p.backoff == 4.0
    assert logs.events().count("otlp_endpoint_unreachable") == 1
    assert p.export_once()
    assert p.backoff == 0.0
    assert [s.name for s in fake.exported] == ["s0", "s1", "s2"]
    assert p.exported + p.dropped + p.queued == p.ended


def test_backoff_caps_at_30() -> None:
    fake = FakeExporter([SpanExportResult.FAILURE] * 10)
    t = _tracer(fake)
    _ended_span(t)
    for _ in range(10):
        t.processor.export_once()
    assert t.processor.backoff == 30.0


def test_batch_size_limit() -> None:
    fake = FakeExporter()
    t = _tracer(fake, max_queue_size=2048)
    for _ in range(600):
        _ended_span(t)
    t.processor.export_once()
    assert t.processor.exported == 512 and t.processor.queued == 88


def test_lazy_exporter_factory_failure_is_contained(logs: _Recorder) -> None:
    calls = 0

    def factory() -> SpanExporter:
        nonlocal calls
        calls += 1
        raise RuntimeError("cannot build")

    p = BoundedExportProcessor(factory, max_queue_size=4, clock=FakeClock(), start_thread=False)
    assert calls == 0  # nothing built at startup (Req 8.12)
    t = OtelTracer(tracing.TracerProvider(), p)
    t._provider.add_span_processor(p)  # noqa: SLF001
    _ended_span(t)
    assert not p.export_once()
    assert calls == 1 and p.queued == 1
    assert "otlp_endpoint_unreachable" in logs.events()


def test_shutdown_drops_unexported_and_returns_count(logs: _Recorder) -> None:
    fake = FakeExporter([SpanExportResult.FAILURE])
    t = _tracer(fake, max_queue_size=10)
    for _ in range(4):
        _ended_span(t)
    p = t.processor
    assert p.shutdown(10) == 4
    assert p.queued == 0 and p.dropped == 4
    assert "spans dropped at shutdown" in logs.events()
    _ended_span(t)  # ignored after shutdown
    assert p.ended == 4
    assert p.shutdown(10) == 0


def test_shutdown_zero_budget_drops_everything() -> None:
    fake = FakeExporter()
    t = _tracer(fake)
    _ended_span(t)
    assert t.processor.shutdown(0) == 1
    assert fake.calls == 0


def test_background_thread_exports() -> None:
    mem = InMemorySpanExporter()
    p = BoundedExportProcessor(mem, max_queue_size=8, schedule_delay=0.01)
    provider = tracing.TracerProvider()
    provider.add_span_processor(p)
    t = OtelTracer(provider, p)
    _ended_span(t)
    import time

    deadline = time.monotonic() + 5
    while not mem.get_finished_spans() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(mem.get_finished_spans()) == 1
    assert p.shutdown(1) == 0
