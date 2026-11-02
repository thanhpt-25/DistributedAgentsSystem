"""Property test for log-trace correlation (task 6.8).

**Property 10: Tương quan log-trace**

For any randomly generated nested Span tree (depth up to 5), with an arbitrary
sampled flag at the root and log entries emitted at arbitrary positions (inside
or outside Spans), every log entry emitted while a sampled Span is active has
``trace_id`` / ``span_id`` equal to that Span's ids (32 / 16 lowercase hex).
Entries emitted with no sampled Span active, including under ``NoopTracer``,
have neither field.

**Validates: Requirements 8.9, 8.10, 8.11**
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import count

from hypothesis import given, settings
from hypothesis import strategies as st
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from das.codec.envelope import TraceContextModel
from das.config.schema import TracingConfig
from das.observability import configure_logging, get_logger
from das.observability.tracing import NoopTracer, OtelTracer, Tracer, build_tracer

TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
SPAN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
MAX_DEPTH = 5

# --------------------------------------------------------------------------- tree model


@dataclass(frozen=True)
class Log:
    """A log call; ``forge`` passes caller-supplied (bogus) trace ids."""

    forge: bool


@dataclass(frozen=True)
class Span:
    children: tuple[Log | Span, ...] = field(default_factory=tuple)


def items(depth: int) -> st.SearchStrategy[tuple[Log | Span, ...]]:
    log = st.builds(Log, forge=st.booleans())
    if depth >= MAX_DEPTH:
        return st.lists(log, max_size=3).map(tuple)
    child = st.deferred(lambda: st.builds(Span, children=items(depth + 1)))
    return st.lists(st.one_of(log, child), max_size=4).map(tuple)


@dataclass(frozen=True)
class Scenario:
    sampled: bool
    trace_id: int
    parent_span_id: int
    before: tuple[Log, ...]
    root: Span
    after: tuple[Log, ...]


scenarios = st.builds(
    Scenario,
    sampled=st.booleans(),
    trace_id=st.integers(min_value=1, max_value=2**128 - 1),
    parent_span_id=st.integers(min_value=1, max_value=2**64 - 1),
    before=st.lists(st.builds(Log, forge=st.booleans()), max_size=2).map(tuple),
    root=st.builds(Span, children=items(1)),
    after=st.lists(st.builds(Log, forge=st.booleans()), max_size=2).map(tuple),
)


def traceparent(s: Scenario) -> str:
    flags = "01" if s.sampled else "00"
    return f"00-{s.trace_id:032x}-{s.parent_span_id:016x}-{flags}"


# --------------------------------------------------------------------------- execution


class Runner:
    """Executes a scenario, recording for each log message the enclosing span name."""

    def __init__(self, tracer: Tracer) -> None:
        self.tracer = tracer
        self.log = get_logger("prop.trace")
        self.ids = count()
        self.enclosing: dict[str, str | None] = {}

    def emit(self, item: Log, span_name: str | None) -> None:
        message = f"log-{next(self.ids)}"
        self.enclosing[message] = span_name
        extra = {"trace_id": "f" * 32, "span_id": "e" * 16} if item.forge else {}
        self.log.info(message, **extra)

    def span(self, node: Span, parent: TraceContextModel | None = None) -> None:
        name = f"span-{next(self.ids)}"
        with self.tracer.span(name, parent=parent):
            for child in node.children:
                if isinstance(child, Log):
                    self.emit(child, name)
                else:
                    self.span(child)

    def run(self, s: Scenario) -> None:
        for item in s.before:
            self.emit(item, None)
        self.span(s.root, parent=TraceContextModel(traceparent=traceparent(s)))
        for item in s.after:
            self.emit(item, None)


@contextmanager
def capture_logs() -> Iterator[io.StringIO]:
    stream = io.StringIO()
    configure_logging("prop-trace", stream=stream)
    try:
        yield stream
    finally:
        configure_logging("test", stream=io.StringIO())


def entries(stream: io.StringIO) -> dict[str, dict]:
    parsed = [json.loads(line) for line in stream.getvalue().splitlines()]
    return {e["message"]: e for e in parsed if e["message"].startswith("log-")}


def otel_tracer(exporter: InMemorySpanExporter) -> OtelTracer:
    tracer = build_tracer(
        TracingConfig(enabled=True, otlp_endpoint="http://127.0.0.1:1", sample_ratio=1.0),
        role="worker",
        agent_id="w-1",
        exporter=exporter,
        start_thread=False,
    )
    assert isinstance(tracer, OtelTracer)
    return tracer


# --------------------------------------------------------------------------- properties


@settings(max_examples=150, deadline=None)
@given(scenarios)
def test_log_entries_correlate_with_active_sampled_span(s: Scenario) -> None:
    exporter = InMemorySpanExporter()
    tracer = otel_tracer(exporter)
    with capture_logs() as stream:
        runner = Runner(tracer)
        runner.run(s)
        assert tracer.processor.force_flush()
        logged = entries(stream)

    # Only sampled spans reach the exporter; map name -> exported ids.
    exported = {}
    for span in exporter.get_finished_spans():
        ctx = span.get_span_context()
        assert ctx.trace_flags.sampled
        exported[span.name] = (format(ctx.trace_id, "032x"), format(ctx.span_id, "016x"))
    if not s.sampled:
        assert exported == {}

    assert set(logged) == set(runner.enclosing)
    for message, span_name in runner.enclosing.items():
        entry = logged[message]
        if span_name is not None and s.sampled:
            trace_id, span_id = exported[span_name]
            assert entry.get("trace_id") == trace_id, message
            assert entry.get("span_id") == span_id, message
            assert TRACE_ID_RE.match(entry["trace_id"])
            assert SPAN_ID_RE.match(entry["span_id"])
            assert entry["trace_id"] == f"{s.trace_id:032x}"
        else:
            assert "trace_id" not in entry, message
            assert "span_id" not in entry, message


@settings(max_examples=50, deadline=None)
@given(scenarios)
def test_noop_tracer_logs_never_carry_trace_ids(s: Scenario) -> None:
    with capture_logs() as stream:
        runner = Runner(NoopTracer())
        runner.run(s)
        logged = entries(stream)

    assert set(logged) == set(runner.enclosing)
    for message, entry in logged.items():
        assert "trace_id" not in entry, message
        assert "span_id" not in entry, message
