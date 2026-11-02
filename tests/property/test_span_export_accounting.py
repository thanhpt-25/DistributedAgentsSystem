# Feature: das-observability-operations, Property 9: Hạch toán hàng đợi export Span
"""Property 9: Span export queue accounting.

For any ``max_queue_size`` in [1, 64] and any interleaving of ended Spans and
export outcomes (success, ``FAILURE``, exception), driving ``BoundedExportProcessor``
with a scripted fake exporter (no background thread, FakeClock):

- the queue length never exceeds ``max_queue_size``;
- ``exported + dropped + queued == ended`` after every step;
- no Span is exported twice;
- a Span is dropped only when the queue is full.

**Validates: Requirements 8.7**
"""
from __future__ import annotations

from collections.abc import Sequence

from hypothesis import given
from hypothesis import strategies as st
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import SpanContext, TraceFlags

from das.clock import FakeClock
from das.observability.tracing import BoundedExportProcessor

_SUCCESS = "success"
_FAILURE = "failure"
_RAISE = "raise"


class ScriptedExporter(SpanExporter):
    """Fake exporter: each call consumes the next scripted outcome."""

    def __init__(self) -> None:
        self.next: str = _SUCCESS
        self.exported: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        if self.next == _RAISE:
            raise ConnectionError("collector down")
        if self.next == _FAILURE:
            return SpanExportResult.FAILURE
        self.exported.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None


def _span(i: int) -> ReadableSpan:
    ctx = SpanContext(
        trace_id=i + 1, span_id=i + 1, is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return ReadableSpan(name=f"s{i}", context=ctx)


_op = st.one_of(
    st.tuples(st.just("end"), st.integers(min_value=1, max_value=20)),
    st.tuples(st.just("export"), st.sampled_from([_SUCCESS, _FAILURE, _RAISE])),
)


@given(
    max_queue_size=st.integers(min_value=1, max_value=64),
    max_batch=st.integers(min_value=1, max_value=16),
    ops=st.lists(_op, max_size=60),
    shutdown=st.booleans(),
)
def test_span_export_accounting(
    max_queue_size: int, max_batch: int, ops: list[tuple[str, object]], shutdown: bool
) -> None:
    exporter = ScriptedExporter()
    p = BoundedExportProcessor(
        exporter, max_queue_size=max_queue_size, max_batch=max_batch,
        clock=FakeClock(), start_thread=False,
    )
    next_id = 0

    def check() -> None:
        assert 0 <= p.queued <= max_queue_size
        assert p.exported + p.dropped + p.queued == p.ended
        ids = [s.context.span_id for s in exporter.exported]
        assert len(ids) == len(set(ids)), "span exported twice"
        assert p.exported == len(exporter.exported)

    for kind, arg in ops:
        if kind == "end":
            for _ in range(int(arg)):  # type: ignore[call-overload]
                full = p.queued >= max_queue_size
                dropped_before = p.dropped
                p.on_end(_span(next_id))
                next_id += 1
                # Dropped iff the queue was full when the Span ended.
                assert p.dropped - dropped_before == (1 if full else 0)
                check()
        else:
            exporter.next = str(arg)
            dropped_before = p.dropped
            queued_before = p.queued
            ok = p.export_once()
            assert ok == (arg == _SUCCESS or queued_before == 0)
            # A failed batch is put back (queue was not full after popping it): no drop.
            assert p.dropped == dropped_before
            if not ok:
                assert p.queued == queued_before
            check()

    assert p.ended == next_id

    if shutdown:
        exporter.next = _SUCCESS
        p.shutdown(timeout=5.0)
        assert p.queued == 0
        assert p.exported + p.dropped == p.ended
        ids = [s.context.span_id for s in exporter.exported]
        assert len(ids) == len(set(ids))
