"""OpenTelemetry tracing shell (Requirements 6, 8; design §3).

``build_tracer`` returns a :class:`NoopTracer` when ``tracing.enabled`` is false:
it creates no Span, opens no connection and never yields a Trace_Context
(Req 6.13, 8.2). When enabled it returns an :class:`OtelTracer` backed by a
private ``TracerProvider`` (the global OpenTelemetry provider is not touched)
with ``Resource(service.name)``, a ``ParentBased(TraceIdRatioBased(ratio))``
sampler (Req 8.4) and a :class:`BoundedExportProcessor` around a lazily built
OTLP exporter (gRPC or HTTP/protobuf, Req 8.12).

Spans are made current in the OpenTelemetry context, so the ``add_trace_ids``
log processor sees them (Req 8.9-8.11).
"""
from __future__ import annotations

import asyncio
import threading
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from das.clock import Clock, SystemClock
from das.codec.envelope import TraceContextModel
from das.observability.logging import get_logger, get_redactor
from das.observability.trace_context import TraceContext

if TYPE_CHECKING:
    from das.config.schema import TracingConfig

__all__ = [
    "ALLOWED_ATTRIBUTES",
    "BoundedExportProcessor",
    "NoopTracer",
    "OtelTracer",
    "SpanHandle",
    "TraceContext",
    "Tracer",
    "build_tracer",
    "filter_attributes",
]

log = get_logger(__name__)

ALLOWED_ATTRIBUTES: Final = frozenset(
    {
        "das.request_id",
        "das.job_id",
        "das.attempt",
        "das.capability",
        "das.agent_id",
        "das.role",
        "llm.provider",
        "llm.model",
        "llm.operation",
        "llm.input_tokens",
        "llm.output_tokens",
        "error.type",
    }
)

#: Anything with W3C ``traceparent``/``tracestate`` strings: :class:`TraceContext`
#: (pure core) or :class:`TraceContextModel` (envelope / State_Store).
ParentContext = TraceContext | TraceContextModel

_EMPTY: Mapping[str, str | int] = {}
_PROPAGATOR = TraceContextTextMapPropagator()


def filter_attributes(attrs: Mapping[str, object]) -> dict[str, str | int]:
    """Keep only allowlisted keys with ``str``/``int`` values (Req 6.9).

    String values are additionally passed through the process Redactor so a
    configured secret can never reach an exported Span.
    """
    out: dict[str, str | int] = {}
    redactor = get_redactor()
    for key, value in attrs.items():
        if key not in ALLOWED_ATTRIBUTES:
            continue
        if isinstance(value, bool) or not isinstance(value, str | int):
            continue
        out[key] = redactor.redact(value) if isinstance(value, str) and redactor else value
    return out


# --------------------------------------------------------------------------- protocols


@runtime_checkable
class SpanHandle(Protocol):
    def set_attributes(self, attrs: Mapping[str, str | int]) -> None: ...

    def add_event(self, name: str, attrs: Mapping[str, str | int] = _EMPTY) -> None: ...

    def fail(self, error_type: str) -> None: ...


@runtime_checkable
class Tracer(Protocol):
    enabled: bool

    def span(
        self,
        name: str,
        *,
        parent: ParentContext | None = None,
        attributes: Mapping[str, str | int] = _EMPTY,
        root: bool = False,
    ) -> AbstractContextManager[SpanHandle]: ...

    def current_context(self) -> TraceContextModel | None: ...

    async def shutdown(self, timeout: float) -> int: ...


# --------------------------------------------------------------------------- noop


class _NoopSpanHandle:
    def set_attributes(self, attrs: Mapping[str, str | int]) -> None:
        return None

    def add_event(self, name: str, attrs: Mapping[str, str | int] = _EMPTY) -> None:
        return None

    def fail(self, error_type: str) -> None:
        return None


_NOOP_HANDLE: Final = _NoopSpanHandle()


class NoopTracer:
    """Tracer used when tracing is disabled: no Span, no connection (Req 6.13, 8.2)."""

    enabled = False

    @contextmanager
    def span(
        self,
        name: str,
        *,
        parent: ParentContext | None = None,
        attributes: Mapping[str, str | int] = _EMPTY,
        root: bool = False,
    ) -> Iterator[SpanHandle]:
        yield _NOOP_HANDLE

    def current_context(self) -> TraceContextModel | None:
        return None

    async def shutdown(self, timeout: float) -> int:
        return 0


# --------------------------------------------------------------------------- otel


class _OtelSpanHandle:
    def __init__(self, span: otel_trace.Span) -> None:
        self._span = span

    def set_attributes(self, attrs: Mapping[str, str | int]) -> None:
        clean = filter_attributes(attrs)
        if clean:
            self._span.set_attributes(clean)

    def add_event(self, name: str, attrs: Mapping[str, str | int] = _EMPTY) -> None:
        self._span.add_event(name, filter_attributes(attrs))

    def fail(self, error_type: str) -> None:
        """Mark a non-exception failure (``LEASE_LOST``, ``REQUEST_TERMINAL``, ...) (Req 6.10)."""
        self._span.set_attribute("error.type", str(error_type))
        self._span.set_status(Status(StatusCode.ERROR))


def _to_model(span_context: otel_trace.SpanContext) -> TraceContextModel | None:
    if not span_context.is_valid:
        return None
    traceparent = (
        f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-"
        f"{int(span_context.trace_flags):02x}"
    )
    state = span_context.trace_state.to_header() if span_context.trace_state else ""
    parsed = TraceContext.parse(traceparent, state or None)
    if parsed is None:
        return None
    return TraceContextModel(traceparent=parsed.traceparent, tracestate=parsed.tracestate)


class OtelTracer:
    """Tracer backed by a private OpenTelemetry ``TracerProvider``."""

    enabled = True

    def __init__(
        self,
        provider: TracerProvider,
        processor: BoundedExportProcessor,
        *,
        default_attributes: Mapping[str, str | int] = _EMPTY,
    ) -> None:
        self._provider = provider
        self._processor = processor
        self._tracer = provider.get_tracer("das")
        self._defaults = filter_attributes(default_attributes)

    @property
    def processor(self) -> BoundedExportProcessor:
        return self._processor

    @contextmanager
    def span(
        self,
        name: str,
        *,
        parent: ParentContext | None = None,
        attributes: Mapping[str, str | int] = _EMPTY,
        root: bool = False,
    ) -> Iterator[SpanHandle]:
        """Open ``name`` as the current Span.

        ``parent`` (a W3C context) wins; otherwise ``root=True`` starts a new trace
        and the default is a child of the currently active Span. An unhandled
        exception sets status error and ``error.type`` to its class name (Req 6.10).
        """
        if parent is not None:
            carrier = {"traceparent": parent.traceparent}
            if parent.tracestate:
                carrier["tracestate"] = parent.tracestate
            ctx: otel_context.Context | None = _PROPAGATOR.extract(carrier)
        elif root:
            ctx = otel_context.Context()
        else:
            ctx = None
        attrs = {**self._defaults, **filter_attributes(attributes)}
        with self._tracer.start_as_current_span(
            name,
            context=ctx,
            attributes=attrs,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            handle = _OtelSpanHandle(span)
            try:
                yield handle
            except BaseException as exc:
                if not isinstance(exc, GeneratorExit):
                    handle.fail(type(exc).__name__)
                raise

    def current_context(self) -> TraceContextModel | None:
        """Trace_Context of the active Span, sampled or not (Req 6.1, 6.4, 6.8)."""
        return _to_model(otel_trace.get_current_span().get_span_context())

    async def shutdown(self, timeout: float) -> int:
        """Flush for ``min(5, timeout)`` seconds; returns Spans discarded (Req 8.8)."""
        return await asyncio.to_thread(self._processor.shutdown, timeout)


# --------------------------------------------------------------------------- export


class BoundedExportProcessor(SpanProcessor):
    """Bounded, non-blocking span queue with a background exporter (design D12, Req 8.7).

    ``on_end`` only takes a lock and appends (O(1), no I/O). A full queue drops the
    Span and counts it. A daemon thread exports batches of at most ``max_batch``
    every ``schedule_delay`` seconds or as soon as a full batch is waiting. A failed
    batch is put back at the head of the queue (bounded by ``max_queue_size``; the
    excess is dropped and counted) and the next attempt waits with backoff 1→30 s.

    The exporter is created lazily on first export so an unreachable endpoint never
    blocks startup (Req 8.12). Accounting: ``exported + dropped + queued == ended``
    whenever no export is in flight.
    """

    DROP_WARN_INTERVAL: Final = 60.0
    SHUTDOWN_FLUSH_MAX: Final = 5.0
    BACKOFF_INITIAL: Final = 1.0
    BACKOFF_MAX: Final = 30.0

    def __init__(
        self,
        exporter: SpanExporter | Callable[[], SpanExporter],
        *,
        max_queue_size: int = 2048,
        max_batch: int = 512,
        schedule_delay: float = 5.0,
        clock: Clock | None = None,
        start_thread: bool = True,
    ) -> None:
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be >= 1")
        if isinstance(exporter, SpanExporter):
            self._exporter: SpanExporter | None = exporter
            self._factory: Callable[[], SpanExporter] | None = None
        else:
            self._exporter = None
            self._factory = exporter
        self.max_queue_size = max_queue_size
        self.max_batch = max_batch
        self.schedule_delay = schedule_delay
        self._clock: Clock = clock or SystemClock()
        self._queue: deque[ReadableSpan] = deque()
        self._lock = threading.Lock()
        self._export_lock = threading.Lock()
        self._wake = threading.Event()
        self._stopped = False
        self.ended = 0
        self.exported = 0
        self.dropped = 0
        self._dropped_reported = 0
        self._last_drop_warn: float | None = None
        self._backoff = 0.0
        self._next_attempt = 0.0
        self._failing = False
        self._thread: threading.Thread | None = None
        if start_thread:
            self._thread = threading.Thread(
                target=self._run, name="das-span-export", daemon=True
            )
            self._thread.start()

    # -- SpanProcessor -------------------------------------------------------------

    def on_start(self, span: Any, parent_context: Any = None) -> None:
        return None

    def on_end(self, span: ReadableSpan) -> None:
        ctx = span.context
        if ctx is None or not ctx.trace_flags.sampled:
            return
        wake = False
        with self._lock:
            if self._stopped:
                return
            self.ended += 1
            if len(self._queue) >= self.max_queue_size:
                self.dropped += 1
            else:
                self._queue.append(span)
                wake = len(self._queue) >= self.max_batch
        if wake:
            self._wake.set()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        deadline = self._clock.monotonic() + timeout_millis / 1000
        while self.queued and self._clock.monotonic() < deadline:
            if not self.export_once():
                return False
        return not self.queued

    # -- state ---------------------------------------------------------------------

    @property
    def queued(self) -> int:
        with self._lock:
            return len(self._queue)

    @property
    def backoff(self) -> float:
        return self._backoff

    # -- export --------------------------------------------------------------------

    def _get_exporter(self) -> SpanExporter:
        if self._exporter is None:
            assert self._factory is not None
            self._exporter = self._factory()
        return self._exporter

    def export_once(self) -> bool:
        """Export one batch now (ignores backoff); True on success or empty queue."""
        with self._export_lock:
            with self._lock:
                n = min(self.max_batch, len(self._queue))
                batch = [self._queue.popleft() for _ in range(n)]
            ok = self._export_batch(batch) if batch else True
        self._maybe_warn_dropped()
        return ok

    def _export_batch(self, batch: list[ReadableSpan]) -> bool:
        """Export ``batch``; on failure put it back at the head and back off."""
        try:
            ok = self._get_exporter().export(batch) is SpanExportResult.SUCCESS
        except Exception:  # noqa: BLE001 - exporter errors must never escape
            ok = False
        if ok:
            with self._lock:
                self.exported += len(batch)
            self._backoff = 0.0
            self._failing = False
        else:
            self._requeue(batch)
            self._on_failure()
        return ok

    def _requeue(self, batch: Sequence[ReadableSpan]) -> None:
        with self._lock:
            room = max(0, self.max_queue_size - len(self._queue))
            keep = batch[:room]
            self.dropped += len(batch) - len(keep)
            self._queue.extendleft(reversed(keep))

    def _on_failure(self) -> None:
        self._backoff = (
            self.BACKOFF_INITIAL if self._backoff <= 0 else min(self.BACKOFF_MAX, self._backoff * 2)
        )
        self._next_attempt = self._clock.monotonic() + self._backoff
        if not self._failing:
            self._failing = True
            log.warning("otlp_endpoint_unreachable", retry_in_s=self._backoff)

    def _maybe_warn_dropped(self) -> None:
        with self._lock:
            total = self.dropped
        new = total - self._dropped_reported
        if new <= 0:
            return
        now = self._clock.monotonic()
        if self._last_drop_warn is not None and now - self._last_drop_warn < self.DROP_WARN_INTERVAL:
            return
        self._last_drop_warn = now
        self._dropped_reported = total
        log.warning("spans dropped", dropped=new, dropped_total=total)

    def _run(self) -> None:
        while True:
            now = self._clock.monotonic()
            wait = self._next_attempt - now if self._backoff > 0 else self.schedule_delay
            self._wake.wait(max(0.0, wait))
            self._wake.clear()
            if self._stopped:
                return
            if self._backoff > 0 and self._clock.monotonic() < self._next_attempt:
                continue
            try:
                while self.export_once() and self.queued >= self.max_batch and not self._stopped:
                    pass
            except Exception:  # noqa: BLE001 - keep the export thread alive
                log.warning("span_export_error", exc_info=True)
            self._maybe_warn_dropped()

    # -- shutdown ------------------------------------------------------------------

    def shutdown(self, timeout: float | None = None) -> int:  # type: ignore[override]
        """Flush for ``min(5, timeout)`` seconds, then drop the rest (Req 8.8).

        Returns the number of Spans discarded at shutdown. Never raises.
        """
        budget = self.SHUTDOWN_FLUSH_MAX if timeout is None else min(self.SHUTDOWN_FLUSH_MAX, timeout)
        budget = max(0.0, budget)
        start = self._clock.monotonic()
        with self._lock:
            already = self._stopped
            self._stopped = True
        if already:
            return 0
        self._wake.set()
        if self._thread is not None:
            self._thread.join(budget)
        remaining = max(0.0, budget - (self._clock.monotonic() - start))
        # Only flush when no export is still in flight on the background thread.
        if remaining > 0 and self._export_lock.acquire(timeout=remaining):
            self._export_lock.release()
            deadline = start + budget
            while self.queued and self._clock.monotonic() < deadline:
                if not self.export_once():
                    break
        with self._lock:
            discarded = len(self._queue)
            self._queue.clear()
            self.dropped += discarded
        if discarded:
            log.warning("spans dropped at shutdown", dropped=discarded, dropped_total=self.dropped)
        exporter = self._exporter
        if exporter is not None:
            try:
                exporter.shutdown()
            except Exception:  # noqa: BLE001
                log.debug("span_exporter_shutdown_error", exc_info=True)
        return discarded


# --------------------------------------------------------------------------- factory


def _http_traces_endpoint(endpoint: str) -> str:
    parts = urlsplit(endpoint)
    if parts.path in ("", "/"):
        parts = parts._replace(path="/v1/traces")
    return urlunsplit(parts)


def make_exporter_factory(config: TracingConfig) -> Callable[[], SpanExporter]:
    """Lazy OTLP exporter constructor; imports the exporter package only when called."""
    endpoint = config.otlp_endpoint or ""
    headers = dict(config.header_pairs()) or None
    protocol = config.otlp_protocol

    def factory() -> SpanExporter:
        if protocol == "grpc":
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter as GrpcExporter,
            )

            return GrpcExporter(
                endpoint=endpoint,
                insecure=urlsplit(endpoint).scheme == "http",
                headers=headers,
            )
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as HttpExporter,
        )

        return HttpExporter(endpoint=_http_traces_endpoint(endpoint), headers=headers)

    return factory


def build_tracer(
    config: TracingConfig,
    role: str,
    agent_id: str,
    clock: Clock | None = None,
    *,
    exporter: SpanExporter | Callable[[], SpanExporter] | None = None,
    start_thread: bool = True,
) -> Tracer:
    """Build the agent's Tracer; ``exporter`` overrides the OTLP exporter (tests)."""
    if not config.enabled:
        return NoopTracer()
    processor = BoundedExportProcessor(
        exporter if exporter is not None else make_exporter_factory(config),
        max_queue_size=config.max_queue_size,
        clock=clock,
        start_thread=start_thread,
    )
    provider = TracerProvider(
        resource=Resource.create({"service.name": config.service_name or f"das-{role}"}),
        sampler=ParentBased(TraceIdRatioBased(config.sample_ratio)),
    )
    provider.add_span_processor(processor)
    return OtelTracer(
        provider,
        processor,
        default_attributes={"das.role": role, "das.agent_id": agent_id},
    )
