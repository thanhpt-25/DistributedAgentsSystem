"""RequestDriver metrics and Spans (das-observability-operations task 10.4;
Req 2.2, 2.4, 2.5, 2.7, 6.2, 6.3, 6.4, 6.8, 6.10, 6.12).

Same real pieces as ``test_manager_driver`` (fakeredis + Lua, MemoryTransport,
Planner/Summarizer over FakeAdapter), plus a real ``MetricsRecorder`` and an
``OtelTracer`` exporting into an ``InMemorySpanExporter``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from das.clock import FakeClock
from das.config.schema import TracingConfig
from das.manager.driver import DriverDeps, DriverOutcome, RequestDrivers
from das.models import JobState, RequestStatus
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.observability.trace_context import TraceContext
from das.observability.tracing import OtelTracer, build_tracer
from das.state import ClientRequestSpec, StateStore, WriteResult, keys
from das.transport.base import Delivery
from tests.fakes.memory_transport import MemoryTransport
from tests.unit.test_manager_driver import (
    ONE_JOB,
    TIMEOUT,
    _status_of,
    _true,
    make_env,
    steal_lease,
    wait_until,
)

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
GATEWAY_CTX = TraceContext.parse(f"00-{TRACE_ID}-b7ad6b7169203331-01")


class _Log:
    def warning(self, *_a, **_k) -> None: ...


def _recorder() -> MetricsRecorder:
    return MetricsRecorder(clock=FakeClock(), log=_Log())


def _tracer(exporter: InMemorySpanExporter) -> OtelTracer:
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317")
    tracer = build_tracer(cfg, "manager", "m1", FakeClock(), exporter=exporter,
                          start_thread=False)
    assert isinstance(tracer, OtelTracer)
    return tracer


def _spans(tracer: OtelTracer, exporter: InMemorySpanExporter) -> list[ReadableSpan]:
    while tracer.processor.queued:
        assert tracer.processor.export_once()
    return list(exporter.get_finished_spans())


def _wire(rec: MetricsRecorder | None = None, tracer: OtelTracer | None = None):
    def tweak(deps: DriverDeps) -> DriverDeps:
        changes: dict = {}
        if rec is not None:
            changes["metrics"] = ManagerMetrics(rec)
        if tracer is not None:
            changes["tracer"] = tracer
        return dataclasses.replace(deps, **changes)

    return tweak


def _value(rec: MetricsRecorder, name: str, labels: dict[str, str] | None = None) -> float:
    return rec.registry.get_sample_value(name, labels or {}) or 0.0


def _transitions_total(rec: MetricsRecorder) -> float:
    total = 0.0
    for family in rec.registry.collect():
        if family.name == "das_job_transitions":
            total += sum(s.value for s in family.samples if s.name.endswith("_total"))
    return total


# --------------------------------------------------------------------------- metrics


async def test_completed_request_records_finish_transitions_and_attempts(
    state_store: StateStore, fake_redis
):
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED

    completed = {"status": "Completed"}
    assert _value(rec, "das_requests_finished_total", {**completed, "tenant": "default"}) == 1  # Req 2.2
    assert _value(rec, "das_request_duration_seconds_count", completed) == 1  # per status only (Phase 5 Req 8.1)
    assert _value(rec, "das_requests_finished_total", {"status": "Failed", "tenant": "default"}) == 0

    for cap in ("search", "write"):  # Req 2.4: one per transition entry, routing cap
        for state in (JobState.WAITING, JobState.READY, JobState.DISPATCHED, JobState.SUCCEEDED):
            labels = {"capability": cap, "state": state.value}
            assert _value(rec, "das_job_transitions_total", labels) == 1, labels
        assert _value(rec, "das_job_attempts_count", {"capability": cap}) == 1  # Req 2.7
        assert _value(rec, "das_job_attempts_sum", {"capability": cap}) == 1

    stored = await state_store.load_snapshot(rid, include_transitions=True)
    assert stored is not None
    job_entries = [t for t in stored.transitions or [] if t.job_id]
    assert _transitions_total(rec) == len(job_entries)  # Req 2.6 (single Manager)


async def test_negative_duration_is_clamped_to_zero(state_store: StateStore, fake_redis):
    rec = _recorder()
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,), tweak=_wire(rec))
    real_finalize = state_store.finalize

    async def skewed_finalize(*args, **kwargs) -> WriteResult:
        result = await real_finalize(*args, **kwargs)
        assert result.ok and result.finished_at_ms is not None
        return dataclasses.replace(result, created_at_ms=result.finished_at_ms + 5_000)

    state_store.finalize = skewed_finalize  # type: ignore[method-assign]
    _, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    completed = {"status": "Completed"}
    assert _value(rec, "das_request_duration_seconds_count", completed) == 1
    assert _value(rec, "das_request_duration_seconds_sum", completed) == 0.0  # Req 2.2


async def test_rejected_writes_record_nothing(state_store: StateStore, fake_redis):
    rec = _recorder()
    env = await make_env(state_store, fake_redis, lease_duration=1.0, tweak=_wire(rec))
    env.worker.mode = "hold"
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))
    before = _transitions_total(rec)
    await steal_lease(fake_redis, state_store, rid)
    env.drivers.notify(rid)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST

    assert _transitions_total(rec) == before  # Req 2.5
    for status in ("Completed", "Failed"):
        assert _value(rec, "das_requests_finished_total", {"status": status, "tenant": "default"}) == 0
    stored = await state_store.load_snapshot(rid, include_transitions=True)
    assert stored is not None
    assert _transitions_total(rec) <= len([t for t in stored.transitions or [] if t.job_id])


async def test_failed_request_counts_failed_and_attempts(state_store: StateStore, fake_redis):
    rec = _recorder()
    from das.manager.machine import MachineConfig

    env = await make_env(
        state_store, fake_redis, machine_config=MachineConfig(max_attempts=1), tweak=_wire(rec)
    )
    env.worker.status = "Failed"
    _, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    assert _value(rec, "das_requests_finished_total", {"status": "Failed", "tenant": "default"}) == 1
    # "a" Failed after 1 attempt; "b" Skipped before any dispatch: attempts 0 (Req 2.7).
    assert _value(rec, "das_job_attempts_sum", {"capability": "search"}) == 1
    assert _value(rec, "das_job_attempts_count", {"capability": "write"}) == 1
    assert _value(rec, "das_job_attempts_sum", {"capability": "write"}) == 0
    assert _value(rec, "das_job_transitions_total",
                  {"capability": "write", "state": JobState.SKIPPED.value}) == 1


# --------------------------------------------------------------------------- spans


async def test_spans_share_persisted_trace_and_dispatch_carries_its_context(
    state_store: StateStore, fake_redis
):
    exporter = InMemorySpanExporter()
    tracer = _tracer(exporter)
    env = await make_env(state_store, fake_redis, tweak=_wire(tracer=tracer))
    rid = await state_store.create_request("hello", trace_context=GATEWAY_CTX)
    acquired = await state_store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.lease is not None
    task = env.drivers.on_claimed(acquired.lease)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED

    spans = _spans(tracer, exporter)
    by_name: dict[str, list[ReadableSpan]] = {}
    for s in spans:
        by_name.setdefault(s.name, []).append(s)
    [request] = by_name["manager.request"]
    assert f"{request.context.trace_id:032x}" == TRACE_ID  # Req 6.2
    assert request.parent is not None and request.parent.span_id == int("b7ad6b7169203331", 16)
    assert request.status.status_code is not StatusCode.ERROR
    assert "error.type" not in (request.attributes or {})  # Req 6.10
    assert len(by_name["manager.plan"]) == 1 and len(by_name["manager.summarize"]) == 1
    dispatches = by_name["manager.dispatch"]
    assert len(dispatches) == 2  # Req 6.3
    for child in (*by_name["manager.plan"], *by_name["manager.summarize"], *dispatches):
        assert child.parent is not None and child.parent.span_id == request.context.span_id
        assert child.context.trace_id == request.context.trace_id

    # Req 6.4: each envelope carries the context of its own manager.dispatch Span.
    span_by_job = {s.attributes["das.job_id"]: s for s in dispatches}
    for msg in env.worker.dispatches:
        assert msg.trace_context is not None
        span = span_by_job[msg.payload.job_id]
        assert msg.trace_context.traceparent == (
            f"00-{span.context.trace_id:032x}-{span.context.span_id:016x}-01"
        )
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.trace_context == GATEWAY_CTX


async def test_request_without_context_is_new_root_and_not_written_back(
    state_store: StateStore, fake_redis
):
    exporter = InMemorySpanExporter()
    tracer = _tracer(exporter)
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,),
                         tweak=_wire(tracer=tracer))
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    [request] = [s for s in _spans(tracer, exporter) if s.name == "manager.request"]
    assert request.parent is None  # Req 6.12
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.trace_context is None
    assert stored.status is RequestStatus.COMPLETED


async def test_rejected_plan_marks_spans_failed(state_store: StateStore, fake_redis):
    exporter = InMemorySpanExporter()
    tracer = _tracer(exporter)
    bad = json.dumps({"jobs": [{"job_id": "a", "instruction": "x", "capabilities": ["paint"]}]})
    env = await make_env(state_store, fake_redis, plan_outcomes=(bad,),
                         tweak=_wire(tracer=tracer))
    _, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    spans = {s.name: s for s in _spans(tracer, exporter)}
    for name in ("manager.request", "manager.plan"):
        assert spans[name].status.status_code is StatusCode.ERROR
        assert spans[name].attributes["error.type"] == "PlanRejected"  # Req 6.10


async def test_lease_loss_marks_request_span_lease_lost(state_store: StateStore, fake_redis):
    exporter = InMemorySpanExporter()
    tracer = _tracer(exporter)
    env = await make_env(state_store, fake_redis, lease_duration=1.0, tweak=_wire(tracer=tracer))
    env.worker.mode = "hold"
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))
    await steal_lease(fake_redis, state_store, rid)
    env.drivers.notify(rid)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
    [request] = [s for s in _spans(tracer, exporter) if s.name == "manager.request"]
    assert request.status.status_code is StatusCode.ERROR
    assert request.attributes["error.type"] == "LEASE_LOST"  # Req 6.10


async def test_cancel_job_carries_active_trace_context(state_store: StateStore, fake_redis):
    exporter = InMemorySpanExporter()
    tracer = _tracer(exporter)
    env = await make_env(state_store, fake_redis, tweak=_wire(tracer=tracer))
    env.worker.mode = "hold"
    worker_control = MemoryTransport(env.control_broker, agent_id="w1")
    await worker_control.start()
    received: list = []

    async def on_control(delivery: Delivery) -> None:
        received.append(delivery.message)
        await worker_control.acknowledge(delivery)

    await worker_control.subscribe("w1", on_control)
    rid = await state_store.create_request("hello", trace_context=GATEWAY_CTX)
    acquired = await state_store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.lease is not None
    task = env.drivers.on_claimed(acquired.lease)
    await wait_until(lambda: _true(len(env.worker.dispatches) == 1))
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))
    assert (await state_store.cancel_request(rid)).ok
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.CANCELLED
    await env.control_broker.settle()

    [request] = [s for s in _spans(tracer, exporter) if s.name == "manager.request"]
    [cancel] = received
    assert cancel.payload.command == "cancel_job"
    assert cancel.trace_context is not None  # Req 6.8: the active manager.request Span
    assert cancel.trace_context.traceparent == (
        f"00-{TRACE_ID}-{request.context.span_id:016x}-01"
    )


# --------------------------------------------------------------------------- task 11.12


def _envelope_ctx(msg) -> TraceContext:
    assert msg.trace_context is not None
    ctx = TraceContext.parse(msg.trace_context.traceparent, msg.trace_context.tracestate)
    assert ctx is not None
    return ctx


async def test_finalize_lease_lost_records_no_finish_and_counts_loss(
    state_store: StateStore, fake_redis
):
    """``finalize`` answering LEASE_LOST changes no Req 2.2 metric (Req 2.5) and is a
    ``held_by_other`` Lease loss (Req 3.4)."""
    rec = _recorder()
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,), tweak=_wire(rec))
    real_finalize = state_store.finalize
    answers: list[WriteResult] = []

    async def finalize_after_takeover(lease, *args, **kwargs) -> WriteResult:
        await steal_lease(fake_redis, state_store, lease.request_id)
        result = await real_finalize(lease, *args, **kwargs)
        answers.append(result)
        return result

    state_store.finalize = finalize_after_takeover  # type: ignore[method-assign]
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
    assert [a.ok for a in answers] == [False]

    for status in ("Completed", "Failed"):
        assert _value(rec, "das_requests_finished_total", {"status": status, "tenant": "default"}) == 0
        assert _value(rec, "das_request_duration_seconds_count", {"status": status}) == 0
    assert _value(rec, "das_lease_losses_total", {"reason": "held_by_other"}) == 1  # Req 3.4
    assert _value(rec, "das_lease_losses_total", {"reason": "store_unreachable"}) == 0
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.RUNNING


async def test_cancelled_result_skips_dependent_with_zero_attempts(
    state_store: StateStore, fake_redis
):
    """Req 2.7: the Cancelled Job observes its attempt count (1); the dependent Job
    Skipped before any dispatch observes 0."""
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    env.worker.status = "Cancelled"
    _, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    assert _value(rec, "das_job_transitions_total",
                  {"capability": "search", "state": JobState.CANCELLED.value}) == 1
    assert _value(rec, "das_job_attempts_count", {"capability": "search"}) == 1
    assert _value(rec, "das_job_attempts_sum", {"capability": "search"}) == 1
    assert _value(rec, "das_job_transitions_total",
                  {"capability": "write", "state": JobState.SKIPPED.value}) == 1
    assert _value(rec, "das_job_attempts_count", {"capability": "write"}) == 1
    assert _value(rec, "das_job_attempts_sum", {"capability": "write"}) == 0
    assert _value(rec, "das_job_transitions_total",
                  {"capability": "write", "state": JobState.DISPATCHED.value}) == 0


async def test_request_cancel_by_gateway_records_no_manager_job_metrics(
    state_store: StateStore, fake_redis
):
    """Jobs Cancelled by the Gateway's ``cancel_request`` (b never dispatched) are not
    a Manager write: the Manager observes no attempts and no finish for them (Req 2.5,
    2.7); the Request's Cancelled finish belongs to the Gateway (Req 2.3)."""
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    env.worker.mode = "hold"
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))
    before = _transitions_total(rec)
    assert (await state_store.cancel_request(rid)).ok
    env.drivers.notify(rid)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.CANCELLED

    assert _transitions_total(rec) == before
    for cap in ("search", "write"):
        assert _value(rec, "das_job_attempts_count", {"capability": cap}) == 0
    for status in ("Completed", "Failed", "Cancelled"):
        assert _value(rec, "das_requests_finished_total", {"status": status, "tenant": "default"}) == 0


async def test_a_finished_request_is_counted_for_its_own_tenant(state_store: StateStore, fake_redis):
    """Phase 5 Req 8.1: the Manager takes the tenant from the Request it finished, not from anything it was started
    with, so a Request of ``team-a`` never lands on ``default``."""
    rec = _recorder()
    env = await make_env(state_store, fake_redis, tweak=_wire(rec))
    spec = ClientRequestSpec(fingerprint="cbabecbd", tenant="team-a")
    rid = (await state_store.submit_request("hello", spec)).request_id
    assert rid is not None
    acquired = await state_store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.ok and acquired.lease is not None
    assert await asyncio.wait_for(env.drivers.on_claimed(acquired.lease), TIMEOUT) is DriverOutcome.COMPLETED

    assert _value(rec, "das_requests_finished_total", {"status": "Completed", "tenant": "team-a"}) == 1
    assert _value(rec, "das_requests_finished_total", {"status": "Completed", "tenant": "default"}) == 0


async def test_span_chain_across_takeover_shares_gateway_trace(
    state_store: StateStore, fake_redis
):
    """Req 6.2: the ``manager.request`` Span of the first owner and of the Manager that
    takes over are both children of the persisted ``gateway.submit`` context, so every
    Manager Span of the Request carries the same trace ID."""
    exp1, exp2 = InMemorySpanExporter(), InMemorySpanExporter()
    tracer1, tracer2 = _tracer(exp1), _tracer(exp2)
    env = await make_env(state_store, fake_redis, lease_duration=1.0,
                         tweak=_wire(tracer=tracer1))
    env.worker.mode = "hold"
    rid = await state_store.create_request("hello", trace_context=GATEWAY_CTX)
    acquired = await state_store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.lease is not None
    task1 = env.drivers.on_claimed(acquired.lease)
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))

    # Takeover: m1's Lease expires and m2 acquires it.
    sec, _ = await fake_redis.time()
    await fake_redis.hset(keys.lease(rid), "expires_at", f"{int(sec) - 1}.000000")
    taken = await state_store.acquire_lease(rid, "m2", 30)
    assert taken.lease is not None and taken.lease.owner == "m2"
    env.drivers.notify(rid)
    assert await asyncio.wait_for(task1, TIMEOUT) is DriverOutcome.LEASE_LOST

    env.worker.mode = "reply"
    m2 = RequestDrivers(dataclasses.replace(env.drivers.deps, tracer=tracer2), 30.0)
    assert await asyncio.wait_for(m2.on_claimed(taken.lease), TIMEOUT) is (
        DriverOutcome.COMPLETED
    )

    spans1, spans2 = _spans(tracer1, exp1), _spans(tracer2, exp2)
    [req1] = [s for s in spans1 if s.name == "manager.request"]
    [req2] = [s for s in spans2 if s.name == "manager.request"]
    gateway_span_id = int(GATEWAY_CTX.parent_id, 16)
    for req in (req1, req2):
        assert f"{req.context.trace_id:032x}" == TRACE_ID
        assert req.parent is not None and req.parent.span_id == gateway_span_id
    assert req1.context.span_id != req2.context.span_id
    assert req1.attributes["error.type"] == "LEASE_LOST"
    assert req2.status.status_code is not StatusCode.ERROR
    for s in (*spans1, *spans2):
        assert f"{s.context.trace_id:032x}" == TRACE_ID, s.name
    assert any(s.name == "manager.dispatch" for s in spans2)
    assert any(s.name == "manager.summarize" for s in spans2)
    # Every dispatch envelope, before and after takeover, stays on the same trace.
    assert env.worker.dispatches
    for msg in env.worker.dispatches:
        assert _envelope_ctx(msg).trace_id == TRACE_ID
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.trace_context == GATEWAY_CTX
    assert stored.status is RequestStatus.COMPLETED


async def test_unsampled_persisted_context_propagates_with_sample_ratio_zero(
    state_store: StateStore, fake_redis
):
    """``sample_ratio=0``: a persisted context with ``sampled=0`` still parents the
    Manager's Spans; nothing is exported, envelopes carry the same trace ID with
    ``sampled=0``, and the persisted context is left untouched (Req 6.2, 6.4, 8.4)."""
    unsampled = TraceContext.parse(f"00-{TRACE_ID}-b7ad6b7169203331-00")
    assert unsampled is not None and not unsampled.sampled
    exporter = InMemorySpanExporter()
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317", sample_ratio=0.0)
    tracer = build_tracer(cfg, "manager", "m1", FakeClock(), exporter=exporter,
                          start_thread=False)
    assert isinstance(tracer, OtelTracer)
    env = await make_env(state_store, fake_redis, tweak=_wire(tracer=tracer))
    rid = await state_store.create_request("hello", trace_context=unsampled)
    acquired = await state_store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.lease is not None
    assert await asyncio.wait_for(env.drivers.on_claimed(acquired.lease), TIMEOUT) is (
        DriverOutcome.COMPLETED
    )

    assert _spans(tracer, exporter) == []
    assert len(env.worker.dispatches) == 2
    for msg in env.worker.dispatches:
        ctx = _envelope_ctx(msg)  # propagated even though not sampled
        assert ctx.trace_id == TRACE_ID and not ctx.sampled
        assert ctx.parent_id != unsampled.parent_id  # the manager.dispatch Span's ID
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.trace_context == unsampled
    assert stored.status is RequestStatus.COMPLETED


async def test_v1_request_with_sample_ratio_zero_is_unsampled_root_not_persisted(
    state_store: StateStore, fake_redis
):
    """Req 6.12: a Request without context gets a new (here unsampled) root; no
    Trace_Context is written to the record and the Request completes normally."""
    exporter = InMemorySpanExporter()
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317", sample_ratio=0.0)
    tracer = build_tracer(cfg, "manager", "m1", FakeClock(), exporter=exporter,
                          start_thread=False)
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,),
                         tweak=_wire(tracer=tracer))  # type: ignore[arg-type]
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    assert isinstance(tracer, OtelTracer) and _spans(tracer, exporter) == []
    [msg] = env.worker.dispatches
    assert not _envelope_ctx(msg).sampled
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.trace_context is None
    assert stored.status is RequestStatus.COMPLETED
