"""Unit tests for the Manager ingest loops (task 14.2; Req 6.6, 9.5, 13.5, 13.6)."""

from __future__ import annotations

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client.parser import text_string_to_metric_families
from structlog.testing import capture_logs

from das.clock import FakeClock
from das.codec import MessageCodec, new_message
from das.codec.envelope import TraceContextModel
from das.config.schema import TracingConfig
from das.manager.ingest import (
    DEAD_LETTER_REASON_HEADER,
    LLM_MODEL_HEADER,
    LLM_PROVIDER_HEADER,
    ORIGINAL_DESTINATION_HEADER,
    DeadLetterIngest,
    DeadLetterRecorder,
    ResultIngest,
    llm_meta,
)
from das.models import JobDispatchPayload, JobResultPayload, TokenUsage
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.observability.tracing import OtelTracer, build_tracer
from das.ops.dlq import dead_letter_id, dead_letter_identity
from das.state import INBOX_JOB_RESULT, INBOX_UNACKED_EXHAUSTED, StateStore
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport


def result(rid: str) -> JobResultPayload:
    return JobResultPayload(
        request_id=rid, job_id="a", attempt=1, worker_id="w1", status="Succeeded",
        content="x", usage=TokenUsage(), duration_ms=1,
    )


async def setup(store: StateStore):
    broker = MemoryBroker()
    manager = MemoryTransport(broker, agent_id="m1")
    sender = MemoryTransport(broker, agent_id="w1")
    await manager.start()
    await sender.start()
    woken: list[str] = []
    await ResultIngest(manager, store, on_ingested=woken.append).start()
    await DeadLetterIngest(manager, store, on_ingested=woken.append).start()
    return broker, sender, woken


async def test_result_is_appended_to_inbox_then_acked(state_store: StateStore):
    rid = await state_store.create_request("p")
    broker, sender, woken = await setup(state_store)
    await sender.publish("results", new_message(result(rid), correlation_id=rid))
    await broker.settle()
    [entry] = await state_store.read_inbox(rid, after="0-0")
    assert entry.kind == INBOX_JOB_RESULT and entry.job_result() == result(rid)
    assert broker.unacked_count("results") == 0 and broker.pending("results") == []
    assert woken == [rid]


async def test_unparseable_result_is_rejected_without_side_effects(state_store: StateStore):
    rid = await state_store.create_request("p")
    broker, _sender, woken = await setup(state_store)
    broker.publish_raw("results", b"{not json")
    await broker.settle()
    assert broker.pending("dlq") == [b"{not json"]  # REJECT -> dead letter (Req 9.5)
    assert await state_store.read_inbox(rid, after="0-0") == []
    assert woken == []


async def test_missing_correlation_id_warns_and_still_ingests(state_store: StateStore):
    rid = await state_store.create_request("p")
    broker, sender, _ = await setup(state_store)
    with capture_logs() as logs:
        await sender.publish("results", new_message(result(rid), correlation_id=""))
        await broker.settle()
    warnings = [e for e in logs if e["event"] == "message_missing_correlation_id"]
    assert warnings and warnings[0]["log_level"] == "warning"
    assert warnings[0]["message_type"] == "job_result" and warnings[0]["sender"] == "w1"
    assert len(await state_store.read_inbox(rid, after="0-0")) == 1


def dispatch_body(rid: str) -> bytes:
    payload = JobDispatchPayload(
        request_id=rid, job_id="a", attempt=2, instruction="i", required_capabilities=["search"]
    )
    return MessageCodec().encode(new_message(payload, correlation_id=rid))


async def test_delivery_limit_dead_letter_becomes_inbox_event(state_store: StateStore):
    rid = await state_store.create_request("p")
    broker, _sender, woken = await setup(state_store)
    broker.publish_raw(
        "dlq-events",
        dispatch_body(rid),
        {DEAD_LETTER_REASON_HEADER: "delivery_limit", ORIGINAL_DESTINATION_HEADER: "jobs/search"},
    )
    await broker.settle()
    [entry] = await state_store.read_inbox(rid, after="0-0")
    assert entry.kind == INBOX_UNACKED_EXHAUSTED
    assert entry.payload() == {"job_id": "a", "attempt": 2}
    assert woken == [rid]
    assert broker.pending("dlq-events") == []


async def test_other_dead_letters_are_ignored(state_store: StateStore):
    rid = await state_store.create_request("p")
    broker, _sender, woken = await setup(state_store)
    broker.publish_raw(
        "dlq-events",
        dispatch_body(rid),
        {DEAD_LETTER_REASON_HEADER: "max_attempts", ORIGINAL_DESTINATION_HEADER: "jobs/search"},
    )
    broker.publish_raw("dlq-events", b"garbage", {DEAD_LETTER_REASON_HEADER: "rejected"})
    await broker.settle()
    assert await state_store.read_inbox(rid, after="0-0") == []
    assert woken == [] and broker.pending("dlq-events") == []


# --------------------------------------------------------------------------- Phase 2


def test_llm_meta_defaults_missing_headers_to_unknown():
    assert llm_meta({}) == {"provider": "unknown", "model": "unknown"}
    assert llm_meta({LLM_PROVIDER_HEADER: "openai", LLM_MODEL_HEADER: ""}) == {
        "provider": "openai", "model": "unknown",
    }


async def test_result_headers_become_inbox_meta(state_store: StateStore):
    rid = await state_store.create_request("p")
    broker, sender, _ = await setup(state_store)
    await sender.publish(
        "results",
        new_message(result(rid), correlation_id=rid),
        {LLM_PROVIDER_HEADER: "anthropic", LLM_MODEL_HEADER: "claude"},
    )
    await sender.publish("results", new_message(result(rid), correlation_id=rid))
    await broker.settle()
    first, second = await state_store.read_inbox(rid, after="0-0")
    assert first.meta_payload() == {"provider": "anthropic", "model": "claude"}
    assert second.meta_payload() == {"provider": "unknown", "model": "unknown"}


PARENT = TraceContextModel(
    traceparent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
)


async def test_ingest_result_span_is_child_of_envelope_context(state_store: StateStore):
    mem = InMemorySpanExporter()
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317")
    tracer = build_tracer(cfg, "manager", "m1", FakeClock(), exporter=mem, start_thread=False)
    assert isinstance(tracer, OtelTracer)
    rid = await state_store.create_request("p")
    broker = MemoryBroker()
    manager = MemoryTransport(broker, agent_id="m1")
    sender = MemoryTransport(broker, agent_id="w1")
    await manager.start()
    await sender.start()
    await ResultIngest(manager, state_store, tracer=tracer).start()
    await sender.publish("results", new_message(result(rid), correlation_id=rid,
                                                trace_context=PARENT))
    await sender.publish("results", new_message(result(rid), correlation_id=rid))
    await broker.settle()
    while tracer.processor.queued:
        assert tracer.processor.export_once()
    child, root = [s for s in mem.get_finished_spans() if s.name == "manager.ingest_result"]
    assert format(child.context.trace_id, "032x") == "0af7651916cd43dd8448eb211c80319c"
    assert child.parent is not None and format(child.parent.span_id, "016x") == (
        "b7ad6b7169203331"
    )
    assert child.attributes["das.request_id"] == rid and child.attributes["das.job_id"] == "a"
    assert root.parent is None  # no context on the envelope: new trace (Req 6.11)


def _created(recorder: MetricsRecorder, reason: str) -> float:
    for family in text_string_to_metric_families(recorder.render()[0].decode()):
        for sample in family.samples:
            if (sample.name == "das_dlq_records_created_total"
                    and sample.labels.get("reason") == reason):
                return sample.value
    return 0.0


async def dlq_setup(store, metrics: ManagerMetrics, managers: int = 1):
    broker = MemoryBroker()
    for i in range(managers):
        t = MemoryTransport(broker, agent_id=f"m{i}")
        await t.start()
        await DeadLetterRecorder(t, store, metrics, max_message_bytes=1024).start()
    return broker


async def test_dead_letter_recorder_creates_record_once(state_store: StateStore):
    rid = await state_store.create_request("p")
    recorder = MetricsRecorder(clock=FakeClock())
    broker = await dlq_setup(state_store, ManagerMetrics(recorder), managers=2)
    body = dispatch_body(rid)
    headers = {DEAD_LETTER_REASON_HEADER: "max_attempts", "x-last-error": "boom"}
    with capture_logs() as logs:
        broker.publish_raw("dlq", body, headers)
        broker.publish_raw("dlq", body, headers)  # redelivered copy, same message_id
        await broker.settle()
    msg = MessageCodec().decode(body)
    dlid = dead_letter_id(dead_letter_identity(msg, body))
    record = await state_store.get_dead_letter(dlid)
    assert record is not None
    assert record.reason == "retries_exhausted" and record.request_id == rid
    assert record.job_id == "a" and record.attempt == 2 and record.last_error == "boom"
    assert broker.pending("dlq") == [] and broker.unacked_count("dlq") == 0
    assert _created(recorder, "retries_exhausted") == 1
    # Req 12.10: never payload, raw bytes or last error in logs.
    for entry in logs:
        assert "boom" not in repr(entry) and "raw_b64" not in entry
    [created] = [e for e in logs if e["event"] == "dead_letter_recorded"]
    assert created["dead_letter_id"] == dlid and created["request_id"] == rid


async def test_dead_letter_recorder_keeps_malformed_raw_bytes(state_store: StateStore):
    recorder = MetricsRecorder(clock=FakeClock())
    broker = await dlq_setup(state_store, ManagerMetrics(recorder))
    body = b"{" + b"x" * 2000
    broker.publish_raw("dlq", body, {DEAD_LETTER_REASON_HEADER: "rejected"})
    await broker.settle()
    record = await state_store.get_dead_letter(dead_letter_id(dead_letter_identity(None, body)))
    assert record is not None and record.reason == "malformed"
    assert record.request_id is None and record.job_id is None
    assert _created(recorder, "malformed") == 1


class _FailOnce:
    """Delegates to the real store; the first ``create_dead_letter`` raises."""

    def __init__(self, store: StateStore) -> None:
        self._store = store
        self.calls = 0

    async def create_dead_letter(self, fields):
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("redis down")
        return await self._store.create_dead_letter(fields)


async def test_dead_letter_recorder_requeues_when_store_fails(state_store: StateStore):
    rid = await state_store.create_request("p")
    store = _FailOnce(state_store)
    recorder = MetricsRecorder(clock=FakeClock())
    broker = await dlq_setup(store, ManagerMetrics(recorder))
    body = dispatch_body(rid)
    with capture_logs() as logs:
        broker.publish_raw("dlq", body, {DEAD_LETTER_REASON_HEADER: "delivery_limit"})
        await broker.settle()
    [warning] = [e for e in logs if e["event"] == "dead_letter_record_failed"]
    assert warning["log_level"] == "warning" and warning["reason"] == "delivery_limit"
    assert warning["message_id"] == MessageCodec().decode(body).message_id
    # Redelivered after REQUEUE and recorded on the second attempt (Req 12.11).
    assert store.calls == 2 and broker.pending("dlq") == []
    assert _created(recorder, "delivery_limit") == 1
