"""Unit tests for the Message envelope and Message_Codec (Requirement 9, 13.4)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest
from hypothesis import given

from das.codec import (
    ENVELOPE_FIELDS,
    ControlMessage,
    HeartbeatMessage,
    JobDispatchMessage,
    JobResultMessage,
    MessageCodec,
    MessageTooLarge,
    ParseError,
    new_message,
)
from das.models.results import (
    ControlPayload,
    HeartbeatPayload,
    JobDispatchPayload,
    JobResultPayload,
    TokenUsage,
)
from das.observability.context import bind_request
from tests.strategies import messages

TS = datetime(2025, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)


def _dispatch(**overrides: object) -> JobDispatchMessage:
    payload = JobDispatchPayload(
        request_id="req-1",
        job_id="j1",
        attempt=1,
        instruction="Résumé du travail ✓",
        required_capabilities=["research"],
        dependency_results={"j0": "dep output"},
    )
    fields: dict[str, object] = {
        "message_id": "m-1",
        "correlation_id": "req-1",
        "timestamp": TS,
        "payload": payload,
    }
    fields.update(overrides)
    return JobDispatchMessage(**fields)  # type: ignore[arg-type]


def _result() -> JobResultMessage:
    return JobResultMessage(
        message_id="m-2",
        correlation_id="req-1",
        timestamp=TS,
        payload=JobResultPayload(
            request_id="req-1",
            job_id="j1",
            attempt=2,
            worker_id="worker-a",
            status="Failed",
            content="",
            error="boom",
            usage=TokenUsage(input_tokens=3, output_tokens=4, total_tokens=7),
            duration_ms=12,
        ),
    )


def _envelope_dict(message: object, codec: MessageCodec) -> dict:
    return json.loads(codec.encode(message))  # type: ignore[arg-type]


def _raw(obj: object) -> bytes:
    return json.dumps(obj).encode()


# --------------------------------------------------------------------------- envelope


def test_timestamp_is_truncated_to_milliseconds_and_utc() -> None:
    plus2 = timezone(timedelta(hours=2))
    msg = _dispatch(timestamp=datetime(2025, 1, 1, 2, 0, 0, 999999, tzinfo=plus2))
    assert msg.timestamp == datetime(2025, 1, 1, 0, 0, 0, 999000, tzinfo=UTC)


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError):
        _dispatch(timestamp=datetime(2025, 1, 1))


def test_message_id_length_is_bounded() -> None:
    _dispatch(message_id="x" * 64)
    with pytest.raises(ValueError):
        _dispatch(message_id="x" * 65)
    with pytest.raises(ValueError):
        _dispatch(message_id="")


def test_new_message_infers_type_and_copies_bound_request_id() -> None:
    payload = ControlPayload(command="cancel_job", request_id="req-9", job_id="j1")
    with bind_request("req-9"):
        msg = new_message(payload)
    assert isinstance(msg, ControlMessage)
    assert msg.correlation_id == "req-9"
    assert msg.schema_version == 1
    assert 1 <= len(msg.message_id) <= 64
    assert msg.timestamp.tzinfo is not None and msg.timestamp.microsecond % 1000 == 0


def test_new_message_without_binding_uses_empty_correlation_id() -> None:
    msg = new_message(HeartbeatPayload(agent_id="w1", role="worker", capabilities=["a"]))
    assert isinstance(msg, HeartbeatMessage)
    assert msg.correlation_id == ""


def test_new_message_explicit_correlation_id_wins() -> None:
    payload = ControlPayload(command="cancel_request", request_id="req-2")
    with bind_request("req-other"):
        msg = new_message(payload, correlation_id="req-2")
    assert msg.correlation_id == "req-2"


def test_new_message_ids_are_unique() -> None:
    payload = HeartbeatPayload(agent_id="w1", role="worker")
    assert len({new_message(payload).message_id for _ in range(100)}) == 100


# --------------------------------------------------------------------------- encode


def test_encode_is_canonical_json_with_exact_envelope_fields() -> None:
    codec = MessageCodec()
    data = codec.encode(_dispatch())
    obj = json.loads(data)
    assert set(obj) == ENVELOPE_FIELDS
    assert obj["timestamp"] == "2025-01-01T00:00:00.123Z"
    assert obj["type"] == "job_dispatch"
    # sorted keys, compact separators, raw (non-escaped) Unicode
    assert data == json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    assert "Résumé du travail ✓".encode() in data


def test_round_trip_examples() -> None:
    codec = MessageCodec()
    for msg in (_dispatch(), _result()):
        data = codec.encode(msg)
        decoded = codec.decode(data)
        assert decoded == msg
        assert type(decoded) is type(msg)
        assert codec.encode(decoded) == data


@given(messages())
def test_round_trip_generated(msg: object) -> None:
    codec = MessageCodec()
    data = codec.encode(msg)  # type: ignore[arg-type]
    assert codec.decode(data) == msg


def test_encode_rejects_oversized_message() -> None:
    msg = _dispatch()
    size = len(MessageCodec().encode(msg))
    assert len(MessageCodec(max_bytes=size).encode(msg)) == size
    with pytest.raises(MessageTooLarge) as info:
        MessageCodec(max_bytes=size - 1).encode(msg)
    assert info.value.size == size and info.value.limit == size - 1


# --------------------------------------------------------------------------- decode errors


def test_decode_rejects_oversized_input_before_parsing() -> None:
    with pytest.raises(ParseError) as info:
        MessageCodec(max_bytes=10).decode(b"x" * 11)
    assert "size" in info.value.reason


@pytest.mark.parametrize("data", [b"\xff\xfe", b"not json", b"[1,2]", b'{"a":NaN}', b""])
def test_decode_rejects_unparseable_input(data: bytes) -> None:
    with pytest.raises(ParseError) as info:
        MessageCodec().decode(data)
    assert info.value.field == "$"


@pytest.mark.parametrize("field", sorted(ENVELOPE_FIELDS))
def test_decode_reports_missing_envelope_field(field: str) -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    del obj[field]
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == field


def test_decode_rejects_unknown_envelope_field() -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["extra"] = 1
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == "extra"


def test_decode_rejects_unknown_type() -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["type"] = "bogus"
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == "type"


@pytest.mark.parametrize("version", [3, 0, -1, True, "1", 1.0])
def test_decode_rejects_bad_schema_version(version: object) -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["schema_version"] = version
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == "schema_version"


@pytest.mark.parametrize(
    ("field", "value"),
    [("message_id", ""), ("message_id", "x" * 65), ("correlation_id", None),
     ("timestamp", "yesterday"), ("timestamp", "2025-01-01T00:00:00"), ("payload", [])],
)
def test_decode_rejects_bad_envelope_values(field: str, value: object) -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj[field] = value
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == field


@pytest.mark.parametrize(
    ("mutate", "field"),
    [
        (lambda p: p.update(attempt="1"), "payload.attempt"),
        (lambda p: p.pop("job_id"), "payload.job_id"),
        (lambda p: p.update(instruction=""), "payload.instruction"),
        (lambda p: p.update(unexpected=True), "payload.unexpected"),
        (lambda p: p.update(dependency_results={"j0": 5}), "payload.dependency_results.j0"),
    ],
)
def test_decode_rejects_payload_schema_mismatch(mutate, field: str) -> None:  # type: ignore[no-untyped-def]
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    mutate(obj["payload"])
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == field


def test_decode_rejects_nested_payload_type_error() -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_result(), codec)
    obj["payload"]["usage"]["input_tokens"] = "3"
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == "payload.usage.input_tokens"


def test_decode_rejects_payload_of_other_type() -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_result(), codec)
    obj["type"] = "job_dispatch"
    with pytest.raises(ParseError):
        codec.decode(_raw(obj))


def test_codec_requires_positive_limit() -> None:
    with pytest.raises(ValueError):
        MessageCodec(max_bytes=0)


# ---------------------------------------------------------------- Envelope_V2 (Phase 2 Req 7)
from das.codec import (  # noqa: E402
    AgentControlMessage,
    AgentControlPayload,
    AgentControlReplyMessage,
    AgentControlReplyPayload,
    TraceContextModel,
)
from tests.fixtures.v1_codec import V1ReferenceCodec  # noqa: E402

TP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def test_message_without_trace_context_encodes_v1_bytes() -> None:
    msg = _dispatch()
    data = MessageCodec().encode(msg)
    assert data == V1ReferenceCodec().encode(msg)
    obj = json.loads(data)
    assert obj["schema_version"] == 1 and "trace_context" not in obj


@pytest.mark.parametrize("tracestate", [None, "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7"])
def test_trace_context_round_trip_v2(tracestate: str | None) -> None:
    codec = MessageCodec()
    msg = _dispatch(trace_context=TraceContextModel(traceparent=TP, tracestate=tracestate))
    assert msg.schema_version == 2
    obj = _envelope_dict(msg, codec)
    assert obj["schema_version"] == 2
    expected = {"traceparent": TP} if tracestate is None else {"traceparent": TP, "tracestate": tracestate}
    assert obj["trace_context"] == expected
    decoded = codec.decode(codec.encode(msg))
    assert decoded == msg
    assert decoded.trace_context == msg.trace_context


def test_schema_version_is_normalized() -> None:
    assert _dispatch(schema_version=2).schema_version == 1
    msg = new_message(_dispatch().payload, trace_context=TraceContextModel(traceparent=TP))
    assert msg.schema_version == 2 and msg.trace_context is not None
    assert new_message(_dispatch().payload).trace_context is None


def test_trace_context_model_rejects_invalid_values() -> None:
    with pytest.raises(ValueError):
        TraceContextModel(traceparent="00-" + "0" * 32 + "-b7ad6b7169203331-01")
    with pytest.raises(ValueError):
        TraceContextModel(traceparent=TP, tracestate="BAD KEY=1")


def test_v1_bytes_with_trace_context_field_are_rejected() -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["trace_context"] = {"traceparent": TP}
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == "trace_context"


def test_v2_without_trace_context_decodes_without_context() -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["schema_version"] = 2
    msg = codec.decode(_raw(obj))
    assert msg.trace_context is None and msg == _dispatch()


@pytest.mark.parametrize(
    "raw",
    [None, "x", 7, [], {}, {"traceparent": 1}, {"traceparent": TP.upper()},
     {"traceparent": "ff" + TP[2:]}, {"traceparent": TP + "-x"}],
)
def test_v2_malformed_trace_context_is_dropped(raw: object) -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["schema_version"] = 2
    obj["trace_context"] = raw
    msg = codec.decode(_raw(obj))
    assert msg.trace_context is None and msg == _dispatch()


@pytest.mark.parametrize("tracestate", [5, "", "a=" + "x" * 600, ",".join(f"k{i}=v" for i in range(33))])
def test_v2_invalid_tracestate_keeps_traceparent(tracestate: object) -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["schema_version"] = 2
    obj["trace_context"] = {"traceparent": TP, "tracestate": tracestate}
    msg = codec.decode(_raw(obj))
    assert msg.trace_context == TraceContextModel(traceparent=TP)


def test_unsupported_version_checked_before_field_set() -> None:
    codec = MessageCodec()
    obj = _envelope_dict(_dispatch(), codec)
    obj["schema_version"] = 3
    obj["trace_context"] = {"traceparent": TP}
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == "schema_version"


def test_agent_control_messages_round_trip() -> None:
    codec = MessageCodec()
    ctl = new_message(
        AgentControlPayload(command="drain", agent_id="worker-a", release_leases=True),
        correlation_id="",
    )
    assert isinstance(ctl, AgentControlMessage)
    reply = new_message(
        AgentControlReplyPayload(agent_id="worker-a", status="Draining", in_flight=2),
        correlation_id="",
        trace_context=TraceContextModel(traceparent=TP),
    )
    assert isinstance(reply, AgentControlReplyMessage)
    for msg in (ctl, reply):
        assert codec.decode(codec.encode(msg)) == msg
    with pytest.raises(ValueError):
        AgentControlReplyPayload(agent_id="a", status="Draining", in_flight=-1)
    obj = _envelope_dict(ctl, codec)
    obj["payload"]["release_leases"] = "yes"
    with pytest.raises(ParseError) as info:
        codec.decode(_raw(obj))
    assert info.value.field == "payload.release_leases"
