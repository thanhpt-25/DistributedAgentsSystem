"""Unit tests for das.models.dlq and das.ops.dlq (Req 12.1, 12.2, 12.3)."""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from das.codec import MessageCodec, ParseError, new_message
from das.models.dlq import (
    DeadLetterRecord,
    ReplayHistoryEntry,
    RequestTransitionEntry,
)
from das.models.request import RequestStatus
from das.models.results import (
    ControlPayload,
    HeartbeatPayload,
    JobDispatchPayload,
    JobResultPayload,
    TokenUsage,
)
from das.ops.dlq import (
    DELIVERY_LIMIT_ERROR,
    build_record,
    classify_reason,
    dead_letter_id,
    dead_letter_identity,
)
from tests.strategies import dead_letter_records

TS = datetime(2025, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
DLID = "0123456789abcdef0123456789abcdef"


def _dispatch(message_id: str = "m1"):  # type: ignore[no-untyped-def]
    payload = JobDispatchPayload(
        request_id="r1",
        job_id="j1",
        attempt=3,
        instruction="do it",
        required_capabilities=["research", "review"],
    )
    return new_message(payload, correlation_id="r1", message_id=message_id, timestamp=TS)


# ------------------------------------------------------------------ identity / id
def test_identity_uses_message_id_when_parsed() -> None:
    assert dead_letter_identity(_dispatch("abc"), b"ignored") == "msg:abc"


def test_identity_uses_body_hash_when_unparsed() -> None:
    body = b"\xffnot json"
    assert dead_letter_identity(None, body) == "raw:" + hashlib.sha256(body).hexdigest()


def test_dead_letter_id_is_first_32_hex_of_sha256() -> None:
    ident = "msg:abc"
    dlid = dead_letter_id(ident)
    assert dlid == hashlib.sha256(ident.encode()).hexdigest()[:32]
    assert len(dlid) == 32 and all(c in "0123456789abcdef" for c in dlid)


@settings(max_examples=50)
@given(body=st.binary(max_size=64))
def test_dead_letter_id_is_deterministic_per_body(body: bytes) -> None:
    # Redeliveries of the same bytes always map to the same record (Req 12.2).
    a = dead_letter_id(dead_letter_identity(None, body))
    b = dead_letter_id(dead_letter_identity(None, bytes(body)))
    assert a == b


def test_distinct_message_ids_give_distinct_ids() -> None:
    assert dead_letter_id("msg:a") != dead_letter_id("msg:b")
    assert dead_letter_id("msg:a") != dead_letter_id("raw:a")


# ------------------------------------------------------------------ classify_reason
@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"x-death-reason": "max_attempts"}, "retries_exhausted"),
        ({"x-death-reason": "delivery_limit"}, "delivery_limit"),
        ({"x-death-reason": "rejected"}, "malformed"),
        ({"x-death-reason": "expired"}, "malformed"),
        ({}, "malformed"),
    ],
)
def test_classify_reason(headers: dict[str, str], expected: str) -> None:
    assert classify_reason(headers) == expected


# ------------------------------------------------------------------ build_record
def test_build_record_dispatch_retries_exhausted() -> None:
    msg = _dispatch()
    body = MessageCodec().encode(msg)
    rec = build_record(
        DLID, "retries_exhausted", msg, body, {"x-last-error": "boom"}, None, 1_048_576
    )
    assert (rec.request_id, rec.job_id, rec.attempt) == ("r1", "j1", 3)
    assert rec.capabilities == ["research", "review"]
    assert rec.last_error == "boom"
    assert rec.message_id == "m1"
    assert rec.envelope_json is not None
    assert rec.envelope_json.encode("utf-8") == body  # canonical codec bytes
    assert rec.raw_b64 is None and rec.original_size is None and rec.truncated is False
    assert rec.parse_reason is None and rec.parse_field is None


def test_build_record_last_error_truncated_to_4096() -> None:
    msg = _dispatch()
    rec = build_record(
        DLID, "retries_exhausted", msg, b"", {"x-last-error": "e" * 5000}, None, 100
    )
    assert rec.last_error == "e" * 4096


def test_build_record_delivery_limit_default_error() -> None:
    rec = build_record(DLID, "delivery_limit", _dispatch(), b"", {}, None, 100)
    assert rec.last_error == DELIVERY_LIMIT_ERROR


def test_build_record_missing_last_error_is_null() -> None:
    rec = build_record(DLID, "retries_exhausted", _dispatch(), b"", {}, None, 100)
    assert rec.last_error is None


def test_build_record_job_result_has_no_capabilities() -> None:
    payload = JobResultPayload(
        request_id="r2",
        job_id="j2",
        attempt=1,
        worker_id="w",
        status="Succeeded",
        content="x",
        usage=TokenUsage(),
        duration_ms=1,
    )
    msg = new_message(payload, correlation_id="r2", message_id="m2", timestamp=TS)
    rec = build_record(DLID, "delivery_limit", msg, b"", {}, None, 100)
    assert (rec.request_id, rec.job_id, rec.attempt, rec.capabilities) == ("r2", "j2", 1, None)


def test_build_record_control_and_heartbeat() -> None:
    ctl = new_message(
        ControlPayload(command="cancel_job", request_id="r3", job_id="j3"),
        correlation_id="r3",
        message_id="m3",
        timestamp=TS,
    )
    rec = build_record(DLID, "delivery_limit", ctl, b"", {}, None, 100)
    assert (rec.request_id, rec.job_id, rec.attempt, rec.capabilities) == ("r3", "j3", None, None)

    hb = new_message(
        HeartbeatPayload(agent_id="a", role="worker", capabilities=["research"]),
        correlation_id="",
        message_id="m4",
        timestamp=TS,
    )
    rec = build_record(DLID, "delivery_limit", hb, b"", {}, None, 100)
    assert (rec.request_id, rec.job_id, rec.attempt, rec.capabilities) == (None, None, None, None)
    assert rec.message_id == "m4"


def test_build_record_malformed_root_error() -> None:
    body = b"\xff\xfe not json"
    err = ParseError("invalid UTF-8")
    rec = build_record(DLID, "malformed", None, body, {}, err, 1_048_576)
    assert rec.request_id is rec.job_id is rec.attempt is rec.capabilities is None
    assert rec.message_id is None and rec.envelope_json is None
    assert rec.parse_reason == "invalid UTF-8"
    assert rec.parse_field is None  # "$" -> None
    assert rec.last_error == "invalid UTF-8"
    assert rec.original_size == len(body)
    assert base64.b64decode(rec.raw_b64 or "") == body
    assert rec.truncated is False


def test_build_record_malformed_field_error_and_truncation() -> None:
    body = json.dumps({"type": "job_dispatch", "pad": "x" * 200}).encode()
    err = ParseError("unknown envelope field", "pad")
    rec = build_record(DLID, "malformed", None, body, {}, err, 16)
    assert rec.parse_field == "pad"
    assert rec.original_size == len(body)
    assert base64.b64decode(rec.raw_b64 or "") == body[:16]
    assert rec.truncated is True


def test_build_record_malformed_exact_limit_not_truncated() -> None:
    body = b"x" * 16
    rec = build_record(DLID, "malformed", None, body, {}, ParseError("bad"), 16)
    assert rec.truncated is False
    assert base64.b64decode(rec.raw_b64 or "") == body


def test_build_record_malformed_long_parse_reason_last_error_truncated() -> None:
    rec = build_record(DLID, "malformed", None, b"x", {}, ParseError("r" * 5000), 16)
    assert rec.last_error == "r" * 4096


@settings(max_examples=50)
@given(body=st.binary(max_size=300), limit=st.integers(min_value=0, max_value=400))
def test_build_record_malformed_raw_prefix(body: bytes, limit: int) -> None:
    rec = build_record(DLID, "malformed", None, body, {}, ParseError("bad"), limit)
    raw = base64.b64decode(rec.raw_b64 or "")
    assert raw == body[:limit]
    assert rec.truncated == (len(body) > limit)
    assert rec.original_size == len(body)


# ------------------------------------------------------------------ models
@settings(max_examples=50)
@given(dead_letter_records(as_model=True))
def test_strategy_records_validate_and_round_trip(rec: DeadLetterRecord) -> None:
    assert isinstance(rec, DeadLetterRecord)
    assert DeadLetterRecord.model_validate_json(rec.model_dump_json()) == rec


def test_record_rejects_bad_id_and_long_error() -> None:
    base = {
        "dead_letter_id": DLID,
        "status": "Pending",
        "reason": "malformed",
        "dead_lettered_at": TS,
    }
    rec = DeadLetterRecord.model_validate(base)
    assert rec.dead_lettered_at.microsecond == 123000
    with pytest.raises(ValidationError):
        DeadLetterRecord.model_validate({**base, "dead_letter_id": "XYZ"})
    with pytest.raises(ValidationError):
        DeadLetterRecord.model_validate({**base, "last_error": "e" * 4097})
    with pytest.raises(ValidationError):
        DeadLetterRecord.model_validate({**base, "status": "Unknown"})
    with pytest.raises(ValidationError):
        DeadLetterRecord.model_validate({**base, "extra": 1})


def test_replay_history_and_request_transition_entries() -> None:
    h = ReplayHistoryEntry(
        dead_letter_id=DLID, at=TS, error="failed", final_result=None, finished_at=TS
    )
    assert h.at.microsecond == 123000 and h.finished_at is not None
    t = RequestTransitionEntry(
        from_status=RequestStatus.FAILED,
        to_status=RequestStatus.PENDING,
        dead_letter_id=DLID,
        at=TS,
    )
    assert t.scope == "request"
    assert t.model_dump(mode="json")["from_status"] == "Failed"
