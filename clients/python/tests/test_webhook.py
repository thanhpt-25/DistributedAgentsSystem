"""Webhook signature checks, against the vectors the server's own tests use (Phase 4 Req 9.7, 9.8)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from das_client import Event, InvalidSignature, verify_webhook
from das_client.webhook import sign, verify

VECTOR_FILE = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "webhook_vectors.json"
FIXTURE: dict[str, Any] = json.loads(VECTOR_FILE.read_text(encoding="utf-8"))
VECTORS: list[dict[str, Any]] = FIXTURE["vectors"]
ACCEPT: list[dict[str, Any]] = FIXTURE["accept"]
REJECT: list[dict[str, Any]] = FIXTURE["reject"]

SECRET = b"whsec_unit_test_0123456789"
TS = 1_793_000_000


def envelope(**fields: Any) -> bytes:
    body = {"request_id": "r1", "event_id": "1793000000000-0", "type": "request.completed", "data": {"a": 1}}
    return json.dumps({**body, **fields}, sort_keys=True, separators=(",", ":")).encode()


def headers_for(body: bytes, ts: int = TS, secret: bytes = SECRET) -> dict[str, str]:
    return {
        "DAS-Webhook-Id": "r1:1793000000000-0",
        "DAS-Webhook-Timestamp": str(ts),
        "DAS-Webhook-Signature": sign(secret, ts, body),
    }


def entry_headers(entry: dict[str, Any]) -> dict[str, str]:
    return {"DAS-Webhook-Timestamp": str(entry["timestamp"]), "DAS-Webhook-Signature": entry["signature"]}


def vector_id(v: dict[str, Any]) -> str:
    return str(v.get("reason") or f"{v['secret'][:6]}-{len(v['body'])}")


# --------------------------------------------------------------------------- the shared vectors


def test_the_vector_file_has_every_entry_the_server_tests_use() -> None:
    assert (len(VECTORS), len(ACCEPT), len(REJECT)) == (3, 2, 8)  # a silently shortened file must fail


@pytest.mark.parametrize("v", VECTORS, ids=vector_id)
def test_sign_reproduces_every_vector(v: dict[str, Any]) -> None:
    assert sign(v["secret"].encode(), v["timestamp"], v["body"].encode()) == v["signature"]


@pytest.mark.parametrize("v", VECTORS, ids=vector_id)
def test_verify_accepts_every_vector(v: dict[str, Any]) -> None:
    secret, body, ts = v["secret"].encode(), v["body"].encode(), v["timestamp"]
    assert verify(secret, str(ts), body, v["signature"], float(ts), 300)


@pytest.mark.parametrize("v", ACCEPT, ids=vector_id)
def test_verify_accepts_every_header_case(v: dict[str, Any]) -> None:
    secret, body = v["secret"].encode(), v["body"].encode()
    assert verify(secret, v["timestamp"], body, v["signature"], now=v["now"], tolerance=v["tolerance"])


@pytest.mark.parametrize("v", REJECT, ids=vector_id)
def test_verify_rejects_every_reject_case(v: dict[str, Any]) -> None:
    secret, body = v["secret"].encode(), v["body"].encode()
    assert not verify(secret, v["timestamp"], body, v["signature"], now=v["now"], tolerance=v["tolerance"])


@pytest.mark.parametrize("v", ACCEPT, ids=vector_id)
def test_verify_webhook_accepts_every_accept_case(v: dict[str, Any]) -> None:
    event = verify_webhook(v["body"].encode(), entry_headers(v), v["secret"], tolerance=v["tolerance"], now=v["now"])
    assert event == Event("request.completed", "1793000000000-0", {"status": "Completed"}, "r1")


@pytest.mark.parametrize("v", REJECT, ids=vector_id)
def test_verify_webhook_rejects_every_reject_case(v: dict[str, Any]) -> None:
    with pytest.raises(InvalidSignature):
        verify_webhook(v["body"].encode(), entry_headers(v), v["secret"], tolerance=v["tolerance"], now=v["now"])


@pytest.mark.parametrize("v", VECTORS, ids=vector_id)
def test_verify_webhook_on_every_signing_vector(v: dict[str, Any]) -> None:
    """Two vectors sign an event envelope; one signs an empty body, which is genuine but not an event."""
    call = {
        "body": v["body"].encode(),
        "headers": entry_headers(v),
        "secret": v["secret"],
        "now": float(v["timestamp"]),
    }
    if v["body"]:
        event = verify_webhook(**call)
        assert isinstance(event, Event) and event.request_id == "r1"
    else:
        with pytest.raises(InvalidSignature):
            verify_webhook(**call)


# --------------------------------------------------------------------------- verify_webhook


def test_a_valid_delivery_returns_the_parsed_event() -> None:
    body = envelope()
    event = verify_webhook(body, headers_for(body), SECRET, now=float(TS))
    assert event == Event("request.completed", "1793000000000-0", {"a": 1}, "r1")
    assert event.is_terminal


def test_header_names_are_case_insensitive() -> None:
    body = envelope()
    mixed = {k.lower(): v for k, v in headers_for(body).items()}
    upper = {k.upper(): v for k, v in headers_for(body).items()}
    for headers in (mixed, upper, httpx.Headers(headers_for(body)), httpx.Headers(mixed)):
        assert verify_webhook(body, headers, SECRET, now=float(TS)).type == "request.completed"


def test_text_body_and_text_secret_are_accepted() -> None:
    body = envelope()
    assert verify_webhook(body.decode(), headers_for(body), SECRET.decode(), now=float(TS)).request_id == "r1"


def test_the_default_tolerance_is_300_seconds() -> None:
    body = envelope()
    headers = headers_for(body)
    assert verify_webhook(body, headers, SECRET, now=float(TS + 300)).request_id == "r1"
    with pytest.raises(InvalidSignature):
        verify_webhook(body, headers, SECRET, now=float(TS + 301))
    assert verify_webhook(body, headers, SECRET, tolerance=600, now=float(TS + 600)).request_id == "r1"


def test_now_defaults_to_the_current_time() -> None:
    body = envelope()
    now = int(time.time())
    assert verify_webhook(body, headers_for(body, ts=now), SECRET).request_id == "r1"
    with pytest.raises(InvalidSignature):
        verify_webhook(body, headers_for(body, ts=now - 3600), SECRET)


def test_a_nan_clock_is_rejected() -> None:
    body = envelope()
    with pytest.raises(InvalidSignature):
        verify_webhook(body, headers_for(body), SECRET, now=float("nan"))


@pytest.mark.parametrize("missing", ["DAS-Webhook-Timestamp", "DAS-Webhook-Signature"])
def test_a_missing_header_is_an_invalid_signature_not_a_key_error(missing: str) -> None:
    body = envelope()
    headers = {k: v for k, v in headers_for(body).items() if k != missing}
    with pytest.raises(InvalidSignature):
        verify_webhook(body, headers, SECRET, now=float(TS))


@pytest.mark.parametrize("change", ["body", "timestamp", "signature", "secret"])
def test_changing_any_one_input_is_rejected(change: str) -> None:
    body = envelope()
    headers, secret, shown = headers_for(body), SECRET, body
    if change == "body":
        shown = envelope(data={"a": 2})
    elif change == "timestamp":
        headers["DAS-Webhook-Timestamp"] = str(TS + 1)
    elif change == "signature":
        headers["DAS-Webhook-Signature"] = headers["DAS-Webhook-Signature"][:-1] + "0"
        assert headers["DAS-Webhook-Signature"] != sign(SECRET, TS, body)
    else:
        secret = b"another-secret-0123456789"
    with pytest.raises(InvalidSignature):
        verify_webhook(shown, headers, secret, now=float(TS))


def test_a_non_ascii_signature_is_invalid_not_a_type_error() -> None:
    body = envelope()
    headers = headers_for(body)
    headers["DAS-Webhook-Signature"] = "v1=é"
    with pytest.raises(InvalidSignature):
        verify_webhook(body, headers, SECRET, now=float(TS))


@pytest.mark.parametrize("body", [b"not json", b"[1]", b"{}", b'{"request_id":"r1"}', b"", b"\xff\xfe"])
def test_a_genuinely_signed_body_that_is_not_an_event_is_still_refused(body: bytes) -> None:
    with pytest.raises(InvalidSignature):
        verify_webhook(body, headers_for(body), SECRET, now=float(TS))


@pytest.mark.parametrize("fields", [{"data": []}, {"type": 5}, {"event_id": None}, {"request_id": 1}])
def test_a_signed_envelope_with_the_wrong_field_types_is_refused(fields: dict[str, Any]) -> None:
    body = envelope(**fields)
    with pytest.raises(InvalidSignature):
        verify_webhook(body, headers_for(body), SECRET, now=float(TS))


def test_an_unsigned_body_is_never_parsed() -> None:
    """A bad signature must be reported as one, even over a body that would not parse."""
    headers = {"DAS-Webhook-Timestamp": str(TS), "DAS-Webhook-Signature": "v1=" + "0" * 64}
    with pytest.raises(InvalidSignature, match="signature"):
        verify_webhook(b"\xff not json", headers, SECRET, now=float(TS))


def test_extra_envelope_fields_are_ignored() -> None:
    body = envelope(brand_new_field=1)
    assert verify_webhook(body, headers_for(body), SECRET, now=float(TS)).request_id == "r1"


def test_the_error_text_reveals_nothing_secret() -> None:
    body = envelope()
    headers = headers_for(body)
    with pytest.raises(InvalidSignature) as caught:
        verify_webhook(body, headers, b"another-secret-0123456789", now=float(TS))
    text = f"{caught.value} {caught.value!r}"
    for private in ("another-secret-0123456789", headers["DAS-Webhook-Signature"], body.decode()):
        assert private not in text
