"""The webhook sink of the client API e2e stack (das-client-api task 11.1; Phase 4 Req 8.4, 8.6, 9.7).

The sink runs in a plain ``python:3.12-slim`` container, so it is standard library only and carries its own copy
of ``sign`` and ``verify``. Two things keep that copy honest: the shared vector file, and the server's real ``sign``
(so a change to the server's signing breaks this test, not a Compose run). The HTTP behaviour is exercised
in-process on an ephemeral port."""

from __future__ import annotations

import http.client
import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from das.client.webhook import sign as server_sign
from das.client.webhook import webhook_body
from tests.integration.e2e.client_api_constants import WEBHOOK_SECRET
from tests.integration.e2e.webhook_sink import make_server, sign, verify

SECRET = WEBHOOK_SECRET.encode()
FIXTURE: dict[str, Any] = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures" / "webhook_vectors.json").read_text(encoding="utf-8")
)
VECTORS, ACCEPT, REJECT = FIXTURE["vectors"], FIXTURE["accept"], FIXTURE["reject"]


# --------------------------------------------------------------------------- the copy of sign and verify


def test_the_vector_file_has_every_entry_the_other_suites_use() -> None:
    assert (len(VECTORS), len(ACCEPT), len(REJECT)) == (3, 2, 8)


@pytest.mark.parametrize("v", VECTORS, ids=lambda v: f"{v['secret'][:6]}-{len(v['body'])}")
def test_sign_reproduces_every_vector(v: dict[str, Any]) -> None:
    assert sign(v["secret"].encode(), v["timestamp"], v["body"].encode()) == v["signature"]


@pytest.mark.parametrize("v", VECTORS, ids=lambda v: f"{v['secret'][:6]}-{len(v['body'])}")
def test_verify_accepts_every_vector(v: dict[str, Any]) -> None:
    now = float(v["timestamp"])
    assert verify(v["secret"].encode(), str(v["timestamp"]), v["body"].encode(), v["signature"], now, 300)


@pytest.mark.parametrize("v", ACCEPT, ids=lambda v: v["reason"])
def test_verify_accepts_every_header_case(v: dict[str, Any]) -> None:
    secret, body = v["secret"].encode(), v["body"].encode()
    assert verify(secret, v["timestamp"], body, v["signature"], v["now"], v["tolerance"])


@pytest.mark.parametrize("v", REJECT, ids=lambda v: v["reason"])
def test_verify_rejects_every_reject_case(v: dict[str, Any]) -> None:
    secret, body = v["secret"].encode(), v["body"].encode()
    assert not verify(secret, v["timestamp"], body, v["signature"], v["now"], v["tolerance"])


@pytest.mark.parametrize("ts", [0, 1, 1_793_000_000, 2**40])
@pytest.mark.parametrize("body", [b"", b"{}", b'{"a":"\xc3\xa9"}', b"x" * 5000])
def test_sign_equals_the_servers_sign(ts: int, body: bytes) -> None:
    assert sign(SECRET, ts, body) == server_sign(SECRET, ts, body)


@pytest.mark.parametrize("cut", [0, 1, 3, 10, 40, 66])
def test_verify_rejects_a_truncated_signature(cut: int) -> None:
    """A signature that is only the start of the right one (a prefix compare would accept it) is not valid."""
    body, ts = b"{}", 1_793_000_000
    signature = sign(SECRET, ts, body)
    assert len(signature) == 67 and cut < len(signature)  # "v1=" and 64 hex characters
    assert not verify(SECRET, str(ts), body, signature[:cut], float(ts), 300)


def test_verify_rejects_an_extended_or_recased_signature() -> None:
    body, ts = b"{}", 1_793_000_000
    signature = sign(SECRET, ts, body)
    assert verify(SECRET, str(ts), body, signature, float(ts), 300)
    assert not verify(SECRET, str(ts), body, signature + "0", float(ts), 300)
    assert not verify(SECRET, str(ts), body, signature.upper(), float(ts), 300)


def test_verify_rejects_a_clock_that_is_not_a_number() -> None:
    body, ts = b"{}", 1_793_000_000
    assert not verify(SECRET, str(ts), body, sign(SECRET, ts, body), float("nan"), 300)


# --------------------------------------------------------------------------- the HTTP behaviour


@pytest.fixture
def sink() -> Iterator[Any]:
    server = make_server("127.0.0.1", 0, SECRET, fail_first=2)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def call(server: Any, method: str, path: str, body: bytes | None = None, headers: dict[str, str] | None = None):
    conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=10)
    try:
        conn.request(method, path, body, headers or {})
        response = conn.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        conn.close()


def delivery(
    request_id: str = "r-1",
    event_id: str = "1-0",
    event_type: str = "request.completed",
    *,
    ts: int | None = None,
    secret: bytes = SECRET,
    body: bytes | None = None,
) -> tuple[bytes, dict[str, str]]:
    """A delivery signed the way the Gateway signs one (the server's own ``sign`` and ``webhook_body``)."""
    payload = webhook_body(request_id, event_id, event_type, {"status": "Completed"}) if body is None else body
    stamp = int(time.time()) if ts is None else ts
    headers = {
        "Content-Type": "application/json",
        "DAS-Webhook-Id": f"{request_id}:{event_id}",
        "DAS-Webhook-Timestamp": str(stamp),
        "DAS-Webhook-Signature": server_sign(secret, stamp, payload),
    }
    return payload, headers


def post(server: Any, **kwargs: Any) -> int:
    body, headers = delivery(**kwargs)
    return call(server, "POST", "/hook", body, headers)[0]


def received(server: Any, query: str = "") -> list[dict[str, Any]]:
    status, _, raw = call(server, "GET", f"/received{query}")
    assert status == 200
    return json.loads(raw)


def test_the_first_two_deliveries_of_each_id_fail_and_the_rest_succeed(sink: Any) -> None:
    assert [post(sink) for _ in range(4)] == [500, 500, 204, 204]
    assert [post(sink, request_id="r-2", event_id="9-0") for _ in range(3)] == [500, 500, 204]  # counted per id


def test_every_delivery_is_recorded_with_its_attempt_and_verdict(sink: Any) -> None:
    for _ in range(3):
        post(sink, request_id="r-7", event_id="5-0", event_type="request.failed")
    records = received(sink)
    assert [r["attempt"] for r in records] == [1, 2, 3]
    for r in records:
        assert (r["id"], r["request_id"], r["event_type"]) == ("r-7:5-0", "r-7", "request.failed")
        assert r["verified"] is True
    assert [r["status"] for r in records] == [500, 500, 204]
    times = [r["t"] for r in records]
    assert times == sorted(times) and all(isinstance(t, float) for t in times)  # the sink's own monotonic clock


def test_a_delivery_with_a_bad_signature_is_recorded_unverified_and_refused(sink: Any) -> None:
    body, headers = delivery()
    headers["DAS-Webhook-Signature"] = "v1=" + "0" * 64
    assert call(sink, "POST", "/hook", body, headers)[0] == 401
    (record,) = received(sink)
    assert (record["verified"], record["attempt"], record["status"]) == (False, 1, 401)


def test_a_delivery_signed_with_another_secret_is_unverified(sink: Any) -> None:
    assert post(sink, secret=b"another-secret-0123456789") == 401
    assert received(sink)[0]["verified"] is False


def test_a_stale_timestamp_is_unverified(sink: Any) -> None:
    assert post(sink, ts=int(time.time()) - 1000) == 401  # outside the 300 s tolerance
    assert received(sink)[0]["verified"] is False


def test_a_delivery_without_the_webhook_headers_does_not_crash_the_sink(sink: Any) -> None:
    assert call(sink, "POST", "/hook", b"{}", {"Content-Type": "application/json"})[0] == 401
    (record,) = received(sink)
    assert (record["id"], record["verified"]) == (None, False)
    assert post(sink) == 500  # and it still serves


def test_a_signed_body_that_is_not_json_is_verified_but_has_no_event(sink: Any) -> None:
    body, headers = delivery(body=b"not json at all")
    assert call(sink, "POST", "/hook", body, headers)[0] == 500  # still the first attempt of its id
    (record,) = received(sink)
    assert (record["verified"], record["request_id"], record["event_type"]) == (True, None, None)


def test_the_log_can_be_filtered_by_request_id(sink: Any) -> None:
    post(sink, request_id="r-a", event_id="1-0")
    post(sink, request_id="r-b", event_id="2-0")
    post(sink, request_id="r-a", event_id="1-0")
    assert [r["request_id"] for r in received(sink)] == ["r-a", "r-b", "r-a"]
    assert [r["attempt"] for r in received(sink, "?request_id=r-a")] == [1, 2]
    assert received(sink, "?request_id=nobody") == []


def test_reset_clears_the_log_and_the_attempt_counts(sink: Any) -> None:
    post(sink)
    post(sink)
    assert call(sink, "POST", "/reset")[0] == 204
    assert received(sink) == []
    assert post(sink) == 500  # counted from attempt 1 again


def test_health_and_unknown_paths(sink: Any) -> None:
    assert call(sink, "GET", "/healthz")[0] == 200
    assert call(sink, "GET", "/nope")[0] == 404
    assert call(sink, "POST", "/nope", b"{}")[0] == 404
    assert received(sink) == []  # none of that was a delivery


def test_the_answer_body_is_tiny(sink: Any) -> None:
    for expected in (500, 500, 204):
        status, headers, raw = call(sink, "POST", "/hook", *delivery())
        assert status == expected
        assert len(raw) <= 64 and int(headers.get("Content-Length", "0")) <= 64


def test_fail_first_is_configurable() -> None:
    server = make_server("127.0.0.1", 0, SECRET, fail_first=0)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    try:
        assert [post(server) for _ in range(3)] == [204, 204, 204]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def test_concurrent_deliveries_of_one_id_get_distinct_attempt_numbers(sink: Any) -> None:
    statuses: list[int] = []
    lock = threading.Lock()

    def send() -> None:
        status = post(sink, request_id="r-c", event_id="3-0")
        with lock:
            statuses.append(status)

    threads = [threading.Thread(target=send) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert sorted(r["attempt"] for r in received(sink)) == list(range(1, 9))
    assert sorted(statuses) == [204] * 6 + [500] * 2  # exactly two failures, whatever the interleaving


def test_each_delivery_leaves_one_line_in_the_log(sink: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """``docker compose logs`` is how a failed e2e run is explained."""
    post(sink, request_id="r-log", event_id="4-0")
    line = next(ln for ln in capsys.readouterr().out.splitlines() if "r-log" in ln)
    assert "attempt=1" in line and "status=500" in line and "verified=True" in line
