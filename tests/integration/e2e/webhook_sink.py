"""Webhook receiver for the client API end-to-end tests (das-client-api task 11.1; Phase 4 Req 8.4, 8.6).

Runs inside a plain ``python:3.12-slim`` container (standard library only, like ``llm_stub.py``), so it carries its
own copy of ``sign`` and ``verify``; ``tests/unit/test_webhook_sink.py`` pins that copy to
``tests/fixtures/webhook_vectors.json`` and to the server's real ``sign``.

What it does:

- ``POST /hook``: a webhook delivery. The signature and timestamp are checked with ``SINK_SECRET`` (a bad one is
  answered ``401``). The first ``SINK_FAIL_FIRST`` deliveries (default 2) of each ``DAS-Webhook-Id`` are answered
  ``500`` so the Gateway has to retry; later ones get ``204``. Every delivery is recorded and logged.
- ``GET /received[?request_id=...]``: the records, oldest first, as a JSON list. Each has ``id``, ``request_id``,
  ``event_type``, ``attempt`` (1 for the first delivery of that id), ``t`` (this process's monotonic clock, so spacing
  between attempts can be measured without trusting any other clock), ``verified`` and ``status``.
- ``POST /reset``: forget every record and attempt count. ``GET /healthz``: ``200``.

The answer body is a few bytes: the Gateway reads at most 64 KiB of it and ignores it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

SIGNATURE_PREFIX = "v1="
#: Longest accepted ``DAS-Webhook-Timestamp``; Unix seconds need 10 digits.
MAX_TIMESTAMP_DIGITS = 19
#: Seconds the timestamp may differ from this container's clock (the default of ``verify_webhook``).
TOLERANCE = 300


def sign(secret: bytes, timestamp: int, body: bytes) -> str:
    """``DAS-Webhook-Signature`` value: ``v1=`` + hex HMAC-SHA256 of ``"{timestamp}.{body}"``."""
    mac = hmac.new(secret, f"{timestamp}.".encode("ascii") + body, hashlib.sha256)
    return SIGNATURE_PREFIX + mac.hexdigest()


def verify(secret: bytes, timestamp: str, body: bytes, signature: str, now: float, tolerance: int) -> bool:
    """True iff ``signature`` signs ``body`` at ``timestamp`` and ``timestamp`` is within ``tolerance`` of ``now``."""
    if len(timestamp) > MAX_TIMESTAMP_DIGITS or not (timestamp.isascii() and timestamp.isdigit()):
        return False
    ts = int(timestamp)
    if not abs(now - ts) <= tolerance:  # written so that a NaN clock fails instead of skipping the check
        return False
    expected = sign(secret, ts, body).encode("ascii")
    return hmac.compare_digest(expected, signature.encode("utf-8", "surrogatepass"))


class Sink(ThreadingHTTPServer):
    """The server and its state: the delivery records and the number of deliveries seen per ``DAS-Webhook-Id``."""

    daemon_threads = True
    #: ``http.server`` defaults to a backlog of 5, so a burst of simultaneous deliveries (two Gateways, two sender
    #: workers each, plus the test polling) could have a connection refused, and the sink would add noise.
    request_queue_size = 128

    def __init__(self, address: tuple[str, int], secret: bytes, fail_first: int) -> None:
        super().__init__(address, Handler)
        self.secret = secret
        self.fail_first = fail_first
        self.lock = threading.Lock()
        self.records: list[dict[str, Any]] = []
        self.attempts: dict[str | None, int] = {}

    def deliver(self, headers: Mapping[str, str], body: bytes) -> int:
        """Check, record and log one delivery; returns the HTTP status to answer with."""
        delivery_id = headers.get("DAS-Webhook-Id")
        timestamp = headers.get("DAS-Webhook-Timestamp")
        signature = headers.get("DAS-Webhook-Signature")
        verified = (
            timestamp is not None
            and signature is not None
            and verify(self.secret, timestamp, body, signature, time.time(), TOLERANCE)
        )
        request_id, event_type = None, None
        try:
            envelope = json.loads(body)
        except ValueError:
            envelope = None
        if isinstance(envelope, dict):
            request_id = envelope.get("request_id") if isinstance(envelope.get("request_id"), str) else None
            event_type = envelope.get("type") if isinstance(envelope.get("type"), str) else None
        with self.lock:
            attempt = self.attempts[delivery_id] = self.attempts.get(delivery_id, 0) + 1
            status = 401 if not verified else (500 if attempt <= self.fail_first else 204)
            self.records.append(
                {
                    "id": delivery_id,
                    "request_id": request_id,
                    "event_type": event_type,
                    "attempt": attempt,
                    "t": time.monotonic(),
                    "verified": verified,
                    "status": status,
                }
            )
        print(
            f"webhook-sink delivery id={delivery_id} request_id={request_id} type={event_type} "
            f"attempt={attempt} status={status} verified={verified}",
            flush=True,
        )
        return status

    def snapshot(self, request_id: str | None) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(r) for r in self.records if request_id is None or r["request_id"] == request_id]

    def reset(self) -> None:
        with self.lock:
            self.records.clear()
            self.attempts.clear()


class Handler(BaseHTTPRequestHandler):
    server: Sink

    def _answer(self, code: int, body: bytes = b"", content_type: str = "text/plain") -> None:
        self.send_response(code)
        if body:
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        url = urlparse(self.path)
        if url.path == "/healthz":
            self._answer(200, b"ok")
        elif url.path == "/received":
            wanted = parse_qs(url.query).get("request_id", [None])[0]
            self._answer(200, json.dumps(self.server.snapshot(wanted)).encode(), "application/json")
        else:
            self._answer(404, b"not found")

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        path = urlparse(self.path).path
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self._answer(400, b"bad length")
            return
        body = self.rfile.read(length) if length > 0 else b""
        if path == "/hook":
            status = self.server.deliver(self.headers, body)
            self._answer(status, b"" if status == 204 else b"no" if status == 500 else b"bad signature")
        elif path == "/reset":
            self.server.reset()
            self._answer(204)
        else:
            self._answer(404, b"not found")

    def log_message(self, fmt: str, *args: object) -> None:
        return None  # deliveries log themselves; polling by the tests would only add noise


def make_server(host: str, port: int, secret: bytes, fail_first: int = 2) -> Sink:
    """A sink bound to ``host:port`` (port 0 picks a free one; see ``server.server_address``)."""
    return Sink((host, port), secret, fail_first)


def main() -> None:
    secret = os.environ["SINK_SECRET"].encode("utf-8")
    port = int(os.environ.get("SINK_PORT", "8099"))
    fail_first = int(os.environ.get("SINK_FAIL_FIRST", "2"))
    print(f"webhook-sink listening on :{port}, failing the first {fail_first} deliveries of each id", flush=True)
    make_server("0.0.0.0", port, secret, fail_first).serve_forever()  # noqa: S104 - a container-only test server


if __name__ == "__main__":
    main()
