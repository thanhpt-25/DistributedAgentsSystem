"""Webhook body, signing and retry schedule (Phase 4 Req 8.4, 8.6, 9.7). Pure.

The Client_Package keeps a copy of :func:`sign` and :func:`verify`; both copies are
checked against ``tests/fixtures/webhook_vectors.json`` so they cannot drift.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Final

from das.manager.backoff import backoff

SIGNATURE_PREFIX: Final = "v1="
#: Retry delays are ``backoff(attempts) * (JITTER_LOW + JITTER_SPAN * rand)``: 0.8x to 1.2x.
JITTER_LOW: Final = 0.8
JITTER_SPAN: Final = 0.4
#: Longest accepted DAS-Webhook-Timestamp; Unix seconds need 10 digits.
MAX_TIMESTAMP_DIGITS: Final = 19


def webhook_body(request_id: str, event_id: str, event_type: str, data: dict[str, Any]) -> bytes:
    """Canonical JSON body of a Webhook_Delivery (Req 8.4)."""
    obj = {"request_id": request_id, "event_id": event_id, "type": event_type, "data": data}
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return text.encode("utf-8")


def sign(secret: bytes, timestamp: int, body: bytes) -> str:
    """``DAS-Webhook-Signature`` value: ``v1=`` + hex HMAC-SHA256 of ``"{timestamp}.{body}"``."""
    mac = hmac.new(secret, f"{timestamp}.".encode("ascii") + body, hashlib.sha256)
    return SIGNATURE_PREFIX + mac.hexdigest()


def verify(secret: bytes, timestamp: str, body: bytes, signature: str, now: float, tolerance: int) -> bool:
    """True iff ``signature`` signs ``body`` at ``timestamp``, and ``timestamp`` is near ``now``.

    "Near" means at most ``tolerance`` seconds away. ``timestamp`` is the header text and
    must be 1 to 19 ASCII digits. The comparison runs in constant time.
    """
    if len(timestamp) > MAX_TIMESTAMP_DIGITS or not (timestamp.isascii() and timestamp.isdigit()):
        return False
    ts = int(timestamp)
    # Written as "not <=" so a NaN ``now`` fails the window check instead of skipping it.
    if not abs(now - ts) <= tolerance:
        return False
    expected = sign(secret, ts, body).encode("ascii")
    return hmac.compare_digest(expected, signature.encode("utf-8", "surrogatepass"))


def next_due(now: float, attempts: int, base: float, max_backoff: float, rand: float) -> float:
    """Next attempt time after ``attempts`` failed attempts (Req 8.6). ``rand`` is in [0, 1]."""
    if not 0.0 <= rand <= 1.0:
        raise ValueError(f"rand must be in [0, 1], got {rand}")
    return now + backoff(attempts, base, max_backoff) * (JITTER_LOW + JITTER_SPAN * rand)
