"""Webhook signature verification (Phase 4 Req 9.7, 9.8).

:func:`sign` and :func:`verify` are a copy of the server's ``das.client.webhook`` (this package cannot import
the server). Both copies are checked against ``tests/fixtures/webhook_vectors.json`` and by a property test
that runs them side by side, so they cannot drift.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from collections.abc import Mapping

from .errors import InvalidSignature
from .models import Event

SIGNATURE_PREFIX = "v1="
#: Longest accepted ``DAS-Webhook-Timestamp``; Unix seconds need 10 digits.
MAX_TIMESTAMP_DIGITS = 19


def sign(secret: bytes, timestamp: int, body: bytes) -> str:
    """``DAS-Webhook-Signature`` value: ``v1=`` + hex HMAC-SHA256 of ``"{timestamp}.{body}"``."""
    mac = hmac.new(secret, f"{timestamp}.".encode("ascii") + body, hashlib.sha256)
    return SIGNATURE_PREFIX + mac.hexdigest()


def verify(secret: bytes, timestamp: str, body: bytes, signature: str, now: float, tolerance: int) -> bool:
    """True iff ``signature`` signs ``body`` at ``timestamp``, and ``timestamp`` is within ``tolerance`` of ``now``.

    ``timestamp`` is the header text and must be 1 to 19 ASCII digits. The comparison runs in constant time.
    """
    if len(timestamp) > MAX_TIMESTAMP_DIGITS or not (timestamp.isascii() and timestamp.isdigit()):
        return False
    ts = int(timestamp)
    # Written as "not <=" so a NaN ``now`` fails the window check instead of skipping it.
    if not abs(now - ts) <= tolerance:
        return False
    expected = sign(secret, ts, body).encode("ascii")
    return hmac.compare_digest(expected, signature.encode("utf-8", "surrogatepass"))


def _as_bytes(value: bytes | str) -> bytes:
    return value if isinstance(value, bytes) else value.encode("utf-8")


def verify_webhook(
    body: bytes | str,
    headers: Mapping[str, str],
    secret: bytes | str,
    *,
    tolerance: int = 300,
    now: float | None = None,
) -> Event:
    """Check a webhook delivery and return its event, or raise :class:`InvalidSignature`.

    ``body`` must be the raw request body exactly as received (not re-serialised JSON). ``headers`` may be any
    mapping; names are matched case-insensitively. The signature and timestamp are checked first and the body is
    parsed only once they pass, so an unsigned body is never interpreted. ``now`` defaults to the current time.
    """
    lowered = {str(name).lower(): value for name, value in headers.items()}
    timestamp = lowered.get("das-webhook-timestamp")
    signature = lowered.get("das-webhook-signature")
    if not isinstance(timestamp, str) or not isinstance(signature, str):
        raise InvalidSignature("missing DAS-Webhook-Timestamp or DAS-Webhook-Signature header")
    raw = _as_bytes(body)
    if not verify(_as_bytes(secret), timestamp, raw, signature, time.time() if now is None else now, tolerance):
        raise InvalidSignature("invalid signature, or timestamp outside the allowed tolerance")
    try:
        event = Event.from_envelope(json.loads(raw))
    except (ValueError, RecursionError):
        event = None
    if event is None:
        raise InvalidSignature("the signed body is not a webhook event")
    return event
