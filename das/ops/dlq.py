"""Dead_Letter_Record identity, reason classification and record building (Req 12).

Pure module (Req 19.5): no I/O, no clock reads, no randomness.

- Identity (design D6): ``msg:<message_id>`` when the envelope parsed, otherwise
  ``raw:<sha256(body)>``. Byte-identical malformed Messages share one identity.
- Dead_Letter_ID: first 32 hex characters of ``sha256(identity)``, so every
  redelivery to any Manager yields the same ID (Req 12.2).
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from typing import Final

from das.codec.envelope import (
    ControlMessage,
    HeartbeatMessage,
    JobDispatchMessage,
    JobResultMessage,
)
from das.codec.message_codec import (
    ROOT_FIELD,
    ParseError,
    TypedMessage,
    canonical_json,
    envelope_to_wire,
)
from das.models.dlq import MAX_LAST_ERROR_LENGTH, DeadLetterFields, DeadLetterReason
from das.models.results import ControlPayload, JobDispatchPayload, JobResultPayload

#: Header carrying the dead-letter cause (set by transports and by the Manager).
DEATH_REASON_HEADER: Final = "x-death-reason"
#: Header carrying the last Job error on Manager-published dead letters.
LAST_ERROR_HEADER: Final = "x-last-error"
#: ``x-death-reason`` values and their Dead_Letter_Reason.
_REASONS: Final[dict[str, DeadLetterReason]] = {
    "max_attempts": "retries_exhausted",
    "delivery_limit": "delivery_limit",
}
DELIVERY_LIMIT_ERROR: Final = "repeated unacknowledged delivery"


def dead_letter_identity(message: TypedMessage | None, body: bytes) -> str:
    """Stable identity of a dead-lettered Message."""
    if message is not None:
        return f"msg:{message.message_id}"
    return f"raw:{hashlib.sha256(bytes(body)).hexdigest()}"


def dead_letter_id(identity: str) -> str:
    """Dead_Letter_ID: the first 32 hex characters of ``sha256(identity)``."""
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


def classify_reason(headers: Mapping[str, str]) -> DeadLetterReason:
    """Map ``x-death-reason`` to a Dead_Letter_Reason; anything else is ``malformed``."""
    return _REASONS.get(headers.get(DEATH_REASON_HEADER, ""), "malformed")


def _truncate_error(value: str | None) -> str | None:
    return None if value is None else value[:MAX_LAST_ERROR_LENGTH]


def _canonical_json(message: TypedMessage) -> str:
    """Canonical envelope JSON, byte-identical to ``MessageCodec.encode``."""
    return canonical_json(envelope_to_wire(message))


def _message_fields(
    message: TypedMessage,
) -> tuple[str | None, str | None, int | None, list[str] | None]:
    """``(request_id, job_id, attempt, capabilities)`` parsed from ``message``."""
    p: JobDispatchPayload | JobResultPayload | ControlPayload
    if isinstance(message, JobDispatchMessage):
        p = message.payload
        return p.request_id, p.job_id, p.attempt, list(p.required_capabilities)
    if isinstance(message, JobResultMessage):
        p = message.payload
        return p.request_id, p.job_id, p.attempt, None
    if isinstance(message, ControlMessage):
        p = message.payload
        return p.request_id, p.job_id, None, None
    if isinstance(message, HeartbeatMessage):
        return None, None, None, None
    return message.correlation_id or None, None, None, None  # pragma: no cover


def build_record(
    dlid: str,
    reason: DeadLetterReason,
    message: TypedMessage | None,
    body: bytes,
    headers: Mapping[str, str],
    parse_error: ParseError | None,
    max_bytes: int,
) -> DeadLetterFields:
    """Fields of the Dead_Letter_Record for one dead-lettered Message (Req 12.1, 12.3).

    Request_ID, Job_ID, attempt, Capabilities and last error are ``None`` when
    they cannot be obtained. The last error is cut to 4096 characters. For
    ``malformed`` the raw bytes are kept (at most ``max_bytes``) with the parse
    error, the offending field (``None`` for the whole input) and the original size.
    """
    if max_bytes < 0:
        raise ValueError("max_bytes must be >= 0")
    request_id = job_id = None
    attempt: int | None = None
    capabilities: list[str] | None = None
    message_id: str | None = None
    envelope_json: str | None = None
    if message is not None:
        request_id, job_id, attempt, capabilities = _message_fields(message)
        message_id = message.message_id
        envelope_json = _canonical_json(message)

    last_error = headers.get(LAST_ERROR_HEADER)
    if last_error is None and reason == "delivery_limit":
        last_error = DELIVERY_LIMIT_ERROR
    if last_error is None and reason == "malformed" and parse_error is not None:
        last_error = parse_error.reason

    parse_reason: str | None = None
    parse_field: str | None = None
    original_size: int | None = None
    raw_b64: str | None = None
    truncated = False
    if reason == "malformed":
        raw = bytes(body)
        if parse_error is not None:
            parse_reason = parse_error.reason
            parse_field = None if parse_error.field == ROOT_FIELD else parse_error.field
        original_size = len(raw)
        raw_b64 = base64.b64encode(raw[:max_bytes]).decode("ascii")
        truncated = len(raw) > max_bytes

    return DeadLetterFields(
        dead_letter_id=dlid,
        reason=reason,
        request_id=request_id,
        job_id=job_id,
        attempt=attempt,
        capabilities=capabilities,
        last_error=_truncate_error(last_error),
        message_id=message_id,
        parse_reason=parse_reason,
        parse_field=parse_field,
        original_size=original_size,
        raw_b64=raw_b64,
        truncated=truncated,
        envelope_json=envelope_json,
    )
