"""Canonical JSON Message_Codec (Requirement 9).

Wire format: UTF-8 JSON with sorted keys, compact separators, and
``ensure_ascii=False``. The canonical form is deterministic, so every Transport
hands the same bytes to the wire for the same Message (Req 9.3).

Schema versions 1 and 2 are supported (Phase 2 Req 7). A Message without a
Trace_Context encodes as schema version 1, byte-identical to the v1 codec; a
Message with one encodes as Envelope_V2 with a ``trace_context`` object.

``decode`` checks, in order: size, UTF-8/JSON, ``schema_version`` (the field set
depends on it), envelope field set, ``type``, remaining envelope fields, then the
payload against the per-type model (strict, no coercion). A malformed
``trace_context`` in Envelope_V2 is dropped, never a parse error (Req 7.7, 7.8).
Any other failure raises :class:`ParseError` and no partially populated Message
is ever returned (Req 9.4, 9.6).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Final

from pydantic import ValidationError

from das.codec.envelope import (
    ENVELOPE_FIELDS,
    ENVELOPE_V2_OPTIONAL_FIELDS,
    MAX_MESSAGE_ID_LENGTH,
    MESSAGE_CLASSES,
    PAYLOAD_MODELS,
    TRACE_SCHEMA_VERSION,
    TraceContextModel,
    TypedMessage,
)
from das.models.base import to_utc_ms
from das.observability.logging import get_logger
from das.observability.trace_context import TraceContext

DEFAULT_MAX_BYTES: Final = 1_048_576  # 1 MiB (Req 9.6)

_TYPED_CLASSES: Final = tuple(MESSAGE_CLASSES.values())

_log = get_logger(__name__)

#: ``field`` value used when the failure concerns the whole input, not one field.
ROOT_FIELD: Final = "$"


class CodecError(ValueError):
    """Base class for Message_Codec errors."""


class ParseError(CodecError):
    """Received bytes are not a valid Message (Req 9.4, 9.6).

    ``reason`` describes the failure; ``field`` names the offending field as a
    dotted path (``payload.attempt``), or ``"$"`` for the input as a whole.
    """

    def __init__(self, reason: str, field: str = ROOT_FIELD) -> None:
        self.reason = reason
        self.field = field
        super().__init__(f"{field}: {reason}")


class MessageEncodeError(CodecError):
    """A Message cannot be serialized."""


class MessageTooLarge(MessageEncodeError):
    """The serialized Message exceeds the configured maximum size (Req 9.6)."""

    def __init__(self, size: int, limit: int) -> None:
        self.size = size
        self.limit = limit
        super().__init__(f"message size {size} bytes exceeds the limit of {limit} bytes")


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-standard JSON constant {name}")


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"duplicate key {key!r}")
        obj[key] = value
    return obj


def _loc_to_field(prefix: str, loc: tuple[int | str, ...]) -> str:
    return ".".join([prefix, *(str(part) for part in loc)])


def envelope_to_wire(message: TypedMessage) -> dict[str, Any]:
    """JSON object of ``message`` as it goes on the wire (Req 7.2, 7.3).

    Without a Trace_Context: exactly the six v1 fields with ``schema_version`` 1.
    With one: ``schema_version`` 2 plus ``trace_context`` holding ``traceparent``
    and, only when present, ``tracestate``.
    """
    obj: dict[str, Any] = message.model_dump(mode="json", exclude={"trace_context"})
    ctx = message.trace_context
    if ctx is None:
        obj["schema_version"] = 1
        return obj
    obj["schema_version"] = TRACE_SCHEMA_VERSION
    trace: dict[str, str] = {"traceparent": ctx.traceparent}
    if ctx.tracestate is not None:
        trace["tracestate"] = ctx.tracestate
    obj["trace_context"] = trace
    return obj


def canonical_json(obj: Any) -> str:
    """Canonical JSON text: sorted keys, compact separators, ``ensure_ascii=False``."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def parse_trace_context(raw: object) -> TraceContextModel | None:
    """Lenient parse of an Envelope_V2 ``trace_context`` value (Req 7.4, 7.7, 7.8).

    Returns ``None`` (and logs at debug) when ``raw`` is not an object, lacks
    ``traceparent`` or has an invalid one. An invalid or non-string ``tracestate``
    is dropped while a valid ``traceparent`` is kept. Never raises.
    """
    if not isinstance(raw, dict):
        _log.debug("trace_context_dropped", reason="not an object")
        return None
    ctx = TraceContext.parse(raw.get("traceparent"), raw.get("tracestate"))
    if ctx is None:
        _log.debug("trace_context_dropped", reason="missing or invalid traceparent")
        return None
    if ctx.tracestate is None and raw.get("tracestate") is not None:
        _log.debug("tracestate_dropped", reason="invalid tracestate")
    return TraceContextModel(traceparent=ctx.traceparent, tracestate=ctx.tracestate)


class MessageCodec:
    """Serialize Messages to canonical bytes and parse bytes into typed Messages."""

    SUPPORTED_VERSIONS: frozenset[int] = frozenset({1, 2})

    def __init__(self, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        self.max_bytes = max_bytes

    # ------------------------------------------------------------------ encode
    def encode(self, message: TypedMessage) -> bytes:
        """Return the canonical bytes of ``message``.

        Raises :class:`MessageTooLarge` (without returning bytes) when the
        serialization exceeds ``max_bytes``.
        """
        if not isinstance(message, _TYPED_CLASSES):
            raise TypeError(f"not a Message: {type(message).__name__}")
        try:
            data = canonical_json(envelope_to_wire(message)).encode("utf-8")
        except (UnicodeEncodeError, ValueError) as exc:
            raise MessageEncodeError(f"message cannot be serialized: {exc}") from exc
        if len(data) > self.max_bytes:
            raise MessageTooLarge(len(data), self.max_bytes)
        return data

    # ------------------------------------------------------------------ decode
    def decode(self, data: bytes | bytearray | memoryview) -> TypedMessage:
        """Parse ``data`` into the typed Message for its envelope ``type``."""
        if not isinstance(data, bytes | bytearray | memoryview):
            raise ParseError(f"expected bytes, got {type(data).__name__}")
        raw = bytes(data)

        # 1. size
        if len(raw) > self.max_bytes:
            raise ParseError(
                f"message size {len(raw)} bytes exceeds the limit of {self.max_bytes} bytes"
            )

        # 2. JSON
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ParseError(f"invalid UTF-8: {exc.reason}") from None
        try:
            obj = json.loads(
                text, parse_constant=_reject_constant, object_pairs_hook=_no_duplicate_keys
            )
        except (ValueError, RecursionError) as exc:
            raise ParseError(f"invalid JSON: {exc}") from None
        if not isinstance(obj, dict):
            raise ParseError("envelope must be a JSON object")

        # 3. schema_version (checked first: the allowed field set depends on it)
        version: object = obj.get("schema_version", 1)
        if "schema_version" in obj:
            if isinstance(version, bool) or not isinstance(version, int) or version < 1:
                raise ParseError("schema_version must be a positive integer", "schema_version")
            if version not in self.SUPPORTED_VERSIONS:
                raise ParseError(f"unsupported schema_version {version}", "schema_version")

        # 4. envelope field set (v1: exact; v2: v1 plus optional trace_context)
        allowed = (
            ENVELOPE_FIELDS | ENVELOPE_V2_OPTIONAL_FIELDS
            if version == TRACE_SCHEMA_VERSION
            else ENVELOPE_FIELDS
        )
        missing = ENVELOPE_FIELDS - obj.keys()
        if missing:
            raise ParseError("missing required envelope field", min(missing))
        extra = obj.keys() - allowed
        if extra:
            raise ParseError("unknown envelope field", min(extra))

        # 5. type
        msg_type = obj["type"]
        if not isinstance(msg_type, str) or msg_type not in MESSAGE_CLASSES:
            raise ParseError(f"unrecognized message type {msg_type!r}", "type")

        # remaining envelope fields
        message_id = obj["message_id"]
        if not isinstance(message_id, str) or not 1 <= len(message_id) <= MAX_MESSAGE_ID_LENGTH:
            raise ParseError(
                f"message_id must be a string of 1 to {MAX_MESSAGE_ID_LENGTH} characters",
                "message_id",
            )
        correlation_id = obj["correlation_id"]
        if not isinstance(correlation_id, str):
            raise ParseError("correlation_id must be a string", "correlation_id")
        timestamp = self._parse_timestamp(obj["timestamp"])
        trace_context = (
            parse_trace_context(obj["trace_context"]) if "trace_context" in obj else None
        )

        # 6. payload
        payload_raw = obj["payload"]
        if not isinstance(payload_raw, dict):
            raise ParseError("payload must be a JSON object", "payload")
        try:
            payload = PAYLOAD_MODELS[msg_type].model_validate(payload_raw, strict=True)
        except ValidationError as exc:
            err = exc.errors()[0]
            raise ParseError(
                f"payload does not match the {msg_type} schema: {err['msg']}",
                _loc_to_field("payload", tuple(err["loc"])),
            ) from None

        try:
            return MESSAGE_CLASSES[msg_type](
                message_id=message_id,
                correlation_id=correlation_id,
                timestamp=timestamp,
                payload=payload,
                trace_context=trace_context,
            )
        except ValidationError as exc:  # defensive; all fields were checked above
            err = exc.errors()[0]
            raise ParseError(err["msg"], _loc_to_field("", tuple(err["loc"])).lstrip(".")) from None

    @staticmethod
    def _parse_timestamp(value: object) -> datetime:
        if not isinstance(value, str):
            raise ParseError("timestamp must be an ISO 8601 string", "timestamp")
        try:
            parsed = datetime.fromisoformat(value)
            return to_utc_ms(parsed)
        except ValueError as exc:
            raise ParseError(f"invalid timestamp: {exc}", "timestamp") from None


__all__ = [
    "DEFAULT_MAX_BYTES",
    "ROOT_FIELD",
    "CodecError",
    "MessageCodec",
    "MessageEncodeError",
    "MessageTooLarge",
    "ParseError",
    "TypedMessage",
    "canonical_json",
    "envelope_to_wire",
    "parse_trace_context",
]
