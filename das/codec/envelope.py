"""Message envelope and typed Message classes (Requirement 9.1, 13.4; Phase 2 Req 7).

A schema version 1 envelope has exactly six fields: ``type``, ``schema_version``,
``message_id``, ``correlation_id``, ``timestamp``, and ``payload``. Envelope_V2
(schema version 2) adds the optional ``trace_context`` object. ``schema_version``
is normalized on construction: 2 when the Message carries a Trace_Context, else 1,
so a Message without one always serializes byte-identical to v1 (Req 7.2, 19.7).
Each message type has its own typed class whose ``payload`` is the matching
pydantic payload model.

Timestamps are UTC and truncated to millisecond precision on construction, so
``decode(encode(m)) == m`` holds exactly. On the wire they render as
``2025-01-01T00:00:00.123Z``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, Final, Literal

from pydantic import Field, PositiveInt, field_serializer, field_validator, model_validator

from das.models.base import DasModel, to_utc_ms
from das.models.results import (
    ControlPayload,
    HeartbeatPayload,
    JobDispatchPayload,
    JobResultPayload,
)
from das.observability.context import get_request_id
from das.observability.trace_context import is_valid_traceparent, is_valid_tracestate

MessageType = Literal[
    "job_dispatch", "job_result", "heartbeat", "control", "agent_control", "agent_control_reply"
]

CURRENT_SCHEMA_VERSION: Final = 1
#: Schema version of an envelope carrying ``trace_context`` (Envelope_V2).
TRACE_SCHEMA_VERSION: Final = 2
MAX_MESSAGE_ID_LENGTH: Final = 64

#: Exact top-level field set of a schema version 1 envelope (Req 9.1).
ENVELOPE_FIELDS: Final = frozenset(
    {"type", "schema_version", "message_id", "correlation_id", "timestamp", "payload"}
)
#: Optional top-level fields allowed in addition to ``ENVELOPE_FIELDS`` in Envelope_V2.
ENVELOPE_V2_OPTIONAL_FIELDS: Final = frozenset({"trace_context"})

#: Message types that belong to a Request and must carry its Request_ID (Req 13.4, 13.6).
REQUEST_SCOPED_TYPES: Final = frozenset({"job_dispatch", "job_result", "control"})


def format_timestamp(value: datetime) -> str:
    """Render a UTC datetime as ISO 8601 with milliseconds and a ``Z`` suffix."""
    return to_utc_ms(value).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_now_ms() -> datetime:
    return to_utc_ms(datetime.now(UTC))


class TraceContextModel(DasModel):
    """W3C Trace_Context carried in Envelope_V2 (Req 7.1).

    Construction rejects an invalid ``traceparent`` or ``tracestate``; the codec's
    lenient decode path drops invalid values before building this model.
    """

    traceparent: str
    tracestate: str | None = None

    @field_validator("traceparent")
    @classmethod
    def _check_traceparent(cls, value: str) -> str:
        if not is_valid_traceparent(value):
            raise ValueError("invalid W3C traceparent")
        return value

    @field_validator("tracestate")
    @classmethod
    def _check_tracestate(cls, value: str | None) -> str | None:
        if value is not None and not is_valid_tracestate(value):
            raise ValueError("invalid W3C tracestate")
        return value


class AgentControlPayload(DasModel):
    """Admin drain/undrain command sent to one agent (Req 17, design D3)."""

    command: Literal["drain", "undrain"]
    agent_id: str = Field(min_length=1)
    release_leases: bool = False


class AgentControlReplyPayload(DasModel):
    """Reply of an agent to an ``agent_control`` command (Req 17)."""

    agent_id: str = Field(min_length=1)
    status: Literal["Available", "Draining"]
    in_flight: int = Field(ge=0)


class _EnvelopeFields(DasModel):
    """Envelope fields shared by the generic Envelope and every typed Message."""

    # Normalized after validation: 2 with trace_context, else 1 (Req 7.2, 7.3).
    schema_version: PositiveInt = Field(default=CURRENT_SCHEMA_VERSION, strict=True)
    message_id: str = Field(min_length=1, max_length=MAX_MESSAGE_ID_LENGTH)
    # Request_ID for request-scoped types; "" allowed for heartbeat/control (Req 9.1).
    correlation_id: str = ""
    timestamp: datetime
    # Envelope_V2 only; never present in v1 output (Req 7.1).
    trace_context: TraceContextModel | None = None

    @model_validator(mode="after")
    def _normalize_version(self) -> _EnvelopeFields:
        version = TRACE_SCHEMA_VERSION if self.trace_context is not None else 1
        object.__setattr__(self, "schema_version", version)
        return self

    @field_validator("timestamp")
    @classmethod
    def _truncate_timestamp(cls, value: datetime) -> datetime:
        return to_utc_ms(value)

    @field_serializer("timestamp", when_used="json")
    def _serialize_timestamp(self, value: datetime) -> str:
        return format_timestamp(value)


class Envelope(_EnvelopeFields):
    """Untyped envelope view: the payload is a plain JSON object."""

    type: MessageType
    payload: dict[str, Any]


class JobDispatchMessage(_EnvelopeFields):
    type: Literal["job_dispatch"] = "job_dispatch"
    payload: JobDispatchPayload


class JobResultMessage(_EnvelopeFields):
    type: Literal["job_result"] = "job_result"
    payload: JobResultPayload


class HeartbeatMessage(_EnvelopeFields):
    type: Literal["heartbeat"] = "heartbeat"
    payload: HeartbeatPayload


class ControlMessage(_EnvelopeFields):
    type: Literal["control"] = "control"
    payload: ControlPayload


class AgentControlMessage(_EnvelopeFields):
    type: Literal["agent_control"] = "agent_control"
    payload: AgentControlPayload


class AgentControlReplyMessage(_EnvelopeFields):
    type: Literal["agent_control_reply"] = "agent_control_reply"
    payload: AgentControlReplyPayload


TypedMessage = (
    JobDispatchMessage
    | JobResultMessage
    | HeartbeatMessage
    | ControlMessage
    | AgentControlMessage
    | AgentControlReplyMessage
)

Message = Annotated[TypedMessage, Field(discriminator="type")]

AnyPayload = (
    JobDispatchPayload
    | JobResultPayload
    | HeartbeatPayload
    | ControlPayload
    | AgentControlPayload
    | AgentControlReplyPayload
)

#: message type -> typed Message class.
MESSAGE_CLASSES: Final[dict[str, type[TypedMessage]]] = {
    "job_dispatch": JobDispatchMessage,
    "job_result": JobResultMessage,
    "heartbeat": HeartbeatMessage,
    "control": ControlMessage,
    "agent_control": AgentControlMessage,
    "agent_control_reply": AgentControlReplyMessage,
}

#: message type -> payload model.
PAYLOAD_MODELS: Final[dict[str, type[DasModel]]] = {
    "job_dispatch": JobDispatchPayload,
    "job_result": JobResultPayload,
    "heartbeat": HeartbeatPayload,
    "control": ControlPayload,
    "agent_control": AgentControlPayload,
    "agent_control_reply": AgentControlReplyPayload,
}

_TYPE_BY_PAYLOAD: Final[dict[type[DasModel], str]] = {v: k for k, v in PAYLOAD_MODELS.items()}


def new_message_id() -> str:
    return uuid.uuid4().hex


def new_message(
    payload: AnyPayload,
    *,
    correlation_id: str | None = None,
    message_id: str | None = None,
    timestamp: datetime | None = None,
    schema_version: int = CURRENT_SCHEMA_VERSION,
    trace_context: TraceContextModel | None = None,
) -> TypedMessage:
    """Build a typed Message for ``payload``.

    ``trace_context`` is attached when given (Envelope_V2); ``schema_version`` is
    then normalized to 2, otherwise to 1, whatever value is passed.

    The message type is inferred from the payload class. When ``correlation_id``
    is not given, the Request_ID bound in the current context (see
    :mod:`das.observability.context`) is copied in, so outbound Messages
    produced while handling an inbound Message inherit its correlation ID
    (Req 13.4, 13.5). With nothing bound, the correlation ID is ``""``.
    """
    msg_type = _TYPE_BY_PAYLOAD.get(type(payload))
    if msg_type is None:
        raise TypeError(f"unsupported payload type: {type(payload).__name__}")
    if correlation_id is None:
        correlation_id = get_request_id() or ""
    cls = MESSAGE_CLASSES[msg_type]
    return cls(
        schema_version=schema_version,
        message_id=message_id or new_message_id(),
        correlation_id=correlation_id,
        timestamp=timestamp if timestamp is not None else utc_now_ms(),
        payload=payload,
        trace_context=trace_context,
    )


__all__ = [
    "CURRENT_SCHEMA_VERSION",
    "ENVELOPE_FIELDS",
    "ENVELOPE_V2_OPTIONAL_FIELDS",
    "TRACE_SCHEMA_VERSION",
    "AgentControlMessage",
    "AgentControlPayload",
    "AgentControlReplyMessage",
    "AgentControlReplyPayload",
    "TraceContextModel",
    "TypedMessage",
    "MAX_MESSAGE_ID_LENGTH",
    "MESSAGE_CLASSES",
    "PAYLOAD_MODELS",
    "REQUEST_SCOPED_TYPES",
    "AnyPayload",
    "ControlMessage",
    "Envelope",
    "HeartbeatMessage",
    "JobDispatchMessage",
    "JobResultMessage",
    "Message",
    "MessageType",
    "format_timestamp",
    "new_message",
    "new_message_id",
    "utc_now_ms",
]
