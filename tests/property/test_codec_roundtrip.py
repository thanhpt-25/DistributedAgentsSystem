# Feature: distributed-agent-system, Property 4: Message codec round trip and canonical encoding
"""Property 4: Message codec round trip and canonical encoding.

For any valid Message of every type (job_dispatch, job_result, heartbeat,
control):

- ``decode(encode(m)) == m`` across every envelope and payload field.
- ``encode(decode(encode(m))) == encode(m)`` byte-for-byte.
- The top-level JSON keys of ``encode(m)`` are exactly
  ``{type, schema_version, message_id, correlation_id, timestamp, payload}``.
- The encoding is canonical (sorted keys, compact separators, UTF-8 with
  ``ensure_ascii=False``) and identical across repeated encodes and independent
  codec instances, so every Transport hands the same bytes to the wire.

Note: byte equality of what the RabbitMQ and gRPC adapters (and the in-memory
fake transport) actually put on the wire is asserted in task 8.7, once those
adapters exist. Here we pin the transport-independent part: ``encode`` is a
pure, canonical function of the Message.

**Validates: Requirements 9.1, 9.2, 9.3, 9.7**
"""

from __future__ import annotations

import json
import re

from hypothesis import given
from hypothesis import strategies as st

from das.codec.envelope import ENVELOPE_FIELDS, MESSAGE_CLASSES
from das.codec.message_codec import MessageCodec
from tests.strategies import messages

_MESSAGE_TYPES = ("job_dispatch", "job_result", "heartbeat", "control")
_TIMESTAMP_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")

_codec = MessageCodec()


def _canonical(obj: object) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


@given(st.sampled_from(_MESSAGE_TYPES).flatmap(lambda t: messages((t,))))
def test_round_trip_and_canonical_encoding(message: object) -> None:
    """**Validates: Requirements 9.1, 9.2, 9.3, 9.7**"""
    encoded = _codec.encode(message)  # type: ignore[arg-type]

    # Req 9.7 / 9.2: round trip yields the same typed Message in every field.
    decoded = _codec.decode(encoded)
    assert type(decoded) is type(message)
    assert type(decoded) is MESSAGE_CLASSES[decoded.type]
    assert decoded == message
    assert decoded.model_dump() == message.model_dump()  # type: ignore[attr-defined]

    # Re-encoding the decoded Message is byte-identical.
    assert _codec.encode(decoded) == encoded

    # Req 9.1: exactly the six envelope fields, with well-formed values.
    obj = json.loads(encoded.decode("utf-8"))
    assert set(obj) == ENVELOPE_FIELDS
    assert obj["type"] == message.type  # type: ignore[attr-defined]
    assert isinstance(obj["schema_version"], int) and obj["schema_version"] >= 1
    assert isinstance(obj["message_id"], str) and 1 <= len(obj["message_id"]) <= 64
    assert isinstance(obj["correlation_id"], str)
    assert _TIMESTAMP_RE.match(obj["timestamp"])
    assert isinstance(obj["payload"], dict)
    if obj["type"] in ("job_dispatch", "job_result"):
        assert obj["correlation_id"] == obj["payload"]["request_id"]
    elif obj["type"] == "heartbeat":
        assert obj["correlation_id"] == ""

    # Req 9.3: canonical form (sorted keys, compact, UTF-8, non-ASCII kept),
    # deterministic across repeated encodes and independent codec instances.
    assert encoded == _canonical(obj)
    assert _codec.encode(message) == encoded  # type: ignore[arg-type]
    assert MessageCodec().encode(message) == encoded  # type: ignore[arg-type]
    assert MessageCodec(max_bytes=len(encoded)).encode(message) == encoded  # type: ignore[arg-type]
