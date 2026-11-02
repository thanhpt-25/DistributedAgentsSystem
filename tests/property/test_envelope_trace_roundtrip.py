"""Property 5: Round-trip envelope có Trace_Context.

For any valid Message of every type, with or without a valid Trace_Context
(with or without ``tracestate``):

- ``decode(encode(m)) == m`` on every envelope field, payload and ``trace_context``;
- the bytes carry ``schema_version`` 2 iff ``m`` has a Trace_Context;
- the ``trace_context`` object has a ``tracestate`` key iff ``m`` has one;
- v1 bytes (no Trace_Context) decode to a Message without a Trace_Context.

**Validates: Requirements 7.3, 7.4, 7.5, 7.9**
"""

from __future__ import annotations

import json

from hypothesis import given, settings
from hypothesis import strategies as st

from das.codec.envelope import (
    AgentControlMessage,
    AgentControlPayload,
    AgentControlReplyMessage,
    AgentControlReplyPayload,
    TraceContextModel,
    TypedMessage,
)
from das.codec.message_codec import MessageCodec
from tests.strategies import message_timestamps, messages, trace_contexts

_ids = st.text(min_size=1, max_size=64)
_agent_ids = st.text(min_size=1, max_size=64)

_agent_control_messages = st.builds(
    AgentControlMessage,
    message_id=_ids,
    correlation_id=st.sampled_from(["", "corr"]) | st.uuids().map(str),
    timestamp=message_timestamps,
    payload=st.builds(
        AgentControlPayload,
        command=st.sampled_from(["drain", "undrain"]),
        agent_id=_agent_ids,
        release_leases=st.booleans(),
    ),
)

_agent_control_reply_messages = st.builds(
    AgentControlReplyMessage,
    message_id=_ids,
    correlation_id=st.sampled_from(["", "corr"]) | st.uuids().map(str),
    timestamp=message_timestamps,
    payload=st.builds(
        AgentControlReplyPayload,
        agent_id=_agent_ids,
        status=st.sampled_from(["Available", "Draining"]),
        in_flight=st.integers(min_value=0, max_value=10_000),
    ),
)

all_messages = st.one_of(messages(), _agent_control_messages, _agent_control_reply_messages)


def _with_trace(message: TypedMessage, ctx: TraceContextModel | None) -> TypedMessage:
    """Rebuild ``message`` with ``ctx`` (re-validated, so ``schema_version`` normalizes)."""
    return type(message)(
        message_id=message.message_id,
        correlation_id=message.correlation_id,
        timestamp=message.timestamp,
        payload=message.payload,
        trace_context=ctx,
    )


_codec = MessageCodec()


@settings(max_examples=300)
@given(
    base=all_messages,
    ctx=st.none() | trace_contexts(as_model=True),
)
def test_envelope_roundtrip_with_trace_context(
    base: TypedMessage, ctx: TraceContextModel | None
) -> None:
    message = _with_trace(base, ctx)
    data = _codec.encode(message)
    wire = json.loads(data.decode("utf-8"))

    # schema_version 2 iff the Message has a Trace_Context (Req 7.3).
    assert wire["schema_version"] == (2 if ctx is not None else 1)
    assert ("trace_context" in wire) == (ctx is not None)
    if ctx is not None:
        # tracestate key present iff the Message has one (Req 7.3).
        assert ("tracestate" in wire["trace_context"]) == (ctx.tracestate is not None)
        assert wire["trace_context"]["traceparent"] == ctx.traceparent

    decoded = _codec.decode(data)

    # Full round-trip (Req 7.4, 7.5).
    assert decoded == message
    assert type(decoded) is type(message)
    assert decoded.type == message.type
    assert decoded.message_id == message.message_id
    assert decoded.correlation_id == message.correlation_id
    assert decoded.timestamp == message.timestamp
    assert decoded.payload == message.payload
    assert decoded.trace_context == ctx
    assert decoded.schema_version == message.schema_version

    # v1 bytes decode to a Message without a Trace_Context (Req 7.9).
    if ctx is None:
        assert decoded.trace_context is None
        assert decoded.schema_version == 1
