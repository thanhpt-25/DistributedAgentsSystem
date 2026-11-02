# Feature: das-observability-operations, Property 6: Bytes độc lập Transport và trùng v1 khi không có Trace_Context
"""Property 6: wire bytes are Transport-independent and v1-identical without Trace_Context.

For any valid Message (every type, with or without a Trace_Context):

- The bytes the RabbitMQ adapter (fake aio-pika channel) and the gRPC adapter
  (fake ``AgentControlStub``) hand to the wire both equal ``encode(m)``.
- When ``m`` has no Trace_Context, ``encode(m)`` is byte-identical to
  ``V1ReferenceCodec.encode(m)`` (the frozen v1 codec in ``tests/fixtures``), and
  ``V1ReferenceCodec.decode`` parses those bytes back to ``m`` without error.
- With tracing disabled (``NoopTracer``), every Message built by ``new_message``
  has no Trace_Context and therefore serializes as v1.

**Validates: Requirements 6.13, 7.2, 7.6, 19.7**
"""

from __future__ import annotations

import asyncio
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from das.codec.envelope import (
    AgentControlMessage,
    AgentControlPayload,
    AgentControlReplyMessage,
    AgentControlReplyPayload,
    TraceContextModel,
    new_message,
)
from das.codec.message_codec import MessageCodec
from das.transport.grpc_rpc import GrpcTransport
from das.transport.proto import das_pb2
from das.transport.rabbitmq import RabbitMQTransport
from tests.fixtures.v1_codec import V1ReferenceCodec
from tests.strategies import message_timestamps, messages, trace_contexts
from tests.unit.test_rabbitmq_transport import FakeBroker

try:  # Tracer shell lands in a later task; until then use its documented contract.
    from das.observability.tracing import NoopTracer  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - depends on task order

    class NoopTracer:  # type: ignore[no-redef]
        """Stand-in with the design contract: ``current_context()`` is always ``None``."""

        def current_context(self) -> TraceContextModel | None:
            return None


_V1_TYPES = ("job_dispatch", "job_result", "heartbeat", "control")
_codec = MessageCodec()
_v1 = V1ReferenceCodec()

_ids = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=64)


@st.composite
def _agent_control_messages(draw: st.DrawFn) -> object:
    if draw(st.booleans()):
        return AgentControlMessage(
            message_id=draw(_ids),
            correlation_id="",
            timestamp=draw(message_timestamps),
            payload=AgentControlPayload(
                command=draw(st.sampled_from(["drain", "undrain"])),
                agent_id=draw(_ids),
                release_leases=draw(st.booleans()),
            ),
        )
    return AgentControlReplyMessage(
        message_id=draw(_ids),
        correlation_id="",
        timestamp=draw(message_timestamps),
        payload=AgentControlReplyPayload(
            agent_id=draw(_ids),
            status=draw(st.sampled_from(["Available", "Draining"])),
            in_flight=draw(st.integers(min_value=0, max_value=10_000)),
        ),
    )


@st.composite
def _any_messages(draw: st.DrawFn) -> object:
    """Any valid Message of all six types, with or without a Trace_Context."""
    base = draw(messages() | _agent_control_messages())
    trace = draw(st.none() | trace_contexts(as_model=True))
    if trace is None:
        return base
    return type(base).model_validate({**base.model_dump(), "trace_context": trace})


# --------------------------------------------------------------------------- fake gRPC stub


class _FakeStub:
    """Records the ``Frame`` bytes the adapter would send; acks every ``Send``."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.called: list[bytes] = []
        self.reply: bytes = b""

    async def Send(self, frame: das_pb2.Frame, **_: Any) -> das_pb2.Ack:  # noqa: N802
        self.sent.append(bytes(frame.envelope))
        return das_pb2.Ack(ok=True)

    async def Call(self, frame: das_pb2.Frame, **_: Any) -> das_pb2.Frame:  # noqa: N802
        self.called.append(bytes(frame.envelope))
        return das_pb2.Frame(envelope=self.reply)


async def _grpc_wire_bytes(message: object) -> tuple[bytes, bytes, object]:
    endpoint = "peer:50051"
    transport = GrpcTransport(agent_id="sender", bind="127.0.0.1:0", resolver=lambda _: endpoint)
    stub = _FakeStub()
    stub.reply = _codec.encode(message)  # type: ignore[arg-type]
    # Skip binding a real server / channel: inject the fake stub for the endpoint.
    transport._started = True
    transport._stubs[endpoint] = stub  # type: ignore[assignment]
    await transport.publish("peer", message)  # type: ignore[arg-type]
    reply = await transport.request_reply("peer", message, timeout=1)  # type: ignore[arg-type]
    (sent,) = stub.sent
    (called,) = stub.called
    return sent, called, reply


async def _rabbitmq_wire_bytes(message: object) -> bytes:
    broker = FakeBroker()
    transport = RabbitMQTransport(url="amqp://guest:guest@localhost:5672/", connect=broker.connect)
    await transport.start()
    try:
        await transport.publish("results", message)  # type: ignore[arg-type]
    finally:
        await transport.close()
    (_, _, amqp_message, _, _) = broker.published[-1]
    return bytes(amqp_message.body)


# --------------------------------------------------------------------------- properties


@given(_any_messages())
def test_wire_bytes_equal_encode_on_every_transport(message: object) -> None:
    """**Validates: Requirements 7.6**"""
    encoded = _codec.encode(message)  # type: ignore[arg-type]

    rabbit_body = asyncio.run(_rabbitmq_wire_bytes(message))
    grpc_sent, grpc_called, grpc_reply = asyncio.run(_grpc_wire_bytes(message))

    assert rabbit_body == encoded
    assert grpc_sent == encoded
    assert grpc_called == encoded
    assert grpc_reply == message


@given(st.sampled_from(_V1_TYPES).flatmap(lambda t: messages((t,))))
def test_encoding_without_trace_context_is_v1_identical(message: object) -> None:
    """**Validates: Requirements 7.2, 19.7**"""
    assert message.trace_context is None  # type: ignore[attr-defined]
    encoded = _codec.encode(message)  # type: ignore[arg-type]

    assert encoded == _v1.encode(message)
    assert _v1.decode(encoded) == message


@given(st.sampled_from(_V1_TYPES).flatmap(lambda t: messages((t,))))
def test_noop_tracer_messages_have_no_trace_context(message: object) -> None:
    """**Validates: Requirements 6.13, 7.2**"""
    tracer = NoopTracer()
    built = new_message(
        message.payload,  # type: ignore[attr-defined]
        correlation_id=message.correlation_id,  # type: ignore[attr-defined]
        message_id=message.message_id,  # type: ignore[attr-defined]
        timestamp=message.timestamp,  # type: ignore[attr-defined]
        trace_context=tracer.current_context(),
    )

    assert built.trace_context is None
    assert built.schema_version == 1
    assert built == message
    encoded = _codec.encode(built)
    assert encoded == _v1.encode(built)
    assert _v1.decode(encoded) == built
