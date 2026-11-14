"""Cross-adapter Transport contract tests (task 8.7).

Every Transport adapter (the in-memory fake, RabbitMQ, gRPC) is checked against
the same contract:

- the four operations have the same signatures as the :class:`Transport`
  protocol (Req 8.1);
- ``request_reply`` without a reply raises :class:`RequestTimeout` naming the
  destination (Req 8.7);
- ``publish`` and ``request_reply`` while the connection is down raise
  :class:`TransportUnavailable` and put nothing on the wire (Req 8.8);
- the bytes on the wire equal ``MessageCodec.encode`` of the Message, so the
  same Message is byte-identical over every adapter (Req 9.3);
- third-party ``http`` and ``kafka`` adapters registered only through the
  ``das.transports`` entry-point group load and work without code changes
  (Req 15.1, 15.2).

RabbitMQ runs against a minimal in-process fake of aio-pika (a trimmed copy of
the one in ``test_rabbitmq_transport.py``); gRPC runs real ``grpc.aio`` servers
on ephemeral localhost ports.

Requirements: 8.1, 8.7, 8.8, 9.3, 15.1, 15.2
"""

from __future__ import annotations

import asyncio
import inspect
from collections import deque
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from importlib.metadata import EntryPoint
from typing import Any, ClassVar

import aiormq
import pytest
from pamqp import commands as spec

from das.codec import MessageCodec, new_message
from das.codec.message_codec import TypedMessage
from das.models.results import ControlPayload, HeartbeatPayload
from das.transport.base import (
    TRANSPORT_OPERATIONS,
    Delivery,
    RequestTimeout,
    Transport,
    TransportUnavailable,
    missing_operations,
)
from das.transport.grpc_rpc import GrpcTransport
from das.transport.loader import ENTRY_POINT_GROUP, available_transports, load_transport
from das.transport.rabbitmq import QUEUE_RESULTS, RabbitMQTransport
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

TS = datetime(2025, 1, 1, 12, 30, 45, 123000, tzinfo=UTC)
CODEC = MessageCodec()


def heartbeat(n: int = 1) -> TypedMessage:
    return new_message(
        HeartbeatPayload(agent_id="w1", role="worker", capabilities=["code"]),
        correlation_id="",
        message_id=f"hb-{n}",
        timestamp=TS,
    )


def control(n: int = 1) -> TypedMessage:
    return new_message(
        ControlPayload(command="cancel_job", request_id="r1", job_id=f"j{n}"),
        correlation_id="",
        message_id=f"ctl-{n}",
        timestamp=TS,
    )


ADAPTERS: list[type] = [MemoryTransport, RabbitMQTransport, GrpcTransport]


# --------------------------------------------------------------------------- signatures (8.1)


def _signature(cls: type, op: str) -> inspect.Signature:
    return inspect.signature(getattr(cls, op))


@pytest.mark.parametrize("op", TRANSPORT_OPERATIONS)
@pytest.mark.parametrize("adapter", ADAPTERS, ids=lambda c: c.__name__)
def test_operation_signature_matches_protocol(adapter: type, op: str) -> None:
    expected = _signature(Transport, op)
    actual = _signature(adapter, op)
    # Names, kinds, defaults, and annotations of every parameter, and the return type.
    assert actual == expected, f"{adapter.__name__}.{op}{actual} != Transport.{op}{expected}"
    assert inspect.iscoroutinefunction(getattr(adapter, op))


@pytest.mark.parametrize("op", TRANSPORT_OPERATIONS)
def test_operation_signatures_identical_across_adapters(op: str) -> None:
    signatures = {cls.__name__: _signature(cls, op) for cls in ADAPTERS}
    assert len(set(signatures.values())) == 1, signatures


@pytest.mark.parametrize("adapter", ADAPTERS, ids=lambda c: c.__name__)
def test_adapter_is_complete(adapter: type) -> None:
    assert missing_operations(adapter) == []
    assert isinstance(adapter.name, str) and adapter.name


# --------------------------------------------------------------------------- fake aio-pika


class _Callbacks:
    def __init__(self, owner: object) -> None:
        self.owner = owner
        self._items: list[Callable[..., Any]] = []

    def add(self, cb: Callable[..., Any]) -> None:
        self._items.append(cb)

    def fire(self, exc: BaseException | None = None) -> None:
        for cb in list(self._items):
            cb(self.owner, exc)


class _Incoming:
    def __init__(self, queue: str, body: bytes, message: Any) -> None:
        self.queue = queue
        self.body = body
        self.headers = dict(message.headers or {})
        self.redelivered = False
        self.reply_to = message.reply_to
        self.correlation_id = message.correlation_id

    async def ack(self) -> None: ...

    async def nack(self, requeue: bool = True) -> None: ...

    async def reject(self, requeue: bool = False) -> None: ...


class _Queue:
    def __init__(self, broker: FakeAmqp, name: str) -> None:
        self.broker = broker
        self.name = name
        self.messages: deque[_Incoming] = deque()

    async def bind(self, exchange: Any, routing_key: str = "") -> None:
        self.broker.bindings.add((exchange.name, routing_key, self.name))

    async def get(self, *, no_ack: bool = False, fail: bool = True, timeout: float = 5) -> Any:
        if self.broker.down:
            raise aiormq.exceptions.ChannelInvalidStateError("closed")
        return self.messages.popleft() if self.messages else None

    async def consume(self, callback: Callable[..., Any], no_ack: bool = False) -> str:
        self.broker.consumers[self.name] = callback
        return "ctag"


class _Exchange:
    def __init__(self, broker: FakeAmqp, name: str) -> None:
        self.broker = broker
        self.name = name

    async def publish(
        self, message: Any, routing_key: str, *, mandatory: bool = True, timeout: Any = None
    ) -> Any:
        if self.broker.down:
            raise aiormq.exceptions.ChannelInvalidStateError("closed")
        self.broker.wire.append(bytes(message.body))
        if self.name == "":
            targets = [routing_key] if routing_key in self.broker.queues else []
        else:
            targets = [
                q for (ex, rk, q) in self.broker.bindings if ex == self.name and rk == routing_key
            ]
        for target in targets:
            incoming = _Incoming(target, message.body, message)
            if target in self.broker.consumers:
                await self.broker.consumers[target](incoming)
            else:
                self.broker.queues[target].messages.append(incoming)
        return spec.Basic.Ack(delivery_tag=len(self.broker.wire))


class _Channel:
    def __init__(self, broker: FakeAmqp) -> None:
        self.broker = broker
        self.close_callbacks = _Callbacks(self)
        self.default_exchange = _Exchange(broker, "")

    async def declare_exchange(self, name: str, kind: Any, durable: bool = False) -> _Exchange:
        return _Exchange(self.broker, name)

    async def declare_queue(self, name: str = "", **_: Any) -> _Queue:
        if not name:
            name = f"amq.gen-{len(self.broker.queues)}"
        return self.broker.queues.setdefault(name, _Queue(self.broker, name))


class _Connection:
    def __init__(self, broker: FakeAmqp) -> None:
        self.broker = broker
        self.close_callbacks = _Callbacks(self)
        self.is_closed = False

    async def channel(self, publisher_confirms: bool = True) -> _Channel:
        return _Channel(self.broker)

    async def close(self) -> None:
        if not self.is_closed:
            self.is_closed = True
            self.close_callbacks.fire(None)


class FakeAmqp:
    """Shared state of a minimal fake aio-pika broker; ``wire`` holds published bodies."""

    def __init__(self) -> None:
        self.queues: dict[str, _Queue] = {}
        self.bindings: set[tuple[str, str, str]] = set()
        self.consumers: dict[str, Callable[..., Any]] = {}
        self.connections: list[_Connection] = []
        self.wire: list[bytes] = []
        self.down = False
        self.refuse = False

    async def connect(self, url: str, timeout: float | None = None) -> _Connection:
        if self.refuse:
            raise ConnectionRefusedError("refused")
        connection = _Connection(self)
        self.connections.append(connection)
        return connection

    def drop(self) -> None:
        """Broker goes away: close the connection and refuse reconnects."""
        self.down = True
        self.refuse = True
        connection = self.connections[-1]
        connection.is_closed = True
        connection.close_callbacks.fire(ConnectionResetError("reset"))


# --------------------------------------------------------------------------- harnesses


class Harness:
    """One adapter under test plus access to its wire and a way to take it down."""

    sender: Any
    destination: str

    def wire(self) -> list[bytes]:
        raise NotImplementedError

    async def drop(self) -> None:
        raise NotImplementedError

    async def subscribe_silent(self) -> None:
        """Subscribe a handler at ``destination`` that never replies."""
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class MemoryHarness(Harness):
    async def setup(self) -> None:
        self.broker = MemoryBroker()
        self.sender = MemoryTransport(self.broker, agent_id="sender")
        self.receiver = MemoryTransport(self.broker, agent_id="receiver")
        self.destination = "results"
        await self.sender.start()
        await self.receiver.start()

    def wire(self) -> list[bytes]:
        return [r.body for r in self.broker.wire if r.sender == "sender"]

    async def drop(self) -> None:
        self.sender.simulate_drop()

    async def subscribe_silent(self) -> None:
        async def handler(delivery: Delivery) -> None:
            await self.receiver.acknowledge(delivery)  # no-op for request-reply calls

        await self.receiver.subscribe(self.destination, handler)

    async def close(self) -> None:
        await self.sender.close()
        await self.receiver.close()


class RabbitHarness(Harness):
    async def setup(self) -> None:
        self.amqp = FakeAmqp()

        async def no_wait(_: float) -> None:
            await asyncio.sleep(0.01)  # keep reconnect attempts short but not busy

        self.sender = RabbitMQTransport(
            url="amqp://guest:guest@localhost:5672/",
            connect=self.amqp.connect,
            poll_interval=0.01,
            sleep=no_wait,
        )
        self.destination = "results"
        await self.sender.start()

    def wire(self) -> list[bytes]:
        return list(self.amqp.wire)

    async def drop(self) -> None:
        self.amqp.drop()

    async def subscribe_silent(self) -> None:
        # Nobody consumes das.results, so a request sits unanswered in the queue.
        assert QUEUE_RESULTS in self.amqp.queues

    async def close(self) -> None:
        await self.sender.close()


class GrpcHarness(Harness):
    async def setup(self) -> None:
        self.endpoints: dict[str, str] = {}
        self.received: list[bytes] = []

        async def resolve(agent_id: str) -> str | None:
            return self.endpoints.get(agent_id)

        self.sender = GrpcTransport(
            agent_id="sender", bind="127.0.0.1:0", resolver=resolve, request_reply_timeout=2
        )
        self.receiver = GrpcTransport(
            agent_id="receiver", bind="127.0.0.1:0", resolver=resolve, request_reply_timeout=2
        )
        for t in (self.sender, self.receiver):
            await t.start()
            self.endpoints[t.agent_id] = f"127.0.0.1:{t.bound_port}"
        self.destination = "receiver"

        async def record(delivery: Delivery) -> None:
            # Delivery.body is the envelope field of the received gRPC Frame.
            self.received.append(delivery.body)
            await self.receiver.acknowledge(delivery)

        await self.receiver.subscribe(self.destination, record)

    def wire(self) -> list[bytes]:
        return list(self.received)

    async def drop(self) -> None:
        await self.sender.close()

    async def subscribe_silent(self) -> None:
        pass  # the recording handler acknowledges (a no-op for calls) and returns no reply

    async def close(self) -> None:
        await self.sender.close()
        await self.receiver.close()


HARNESSES = {"memory": MemoryHarness, "rabbitmq": RabbitHarness, "grpc": GrpcHarness}


@pytest.fixture(params=list(HARNESSES))
async def harness(request: pytest.FixtureRequest) -> AsyncIterator[Harness]:
    h = HARNESSES[request.param]()
    await h.setup()
    yield h
    await h.close()


async def _wait_for(cond: Callable[[], bool], timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.005)


# --------------------------------------------------------------------------- wire bytes (9.3)


async def test_publish_wire_bytes_equal_codec_encode(harness: Harness) -> None:
    if isinstance(harness, MemoryHarness):
        await harness.subscribe_silent()
    for msg in (heartbeat(), control()):
        await harness.sender.publish(harness.destination, msg)
    await _wait_for(lambda: len(harness.wire()) == 2)
    assert harness.wire() == [CODEC.encode(heartbeat()), CODEC.encode(control())]


async def test_request_wire_bytes_equal_codec_encode(harness: Harness) -> None:
    await harness.subscribe_silent()
    with pytest.raises(RequestTimeout):
        await harness.sender.request_reply(harness.destination, control(7), timeout=0.2)
    assert harness.wire() == [CODEC.encode(control(7))]


async def test_same_message_byte_identical_across_adapters() -> None:
    """One Message published over every adapter yields one set of bytes (9.3)."""
    msg = heartbeat(42)
    seen: dict[str, list[bytes]] = {}
    for name, cls in HARNESSES.items():
        h = cls()
        await h.setup()
        try:
            if isinstance(h, MemoryHarness):
                await h.subscribe_silent()
            await h.sender.publish(h.destination, msg)
            await _wait_for(lambda h=h: len(h.wire()) == 1)
            seen[name] = h.wire()
        finally:
            await h.close()
    assert {tuple(v) for v in seen.values()} == {(CODEC.encode(msg),)}, seen


# --------------------------------------------------------------------------- request-reply timeout (8.7)


async def test_request_reply_timeout_names_destination(harness: Harness) -> None:
    await harness.subscribe_silent()
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(RequestTimeout) as info:
        await harness.sender.request_reply(harness.destination, control(), timeout=0.2)
    elapsed = loop.time() - started
    assert info.value.destination == harness.destination
    assert info.value.timeout == pytest.approx(0.2)
    assert harness.destination in str(info.value)
    assert 0.15 <= elapsed < 1.5


# --------------------------------------------------------------------------- down (8.8)


async def test_publish_and_request_while_down_raise_unavailable(harness: Harness) -> None:
    await harness.drop()
    with pytest.raises(TransportUnavailable):
        await harness.sender.publish(harness.destination, heartbeat())
    with pytest.raises(TransportUnavailable):
        await harness.sender.request_reply(harness.destination, control(), timeout=0.5)
    await asyncio.sleep(0.02)
    assert harness.wire() == []  # nothing was sent


@pytest.mark.parametrize("name", list(HARNESSES))
async def test_publish_before_start_raises_unavailable(name: str) -> None:
    h = HARNESSES[name]()
    await h.setup()
    try:
        fresh = {
            "memory": lambda: MemoryTransport(MemoryBroker()),
            "rabbitmq": lambda: RabbitMQTransport(url="amqp://x/", connect=FakeAmqp().connect),
            "grpc": lambda: GrpcTransport(agent_id="a", bind="127.0.0.1:0", resolver=lambda _: None),
        }[name]()
        with pytest.raises(TransportUnavailable):
            await fresh.publish(h.destination, heartbeat())
        with pytest.raises(TransportUnavailable):
            await fresh.request_reply(h.destination, heartbeat(), timeout=0.5)
    finally:
        await h.close()


# --------------------------------------------------------------------------- http / kafka plug-ins (15.1, 15.2)


class DummyHttpTransport(MemoryTransport):
    """Stand-in for a third-party HTTP adapter; implements the four operations."""

    name: ClassVar[str] = "http"


class DummyKafkaTransport(MemoryTransport):
    """Stand-in for a third-party Kafka adapter; implements the four operations."""

    name: ClassVar[str] = "kafka"


def _installed_plus(*extra: EntryPoint) -> Callable[[], list[EntryPoint]]:
    """Installed das.transports entry points plus third-party ones (no code changes)."""
    shipped = [
        EntryPoint("rabbitmq", "das.transport.rabbitmq:RabbitMQTransport", ENTRY_POINT_GROUP),
        EntryPoint("grpc", "das.transport.grpc_rpc:GrpcTransport", ENTRY_POINT_GROUP),
    ]
    return lambda: [*shipped, *extra]


PLUGINS = _installed_plus(
    EntryPoint("http", f"{__name__}:DummyHttpTransport", ENTRY_POINT_GROUP),
    EntryPoint("kafka", f"{__name__}:DummyKafkaTransport", ENTRY_POINT_GROUP),
)


def test_shipped_entry_points_are_installed() -> None:
    # The installed package metadata registers rabbitmq and grpc (v1, Req 15.3) and http (Phase 7 Req 2.1);
    # kafka joins in wave 10.
    assert available_transports() == ["grpc", "http", "rabbitmq"]


@pytest.mark.parametrize(
    ("name", "cls"), [("http", DummyHttpTransport), ("kafka", DummyKafkaTransport)]
)
async def test_plugin_adapter_loads_and_exchanges_messages(name: str, cls: type) -> None:
    assert available_transports(PLUGINS) == ["grpc", "http", "kafka", "rabbitmq"]
    adapter = load_transport(name, PLUGINS)
    assert adapter is cls and adapter.name == name
    for op in TRANSPORT_OPERATIONS:
        assert _signature(adapter, op) == _signature(Transport, op)

    # Use the loaded class exactly as agent startup would: build, start, exchange.
    broker = MemoryBroker()
    sender, receiver = adapter(broker, agent_id="s"), adapter(broker, agent_id="r")
    assert isinstance(sender, Transport)
    await sender.start()
    await receiver.start()
    got: list[TypedMessage | None] = []

    async def handler(delivery: Delivery) -> TypedMessage | None:
        got.append(delivery.message)
        await receiver.acknowledge(delivery)
        return control(9)

    await receiver.subscribe("results", handler)
    await sender.publish("results", heartbeat())
    await broker.settle()
    assert await sender.request_reply("results", control(1), timeout=1) == control(9)
    assert got == [heartbeat(), control(1)]
    await sender.close()
    await receiver.close()
