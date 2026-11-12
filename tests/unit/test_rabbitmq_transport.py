"""Unit tests for the RabbitMQ adapter with an in-process fake of aio-pika (task 8.5).

Broker behaviour (quorum delivery limits, real requeue on drop) is covered by the
integration tests in task 8.8; these tests check the adapter's own logic:
destination mapping, the das-jobs policy, header extraction, limiter-gated
pull polling, and error mapping for publish.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import aiormq
import httpx
import pytest
from pamqp import commands as spec

from das.codec.envelope import HeartbeatMessage
from das.codec.message_codec import MessageCodec
from das.models.results import HeartbeatPayload
from das.transport.base import (
    PRIORITY_HEADER,
    Delivery,
    Outcome,
    PublishNotConfirmed,
    ReconnectPolicy,
    SubscribeOptions,
    TransportUnavailable,
)
from das.transport.rabbitmq import (
    EXCHANGE_DLX,
    EXCHANGE_JOBS,
    HIGH_AMQP_PRIORITY,
    QUEUE_DLQ,
    QUEUE_DLQ_EVENTS,
    QUEUE_RESULTS,
    ManagementAPIError,
    RabbitMQTransport,
    delivery_count_from,
    destination_for_queue,
    extract_headers,
    jobs_policy_body,
    policy_request,
    resolve_destination,
)

# --------------------------------------------------------------------------- fakes


class _Callbacks:
    def __init__(self, owner: object) -> None:
        self.owner = owner
        self._items: list[Callable[..., Any]] = []

    def add(self, cb: Callable[..., Any]) -> None:
        self._items.append(cb)

    def fire(self, exc: BaseException | None = None) -> None:
        for cb in list(self._items):
            cb(self.owner, exc)


class FakeIncoming:
    def __init__(self, broker: FakeBroker, queue: str, body: bytes, props: dict[str, Any]) -> None:
        self.broker = broker
        self.queue = queue
        self.body = body
        self.headers = props.get("headers") or {}
        self.redelivered = props.get("redelivered", False)
        self.reply_to = props.get("reply_to")
        self.correlation_id = props.get("correlation_id")

    async def ack(self) -> None:
        self.broker.settlements.append(("ack", self.queue, self.body))

    async def nack(self, requeue: bool = True) -> None:
        self.broker.settlements.append(("nack", self.queue, self.body))
        if requeue:
            self.broker.queues[self.queue].messages.appendleft(self)

    async def reject(self, requeue: bool = False) -> None:
        self.broker.settlements.append(("reject", self.queue, self.body))


class FakeQueue:
    def __init__(self, broker: FakeBroker, name: str) -> None:
        self.broker = broker
        self.name = name
        self.messages: deque[FakeIncoming] = deque()
        self.get_calls = 0

    async def bind(self, exchange: Any, routing_key: str = "") -> None:
        self.broker.bindings.add((exchange.name, routing_key, self.name))

    async def get(self, *, no_ack: bool = False, fail: bool = True, timeout: float = 5) -> Any:
        self.get_calls += 1
        if self.broker.get_failures:
            exc, drop = self.broker.get_failures.popleft()
            if drop:
                self.broker.drop()  # the broker closes the connection mid basic.get
            raise exc
        if self.broker.down:
            raise aiormq.exceptions.ChannelInvalidStateError("closed")
        return self.messages.popleft() if self.messages else None

    async def consume(self, callback: Callable[..., Any], no_ack: bool = False) -> str:
        self.broker.consumers[self.name] = callback
        return "ctag"


class FakeExchange:
    def __init__(self, broker: FakeBroker, name: str, kind: str = "direct") -> None:
        self.broker = broker
        self.name = name
        self.kind = kind

    async def publish(
        self, message: Any, routing_key: str, *, mandatory: bool = True, timeout: Any = None
    ) -> Any:
        if self.broker.down:
            raise aiormq.exceptions.ChannelInvalidStateError("closed")
        if self.broker.publish_error is not None:
            raise self.broker.publish_error
        self.broker.published.append((self.name, routing_key, message, mandatory, timeout))
        targets = [q for (ex, rk, q) in self.broker.bindings if ex == self.name and rk == routing_key]
        if self.name == "":
            targets = [routing_key] if routing_key in self.broker.queues else []
        for target in targets:
            props = {
                "headers": dict(message.headers or {}),
                "reply_to": message.reply_to,
                "correlation_id": message.correlation_id,
            }
            incoming = FakeIncoming(self.broker, target, message.body, props)
            if target in self.broker.consumers:
                await self.broker.consumers[target](incoming)
            else:
                self.broker.queues[target].messages.append(incoming)
        return spec.Basic.Ack(delivery_tag=len(self.broker.published))


class FakeChannel:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.close_callbacks = _Callbacks(self)
        self.default_exchange = FakeExchange(broker, "")

    async def declare_exchange(self, name: str, kind: Any, durable: bool = False) -> FakeExchange:
        self.broker.exchanges[name] = (str(kind.value), durable)
        return FakeExchange(self.broker, name)

    async def declare_queue(
        self,
        name: str = "",
        *,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> FakeQueue:
        if not name:
            name = f"amq.gen-{len(self.broker.queues)}"
        self.broker.declarations.append((name, durable, dict(arguments or {})))
        return self.broker.queues.setdefault(name, FakeQueue(self.broker, name))


class FakeConnection:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.close_callbacks = _Callbacks(self)
        self.is_closed = False
        self.channels: list[FakeChannel] = []

    async def channel(self, publisher_confirms: bool = True) -> FakeChannel:
        assert publisher_confirms is True
        channel = FakeChannel(self.broker)
        self.channels.append(channel)
        return channel

    async def close(self) -> None:
        if not self.is_closed:
            self.is_closed = True
            self.close_callbacks.fire(None)


class FakeBroker:
    """Shared state of the fake aio-pika world."""

    def __init__(self) -> None:
        self.queues: dict[str, FakeQueue] = {}
        self.exchanges: dict[str, tuple[str, bool]] = {}
        self.bindings: set[tuple[str, str, str]] = set()
        self.declarations: list[tuple[str, bool, dict[str, Any]]] = []
        self.published: list[tuple[str, str, Any, bool, Any]] = []
        self.settlements: list[tuple[str, str, bytes]] = []
        self.consumers: dict[str, Callable[..., Any]] = {}
        self.connections: list[FakeConnection] = []
        self.publish_error: BaseException | None = None
        self.down = False
        self.refuse_connections = 0
        #: Errors the next ``queue.get`` calls raise, with whether to drop first.
        self.get_failures: deque[tuple[BaseException, bool]] = deque()

    async def connect(self, url: str, timeout: float | None = None) -> FakeConnection:
        if self.refuse_connections:
            self.refuse_connections -= 1
            raise ConnectionRefusedError("refused")
        connection = FakeConnection(self)
        self.connections.append(connection)
        return connection

    def drop(self) -> None:
        """Simulate the broker closing the connection."""
        connection = self.connections[-1]
        connection.is_closed = True
        connection.close_callbacks.fire(ConnectionResetError("reset"))

    def put(self, queue: str, body: bytes, **props: Any) -> None:
        self.queues[queue].messages.append(FakeIncoming(self, queue, body, props))


class Limiter:
    def __init__(self, size: int) -> None:
        self.size = size
        self.used = 0

    async def acquire(self) -> None:  # pragma: no cover - not used by the adapter
        raise NotImplementedError

    def try_acquire(self) -> bool:
        if self.used >= self.size:
            return False
        self.used += 1
        return True

    def release(self) -> None:
        assert self.used > 0, "released more slots than taken"
        self.used -= 1


def heartbeat(n: int = 1) -> HeartbeatMessage:
    return HeartbeatMessage(
        message_id=f"m-{n}",
        correlation_id="",
        timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        payload=HeartbeatPayload(agent_id="w1", role="worker", capabilities=["code"]),
    )


async def wait_until(cond: Callable[[], bool], timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.005)


@pytest.fixture
def broker() -> FakeBroker:
    return FakeBroker()


@pytest.fixture
async def transport(broker: FakeBroker):
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        await asyncio.sleep(0)

    t = RabbitMQTransport(
        url="amqp://guest:guest@localhost:5672/",
        connect=broker.connect,
        poll_interval=0.01,
        sleep=fake_sleep,
    )
    t.sleeps = sleeps  # type: ignore[attr-defined]
    yield t
    await t.close()


# --------------------------------------------------------------------------- pure helpers


def test_destination_mapping() -> None:
    jobs = resolve_destination("jobs/code")
    assert (jobs.exchange, jobs.routing_key, jobs.queue, jobs.capability) == (
        EXCHANGE_JOBS,
        "code",
        "das.jobs.code",
        "code",
    )
    assert resolve_destination("results").queue == QUEUE_RESULTS
    assert resolve_destination("dlq").queue == QUEUE_DLQ
    assert resolve_destination("dlq-events").queue == QUEUE_DLQ_EVENTS
    for fixed in ("results", "dlq", "dlq-events"):
        route = resolve_destination(fixed)
        assert route.exchange == "" and route.routing_key == route.queue
        assert destination_for_queue(route.queue) == fixed
    assert destination_for_queue("das.jobs.code") == "jobs/code"
    assert destination_for_queue("other") is None


@pytest.mark.parametrize("bad", ["jobs/", "jobs/bad cap", "jobs/-x", "unknown", "das.results"])
def test_destination_mapping_rejects_unknown(bad: str) -> None:
    with pytest.raises(ValueError):
        resolve_destination(bad)


@pytest.mark.parametrize(("max_attempts", "limit"), [(1, 0), (3, 2), (10, 9)])
def test_jobs_policy_body(max_attempts: int, limit: int) -> None:
    body = jobs_policy_body(max_attempts)
    assert body["pattern"] == r"^das\.jobs\."
    assert body["definition"] == {"delivery-limit": limit, "dead-letter-exchange": EXCHANGE_DLX}


@pytest.mark.parametrize("bad", [0, 11, True, 2.5])
def test_jobs_policy_body_rejects_out_of_range(bad: Any) -> None:
    with pytest.raises(ValueError):
        jobs_policy_body(bad)


def test_policy_request_url_and_credentials() -> None:
    url, auth = policy_request("http://mgmt:15672", "amqp://u:p@rabbit:5672/")
    assert url == "http://mgmt:15672/api/policies/%2F/das-jobs"
    assert auth == ("u", "p")
    url, auth = policy_request("https://admin:s3cr%40t@mgmt:15671/", "amqp://u:p@rabbit/prod")
    assert url == "https://mgmt:15671/api/policies/prod/das-jobs"
    assert auth == ("admin", "s3cr@t")
    _, auth = policy_request("http://mgmt:15672", "amqp://rabbit")
    assert auth == ("guest", "guest")


def test_extract_headers_and_delivery_count() -> None:
    raw = {
        "x-delivery-count": 2,
        "x-first-death-reason": b"delivery_limit",
        "x-first-death-queue": "das.jobs.code",
        "x-death": [{"count": 1}],
        "flag": True,
        "plain": "v",
    }
    headers = extract_headers(raw)
    assert headers["x-delivery-count"] == "2"
    assert headers["x-death-reason"] == "delivery_limit"
    assert headers["x-original-destination"] == "jobs/code"
    assert headers["flag"] == "true" and headers["plain"] == "v"
    assert "x-death" not in headers
    assert delivery_count_from(raw) == 2
    assert delivery_count_from({"x-delivery-count": b"3"}) == 3
    assert delivery_count_from({}) == 0
    assert delivery_count_from({"x-delivery-count": "junk"}) == 0


# --------------------------------------------------------------------------- policy over HTTP


async def test_apply_jobs_policy_puts_policy() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        t = RabbitMQTransport(
            url="amqp://u:p@rabbit:5672/",
            management_url="http://mgmt:15672",
            max_attempts=4,
            apply_policy=True,
            http_client=client,
        )
        await t.apply_jobs_policy()
    (request,) = seen
    assert request.method == "PUT"
    assert str(request.url) == "http://mgmt:15672/api/policies/%2F/das-jobs"
    assert json.loads(request.content)["definition"]["delivery-limit"] == 3
    assert request.headers["authorization"] == "Basic " + base64.b64encode(b"u:p").decode()


async def test_apply_jobs_policy_error_status() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(401)))
    t = RabbitMQTransport(url="amqp://rabbit/", management_url="http://mgmt", http_client=client)
    with pytest.raises(ManagementAPIError) as info:
        await t.apply_jobs_policy()
    assert info.value.status == 401
    await client.aclose()


def test_apply_policy_requires_management_url() -> None:
    with pytest.raises(ValueError):
        RabbitMQTransport(url="amqp://rabbit/", apply_policy=True)


# --------------------------------------------------------------------------- lifecycle and publish


async def test_start_declares_topology(transport: RabbitMQTransport, broker: FakeBroker) -> None:
    await transport.start()
    assert broker.exchanges[EXCHANGE_JOBS] == ("direct", True)
    assert broker.exchanges[EXCHANGE_DLX] == ("fanout", True)
    declared = {name: args for name, _, args in broker.declarations}
    for name in (QUEUE_RESULTS, QUEUE_DLQ, QUEUE_DLQ_EVENTS):
        assert declared[name]["x-queue-type"] == "quorum"
    assert {(EXCHANGE_DLX, "", QUEUE_DLQ), (EXCHANGE_DLX, "", QUEUE_DLQ_EVENTS)} <= broker.bindings


async def test_start_failure_raises_unavailable(broker: FakeBroker) -> None:
    broker.refuse_connections = 1
    t = RabbitMQTransport(url="amqp://rabbit/", connect=broker.connect)
    with pytest.raises(TransportUnavailable):
        await t.start()


async def test_publish_jobs_declares_quorum_queue_and_is_persistent(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    await transport.publish("jobs/code", heartbeat(), headers={"k": "v"})
    declared = {name: args for name, _, args in broker.declarations}
    assert declared["das.jobs.code"] == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": EXCHANGE_DLX,
    }
    assert (EXCHANGE_JOBS, "code", "das.jobs.code") in broker.bindings
    exchange, key, message, mandatory, _ = broker.published[-1]
    assert (exchange, key, mandatory) == (EXCHANGE_JOBS, "code", True)
    assert message.delivery_mode.value == 2
    assert message.headers == {"k": "v"}
    assert message.body == MessageCodec().encode(heartbeat())  # codec bytes on the wire
    assert broker.queues["das.jobs.code"].messages[0].body == message.body


# Priority (das-multi-tenant task 6.4; Phase 5 Req 7.5, 7.7). Broker ordering is covered by task 6.5 on a real broker.


def test_the_priority_header_and_the_amqp_value_are_the_documented_ones() -> None:
    assert PRIORITY_HEADER == "das-priority"
    assert HIGH_AMQP_PRIORITY == 9  # 5 to 9 are the "high" class of a RabbitMQ 4.1 quorum queue (G0a)


async def test_a_high_priority_header_becomes_amqp_priority_9_and_never_reaches_the_wire_as_a_header(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    await transport.publish("jobs/code", heartbeat(), headers={PRIORITY_HEADER: "high", "k": "v"})
    message = broker.published[-1][2]
    assert message.priority == HIGH_AMQP_PRIORITY
    assert message.headers == {"k": "v"}  # the reserved header is consumed by the adapter


@pytest.mark.parametrize(
    "headers",
    [None, {}, {"k": "v"}, {PRIORITY_HEADER: "normal"}, {PRIORITY_HEADER: "urgent"}, {PRIORITY_HEADER: ""}],
)
async def test_anything_but_high_leaves_the_message_as_it_is_today(
    transport: RabbitMQTransport, broker: FakeBroker, headers: dict[str, str] | None
) -> None:
    await transport.start()
    await transport.publish("jobs/code", heartbeat(), headers=headers)
    message = broker.published[-1][2]
    assert message.priority == 0  # what aio-pika sends for every message: the "normal" class
    assert PRIORITY_HEADER not in message.headers  # a reserved header never leaks, whatever its value


async def test_priority_changes_neither_the_body_nor_the_queue_declarations(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    await transport.publish("jobs/code", heartbeat(), headers={PRIORITY_HEADER: "high"})
    await transport.publish("jobs/code", heartbeat())
    high, normal = broker.published[-2][2], broker.published[-1][2]
    assert high.body == normal.body == MessageCodec().encode(heartbeat())
    for _, arguments in [(name, args) for name, _, args in broker.declarations]:
        assert "x-max-priority" not in arguments  # a quorum queue rejects it (G0a)


async def test_publish_while_disconnected_raises_unavailable(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    with pytest.raises(TransportUnavailable):
        await transport.publish("results", heartbeat())  # never started
    await transport.start()
    broker.down = True
    broker.drop()
    with pytest.raises(TransportUnavailable):
        await transport.publish("results", heartbeat())
    assert broker.published == []


async def test_publish_nack_raises_not_confirmed(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    broker.publish_error = aiormq.exceptions.DeliveryError(None, spec.Basic.Nack(delivery_tag=1))
    with pytest.raises(PublishNotConfirmed) as info:
        await transport.publish("results", heartbeat())
    assert info.value.destination == "results"
    broker.publish_error = TimeoutError()
    with pytest.raises(PublishNotConfirmed):
        await transport.publish("results", heartbeat())


# --------------------------------------------------------------------------- pull consumption


async def test_limiter_gates_polling_round_robin(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    limiter = Limiter(1)
    options = SubscribeOptions(limiter=limiter)
    await transport.subscribe("jobs/a", handler, options)
    await transport.subscribe("jobs/b", handler, options)
    codec = MessageCodec()
    for n in (1, 2):
        broker.put("das.jobs.a", codec.encode(heartbeat(n)))
        broker.put("das.jobs.b", codec.encode(heartbeat(n + 10)))

    await wait_until(lambda: len(received) == 1)
    gets = broker.queues["das.jobs.a"].get_calls + broker.queues["das.jobs.b"].get_calls
    await asyncio.sleep(0.08)  # several poll intervals with the limiter full
    assert len(received) == 1 and limiter.used == 1
    assert broker.queues["das.jobs.a"].get_calls + broker.queues["das.jobs.b"].get_calls == gets
    assert transport.in_flight() == 1

    await transport.acknowledge(received[0])
    await wait_until(lambda: len(received) == 2)
    # Round robin: the second fetch comes from the other Capability queue.
    assert {received[0].destination, received[1].destination} == {"jobs/a", "jobs/b"}
    assert limiter.used == 1
    assert broker.settlements[0][0] == "ack"


async def test_delivery_exposes_delivery_count_and_parse_error(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    await transport.subscribe("results", handler)
    broker.put(QUEUE_RESULTS, MessageCodec().encode(heartbeat()),
               headers={"x-delivery-count": 2}, redelivered=True)
    broker.put(QUEUE_RESULTS, b"not json")
    await wait_until(lambda: len(received) == 2)
    good, bad = received
    assert good.message == heartbeat() and good.delivery_count == 2 and good.redelivered
    assert good.headers["x-delivery-count"] == "2"
    assert bad.message is None and bad.parse_error is not None and bad.body == b"not json"
    await transport.acknowledge(bad, Outcome.REJECT)
    await transport.acknowledge(good, Outcome.REQUEUE)
    assert [s[0] for s in broker.settlements] == ["reject", "nack"]


async def test_max_in_flight_caps_unsettled(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    await transport.subscribe("results", handler, SubscribeOptions(max_in_flight=2))
    for n in range(4):
        broker.put(QUEUE_RESULTS, MessageCodec().encode(heartbeat(n)))
    await wait_until(lambda: len(received) == 2)
    await asyncio.sleep(0.05)
    assert len(received) == 2


async def test_drop_releases_slots_and_resubscribes_before_delivery(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    limiter = Limiter(1)
    await transport.subscribe("jobs/a", handler, SubscribeOptions(limiter=limiter))
    broker.put("das.jobs.a", MessageCodec().encode(heartbeat(1)))
    await wait_until(lambda: len(received) == 1)
    assert limiter.used == 1

    broker.refuse_connections = 2  # two failed attempts, then success
    broker.drop()
    assert not transport.connected and not transport.registry.is_open
    assert limiter.used == 0  # lost delivery's slot is released

    await wait_until(lambda: transport.connected and transport.registry.is_open)
    assert transport.sleeps == [1.0, 2.0, 4.0]  # type: ignore[attr-defined]
    assert len(transport.subscriptions) == 1
    # Acknowledging the delivery lost on the drop is a no-op.
    await transport.acknowledge(received[0])
    assert transport.stale_acknowledgements == 1 and broker.settlements == []

    broker.put("das.jobs.a", MessageCodec().encode(heartbeat(2)))
    await wait_until(lambda: len(received) == 2)
    await transport.acknowledge(received[1])
    assert limiter.used == 0


# --------------------------------------------------------------------------- poll errors (task 8.3)


def _connection_closed() -> aiormq.exceptions.ConnectionClosed:
    return aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED")


def _rabbitmq_errors(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r for r in caplog.records
        if r.name == "das.transport.rabbitmq" and r.levelno >= logging.ERROR
    ]


async def test_get_connection_error_returns_false_and_poller_survives(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    await transport.subscribe("results", handler)
    broker.get_failures.append((_connection_closed(), False))
    assert await transport.poll_once() is False  # not TypeError
    assert transport.in_flight() == 0

    broker.get_failures.append((_connection_closed(), False))
    await wait_until(lambda: not broker.get_failures)  # the poll task hit it
    broker.put(QUEUE_RESULTS, MessageCodec().encode(heartbeat(1)))
    await wait_until(lambda: len(received) == 1)
    assert transport._poll_task is not None and not transport._poll_task.done()


async def test_get_timeout_returns_false_without_logging(
    transport: RabbitMQTransport, broker: FakeBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="das.transport.rabbitmq")
    await transport.start()

    async def handler(delivery: Delivery) -> None:
        pass

    await transport.subscribe("results", handler)
    broker.get_failures.append((TimeoutError(), False))
    assert await transport.poll_once() is False
    assert transport.in_flight() == 0
    assert _rabbitmq_errors(caplog) == []


async def test_delivery_resumes_after_poll_error_and_reconnect(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    await transport.subscribe("jobs/a", handler)
    # The broker closes the connection while basic.get is pending.
    broker.get_failures.append((_connection_closed(), True))
    await wait_until(lambda: not broker.get_failures)
    await wait_until(lambda: transport.connected and transport.registry.is_open)
    assert len(broker.connections) == 2 and len(transport.subscriptions) == 1

    await transport.publish("jobs/a", heartbeat(1))
    broker.put("das.jobs.a", MessageCodec().encode(heartbeat(2)))
    await wait_until(lambda: len(received) == 2)
    assert [d.message for d in received] == [heartbeat(1), heartbeat(2)]


async def test_reconnect_restarts_ended_poller(
    transport: RabbitMQTransport, broker: FakeBroker
) -> None:
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    await transport.subscribe("results", handler)
    old_task = transport._poll_task
    assert old_task is not None
    old_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await old_task

    broker.drop()
    await wait_until(lambda: transport.connected and transport.registry.is_open)
    broker.put(QUEUE_RESULTS, MessageCodec().encode(heartbeat(1)))
    await wait_until(lambda: len(received) == 1)
    assert transport._poll_task is not old_task and not transport._poll_task.done()


class _FailOnceLimiter(Limiter):
    """Raises from the first ``try_acquire`` (an unexpected error inside a pass)."""

    def __init__(self, size: int) -> None:
        super().__init__(size)
        self.failed = False

    def try_acquire(self) -> bool:
        if not self.failed:
            self.failed = True
            raise RuntimeError("limiter broke")
        return super().try_acquire()


async def test_poll_loop_survives_failed_pass_and_stays_cancellable(
    transport: RabbitMQTransport, broker: FakeBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="das.transport.rabbitmq")
    await transport.start()
    received: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)

    limiter = _FailOnceLimiter(1)
    broker.put(QUEUE_RESULTS, MessageCodec().encode(heartbeat(1)))
    await transport.subscribe("results", handler, SubscribeOptions(limiter=limiter))
    await wait_until(lambda: len(received) == 1)
    assert limiter.failed
    assert [r.getMessage() for r in _rabbitmq_errors(caplog)] == ["rabbitmq poll pass failed"]

    task = transport._poll_task
    assert task is not None and not task.done()
    task.cancel()
    async with asyncio.timeout(1):
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert task.cancelled()


async def test_reconnect_policy_from_config_cap() -> None:
    t = RabbitMQTransport(url="amqp://rabbit/", reconnect_policy=ReconnectPolicy(2))
    assert [t.reconnect_policy.delay(k) for k in (1, 2, 3)] == [1.0, 2.0, 2.0]


async def test_request_reply_round_trip(broker: FakeBroker) -> None:
    server = RabbitMQTransport(url="amqp://rabbit/", connect=broker.connect, poll_interval=0.01)
    client = RabbitMQTransport(url="amqp://rabbit/", connect=broker.connect, poll_interval=0.01)
    await server.start()
    await client.start()

    async def responder(delivery: Delivery) -> HeartbeatMessage:
        return heartbeat(99)

    await server.subscribe("results", responder)
    reply = await client.request_reply("results", heartbeat(1), timeout=2)
    assert reply == heartbeat(99)
    await wait_until(lambda: ("ack", QUEUE_RESULTS, MessageCodec().encode(heartbeat(1)))
                     in broker.settlements)
    await client.close()
    await server.close()


def test_entry_point_loads_adapter() -> None:
    from importlib.metadata import EntryPoint

    from das.transport.loader import ENTRY_POINT_GROUP, load_transport

    # Same target as the pyproject entry point.
    ep = EntryPoint("rabbitmq", "das.transport.rabbitmq:RabbitMQTransport", ENTRY_POINT_GROUP)
    assert load_transport("rabbitmq", source=lambda: [ep]) is RabbitMQTransport
