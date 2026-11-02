"""In-memory Transport for tests (task 8.1).

Several :class:`MemoryTransport` instances share one :class:`MemoryBroker`, so a
Manager and Workers built in one test can exchange Messages. The fake follows
the broker semantics the design relies on:

- Every destination is a queue with competing consumers: each Message goes to
  one subscriber at a time (round robin) and leaves the queue only on ACK.
- ``REQUEUE`` returns the Message to the front of its queue; ``REJECT`` routes
  it to the dead-letter destinations (default ``"dlq"``).
- ``delivery_count`` counts earlier deliveries returned without an ACK (requeue
  or connection drop), like RabbitMQ's ``x-delivery-count``. An optional
  per-queue ``delivery_limit`` dead-letters a Message once it has been returned
  more than ``delivery_limit`` times (the quorum-queue policy, Req 6.6).
- ``simulate_drop`` closes a transport's connection: its unacked Messages
  return to their queues (Req 8.9) and ``publish`` / ``request_reply`` raise
  :class:`TransportUnavailable` (Req 8.8) until ``simulate_reconnect``, which
  re-establishes every subscription before delivering again (Req 8.6).
- Every Message is encoded with :class:`MessageCodec` and decoded on receipt,
  so the bytes on the "wire" are inspectable through ``MemoryBroker.wire``.

Request-reply: the destination's subscriber handler returns the reply Message
(see :mod:`das.transport.base`).
"""

from __future__ import annotations

import asyncio
import itertools
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import ClassVar

from das.clock import Clock, SystemClock
from das.codec.message_codec import MessageCodec, ParseError, TypedMessage
from das.transport.base import (
    DEFAULT_REQUEST_REPLY_TIMEOUT,
    Delivery,
    Handler,
    Outcome,
    PublishNotConfirmed,
    ReconnectPolicy,
    RequestTimeout,
    SubscribeOptions,
    Subscription,
    SubscriptionRegistry,
    TransportError,
    TransportUnavailable,
    validate_request_reply_timeout,
)

DEAD_LETTER_REASON_HEADER = "x-death-reason"
ORIGINAL_DESTINATION_HEADER = "x-original-destination"
DELIVERY_COUNT_HEADER = "x-delivery-count"


@dataclass(frozen=True, slots=True)
class WireRecord:
    """Bytes handed to the in-memory wire by a transport."""

    sender: str
    destination: str
    body: bytes
    headers: Mapping[str, str]
    kind: str = "publish"  # publish | request | reply | raw


@dataclass(slots=True)
class _Entry:
    body: bytes
    headers: dict[str, str]
    returned: int = 0  # deliveries returned without ACK


@dataclass(slots=True)
class _Consumer:
    transport: MemoryTransport
    subscription: Subscription
    in_flight: int = 0

    def eligible(self) -> bool:
        return (
            self.subscription.active
            and self.transport.connected
            and self.transport.registry.is_open
        )

    def has_capacity(self) -> bool:
        limit = self.subscription.options.max_in_flight
        return limit is None or self.in_flight < limit


_token_ids = itertools.count(1)


@dataclass(slots=True, eq=False)
class _Token:
    queue: _Queue
    consumer: _Consumer
    entry: _Entry
    id: int = field(default_factory=lambda: next(_token_ids))
    settled: bool = False


@dataclass(frozen=True, slots=True)
class _ReplyToken:
    """Token of a request-reply delivery; acknowledging it is a no-op."""


@dataclass(slots=True)
class _Queue:
    name: str
    pending: deque[_Entry] = field(default_factory=deque)
    consumers: list[_Consumer] = field(default_factory=list)
    unacked: dict[int, _Token] = field(default_factory=dict)
    delivery_limit: int | None = None
    rr: int = 0


class MemoryBroker:
    """Shared in-memory broker connecting any number of :class:`MemoryTransport`."""

    def __init__(self, dead_letter_destinations: Iterable[str] = ("dlq",)) -> None:
        self.dead_letter_destinations: tuple[str, ...] = tuple(dead_letter_destinations)
        self.wire: list[WireRecord] = []
        self._queues: dict[str, _Queue] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    # ----------------------------------------------------------- inspection

    def _queue(self, name: str) -> _Queue:
        queue = self._queues.get(name)
        if queue is None:
            queue = self._queues[name] = _Queue(name)
        return queue

    def set_delivery_limit(self, destination: str, limit: int | None) -> None:
        """Dead-letter Messages of ``destination`` returned more than ``limit`` times."""
        if limit is not None and limit < 0:
            raise ValueError("delivery limit must be >= 0")
        self._queue(destination).delivery_limit = limit

    def pending(self, destination: str) -> list[bytes]:
        """Bodies waiting in ``destination`` (not delivered or returned)."""
        return [e.body for e in self._queue(destination).pending]

    def pending_headers(self, destination: str) -> list[dict[str, str]]:
        return [dict(e.headers) for e in self._queue(destination).pending]

    def pending_messages(self, destination: str, codec: MessageCodec | None = None) -> list[TypedMessage]:
        codec = codec or MessageCodec()
        return [codec.decode(body) for body in self.pending(destination)]

    def unacked_count(self, destination: str) -> int:
        return len(self._queue(destination).unacked)

    def wire_for(self, destination: str) -> list[WireRecord]:
        return [r for r in self.wire if r.destination == destination]

    def publish_raw(
        self, destination: str, body: bytes, headers: Mapping[str, str] | None = None
    ) -> None:
        """Enqueue arbitrary bytes, e.g. malformed Messages (Req 5.9, 9.5)."""
        self.wire.append(WireRecord("raw", destination, bytes(body), dict(headers or {}), "raw"))
        self._enqueue(destination, _Entry(bytes(body), dict(headers or {})))

    async def settle(self, timeout: float = 5.0) -> None:
        """Wait until every running handler has finished and no new one starts."""
        async with asyncio.timeout(timeout):
            while True:
                await asyncio.sleep(0)
                running = [t for t in self._tasks if not t.done()]
                if not running:
                    await asyncio.sleep(0)
                    if not any(not t.done() for t in self._tasks):
                        return
                    continue
                await asyncio.gather(*running, return_exceptions=True)

    # ----------------------------------------------------------- internals

    def _enqueue(self, destination: str, entry: _Entry) -> None:
        self._queue(destination).pending.append(entry)
        self._pump_all()

    def _add_consumer(self, destination: str, consumer: _Consumer) -> None:
        self._queue(destination).consumers.append(consumer)

    def _remove_consumer(self, destination: str, consumer: _Consumer) -> None:
        queue = self._queue(destination)
        if consumer in queue.consumers:
            queue.consumers.remove(consumer)

    def _drop_consumer(self, destination: str, consumer: _Consumer) -> None:
        """Connection loss: return the consumer's unacked Messages to the queue."""
        queue = self._queue(destination)
        self._remove_consumer(destination, consumer)
        lost = [t for t in queue.unacked.values() if t.consumer is consumer]
        # Return in reverse delivery order so appendleft restores the original order.
        for token in sorted(lost, key=lambda t: t.id, reverse=True):
            self._release(token)
            self._return(queue, token.entry)

    def _pump_all(self) -> None:
        for queue in list(self._queues.values()):
            self._pump(queue)

    def _pump(self, queue: _Queue) -> None:
        while queue.pending:
            consumer = self._next_consumer(queue)
            if consumer is None:
                return
            self._deliver(queue, consumer, queue.pending.popleft())

    def _next_consumer(self, queue: _Queue) -> _Consumer | None:
        count = len(queue.consumers)
        for offset in range(count):
            index = (queue.rr + offset) % count
            consumer = queue.consumers[index]
            if not (consumer.eligible() and consumer.has_capacity()):
                continue
            limiter = consumer.subscription.options.limiter
            if limiter is not None and not limiter.try_acquire():
                continue
            queue.rr = (index + 1) % count
            return consumer
        return None

    def _deliver(self, queue: _Queue, consumer: _Consumer, entry: _Entry) -> None:
        transport = consumer.transport
        token = _Token(queue=queue, consumer=consumer, entry=entry)
        queue.unacked[token.id] = token
        consumer.in_flight += 1
        delivery = transport._make_delivery(queue.name, entry, token)
        transport.events.append(("deliver", queue.name))
        task = asyncio.get_running_loop().create_task(
            transport._run_handler(consumer.subscription.handler, delivery)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _release(self, token: _Token) -> None:
        token.settled = True
        token.queue.unacked.pop(token.id, None)
        token.consumer.in_flight -= 1
        limiter = token.consumer.subscription.options.limiter
        if limiter is not None:
            limiter.release()

    def _settle(self, token: _Token, outcome: Outcome) -> bool:
        if token.settled:
            return False
        self._release(token)
        if outcome is Outcome.REQUEUE:
            self._return(token.queue, token.entry)
        elif outcome is Outcome.REJECT:
            self._dead_letter(token.entry, token.queue.name, "rejected")
        self._pump_all()
        return True

    def _return(self, queue: _Queue, entry: _Entry) -> None:
        entry.returned += 1
        if queue.delivery_limit is not None and entry.returned > queue.delivery_limit:
            self._dead_letter(entry, queue.name, "delivery_limit")
        else:
            queue.pending.appendleft(entry)

    def _dead_letter(self, entry: _Entry, source: str, reason: str) -> None:
        if source in self.dead_letter_destinations:
            return  # never loop dead letters back into a dead-letter queue
        headers = {
            **entry.headers,
            DEAD_LETTER_REASON_HEADER: reason,
            ORIGINAL_DESTINATION_HEADER: source,
            DELIVERY_COUNT_HEADER: str(entry.returned),
        }
        for destination in self.dead_letter_destinations:
            self._queue(destination).pending.append(_Entry(entry.body, dict(headers)))

    def _reply_consumer(self, destination: str) -> _Consumer | None:
        queue = self._queue(destination)
        count = len(queue.consumers)
        for offset in range(count):
            index = (queue.rr + offset) % count
            consumer = queue.consumers[index]
            if consumer.eligible():
                queue.rr = (index + 1) % count
                return consumer
        return None


class MemoryTransport:
    """In-memory :class:`~das.transport.base.Transport` backed by a :class:`MemoryBroker`."""

    name: ClassVar[str] = "memory"

    def __init__(
        self,
        broker: MemoryBroker,
        *,
        agent_id: str = "memory",
        codec: MessageCodec | None = None,
        request_reply_timeout: float = DEFAULT_REQUEST_REPLY_TIMEOUT,
        reconnect_policy: ReconnectPolicy | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.broker = broker
        self.agent_id = agent_id
        self.codec = codec or MessageCodec()
        self.request_reply_timeout = validate_request_reply_timeout(request_reply_timeout)
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self.clock: Clock = clock or SystemClock()
        self.registry = SubscriptionRegistry()
        self.connected = False
        #: ("subscribe" | "deliver" | "drop" | "resubscribe" | "restored", destination)
        self.events: list[tuple[str, str]] = []
        self.handler_errors: list[BaseException] = []
        self.stale_acknowledgements = 0
        self._consumers: dict[int, _Consumer] = {}
        self._publish_failures: deque[str] = deque()

    # ----------------------------------------------------------- lifecycle

    async def start(self) -> None:
        self.connected = True

    async def close(self) -> None:
        for subscription in self.registry.active():
            consumer = self._consumers.pop(subscription.id, None)
            if consumer is not None:
                self.broker._drop_consumer(subscription.destination, consumer)
            self.registry.remove(subscription)
            await subscription.cancel()
        self.connected = False
        self.broker._pump_all()

    # ----------------------------------------------------------- operations

    async def publish(
        self,
        destination: str,
        message: TypedMessage,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._require_connected()
        body = self.codec.encode(message)
        if self._publish_failures:
            raise PublishNotConfirmed(destination, self._publish_failures.popleft())
        hdrs = _check_headers(headers)
        self.broker.wire.append(WireRecord(self.agent_id, destination, body, hdrs))
        self.broker._enqueue(destination, _Entry(body, dict(hdrs)))

    async def subscribe(
        self,
        destination: str,
        handler: Handler,
        options: SubscribeOptions | None = None,
    ) -> Subscription:
        self._require_connected()
        subscription = Subscription(
            destination, handler, options or SubscribeOptions(), on_cancel=self._unsubscribe
        )
        self.registry.add(subscription)
        self._attach(subscription)
        self.events.append(("subscribe", destination))
        self.broker._pump_all()
        return subscription

    async def acknowledge(self, delivery: Delivery, outcome: Outcome = Outcome.ACK) -> None:
        token = delivery.token
        if isinstance(token, _ReplyToken):
            return
        if not isinstance(token, _Token):
            raise ValueError("delivery was not produced by a MemoryTransport")
        if token.consumer.transport is not self:
            raise ValueError("delivery belongs to another transport")
        self._require_connected()
        if not self.broker._settle(token, outcome):
            # Already settled, or lost when the connection dropped.
            self.stale_acknowledgements += 1

    async def request_reply(
        self,
        destination: str,
        message: TypedMessage,
        timeout: float | None = None,
    ) -> TypedMessage:
        self._require_connected()
        limit = self.request_reply_timeout if timeout is None else float(timeout)
        body = self.codec.encode(message)
        self.broker.wire.append(WireRecord(self.agent_id, destination, body, {}, "request"))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit
        consumer = self.broker._reply_consumer(destination)
        if consumer is None:
            await asyncio.sleep(limit)
            raise RequestTimeout(destination, limit)
        responder = consumer.transport
        delivery = responder._make_delivery(destination, _Entry(body, {}), _ReplyToken())
        try:
            async with asyncio.timeout(limit):
                reply = await consumer.subscription.handler(delivery)
        except TimeoutError:
            raise RequestTimeout(destination, limit) from None
        except Exception as exc:
            raise TransportError(f"request-reply handler at {destination!r} failed: {exc}") from exc
        if reply is None:
            await asyncio.sleep(max(0.0, deadline - loop.time()))
            raise RequestTimeout(destination, limit)
        reply_body = responder.codec.encode(reply)
        self.broker.wire.append(
            WireRecord(responder.agent_id, destination, reply_body, {}, "reply")
        )
        return self.codec.decode(reply_body)

    # ----------------------------------------------------------- simulation

    def fail_next_publishes(self, count: int, reason: str = "broker nack") -> None:
        """Make the next ``count`` publishes raise :class:`PublishNotConfirmed`."""
        self._publish_failures.extend([reason] * count)

    def simulate_drop(self) -> None:
        """Lose the connection: unacked Messages return to their queues (Req 8.9)."""
        if not self.connected:
            return
        self.connected = False
        self.registry.mark_down()
        for subscription in self.registry.active():
            consumer = self._consumers.pop(subscription.id, None)
            if consumer is not None:
                self.broker._drop_consumer(subscription.destination, consumer)
        self.events.append(("drop", ""))
        self.broker._pump_all()

    async def simulate_reconnect(self) -> None:
        """Reconnect and re-establish every subscription before delivering (Req 8.6)."""
        if self.connected and self.registry.is_open:
            return
        self.connected = True
        await self.registry.restore(self._resubscribe)
        self.events.append(("restored", ""))
        self.broker._pump_all()

    async def reconnect_with_backoff(self, failures: int = 0) -> list[float]:
        """Reconnect after ``failures`` failed attempts, sleeping the policy delays.

        Returns the delays slept (one per attempt, ``failures + 1`` in total).
        """
        delays: list[float] = []
        for attempt in range(1, failures + 2):
            delay = self.reconnect_policy.delay(attempt)
            await self.clock.sleep(delay)
            delays.append(delay)
        await self.simulate_reconnect()
        return delays

    @property
    def subscriptions(self) -> list[Subscription]:
        return self.registry.active()

    def in_flight(self) -> int:
        """Unsettled deliveries currently held by this transport's subscriptions."""
        return sum(c.in_flight for c in self._consumers.values())

    async def ready_count(self, destination: str) -> int:
        """``QueueInspector``: Messages waiting in ``destination`` (not delivered)."""
        self._require_connected()
        return len(self.broker.pending(destination))

    # ----------------------------------------------------------- internals

    def _require_connected(self) -> None:
        if not self.connected:
            raise TransportUnavailable(self.name, f"agent {self.agent_id!r} is disconnected")

    def _attach(self, subscription: Subscription) -> None:
        consumer = _Consumer(self, subscription)
        self._consumers[subscription.id] = consumer
        self.broker._add_consumer(subscription.destination, consumer)

    async def _resubscribe(self, subscription: Subscription) -> None:
        self._attach(subscription)
        self.events.append(("resubscribe", subscription.destination))

    async def _unsubscribe(self, subscription: Subscription) -> None:
        self.registry.remove(subscription)
        consumer = self._consumers.pop(subscription.id, None)
        if consumer is not None:
            # Like basic.cancel: stop new deliveries; unacked ones may still be settled.
            self.broker._remove_consumer(subscription.destination, consumer)

    def _make_delivery(self, destination: str, entry: _Entry, token: object) -> Delivery:
        message: TypedMessage | None = None
        error: ParseError | None = None
        try:
            message = self.codec.decode(entry.body)
        except ParseError as exc:
            error = exc
        return Delivery(
            message=message,
            destination=destination,
            redelivered=entry.returned > 0,
            delivery_count=entry.returned,
            headers=dict(entry.headers),
            token=token,
            body=entry.body,
            parse_error=error,
        )

    async def _run_handler(self, handler: Handler, delivery: Delivery) -> None:
        try:
            await handler(delivery)
        except Exception as exc:  # recorded for assertions; delivery stays unacked
            self.handler_errors.append(exc)


def _check_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    result = dict(headers or {})
    for key, value in result.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("transport headers must map str to str")
    return result


__all__ = [
    "DEAD_LETTER_REASON_HEADER",
    "DELIVERY_COUNT_HEADER",
    "ORIGINAL_DESTINATION_HEADER",
    "MemoryBroker",
    "MemoryTransport",
    "WireRecord",
]
