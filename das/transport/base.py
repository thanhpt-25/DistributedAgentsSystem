"""Transport abstraction shared by every adapter (Requirements 8, 15.4).

Agent logic depends only on the :class:`Transport` protocol defined here and on
:class:`~das.codec.MessageCodec`. Adapters (RabbitMQ, gRPC, and the in-memory
test fake) implement the same four operations with identical signatures
(Req 8.1): ``publish``, ``subscribe``, ``acknowledge``, and ``request_reply``.

This module also holds the pieces every adapter shares:

- :class:`ReconnectPolicy`: the pure reconnect delay schedule (Req 8.6).
- :class:`SubscriptionRegistry`: remembers active subscriptions and gates
  delivery so that, after a reconnect, every subscription is re-established
  before any further Message reaches a handler (Req 8.6).
- Typed errors: :class:`TransportUnavailable` (Req 8.8),
  :class:`PublishNotConfirmed` (Req 4.8), :class:`RequestTimeout` (Req 8.7).

Request-reply convention: a handler subscribed to a destination that receives
request-reply calls returns the reply Message. A handler that returns ``None``
sends no reply, so the caller eventually gets :class:`RequestTimeout`.
Handlers on ordinary (queue) destinations return ``None`` and settle the
delivery through ``acknowledge``.
"""

from __future__ import annotations

import asyncio
import enum
import itertools
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Final, Protocol, runtime_checkable

if TYPE_CHECKING:
    from das.codec.message_codec import ParseError, TypedMessage

#: The four operations every Transport must implement (Req 8.1, 15.5).
TRANSPORT_OPERATIONS: Final = ("publish", "subscribe", "acknowledge", "request_reply")

DEFAULT_MAX_RECONNECT_INTERVAL: Final = 30.0
MIN_MAX_RECONNECT_INTERVAL: Final = 1.0
MAX_MAX_RECONNECT_INTERVAL: Final = 300.0
INITIAL_RECONNECT_INTERVAL: Final = 1.0

DEFAULT_REQUEST_REPLY_TIMEOUT: Final = 30.0
MIN_REQUEST_REPLY_TIMEOUT: Final = 1.0
MAX_REQUEST_REPLY_TIMEOUT: Final = 300.0


# --------------------------------------------------------------------------- errors


class TransportError(Exception):
    """Base class for runtime Transport errors."""


class TransportUnavailable(TransportError):
    """The Transport connection is down; nothing was sent (Req 8.8)."""

    def __init__(self, transport: str, reason: str = "connection is down") -> None:
        self.transport = transport
        self.reason = reason
        super().__init__(f"transport {transport!r} is unavailable: {reason}")


class PublishNotConfirmed(TransportError):
    """The broker rejected the publish or did not confirm it in time (Req 4.8)."""

    def __init__(self, destination: str, reason: str = "publish not confirmed") -> None:
        self.destination = destination
        self.reason = reason
        super().__init__(f"publish to {destination!r} not confirmed: {reason}")


class RequestTimeout(TransportError):
    """No reply arrived within the request-reply timeout (Req 8.7)."""

    def __init__(self, destination: str, timeout: float) -> None:
        self.destination = destination
        self.timeout = timeout
        super().__init__(f"request-reply to {destination!r} timed out after {timeout:g} s")


# --------------------------------------------------------------------------- deliveries


class Outcome(enum.Enum):
    """How a received Message is settled through ``acknowledge``."""

    ACK = "ack"  # processed; remove from the queue
    REQUEUE = "requeue"  # return to the queue for redelivery
    REJECT = "reject"  # drop without requeue -> dead-letter (Req 5.9, 9.5)


@dataclass(frozen=True, slots=True)
class Delivery:
    """One received Message handed to a subscriber.

    ``message`` is the decoded Message, or ``None`` when the bytes failed to
    decode, in which case ``parse_error`` holds the codec error. Handlers must
    settle such deliveries with :attr:`Outcome.REJECT` (Req 5.9, 9.5).
    ``delivery_count`` is the number of earlier deliveries of this Message that
    were returned without an ACK (RabbitMQ ``x-delivery-count``; 0 if unknown).
    """

    message: TypedMessage | None
    destination: str
    redelivered: bool
    delivery_count: int
    headers: Mapping[str, str]
    token: object  # opaque, adapter-specific
    body: bytes = b""
    parse_error: ParseError | None = None


#: A subscriber callback. Returns the reply Message for request-reply calls,
#: otherwise ``None``.
Handler = Callable[[Delivery], Awaitable["TypedMessage | None"]]


@runtime_checkable
class InFlightLimiterLike(Protocol):
    """Minimal limiter interface that several subscriptions can share.

    The concrete implementation is ``das.worker.limiter.InFlightLimiter``
    (a semaphore sized by ``concurrency_limit``). A slot is taken before a
    Message is delivered and released when the delivery is settled or lost.
    """

    async def acquire(self) -> None: ...

    def try_acquire(self) -> bool:
        """Take a slot without waiting; return False when none is free."""
        ...

    def release(self) -> None: ...


@dataclass(frozen=True, slots=True)
class SubscribeOptions:
    """Per-subscription delivery limits.

    ``max_in_flight`` caps unsettled deliveries for this subscription;
    ``limiter`` is shared across subscriptions to cap their total (Req 5.3).
    """

    max_in_flight: int | None = None
    limiter: InFlightLimiterLike | None = None

    def __post_init__(self) -> None:
        if self.max_in_flight is not None and (
            isinstance(self.max_in_flight, bool)
            or not isinstance(self.max_in_flight, int)
            or self.max_in_flight < 1
        ):
            raise ValueError("max_in_flight must be a positive integer or None")


_subscription_ids = itertools.count(1)


class Subscription:
    """Handle for an active subscription; ``cancel`` stops further deliveries."""

    def __init__(
        self,
        destination: str,
        handler: Handler,
        options: SubscribeOptions,
        on_cancel: Callable[[Subscription], Awaitable[None]] | None = None,
    ) -> None:
        self.id: int = next(_subscription_ids)
        self.destination = destination
        self.handler = handler
        self.options = options
        self._on_cancel = on_cancel
        self._active = True
        self._closed = asyncio.Event()

    @property
    def active(self) -> bool:
        return self._active

    async def wait_closed(self) -> None:
        """Return once the subscription has been cancelled (liveness handle, Req 16.2)."""
        await self._closed.wait()

    async def cancel(self) -> None:
        if not self._active:
            return
        self._active = False
        self._closed.set()
        if self._on_cancel is not None:
            await self._on_cancel(self)

    def __repr__(self) -> str:
        state = "active" if self._active else "cancelled"
        return f"Subscription(id={self.id}, destination={self.destination!r}, {state})"


# --------------------------------------------------------------------------- protocol


@runtime_checkable
class Transport(Protocol):
    """Pluggable communication abstraction (Req 8.1).

    - ``publish`` returns only after the broker confirms; raises
      :class:`TransportUnavailable` while disconnected (Req 8.8) and
      :class:`PublishNotConfirmed` on nack or confirm timeout.
    - ``subscribe`` registers ``handler`` for ``destination``.
    - ``acknowledge`` settles one delivery with an :class:`Outcome`.
    - ``request_reply`` sends one Message and returns the single reply, raising
      :class:`RequestTimeout` after ``timeout`` (default: the configured
      request-reply timeout) (Req 8.7).
    """

    name: ClassVar[str]

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def publish(
        self,
        destination: str,
        message: TypedMessage,
        headers: Mapping[str, str] | None = None,
    ) -> None: ...

    async def subscribe(
        self,
        destination: str,
        handler: Handler,
        options: SubscribeOptions | None = None,
    ) -> Subscription: ...

    async def acknowledge(self, delivery: Delivery, outcome: Outcome = Outcome.ACK) -> None: ...

    async def request_reply(
        self,
        destination: str,
        message: TypedMessage,
        timeout: float | None = None,
    ) -> TypedMessage: ...


@runtime_checkable
class QueueInspector(Protocol):
    """Optional Transport capability: read the depth of a queue (Req 3.1).

    ``ready_count`` returns the number of Messages that are in ``destination``'s
    queue but not delivered to any consumer. Adapters without broker queues (gRPC)
    don't implement it.
    """

    async def ready_count(self, destination: str) -> int: ...


def queue_name_for(destination: str) -> str:
    """Full broker queue name of a Transport destination (``jobs/x`` -> ``das.jobs.x``).

    Matches the RabbitMQ topology: ``results`` -> ``das.results``, ``dlq`` -> ``das.dlq``,
    ``dlq-events`` -> ``das.dlq.events``.
    """
    if destination.startswith("jobs/"):
        return "das.jobs." + destination[len("jobs/") :]
    if destination == "dlq-events":
        return "das.dlq.events"
    return "das." + destination


def missing_operations(cls: type) -> list[str]:
    """Return the Transport operations ``cls`` lacks, in canonical order (Req 15.5)."""
    return [op for op in TRANSPORT_OPERATIONS if not callable(getattr(cls, op, None))]


def validate_request_reply_timeout(value: float) -> float:
    """Check a configured request-reply timeout against its range (1-300 s)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("request_reply_timeout must be a number")
    if not MIN_REQUEST_REPLY_TIMEOUT <= value <= MAX_REQUEST_REPLY_TIMEOUT:
        raise ValueError(
            f"request_reply_timeout must be between {MIN_REQUEST_REPLY_TIMEOUT:g} "
            f"and {MAX_REQUEST_REPLY_TIMEOUT:g} seconds"
        )
    return float(value)


# --------------------------------------------------------------------------- reconnect


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    """Pure reconnect schedule: 1, 2, 4, ... seconds capped at ``max_interval`` (Req 8.6)."""

    max_interval: float = DEFAULT_MAX_RECONNECT_INTERVAL

    def __post_init__(self) -> None:
        value = self.max_interval
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError("max_reconnect_interval must be a number")
        if not MIN_MAX_RECONNECT_INTERVAL <= value <= MAX_MAX_RECONNECT_INTERVAL:
            raise ValueError(
                f"max_reconnect_interval must be between {MIN_MAX_RECONNECT_INTERVAL:g} "
                f"and {MAX_MAX_RECONNECT_INTERVAL:g} seconds"
            )

    def delay(self, attempt: int) -> float:
        """Wait before reconnect attempt ``attempt`` (1-based): ``min(2^(k-1), max)``."""
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError("attempt must be a positive integer")
        # Avoid huge powers: once 2^(k-1) exceeds the cap the answer is the cap.
        if attempt - 1 >= 10:  # 2^9 = 512 > 300, the largest allowed cap
            return float(self.max_interval)
        return float(min(INITIAL_RECONNECT_INTERVAL * 2 ** (attempt - 1), self.max_interval))

    def delays(self) -> Iterator[float]:
        """Infinite iterator of successive reconnect delays."""
        for attempt in itertools.count(1):
            yield self.delay(attempt)


# --------------------------------------------------------------------------- registry


@dataclass(slots=True)
class SubscriptionRegistry:
    """Active subscriptions plus a delivery gate (Req 8.6).

    Adapters call :meth:`mark_down` when the connection drops and
    :meth:`restore` after reconnecting. ``restore`` re-establishes every active
    subscription through the adapter-supplied callback and only then reopens
    the gate. Delivery code awaits :meth:`wait_open` (or checks
    :attr:`is_open`) before invoking a handler, so no Message is delivered
    until every subscription is back.
    """

    _subs: dict[int, Subscription] = field(default_factory=dict)
    _gate: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self._gate.set()

    def add(self, subscription: Subscription) -> None:
        self._subs[subscription.id] = subscription

    def remove(self, subscription: Subscription) -> None:
        self._subs.pop(subscription.id, None)

    def active(self) -> list[Subscription]:
        """Active subscriptions in creation order."""
        return [s for s in self._subs.values() if s.active]

    def __len__(self) -> int:
        return len(self.active())

    @property
    def is_open(self) -> bool:
        return self._gate.is_set()

    def mark_down(self) -> None:
        """Close the delivery gate; deliveries wait until :meth:`restore` completes."""
        self._gate.clear()

    async def wait_open(self) -> None:
        await self._gate.wait()

    async def restore(self, resubscribe: Callable[[Subscription], Awaitable[None]]) -> None:
        """Re-establish every active subscription, then reopen the gate.

        If ``resubscribe`` raises, the gate stays closed and the error
        propagates so the adapter can retry the reconnect.
        """
        self._gate.clear()
        for subscription in self.active():
            await resubscribe(subscription)
        self._gate.set()


__all__ = [
    "DEFAULT_MAX_RECONNECT_INTERVAL",
    "DEFAULT_REQUEST_REPLY_TIMEOUT",
    "MAX_MAX_RECONNECT_INTERVAL",
    "MAX_REQUEST_REPLY_TIMEOUT",
    "MIN_MAX_RECONNECT_INTERVAL",
    "MIN_REQUEST_REPLY_TIMEOUT",
    "TRANSPORT_OPERATIONS",
    "Delivery",
    "Handler",
    "InFlightLimiterLike",
    "Outcome",
    "PublishNotConfirmed",
    "ReconnectPolicy",
    "RequestTimeout",
    "SubscribeOptions",
    "Subscription",
    "SubscriptionRegistry",
    "Transport",
    "TransportError",
    "TransportUnavailable",
    "missing_operations",
    "validate_request_reply_timeout",
]
