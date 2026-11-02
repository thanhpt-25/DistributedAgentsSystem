"""RabbitMQ Queue_Transport adapter (Requirements 4, 6, 8.2, 8.6, 8.8, 8.9).

Built on ``aio-pika`` with publisher confirms and persistent delivery mode.

Topology (declared idempotently by every agent on connect):

=====================  ===================  ===========================================
Name                   Type                 Purpose
=====================  ===================  ===========================================
``das.jobs``           direct exchange      routing key = routing Capability
``das.jobs.<cap>``     quorum queue, DLX    Job dispatch per Capability (Req 4.5)
``das.results``        quorum queue         Job_Result Messages; Managers compete
``das.dlx``            fanout exchange      dead-letter target
``das.dlq``            quorum queue         retained dead letters for operators (6.3)
``das.dlq.events``     quorum queue         copy Managers consume to react to 6.6
=====================  ===================  ===========================================

The ``das-jobs`` policy (pattern ``^das\\.jobs\\.``) sets
``delivery-limit = max_attempts - 1`` and ``dead-letter-exchange = das.dlx``, so
a dispatch returned ``max_attempts`` times is dead-lettered instead of
redelivered (Req 6.6). The Manager applies it at startup through the
management HTTP API (:meth:`RabbitMQTransport.apply_jobs_policy`).

Transport destinations map to broker names (:func:`resolve_destination`):
``jobs/<capability>``, ``results``, ``dlq``, and ``dlq-events``.

Consumption is pull mode (``basic.get``): one poll loop walks the active
subscriptions round robin and fetches a Message only after taking a slot from
the subscription's ``max_in_flight`` budget and its shared limiter. When every
queue is empty, or no slot is free, it waits 200 ms (or until a slot is
released) and polls again, so consumption resumes well within 1 s of a slot
freeing (Req 5.3) and no more than the limiter's size are ever unacknowledged.

Unacknowledged Messages return to their queue when the connection closes
(broker behaviour, Req 4.7, 6.4, 8.9). On a drop the adapter closes its
delivery gate, releases the slots of lost deliveries, and reconnects on the
:class:`~das.transport.base.ReconnectPolicy` schedule; every subscription is
re-established before delivery resumes (Req 8.6). ``publish`` and
``request_reply`` raise :class:`~das.transport.base.TransportUnavailable`
while disconnected (Req 8.8).
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as _dt
import decimal
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Final
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import aio_pika
import aiormq
import httpx

from das.codec.message_codec import MessageCodec, ParseError, TypedMessage
from das.config.schema import CAPABILITY_PATTERN
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

if TYPE_CHECKING:
    from das.config.schema import AgentConfig

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- names

EXCHANGE_JOBS: Final = "das.jobs"
JOBS_QUEUE_PREFIX: Final = "das.jobs."
QUEUE_RESULTS: Final = "das.results"
EXCHANGE_DLX: Final = "das.dlx"
QUEUE_DLQ: Final = "das.dlq"
QUEUE_DLQ_EVENTS: Final = "das.dlq.events"

DESTINATION_JOBS_PREFIX: Final = "jobs/"
DESTINATION_RESULTS: Final = "results"
DESTINATION_DLQ: Final = "dlq"
DESTINATION_DLQ_EVENTS: Final = "dlq-events"

POLICY_NAME: Final = "das-jobs"
POLICY_PATTERN: Final = r"^das\.jobs\."

#: Headers exposed on :class:`Delivery` (same names as the in-memory fake).
DELIVERY_COUNT_HEADER: Final = "x-delivery-count"
DEAD_LETTER_REASON_HEADER: Final = "x-death-reason"
ORIGINAL_DESTINATION_HEADER: Final = "x-original-destination"
#: Headers RabbitMQ adds when it dead-letters a Message.
FIRST_DEATH_REASON_HEADER: Final = "x-first-death-reason"
FIRST_DEATH_QUEUE_HEADER: Final = "x-first-death-queue"

DEFAULT_POLL_INTERVAL: Final = 0.2
DEFAULT_CONFIRM_TIMEOUT: Final = 10.0
DEFAULT_CONNECT_TIMEOUT: Final = 10.0
DEFAULT_MANAGEMENT_TIMEOUT: Final = 10.0
_GET_TIMEOUT: Final = 5.0

_QUORUM: Final = {"x-queue-type": "quorum"}
_JOBS_QUEUE_ARGS: Final = {"x-queue-type": "quorum", "x-dead-letter-exchange": EXCHANGE_DLX}
_CAPABILITY_RE: Final = re.compile(CAPABILITY_PATTERN)

#: Errors meaning the connection or channel is gone.
_CONNECTION_ERRORS: Final = (
    aiormq.exceptions.AMQPConnectionError,
    aiormq.exceptions.AMQPChannelError,
    aiormq.exceptions.ChannelInvalidStateError,
    ConnectionError,
)


class ManagementAPIError(TransportError):
    """Applying the ``das-jobs`` policy through the management API failed."""

    def __init__(self, reason: str, status: int | None = None) -> None:
        self.reason = reason
        self.status = status
        detail = f" (HTTP {status})" if status is not None else ""
        super().__init__(f"RabbitMQ management API error{detail}: {reason}")


# --------------------------------------------------------------------------- pure helpers


@dataclass(frozen=True, slots=True)
class Route:
    """Broker addressing for one Transport destination."""

    destination: str
    exchange: str  # "" = default exchange
    routing_key: str
    queue: str
    capability: str | None = None


def resolve_destination(destination: str) -> Route:
    """Map a Transport destination to exchange, routing key, and queue.

    Raises:
        ValueError: unknown destination or invalid Capability name.
    """
    if not isinstance(destination, str):
        raise ValueError("destination must be a string")
    if destination.startswith(DESTINATION_JOBS_PREFIX):
        capability = destination[len(DESTINATION_JOBS_PREFIX) :]
        if not _CAPABILITY_RE.fullmatch(capability):
            raise ValueError(f"invalid capability in destination {destination!r}")
        return Route(
            destination, EXCHANGE_JOBS, capability, JOBS_QUEUE_PREFIX + capability, capability
        )
    fixed = {
        DESTINATION_RESULTS: QUEUE_RESULTS,
        DESTINATION_DLQ: QUEUE_DLQ,
        DESTINATION_DLQ_EVENTS: QUEUE_DLQ_EVENTS,
    }
    queue = fixed.get(destination)
    if queue is None:
        raise ValueError(f"unknown RabbitMQ destination {destination!r}")
    return Route(destination, "", queue, queue)


def destination_for_queue(queue: str) -> str | None:
    """Inverse of :func:`resolve_destination` for queue names (None if unknown)."""
    if queue.startswith(JOBS_QUEUE_PREFIX):
        capability = queue[len(JOBS_QUEUE_PREFIX) :]
        return DESTINATION_JOBS_PREFIX + capability if _CAPABILITY_RE.fullmatch(capability) else None
    return {
        QUEUE_RESULTS: DESTINATION_RESULTS,
        QUEUE_DLQ: DESTINATION_DLQ,
        QUEUE_DLQ_EVENTS: DESTINATION_DLQ_EVENTS,
    }.get(queue)


def delivery_limit_for(max_attempts: int) -> int:
    """Quorum-queue ``delivery-limit`` for ``max_attempts`` (Req 6.6): ``max_attempts - 1``."""
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
        raise ValueError("max_attempts must be an integer")
    if not 1 <= max_attempts <= 10:
        raise ValueError("max_attempts must be between 1 and 10")
    return max_attempts - 1


def jobs_policy_body(max_attempts: int) -> dict[str, Any]:
    """JSON body for ``PUT /api/policies/<vhost>/das-jobs``."""
    return {
        "pattern": POLICY_PATTERN,
        "apply-to": "queues",
        "priority": 0,
        "definition": {
            "delivery-limit": delivery_limit_for(max_attempts),
            "dead-letter-exchange": EXCHANGE_DLX,
        },
    }


def vhost_from_url(amqp_url: str) -> str:
    """Virtual host named by an AMQP URL (``/`` when the path is empty)."""
    path = urlsplit(amqp_url).path
    if path in ("", "/"):
        return "/"
    return unquote(path[1:])


def policy_request(
    management_url: str, amqp_url: str, policy: str = POLICY_NAME
) -> tuple[str, tuple[str, str]]:
    """Return ``(url, (user, password))`` for the policy PUT.

    Credentials come from the management URL's userinfo, else the AMQP URL's,
    else RabbitMQ's ``guest`` default. They are removed from the request URL.
    """
    mgmt = urlsplit(management_url)
    amqp = urlsplit(amqp_url)
    if mgmt.username is not None:
        auth = (unquote(mgmt.username), unquote(mgmt.password or ""))
    elif amqp.username is not None:
        auth = (unquote(amqp.username), unquote(amqp.password or ""))
    else:
        auth = ("guest", "guest")
    netloc = mgmt.hostname or ""
    if ":" in netloc:  # IPv6 literal
        netloc = f"[{netloc}]"
    if mgmt.port is not None:
        netloc = f"{netloc}:{mgmt.port}"
    base = urlunsplit((mgmt.scheme, netloc, mgmt.path.rstrip("/"), "", ""))
    vhost = quote(vhost_from_url(amqp_url), safe="")
    return f"{base}/api/policies/{vhost}/{quote(policy, safe='')}", auth


def _header_text(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | decimal.Decimal):
        return str(value)
    if isinstance(value, _dt.datetime):
        return value.isoformat()
    return None  # tables and arrays (e.g. x-death) are not exposed


def extract_headers(raw: Mapping[str, object] | None) -> dict[str, str]:
    """Flatten AMQP headers to ``str -> str`` for :attr:`Delivery.headers`.

    Scalar values are converted to text; tables and arrays are dropped. When
    RabbitMQ dead-lettered the Message, ``x-death-reason`` and
    ``x-original-destination`` are derived from ``x-first-death-reason`` and
    ``x-first-death-queue`` so consumers see the same keys on every transport.
    """
    headers: dict[str, str] = {}
    for key, value in (raw or {}).items():
        text = _header_text(value)
        if text is not None:
            headers[str(key)] = text
    reason = headers.get(FIRST_DEATH_REASON_HEADER)
    if reason is not None:
        headers.setdefault(DEAD_LETTER_REASON_HEADER, reason)
    queue = headers.get(FIRST_DEATH_QUEUE_HEADER)
    if queue is not None:
        headers.setdefault(ORIGINAL_DESTINATION_HEADER, destination_for_queue(queue) or queue)
    return headers


def delivery_count_from(raw: Mapping[str, object] | None) -> int:
    """``x-delivery-count`` as a non-negative int (0 when absent or unparsable)."""
    value = (raw or {}).get(DELIVERY_COUNT_HEADER)
    if isinstance(value, bool) or value is None:
        return 0
    try:
        count = int(_header_text(value) or "") if not isinstance(value, int) else value
    except ValueError:
        return 0
    return max(count, 0)


def _check_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    result = dict(headers or {})
    for key, value in result.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("transport headers must map str to str")
    return result


# --------------------------------------------------------------------------- adapter


@dataclass(eq=False, slots=True)
class _Token:
    """Opaque :attr:`Delivery.token` of a RabbitMQ delivery."""

    incoming: Any  # aio_pika.abc.AbstractIncomingMessage
    subscription: Subscription
    generation: int
    settled: bool = False
    lost: bool = False


Connect = Callable[..., Awaitable[Any]]
Sleep = Callable[[float], Awaitable[None]]


class RabbitMQTransport:
    """:class:`~das.transport.base.Transport` over RabbitMQ (Req 8.2)."""

    name: ClassVar[str] = "rabbitmq"

    def __init__(
        self,
        *,
        url: str,
        management_url: str | None = None,
        agent_id: str = "rabbitmq",
        codec: MessageCodec | None = None,
        max_attempts: int = 3,
        apply_policy: bool = False,
        request_reply_timeout: float = DEFAULT_REQUEST_REPLY_TIMEOUT,
        reconnect_policy: ReconnectPolicy | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        confirm_timeout: float = DEFAULT_CONFIRM_TIMEOUT,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        connect: Connect | None = None,
        http_client: httpx.AsyncClient | None = None,
        sleep: Sleep | None = None,
    ) -> None:
        if not url:
            raise ValueError("url must not be empty")
        if apply_policy and not management_url:
            raise ValueError("management_url is required to apply the das-jobs policy")
        if poll_interval <= 0 or confirm_timeout <= 0 or connect_timeout <= 0:
            raise ValueError("poll_interval, confirm_timeout and connect_timeout must be positive")
        delivery_limit_for(max_attempts)  # validate early
        self.url = url
        self.management_url = management_url
        self.agent_id = agent_id
        self.codec = codec or MessageCodec()
        self.max_attempts = max_attempts
        self.apply_policy = apply_policy
        self.request_reply_timeout = validate_request_reply_timeout(request_reply_timeout)
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self.poll_interval = float(poll_interval)
        self.confirm_timeout = float(confirm_timeout)
        self.connect_timeout = float(connect_timeout)
        self.registry = SubscriptionRegistry()
        self.stale_acknowledgements = 0
        self.handler_errors: list[BaseException] = []

        self._connect: Connect = connect or aio_pika.connect
        self._http_client = http_client
        self._sleep: Sleep = sleep or asyncio.sleep
        self._connection: Any = None
        self._channel: Any = None
        self._jobs_exchange: Any = None
        self._connected = False
        self._closing = False
        self._generation = 0
        self._declared_jobs: set[str] = set()
        self._queues: dict[int, Any] = {}  # subscription id -> aio_pika queue
        self._in_flight: dict[int, int] = {}  # subscription id -> unsettled count
        self._tokens: set[_Token] = set()
        self._rr = 0
        self._wakeup = asyncio.Event()
        self._poll_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._reply_queue: Any = None
        self._pending_replies: dict[str, asyncio.Future[bytes]] = {}

    @classmethod
    def from_config(cls, config: AgentConfig, **kwargs: Any) -> RabbitMQTransport:
        """Build from an Agent_Config; Managers apply the ``das-jobs`` policy."""
        if config.rabbitmq is None:
            raise ValueError("Agent_Config has no rabbitmq section")
        limits = config.limits
        options: dict[str, Any] = {
            "url": config.rabbitmq.url,
            "management_url": config.rabbitmq.management_url,
            "agent_id": config.agent_id or "rabbitmq",
            "codec": MessageCodec(limits.max_message_bytes),
            "max_attempts": limits.max_attempts,
            "apply_policy": config.role == "manager",
            "request_reply_timeout": limits.request_reply_timeout,
            "reconnect_policy": ReconnectPolicy(limits.max_reconnect_interval),
        }
        options.update(kwargs)
        return cls(**options)

    # ----------------------------------------------------------- lifecycle

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        """Connect, declare the topology, and (Managers) apply the policy."""
        if self._connected:
            return
        self._closing = False
        try:
            await self._open()
        except Exception as exc:
            raise TransportUnavailable(self.name, f"cannot connect: {exc}") from exc
        self._connected = True
        if self.apply_policy:
            await self.apply_jobs_policy()

    async def close(self) -> None:
        self._closing = True
        for task in (self._poll_task, self._reconnect_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._poll_task = self._reconnect_task = None
        for subscription in self.registry.active():
            self.registry.remove(subscription)
            await subscription.cancel()
        self._fail_pending_replies(TransportUnavailable(self.name, "transport closed"))
        self._release_all_tokens()
        self._connected = False
        connection, self._connection, self._channel = self._connection, None, None
        if connection is not None:
            with contextlib.suppress(Exception):
                await connection.close()

    # ----------------------------------------------------------- policy

    async def apply_jobs_policy(self) -> None:
        """PUT the ``das-jobs`` policy (``delivery-limit = max_attempts - 1``)."""
        if not self.management_url:
            raise ManagementAPIError("management_url is not configured")
        url, auth = policy_request(self.management_url, self.url)
        body = jobs_policy_body(self.max_attempts)
        client = self._http_client
        owned = client is None
        if client is None:
            client = httpx.AsyncClient(timeout=DEFAULT_MANAGEMENT_TIMEOUT)
        try:
            response = await client.put(url, json=body, auth=auth)
        except httpx.HTTPError as exc:
            raise ManagementAPIError(f"request failed: {exc}") from exc
        finally:
            if owned:
                await client.aclose()
        if response.status_code not in (200, 201, 204):
            raise ManagementAPIError(response.text[:200], response.status_code)

    # ----------------------------------------------------------- operations

    async def publish(
        self,
        destination: str,
        message: TypedMessage,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        route = resolve_destination(destination)
        body = self.codec.encode(message)
        hdrs = _check_headers(headers)
        self._require_connected()
        amqp_message = aio_pika.Message(
            body,
            headers=dict(hdrs),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            message_id=message.message_id,
        )
        await self._publish(route, amqp_message)

    async def subscribe(
        self,
        destination: str,
        handler: Handler,
        options: SubscribeOptions | None = None,
    ) -> Subscription:
        route = resolve_destination(destination)
        self._require_connected()
        subscription = Subscription(
            destination, handler, options or SubscribeOptions(), on_cancel=self._unsubscribe
        )
        try:
            self._queues[subscription.id] = await self._declare_route_queue(route)
        except _CONNECTION_ERRORS as exc:
            raise TransportUnavailable(self.name, f"subscribe failed: {exc}") from exc
        self._in_flight[subscription.id] = 0
        self.registry.add(subscription)
        self._ensure_poller()
        self._wakeup.set()
        return subscription

    async def acknowledge(self, delivery: Delivery, outcome: Outcome = Outcome.ACK) -> None:
        token = delivery.token
        if not isinstance(token, _Token):
            raise ValueError("delivery was not produced by a RabbitMQTransport")
        if token.settled or token.lost or token.generation != self._generation:
            # Already settled, or lost on a connection drop (the broker requeued it).
            self.stale_acknowledgements += 1
            return
        self._require_connected()
        try:
            if outcome is Outcome.ACK:
                await token.incoming.ack()
            elif outcome is Outcome.REQUEUE:
                await token.incoming.nack(requeue=True)
            else:
                await token.incoming.reject(requeue=False)
        except _CONNECTION_ERRORS as exc:
            raise TransportUnavailable(self.name, f"acknowledge failed: {exc}") from exc
        self._settle(token)

    async def request_reply(
        self,
        destination: str,
        message: TypedMessage,
        timeout: float | None = None,
    ) -> TypedMessage:
        route = resolve_destination(destination)
        limit = self.request_reply_timeout if timeout is None else float(timeout)
        body = self.codec.encode(message)
        self._require_connected()
        reply_queue = await self._ensure_reply_queue()
        correlation = uuid.uuid4().hex
        future: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        self._pending_replies[correlation] = future
        try:
            amqp_message = aio_pika.Message(
                body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                message_id=message.message_id,
                correlation_id=correlation,
                reply_to=reply_queue.name,
                expiration=limit,  # stale requests expire unprocessed
            )
            await self._publish(route, amqp_message)
            try:
                async with asyncio.timeout(limit):
                    reply_body = await future
            except TimeoutError:
                raise RequestTimeout(destination, limit) from None
        finally:
            self._pending_replies.pop(correlation, None)
        try:
            return self.codec.decode(reply_body)
        except ParseError as exc:
            raise TransportError(f"invalid reply from {destination!r}: {exc}") from exc

    async def ready_count(self, destination: str) -> int:
        """Messages ready in ``destination``'s queue (``QueueInspector``, Req 3.1).

        Uses a passive ``queue.declare`` on a short-lived channel, so a missing
        queue (``NOT_FOUND`` closes the channel) never affects the main channel.
        Raises :class:`TransportUnavailable` while disconnected.
        """
        route = resolve_destination(destination)
        self._require_connected()
        connection = self._connection
        try:
            channel = await connection.channel(publisher_confirms=False)
        except _CONNECTION_ERRORS as exc:
            raise TransportUnavailable(self.name, f"cannot open channel: {exc}") from exc
        try:
            queue = await channel.declare_queue(route.queue, passive=True)
            return int(queue.declaration_result.message_count or 0)
        except _CONNECTION_ERRORS as exc:
            raise TransportError(f"cannot inspect queue {route.queue!r}: {exc}") from exc
        finally:
            with contextlib.suppress(Exception):
                await channel.close()

    @property
    def subscriptions(self) -> list[Subscription]:
        return self.registry.active()

    def in_flight(self) -> int:
        """Unsettled deliveries currently held by this transport."""
        return sum(self._in_flight.values())

    # ----------------------------------------------------------- connection

    def _require_connected(self) -> None:
        if not self._connected or self._channel is None:
            raise TransportUnavailable(self.name, f"agent {self.agent_id!r} is disconnected")

    async def _open(self) -> None:
        connection = await self._connect(self.url, timeout=self.connect_timeout)
        try:
            channel = await connection.channel(publisher_confirms=True)
            await self._declare_topology(channel)
        except BaseException:
            with contextlib.suppress(Exception):
                await connection.close()
            raise
        self._connection = connection
        self._channel = channel
        self._declared_jobs = set()
        self._reply_queue = None
        generation = self._generation
        connection.close_callbacks.add(lambda *_: self._on_lost(generation))
        channel.close_callbacks.add(lambda *_: self._on_lost(generation))

    async def _declare_topology(self, channel: Any) -> None:
        self._jobs_exchange = await channel.declare_exchange(
            EXCHANGE_JOBS, aio_pika.ExchangeType.DIRECT, durable=True
        )
        dlx = await channel.declare_exchange(
            EXCHANGE_DLX, aio_pika.ExchangeType.FANOUT, durable=True
        )
        await channel.declare_queue(QUEUE_RESULTS, durable=True, arguments=dict(_QUORUM))
        for name in (QUEUE_DLQ, QUEUE_DLQ_EVENTS):
            queue = await channel.declare_queue(name, durable=True, arguments=dict(_QUORUM))
            await queue.bind(dlx, routing_key="")

    async def _declare_route_queue(self, route: Route) -> Any:
        channel = self._channel
        if route.capability is None:
            return await channel.declare_queue(route.queue, durable=True, arguments=dict(_QUORUM))
        queue = await channel.declare_queue(
            route.queue, durable=True, arguments=dict(_JOBS_QUEUE_ARGS)
        )
        if route.queue not in self._declared_jobs:
            await queue.bind(self._jobs_exchange, routing_key=route.routing_key)
            self._declared_jobs.add(route.queue)
        return queue

    def _on_lost(self, generation: int) -> None:
        """Connection or channel closed: gate delivery and start reconnecting."""
        if self._closing or generation != self._generation:
            return
        log.warning("rabbitmq connection lost; reconnecting")
        self._connected = False
        self._generation += 1
        self.registry.mark_down()
        self._queues.clear()
        self._release_all_tokens()
        self._fail_pending_replies(TransportUnavailable(self.name, "connection lost"))
        old, self._connection, self._channel = self._connection, None, None
        if old is not None and not getattr(old, "is_closed", True):
            asyncio.get_running_loop().create_task(self._quiet_close(old))
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.get_running_loop().create_task(self._reconnect())

    @staticmethod
    async def _quiet_close(connection: Any) -> None:
        with contextlib.suppress(Exception):
            await connection.close()

    async def _reconnect(self) -> None:
        for attempt in range(1, 1_000_000):
            await self._sleep(self.reconnect_policy.delay(attempt))
            if self._closing:
                return
            try:
                await self._open()
                self._connected = True
                await self.registry.restore(self._resubscribe)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("rabbitmq reconnect attempt %d failed: %s", attempt, exc)
                self._connected = False
                self._generation += 1  # ignore close callbacks of the failed attempt
                connection, self._connection, self._channel = self._connection, None, None
                if connection is not None:
                    await self._quiet_close(connection)
                continue
            log.info("rabbitmq reconnected after %d attempt(s)", attempt)
            if self.registry.active():
                self._ensure_poller()  # restart the poller if it ended while disconnected
            self._wakeup.set()
            return

    async def _resubscribe(self, subscription: Subscription) -> None:
        route = resolve_destination(subscription.destination)
        self._queues[subscription.id] = await self._declare_route_queue(route)
        self._in_flight[subscription.id] = 0

    async def _unsubscribe(self, subscription: Subscription) -> None:
        self.registry.remove(subscription)
        self._queues.pop(subscription.id, None)

    async def _publish(self, route: Route, amqp_message: Any) -> None:
        try:
            if route.capability is not None:
                await self._declare_route_queue(route)  # binding must exist (idempotent)
                exchange = self._jobs_exchange
            else:
                exchange = self._channel.default_exchange
            await exchange.publish(
                amqp_message,
                routing_key=route.routing_key,
                mandatory=True,
                timeout=self.confirm_timeout,
            )
        except aiormq.exceptions.PublishError as exc:
            raise PublishNotConfirmed(route.destination, "message returned as unroutable") from exc
        except aiormq.exceptions.DeliveryError as exc:
            raise PublishNotConfirmed(route.destination, "broker nack") from exc
        except TimeoutError as exc:
            raise PublishNotConfirmed(
                route.destination, f"no confirm within {self.confirm_timeout:g} s"
            ) from exc
        except _CONNECTION_ERRORS as exc:
            raise TransportUnavailable(self.name, f"publish failed: {exc}") from exc

    # ----------------------------------------------------------- request-reply

    async def _ensure_reply_queue(self) -> Any:
        if self._reply_queue is None:
            try:
                queue = await self._channel.declare_queue(
                    "", exclusive=True, auto_delete=True, arguments={"x-queue-type": "classic"}
                )
                await queue.consume(self._on_reply, no_ack=True)
            except _CONNECTION_ERRORS as exc:
                raise TransportUnavailable(self.name, f"reply queue failed: {exc}") from exc
            self._reply_queue = queue
        return self._reply_queue

    async def _on_reply(self, incoming: Any) -> None:
        future = self._pending_replies.get(incoming.correlation_id or "")
        if future is not None and not future.done():
            future.set_result(bytes(incoming.body))

    def _fail_pending_replies(self, error: Exception) -> None:
        for future in self._pending_replies.values():
            if not future.done():
                future.set_exception(error)
        self._pending_replies.clear()

    # ----------------------------------------------------------- pull consumption

    def _ensure_poller(self) -> None:
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.get_running_loop().create_task(self._poll_loop())

    async def _idle(self) -> None:
        self._wakeup.clear()
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(self.poll_interval):
                await self._wakeup.wait()

    def _take_slot(self, subscription: Subscription) -> bool:
        cap = subscription.options.max_in_flight
        if cap is not None and self._in_flight.get(subscription.id, 0) >= cap:
            return False
        limiter = subscription.options.limiter
        if limiter is not None and not limiter.try_acquire():
            return False
        self._in_flight[subscription.id] = self._in_flight.get(subscription.id, 0) + 1
        return True

    def _give_slot(self, subscription: Subscription) -> None:
        if subscription.id in self._in_flight and self._in_flight[subscription.id] > 0:
            self._in_flight[subscription.id] -= 1
        limiter = subscription.options.limiter
        if limiter is not None:
            limiter.release()
        self._wakeup.set()

    async def poll_once(self) -> bool:
        """Run one round-robin pass; return True if a Message was fetched."""
        if not self.registry.is_open or not self._connected:
            return False
        subscriptions = self.registry.active()
        count = len(subscriptions)
        for offset in range(count):
            index = (self._rr + offset) % count
            subscription = subscriptions[index]
            queue = self._queues.get(subscription.id)
            if queue is None or not self._take_slot(subscription):
                continue
            generation = self._generation
            try:
                incoming = await queue.get(no_ack=False, fail=False, timeout=_GET_TIMEOUT)
            except Exception as exc:
                self._give_slot(subscription)
                if not isinstance(exc, (*_CONNECTION_ERRORS, TimeoutError)):
                    log.exception("rabbitmq basic.get failed")
                return False
            if incoming is None:
                self._give_slot(subscription)
                continue
            if generation != self._generation:
                # The connection dropped during the get; the broker requeued it.
                self._give_slot(subscription)
                continue
            if not subscription.active:
                # Cancelled while fetching: hand the Message back.
                self._give_slot(subscription)
                with contextlib.suppress(Exception):
                    await incoming.nack(requeue=True)
                continue
            self._rr = (index + 1) % count
            self._dispatch(subscription, incoming, generation)
            return True
        return False

    async def _poll_loop(self) -> None:
        while not self._closing:
            await self.registry.wait_open()
            try:
                fetched = await self.poll_once()
            except Exception:
                # One failed pass must not end the poller; CancelledError passes through.
                if self._closing:
                    return
                log.exception("rabbitmq poll pass failed")
                fetched = False
            if not fetched:
                await self._idle()

    def _dispatch(self, subscription: Subscription, incoming: Any, generation: int) -> None:
        token = _Token(incoming, subscription, generation)
        self._tokens.add(token)
        delivery = self._make_delivery(subscription.destination, incoming, token)
        task = asyncio.get_running_loop().create_task(
            self._run_handler(subscription.handler, delivery, incoming)
        )
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)

    def _make_delivery(self, destination: str, incoming: Any, token: _Token) -> Delivery:
        body = bytes(incoming.body)
        message: TypedMessage | None = None
        error: ParseError | None = None
        try:
            message = self.codec.decode(body)
        except ParseError as exc:
            error = exc
        raw = incoming.headers or {}
        return Delivery(
            message=message,
            destination=destination,
            redelivered=bool(incoming.redelivered),
            delivery_count=delivery_count_from(raw),
            headers=extract_headers(raw),
            token=token,
            body=body,
            parse_error=error,
        )

    async def _run_handler(self, handler: Handler, delivery: Delivery, incoming: Any) -> None:
        try:
            reply = await handler(delivery)
        except Exception as exc:  # recorded; the delivery stays unacked
            log.exception("rabbitmq handler for %s failed", delivery.destination)
            self.handler_errors.append(exc)
            return
        reply_to = getattr(incoming, "reply_to", None)
        if not reply_to:
            return
        token = delivery.token
        assert isinstance(token, _Token)
        try:
            if reply is not None and self._connected and token.generation == self._generation:
                await self._channel.default_exchange.publish(
                    aio_pika.Message(
                        self.codec.encode(reply),
                        content_type="application/json",
                        message_id=reply.message_id,
                        correlation_id=incoming.correlation_id,
                    ),
                    routing_key=reply_to,
                    mandatory=False,
                    timeout=self.confirm_timeout,
                )
            if not token.settled and not token.lost:
                await self.acknowledge(delivery, Outcome.ACK)
        except Exception as exc:
            log.warning("rabbitmq reply to %s failed: %s", reply_to, exc)

    def _settle(self, token: _Token) -> None:
        token.settled = True
        self._tokens.discard(token)
        self._give_slot(token.subscription)

    def _release_all_tokens(self) -> None:
        """Deliveries lost on a drop return to the broker; free their slots."""
        tokens, self._tokens = self._tokens, set()
        for token in tokens:
            if not token.settled:
                token.lost = True
                self._give_slot(token.subscription)


__all__ = [
    "DEAD_LETTER_REASON_HEADER",
    "DEFAULT_POLL_INTERVAL",
    "DELIVERY_COUNT_HEADER",
    "DESTINATION_DLQ",
    "DESTINATION_DLQ_EVENTS",
    "DESTINATION_JOBS_PREFIX",
    "DESTINATION_RESULTS",
    "EXCHANGE_DLX",
    "EXCHANGE_JOBS",
    "JOBS_QUEUE_PREFIX",
    "ORIGINAL_DESTINATION_HEADER",
    "POLICY_NAME",
    "POLICY_PATTERN",
    "QUEUE_DLQ",
    "QUEUE_DLQ_EVENTS",
    "QUEUE_RESULTS",
    "ManagementAPIError",
    "RabbitMQTransport",
    "Route",
    "delivery_count_from",
    "delivery_limit_for",
    "destination_for_queue",
    "extract_headers",
    "jobs_policy_body",
    "policy_request",
    "resolve_destination",
    "vhost_from_url",
]
