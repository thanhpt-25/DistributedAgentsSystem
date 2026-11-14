"""The Kafka Transport (das-scale-resilience task 10.2; Phase 7 Req 3).

Implements the Transport protocol over Kafka with ``aiokafka``. One topic per destination, named as the RabbitMQ
queue (``das.jobs.<cap>``, ``das.results``, ``das.dlq``), plus ``<topic>.high`` for priority and ``<topic>.retry`` for
requeues.

* ``publish`` goes through an idempotent producer with ``acks=all`` and returns once the broker has the record.
* ``subscribe`` runs two consumers per subscription in the group ``das-<queue>``: one on ``.high``, read first, and one
  on the main and ``.retry`` topics. Offsets are committed only after ``acknowledge``, and only up to the lowest message
  still in flight (:class:`~das.transport.kafka_offsets.OffsetTracker`), so a message acknowledged out of order never
  carries an unfinished one past the commit point (Req 3.4, 3.6).
* A rebalance commits what is acknowledged for the partitions taken away and forgets the rest; a late acknowledgement of
  such a message commits nothing, and its message goes to the new owner (Req 3.6).
* Kafka has no requeue or dead letter, so REQUEUE republishes to ``.retry`` with ``das-not-before`` and the delivery
  count, and the subscriber holds it back until then; REJECT, or a count above the limit, publishes to the dead-letter
  topics with the headers the other transports write, and then the original is committed (Req 3.5).
* ``request_reply`` uses a per-process reply topic and correlation ids (Req 3.7).
* A lost connection closes the delivery gate, the reconnect loop pings the cluster on the :class:`ReconnectPolicy`
  schedule, and a commit that could not be made is retried when it is back, so an acknowledged message is not redone.

All client objects come from a :class:`KafkaFactory`, so tests run the same code against an in-memory cluster.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import re
import ssl
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Final, Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import (
    CommitFailedError,
    KafkaConnectionError,
    KafkaError,
    KafkaTimeoutError,
    TopicAlreadyExistsError,
)
from aiokafka.structs import TopicPartition

from das.codec.message_codec import MessageCodec, ParseError, TypedMessage
from das.observability import get_logger
from das.transport.base import (
    DEFAULT_REQUEST_REPLY_TIMEOUT,
    PRIORITY_HEADER,
    PRIORITY_HIGH,
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
    queue_name_for,
    validate_request_reply_timeout,
)
from das.transport.kafka_offsets import OffsetTracker
from das.transport.slots import Slots

log = get_logger("das.transport.kafka")

HIGH_SUFFIX: Final = ".high"
RETRY_SUFFIX: Final = ".retry"
#: Wire headers the adapter writes and consumes; they are never delivered to a handler.
NOT_BEFORE_HEADER: Final = "das-not-before"
COUNT_HEADER: Final = "das-delivery-count"
REPLY_TO_HEADER: Final = "das-reply-to"
CORRELATION_HEADER: Final = "das-correlation-id"
DEFAULT_DEAD_LETTERS: Final = ("dlq", "dlq-events")
DEFAULT_RETRY_DELAY: Final = 1.0
#: How long one fetch waits for a record before the loop looks at the gate, the delayed retries and the commits again.
POLL_TIMEOUT_MS: Final = 200
#: A new consumer's first fetch takes a round trip, so its first non-blocking poll returns nothing even when records
#: wait. The high-priority lane is given this long on a subscription's first poll, so a backlog is read high first;
#: after that the client's background fetching keeps its records buffered for the non-blocking polls.
HIGH_WARMUP_MS: Final = 500
PULL_BATCH: Final = 10
CONNECTION_ERRORS: Final = (KafkaConnectionError, KafkaTimeoutError, ConnectionError, OSError)
_INVALID_TOPIC_CHARACTERS: Final = re.compile(r"[^A-Za-z0-9._-]")
MAX_TOPIC_NAME: Final = 249


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    """Where the cluster is and how to talk to it. The passwords are secrets (``SecretStr`` is unwrapped by
    :meth:`KafkaTransport.from_config`, and they are left out of ``repr``)."""

    bootstrap_servers: tuple[str, ...]
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str | None = None
    sasl_password: str | None = field(default=None, repr=False)
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_key_password: str | None = field(default=None, repr=False)
    partitions: int = 3
    replication_factor: int = 1
    auto_create_topics: bool = True
    retry_delay: float = DEFAULT_RETRY_DELAY
    delivery_limit: int = 4
    dead_letter_destinations: tuple[str, ...] = DEFAULT_DEAD_LETTERS

    def __post_init__(self) -> None:
        if not self.bootstrap_servers:
            raise ValueError("kafka needs at least one bootstrap server")
        if self.partitions < 1 or self.replication_factor < 1:
            raise ValueError("partitions and replication_factor must be positive")
        if self.retry_delay < 0 or self.delivery_limit < 0:
            raise ValueError("retry_delay and delivery_limit must not be negative")


class KafkaFactory(Protocol):
    """Makes the client objects, so the adapter never builds an ``aiokafka`` class itself."""

    def producer(self) -> Any: ...

    def consumer(self, group_id: str | None, *, from_start: bool) -> Any: ...

    def admin(self) -> Any: ...


class AioKafkaFactory:
    """The real clients, configured from :class:`KafkaSettings`."""

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings

    def _common(self) -> dict[str, Any]:
        s = self._settings
        common: dict[str, Any] = {
            "bootstrap_servers": list(s.bootstrap_servers),
            "security_protocol": s.security_protocol,
        }
        if s.security_protocol in ("SSL", "SASL_SSL"):
            context = ssl.create_default_context(cafile=s.ssl_cafile)
            if s.ssl_certfile:
                context.load_cert_chain(s.ssl_certfile, s.ssl_keyfile, s.ssl_key_password)
            common["ssl_context"] = context
        if s.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            common.update(
                sasl_mechanism=s.sasl_mechanism,
                sasl_plain_username=s.sasl_username,
                sasl_plain_password=s.sasl_password,
            )
        return common

    def producer(self) -> Any:
        return AIOKafkaProducer(enable_idempotence=True, acks="all", **self._common())

    def consumer(self, group_id: str | None, *, from_start: bool) -> Any:
        return AIOKafkaConsumer(
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest" if from_start else "latest",
            **self._common(),
        )

    def admin(self) -> Any:
        return AIOKafkaAdminClient(**self._common())


def topic_for(destination: str, *, high: bool = False) -> str:
    """The Kafka topic of a destination: the RabbitMQ queue name, with ``/`` turned into ``.`` and anything else Kafka
    does not allow in a topic name (it takes letters, digits, ``.``, ``_`` and ``-``) into ``_``."""
    name = _INVALID_TOPIC_CHARACTERS.sub("_", queue_name_for(destination).replace("/", "."))
    if len(name) + len(RETRY_SUFFIX) > MAX_TOPIC_NAME:
        raise ValueError(f"destination {destination!r} is too long for a Kafka topic name")
    return name + (HIGH_SUFFIX if high else "")


def _headers_out(headers: Mapping[str, str]) -> list[tuple[str, bytes]]:
    return [(k, v.encode("utf-8")) for k, v in headers.items()]


def _headers_in(raw: Any) -> dict[str, str]:
    return {str(k): bytes(v).decode("utf-8", "replace") for k, v in (raw or [])}


def _check_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    result = dict(headers or {})
    for key, value in result.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("transport headers must map str to str")
    return result


def _count(raw: str | None) -> int:
    try:
        return max(0, int(raw or 0))
    except ValueError:
        return 0


class _Lane:
    """One consumer of a subscription, with its own offsets: what is in flight, what to commit, and which generation of
    a partition's ownership a delivery belongs to (a rebalance bumps it)."""

    def __init__(self, consumer: Any) -> None:
        self.consumer = consumer
        self.tracker = OffsetTracker()
        self.pending: dict[TopicPartition, int] = {}
        self.generation: dict[TopicPartition, int] = {}


@dataclass(slots=True)
class _Token:
    """The ``Delivery.token``: the partition and offset to commit once the message is finished."""

    lane: _Lane
    partition: TopicPartition
    offset: int
    generation: int
    subscription: Subscription
    settled: bool = False


class _Listener(ConsumerRebalanceListener):
    """Commits what is acknowledged for the partitions taken away, then forgets them (Req 3.6)."""

    def __init__(self, transport: KafkaTransport, lane: _Lane) -> None:
        self._transport = transport
        self._lane = lane

    async def on_partitions_revoked(self, revoked: Any) -> None:
        await self._transport._revoked(self._lane, list(revoked))  # noqa: SLF001

    async def on_partitions_assigned(self, assigned: Any) -> None:
        return None


@dataclass(slots=True)
class _Stream:
    """Everything one subscription runs: its lanes, slots, and the retries waiting for their time."""

    main: _Lane
    high: _Lane
    slots: Slots
    delayed: list[tuple[float, int, tuple[Any, ...]]] = field(default_factory=list)
    sequence: itertools.count[int] = field(default_factory=itertools.count)


@dataclass(slots=True)
class _Reply:
    topic: str
    consumer: Any
    task: asyncio.Task[None] | None = None
    waiting: dict[str, asyncio.Future[TypedMessage]] = field(default_factory=dict)


class KafkaTransport:
    """:class:`~das.transport.base.Transport` over Kafka."""

    name: ClassVar[str] = "kafka"

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        agent_id: str = "kafka",
        codec: MessageCodec | None = None,
        request_reply_timeout: float = DEFAULT_REQUEST_REPLY_TIMEOUT,
        reconnect_policy: ReconnectPolicy | None = None,
        factory: KafkaFactory | None = None,
        sleep: Any = asyncio.sleep,
        now: Any = time.time,
        batch: int = PULL_BATCH,
    ) -> None:
        if batch < 1:
            raise ValueError("batch must be positive")
        self.settings = settings
        self.agent_id = agent_id
        self.codec = codec or MessageCodec()
        self.request_reply_timeout = validate_request_reply_timeout(request_reply_timeout)
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self.registry = SubscriptionRegistry()
        self.handler_errors: list[BaseException] = []
        self._factory: KafkaFactory = factory or AioKafkaFactory(settings)
        self._sleep = sleep
        self._now = now
        self._batch = batch
        self._producer: Any = None
        self._admin: Any = None
        self._connected = False
        self._closing = False
        self._known_topics: set[str] = set()
        self._streams: dict[int, _Stream] = {}
        self._pollers: dict[int, asyncio.Task[None]] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._reconnect_task: asyncio.Task[None] | None = None
        self._reply: _Reply | None = None
        self._reply_lock = asyncio.Lock()
        self._process = uuid.uuid4().hex[:12]

    @classmethod
    def from_config(cls, config: Any, **kwargs: Any) -> KafkaTransport:
        """Build from an Agent_Config ``kafka`` section and ``limits``."""
        kafka = config.kafka
        if kafka is None:
            raise ValueError("Agent_Config has no kafka section")
        limits = config.limits

        def secret(value: Any) -> str | None:
            return value.get_secret_value() if value is not None else None

        settings = KafkaSettings(
            bootstrap_servers=tuple(kafka.bootstrap_servers),
            security_protocol=kafka.security_protocol,
            sasl_mechanism=kafka.sasl_mechanism,
            sasl_username=kafka.sasl_username,
            sasl_password=secret(kafka.sasl_password),
            ssl_cafile=kafka.ssl_cafile,
            ssl_certfile=kafka.ssl_certfile,
            ssl_keyfile=kafka.ssl_keyfile,
            ssl_key_password=secret(kafka.ssl_key_password),
            partitions=kafka.partitions,
            replication_factor=kafka.replication_factor,
            auto_create_topics=kafka.auto_create_topics,
            retry_delay=kafka.retry_delay,
            delivery_limit=max(0, limits.max_attempts - 1),
        )
        options: dict[str, Any] = {
            "agent_id": config.agent_id or "kafka",
            "codec": MessageCodec(limits.max_message_bytes),
            "request_reply_timeout": limits.request_reply_timeout,
            "reconnect_policy": ReconnectPolicy(limits.max_reconnect_interval),
        }
        options.update(kwargs)
        return cls(settings, **options)

    # ------------------------------------------------------------------ lifecycle

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        if self._connected:
            return
        self._closing = False
        producer, admin = self._factory.producer(), self._factory.admin()
        try:
            await producer.start()
            await admin.start()
            await admin.list_topics()
        except Exception as exc:
            await self._stop_quietly(producer)
            await self._stop_quietly(admin, close=True)
            if isinstance(exc, KafkaError | OSError):
                raise TransportUnavailable(self.name, f"cannot connect: {type(exc).__name__}") from exc
            raise
        self._producer, self._admin = producer, admin
        self._connected = True

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        for subscription in self.registry.active():
            await subscription.cancel()
        reply, self._reply = self._reply, None
        tasks = [t for t in (*self._tasks, self._reconnect_task, reply.task if reply else None) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if reply is not None:
            await self._stop_quietly(reply.consumer)
            await self._delete_quietly(reply.topic)
        await self._stop_quietly(self._producer)
        await self._stop_quietly(self._admin, close=True)
        self._producer = self._admin = None

    @staticmethod
    async def _stop_quietly(client: Any, *, close: bool = False) -> None:
        if client is None:
            return
        try:
            await (client.close() if close else client.stop())
        except Exception:  # noqa: BLE001 - shutting down must not fail on a client that is already gone
            log.debug("kafka client did not stop cleanly", exc_info=True)

    async def _delete_quietly(self, topic: str) -> None:
        if self._admin is None or not self.settings.auto_create_topics:
            return
        try:
            await self._admin.delete_topics([topic])
        except Exception:  # noqa: BLE001 - a leftover reply topic is harmless; retention removes it
            log.debug("kafka reply topic was not deleted", exc_info=True)

    def _require_connected(self) -> None:
        if not self._connected:
            raise TransportUnavailable(self.name, f"agent {self.agent_id!r} is disconnected")

    # ------------------------------------------------------------------ reconnecting (Req 8.6, 8.9)

    def _mark_down(self) -> None:
        if not self._connected or self._closing:
            return
        self._connected = False
        self.registry.mark_down()
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.get_running_loop().create_task(self._reconnect())

    async def _reconnect(self) -> None:
        for attempt in itertools.count(1):
            await self._sleep(self.reconnect_policy.delay(attempt))
            if self._closing:
                return
            try:
                await self._admin.list_topics()
            except Exception:  # noqa: BLE001 - any failure means it is still down
                continue
            self._connected = True
            await self.registry.restore(_nothing)  # consumers keep their assignment: reopening the gate resumes them
            log.info("kafka transport reconnected", agent_id=self.agent_id)
            return

    def _unavailable(self, exc: BaseException) -> TransportUnavailable:
        self._mark_down()
        return TransportUnavailable(self.name, f"cluster unreachable: {type(exc).__name__}")

    # ------------------------------------------------------------------ topics and publishing

    async def _ensure_topic(self, topic: str, partitions: int | None = None) -> None:
        if not self.settings.auto_create_topics or topic in self._known_topics:
            return
        wanted = NewTopic(
            name=topic,
            num_partitions=partitions or self.settings.partitions,
            replication_factor=self.settings.replication_factor,
        )
        try:
            await self._admin.create_topics([wanted])
        except TopicAlreadyExistsError:
            pass
        except CONNECTION_ERRORS as exc:
            raise self._unavailable(exc) from exc
        self._known_topics.add(topic)

    async def _send(self, destination: str, topic: str, body: bytes, headers: Mapping[str, str]) -> None:
        await self._ensure_topic(topic)
        try:
            await self._producer.send_and_wait(topic, value=body, headers=_headers_out(headers))
        except CONNECTION_ERRORS as exc:
            raise self._unavailable(exc) from exc
        except KafkaError as exc:
            raise PublishNotConfirmed(destination, f"the cluster refused the record: {type(exc).__name__}") from exc

    async def publish(
        self, destination: str, message: TypedMessage, headers: Mapping[str, str] | None = None
    ) -> None:
        self._require_connected()
        body = self.codec.encode(message)
        hdrs = _check_headers(headers)
        high = hdrs.pop(PRIORITY_HEADER, None) == PRIORITY_HIGH
        await self._send(destination, topic_for(destination, high=high), body, hdrs)

    # ------------------------------------------------------------------ subscribing

    async def subscribe(
        self, destination: str, handler: Handler, options: SubscribeOptions | None = None
    ) -> Subscription:
        self._require_connected()
        opts = options or SubscribeOptions()
        main_topic, high_topic = topic_for(destination), topic_for(destination, high=True)
        retry_topic = main_topic + RETRY_SUFFIX
        for topic in (main_topic, high_topic, retry_topic):
            await self._ensure_topic(topic)
        group = f"das-{main_topic}"
        main = _Lane(self._factory.consumer(group, from_start=True))
        high = _Lane(self._factory.consumer(f"{group}-high", from_start=True))
        try:
            for lane, topics in ((main, [main_topic, retry_topic]), (high, [high_topic])):
                lane.consumer.subscribe(topics, listener=_Listener(self, lane))
                await lane.consumer.start()
        except Exception as exc:
            await self._stop_quietly(main.consumer)
            await self._stop_quietly(high.consumer)
            if isinstance(exc, CONNECTION_ERRORS):
                raise self._unavailable(exc) from exc
            raise
        subscription = Subscription(destination, handler, opts, on_cancel=self._unsubscribe)
        self.registry.add(subscription)
        stream = _Stream(main, high, Slots(opts.max_in_flight, opts.limiter))
        self._streams[subscription.id] = stream
        self._pollers[subscription.id] = asyncio.get_running_loop().create_task(self._poll(subscription, stream))
        return subscription

    async def _unsubscribe(self, subscription: Subscription) -> None:
        self.registry.remove(subscription)
        poller = self._pollers.pop(subscription.id, None)
        if poller is not None:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
        stream = self._streams.pop(subscription.id, None)
        if stream is not None:
            for lane in (stream.main, stream.high):
                await self._flush(lane)
                await self._stop_quietly(lane.consumer)

    async def _revoked(self, lane: _Lane, revoked: list[TopicPartition]) -> None:
        """Commit what is acknowledged for partitions that are taken away, then forget them (Req 3.6)."""
        await self._flush(lane, only=revoked)
        for partition in revoked:
            lane.pending.pop(partition, None)
            lane.tracker.forget(partition)
            lane.generation[partition] = lane.generation.get(partition, 0) + 1

    # ------------------------------------------------------------------ committing

    async def _flush(self, lane: _Lane, only: list[TopicPartition] | None = None) -> None:
        """Commit the points that are waiting. A rebalanced commit is dropped (the new owner redoes the message), a
        lost connection keeps it for the next try."""
        for partition, point in list(lane.pending.items()):
            if only is not None and partition not in only:
                continue
            try:
                await lane.consumer.commit({partition: point})
            except CommitFailedError:
                lane.pending.pop(partition, None)
                continue
            except CONNECTION_ERRORS:
                self._mark_down()
                return
            except KafkaError:
                log.warning("kafka commit failed", topic=partition.topic, exc_info=True)
                return
            if lane.pending.get(partition) == point:
                lane.pending.pop(partition, None)

    # ------------------------------------------------------------------ polling

    async def _poll(self, subscription: Subscription, stream: _Stream) -> None:
        slots = stream.slots
        warmed = False
        while subscription.active and not self._closing:
            try:
                await self.registry.wait_open()
                await self._flush(stream.high)
                await self._flush(stream.main)
                await self._release_due(subscription, stream)
                await slots.acquire_first()
                wanted = 1
                while wanted < self._batch and slots.try_more():
                    wanted += 1
                used = 0
                high_wait = 0 if warmed else HIGH_WARMUP_MS
                warmed = True
                try:
                    for lane, wait in ((stream.high, high_wait), (stream.main, POLL_TIMEOUT_MS)):
                        fetched = await lane.consumer.getmany(timeout_ms=wait, max_records=wanted)
                        for partition, records in fetched.items():
                            for record in records:
                                used += self._receive(subscription, stream, lane, partition, record)
                        if used:
                            break
                except BaseException:
                    for _ in range(wanted - used):
                        slots.release()
                    raise
                for _ in range(wanted - used):
                    slots.release()
            except asyncio.CancelledError:
                raise
            except CONNECTION_ERRORS:
                self._mark_down()
                await self._sleep(0.05)
            except Exception:  # noqa: BLE001 - a bad answer must not end the subscription
                log.warning("kafka poll failed", destination=subscription.destination, exc_info=True)
                await self._sleep(1.0)

    def _receive(
        self, subscription: Subscription, stream: _Stream, lane: _Lane, partition: TopicPartition, record: Any
    ) -> int:
        """Take one record. Returns 1 when it went to a handler now (it keeps its slot), 0 when it waits for its time
        (a retry): it is in the tracker either way, so the commit point cannot pass it."""
        lane.tracker.deliver(partition, record.offset)
        headers = _headers_in(record.headers)
        not_before = _count(headers.pop(NOT_BEFORE_HEADER, None))
        entry = (lane, partition, record, headers)
        if record.topic.endswith(RETRY_SUFFIX) and not_before > self._now() * 1000:
            heapq.heappush(stream.delayed, (not_before / 1000, next(stream.sequence), entry))
            return 0
        self._dispatch(subscription, *entry)
        return 1

    async def _release_due(self, subscription: Subscription, stream: _Stream) -> None:
        while stream.delayed and stream.delayed[0][0] <= self._now():
            _, _, entry = heapq.heappop(stream.delayed)
            await stream.slots.acquire_first()
            self._dispatch(subscription, *entry)

    def _dispatch(
        self, subscription: Subscription, lane: _Lane, partition: TopicPartition, record: Any, headers: dict[str, str]
    ) -> None:
        count = _count(headers.pop(COUNT_HEADER, None))
        reply_to = headers.pop(REPLY_TO_HEADER, None)
        correlation = headers.pop(CORRELATION_HEADER, None)
        message: TypedMessage | None = None
        error: ParseError | None = None
        try:
            message = self.codec.decode(record.value)
        except ParseError as exc:
            error = exc
        token = _Token(lane, partition, record.offset, lane.generation.get(partition, 0), subscription)
        delivery = Delivery(
            message=message, destination=subscription.destination, redelivered=count > 0, delivery_count=count,
            headers=headers, token=token, body=record.value, parse_error=error,
        )
        task = asyncio.get_running_loop().create_task(self._run_handler(subscription, delivery, reply_to, correlation))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_handler(
        self, subscription: Subscription, delivery: Delivery, reply_to: str | None, correlation: str | None
    ) -> None:
        token = delivery.token
        assert isinstance(token, _Token)
        try:
            reply = await subscription.handler(delivery)
        except Exception as exc:  # noqa: BLE001 - recorded; the message stays uncommitted and is delivered again
            log.exception("kafka handler for %s failed", delivery.destination)
            self.handler_errors.append(exc)
            self._lose(token)
            return
        if reply_to is None:
            return
        try:
            if reply is not None:
                await self._send(reply_to, reply_to, self.codec.encode(reply), {CORRELATION_HEADER: correlation or ""})
            await self.acknowledge(delivery, Outcome.ACK)
        except Exception as exc:  # noqa: BLE001
            log.warning("kafka reply to %s failed: %s", reply_to, exc)
            self._lose(token)

    # ------------------------------------------------------------------ settling

    async def acknowledge(self, delivery: Delivery, outcome: Outcome = Outcome.ACK) -> None:
        token = delivery.token
        if not isinstance(token, _Token):
            raise ValueError("delivery was not produced by a KafkaTransport")
        if token.settled:
            return
        self._require_connected()
        if outcome is Outcome.REQUEUE:
            await self._requeue(delivery, token)
        elif outcome is Outcome.REJECT:
            await self._dead_letter(delivery, token, "rejected", delivery.delivery_count)
        token.settled = True
        await self._commit(token)

    async def _requeue(self, delivery: Delivery, token: _Token) -> None:
        count = delivery.delivery_count + 1
        destination = token.subscription.destination
        if destination.startswith("jobs/") and count > self.settings.delivery_limit:
            await self._dead_letter(delivery, token, "delivery_limit", count)
            return
        due_ms = int((self._now() + self.settings.retry_delay) * 1000)
        headers = {**delivery.headers, COUNT_HEADER: str(count), NOT_BEFORE_HEADER: str(due_ms)}
        await self._send(destination, topic_for(destination) + RETRY_SUFFIX, delivery.body or b"", headers)

    async def _dead_letter(self, delivery: Delivery, token: _Token, reason: str, count: int) -> None:
        destination = token.subscription.destination
        if destination in self.settings.dead_letter_destinations:
            return  # a dead letter that is rejected again is dropped, as in the other transports
        headers = {
            **delivery.headers,
            "x-death-reason": reason,
            "x-original-destination": destination,
            "x-delivery-count": str(count),
        }
        for target in self.settings.dead_letter_destinations:
            await self._send(target, topic_for(target), delivery.body or b"", headers)

    async def _commit(self, token: _Token) -> None:
        lane = token.lane
        try:
            if token.generation == lane.generation.get(token.partition, 0):
                point = lane.tracker.ack(token.partition, token.offset)
                if point is not None:
                    lane.pending[token.partition] = point
                    await self._flush(lane)
        finally:
            self._release(token)

    def _release(self, token: _Token) -> None:
        stream = self._streams.get(token.subscription.id)
        if stream is not None:
            stream.slots.release()

    def _lose(self, token: _Token) -> None:
        """Give up a delivery that was not settled: nothing is committed for it, so it is delivered again."""
        if not token.settled:
            token.settled = True
            self._release(token)

    # ------------------------------------------------------------------ request-reply (Req 3.7)

    async def _ensure_reply(self) -> _Reply:
        async with self._reply_lock:
            if self._reply is not None:
                return self._reply
            topic = f"das.reply.{self._process}"
            await self._ensure_topic(topic, partitions=1)
            consumer = self._factory.consumer(None, from_start=False)
            try:
                await consumer.start()
                partition = TopicPartition(topic, 0)
                consumer.assign([partition])
                await consumer.seek_to_end(partition)
            except CONNECTION_ERRORS as exc:
                await self._stop_quietly(consumer)
                raise self._unavailable(exc) from exc
            reply = _Reply(topic, consumer)
            reply.task = asyncio.get_running_loop().create_task(self._reply_loop(reply))
            self._reply = reply
            return reply

    async def _reply_loop(self, reply: _Reply) -> None:
        while not self._closing:
            try:
                await self.registry.wait_open()
                fetched = await reply.consumer.getmany(timeout_ms=POLL_TIMEOUT_MS, max_records=self._batch)
            except asyncio.CancelledError:
                raise
            except CONNECTION_ERRORS:
                self._mark_down()
                await self._sleep(0.05)
                continue
            except Exception:  # noqa: BLE001
                log.warning("kafka reply poll failed", exc_info=True)
                await self._sleep(1.0)
                continue
            for records in fetched.values():
                for record in records:
                    waiting = reply.waiting.get(_headers_in(record.headers).get(CORRELATION_HEADER, ""))
                    if waiting is None or waiting.done():
                        continue
                    try:
                        waiting.set_result(self.codec.decode(record.value))
                    except ParseError as exc:
                        waiting.set_exception(TransportError(f"kafka reply was unreadable: {exc}"))

    async def request_reply(
        self, destination: str, message: TypedMessage, timeout: float | None = None
    ) -> TypedMessage:
        self._require_connected()
        limit = self.request_reply_timeout if timeout is None else float(timeout)
        reply = await self._ensure_reply()
        correlation = uuid.uuid4().hex
        future: asyncio.Future[TypedMessage] = asyncio.get_running_loop().create_future()
        reply.waiting[correlation] = future
        try:
            await self.publish(destination, message, {REPLY_TO_HEADER: reply.topic, CORRELATION_HEADER: correlation})
            try:
                return await asyncio.wait_for(future, limit)
            except TimeoutError:
                raise RequestTimeout(destination, limit) from None
        finally:
            reply.waiting.pop(correlation, None)


async def _nothing(subscription: Subscription) -> None:
    return None


__all__ = ["KafkaSettings", "KafkaTransport", "topic_for"]
