"""The HTTP Transport: agents exchange Messages through the HTTP_Broker over plain HTTPS (Phase 7 Req 2).

It implements the Transport protocol like the RabbitMQ adapter does, with the broker (``das.transport.httpbroker``,
served by the Gateway on its Ops_Port) as the queue:

* ``publish`` returns only after the broker has stored the Message (a broker error is :class:`PublishNotConfirmed`, an
  unreachable broker :class:`TransportUnavailable`).
* ``subscribe`` long-polls ``/pull``: one poll task per subscription, taking a slot of ``max_in_flight`` and of the
  shared ``limiter`` for every Message it asks for, so a slow handler never holds more than its share. A Message that
  is not settled within the broker's visibility timeout is delivered again.
* ``acknowledge`` maps ACK, REQUEUE and REJECT to ``/settle``; a handler that raises leaves its Message unsettled, so it
  is redelivered.
* ``request_reply`` publishes with a per-call reply destination (``das-reply-to``) and a correlation id, and long-polls
  the reply destination until the timeout. The responder is a subscriber whose handler returns the reply.
* A lost connection closes the delivery gate (:class:`SubscriptionRegistry`), the reconnect loop pings the broker on the
  :class:`ReconnectPolicy` schedule, and when it answers every subscription resumes (Req 8.6, 8.8, 8.9).

The codec bytes and the headers are the same as on the other transports; ``das-priority`` is consumed by the broker.
"""

from __future__ import annotations

import asyncio
import base64
import itertools
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Final

import httpx

from das.codec.message_codec import MessageCodec, ParseError, TypedMessage
from das.observability import get_logger
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

log = get_logger("das.transport.http")

#: Reserved headers of a request-reply call; the adapter consumes them (they are never delivered to a handler).
REPLY_TO_HEADER: Final = "das-reply-to"
CORRELATION_HEADER: Final = "das-correlation-id"
DEFAULT_POLL_WAIT: Final = 20.0
MAX_POLL_WAIT: Final = 30.0
#: Messages one poll asks for at most (it never asks for more than it has free slots for).
PULL_BATCH: Final = 10
_PATH: Final = "/broker/v1/"


class _Refused(Exception):
    """The broker answered with an error status."""

    def __init__(self, status: int) -> None:
        super().__init__(f"broker answered {status}")
        self.status = status


@dataclass(slots=True)
class _Token:
    """The ``Delivery.token`` of an HTTP delivery: where to settle it, and whether that has happened."""

    destination: str
    id: str
    subscription: Subscription
    settled: bool = False


@dataclass(slots=True)
class _Slots:
    """How many unsettled deliveries a subscription may hold: its own ``max_in_flight`` and the shared limiter's."""

    max_in_flight: int | None
    limiter: Any
    used: int = 0
    _freed: asyncio.Event = field(default_factory=asyncio.Event)

    async def acquire_first(self) -> None:
        while self.max_in_flight is not None and self.used >= self.max_in_flight:
            self._freed.clear()
            await self._freed.wait()
        if self.limiter is not None:
            await self.limiter.acquire()
        self.used += 1

    def try_more(self) -> bool:
        if self.max_in_flight is not None and self.used >= self.max_in_flight:
            return False
        if self.limiter is not None and not self.limiter.try_acquire():
            return False
        self.used += 1
        return True

    def release(self) -> None:
        self.used = max(0, self.used - 1)
        if self.limiter is not None:
            self.limiter.release()
        self._freed.set()


def _plain(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.:-]", "-", name)[:100] or "agent"


def _check_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    result = dict(headers or {})
    for key, value in result.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("transport headers must map str to str")
    return result


async def _nothing(subscription: Subscription) -> None:
    return None


class HttpTransport:
    """:class:`~das.transport.base.Transport` over the HTTP_Broker."""

    name: ClassVar[str] = "http"

    def __init__(
        self,
        url: str,
        token: str,
        *,
        agent_id: str = "http",
        codec: MessageCodec | None = None,
        request_reply_timeout: float = DEFAULT_REQUEST_REPLY_TIMEOUT,
        poll_wait: float = DEFAULT_POLL_WAIT,
        reconnect_policy: ReconnectPolicy | None = None,
        client: httpx.AsyncClient | None = None,
        sleep: Any = asyncio.sleep,
        batch: int = PULL_BATCH,
    ) -> None:
        if not url.startswith(("http://", "https://")):
            raise ValueError("url must be an http or https URL")
        if not token:
            raise ValueError("the broker token must not be empty")
        if not 0 < poll_wait <= MAX_POLL_WAIT:
            raise ValueError(f"poll_wait must be above 0 and at most {MAX_POLL_WAIT:g} seconds")
        if batch < 1:
            raise ValueError("batch must be positive")
        self.url = url.rstrip("/")
        self.agent_id = agent_id
        self.codec = codec or MessageCodec()
        self.request_reply_timeout = validate_request_reply_timeout(request_reply_timeout)
        self.poll_wait = float(poll_wait)
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self.registry = SubscriptionRegistry()
        self.handler_errors: list[BaseException] = []
        self._auth = {"Authorization": f"Bearer {token}"}
        self._client = client
        self._owns_client = client is None
        self._sleep = sleep
        self._batch = batch
        self._connected = False
        self._closing = False
        self._pollers: dict[int, asyncio.Task[None]] = {}
        self._slots: dict[int, _Slots] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._reconnect_task: asyncio.Task[None] | None = None

    @classmethod
    def from_config(cls, config: Any, **kwargs: Any) -> HttpTransport:
        """Build from an Agent_Config ``http`` section (``url``, the secret ``token`` and ``poll_wait``)."""
        http = config.http
        if http is None:
            raise ValueError("Agent_Config has no http section")
        if http.token is None:
            raise ValueError("Agent_Config has no http token (set DAS__HTTP__TOKEN)")
        limits = config.limits
        options: dict[str, Any] = {
            "url": http.url,
            "token": http.token.get_secret_value(),
            "agent_id": config.agent_id or "http",
            "codec": MessageCodec(limits.max_message_bytes),
            "request_reply_timeout": limits.request_reply_timeout,
            "poll_wait": http.poll_wait,
            "reconnect_policy": ReconnectPolicy(limits.max_reconnect_interval),
        }
        options.update(kwargs)
        return cls(**options)

    # ------------------------------------------------------------------ lifecycle

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        if self._connected:
            return
        self._closing = False
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
            self._owns_client = True
        try:
            await self._ping()
        except TransportUnavailable as exc:
            raise TransportUnavailable(self.name, f"cannot connect: {exc.reason}") from exc
        self._connected = True

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        for subscription in self.registry.active():
            await subscription.cancel()
        tasks = [t for t in (*self._tasks, self._reconnect_task) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------ the broker

    async def _post(self, path: str, payload: dict[str, Any], *, timeout: float | None = None) -> Any:
        client = self._client
        if client is None:
            raise TransportUnavailable(self.name, "not started")
        try:
            response = await client.post(
                f"{self.url}{_PATH}{path}", json=payload, headers=self._auth, timeout=timeout or 10.0
            )
        except httpx.HTTPError as exc:
            self._mark_down()
            raise TransportUnavailable(self.name, f"broker unreachable: {type(exc).__name__}") from exc
        if response.status_code == 401:
            raise TransportUnavailable(self.name, "the broker refused the token")
        if response.status_code >= 400:
            raise _Refused(response.status_code)
        return response.json()

    async def _ping(self) -> None:
        client = self._client
        if client is None:
            raise TransportUnavailable(self.name, "not started")
        try:
            response = await client.post(f"{self.url}{_PATH}ping", headers=self._auth, timeout=10.0)
        except httpx.HTTPError as exc:
            raise TransportUnavailable(self.name, f"broker unreachable: {type(exc).__name__}") from exc
        if response.status_code != 200:
            raise TransportUnavailable(self.name, f"the broker answered {response.status_code}")

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
                await self._ping()
            except TransportUnavailable:
                continue
            self._connected = True
            await self.registry.restore(_nothing)  # polling is stateless: reopening the gate resumes subscriptions
            log.info("http transport reconnected", agent_id=self.agent_id)
            return

    # ------------------------------------------------------------------ operations

    async def publish(
        self, destination: str, message: TypedMessage, headers: Mapping[str, str] | None = None
    ) -> None:
        self._require_connected()
        body = self.codec.encode(message)
        hdrs = _check_headers(headers)
        try:
            await self._post(
                "publish",
                {"destination": destination, "body": base64.b64encode(body).decode("ascii"), "headers": hdrs},
            )
        except _Refused as exc:
            raise PublishNotConfirmed(destination, f"the broker answered {exc.status}") from exc

    async def subscribe(
        self, destination: str, handler: Handler, options: SubscribeOptions | None = None
    ) -> Subscription:
        self._require_connected()
        opts = options or SubscribeOptions()
        subscription = Subscription(destination, handler, opts, on_cancel=self._unsubscribe)
        self.registry.add(subscription)
        self._slots[subscription.id] = _Slots(opts.max_in_flight, opts.limiter)
        self._pollers[subscription.id] = asyncio.get_running_loop().create_task(self._poll(subscription))
        return subscription

    async def _unsubscribe(self, subscription: Subscription) -> None:
        self.registry.remove(subscription)
        poller = self._pollers.pop(subscription.id, None)
        if poller is not None:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)

    async def acknowledge(self, delivery: Delivery, outcome: Outcome = Outcome.ACK) -> None:
        token = delivery.token
        if not isinstance(token, _Token):
            raise ValueError("delivery was not produced by an HttpTransport")
        if token.settled:
            return
        self._require_connected()
        token.settled = True  # before the call, so two settlements of one delivery cannot both go out
        try:
            await self._post("settle", {"destination": token.destination, "id": token.id, "outcome": outcome.value})
        except _Refused as exc:
            raise TransportError(f"the broker refused to settle a delivery: {exc.status}") from exc
        finally:
            self._release(token)

    def _release(self, token: _Token) -> None:
        slots = self._slots.get(token.subscription.id)
        if slots is not None:
            slots.release()

    def _lose(self, token: _Token) -> None:
        """Give up a delivery that was not settled: the broker redelivers it after its visibility timeout."""
        if not token.settled:
            token.settled = True
            self._release(token)

    async def request_reply(
        self, destination: str, message: TypedMessage, timeout: float | None = None
    ) -> TypedMessage:
        limit = self.request_reply_timeout if timeout is None else float(timeout)
        reply_to, correlation = f"reply.{uuid.uuid4().hex}", uuid.uuid4().hex
        await self.publish(destination, message, {REPLY_TO_HEADER: reply_to, CORRELATION_HEADER: correlation})
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit
        consumer = f"{_plain(self.agent_id)}.rr"
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RequestTimeout(destination, limit)
            wait = min(remaining, self.poll_wait)
            data = await self._post(
                "pull", {"destination": reply_to, "consumer": consumer, "max": 1, "wait": wait}, timeout=wait + 10.0
            )
            for item in data["deliveries"]:
                await self._post("settle", {"destination": reply_to, "id": item["id"], "outcome": "ack"})
                if item.get("headers", {}).get(CORRELATION_HEADER, correlation) != correlation:
                    continue
                try:
                    return self.codec.decode(base64.b64decode(item["body"]))
                except ParseError as exc:
                    raise TransportError(f"request-reply to {destination!r} got an unreadable reply: {exc}") from exc

    async def ready_count(self, destination: str) -> int:
        """``QueueInspector``: Messages waiting in ``destination`` that no consumer holds."""
        self._require_connected()
        return int((await self._post("depth", {"destination": destination}))["ready"])

    # ------------------------------------------------------------------ polling

    async def _poll(self, subscription: Subscription) -> None:
        slots = self._slots[subscription.id]
        consumer = f"{_plain(self.agent_id)}.{subscription.id}"
        while subscription.active and not self._closing:
            try:
                await self.registry.wait_open()
                await slots.acquire_first()
                wanted = 1
                while wanted < self._batch and slots.try_more():
                    wanted += 1
                try:
                    data = await self._post(
                        "pull",
                        {"destination": subscription.destination, "consumer": consumer, "max": wanted,
                         "wait": self.poll_wait},
                        timeout=self.poll_wait + 10.0,
                    )
                except BaseException:
                    for _ in range(wanted):
                        slots.release()
                    raise
                items = data["deliveries"]
                for _ in range(wanted - len(items)):
                    slots.release()
                for item in items:
                    self._dispatch(subscription, item)
            except asyncio.CancelledError:
                raise
            except TransportUnavailable:
                await self._sleep(0.05)  # the gate is closed by _mark_down; this only paces a refused token
            except Exception:  # noqa: BLE001 - a bad answer must not end the subscription
                log.warning("http poll failed", destination=subscription.destination, exc_info=True)
                await self._sleep(1.0)

    def _dispatch(self, subscription: Subscription, item: Mapping[str, Any]) -> None:
        headers = dict(item.get("headers") or {})
        reply_to = headers.pop(REPLY_TO_HEADER, None)
        correlation = headers.pop(CORRELATION_HEADER, None)
        body = base64.b64decode(item["body"])
        message: TypedMessage | None = None
        error: ParseError | None = None
        try:
            message = self.codec.decode(body)
        except ParseError as exc:
            error = exc
        count = int(item.get("delivery_count", 0))
        token = _Token(subscription.destination, item["id"], subscription)
        delivery = Delivery(
            message=message, destination=subscription.destination, redelivered=count > 0, delivery_count=count,
            headers=headers, token=token, body=body, parse_error=error,
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
        except Exception as exc:  # noqa: BLE001 - recorded; the Message stays unsettled and is redelivered
            log.exception("http handler for %s failed", delivery.destination)
            self.handler_errors.append(exc)
            self._lose(token)
            return
        if reply_to is None:
            return
        try:
            if reply is not None:
                await self.publish(reply_to, reply, {CORRELATION_HEADER: correlation or ""})
            await self.acknowledge(delivery, Outcome.ACK)
        except Exception as exc:  # noqa: BLE001
            log.warning("http reply to %s failed: %s", reply_to, exc)
            self._lose(token)


__all__ = ["CORRELATION_HEADER", "REPLY_TO_HEADER", "HttpTransport"]
