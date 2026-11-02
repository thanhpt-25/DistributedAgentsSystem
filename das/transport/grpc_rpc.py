"""gRPC Transport adapter (Requirements 8.3, 8.7, 8.8).

Carries control commands (``cancel_job``, ``cancel_request``) and direct
agent-to-agent request-reply calls over the ``das.v1.AgentControl`` service
defined in ``das/transport/proto/das.proto``:

- ``Send(Frame) -> Ack`` backs :meth:`GrpcTransport.publish`. The receiving
  side hands the Message to its subscriber and completes the RPC only when the
  subscriber settles the delivery through :meth:`GrpcTransport.acknowledge`
  (``ACK`` -> ``Ack(ok=True)``; ``REQUEUE``/``REJECT`` or a handler error ->
  ``Ack(ok=False)``, which the sender reports as :class:`PublishNotConfirmed`).
- ``Call(Frame) -> Frame`` backs :meth:`GrpcTransport.request_reply`. The
  subscriber's return value is the reply (see :mod:`das.transport.base`).

A destination is an agent ID. It is resolved to a ``host:port`` endpoint by an
injected resolver (normally the Agent_Registry, see :func:`registry_resolver`).
Every agent serves exactly one destination, its own ID: ``subscribe`` accepts
only ``destination == agent_id`` and registers the handler on the local
``grpc.aio`` server.

Frame bytes are produced by the shared :class:`~das.codec.MessageCodec`, so the
wire bytes equal ``MessageCodec.encode(message)`` (Req 9.3).

gRPC channels reconnect on their own; the channel backoff options follow the
:class:`~das.transport.base.ReconnectPolicy` schedule (1 s doubling up to the
configured maximum, Req 8.6). Server-side subscriptions live in-process and are
not affected by outbound connection drops.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Final

import grpc
import grpc.aio

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
from das.transport.proto import das_pb2, das_pb2_grpc

#: Resolves an agent ID to a ``host:port`` endpoint, or ``None`` when unknown.
EndpointResolver = Callable[[str], "Awaitable[str | None] | str | None"]

#: Prefix for Message headers carried as gRPC metadata.
HEADER_PREFIX: Final = "das-h-"
#: Extra bytes allowed on top of the codec limit for the Frame wrapper.
_FRAME_OVERHEAD: Final = 1024
#: Seconds the server waits for in-flight RPCs on close.
_STOP_GRACE: Final = 1.0


# --------------------------------------------------------------------------- tokens


_send_ids = itertools.count(1)


@dataclass(slots=True, eq=False)
class _SendToken:
    """Pending ``Send`` RPC; settled by :meth:`GrpcTransport.acknowledge`."""

    transport: GrpcTransport
    subscription: Subscription
    future: asyncio.Future[das_pb2.Ack]
    id: int = field(default_factory=lambda: next(_send_ids))

    def settle(self, ack: das_pb2.Ack) -> bool:
        if self.future.done():
            return False
        self.future.set_result(ack)
        return True


@dataclass(frozen=True, slots=True)
class _ReplyToken:
    """Token of a request-reply delivery; acknowledging it is a no-op."""


# --------------------------------------------------------------------------- servicer


class _AgentControlServicer(das_pb2_grpc.AgentControlServicer):
    def __init__(self, transport: GrpcTransport) -> None:
        self._transport = transport

    async def Send(  # noqa: N802 - gRPC method name
        self, request: das_pb2.Frame, context: grpc.aio.ServicerContext
    ) -> das_pb2.Ack:
        return await self._transport._serve_send(request, context)

    async def Call(  # noqa: N802 - gRPC method name
        self, request: das_pb2.Frame, context: grpc.aio.ServicerContext
    ) -> das_pb2.Frame:
        return await self._transport._serve_call(request, context)


# --------------------------------------------------------------------------- transport


class GrpcTransport:
    """RPC_Transport backed by ``grpc.aio`` (Req 8.3)."""

    name: ClassVar[str] = "grpc"

    def __init__(
        self,
        *,
        agent_id: str,
        bind: str,
        resolver: EndpointResolver,
        codec: MessageCodec | None = None,
        request_reply_timeout: float = DEFAULT_REQUEST_REPLY_TIMEOUT,
        send_timeout: float | None = None,
        reconnect_policy: ReconnectPolicy | None = None,
    ) -> None:
        if not agent_id:
            raise ValueError("agent_id must not be empty")
        self.agent_id = agent_id
        self.bind = bind
        self.resolver = resolver
        self.codec = codec or MessageCodec()
        self.request_reply_timeout = validate_request_reply_timeout(request_reply_timeout)
        #: Deadline for a ``Send`` to be acknowledged; defaults to the request-reply timeout.
        self.send_timeout = (
            self.request_reply_timeout
            if send_timeout is None
            else validate_request_reply_timeout(send_timeout)
        )
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self.registry = SubscriptionRegistry()
        #: Port the server is bound to after :meth:`start` (resolves ``:0``).
        self.bound_port: int | None = None
        self.handler_errors: list[BaseException] = []
        self._server: grpc.aio.Server | None = None
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, das_pb2_grpc.AgentControlStub] = {}
        self._pending: dict[int, _SendToken] = {}
        self._semaphores: dict[int, asyncio.Semaphore] = {}
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._rr = 0
        self._started = False

    @classmethod
    def from_config(cls, config: Any, *, resolver: EndpointResolver, **kwargs: Any) -> GrpcTransport:
        """Build from a validated Agent_Config; ``resolver`` maps agent IDs to endpoints."""
        if config.grpc is None:
            raise ValueError("Agent_Config has no grpc section")
        limits = config.limits
        options: dict[str, Any] = {
            "agent_id": str(config.agent_id),
            "bind": config.grpc.bind,
            "resolver": resolver,
            "codec": MessageCodec(limits.max_message_bytes),
            "request_reply_timeout": limits.request_reply_timeout,
            "reconnect_policy": ReconnectPolicy(limits.max_reconnect_interval),
        }
        options.update(kwargs)
        return cls(**options)

    # ----------------------------------------------------------- lifecycle

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        if self._started:
            return
        server = grpc.aio.server(options=self._channel_options())
        das_pb2_grpc.add_AgentControlServicer_to_server(_AgentControlServicer(self), server)
        port = server.add_insecure_port(self.bind)
        if port == 0:
            raise TransportUnavailable(self.name, f"cannot bind gRPC server to {self.bind!r}")
        await server.start()
        self._server = server
        self.bound_port = port
        self._started = True

    async def close(self) -> None:
        self._started = False
        for token in list(self._pending.values()):
            token.settle(das_pb2.Ack(ok=False, error="transport closing"))
        self._pending.clear()
        for subscription in self.registry.active():
            self.registry.remove(subscription)
            await subscription.cancel()
        if self._server is not None:
            await self._server.stop(_STOP_GRACE)
            self._server = None
        tasks = list(self._handler_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        channels = list(self._channels.values())
        self._channels.clear()
        self._stubs.clear()
        for channel in channels:
            await channel.close()

    # ----------------------------------------------------------- operations

    async def publish(
        self,
        destination: str,
        message: TypedMessage,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._require_started()
        body = self.codec.encode(message)
        metadata = _encode_headers(headers)
        stub = await self._stub_for(destination)
        try:
            ack = await stub.Send(
                das_pb2.Frame(envelope=body), timeout=self.send_timeout, metadata=metadata
            )
        except grpc.aio.AioRpcError as exc:
            code = exc.code()
            if code == grpc.StatusCode.UNAVAILABLE:
                raise TransportUnavailable(
                    self.name, f"agent {destination!r} unreachable: {exc.details()}"
                ) from exc
            if code == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise PublishNotConfirmed(
                    destination, f"not acknowledged within {self.send_timeout:g} s"
                ) from exc
            raise PublishNotConfirmed(destination, f"{code.name}: {exc.details()}") from exc
        if not ack.ok:
            raise PublishNotConfirmed(destination, ack.error or "not acknowledged")

    async def subscribe(
        self,
        destination: str,
        handler: Handler,
        options: SubscribeOptions | None = None,
    ) -> Subscription:
        self._require_started()
        if destination != self.agent_id:
            raise ValueError(
                f"gRPC transport of {self.agent_id!r} can only subscribe to its own "
                f"agent ID, not {destination!r}"
            )
        opts = options or SubscribeOptions()
        subscription = Subscription(destination, handler, opts, on_cancel=self._unsubscribe)
        if opts.max_in_flight is not None:
            self._semaphores[subscription.id] = asyncio.Semaphore(opts.max_in_flight)
        self.registry.add(subscription)
        return subscription

    async def acknowledge(self, delivery: Delivery, outcome: Outcome = Outcome.ACK) -> None:
        token = delivery.token
        if isinstance(token, _ReplyToken):
            return
        if not isinstance(token, _SendToken):
            raise ValueError("delivery was not produced by a GrpcTransport")
        if token.transport is not self:
            raise ValueError("delivery belongs to another transport")
        if outcome is Outcome.ACK:
            ack = das_pb2.Ack(ok=True)
        else:
            ack = das_pb2.Ack(ok=False, error=outcome.value)
        # Settling twice, or after the sender gave up, is a no-op.
        token.settle(ack)

    async def request_reply(
        self,
        destination: str,
        message: TypedMessage,
        timeout: float | None = None,
    ) -> TypedMessage:
        self._require_started()
        limit = self.request_reply_timeout if timeout is None else float(timeout)
        body = self.codec.encode(message)
        try:
            async with asyncio.timeout(limit):
                stub = await self._stub_for(destination)
                reply = await stub.Call(das_pb2.Frame(envelope=body), timeout=limit)
        except TimeoutError:
            raise RequestTimeout(destination, limit) from None
        except grpc.aio.AioRpcError as exc:
            code = exc.code()
            if code == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise RequestTimeout(destination, limit) from exc
            if code == grpc.StatusCode.UNAVAILABLE:
                raise TransportUnavailable(
                    self.name, f"agent {destination!r} unreachable: {exc.details()}"
                ) from exc
            raise TransportError(
                f"request-reply to {destination!r} failed: {code.name}: {exc.details()}"
            ) from exc
        try:
            return self.codec.decode(reply.envelope)
        except ParseError as exc:
            raise TransportError(f"invalid reply from {destination!r}: {exc}") from exc

    # ----------------------------------------------------------- inspection

    @property
    def subscriptions(self) -> list[Subscription]:
        return self.registry.active()

    def pending_sends(self) -> int:
        """``Send`` RPCs delivered to a handler but not yet acknowledged."""
        return len(self._pending)

    # ----------------------------------------------------------- server side

    async def _serve_send(
        self, request: das_pb2.Frame, context: grpc.aio.ServicerContext
    ) -> das_pb2.Ack:
        await self.registry.wait_open()
        subscription = self._pick_subscription()
        if subscription is None:
            return das_pb2.Ack(ok=False, error=f"agent {self.agent_id!r} has no subscriber")
        future: asyncio.Future[das_pb2.Ack] = asyncio.get_running_loop().create_future()
        token = _SendToken(self, subscription, future)
        semaphore = self._semaphores.get(subscription.id)
        limiter = subscription.options.limiter
        if semaphore is not None:
            await semaphore.acquire()
        if limiter is not None:
            await limiter.acquire()
        self._pending[token.id] = token
        delivery = self._make_delivery(request.envelope, _decode_headers(context), token)
        task = asyncio.create_task(self._run_send_handler(subscription.handler, delivery, token))
        # The handler may outlive the RPC; keep a strong reference until it finishes.
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)
        try:
            return await asyncio.shield(future)
        finally:
            # Sender gave up (deadline / cancel) or the delivery was settled.
            if not future.done():
                future.cancel()
            self._pending.pop(token.id, None)
            if limiter is not None:
                limiter.release()
            if semaphore is not None:
                semaphore.release()

    async def _run_send_handler(
        self, handler: Handler, delivery: Delivery, token: _SendToken
    ) -> None:
        try:
            await handler(delivery)
        except Exception as exc:
            self.handler_errors.append(exc)
            token.settle(das_pb2.Ack(ok=False, error=f"handler failed: {exc}"))

    async def _serve_call(
        self, request: das_pb2.Frame, context: grpc.aio.ServicerContext
    ) -> das_pb2.Frame:
        await self.registry.wait_open()
        subscription = self._pick_subscription()
        if subscription is None:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION, f"agent {self.agent_id!r} has no subscriber"
            )
        assert subscription is not None
        delivery = self._make_delivery(request.envelope, _decode_headers(context), _ReplyToken())
        if delivery.parse_error is not None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(delivery.parse_error))
        try:
            reply = await subscription.handler(delivery)
        except Exception as exc:
            self.handler_errors.append(exc)
            await context.abort(grpc.StatusCode.INTERNAL, f"handler failed: {exc}")
        if reply is None:
            # No reply: the caller's deadline produces RequestTimeout (base convention).
            await asyncio.Event().wait()
        assert reply is not None  # the wait above never returns normally
        return das_pb2.Frame(envelope=self.codec.encode(reply))

    def _pick_subscription(self) -> Subscription | None:
        active = self.registry.active()
        if not active:
            return None
        subscription = active[self._rr % len(active)]
        self._rr += 1
        return subscription

    def _make_delivery(self, body: bytes, headers: dict[str, str], token: object) -> Delivery:
        message: TypedMessage | None = None
        error: ParseError | None = None
        try:
            message = self.codec.decode(body)
        except ParseError as exc:
            error = exc
        return Delivery(
            message=message,
            destination=self.agent_id,
            redelivered=False,
            delivery_count=0,
            headers=headers,
            token=token,
            body=bytes(body),
            parse_error=error,
        )

    async def _unsubscribe(self, subscription: Subscription) -> None:
        self.registry.remove(subscription)
        self._semaphores.pop(subscription.id, None)

    # ----------------------------------------------------------- client side

    def _require_started(self) -> None:
        if not self._started:
            raise TransportUnavailable(self.name, f"agent {self.agent_id!r} is not started")

    async def _resolve(self, destination: str) -> str:
        result = self.resolver(destination)
        endpoint = await result if inspect.isawaitable(result) else result
        if not endpoint:
            raise TransportUnavailable(
                self.name, f"no gRPC endpoint registered for agent {destination!r}"
            )
        return str(endpoint)

    async def _stub_for(self, destination: str) -> das_pb2_grpc.AgentControlStub:
        endpoint = await self._resolve(destination)
        stub = self._stubs.get(endpoint)
        if stub is None:
            channel = grpc.aio.insecure_channel(endpoint, options=self._channel_options())
            self._channels[endpoint] = channel
            stub = self._stubs[endpoint] = das_pb2_grpc.AgentControlStub(channel)
        return stub

    def _channel_options(self) -> list[tuple[str, Any]]:
        max_bytes = self.codec.max_bytes + _FRAME_OVERHEAD
        initial_ms = int(self.reconnect_policy.delay(1) * 1000)
        max_ms = int(self.reconnect_policy.max_interval * 1000)
        return [
            ("grpc.max_send_message_length", max_bytes),
            ("grpc.max_receive_message_length", max_bytes),
            ("grpc.initial_reconnect_backoff_ms", initial_ms),
            ("grpc.min_reconnect_backoff_ms", initial_ms),
            ("grpc.max_reconnect_backoff_ms", max_ms),
        ]


# --------------------------------------------------------------------------- helpers


def _encode_headers(headers: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
    """Message headers -> gRPC metadata (keys are lowercased, as gRPC requires)."""
    result: list[tuple[str, str]] = []
    for key, value in (headers or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("transport headers must map str to str")
        result.append((HEADER_PREFIX + key.lower(), value))
    return tuple(result)


def _decode_headers(context: grpc.aio.ServicerContext) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, value in context.invocation_metadata() or ():
        if key.startswith(HEADER_PREFIX) and isinstance(value, str):
            headers[key[len(HEADER_PREFIX) :]] = value
    return headers


def registry_resolver(registry: Any) -> EndpointResolver:
    """Resolve agent IDs through an Agent_Registry (``snapshot().get(id).grpc_endpoint``)."""

    async def resolve(agent_id: str) -> str | None:
        snapshot = await registry.snapshot()
        record = snapshot.get(agent_id)
        return None if record is None else record.grpc_endpoint

    return resolve


__all__ = ["HEADER_PREFIX", "EndpointResolver", "GrpcTransport", "registry_resolver"]
