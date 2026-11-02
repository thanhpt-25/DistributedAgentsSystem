"""Unit tests for the gRPC Transport adapter (task 8.6).

Each test runs real ``grpc.aio`` servers on ephemeral localhost ports.

Requirements: 8.3, 8.7, 8.8
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from importlib.metadata import EntryPoint

import pytest

from das.codec import MessageCodec, new_message
from das.models.results import ControlPayload
from das.transport.base import (
    Delivery,
    Outcome,
    PublishNotConfirmed,
    RequestTimeout,
    Transport,
    TransportUnavailable,
    missing_operations,
)
from das.transport.grpc_rpc import GrpcTransport, registry_resolver
from das.transport.loader import load_transport

TS = datetime(2025, 1, 1, tzinfo=UTC)


def control(command: str = "cancel_job", job_id: str = "j1"):
    return new_message(
        ControlPayload(command=command, request_id="r1", job_id=job_id),
        correlation_id="",
        message_id=f"m-{command}-{job_id}",
        timestamp=TS,
    )


class Directory:
    """Test resolver: agent ID -> endpoint of a started transport."""

    def __init__(self) -> None:
        self.endpoints: dict[str, str] = {}

    async def __call__(self, agent_id: str) -> str | None:
        return self.endpoints.get(agent_id)

    async def add(self, transport: GrpcTransport) -> GrpcTransport:
        await transport.start()
        self.endpoints[transport.agent_id] = f"127.0.0.1:{transport.bound_port}"
        return transport


@pytest.fixture
async def pair() -> AsyncIterator[tuple[GrpcTransport, GrpcTransport]]:
    directory = Directory()
    manager = await directory.add(
        GrpcTransport(agent_id="manager-1", bind="127.0.0.1:0", resolver=directory,
                      request_reply_timeout=1)
    )
    worker = await directory.add(
        GrpcTransport(agent_id="worker-1", bind="127.0.0.1:0", resolver=directory,
                      request_reply_timeout=1)
    )
    yield manager, worker
    await manager.close()
    await worker.close()


def test_conforms_to_transport_protocol() -> None:
    assert missing_operations(GrpcTransport) == []
    assert GrpcTransport.name == "grpc"
    # Same target as the pyproject entry point (independent of install metadata).
    ep = EntryPoint("grpc", "das.transport.grpc_rpc:GrpcTransport", "das.transports")
    assert load_transport("grpc", source=lambda: [ep]) is GrpcTransport
    t = GrpcTransport(agent_id="a", bind="127.0.0.1:0", resolver=lambda _: None)
    assert isinstance(t, Transport)


async def test_send_completes_on_acknowledge(pair) -> None:
    manager, worker = pair
    received: list[Delivery] = []
    release = asyncio.Event()

    async def handler(delivery: Delivery) -> None:
        received.append(delivery)
        await release.wait()
        await worker.acknowledge(delivery)

    await worker.subscribe("worker-1", handler)
    msg = control()
    publish = asyncio.create_task(
        manager.publish("worker-1", msg, headers={"x-trace": "abc"})
    )
    # The Send stays pending until the handler acknowledges.
    for _ in range(100):
        if received:
            break
        await asyncio.sleep(0.01)
    assert worker.pending_sends() == 1
    assert not publish.done()
    release.set()
    await asyncio.wait_for(publish, 2)

    delivery = received[0]
    assert delivery.message == msg
    assert delivery.body == MessageCodec().encode(msg)  # wire bytes == codec bytes (9.3)
    assert delivery.headers == {"x-trace": "abc"}
    assert delivery.destination == "worker-1"
    assert worker.pending_sends() == 0


@pytest.mark.parametrize("outcome", [Outcome.REJECT, Outcome.REQUEUE])
async def test_send_not_confirmed_when_not_acked(pair, outcome: Outcome) -> None:
    manager, worker = pair

    async def handler(delivery: Delivery) -> None:
        await worker.acknowledge(delivery, outcome)

    await worker.subscribe("worker-1", handler)
    with pytest.raises(PublishNotConfirmed) as info:
        await manager.publish("worker-1", control())
    assert info.value.destination == "worker-1"
    assert outcome.value in info.value.reason


async def test_send_not_confirmed_when_handler_fails(pair) -> None:
    manager, worker = pair

    async def handler(delivery: Delivery) -> None:
        raise RuntimeError("boom")

    await worker.subscribe("worker-1", handler)
    with pytest.raises(PublishNotConfirmed, match="boom"):
        await manager.publish("worker-1", control())
    assert len(worker.handler_errors) == 1


async def test_send_without_subscriber_is_not_confirmed(pair) -> None:
    manager, _ = pair
    with pytest.raises(PublishNotConfirmed, match="no subscriber"):
        await manager.publish("worker-1", control())


async def test_call_returns_reply(pair) -> None:
    manager, worker = pair
    reply = control("cancel_request", "j2")

    async def handler(delivery: Delivery):
        assert delivery.message == control()
        await worker.acknowledge(delivery)  # no-op for request-reply deliveries
        return reply

    await worker.subscribe("worker-1", handler)
    result = await manager.request_reply("worker-1", control())
    assert result == reply


async def test_call_timeout_names_destination(pair) -> None:
    manager, worker = pair

    async def handler(delivery: Delivery):
        await asyncio.sleep(5)

    await worker.subscribe("worker-1", handler)
    with pytest.raises(RequestTimeout) as info:
        await manager.request_reply("worker-1", control(), timeout=0.2)
    assert info.value.destination == "worker-1"
    assert "worker-1" in str(info.value)


async def test_call_without_reply_times_out(pair) -> None:
    manager, worker = pair

    async def handler(delivery: Delivery):
        return None

    await worker.subscribe("worker-1", handler)
    with pytest.raises(RequestTimeout) as info:
        await manager.request_reply("worker-1", control(), timeout=0.2)
    assert info.value.destination == "worker-1"


async def test_operations_while_not_started_raise_unavailable() -> None:
    t = GrpcTransport(agent_id="a", bind="127.0.0.1:0", resolver=lambda _: "127.0.0.1:1")
    with pytest.raises(TransportUnavailable):
        await t.publish("b", control())
    with pytest.raises(TransportUnavailable):
        await t.request_reply("b", control())


async def test_operations_after_close_raise_unavailable(pair) -> None:
    manager, _ = pair
    await manager.close()
    with pytest.raises(TransportUnavailable):
        await manager.publish("worker-1", control())


async def test_unknown_destination_raises_unavailable(pair) -> None:
    manager, _ = pair
    with pytest.raises(TransportUnavailable, match="ghost"):
        await manager.publish("ghost", control())


async def test_unreachable_endpoint_raises_unavailable() -> None:
    directory = Directory()
    sender = await directory.add(
        GrpcTransport(agent_id="s", bind="127.0.0.1:0", resolver=directory,
                      request_reply_timeout=2)
    )
    # Bind and stop a server to get a port that refuses connections.
    probe = await directory.add(
        GrpcTransport(agent_id="gone", bind="127.0.0.1:0", resolver=directory)
    )
    await probe.close()
    try:
        with pytest.raises(TransportUnavailable):
            await sender.publish("gone", control())
    finally:
        await sender.close()


async def test_subscribe_only_own_agent_id(pair) -> None:
    _, worker = pair

    async def handler(delivery: Delivery) -> None: ...

    with pytest.raises(ValueError):
        await worker.subscribe("someone-else", handler)


async def test_registry_resolver_reads_grpc_endpoint() -> None:
    class Record:
        grpc_endpoint = "10.0.0.5:50051"

    class Snapshot:
        def get(self, agent_id: str):
            return Record() if agent_id == "w1" else None

    class Registry:
        async def snapshot(self):
            return Snapshot()

    resolve = registry_resolver(Registry())
    assert await resolve("w1") == "10.0.0.5:50051"
    assert await resolve("missing") is None
