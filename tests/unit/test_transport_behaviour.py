"""Every Transport adapter against the shared behaviour contract (das-scale-resilience task 9.1; Phase 7 Req 6.2, 2.1).

The in-memory fake and the HTTP transport (talking to the broker through its ASGI app, over fakeredis streams) run the
same ``tests/transport_contract.py``, and so does the Kafka adapter over an in-memory cluster (wave 10)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from starlette.applications import Starlette

from das.transport.base import ReconnectPolicy, Transport
from das.transport.http import HttpTransport
from das.transport.httpbroker import BrokerSettings, broker_routes
from das.transport.kafka import KafkaSettings, KafkaTransport
from tests.fakes.kafka_cluster import FakeCluster
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport
from tests.transport_contract import TransportContract, until

TOKEN = "contract-token-0123456789abcdef"


class MemoryRig:
    name = "memory"
    supports_priority = False
    redelivery_delay = 0.0

    def __init__(self) -> None:
        self.broker = MemoryBroker(("dlq", "dlq-events"))

    async def new(self, agent_id: str) -> Transport:
        transport = MemoryTransport(self.broker, agent_id=agent_id)
        await transport.start()
        return transport

    async def drop(self, transport: Any) -> None:
        transport.simulate_drop()

    async def reconnect(self, transport: Any) -> None:
        await transport.simulate_reconnect()

    async def close(self) -> None:
        return None


class Flaky(httpx.AsyncBaseTransport):
    """An ASGI transport that can be told the network is down."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.down = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            raise httpx.ConnectError("network is down", request=request)
        return await self.inner.handle_async_request(request)


class HttpRig:
    name = "http"
    supports_priority = True
    redelivery_delay = 0.3

    def __init__(self, redis: FakeRedis) -> None:
        settings = BrokerSettings(token=TOKEN, visibility_timeout=self.redelivery_delay, delivery_limit=3)
        self.app = Starlette(routes=broker_routes(redis, settings))
        self.redis = redis
        self.made: list[tuple[HttpTransport, Flaky]] = []

    async def new(self, agent_id: str) -> Transport:
        flaky = Flaky(httpx.ASGITransport(app=self.app))
        client = httpx.AsyncClient(transport=flaky, base_url="http://broker")

        async def quick(seconds: float) -> None:
            await asyncio.sleep(0.05)

        transport = HttpTransport(
            "http://broker", TOKEN, agent_id=agent_id, client=client, poll_wait=0.3, request_reply_timeout=5,
            reconnect_policy=ReconnectPolicy(1.0), sleep=quick,
        )
        await transport.start()
        self.made.append((transport, flaky))
        return transport

    def _flaky(self, transport: Any) -> Flaky:
        return next(flaky for made, flaky in self.made if made is transport)

    async def drop(self, transport: Any) -> None:
        self._flaky(transport).down = True

    async def reconnect(self, transport: Any) -> None:
        self._flaky(transport).down = False
        await until(lambda: transport.connected, timeout=10)

    async def close(self) -> None:
        for transport, _ in self.made:
            await transport.close()


class KafkaRig:
    """The Kafka adapter over the in-memory cluster (``tests/fakes/kafka_cluster.py``)."""

    name = "kafka"
    supports_priority = True
    redelivery_delay = 0.3

    def __init__(self) -> None:
        self.cluster = FakeCluster(partitions=3)
        self.made: list[KafkaTransport] = []

    async def new(self, agent_id: str) -> Transport:
        async def quick(seconds: float) -> None:
            await asyncio.sleep(0.05)

        settings = KafkaSettings(("fake:9092",), retry_delay=0.1, delivery_limit=3)
        transport = KafkaTransport(
            settings, agent_id=agent_id, factory=self.cluster.factory(agent_id), request_reply_timeout=5,
            reconnect_policy=ReconnectPolicy(1.0), sleep=quick,
        )
        await transport.start()
        self.made.append(transport)
        return transport

    async def drop(self, transport: Any) -> None:
        self.cluster.down.add(transport.agent_id)

    async def reconnect(self, transport: Any) -> None:
        self.cluster.down.discard(transport.agent_id)
        await until(lambda: transport.connected, timeout=10)

    async def close(self) -> None:
        for transport in self.made:
            await transport.close()


class TestMemoryTransport(TransportContract):
    @pytest.fixture
    async def rig(self) -> AsyncIterator[MemoryRig]:
        rig = MemoryRig()
        yield rig
        await rig.close()


class TestHttpTransport(TransportContract):
    @pytest.fixture
    async def rig(self) -> AsyncIterator[HttpRig]:
        redis = FakeRedis(server=FakeServer(), decode_responses=True)
        rig = HttpRig(redis)
        yield rig
        await rig.close()
        await redis.aclose()


class TestKafkaTransport(TransportContract):
    @pytest.fixture
    async def rig(self) -> AsyncIterator[KafkaRig]:
        rig = KafkaRig()
        yield rig
        await rig.close()
