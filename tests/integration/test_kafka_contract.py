"""The shared transport contract against a real Kafka (das-scale-resilience task 10.4; Phase 7 Req 3.10).

Runs the same ``tests/transport_contract.py`` that the in-memory fake and the HTTP transport pass, with
``confluentinc/cp-kafka`` in KRaft mode under testcontainers: real topics, real consumer groups, real offset commits.
Skipped when Docker is not reachable (a failure under ``DAS_REQUIRE_DOCKER=1``).

A lost connection is injected at the client boundary (``FlakyFactory`` wraps the real clients and fails them while the
rig says the network is down): what is under test is how the adapter reacts to the errors the clients raise, against a
real cluster for everything else. Topics are deleted after each test, so the destinations the contract shares do not
carry messages from one test to the next.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from aiokafka.errors import KafkaConnectionError

import tests.transport_contract as contract
from das.transport.base import ReconnectPolicy, Transport
from das.transport.kafka import AioKafkaFactory, KafkaSettings, KafkaTransport
from tests.transport_contract import TransportContract, until

pytestmark = pytest.mark.integration

IMAGE = "confluentinc/cp-kafka:7.6.0"
#: A cold consumer group takes seconds to form on a real broker; the in-memory suites need far less.
SLOW_UNTIL = 60.0


def _docker_available() -> bool:
    try:
        import docker

        client = docker.from_env(timeout=5)
        try:
            return bool(client.ping())
        finally:
            client.close()
    except Exception:
        return False


@pytest.fixture(scope="module")
def bootstrap() -> Iterator[str]:
    if not _docker_available():
        pytest.skip("Docker is not available; the Kafka contract tests need a real broker")
    from testcontainers.community.kafka import KafkaContainer

    container = KafkaContainer(IMAGE).with_kraft()
    container.start()
    try:
        server = container.get_bootstrap_server()
        _wait_ready(server, time.monotonic() + 90)
        yield server
    finally:
        container.stop()


def _wait_ready(server: str, deadline: float) -> None:
    from aiokafka.admin import AIOKafkaAdminClient

    async def probe() -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=server)
        await admin.start()
        try:
            await admin.list_topics()
        finally:
            await admin.close()

    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            asyncio.run(probe())
            return
        except Exception as exc:
            last = exc
            time.sleep(1.0)
    raise RuntimeError(f"Kafka did not become ready: {last}")


class _Gate:
    """Whether one transport's network is up."""

    def __init__(self) -> None:
        self.down = False

    def check(self) -> None:
        if self.down:
            raise KafkaConnectionError("the network is down")


class _Flaky:
    """Wraps a real client: the calls the adapter makes fail while the gate says down, all else passes through."""

    GUARDED = ("send_and_wait", "getmany", "commit", "list_topics", "create_topics", "delete_topics")

    def __init__(self, inner: Any, gate: _Gate) -> None:
        self._inner, self._gate = inner, gate

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._inner, name)
        if name not in self.GUARDED:
            return attribute

        async def guarded(*args: Any, **kwargs: Any) -> Any:
            self._gate.check()
            return await attribute(*args, **kwargs)

        return guarded


class FlakyFactory:
    def __init__(self, inner: AioKafkaFactory, gate: _Gate) -> None:
        self._inner, self._gate = inner, gate

    def producer(self) -> Any:
        return _Flaky(self._inner.producer(), self._gate)

    def consumer(self, group_id: str | None, *, from_start: bool) -> Any:
        return _Flaky(self._inner.consumer(group_id, from_start=from_start), self._gate)

    def admin(self) -> Any:
        return _Flaky(self._inner.admin(), self._gate)


class RealKafkaRig:
    name = "kafka"
    supports_priority = True
    redelivery_delay = 0.5

    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self.made: list[tuple[KafkaTransport, _Gate]] = []

    async def new(self, agent_id: str) -> Transport:
        settings = KafkaSettings((self.bootstrap,), partitions=3, retry_delay=0.5, delivery_limit=3)
        gate = _Gate()
        transport = KafkaTransport(
            settings, agent_id=agent_id, factory=FlakyFactory(AioKafkaFactory(settings), gate),
            request_reply_timeout=20, reconnect_policy=ReconnectPolicy(1.0),
        )
        await transport.start()
        self.made.append((transport, gate))
        return transport

    def _gate(self, transport: Any) -> _Gate:
        return next(gate for made, gate in self.made if made is transport)

    async def drop(self, transport: Any) -> None:
        self._gate(transport).down = True

    async def reconnect(self, transport: Any) -> None:
        self._gate(transport).down = False
        await until(lambda: transport.connected, timeout=30)

    async def close(self) -> None:
        for transport, _ in self.made:
            await transport.close()
        await self._delete_all_topics()

    async def _delete_all_topics(self) -> None:
        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap)
        await admin.start()
        try:
            names = [t for t in await admin.list_topics() if not t.startswith("__")]
            if names:
                await admin.delete_topics(names)
            for _ in range(60):  # deletion is asynchronous: wait until the names can be created again
                if not [t for t in await admin.list_topics() if not t.startswith("__")]:
                    return
                await asyncio.sleep(0.5)
        finally:
            await admin.close()


class TestKafka(TransportContract):
    @pytest.fixture(autouse=True)
    def _patient(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(contract, "UNTIL_TIMEOUT", SLOW_UNTIL)

    @pytest.fixture
    async def rig(self, bootstrap: str) -> AsyncIterator[RealKafkaRig]:
        rig = RealKafkaRig(bootstrap)
        yield rig
        await rig.close()
