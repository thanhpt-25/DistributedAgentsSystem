"""The Redis dependency is unhealthy while Sentinel names no reachable master and healthy again once it names one
(das-scale-resilience task 8.5; Phase 7 Req 1.7). Real client, real health evaluation; the master is a tiny in-process
server that answers every command with ``+PONG``, and the Sentinel's answer is scripted."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from redis.asyncio.sentinel import MasterNotFoundError

from das.cli import _redis_check
from das.config.schema import RedisConfig
from das.health.server import CONNECTED, DISCONNECTED, HEALTHY, STATE_STORE, UNHEALTHY, evaluate_health
from das.state.redisclient import FailoverRetry, build_redis_client


async def _no_wait(seconds: float) -> None:
    return None


@pytest.fixture
async def pong_master() -> AsyncIterator[int]:
    """A server that answers each RESP command it receives with +PONG; yields its port."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(4096):
                writer.write(b"+PONG\r\n" * max(1, data.count(b"*")))
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()


class ScriptedSentinel:
    """Names no master for the first ``silent`` questions, then names the master at ``port``."""

    def __init__(self, port: int, silent: int) -> None:
        self.port, self.silent, self.asked = port, silent, 0

    async def discover_master(self, service_name: str) -> tuple[str, int]:
        self.asked += 1
        if self.asked <= self.silent:
            raise MasterNotFoundError(f"No master found for {service_name!r}")
        return ("127.0.0.1", self.port)


def sentinel_client(manager: Any) -> Any:
    config = RedisConfig.model_validate(
        {"mode": "sentinel", "sentinels": ["127.0.0.1:1"], "master_name": "das-master"}
    )
    client = build_redis_client(config, socket_timeout=1.0, connect_timeout=0.5)
    pool = client.connection_pool
    pool.sentinel_manager = manager
    pool.connection_kwargs["retry"] = FailoverRetry(sleep=_no_wait)  # no real waiting between the three retries
    return client


async def report(client: Any) -> dict[str, Any]:
    return await evaluate_health({STATE_STORE: _redis_check(client)})


async def test_without_a_master_the_dependency_is_unhealthy_and_nothing_raises(pong_master: int) -> None:
    client = sentinel_client(ScriptedSentinel(pong_master, silent=10_000))
    try:
        health = await report(client)
        assert health["status"] == UNHEALTHY and health["dependencies"] == {STATE_STORE: DISCONNECTED}
    finally:
        await client.aclose()


async def test_once_sentinel_names_a_master_the_dependency_is_healthy_again(pong_master: int) -> None:
    sentinel = ScriptedSentinel(pong_master, silent=4)  # the first ping asks 4 times (1 try + 3 retries) and fails
    client = sentinel_client(sentinel)
    try:
        assert (await report(client))["status"] == UNHEALTHY
        health = await report(client)
        assert health["status"] == HEALTHY and health["dependencies"] == {STATE_STORE: CONNECTED}
        assert sentinel.asked == 5
    finally:
        await client.aclose()


async def test_a_master_that_is_named_but_cannot_be_reached_is_unhealthy_too() -> None:
    client = sentinel_client(ScriptedSentinel(1, silent=0))  # nothing listens on port 1
    try:
        assert (await report(client))["status"] == UNHEALTHY
    finally:
        await client.aclose()


async def test_a_standalone_client_is_checked_the_same_way(pong_master: int) -> None:
    config = RedisConfig.model_validate({"url": f"redis://127.0.0.1:{pong_master}/0"})
    client = build_redis_client(config, socket_timeout=1.0, connect_timeout=0.5)
    try:
        assert (await report(client))["status"] == HEALTHY
    finally:
        await client.aclose()
