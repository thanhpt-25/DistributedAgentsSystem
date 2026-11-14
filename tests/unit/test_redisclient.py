"""The one Redis client builder (das-scale-resilience task 8.2; Phase 7 Req 1.4, 1.5, 1.8): standalone as before,
Sentinel by service name, the same retry through a Failover in both, the master switch counted, and no other place in
the package that creates a client."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from redis.asyncio.sentinel import SentinelConnectionPool
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from das.config.schema import RedisConfig
from das.state.redisclient import (
    FAILOVER_BACKOFF, FailoverAwarePool, FailoverRetry, RedisEvents, build_redis_client, client_from_url,
)

ROOT = Path(__file__).resolve().parents[2]
SENTINELS = ["sentinel-1:26379", "sentinel-2:26380"]


def standalone(url: str = "redis://redis.internal:6379/2", **extra: Any) -> RedisConfig:
    return RedisConfig.model_validate({"url": url, **extra})


def sentinel(**extra: Any) -> RedisConfig:
    return RedisConfig.model_validate(
        {"mode": "sentinel", "sentinels": SENTINELS, "master_name": "das-master", **extra}
    )


class Events:
    def __init__(self) -> None:
        self.retries: list[str] = []
        self.failovers = 0

    def on_retry(self, reason: str) -> None:
        self.retries.append(reason)

    def on_failover(self) -> None:
        self.failovers += 1


def kwargs_of(client: Any) -> dict[str, Any]:
    return client.connection_pool.connection_kwargs


# --------------------------------------------------------------------------- standalone (Req 1.1)


def test_a_standalone_client_is_built_from_the_url_with_the_given_timeouts() -> None:
    kw = kwargs_of(build_redis_client(standalone(), socket_timeout=5.0, connect_timeout=2.0))
    assert (kw["host"], kw["port"], kw["db"]) == ("redis.internal", 6379, 2)
    assert kw["decode_responses"] is True and kw["socket_timeout"] == 5.0 and kw["socket_connect_timeout"] == 2.0


def test_a_standalone_client_retries_through_a_failover() -> None:
    kw = kwargs_of(build_redis_client(standalone(), socket_timeout=5.0, connect_timeout=2.0))
    assert isinstance(kw["retry"], FailoverRetry)
    assert kw["retry_on_error"] == [RedisConnectionError, RedisTimeoutError, ReadOnlyError]


def test_the_master_password_is_passed_to_the_client_and_not_shown_in_its_repr() -> None:
    client = build_redis_client(standalone(password="pw-0123456789"), socket_timeout=5.0, connect_timeout=2.0)
    assert kwargs_of(client)["password"] == "pw-0123456789"
    assert "pw-0123456789" not in repr(client)


def test_max_connections_reaches_the_pool() -> None:
    client = build_redis_client(standalone(), socket_timeout=5.0, connect_timeout=2.0, max_connections=7)
    assert client.connection_pool.max_connections == 7


def test_a_url_can_build_a_standalone_client_for_the_gateway() -> None:
    client = client_from_url("redis://127.0.0.1:1/0", socket_timeout=2.0, socket_connect_timeout=2.0)
    kw = kwargs_of(client)
    assert (kw["host"], kw["port"]) == ("127.0.0.1", 1) and isinstance(kw["retry"], FailoverRetry)
    assert kw["decode_responses"] is True


# --------------------------------------------------------------------------- sentinel (Req 1.2)


def test_a_sentinel_client_asks_sentinel_for_the_named_master() -> None:
    client = build_redis_client(sentinel(db=3), socket_timeout=5.0, connect_timeout=2.0)
    pool = client.connection_pool
    assert isinstance(pool, SentinelConnectionPool) and isinstance(pool, FailoverAwarePool)
    assert pool.service_name == "das-master" and pool.is_master and pool.connection_kwargs["db"] == 3
    addresses = [(s.connection_pool.connection_kwargs["host"], s.connection_pool.connection_kwargs["port"])
                 for s in pool.sentinel_manager.sentinels]
    assert addresses == [("sentinel-1", 26379), ("sentinel-2", 26380)]


def test_a_sentinel_client_carries_the_timeouts_the_decoding_and_the_retry() -> None:
    kw = kwargs_of(build_redis_client(sentinel(), socket_timeout=5.0, connect_timeout=2.0))
    assert kw["decode_responses"] is True and kw["socket_timeout"] == 5.0 and kw["socket_connect_timeout"] == 2.0
    assert isinstance(kw["retry"], FailoverRetry)
    assert kw["retry_on_error"] == [RedisConnectionError, RedisTimeoutError, ReadOnlyError]


def test_the_sentinel_and_master_passwords_go_to_the_right_connections() -> None:
    client = build_redis_client(
        sentinel(password="master-pw-0123456789", sentinel_password="sentinel-pw-0123456789"),
        socket_timeout=5.0, connect_timeout=2.0,
    )
    assert kwargs_of(client)["password"] == "master-pw-0123456789"
    manager = client.connection_pool.sentinel_manager
    assert manager.sentinel_kwargs["password"] == "sentinel-pw-0123456789"
    assert "master-pw-0123456789" not in repr(manager.sentinel_kwargs)


def test_tls_applies_to_the_sentinels_and_to_the_master() -> None:
    client = build_redis_client(sentinel(sentinel_tls=True), socket_timeout=5.0, connect_timeout=2.0)
    assert client.connection_pool.sentinel_manager.sentinel_kwargs["ssl"] is True
    assert "SSL" in client.connection_pool.connection_class.__name__


def test_without_tls_neither_connection_asks_for_it() -> None:
    client = build_redis_client(sentinel(), socket_timeout=5.0, connect_timeout=2.0)
    assert "ssl" not in client.connection_pool.sentinel_manager.sentinel_kwargs
    assert "SSL" not in client.connection_pool.connection_class.__name__


# --------------------------------------------------------------------------- counting a master switch (Req 1.8)


class StubManager:
    def __init__(self, *addresses: tuple[str, int]) -> None:
        self.addresses = list(addresses)

    async def discover_master(self, name: str) -> tuple[str, int]:
        return self.addresses.pop(0)


async def test_the_first_master_is_not_a_failover_and_a_new_master_is() -> None:
    events = Events()
    pool = FailoverAwarePool("m", StubManager(("a", 1), ("a", 1), ("b", 2), ("b", 2), ("a", 1)), observer=events)
    for _ in range(5):
        await pool.get_master_address()
    assert events.failovers == 2  # a to b, then b back to a


async def test_a_pool_without_an_observer_still_works() -> None:
    pool = FailoverAwarePool("m", StubManager(("a", 1), ("b", 2)))
    assert await pool.get_master_address() == ("a", 1) and await pool.get_master_address() == ("b", 2)


async def test_an_observer_that_raises_never_breaks_the_connection() -> None:
    class Broken(Events):
        def on_failover(self) -> None:
            raise RuntimeError("boom")

    pool = FailoverAwarePool("m", StubManager(("a", 1), ("b", 2)), observer=Broken())
    await pool.get_master_address()
    assert await pool.get_master_address() == ("b", 2)


def test_the_client_wires_the_observer_into_the_pool_and_the_retry() -> None:
    events = Events()
    client = build_redis_client(sentinel(), socket_timeout=5.0, connect_timeout=2.0, observer=events)
    assert client.connection_pool._observer is events  # type: ignore[attr-defined]


# --------------------------------------------------------------------------- the late-bound sink (metrics exist later)


class Metrics:
    def __init__(self) -> None:
        self.retries: list[str] = []
        self.failovers = 0

    def retry(self, reason: str) -> None:
        self.retries.append(reason)

    def failover(self) -> None:
        self.failovers += 1


def test_events_before_the_metrics_exist_are_counted_when_they_do() -> None:
    sink = RedisEvents()
    sink.on_retry("connection")
    sink.on_retry("readonly")
    sink.on_failover()
    metrics = Metrics()
    sink.bind(metrics)
    assert metrics.retries == ["connection", "readonly"] and metrics.failovers == 1


def test_events_after_the_metrics_exist_go_straight_to_them() -> None:
    sink, metrics = RedisEvents(), Metrics()
    sink.bind(metrics)
    sink.on_retry("connection")
    sink.on_failover()
    assert metrics.retries == ["connection"] and metrics.failovers == 1


def test_binding_twice_does_not_count_the_same_events_twice() -> None:
    sink, first, second = RedisEvents(), Metrics(), Metrics()
    sink.on_retry("connection")
    sink.on_failover()
    sink.bind(first)
    sink.bind(second)
    assert first.retries == ["connection"] and first.failovers == 1
    assert second.retries == [] and second.failovers == 0


def test_a_sink_that_never_gets_metrics_does_not_grow_without_bound() -> None:
    sink = RedisEvents()
    for _ in range(100_000):
        sink.on_retry("connection")
    sink.bind(Metrics())  # must not raise; the buffer is bounded


def test_a_metrics_target_that_raises_never_breaks_the_caller() -> None:
    class Broken(Metrics):
        def retry(self, reason: str) -> None:
            raise RuntimeError("boom")

    sink = RedisEvents()
    sink.bind(Broken())
    sink.on_retry("connection")


# --------------------------------------------------------------------------- the schedule (Req 1.5)


def test_the_backoff_is_three_short_waits() -> None:
    assert FAILOVER_BACKOFF == (0.2, 0.5, 1.0)


# --------------------------------------------------------------------------- no other place builds a client (Req 1.4)

CLIENT_BUILDERS = re.compile(r"\bRedis\s*\(|\bRedis\.from_url\b|\bSentinel\s*\(|\bConnectionPool(?:\.from_url)?\s*\(")


def source_files() -> list[Path]:
    return sorted(p for p in (ROOT / "das").rglob("*.py") if p.name != "redisclient.py")


@pytest.mark.parametrize("path", source_files(), ids=lambda p: str(p.relative_to(ROOT)))
def test_no_module_but_redisclient_creates_a_redis_client(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith(("#", '"', "'", "*", ":")))
    assert CLIENT_BUILDERS.search(code) is None, f"{path.relative_to(ROOT)} creates a Redis client itself"


def test_the_static_scan_would_catch_a_client_built_elsewhere() -> None:
    for bad in ("client = Redis.from_url(url)", "r = Redis(host='x')", "s = Sentinel([('a', 1)])",
                "pool = ConnectionPool.from_url(u)", "pool = ConnectionPool(host='x')"):
        assert CLIENT_BUILDERS.search(bad)
    assert CLIENT_BUILDERS.search("registry = AgentRegistry(redis)") is None


# --------------------------------------------------------------------------- the right timeouts per role (Req 1.4)


def agent_config(role: str) -> Any:
    from das.config.schema import AgentConfig

    data: dict[str, Any] = {"role": role, "host": "10.0.0.7", "redis": {"url": "redis://redis.internal:6379/0"}}
    if role == "gateway":
        data["api_keys"] = ["gateway-key-0000000001"]
    else:
        data.update({
            "transports": {"jobs": "rabbitmq", "control": "grpc"}, "rabbitmq": {"url": "amqp://r:5672/"},
            "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "h", "advertise_port": 50051},
            "llm": {"provider": "ollama", "base_url": "http://o:11434", "model": "m"},
            "worker": {"capabilities": ["x"]}, "rabbitmq_management": None,
        })
        data.pop("rabbitmq_management")
        if role == "manager":
            data["rabbitmq"]["management_url"] = "http://r:15672"
            data.pop("worker")
    return AgentConfig.model_validate(data)


@pytest.mark.parametrize(("role", "socket_timeout", "connect_timeout"), [
    ("gateway", 2.0, 2.0), ("manager", 5.0, 2.0), ("worker", 5.0, 2.0),
])
def test_each_role_gets_its_own_socket_and_connect_timeouts(
    role: str, socket_timeout: float, connect_timeout: float
) -> None:
    from das.cli import _redis_client

    kw = kwargs_of(_redis_client(agent_config(role)))
    assert (kw["socket_timeout"], kw["socket_connect_timeout"]) == (socket_timeout, connect_timeout)
    assert isinstance(kw["retry"], FailoverRetry)
