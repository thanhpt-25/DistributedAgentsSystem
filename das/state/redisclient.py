"""The one place that builds a Redis client (Phase 7 Req 1.4, 1.5, 1.8).

Every role (Gateway, Manager, Worker) gets its client from :func:`build_redis_client`, which honours ``redis.mode``:

* ``standalone``: ``Redis.from_url``, as before.
* ``sentinel``: the master that Redis Sentinel names. redis-py's Sentinel pool asks Sentinel for the master on every new
  connection, so after a Failover the next connection finds the new one without code of ours, and a ``READONLY`` from a
  demoted master drops the connection and asks again.

Both carry :class:`FailoverRetry`: a command that fails with a connection error, a timeout or ``READONLY`` is tried
again up to three times after 0.2, 0.5 and 1 second. That is safe for every State_Store script, because each answers a
repeat deterministically (the Lease epoch fences a stale owner, a finished Request answers ``REQUEST_TERMINAL``, writes
are keyed by ``(job_id, attempt)``), and a lost write is covered by at-least-once delivery with idempotent handling.

A test scans the package and fails if any other module creates a Redis client.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, Final, Protocol

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import Sentinel, SentinelConnectionPool
from redis.backoff import ConstantBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from das.config.schema import RedisConfig
from das.observability import get_logger

log = get_logger("das.state.redisclient")

#: Seconds to wait before each retry; three retries, so a command is tried at most four times (Req 1.5).
FAILOVER_BACKOFF: Final = (0.2, 0.5, 1.0)
#: What a Failover looks like to a client: a reset connection, a timeout, or a demoted master saying READONLY.
RETRYABLE: Final = (RedisConnectionError, RedisTimeoutError, ReadOnlyError)


class RedisObserver(Protocol):
    """Told about retries (by reason) and master switches (Req 1.8)."""

    def on_retry(self, reason: str) -> None: ...

    def on_failover(self) -> None: ...


class FailoverRetry(Retry):
    """redis-py's ``Retry`` with the Failover schedule, a count per retry, and a testable clock.

    ``on_retry(reason)`` is called only for a retry that will happen (``connection`` or ``readonly``); an exception
    from it never breaks the call. ``fail`` is redis-py's hook that disconnects the broken connection, so it runs for
    every failure, the last one included."""

    def __init__(
        self,
        on_retry: Callable[[str], None] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        schedule: tuple[float, ...] = FAILOVER_BACKOFF,
    ) -> None:
        super().__init__(ConstantBackoff(0), len(schedule), supported_errors=RETRYABLE)
        self._on_retry = on_retry
        self._sleep = sleep
        self._schedule = schedule

    def __deepcopy__(self, memo: dict[int, Any]) -> FailoverRetry:
        # redis-py deep-copies the Retry it is given; a copy would count into a copy of the observer.
        return self

    async def call_with_retry(self, do, fail, is_retryable=None, with_failure_count=False):
        failures = 0
        while True:
            try:
                return await do()
            except self._supported_errors as error:
                if is_retryable is not None and not is_retryable(error):
                    raise
                failures += 1
                if with_failure_count:
                    await fail(error, failures)
                else:
                    await fail(error)
                if failures > len(self._schedule):
                    raise
                self._count(error)
                await self._sleep(self._schedule[failures - 1])

    def _count(self, error: BaseException) -> None:
        if self._on_retry is None:
            return
        try:
            self._on_retry("readonly" if isinstance(error, ReadOnlyError) else "connection")
        except Exception:  # noqa: BLE001 - counting must never break a command
            log.warning("redis retry observer failed", exc_info=True)


class FailoverAwarePool(SentinelConnectionPool):
    """A Sentinel pool that tells its observer when the master it resolves changes (Req 1.8)."""

    def __init__(
        self, service_name: str, sentinel_manager: Any, *, observer: RedisObserver | None = None, **kwargs: Any
    ) -> None:
        self._observer = observer
        super().__init__(service_name, sentinel_manager, **kwargs)

    async def get_master_address(self) -> Any:
        previous = self.master_address
        address = await super().get_master_address()
        if previous is not None and address != previous and self._observer is not None:
            try:
                self._observer.on_failover()
            except Exception:  # noqa: BLE001 - counting must never break a connection
                log.warning("redis failover observer failed", exc_info=True)
        return address


class RedisEvents:
    """A :class:`RedisObserver` that can be handed to the client before the metrics exist.

    The client is built first (the metrics recorder needs configuration the agent reads afterwards), so events are
    counted here and passed on, with what was already counted, when :meth:`bind` gives it the metrics. The buffer is
    two integers per reason, so it cannot grow."""

    def __init__(self) -> None:
        self._retries: Counter[str] = Counter()
        self._failovers = 0
        self._target: Any = None

    def bind(self, target: Any) -> None:
        """``target`` has ``retry(reason)`` and ``failover()``; what was counted so far is passed on."""
        self._target = target
        for reason, count in self._retries.items():
            for _ in range(count):
                self._forward("retry", reason)
        for _ in range(self._failovers):
            self._forward("failover")
        self._retries.clear()
        self._failovers = 0

    def on_retry(self, reason: str) -> None:
        if self._target is None:
            self._retries[reason] += 1
        else:
            self._forward("retry", reason)

    def on_failover(self) -> None:
        if self._target is None:
            self._failovers += 1
        else:
            self._forward("failover")

    def _forward(self, method: str, *args: str) -> None:
        try:
            getattr(self._target, method)(*args)
        except Exception:  # noqa: BLE001 - metrics must never break the client
            log.warning("redis metrics failed", method=method, exc_info=True)


def _retry_for(observer: RedisObserver | None) -> FailoverRetry:
    return FailoverRetry(on_retry=observer.on_retry if observer is not None else None)


def client_from_url(url: str, *, observer: RedisObserver | None = None, **kwargs: Any) -> Redis:
    """A standalone client for ``url`` with the Failover retry (the Gateway's own pools use this)."""
    kwargs.setdefault("decode_responses", True)
    kwargs.setdefault("retry", _retry_for(observer))
    kwargs.setdefault("retry_on_error", list(RETRYABLE))
    return Redis.from_url(url, **kwargs)


def build_redis_client(
    config: RedisConfig,
    *,
    socket_timeout: float,
    connect_timeout: float,
    max_connections: int | None = None,
    observer: RedisObserver | None = None,
) -> Redis:
    """The client for ``config``: standalone by URL, or the Sentinel-managed master (Req 1.1, 1.2, 1.4)."""
    common: dict[str, Any] = {
        "decode_responses": True,
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": connect_timeout,
        "retry": _retry_for(observer),
        "retry_on_error": list(RETRYABLE),
    }
    if max_connections is not None:
        common["max_connections"] = max_connections
    if config.password is not None:
        common["password"] = config.password.get_secret_value()
    if config.mode != "sentinel":
        assert config.url is not None  # the schema requires it in standalone mode
        return Redis.from_url(config.url, **common)
    assert config.sentinels is not None and config.master_name is not None
    sentinel_kwargs: dict[str, Any] = {"socket_timeout": socket_timeout, "socket_connect_timeout": connect_timeout}
    if config.sentinel_password is not None:
        sentinel_kwargs["password"] = config.sentinel_password.get_secret_value()
    if config.sentinel_tls:
        sentinel_kwargs["ssl"] = True
        common["ssl"] = True
    addresses = [(host, int(port)) for host, _, port in (s.rpartition(":") for s in config.sentinels)]
    manager = Sentinel(addresses, sentinel_kwargs=sentinel_kwargs)
    pool_class: Any = partial(FailoverAwarePool, observer=observer)
    return manager.master_for(config.master_name, db=config.db, connection_pool_class=pool_class, **common)


__all__ = [
    "FAILOVER_BACKOFF",
    "RETRYABLE",
    "FailoverAwarePool",
    "FailoverRetry",
    "RedisEvents",
    "RedisObserver",
    "build_redis_client",
    "client_from_url",
]
