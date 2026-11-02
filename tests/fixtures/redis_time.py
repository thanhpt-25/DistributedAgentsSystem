"""Controllable Redis ``TIME`` for fakeredis (design: Testing Strategy).

Lease expiry, retention and Dead_Letter timestamps use Redis server time
(``redis.call('TIME')`` inside Lua, or ``TIME`` from the client). fakeredis answers
``TIME`` from :func:`time.time`. :class:`FakeRedisTime` swaps the ``time`` module
seen by fakeredis' server command mixin for a shim reading the test's clock, so
both direct ``TIME`` calls and ``redis.call('TIME')`` in Lua follow it.

Only ``TIME`` (and ``SAVE``/``LASTSAVE`` bookkeeping, which share the module) is
affected. Key TTL expiry inside fakeredis still uses the real wall clock.

Usage::

    def test_x(fake_redis_time):
        fake_redis_time.set_ms(1_700_000_000_000)
        fake_redis_time.advance(5.0)

    def test_y(fake_redis_time_factory, fake_clock):
        t = fake_redis_time_factory(fake_clock)   # follows FakeClock.now()
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from types import SimpleNamespace

import pytest
from fakeredis.commands_mixins import server_mixin as _server_mixin

from das.clock import Clock

DEFAULT_START_S: float = 1_700_000_000.0


class FakeRedisTime:
    """Test-controlled source for fakeredis ``TIME``.

    Without a ``clock`` the time is held internally and moved with :meth:`set` /
    :meth:`advance`. With a ``clock`` (e.g. :class:`das.clock.FakeClock`), ``TIME``
    returns ``clock.now()`` and :meth:`set` / :meth:`advance` are not available.
    """

    def __init__(self, clock: Clock | None = None, start: float = DEFAULT_START_S) -> None:
        self._clock = clock
        self._now = float(start)
        self._original: object | None = None

    # ------------------------------------------------------------- time source
    def now(self) -> float:
        """Current Redis time in seconds since the epoch."""
        return self._clock.now() if self._clock is not None else self._now

    def now_ms(self) -> int:
        """Current Redis time in integer ms, as Lua scripts compute it from ``TIME``."""
        now_us = round(self.now() * 1_000_000)
        return now_us // 1_000

    def set(self, seconds: float) -> None:
        self._require_own_time()
        self._now = float(seconds)

    def set_ms(self, ms: int) -> None:
        self.set(ms / 1000)

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("Redis time cannot move backwards via advance()")
        self._require_own_time()
        self._now += seconds

    def _require_own_time(self) -> None:
        if self._clock is not None:
            raise RuntimeError("FakeRedisTime follows an external clock; move that clock instead")

    # ------------------------------------------------------------- patching
    def install(self) -> FakeRedisTime:
        if self._original is not None:
            raise RuntimeError("FakeRedisTime already installed")
        self._original = _server_mixin.time
        _server_mixin.time = SimpleNamespace(time=self.now)  # type: ignore[assignment]
        return self

    def uninstall(self) -> None:
        if self._original is not None:
            _server_mixin.time = self._original  # type: ignore[assignment]
            self._original = None

    def __enter__(self) -> FakeRedisTime:
        return self.install()

    def __exit__(self, *exc: object) -> None:
        self.uninstall()


@pytest.fixture
def fake_redis_time() -> Iterator[FakeRedisTime]:
    """fakeredis ``TIME`` driven by an internal, manually advanced clock."""
    with FakeRedisTime() as t:
        yield t


@pytest.fixture
def fake_redis_time_factory() -> Iterator[Callable[..., FakeRedisTime]]:
    """Build a :class:`FakeRedisTime` (optionally following a ``Clock``); undone at teardown."""
    installed: list[FakeRedisTime] = []

    def make(clock: Clock | None = None, start: float = DEFAULT_START_S) -> FakeRedisTime:
        if installed:
            raise RuntimeError("only one FakeRedisTime may be active per test")
        t = FakeRedisTime(clock, start).install()
        installed.append(t)
        return t

    yield make
    for t in installed:
        t.uninstall()


__all__ = ["FakeRedisTime", "fake_redis_time", "fake_redis_time_factory"]
