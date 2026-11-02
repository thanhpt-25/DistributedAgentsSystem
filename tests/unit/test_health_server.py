"""Unit tests for the health endpoint (Requirement 11.5)."""

from __future__ import annotations

import asyncio
import itertools
import time

import httpx
import pytest

from das.clock import FakeClock
from das.health.server import (
    LLM_PROVIDER,
    STATE_STORE,
    TRANSPORT,
    CachedCheck,
    HealthServer,
    create_health_app,
    evaluate_health,
)


def const(value: bool):
    async def check() -> bool:
        return value

    return check


async def raises() -> bool:
    raise ConnectionError("down")


async def hangs() -> bool:
    await asyncio.sleep(60)
    return True


async def get_health(app) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get("/healthz")


@pytest.mark.parametrize("states", list(itertools.product([True, False], repeat=3)))
async def test_truth_table(states: tuple[bool, bool, bool]) -> None:
    names = (TRANSPORT, STATE_STORE, LLM_PROVIDER)
    app = create_health_app({n: const(s) for n, s in zip(names, states, strict=True)})
    resp = await get_health(app)
    body = resp.json()
    expected = {n: "connected" if s else "disconnected" for n, s in zip(names, states, strict=True)}
    assert body["dependencies"] == expected
    if all(states):
        assert resp.status_code == 200 and body["status"] == "healthy"
    else:
        assert resp.status_code == 503 and body["status"] == "unhealthy"


async def test_exception_and_timeout_report_disconnected() -> None:
    app = create_health_app(
        {TRANSPORT: const(True), STATE_STORE: raises, LLM_PROVIDER: hangs},
        check_timeout=0.05,
        overall_timeout=0.2,
    )
    start = time.monotonic()
    resp = await get_health(app)
    assert time.monotonic() - start < 1.0
    assert resp.status_code == 503
    assert resp.json()["dependencies"] == {
        TRANSPORT: "connected",
        STATE_STORE: "disconnected",
        LLM_PROVIDER: "disconnected",
    }


async def test_checks_run_concurrently() -> None:
    async def slow_ok() -> bool:
        await asyncio.sleep(0.1)
        return True

    start = time.monotonic()
    report = await evaluate_health(
        {TRANSPORT: slow_ok, STATE_STORE: slow_ok, LLM_PROVIDER: slow_ok},
        check_timeout=1.0,
        overall_timeout=2.0,
    )
    assert time.monotonic() - start < 0.25
    assert report["status"] == "healthy"


async def test_overall_bound_cancels_slow_checks() -> None:
    # Per-check timeout larger than the overall bound: overall bound wins.
    start = time.monotonic()
    report = await evaluate_health({TRANSPORT: hangs}, check_timeout=10.0, overall_timeout=0.05)
    assert time.monotonic() - start < 0.5
    assert report == {"status": "unhealthy", "dependencies": {TRANSPORT: "disconnected"}}


async def test_non_true_result_is_disconnected() -> None:
    async def returns_none():
        return None

    report = await evaluate_health({TRANSPORT: returns_none})
    assert report["dependencies"][TRANSPORT] == "disconnected"


async def test_cached_check_caches_for_ttl() -> None:
    clock = FakeClock()
    calls = 0

    async def ping() -> bool:
        nonlocal calls
        calls += 1
        return True

    cached = CachedCheck(ping, ttl=30.0, clock=clock)
    assert await cached() is True
    clock.advance(29.9)
    assert await cached() is True
    assert calls == 1
    clock.advance(0.2)
    assert await cached() is True
    assert calls == 2


async def test_cached_check_caches_failures_and_exceptions() -> None:
    clock = FakeClock()
    calls = 0

    async def ping() -> bool:
        nonlocal calls
        calls += 1
        raise RuntimeError("provider down")

    cached = CachedCheck(ping, ttl=30.0, clock=clock)
    assert await cached() is False
    assert await cached() is False
    assert calls == 1
    clock.advance(30.0)
    assert await cached() is False
    assert calls == 2


async def test_cached_check_timeout_caches_nothing() -> None:
    clock = FakeClock()
    calls = 0

    async def ping() -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(60)
        return True

    cached = CachedCheck(ping, ttl=30.0, clock=clock)
    report = await evaluate_health({LLM_PROVIDER: cached}, check_timeout=0.05)
    assert report["dependencies"][LLM_PROVIDER] == "disconnected"
    report = await evaluate_health({LLM_PROVIDER: cached}, check_timeout=0.05)
    assert report["dependencies"][LLM_PROVIDER] == "connected"
    assert calls == 2


async def test_cached_check_shares_inflight_call() -> None:
    calls = 0

    async def ping() -> bool:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return True

    cached = CachedCheck(ping, ttl=30.0, clock=FakeClock())
    results = await asyncio.gather(cached(), cached(), cached())
    assert results == [True, True, True]
    assert calls == 1


async def test_health_server_serves_and_stops() -> None:
    server = HealthServer(create_health_app({TRANSPORT: const(True)}), port=0, host="127.0.0.1")
    await server.start()
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"http://127.0.0.1:{server.port}/healthz", timeout=5.0)
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"
    finally:
        await server.stop()
    await server.stop()  # idempotent
