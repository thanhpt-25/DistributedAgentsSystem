"""Unit tests for LoopSupervisor, EventLoopWatchdog and the Ops app (Req 1.1, 1.3, 1.7, 1.9, 16)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from das.clock import FakeClock
from das.health.server import (
    LLM_PROVIDER,
    NOT_DRAINING,
    NOT_SHUTTING_DOWN,
    REGISTERED_AVAILABLE,
    STARTUP,
    STATE_STORE,
    CachedCheck,
    LivenessSource,
    agent_readiness,
    create_ops_app,
    gateway_readiness,
)
from das.health.supervisor import EventLoopWatchdog, LoopSupervisor
from das.observability.metrics import CONTENT_TYPE, MetricsRecorder, NoopRecorder


class ListLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str, dict[str, Any]]] = []

    def warning(self, event: str, **fields: Any) -> None:
        self.entries.append(("warning", event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.entries.append(("error", event, fields))

    def events(self, name: str) -> list[dict[str, Any]]:
        return [f for _, e, f in self.entries if e == name]


def const(value: bool):
    async def check() -> bool:
        return value

    return check


async def hangs() -> bool:
    await asyncio.sleep(60)
    return True


async def forever() -> None:
    await asyncio.Event().wait()


class Flags:
    def __init__(self) -> None:
        self.started = True
        self.draining = False
        self.shutting = False


def build(
    *,
    metrics: MetricsRecorder | None = None,
    clock: FakeClock | None = None,
    flags: Flags | None = None,
    checks: dict[str, bool] | None = None,
    llm: Any = None,
    healthz: Any = None,
    log: Any = None,
) -> tuple[Any, LoopSupervisor, EventLoopWatchdog, FakeClock, Flags]:
    clock = clock or FakeClock()
    flags = flags or Flags()
    checks = {"registered": True, "rpc": True, "store": True, **(checks or {})}
    supervisor = LoopSupervisor(log=ListLog())
    watchdog = EventLoopWatchdog(clock)
    readiness = agent_readiness(
        startup=lambda: flags.started,
        registered_available=const(checks["registered"]),
        transports={"rpc": const(checks["rpc"])},
        state_store=const(checks["store"]),
        draining=lambda: flags.draining,
        shutting_down=lambda: flags.shutting,
        llm_provider=llm,
    )
    app = create_ops_app(
        metrics=metrics,
        liveness=LivenessSource(supervisor=supervisor, watchdog=watchdog, clock=clock),
        readiness=readiness,
        healthz=healthz,
        log=log,
    )
    return app, supervisor, watchdog, clock, flags


async def get(app: Any, path: str, **headers: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path, headers=headers)


# --- supervisor / watchdog ---------------------------------------------------------------


async def test_supervisor_reports_exited_raised_and_cancelled_loops() -> None:
    log = ListLog()
    sup = LoopSupervisor(log=log)

    async def boom() -> None:
        raise RuntimeError("x")

    async def done() -> None:
        return None

    alive = asyncio.create_task(forever())
    tasks = {n: asyncio.create_task(f()) for n, f in (("boom", boom), ("done", done))}
    cancelled = asyncio.create_task(forever())
    sup.register("alive", alive)
    for name, task in tasks.items():
        sup.register(name, task)
    sup.register("cancelled", cancelled)
    cancelled.cancel()
    await asyncio.gather(*tasks.values(), cancelled, return_exceptions=True)
    await asyncio.sleep(0)

    assert sup.ended() == ["boom", "done", "cancelled"]
    outcomes = {e["loop"]: e["outcome"] for e in log.events("critical_loop_ended")}
    assert outcomes == {"boom": "error", "done": "exited", "cancelled": "cancelled"}
    alive.cancel()


async def test_watchdog_ticks_every_second_and_goes_stale_after_5s() -> None:
    clock = FakeClock()
    wd = EventLoopWatchdog(clock)
    assert not wd.stale(clock.monotonic())
    task = wd.start()
    for _ in range(3):
        await asyncio.sleep(0)
    await wd.stop()
    assert task.done()
    assert clock.sleeps and all(s == 1.0 for s in clock.sleeps)
    last = wd.last_tick
    assert not wd.stale(last + 5.0)
    assert wd.stale(last + 5.01)


# --- /metrics ------------------------------------------------------------------------------


@pytest.mark.parametrize("key", [None, "user-key", "admin-key", "garbage"])
async def test_metrics_200_regardless_of_api_key(key: str | None) -> None:
    rec = MetricsRecorder(clock=FakeClock(), log=ListLog())
    rec.inc("das_requests_submitted_total")
    app, *_ = build(metrics=rec)
    headers = {"X-API-Key": key} if key else {}
    resp = await get(app, "/metrics", **headers)
    assert resp.status_code == 200
    assert resp.headers["content-type"] == CONTENT_TYPE
    names = {f.name for f in text_string_to_metric_families(resp.text)}
    assert "das_requests_submitted" in names


@pytest.mark.parametrize("metrics", [None, NoopRecorder()])
async def test_metrics_404_when_disabled_but_probes_still_served(metrics: Any) -> None:
    app, *_ = build(metrics=metrics)
    assert (await get(app, "/metrics")).status_code == 404
    assert (await get(app, "/livez")).status_code == 200
    assert (await get(app, "/readyz")).status_code == 200


async def test_metrics_500_on_render_error_rate_limited_and_probes_keep_working(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    log = ListLog()
    rec = MetricsRecorder(clock=clock, log=log)

    def broken(_registry: Any) -> bytes:
        raise RuntimeError("x")

    monkeypatch.setattr("das.observability.metrics.generate_latest", broken)
    app, _, wd, _, _ = build(metrics=rec, clock=clock, log=log)
    for _ in range(3):
        assert (await get(app, "/metrics")).status_code == 500
    assert len(log.events("metrics_render_failed")) == 1
    clock.advance(60)
    wd.tick()
    assert (await get(app, "/metrics")).status_code == 500
    assert len(log.events("metrics_render_failed")) == 2
    assert (await get(app, "/livez")).status_code == 200
    assert (await get(app, "/readyz")).status_code == 200


async def test_metrics_500_on_unexpected_exception_is_logged_by_ops_app() -> None:
    clock = FakeClock()
    log = ListLog()

    class Broken(MetricsRecorder):
        def __init__(self) -> None:
            pass

        def render(self) -> tuple[bytes, str]:
            raise ValueError("boom")

    app, *_ = build(metrics=Broken(), clock=clock, log=log)
    for _ in range(2):
        assert (await get(app, "/metrics")).status_code == 500
    assert len(log.events("metrics_render_failed")) == 1


# --- /livez --------------------------------------------------------------------------------


async def test_livez_pass_then_fail_when_loop_ends() -> None:
    app, sup, *_ = build()
    good = asyncio.create_task(forever())
    bad = asyncio.create_task(forever())
    sup.register("claim", good)
    sup.register("ingest", bad)
    resp = await get(app, "/livez")
    assert resp.status_code == 200
    assert resp.json()["checks"] == {"loops": {"claim": "pass", "ingest": "pass"}, "event_loop": "pass"}

    bad.cancel()
    await asyncio.gather(bad, return_exceptions=True)
    resp = await get(app, "/livez")
    body = resp.json()
    assert resp.status_code == 503 and body["status"] == "fail"
    assert body["checks"]["loops"] == {"claim": "pass", "ingest": "fail"}
    assert body["ended_loops"] == ["ingest"]
    assert body["event_loop_stale"] is False
    good.cancel()


async def test_livez_fails_when_event_loop_check_is_stale() -> None:
    app, _, wd, clock, _ = build()
    clock.advance(5.0)
    assert (await get(app, "/livez")).status_code == 200
    clock.advance(0.5)
    resp = await get(app, "/livez")
    assert resp.status_code == 503
    assert resp.json()["checks"]["event_loop"] == "fail"
    assert resp.json()["event_loop_stale"] is True
    wd.tick()
    assert (await get(app, "/livez")).status_code == 200


async def test_livez_ignores_connections_and_startup() -> None:
    flags = Flags()
    flags.started = False
    app, *_ = build(flags=flags, checks={"registered": False, "rpc": False, "store": False})
    assert (await get(app, "/livez")).status_code == 200


async def test_livez_200_during_shutdown_with_stopped_loops_but_not_when_stale() -> None:
    app, sup, _, clock, flags = build()
    task = asyncio.create_task(forever())
    sup.register("claim", task)
    sup.mark_shutting_down()
    flags.shutting = True
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    resp = await get(app, "/livez")
    assert resp.status_code == 200 and resp.json()["shutting_down"] is True
    ready = await get(app, "/readyz")
    assert ready.status_code == 503 and ready.json()["failed"] == [NOT_SHUTTING_DOWN]
    clock.advance(6)
    assert (await get(app, "/livez")).status_code == 503


# --- /readyz -------------------------------------------------------------------------------


async def test_readyz_all_pass() -> None:
    app, *_ = build()
    resp = await get(app, "/readyz")
    body = resp.json()
    assert resp.status_code == 200 and body["status"] == "pass"
    assert body["checks"] == {
        STARTUP: "pass",
        REGISTERED_AVAILABLE: "pass",
        "transport:rpc": "pass",
        STATE_STORE: "pass",
        NOT_DRAINING: "pass",
        NOT_SHUTTING_DOWN: "pass",
    }
    assert body["failed"] == []
    assert LLM_PROVIDER not in body


@pytest.mark.parametrize(
    ("setup", "expected"),
    [
        (lambda f, c: setattr(f, "started", False), [STARTUP]),
        (lambda f, c: c.update(registered=False), [REGISTERED_AVAILABLE]),
        (lambda f, c: c.update(rpc=False), ["transport:rpc"]),
        (lambda f, c: c.update(store=False), [STATE_STORE]),
        (lambda f, c: setattr(f, "draining", True), [NOT_DRAINING]),
        (lambda f, c: setattr(f, "shutting", True), [NOT_SHUTTING_DOWN]),
    ],
)
async def test_readyz_each_condition_fails_alone(setup: Any, expected: list[str]) -> None:
    flags = Flags()
    checks: dict[str, bool] = {}
    setup(flags, checks)
    app, *_ = build(flags=flags, checks=checks)
    resp = await get(app, "/readyz")
    assert resp.status_code == 503
    assert resp.json()["failed"] == expected


async def test_readyz_hanging_check_is_disconnected_within_bound() -> None:
    clock = FakeClock()
    supervisor = LoopSupervisor(log=ListLog())
    readiness = agent_readiness(
        startup=lambda: True,
        registered_available=const(True),
        transports={"rpc": hangs},
        state_store=hangs,
        draining=lambda: False,
        shutting_down=lambda: False,
        llm_provider=hangs,
    )
    app = create_ops_app(
        metrics=None,
        liveness=LivenessSource(supervisor, EventLoopWatchdog(clock), clock),
        readiness=readiness,
        check_timeout=0.05,
        overall_timeout=0.2,
    )
    start = time.monotonic()
    resp = await get(app, "/readyz")
    assert time.monotonic() - start < 1.0
    body = resp.json()
    assert resp.status_code == 503
    assert body["failed"] == ["transport:rpc", STATE_STORE]
    assert body[LLM_PROVIDER] == "disconnected"


async def test_readyz_llm_provider_is_informational_and_cached() -> None:
    calls = 0

    async def ping() -> bool:
        nonlocal calls
        calls += 1
        return False

    clock = FakeClock()
    llm = CachedCheck(ping, ttl=30, clock=clock)
    app, *_ = build(clock=clock, llm=llm)
    for _ in range(3):
        resp = await get(app, "/readyz")
        assert resp.status_code == 200
        assert resp.json()[LLM_PROVIDER] == "disconnected"
    assert calls == 1


async def test_gateway_readyz_has_only_startup_and_state_store() -> None:
    clock = FakeClock()
    started = {"v": False}
    app = create_ops_app(
        metrics=None,
        liveness=LivenessSource(LoopSupervisor(log=ListLog()), EventLoopWatchdog(clock), clock),
        readiness=gateway_readiness(startup=lambda: started["v"], state_store=const(True)),
    )
    resp = await get(app, "/readyz")
    assert resp.status_code == 503
    assert resp.json()["checks"] == {STARTUP: "fail", STATE_STORE: "pass"}
    started["v"] = True
    resp = await get(app, "/readyz")
    assert resp.status_code == 200 and resp.json()["checks"] == {STARTUP: "pass", STATE_STORE: "pass"}
    # Gateway keeps /healthz on http_port, so the ops app has none.
    assert (await get(app, "/healthz")).status_code == 404


# --- /healthz v1 ---------------------------------------------------------------------------


async def test_healthz_v1_behaviour_kept() -> None:
    app, *_ = build(healthz={"transport": const(True), "state_store": const(False)})
    resp = await get(app, "/healthz")
    assert resp.status_code == 503
    assert resp.json() == {
        "status": "unhealthy",
        "dependencies": {"transport": "connected", "state_store": "disconnected"},
    }
