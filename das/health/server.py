"""Per-agent health endpoint (Requirement 11.5).

``GET /healthz`` runs every injected dependency check concurrently. Each check
gets ``check_timeout`` seconds (default 2 s); a check that raises, returns a
falsy value, or does not finish in time reports ``disconnected``. The overall
status is ``healthy`` only when every check is ``connected``. The whole
response is bounded by ``overall_timeout`` (default 3 s), well inside the 5 s
requirement.

Checks are plain async callables keyed by dependency name, so the CLI decides
what "connected" means for each Transport, the State_Store, and the
LLM_Provider. :class:`CachedCheck` wraps costly checks (``LLMProvider.ping``)
so they run at most once per TTL (default 30 s).

Response body::

    {"status": "healthy" | "unhealthy",
     "dependencies": {"transport": "connected" | "disconnected",
                      "state_store": ..., "llm_provider": ...}}

HTTP 200 when healthy, 503 otherwise.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from collections.abc import Awaitable, Callable, Generator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from das.clock import Clock, SystemClock
from das.health.supervisor import EventLoopWatchdog, LoopSupervisor
from das.observability.logging import get_logger
from das.observability.metrics import MetricsRecorder, NoopRecorder, RenderError
from das.observability.ratelimit import RateLimitedLog

_log = logging.getLogger(__name__)

HealthCheck = Callable[[], Awaitable[bool]]

CONNECTED = "connected"
DISCONNECTED = "disconnected"
HEALTHY = "healthy"
UNHEALTHY = "unhealthy"

DEFAULT_CHECK_TIMEOUT = 2.0
DEFAULT_OVERALL_TIMEOUT = 3.0
DEFAULT_LLM_CACHE_TTL = 30.0

# Conventional dependency names used by the CLI.
TRANSPORT = "transport"
STATE_STORE = "state_store"
LLM_PROVIDER = "llm_provider"


class CachedCheck:
    """Caches the result of an async check for ``ttl`` seconds.

    Any completed outcome is cached: ``True``, ``False``, or an exception
    (cached as ``False``). A call cancelled by a timeout caches nothing, so the
    next request tries again. Concurrent callers share one in-flight call.
    """

    def __init__(
        self,
        check: HealthCheck,
        ttl: float = DEFAULT_LLM_CACHE_TTL,
        clock: Clock | None = None,
    ) -> None:
        if ttl < 0:
            raise ValueError("ttl must be non-negative")
        self._check = check
        self._ttl = float(ttl)
        self._clock: Clock = clock or SystemClock()
        self._value: bool | None = None
        self._expires_at = 0.0
        self._lock = asyncio.Lock()

    def invalidate(self) -> None:
        self._value = None

    async def __call__(self) -> bool:
        if self._fresh():
            return bool(self._value)
        async with self._lock:
            if self._fresh():  # another caller refreshed it while we waited
                return bool(self._value)
            try:
                value = bool(await self._check())
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a failing check is just "disconnected"
                _log.debug("cached health check raised", exc_info=True)
                value = False
            self._value = value
            self._expires_at = self._clock.monotonic() + self._ttl
            return value

    def _fresh(self) -> bool:
        return self._value is not None and self._clock.monotonic() < self._expires_at


async def _run_check(check: HealthCheck, timeout: float) -> bool:
    try:
        return bool(await asyncio.wait_for(check(), timeout=timeout))
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - includes TimeoutError
        _log.debug("health check failed", exc_info=True)
        return False


async def evaluate_health(
    checks: Mapping[str, HealthCheck],
    check_timeout: float = DEFAULT_CHECK_TIMEOUT,
    overall_timeout: float = DEFAULT_OVERALL_TIMEOUT,
) -> dict[str, object]:
    """Run all checks concurrently and build the health report."""
    tasks = {
        name: asyncio.create_task(_run_check(check, check_timeout))
        for name, check in checks.items()
    }
    if tasks:
        _, pending = await asyncio.wait(tasks.values(), timeout=overall_timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    deps: dict[str, str] = {}
    for name, task in tasks.items():
        ok = task.done() and not task.cancelled() and task.result() is True
        deps[name] = CONNECTED if ok else DISCONNECTED
    status = HEALTHY if all(v == CONNECTED for v in deps.values()) else UNHEALTHY
    return {"status": status, "dependencies": deps}


def create_health_app(
    checks: Mapping[str, HealthCheck],
    check_timeout: float = DEFAULT_CHECK_TIMEOUT,
    overall_timeout: float = DEFAULT_OVERALL_TIMEOUT,
) -> Starlette:
    """Build the Starlette app serving ``GET /healthz``."""
    frozen = dict(checks)

    async def healthz(_request: Request) -> JSONResponse:
        report = await evaluate_health(frozen, check_timeout, overall_timeout)
        code = 200 if report["status"] == HEALTHY else 503
        return JSONResponse(report, status_code=code)

    return Starlette(routes=[Route("/healthz", healthz, methods=["GET"])])


# -- Ops server: /metrics, /livez, /readyz (Req 1.1, 1.3, 1.7, 1.9, 16) ----------------

PASS = "pass"
FAIL = "fail"

STARTUP = "startup"
REGISTERED_AVAILABLE = "registered_available"
NOT_DRAINING = "not_draining"
NOT_SHUTTING_DOWN = "not_shutting_down"
TRANSPORT_PREFIX = "transport:"

METRICS_RENDER_WARN_PERIOD = 60.0

Flag = Callable[[], bool]


@dataclass(frozen=True)
class LivenessSource:
    """Inputs of the Liveness_Probe; deliberately free of any connection check (Req 16.3)."""

    supervisor: LoopSupervisor
    watchdog: EventLoopWatchdog
    clock: Clock


@dataclass(frozen=True)
class ReadinessSource:
    """Inputs of the Readiness_Probe.

    ``connections`` are async checks run concurrently with ``check_timeout`` each
    (``registered_available``, ``transport:<name>``, ``state_store``). ``draining``
    and ``shutting_down`` add the ``not_draining`` / ``not_shutting_down``
    conditions when given. ``llm_provider`` (normally a :class:`CachedCheck`) is
    informational only and never changes the HTTP status (Req 16.6).
    """

    startup: Flag
    connections: Mapping[str, HealthCheck] = field(default_factory=dict)
    draining: Flag | None = None
    shutting_down: Flag | None = None
    llm_provider: HealthCheck | None = None


def agent_readiness(
    *,
    startup: Flag,
    registered_available: HealthCheck,
    transports: Mapping[str, HealthCheck],
    state_store: HealthCheck,
    draining: Flag,
    shutting_down: Flag,
    llm_provider: HealthCheck | None = None,
) -> ReadinessSource:
    """Readiness conditions of a Manager_Agent or Worker_Agent (Req 16.4, 16.6, 16.9)."""
    connections: dict[str, HealthCheck] = {REGISTERED_AVAILABLE: registered_available}
    for name, check in transports.items():
        connections[f"{TRANSPORT_PREFIX}{name}"] = check
    connections[STATE_STORE] = state_store
    return ReadinessSource(
        startup=startup,
        connections=connections,
        draining=draining,
        shutting_down=shutting_down,
        llm_provider=llm_provider,
    )


def gateway_readiness(*, startup: Flag, state_store: HealthCheck) -> ReadinessSource:
    """Readiness conditions of the Gateway_API: only ``startup`` and ``state_store`` (Req 16.5)."""
    return ReadinessSource(startup=startup, connections={STATE_STORE: state_store})


def _flag(fn: Flag, default: bool) -> bool:
    try:
        return bool(fn())
    except Exception:  # noqa: BLE001 - a broken flag reports the unsafe value
        _log.debug("readiness flag raised", exc_info=True)
        return default


def evaluate_liveness(source: LivenessSource) -> dict[str, object]:
    """Build the Liveness_Probe report (Req 16.2, 16.3, 16.9, 16.10)."""
    supervisor = source.supervisor
    shutting_down = supervisor.shutting_down
    ended = set(supervisor.ended())
    loops = {
        name: PASS if shutting_down or name not in ended else FAIL for name in supervisor.names
    }
    stale = source.watchdog.stale(source.clock.monotonic())
    ok = not stale and all(v == PASS for v in loops.values())
    return {
        "status": PASS if ok else FAIL,
        "checks": {"loops": loops, "event_loop": FAIL if stale else PASS},
        "ended_loops": [] if shutting_down else [n for n in supervisor.names if n in ended],
        "event_loop_stale": stale,
        "shutting_down": shutting_down,
    }


async def evaluate_readiness(
    source: ReadinessSource,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT,
    overall_timeout: float = DEFAULT_OVERALL_TIMEOUT,
) -> dict[str, object]:
    """Build the Readiness_Probe report (Req 16.4-16.7, 16.9, 16.10)."""
    checks: dict[str, HealthCheck] = dict(source.connections)
    if source.llm_provider is not None:
        checks[LLM_PROVIDER] = source.llm_provider
    report = await evaluate_health(checks, check_timeout, overall_timeout)
    deps = report["dependencies"]
    assert isinstance(deps, dict)

    results: dict[str, str] = {STARTUP: PASS if _flag(source.startup, False) else FAIL}
    for name in source.connections:
        results[name] = PASS if deps.get(name) == CONNECTED else FAIL
    if source.draining is not None:
        results[NOT_DRAINING] = FAIL if _flag(source.draining, True) else PASS
    if source.shutting_down is not None:
        results[NOT_SHUTTING_DOWN] = FAIL if _flag(source.shutting_down, True) else PASS

    failed = [name for name, value in results.items() if value != PASS]
    body: dict[str, object] = {
        "status": FAIL if failed else PASS,
        "checks": results,
        "failed": failed,
    }
    if source.llm_provider is not None:
        body[LLM_PROVIDER] = deps.get(LLM_PROVIDER, DISCONNECTED)
    return body


def create_ops_app(
    *,
    metrics: MetricsRecorder | None,
    liveness: LivenessSource,
    readiness: ReadinessSource,
    healthz: Mapping[str, HealthCheck] | None = None,
    clock: Clock | None = None,
    log: Any = None,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT,
    overall_timeout: float = DEFAULT_OVERALL_TIMEOUT,
    extra_routes: Sequence[Route] = (),
) -> Starlette:
    """Build the Ops_Port app serving ``/metrics``, ``/livez``, ``/readyz`` (and ``/healthz``).

    ``extra_routes`` are served as they are: the Gateway passes the HTTP_Broker's routes here when it is enabled
    (Phase 7 Req 2.2), and they do their own bearer-token check.

    ``metrics`` is ``None`` or a :class:`NoopRecorder` when ``metrics.enabled`` is
    false, in which case ``/metrics`` answers 404 (Req 1.7). No endpoint checks
    ``X-API-Key`` (Req 1.3, 16.1). ``healthz`` keeps the v1 ``/healthz`` endpoint
    (Req 16.8); pass ``None`` on the Gateway_API, which serves it on ``http_port``.
    """
    metrics_enabled = metrics is not None and not isinstance(metrics, NoopRecorder)
    ops_log = log if log is not None else get_logger("das.health.ops")
    render_warnings = RateLimitedLog(clock or liveness.clock, METRICS_RENDER_WARN_PERIOD)

    async def metrics_endpoint(_request: Request) -> Response:
        if not metrics_enabled or metrics is None:
            return PlainTextResponse("metrics disabled", status_code=404)
        try:
            body, content_type = metrics.render()
        except Exception as exc:  # noqa: BLE001 - every render failure is a 500 (Req 1.9)
            # MetricsRecorder.render already logs RenderError with its own 60 s limit.
            if not isinstance(exc, RenderError):
                render_warnings.warning(
                    ops_log, ("metrics_render",), "metrics_render_failed",
                    error_type=type(exc).__name__,
                )
            return PlainTextResponse("metrics render failed", status_code=500)
        return Response(content=body, status_code=200, headers={"Content-Type": content_type})

    async def livez(_request: Request) -> JSONResponse:
        report = evaluate_liveness(liveness)
        return JSONResponse(report, status_code=200 if report["status"] == PASS else 503)

    async def readyz(_request: Request) -> JSONResponse:
        report = await evaluate_readiness(readiness, check_timeout, overall_timeout)
        return JSONResponse(report, status_code=200 if report["status"] == PASS else 503)

    routes = [
        Route("/metrics", metrics_endpoint, methods=["GET"]),
        Route("/livez", livez, methods=["GET"]),
        Route("/readyz", readyz, methods=["GET"]),
    ]
    if healthz is not None:
        frozen = dict(healthz)

        async def healthz_endpoint(_request: Request) -> JSONResponse:
            report = await evaluate_health(frozen, check_timeout, overall_timeout)
            return JSONResponse(report, status_code=200 if report["status"] == HEALTHY else 503)

        routes.append(Route("/healthz", healthz_endpoint, methods=["GET"]))
    return Starlette(routes=[*routes, *extra_routes])


class _NoSignalServer(uvicorn.Server):
    """uvicorn server that leaves signal handling to the agent process."""

    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None, None, None]:
        yield


class HealthServer:
    """Runs a health app with uvicorn as a background task.

    ``start()`` returns once the socket is listening; ``stop()`` shuts it down.
    Pass ``port=0`` to bind an ephemeral port (see :attr:`port`). ``lifespan="on"`` runs the app's
    ASGI startup and shutdown events (the Gateway's Webhook_Sender workers live there); the health
    and ops apps have none and keep the default ``"off"``.
    """

    def __init__(
        self,
        app: Starlette,
        port: int,
        host: str = "0.0.0.0",
        *,
        lifespan: Literal["on", "off"] = "off",
    ) -> None:
        config = uvicorn.Config(
            app, host=host, port=port, lifespan=lifespan, log_config=None, access_log=False
        )
        self._server = _NoSignalServer(config)
        self._task: asyncio.Task[None] | None = None

    @property
    def port(self) -> int:
        """The bound port (meaningful after ``start``)."""
        for srv in self._server.servers:
            for sock in srv.sockets:
                return int(sock.getsockname()[1])
        return self._server.config.port

    @property
    def started(self) -> bool:
        """True once the socket is listening and the server has not been stopped."""
        return self._task is not None and not self._task.done() and bool(self._server.started)

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The background serve task (for a :class:`LoopSupervisor`), once started."""
        return self._task

    def _bind(self) -> socket.socket:
        """Bind the listening socket; raises :class:`OSError` when the port is taken.

        Binding here (instead of inside uvicorn, which calls ``sys.exit`` on a bind
        error) lets the caller map the failure to an exit code (Req 16.11).
        """
        cfg = self._server.config
        family = socket.AF_INET6 if ":" in cfg.host else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((cfg.host, cfg.port))
            sock.listen(cfg.backlog)
            sock.setblocking(False)
        except OSError:
            sock.close()
            raise
        return sock

    async def start(self, startup_timeout: float = 10.0) -> None:
        """Bind and serve; raises :class:`OSError` when the port cannot be bound."""
        if self._task is not None:
            return
        sock = self._bind()
        self._task = asyncio.create_task(
            self._server.serve(sockets=[sock]), name="das-health-server"
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + startup_timeout
        while not self._server.started:
            if self._task.done():
                exc = self._task.exception() if not self._task.cancelled() else None
                raise RuntimeError(f"health server failed to start: {exc!r}")
            if loop.time() > deadline:
                await self.stop()
                raise TimeoutError("health server did not start in time")
            await asyncio.sleep(0.01)

    async def stop(self) -> None:
        if self._task is None:
            return
        self._server.should_exit = True
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await self._task
        self._task = None


__all__ = [
    "CONNECTED",
    "DISCONNECTED",
    "FAIL",
    "HEALTHY",
    "LLM_PROVIDER",
    "NOT_DRAINING",
    "NOT_SHUTTING_DOWN",
    "PASS",
    "REGISTERED_AVAILABLE",
    "STARTUP",
    "STATE_STORE",
    "TRANSPORT",
    "TRANSPORT_PREFIX",
    "UNHEALTHY",
    "CachedCheck",
    "HealthCheck",
    "HealthServer",
    "LivenessSource",
    "ReadinessSource",
    "agent_readiness",
    "create_health_app",
    "create_ops_app",
    "evaluate_health",
    "evaluate_liveness",
    "evaluate_readiness",
    "gateway_readiness",
]
