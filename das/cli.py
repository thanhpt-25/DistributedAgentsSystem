"""``das`` command-line entry point (Requirements 8.4, 8.5, 12.2, 12.3, 14.4, 14.7, 15.3, 15.5).

Usage::

    das run --config /etc/das/config.yaml [--log-level info]
    das drain   <gateway_url> <agent_id> [--release-leases] [--wait] [--timeout SECONDS]
    das undrain <gateway_url> <agent_id>

``drain`` / ``undrain`` read the Admin_API_Key from ``DAS_ADMIN_API_KEY`` and call
the Admin_API (Req 17.14, 17.17); everything they print goes through a Redactor.
The rest of this docstring describes ``das run``.

Startup runs in three phases so failures surface in the right order:

1. **No I/O.** Parse arguments, configure JSON logging, load and validate the
   Agent_Config (env > file > default). Logging is then reconfigured with the
   agent ID and every credential registered with the Redactor (Req 12.7). The
   Transport adapters named in the config are loaded through the
   ``das.transports`` entry points (Req 8.4, 8.5, 15.3, 15.5), and the LLM
   provider, State_Store, Agent_Registry, transports, and role runtime are
   constructed. Nothing has connected yet, so a config error (exit 2) or a
   transport loader error (exit 3) leaves no trace in any dependency
   (Req 12.2, 12.3, 14.4).
2. **Connect.** Redis, then each transport, each retried every
   ``limits.startup_retry_interval`` until ``limits.startup_timeout`` elapses;
   then the process exits with code 4 naming the unreachable dependency
   (Req 14.7).
3. **Run** the role: ``gateway`` serves the Gateway_API with uvicorn on
   ``http_port``; ``manager`` and ``worker`` run their agent. Every role runs
   the Ops server (``/metrics``, ``/livez``, ``/readyz``) on ``health_port``,
   bound before phase 2 so a taken port fails with exit 2 (Req 16.11).
   SIGTERM and SIGINT trigger graceful shutdown; queued Spans are flushed
   last without changing the exit code (Req 8.8).

Exit codes come from :mod:`das.errors` (2 config, 3 transport loader,
4 dependency unreachable, 5 registration failure, 1 anything else).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import inspect
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Final, TextIO
from urllib.parse import quote, urlsplit

import httpx

from das.clock import Clock, SystemClock
from das.config import AgentConfig, load_config
from das.errors import (
    EXIT_CONFIG_ERROR,
    EXIT_GENERIC_FAILURE,
    EXIT_OK,
    ConfigError,
    DependencyUnreachable,
    FieldError,
    OpsPortUnavailable,
    exit_code_for,
)
from das.observability import Redactor, configure_logging, get_logger
from das.transport.loader import EntryPointSource, load_transport

log = get_logger("das.cli")

#: Exit code after Ctrl-C during startup (conventional 128 + SIGINT).
EXIT_INTERRUPTED: Final = 130
#: Agent ID used in log lines emitted before the config is loaded.
BOOT_AGENT_ID: Final = "das"
REDIS_DEPENDENCY: Final = "redis"
#: Socket timeouts for Manager and Worker Redis clients (the Gateway uses its own 2 s).
AGENT_REDIS_SOCKET_TIMEOUT: Final = 5.0
AGENT_REDIS_CONNECT_TIMEOUT: Final = 2.0
LOG_LEVELS: Final = ("debug", "info", "warning", "error", "critical")
#: Critical_Loop name of the Gateway's HTTP server task (Req 16.2).
GATEWAY_HTTP_LOOP: Final = "http_server"
#: ``das drain`` / ``das undrain`` (Req 17.14, 17.17).
ADMIN_KEY_ENV: Final = "DAS_ADMIN_API_KEY"
API_KEY_HEADER: Final = "X-API-Key"
MIN_DRAIN_TIMEOUT: Final = 1
MAX_DRAIN_TIMEOUT: Final = 86_400
DEFAULT_DRAIN_TIMEOUT: Final = 600
DRAIN_POLL_INTERVAL: Final = 2.0
#: Per-call HTTP timeout; the Gateway answers drain/undrain within 5 s (Req 17.1, 17.12).
ADMIN_HTTP_TIMEOUT: Final = 15.0


# --------------------------------------------------------------------------- arguments


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="das", description="Distributed Agent System")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run one agent (role taken from the config)")
    run.add_argument("--config", required=True, help="path to the Agent_Config YAML file")
    run.add_argument("--log-level", default="info", choices=LOG_LEVELS, help="log level")

    drain = sub.add_parser(
        "drain",
        help="drain an agent through the Gateway_API",
        description=f"Admin_API_Key is read from ${ADMIN_KEY_ENV}.",
    )
    _add_target_arguments(drain)
    drain.add_argument(
        "--release-leases",
        action="store_true",
        help="Manager only: release every Lease so another Manager takes over",
    )
    drain.add_argument(
        "--wait", action="store_true", help="poll GET /v1/agents until the agent is drained"
    )
    drain.add_argument(
        "--timeout",
        type=_drain_timeout,
        default=DEFAULT_DRAIN_TIMEOUT,
        help=f"seconds to wait with --wait ({MIN_DRAIN_TIMEOUT}..{MAX_DRAIN_TIMEOUT}, "
        f"default {DEFAULT_DRAIN_TIMEOUT})",
    )
    undrain = sub.add_parser(
        "undrain",
        help="return a draining agent to Available",
        description=f"Admin_API_Key is read from ${ADMIN_KEY_ENV}.",
    )
    _add_target_arguments(undrain)
    return parser


def _add_target_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("gateway_url", type=_gateway_url, help="Gateway_API base URL")
    parser.add_argument("agent_id", help="ID of the agent to act on")


def _gateway_url(value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        parts = None
    if parts is None or parts.scheme not in ("http", "https") or not parts.netloc:
        raise argparse.ArgumentTypeError("must be an http:// or https:// URL")
    return value.rstrip("/")


def _drain_timeout(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number of seconds") from None
    if not MIN_DRAIN_TIMEOUT <= seconds <= MAX_DRAIN_TIMEOUT:
        raise argparse.ArgumentTypeError(
            f"must be between {MIN_DRAIN_TIMEOUT} and {MAX_DRAIN_TIMEOUT} seconds"
        )
    return seconds


# --------------------------------------------------------------------------- secrets


#: URL passwords shorter than this are redacted only as part of ``user:password``;
#: redacting a short bare password everywhere would mangle ordinary log text.
MIN_BARE_URL_PASSWORD: Final = 8


def _url_credentials(url: str | None) -> list[str]:
    """Credential strings in a URL's userinfo, for log redaction."""
    if not url:
        return []
    try:
        parts = urlsplit(url)
        password = parts.password
    except ValueError:
        return []
    if not password:
        return []
    out = [f"{parts.username or ''}:{password}@"]
    if len(password) >= MIN_BARE_URL_PASSWORD:
        out.append(password)
    return out


def secrets_for_redaction(config: AgentConfig) -> list[str]:
    """Every credential to redact: API keys, LLM credentials, and URL passwords."""
    values = list(config.secret_values())
    urls: list[str | None] = []
    if config.rabbitmq is not None:
        urls += [config.rabbitmq.url, config.rabbitmq.management_url]
    if config.redis is not None:
        urls.append(config.redis.url)
    for url in urls:
        values += _url_credentials(url)
    return values


# --------------------------------------------------------------------------- transports


def transport_names(config: AgentConfig) -> tuple[str | None, str | None]:
    """``(jobs, control)`` transport names the role uses; ``None`` when unused.

    The Gateway only needs a control transport (best-effort ``cancel_request``)
    and only when the config names one.
    """
    selected = config.transports
    if selected is None:
        return None, None
    if config.role == "gateway":
        return None, selected.control
    return selected.jobs, selected.control


def load_transports(
    config: AgentConfig, source: EntryPointSource | None = None
) -> dict[str, type]:
    """Load every Transport adapter the config selects, by name (Req 8.4).

    Raises :class:`~das.errors.TransportNotInstalled` or
    :class:`~das.errors.TransportIncomplete` (exit code 3, Req 8.5, 15.5).
    """
    names = [n for n in transport_names(config) if n]
    return {name: load_transport(name, source) for name in dict.fromkeys(names)}


def registry_endpoint_resolver(registry: Any) -> Callable[[str], Awaitable[str | None]]:
    """Resolve an agent ID to its gRPC endpoint through the Agent_Registry."""

    async def resolve(agent_id: str) -> str | None:
        record = (await registry.snapshot()).get(agent_id)
        return None if record is None else record.grpc_endpoint

    return resolve


def build_transport(name: str, cls: type, config: AgentConfig, *, resolver: Any) -> Any:
    """Instantiate an adapter, preferring its ``from_config`` classmethod.

    Only keyword arguments that ``from_config`` names explicitly are offered
    (currently ``resolver``), so adapters that forward ``**kwargs`` to their
    constructor never receive options they don't know.
    """
    try:
        factory = getattr(cls, "from_config", None)
        if callable(factory):
            params = inspect.signature(factory).parameters
            extra: dict[str, Any] = {}
            if "resolver" in params:
                extra["resolver"] = resolver
            return factory(config, **extra)
        return cls()
    except (ValueError, TypeError) as exc:
        raise ConfigError(
            [FieldError("transports", f"cannot configure transport {name!r}: {exc}")],
            reason="invalid configuration",
        ) from exc


def _is_connected(transport: Any) -> bool:
    for attr in ("connected", "started"):
        value = getattr(transport, attr, None)
        if isinstance(value, bool):
            return value
    return True


# --------------------------------------------------------------------------- startup retry


def _describe(exc: BaseException) -> str:
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


async def connect_with_retry(
    dependency: str,
    connect: Callable[[], Awaitable[object]],
    *,
    interval: float,
    timeout: float,
    reset: Callable[[], Awaitable[object]] | None = None,
    clock: Clock | None = None,
) -> None:
    """Call ``connect`` until it succeeds or ``timeout`` elapses (Req 14.7).

    Attempts are spaced ``interval`` seconds apart and each one is bounded by
    the remaining time. ``reset`` (for example ``transport.close``) runs after
    a failed attempt so the next one starts clean. Raises
    :class:`~das.errors.DependencyUnreachable` naming ``dependency``.
    """
    clock = clock or SystemClock()
    deadline = clock.monotonic() + timeout
    attempts = 0
    last_error = "no attempt completed"
    while True:
        attempts += 1
        remaining = max(deadline - clock.monotonic(), 0.001)
        try:
            await asyncio.wait_for(connect(), timeout=remaining)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - retried until the deadline
            last_error = "timed out" if isinstance(exc, TimeoutError) else _describe(exc)
            if reset is not None:
                with contextlib.suppress(Exception):
                    await reset()
        else:
            log.info("dependency connected", dependency=dependency, attempts=attempts)
            return
        remaining = deadline - clock.monotonic()
        if remaining <= 0:
            break
        delay = min(interval, remaining)
        log.warning(
            "dependency unreachable; retrying",
            dependency=dependency,
            attempt=attempts,
            retry_in=delay,
            error=last_error,
        )
        await clock.sleep(delay)
    raise DependencyUnreachable(
        dependency, f"not reachable within {timeout:g}s after {attempts} attempts ({last_error})"
    )


# --------------------------------------------------------------------------- roles


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(sig, stop.set)


async def _close_quietly(what: str, close: Callable[[], Awaitable[object]]) -> None:
    try:
        await close()
    except Exception as exc:  # noqa: BLE001 - shutdown continues
        log.warning("close failed", component=what, error=_describe(exc))


def _redis_client(config: AgentConfig) -> Any:
    assert config.redis is not None  # enforced by the schema for every role
    if config.role == "gateway":
        from das.gateway.app import build_redis_client

        return build_redis_client(config.redis.url)
    from redis.asyncio import Redis

    return Redis.from_url(
        config.redis.url,
        decode_responses=True,
        socket_timeout=AGENT_REDIS_SOCKET_TIMEOUT,
        socket_connect_timeout=AGENT_REDIS_CONNECT_TIMEOUT,
    )


def _health_checks(
    redis: Any, transports: Sequence[Any], llm_check: Callable[[], Awaitable[bool]] | None
) -> dict[str, Callable[[], Awaitable[bool]]]:
    """v1 ``/healthz`` checks (Req 16.8); ``llm_check`` is the shared ``CachedCheck``."""
    from das.health.server import LLM_PROVIDER, STATE_STORE, TRANSPORT

    async def transport_ok() -> bool:
        return all(_is_connected(t) for t in transports)

    checks: dict[str, Callable[[], Awaitable[bool]]] = {STATE_STORE: _redis_check(redis)}
    if transports:
        checks[TRANSPORT] = transport_ok
    if llm_check is not None:
        checks[LLM_PROVIDER] = llm_check
    return checks


def _redis_check(redis: Any) -> Callable[[], Awaitable[bool]]:
    async def redis_ok() -> bool:
        return bool(await redis.ping())

    return redis_ok


def _transport_check(transport: Any) -> Callable[[], Awaitable[bool]]:
    async def connected() -> bool:
        return _is_connected(transport)

    return connected


def _registered_available(redis: Any, agent_id: str) -> Callable[[], Awaitable[bool]]:
    """Readiness ``registered_available``: own registry entry has status Available (Req 16.4)."""
    from das.registry.registry import agent_key

    async def check() -> bool:
        status = await redis.hget(agent_key(agent_id), "status")
        if isinstance(status, bytes):
            status = status.decode("utf-8", "replace")
        return status == "Available"

    return check


async def _start_ops_server(server: Any, port: int) -> None:
    """Bind and start the Ops server; a bind failure is exit 2 naming the port (Req 16.11)."""
    try:
        await server.start()
    except OSError as exc:
        raise OpsPortUnavailable(port, exc.strerror or type(exc).__name__) from exc
    log.info("ops server listening", port=server.port)


def _attach_worker_llm_observability(
    llm: Any, metrics: Any, tracer: Any, price_table: Any
) -> None:
    """Give the Worker's LLMProvider its metrics observer and Tracer (Req 4, 6.6)."""
    from das.llm.retry import LLMProvider
    from das.observability.metrics import LLMMetricsObserver

    if not isinstance(llm, LLMProvider):
        return
    if llm.observer is None:
        llm.observer = LLMMetricsObserver(
            metrics.llm, llm.provider, llm.params.model, price_table
        )
    llm.tracer = tracer


async def run_agent(
    config: AgentConfig,
    transport_classes: Mapping[str, type],
    *,
    stop: asyncio.Event | None = None,
    clock: Clock | None = None,
    ops_clock: Clock | None = None,
) -> int:
    """Construct, connect, and run the configured role; returns the exit code.

    ``clock`` drives the startup dependency retry. ``ops_clock`` (default
    :class:`SystemClock`) drives the event-loop watchdog, metrics and tracing.

    Every role gets a metrics recorder (``NoopRecorder`` when ``metrics.enabled``
    is false), a Tracer from :func:`build_tracer` (``NoopTracer`` when tracing is
    off, so nothing connects to the OTLP endpoint, Req 8.2), and the Ops server on
    ``health_port`` serving ``/metrics``, ``/livez`` and ``/readyz`` (plus v1
    ``/healthz`` on Manager and Worker; the Gateway keeps ``/healthz`` on
    ``http_port``, which has no ops path, Req 1.2, 16.8). The Ops server binds
    before any dependency is contacted, so ``/livez`` answers during startup and
    ``/readyz`` reports ``startup`` failing (Req 16.10); a bind failure is exit 2
    naming the port (Req 16.11). At shutdown queued Spans are flushed within
    ``min(5, remaining shutdown_timeout)`` seconds without changing the exit code
    (Req 8.8).
    """
    from das.health.server import (
        CachedCheck,
        HealthServer,
        LivenessSource,
        agent_readiness,
        create_ops_app,
        gateway_readiness,
    )
    from das.health.supervisor import EventLoopWatchdog, LoopSupervisor
    from das.observability.metrics import MetricsRecorder, WorkerMetrics, build_recorder
    from das.observability.tracing import build_tracer
    from das.registry import AgentRegistry
    from das.state import StateStore

    role = config.role
    limits = config.limits
    stop = stop or asyncio.Event()
    ops_clock = ops_clock or SystemClock()
    agent_id = str(config.agent_id)

    # ---- phase 1: construct everything (no network I/O) --------------------
    supervisor = LoopSupervisor()
    watchdog = EventLoopWatchdog(ops_clock)
    # NoopTracer when tracing is disabled; the OTLP exporter is created lazily, so
    # an unreachable endpoint never blocks startup (Req 8.2, 8.12).
    tracer = build_tracer(config.tracing, role, agent_id, ops_clock)
    price_table = config.pricing.price_table() if config.pricing is not None else None
    currency = config.pricing.currency if config.pricing is not None else None

    llm = None
    if role != "gateway":
        from das.llm.factory import build_llm_provider

        llm = build_llm_provider(config)
    redis = _redis_client(config)
    registry = AgentRegistry(redis)
    store = StateStore(redis)
    resolver = registry_endpoint_resolver(registry)
    instances = {
        name: build_transport(name, cls, config, resolver=resolver)
        for name, cls in transport_classes.items()
    }
    jobs_name, control_name = transport_names(config)
    queue = instances.get(jobs_name) if jobs_name else None
    control = instances.get(control_name) if control_name else None
    transports = list(instances.values())

    runner: Callable[[], Awaitable[None]]
    ops: HealthServer
    if role == "gateway":
        from das.gateway.app import create_app
        from das.health.server import evaluate_health

        checks = _health_checks(redis, transports, None)

        async def gateway_health() -> Mapping[str, Any]:
            report = await evaluate_health(checks)
            return {"overall": report["status"], **report}

        def gateway_recorder(routes: Sequence[str]) -> MetricsRecorder:
            return build_recorder(
                config.metrics, clock=ops_clock, route_templates=routes, currency=currency
            )

        app = create_app(
            store,
            registry,
            control,
            api_keys=config.api_keys,
            max_prompt_length=limits.max_prompt_length,
            health_check=gateway_health,
            admin_api_keys=config.admin_api_keys,
            cursor_secret=config.gateway.cursor_secret,
            price_table=price_table,
            metrics_factory=gateway_recorder,
            tracer=tracer,
            clock=ops_clock,
            agent_id=agent_id,
        )
        server = HealthServer(app, config.http_port)
        ops = HealthServer(
            create_ops_app(
                metrics=app.state.metrics_recorder,
                liveness=LivenessSource(supervisor, watchdog, ops_clock),
                readiness=gateway_readiness(
                    startup=lambda: server.started, state_store=_redis_check(redis)
                ),
                healthz=None,  # v1 /healthz stays on http_port (Req 16.8)
                clock=ops_clock,
            ),
            config.health_port,
        )

        async def run_gateway() -> None:
            await server.start()
            try:
                assert server.task is not None
                supervisor.register(GATEWAY_HTTP_LOOP, server.task)
                log.info("gateway listening", port=server.port)
                await stop.wait()
            finally:
                supervisor.mark_shutting_down()
                await server.stop()

        runner = run_gateway
    else:
        assert queue is not None and control is not None  # schema requires transports
        recorder = build_recorder(config.metrics, clock=ops_clock, currency=currency)
        agent: Any
        if role == "manager":
            from das.manager.agent import ManagerAgent, ManagerSettings

            agent = ManagerAgent(
                ManagerSettings.from_config(config),
                queue=queue,
                control=control,
                store=store,
                registry=registry,
                llm=llm,
                recorder=recorder,
                tracer=tracer,
                supervisor=supervisor,
            )
        else:
            from das.worker.executor import JobExecutor
            from das.worker.worker import Worker, WorkerSettings

            assert config.worker is not None and config.grpc is not None
            assert llm is not None  # built above for every non-gateway role
            settings = WorkerSettings(
                agent_id=agent_id,
                host=config.host,
                capabilities=frozenset(config.worker.capabilities),
                concurrency_limit=config.worker.concurrency_limit,
                heartbeat_interval=limits.heartbeat_interval,
                shutdown_timeout=limits.shutdown_timeout,
                grpc_endpoint=f"{config.grpc.advertise_host}:{config.grpc.advertise_port}",
            )
            worker_metrics = WorkerMetrics(recorder)
            _attach_worker_llm_observability(llm, worker_metrics, tracer, price_table)
            executor = JobExecutor(llm, settings.agent_id, job_timeout=limits.job_timeout)
            agent = Worker(
                settings,
                queue=queue,
                control=control,
                registry=registry,
                executor=executor,
                metrics=worker_metrics,
                tracer=tracer,
                supervisor=supervisor,
            )

        llm_check = CachedCheck(llm.ping) if llm is not None else None
        ops = HealthServer(
            create_ops_app(
                metrics=recorder,
                liveness=LivenessSource(supervisor, watchdog, ops_clock),
                readiness=agent_readiness(
                    startup=lambda: bool(getattr(agent, "started", False)),
                    registered_available=_registered_available(redis, agent_id),
                    transports={name: _transport_check(t) for name, t in instances.items()},
                    state_store=_redis_check(redis),
                    draining=lambda: bool(getattr(agent, "draining", False)),
                    shutting_down=lambda: supervisor.shutting_down,
                    llm_provider=llm_check,
                ),
                healthz=_health_checks(redis, transports, llm_check),
                clock=ops_clock,
            ),
            config.health_port,
        )

        async def run_role_agent() -> None:
            await agent.run(stop)

        runner = run_role_agent

    # ---- phase 2 and 3: Ops server, connect with startup retry, then run ---
    stopped_at: list[float] = []

    async def note_stop() -> None:
        await stop.wait()
        stopped_at.append(ops_clock.monotonic())

    stop_watch = asyncio.create_task(note_stop(), name="das-stop-watch")
    try:
        await _start_ops_server(ops, config.health_port)
        watchdog.start()
        async with contextlib.AsyncExitStack() as stack:
            stack.push_async_callback(_close_quietly, REDIS_DEPENDENCY, redis.aclose)
            await connect_with_retry(
                REDIS_DEPENDENCY,
                redis.ping,
                interval=limits.startup_retry_interval,
                timeout=limits.startup_timeout,
                clock=clock,
            )
            for name, transport in instances.items():
                # Closing twice is harmless; the agents also close their transports.
                stack.push_async_callback(_close_quietly, name, transport.close)
                await connect_with_retry(
                    name,
                    transport.start,
                    interval=limits.startup_retry_interval,
                    timeout=limits.startup_timeout,
                    reset=transport.close,
                    clock=clock,
                )
            _install_signal_handlers(stop)
            log.info("starting role", role=role, transports=sorted(instances))
            await runner()
    finally:
        supervisor.mark_shutting_down()
        stop_watch.cancel()
        await _close_quietly("ops server", ops.stop)
        await watchdog.stop()
        elapsed = ops_clock.monotonic() - stopped_at[0] if stopped_at else 0.0
        await _flush_tracer(tracer, limits.shutdown_timeout - elapsed)
    log.info("stopped", role=role)
    return EXIT_OK


async def _flush_tracer(tracer: Any, remaining: float) -> None:
    """Flush queued Spans within ``min(5, remaining)`` seconds (Req 8.8).

    The Tracer logs the discarded count itself; nothing here changes the exit code.
    """
    try:
        dropped = await tracer.shutdown(max(0.0, remaining))
    except Exception as exc:  # noqa: BLE001 - shutdown continues with the same exit code
        log.warning("tracer shutdown failed", error=_describe(exc))
        return
    if dropped:
        log.info("tracer flushed", dropped=dropped)


# --------------------------------------------------------------------------- drain / undrain


class _Console:
    """Writes CLI output through a Redactor so the Admin_API_Key never appears."""

    def __init__(self, redactor: Redactor, out: TextIO, err: TextIO) -> None:
        self._redactor = redactor
        self._out = out
        self._err = err

    def info(self, text: str) -> None:
        print(self._redactor.redact(text), file=self._out, flush=True)

    def error(self, text: str) -> None:
        print(self._redactor.redact(f"das: error: {text}"), file=self._err, flush=True)


def _error_code(response: httpx.Response) -> str | None:
    """The ``error`` field of a Gateway error body, when there is one."""
    try:
        body = response.json()
    except ValueError:
        return None
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return body["error"]
    return None


def _describe_http_failure(what: str, response: httpx.Response) -> str:
    code = _error_code(response)
    text = f"{what} failed: HTTP {response.status_code}"
    if code is not None:
        text += f" ({code})"
        try:
            body = response.json()
        except ValueError:
            body = {}
        if isinstance(body, dict) and body.get("current_state") is not None:
            text += f", agent state {body['current_state']}"
    return text


def _find_agent(body: object, agent_id: str) -> Mapping[str, Any] | None:
    items = body.get("items") if isinstance(body, dict) else body
    if not isinstance(items, list):
        return None
    for item in items:
        if isinstance(item, dict) and item.get("agent_id") == agent_id:
            return item
    return None


async def _wait_drained(
    client: httpx.AsyncClient,
    args: argparse.Namespace,
    clock: Clock,
    console: _Console,
) -> int:
    """Poll ``GET /v1/agents`` every 2 s until the agent reports ``drained`` (Req 17.14)."""
    deadline = clock.monotonic() + args.timeout
    url = f"{args.gateway_url}/v1/agents"
    while True:
        try:
            response = await client.get(url)
        except httpx.HTTPError as exc:
            console.error(f"GET /v1/agents failed: {_describe(exc)}")
        else:
            if response.status_code in (401, 403):
                console.error(_describe_http_failure("GET /v1/agents", response))
                return EXIT_GENERIC_FAILURE
            if response.status_code == 200:
                try:
                    agent = _find_agent(response.json(), args.agent_id)
                except ValueError:
                    agent = None
                if agent is not None and agent.get("drained") is True:
                    console.info(f"agent {args.agent_id} drained")
                    return EXIT_OK
            else:
                console.error(_describe_http_failure("GET /v1/agents", response))
        remaining = deadline - clock.monotonic()
        if remaining <= 0:
            console.error(
                f"timeout: agent {args.agent_id} not drained within {args.timeout:g} seconds"
            )
            return EXIT_GENERIC_FAILURE
        await clock.sleep(min(DRAIN_POLL_INTERVAL, remaining))


async def admin_command(
    args: argparse.Namespace,
    admin_key: str,
    *,
    clock: Clock,
    console: _Console,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Send drain / undrain to the Gateway_API and map the answer to an exit code."""
    action = args.command
    path = f"/v1/admin/agents/{quote(args.agent_id, safe='')}/{action}"
    body: dict[str, Any] = {}
    if action == "drain":
        body["release_leases"] = bool(args.release_leases)
    async with httpx.AsyncClient(
        headers={API_KEY_HEADER: admin_key},
        timeout=ADMIN_HTTP_TIMEOUT,
        transport=transport,
    ) as client:
        try:
            response = await client.post(f"{args.gateway_url}{path}", json=body)
        except httpx.HTTPError as exc:
            console.error(f"{action} request failed: {_describe(exc)}")
            return EXIT_GENERIC_FAILURE
        if response.status_code != 202:
            console.error(_describe_http_failure(action, response))
            return EXIT_GENERIC_FAILURE
        try:
            accepted = response.json()
        except ValueError:
            accepted = {}
        status = accepted.get("status") if isinstance(accepted, dict) else None
        console.info(
            f"{action} accepted: agent {args.agent_id}" + (f", status {status}" if status else "")
        )
        if action == "drain" and args.wait:
            return await _wait_drained(client, args, clock, console)
    return EXIT_OK


def admin_main(
    args: argparse.Namespace,
    environ: Mapping[str, str],
    *,
    clock: Clock | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    """Entry for ``das drain`` / ``das undrain`` (Req 17.14, 17.17)."""
    admin_key = environ.get(ADMIN_KEY_ENV, "")
    redactor = Redactor([admin_key, *_url_credentials(args.gateway_url)])
    console = _Console(redactor, out or sys.stdout, err or sys.stderr)
    if not admin_key:
        console.error(f"environment variable {ADMIN_KEY_ENV} is not set")
        return EXIT_CONFIG_ERROR
    try:
        return asyncio.run(
            admin_command(
                args,
                admin_key,
                clock=clock or SystemClock(),
                console=console,
                transport=transport,
            )
        )
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    except Exception as exc:  # noqa: BLE001 - reported without the key
        console.error(_describe(exc))
        return EXIT_GENERIC_FAILURE


# --------------------------------------------------------------------------- main


def _fail(exc: BaseException) -> int:
    code = exit_code_for(exc)
    fields: dict[str, Any] = {"error_type": type(exc).__name__, "exit_code": code}
    if isinstance(exc, ConfigError):
        fields["invalid_fields"] = [str(e) for e in exc.errors]
    dependency = getattr(exc, "dependency", None)
    if dependency is not None:
        fields["dependency"] = dependency
    if code == EXIT_GENERIC_FAILURE:
        log.error("startup failed", error=_describe(exc), exc_info=exc, **fields)
    else:
        log.error("startup failed", error=str(exc), **fields)
    return code


def main(
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    *,
    clock: Clock | None = None,
    http_transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Run the CLI and return the process exit code.

    ``clock`` and ``http_transport`` are test seams for ``das drain`` / ``das undrain``.
    """
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:  # --help or a usage error (argparse already printed it)
        return exc.code if isinstance(exc.code, int) else EXIT_GENERIC_FAILURE
    environ = os.environ if env is None else env
    if args.command in ("drain", "undrain"):
        return admin_main(args, environ, clock=clock, transport=http_transport)

    # Before the config is loaded no credential is known; ConfigError messages
    # never carry values (Req 12.5, 12.6).
    configure_logging(BOOT_AGENT_ID, level=args.log_level)
    try:
        config = load_config(args.config, environ)
    except Exception as exc:  # noqa: BLE001 - mapped to an exit code
        return _fail(exc)

    configure_logging(
        str(config.agent_id), secrets=secrets_for_redaction(config), level=args.log_level
    )
    try:
        transport_classes = load_transports(config)
        return asyncio.run(run_agent(config, transport_classes))
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    except Exception as exc:  # noqa: BLE001 - mapped to an exit code
        return _fail(exc)


def entry_point() -> None:
    """Console-script target for ``das``."""
    sys.exit(main())


__all__ = [
    "BOOT_AGENT_ID",
    "EXIT_INTERRUPTED",
    "admin_command",
    "admin_main",
    "build_parser",
    "build_transport",
    "connect_with_retry",
    "entry_point",
    "load_transports",
    "main",
    "registry_endpoint_resolver",
    "run_agent",
    "secrets_for_redaction",
    "transport_names",
]
