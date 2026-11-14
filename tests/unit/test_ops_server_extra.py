"""Extra Ops server unit tests (task 15.3): Req 1.2, 8.2, 16.6, 16.7.

Complements ``test_ops_server.py`` with: the ``llm_provider`` readiness field never
issuing a completion, the default 2 s per-check timeout inside the 3 s bound, the
Gateway ``http_port`` app answering 404 for ops paths, and ``NoopTracer`` opening
no socket.
"""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import Sequence
from typing import Any, ClassVar

import httpx
import pytest

from das.clock import FakeClock
from das.config.schema import TracingConfig
from das.gateway.app import create_app
from das.health.server import (
    DEFAULT_CHECK_TIMEOUT,
    DEFAULT_OVERALL_TIMEOUT,
    LLM_PROVIDER,
    STATE_STORE,
    CachedCheck,
    LivenessSource,
    agent_readiness,
    create_ops_app,
)
from das.health.supervisor import EventLoopWatchdog, LoopSupervisor
from das.llm.base import ChatMessage, Completion, ModelParams
from das.llm.retry import LLMProvider
from das.models.results import TokenUsage
from das.observability.metrics import MetricsRecorder
from das.observability.tracing import NoopTracer, build_tracer
from das.registry import AgentRegistry

KEY = "test-api-key-123"
ADMIN = "test-admin-key-456"


class _Log:
    def warning(self, *_: Any, **__: Any) -> None: ...

    def error(self, *_: Any, **__: Any) -> None: ...


def _const(value: bool):
    async def check() -> bool:
        return value

    return check


async def _get(app: Any, path: str, **headers: str) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.get(path, headers=headers)


def _ops_app(readiness: Any, **kw: Any) -> Any:
    clock = FakeClock()
    return create_ops_app(
        metrics=None,
        liveness=LivenessSource(LoopSupervisor(log=_Log()), EventLoopWatchdog(clock), clock),
        readiness=readiness,
        **kw,
    )


# --- llm_provider never calls a completion (Req 16.6) --------------------------------------


class RecordingAdapter:
    provider: ClassVar[str] = "openai"

    def __init__(self, ping_ok: bool) -> None:
        self.ping_ok = ping_ok
        self.pings = 0
        self.completions = 0

    async def complete_once(
        self, system: str, messages: Sequence[ChatMessage], params: ModelParams, timeout: float
    ) -> Completion:
        self.completions += 1
        return Completion(content="x", usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2))

    async def ping(self) -> bool:
        self.pings += 1
        return self.ping_ok


class RecordingObserver:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        def record(*_: Any, **__: Any) -> None:
            self.calls.append(name)

        return record


@pytest.mark.parametrize("ping_ok", [True, False])
async def test_readyz_llm_provider_uses_ping_never_completion(ping_ok: bool) -> None:
    adapter = RecordingAdapter(ping_ok)
    observer = RecordingObserver()
    llm = LLMProvider(
        adapter, ModelParams(model="m", temperature=0.0, max_output_tokens=16), observer=observer
    )
    clock = FakeClock()
    readiness = agent_readiness(
        startup=lambda: True,
        registered_available=_const(True),
        transports={"rpc": _const(True)},
        state_store=_const(True),
        draining=lambda: False,
        shutting_down=lambda: False,
        llm_provider=CachedCheck(llm.ping, clock=clock),
    )
    app = _ops_app(readiness)
    resp = await _get(app, "/readyz")
    # Informational only: HTTP status stays 200 regardless of the LLM state.
    assert resp.status_code == 200
    assert resp.json()[LLM_PROVIDER] == ("connected" if ping_ok else "disconnected")
    assert adapter.pings == 1
    assert adapter.completions == 0
    assert observer.calls == []  # no LLM call observed => no Usage_Record


# --- default 2 s per-check timeout within 3 s (Req 16.7) -----------------------------------


async def test_readyz_default_timeouts_2s_per_check_3s_overall() -> None:
    assert DEFAULT_CHECK_TIMEOUT == 2.0 and DEFAULT_OVERALL_TIMEOUT == 3.0

    async def slow_ok() -> bool:
        await asyncio.sleep(1.5)
        return True

    async def hangs() -> bool:
        await asyncio.sleep(60)
        return True

    readiness = agent_readiness(
        startup=lambda: True,
        registered_available=_const(True),
        transports={"rpc": slow_ok},
        state_store=hangs,
        draining=lambda: False,
        shutting_down=lambda: False,
    )
    app = _ops_app(readiness)
    start = time.monotonic()
    resp = await _get(app, "/readyz")
    elapsed = time.monotonic() - start
    assert 1.9 <= elapsed < 3.0
    body = resp.json()
    assert resp.status_code == 503
    assert body["checks"]["transport:rpc"] == "pass"
    assert body["failed"] == [STATE_STORE]


# --- Gateway http_port: ops paths are 404 (Req 1.2, 16.8) ----------------------------------


@pytest.mark.parametrize("path", ["/metrics", "/livez", "/readyz"])
@pytest.mark.parametrize("key", [None, KEY, ADMIN])
async def test_gateway_http_port_returns_404_for_ops_paths(
    state_store: Any, fake_redis: Any, path: str, key: str | None
) -> None:
    app = create_app(
        state_store,
        AgentRegistry(fake_redis),
        None,
        [KEY],
        10,
        admin_api_keys=[ADMIN],
        metrics_factory=lambda t: MetricsRecorder(clock=FakeClock(), route_templates=t),
    )
    headers = {"X-API-Key": key} if key else {}
    resp = await _get(app, path, **headers)
    assert resp.status_code == 404
    # /healthz stays on http_port (v1 behaviour).
    assert (await _get(app, "/healthz")).status_code == 200


# --- NoopTracer opens no socket (Req 8.2) --------------------------------------------------


async def test_tracing_disabled_builds_noop_tracer_without_exporter_or_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: list[Any] = []
    real_init = socket.socket.__init__

    def spy_init(self: socket.socket, *args: Any, **kwargs: Any) -> None:
        opened.append(args)
        real_init(self, *args, **kwargs)

    def forbid(*_: Any, **__: Any) -> Any:
        raise AssertionError("exporter must not be created when tracing is disabled")

    monkeypatch.setattr(socket.socket, "__init__", spy_init)
    monkeypatch.setattr("das.observability.tracing.make_exporter_factory", forbid)
    monkeypatch.setattr("das.observability.tracing.BoundedExportProcessor", forbid)

    cfg = TracingConfig(enabled=False, otlp_endpoint="http://otel:4317")
    tracer = build_tracer(cfg, "worker", "w1", FakeClock())
    assert isinstance(tracer, NoopTracer)
    with tracer.span("worker.job", attributes={"das.job_id": "a"}, root=True) as span:
        span.set_attributes({"k": "v"})
        assert tracer.current_context() is None
    assert await tracer.shutdown(5.0) == 0
    assert opened == []


# --- extra routes: the HTTP_Broker on the Gateway's Ops_Port (Phase 7 Req 2.2) ----------------


async def test_extra_routes_are_served_beside_the_ops_endpoints_and_absent_by_default() -> None:
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    async def hello(_request: Any) -> JSONResponse:
        return JSONResponse({"hello": "broker"})

    readiness = {"state_store": _const(True)}
    plain = _ops_app(readiness)
    assert (await _get(plain, "/broker/v1/hello")).status_code == 404

    with_extra = _ops_app(readiness, extra_routes=[Route("/broker/v1/hello", hello, methods=["GET"])])
    assert (await _get(with_extra, "/broker/v1/hello")).json() == {"hello": "broker"}
    assert (await _get(with_extra, "/livez")).status_code == 200
    assert (await _get(with_extra, "/metrics")).status_code == 404  # metrics are off here; the ops routes still answer
