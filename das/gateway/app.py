"""Gateway_API FastAPI application (Requirements 1, 11.4, 11.8, 13.9).

``create_app`` wires the endpoints to injected dependencies so tests and the CLI
can supply a State_Store, Agent_Registry, and control Transport:

- ``POST /v1/requests``            202 ``{request_id}`` | 400 | 401 | 503
- ``GET /v1/requests``             200 page | 400 ``invalid_query`` | 401 | 503 (Req 15)
- ``GET /v1/requests/{id}``        200 snapshot | 401 | 404 | 503
- ``POST /v1/requests/{id}/cancel`` 202 ``{status: "Cancelled"}`` | 401 | 404 | 409 | 503
- ``GET /v1/agents``               200 agent list | 401 | 503
- ``GET /healthz``                 200 / 503 health report

API_Key authentication is a router-level dependency, so it is evaluated before
any handler code touches the State_Store (1.3, 11.8). Every State_Store call is
bounded by :data:`STORE_TIMEOUT` and any failure maps to 503
``service_unavailable`` without a ``request_id`` (1.9). Error bodies are always
flat ``{"error": ...}`` objects.

Phase 2: submit opens the ``gateway.submit`` Span and persists its Trace_Context
with the Request (Req 6.1); ``das_requests_submitted_total`` and the Cancelled
finish metrics only change after a successful write (Req 2.1, 2.3, 2.13); the
Request snapshot gains ``token_usage``, ``estimated_cost`` and ``replay_history``
(Req 9.8, 10.3, 13.2); agents gain ``in_flight`` and ``drained`` (Req 17.7); and
:class:`~das.gateway.http_metrics.HttpMetricsMiddleware` wraps this app (Req 2.10).
"""

from __future__ import annotations

import asyncio
import heapq
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, Final, TypeVar

from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, Header, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, SecretStr
from redis.asyncio import Redis
from starlette.exceptions import HTTPException as StarletteHTTPException

from das.client.budget import BudgetError, budget_status, budget_view, resolve_budget
from das.client.settings import ClientSettings
from das.clock import Clock, SystemClock
from das.codec import new_message
from das.config.schema import ClientLimitsConfig, EventsConfig, TenantConfig, WebhookConfig
from das.gateway.admin import (
    create_admin_router,
    install_admin,
    register_agent_routes,
    register_dlq_routes,
)
from das.gateway.auth import api_key_auth
from das.gateway.cursor_key import CursorKeyProvider
from das.gateway.events import register_events_route
from das.gateway.http_metrics import HttpMetricsMiddleware, route_templates
from das.gateway.limits import rate_limit
from das.gateway.listing import register_list_route
from das.gateway.openapi import (
    CANCEL_RESPONSES,
    QUERY_RESPONSES,
    SUBMIT_RESPONSES,
    V1_RESPONSES,
    published_schema,
)
from das.gateway.submit import (
    InvalidRequest,
    above_limit_body,
    client_spec,
    parse_budget,
    parse_idempotency_key,
)
from das.gateway.validation import validate_prompt
from das.gateway.webhooks import webhook_senders
from das.models import ControlPayload, RequestStatus
from das.models.agent import AgentRecord
from das.observability import bind_request, get_logger
from das.observability.metrics import GatewayMetrics, MetricsRecorder, NoopRecorder
from das.observability.trace_context import TraceContext
from das.observability.tracing import NoopTracer, Tracer
from das.ops.cost import PriceTable, estimate_cost
from das.ops.usage import summarize_usage
from das.registry import AgentRegistry
from das.state.store import StateStore, StoredRequest, StoreStatus
from das.transport.base import Transport

log = get_logger(__name__)

#: Redis socket and connect timeout in seconds (design: Gateway_API).
REDIS_SOCKET_TIMEOUT: Final = 2.0
#: Overall bound on one State_Store / Agent_Registry operation, keeping 503 under 5 s (1.9).
STORE_TIMEOUT: Final = 4.5
#: Bound on the best-effort ``cancel_request`` control command to the lease owner.
CANCEL_NOTIFY_TIMEOUT: Final = 2.0

SERVICE_UNAVAILABLE: Final = {"error": "service_unavailable"}
NOT_FOUND: Final = {"error": "not_found"}
REQUEST_TERMINAL: Final = {"error": "request_terminal"}
INVALID_BODY: Final = {"error": "invalid_body"}

#: Returns a health report; ``report["overall"] == "healthy"`` means HTTP 200.
HealthCheck = Callable[[], Awaitable[Mapping[str, Any]]]
#: Builds the Gateway's metrics recorder from the registered route templates (Req 5.2).
MetricsFactory = Callable[[Sequence[str]], MetricsRecorder]

T = TypeVar("T")


def build_redis_client(url: str, **kwargs: Any) -> Redis:
    """Redis client for the Gateway with the 2 s socket and connect timeouts (1.9)."""
    kwargs.setdefault("socket_timeout", REDIS_SOCKET_TIMEOUT)
    kwargs.setdefault("socket_connect_timeout", REDIS_SOCKET_TIMEOUT)
    kwargs.setdefault("decode_responses", True)
    return Redis.from_url(url, **kwargs)


class StoreUnavailable(Exception):
    """A State_Store or Agent_Registry call failed or timed out."""


class SubmitBody(BaseModel):
    prompt: str
    #: Raw ``budget`` value: parsed by ``parse_budget`` so a bad field is a 400 naming it (Phase 4 Req 5.1).
    budget: Any = None


async def _bounded(op: Awaitable[T], timeout: float = STORE_TIMEOUT) -> T:
    """Await a store operation within ``timeout`` seconds, normalizing failures."""
    try:
        async with asyncio.timeout(timeout):
            return await op
    except Exception as exc:  # redis errors, timeouts, StoreError, decode errors
        raise StoreUnavailable(type(exc).__name__) from exc


def format_heartbeat(ts: float) -> str:
    """UTC ISO 8601 timestamp with second precision, e.g. ``2025-01-01T00:00:00Z`` (11.4)."""
    return datetime.fromtimestamp(int(ts), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def agent_view(record: AgentRecord) -> dict[str, Any]:
    return {
        "agent_id": record.agent_id,
        "role": record.role,
        "host": record.host,
        "capabilities": sorted(record.capabilities),
        "status": record.status,
        "last_heartbeat": format_heartbeat(record.last_heartbeat),
        # Phase 2 (Req 17.7): Jobs executing (Worker) or Leases held (Manager) per
        # the latest heartbeat; ``drained`` iff Draining with nothing in flight.
        "in_flight": record.in_flight,
        "drained": record.status == "Draining" and record.in_flight == 0,
    }


def _transition_views(snap: StoredRequest) -> list[dict[str, Any]]:
    """Job entries (v1 shape) merged with request-level entries (``scope: "request"``).

    Both lists are already in append order; merging by ``at`` keeps each list's own
    order and places the request entries (Replay ``Failed -> Pending``) in time.
    """
    jobs = [(t.at, t.model_dump(mode="json")) for t in (snap.transitions or [])]
    reqs = [(t.at, t.model_dump(mode="json")) for t in (snap.request_transitions or [])]
    if not reqs:
        return [view for _, view in jobs]
    return [view for _, view in heapq.merge(jobs, reqs, key=lambda item: item[0])]


def request_view(
    snap: StoredRequest, price_table: PriceTable | None = None
) -> dict[str, Any]:
    """Status body for ``GET /v1/requests/{id}`` (1.2, 13.9; Phase 2 Req 9.8, 10.3, 13.2).

    Every v1 field keeps its name, type and meaning (Req 9.13, 19.6); Phase 2 adds
    ``token_usage``, ``estimated_cost`` (null without a Price_Table, Req 10.7) and
    ``replay_history``. Phase 4 adds ``budget`` (Req 5.9) and ``webhook`` (Req 8.9), both ``null``
    when the Request has none. ``snap`` must be loaded with ``include_usage=True`` and
    ``include_client=True``.
    """
    body: dict[str, Any] = {
        "request_id": snap.request_id,
        "status": snap.status.value,
        "jobs": [{"job_id": jid, "state": rec.state.value} for jid, rec in snap.jobs.items()],
        "transitions": _transition_views(snap),
    }
    if snap.status is RequestStatus.COMPLETED and snap.final_result is not None:
        body["final_result"] = snap.final_result.model_dump(mode="json")
    if snap.error:
        body["error"] = snap.error
    records = snap.usage or []
    job_ids: Sequence[str] = snap.plan.job_ids() if snap.plan is not None else list(snap.jobs)
    body["token_usage"] = summarize_usage(records, job_ids).model_dump(mode="json")
    body["estimated_cost"] = (
        estimate_cost(records, price_table).model_dump(mode="json")
        if price_table is not None
        else None
    )
    body["replay_history"] = [e.model_dump(mode="json") for e in snap.replay_history]
    body["budget"] = (
        budget_view(snap.budget, budget_status(records, snap.budget, price_table))
        if snap.budget is not None
        else None
    )
    body["webhook"] = (
        {
            "state": snap.webhook.state,
            "attempts": snap.webhook.attempts,
            "last_status": snap.webhook.last_status,
        }
        if snap.webhook is not None
        else None
    )
    return body


def _duration_seconds(finished_at_ms: int | None, created_at_ms: int | None) -> float:
    """``max(0, Finished_At - created_at)`` in seconds, from script-returned values."""
    if finished_at_ms is None or created_at_ms is None:
        return 0.0
    return max(0, finished_at_ms - created_at_ms) / 1000.0


def _store_context(tracer: Tracer) -> TraceContext | None:
    """Trace_Context of the active ``gateway.submit`` Span, sampled or not (Req 6.1)."""
    current = tracer.current_context()
    if current is None:
        return None
    return TraceContext.parse(current.traceparent, current.tracestate)


def _error(code: int, body: Mapping[str, Any], headers: Mapping[str, str] | None = None) -> JSONResponse:
    return JSONResponse(status_code=code, content=dict(body), headers=dict(headers or {}))


def create_app(
    store: StateStore,
    registry: AgentRegistry,
    control_transport: Transport | None,
    api_keys: Iterable[SecretStr | str],
    max_prompt_length: int,
    health_check: HealthCheck | None = None,
    admin_api_keys: Iterable[SecretStr | str] = (),
    *,
    metrics_factory: MetricsFactory | None = None,
    tracer: Tracer | None = None,
    price_table: PriceTable | None = None,
    clock: Clock | None = None,
    cursor_secret: SecretStr | str | None = None,
    agent_id: str | None = None,
    client_limits: ClientLimitsConfig | None = None,
    events: EventsConfig | None = None,
    events_client: Redis | None = None,
    webhook: WebhookConfig | None = None,
    webhook_secrets: Mapping[str, SecretStr] | None = None,
    tenants: Sequence[TenantConfig] = (),
) -> FastAPI:
    """Build the Gateway_API with injected dependencies.

    ``metrics_factory`` receives the registered route templates (known only once
    every route is registered) and returns the recorder backing ``das_*`` Gateway
    metrics; it is exposed as ``app.state.metrics_recorder`` for the Ops server.
    Without a factory, metrics go to a :class:`NoopRecorder`. ``tracer`` defaults
    to :class:`NoopTracer` (no Span, no persisted Trace_Context, Req 6.13).
    ``price_table`` is ``None`` when the Gateway has no Price_Table (Req 10.7).
    ``cursor_secret`` (``gateway.cursor_secret``) keys the Page_Cursor HMAC; without
    it the key shared at ``das:gateway:cursor_key`` is used (design D10). The
    provider is exposed as ``app.state.cursor_keys``. ``agent_id`` is the Gateway's
    own agent ID: drain/undrain naming it is rejected with 409 (Req 17.11).
    ``client_limits`` (``client_limits`` config, Phase 4 Req 1) sets each key's rate limit; without
    it the defaults apply. Every ``/v1`` and admin call takes one token after key auth (Req 2.1, 2.5).
    ``events`` (``events`` config) sets the Event_Stream length stored with each new Request (Req 6.5)
    and the SSE limits (Req 7.5, 7.7). ``events_client`` is the dedicated Redis client for the SSE
    endpoint's blocking reads (its socket timeout must exceed the block time); without one the
    store's client is used. The replica's open-stream gate is ``app.state.event_gate``.
    ``webhook`` and ``webhook_secrets`` (``webhook`` config and ``webhook_secrets``, Phase 4 Req 8)
    configure the Webhook_Sender, whose ``webhook.workers`` tasks run for the app's lifespan, so the
    server must run ASGI lifespan events for deliveries to be sent; secrets are looked up by name at
    send time (Req 8.8).
    """
    tracer = tracer or NoopTracer()
    clock = clock or SystemClock()
    cursor_keys = CursorKeyProvider(lambda: store.client, cursor_secret)
    # Bound at the end of this function, once the route templates are known; the
    # handlers below only read it while serving requests.
    metrics: GatewayMetrics

    async def default_health() -> Mapping[str, Any]:
        try:
            async with asyncio.timeout(REDIS_SOCKET_TIMEOUT):
                await store.client.ping()
            redis_state = "connected"
        except Exception:
            redis_state = "disconnected"
        healthy = redis_state == "connected"
        return {"overall": "healthy" if healthy else "unhealthy", "redis": redis_state}

    check = health_check or default_health
    api_keys = tuple(api_keys)
    admin_api_keys = tuple(admin_api_keys)
    webhook_config = webhook if webhook is not None else WebhookConfig()
    senders_secrets = webhook_secrets if webhook_secrets is not None else {}

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        async with webhook_senders(
            store=store,
            secrets=senders_secrets,
            config=webhook_config,
            clock=clock,
            metrics=lambda: metrics,
        ):
            yield

    app = FastAPI(
        title="das-gateway", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan
    )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        body = exc.detail if isinstance(exc.detail, Mapping) else {"error": str(exc.detail)}
        return _error(exc.status_code, body, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _invalid_body(_: Request, __: RequestValidationError) -> JSONResponse:
        return _error(status.HTTP_400_BAD_REQUEST, INVALID_BODY)

    @app.exception_handler(StoreUnavailable)
    async def _unavailable(_: Request, exc: StoreUnavailable) -> JSONResponse:
        log.warning("state store unavailable", reason=str(exc))
        return _error(status.HTTP_503_SERVICE_UNAVAILABLE, SERVICE_UNAVAILABLE)

    async def notify_owner(rid: str) -> None:
        """Best-effort ``cancel_request`` to the lease owner; failures are logged only."""
        if control_transport is None:
            return
        with bind_request(rid):
            try:
                async with asyncio.timeout(CANCEL_NOTIFY_TIMEOUT):
                    lease = await store.get_lease(rid)
                    if lease is None:
                        return
                    msg = new_message(
                        ControlPayload(command="cancel_request", request_id=rid),
                        correlation_id=rid,
                    )
                    await control_transport.publish(lease.owner, msg)
            except Exception as exc:
                log.warning("cancel_request notify failed", error=type(exc).__name__)

    limits_config = client_limits if client_limits is not None else ClientLimitsConfig()
    events_config = events if events is not None else EventsConfig()
    enforce_rate = rate_limit(store=store, bounded=_bounded, metrics=lambda: metrics)
    v1 = APIRouter(
        prefix="/v1",
        dependencies=[
            Depends(api_key_auth(api_keys, admin_api_keys, limits_config, tenants=tenants)),
            Depends(enforce_rate),
        ],
        responses=V1_RESPONSES,
    )

    @v1.post("/requests", status_code=status.HTTP_202_ACCEPTED, responses=SUBMIT_RESPONSES)
    async def submit(
        request: Request,
        body: SubmitBody,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> Any:
        settings: ClientSettings = request.state.client_settings
        with tracer.span("gateway.submit") as span:
            problem = validate_prompt(body.prompt, max_prompt_length)
            if problem is not None:
                span.fail(problem.error)
                return _error(status.HTTP_400_BAD_REQUEST, problem.to_body())
            try:
                key = parse_idempotency_key(idempotency_key)
                client_budget = parse_budget(body.budget)
                effective_budget = resolve_budget(client_budget, settings.default_budget)
            except InvalidRequest as exc:
                span.fail("invalid_request")
                return _error(status.HTTP_400_BAD_REQUEST, exc.to_body())
            except BudgetError as exc:
                metrics.client_rejected("budget_above_limit")
                span.fail("budget_above_limit")
                return _error(status.HTTP_400_BAD_REQUEST, above_limit_body(exc))
            spec = client_spec(
                settings,
                prompt=body.prompt,
                effective_budget=effective_budget,
                client_budget=client_budget,
                idempotency_key=key,
                limits=limits_config,
                events=events_config,
            )
            # Persisted in the same atomic write that creates the Request (Req 6.1); the same
            # script answers a replay or a quota rejection without writing (Req 4.2-4.5, 3.2).
            result = await _bounded(
                store.submit_request(body.prompt, spec, trace_context=_store_context(tracer))
            )
            if result.outcome == "quota_exceeded":
                metrics.client_rejected("quota_exceeded")
                span.fail("quota_exceeded")
                return _error(
                    status.HTTP_429_TOO_MANY_REQUESTS,
                    {"error": "quota_exceeded", "limit": result.limit},
                )
            if result.outcome == "idempotency_conflict":
                metrics.client_rejected("idempotency_key_reused")
                span.fail("idempotency_key_reused")
                return _error(
                    status.HTTP_422_UNPROCESSABLE_CONTENT, {"error": "idempotency_key_reused"}
                )
            rid = result.request_id
            assert rid is not None  # created and replayed always carry the Request_ID
            span.set_attributes({"das.request_id": rid})
        if result.outcome == "replayed":
            metrics.idempotent_replay()  # not a new Request: no das_requests_submitted_total (Req 4.3)
            with bind_request(rid):
                log.info("request replayed")
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"request_id": rid},
                headers={"Idempotent-Replayed": "true"},
            )
        metrics.request_submitted()  # only after submit_request created the Request (Req 2.1)
        with bind_request(rid):
            log.info("request created")
        return {"request_id": rid}

    @v1.get("/requests/{request_id}", responses=QUERY_RESPONSES)
    async def get_request(request_id: str) -> Any:
        # One MULTI/EXEC including every UsageRecord, the Webhook_Delivery and the budget: a
        # failed or partial read is a 503, never a summary of partial records (Req 9.12).
        snap = await _bounded(
            store.load_snapshot(
                request_id, include_transitions=True, include_usage=True, include_client=True
            )
        )
        if snap is None:
            return _error(status.HTTP_404_NOT_FOUND, NOT_FOUND)
        return request_view(snap, price_table)

    @v1.post(
        "/requests/{request_id}/cancel", status_code=status.HTTP_202_ACCEPTED, responses=CANCEL_RESPONSES
    )
    async def cancel(request_id: str, background: BackgroundTasks) -> Any:
        # The Price_Table lets a budgeted Request's request.cancelled carry its Budget_Used (Req 5.9).
        result = await _bounded(store.cancel_request(request_id, prices=price_table))
        if result.status is StoreStatus.NOT_FOUND:
            return _error(status.HTTP_404_NOT_FOUND, NOT_FOUND)
        if result.status is StoreStatus.TERMINAL:
            body = dict(REQUEST_TERMINAL)
            if result.request_status is not None:
                body["status"] = result.request_status.value
            return _error(status.HTTP_409_CONFLICT, body)
        # Only a successful Cancelled write counts; TERMINAL above leaves it (Req 2.3, 2.13).
        metrics.request_finished(
            RequestStatus.CANCELLED,
            _duration_seconds(result.finished_at_ms, result.created_at_ms),
        )
        background.add_task(notify_owner, request_id)
        return {"status": RequestStatus.CANCELLED.value}

    @v1.get("/agents")
    async def agents() -> Any:
        snap = await _bounded(registry.snapshot())
        return [agent_view(a) for a in snap.agents]

    # GET /v1/requests (Req 15): same API_Key / Admin_API_Key auth as the rest of /v1.
    register_list_route(v1, store=store, cursor_keys=cursor_keys, bounded=_bounded)

    # GET /v1/requests/{id}/events (Phase 4 Req 7): same auth and rate limit as the rest of /v1.
    event_gate = register_events_route(
        v1,
        store=store,
        bounded=_bounded,
        metrics=lambda: metrics,
        clock=clock,
        config=events_config,
        view=lambda snap: request_view(snap, price_table),
        events_client=events_client,
    )
    app.state.event_gate = event_gate

    @v1.get("/openapi.json", include_in_schema=False)
    async def openapi_document() -> Any:
        """The client API's OpenAPI schema, behind the same key check and rate limit (Phase 4 Req 11.4)."""
        if getattr(app.state, "published_schema", None) is None:
            app.state.published_schema = published_schema(app)  # every route is registered by now
        return JSONResponse(app.state.published_schema)

    app.include_router(v1)

    # Admin_API (Req 11): router-level Admin_API_Key auth plus Audit_Entry middleware.
    admin = create_admin_router(
        api_keys, admin_api_keys, client_limits=limits_config, rate_limit=enforce_rate, tenants=tenants
    )
    register_dlq_routes(
        admin,
        store=store,
        cursor_keys=cursor_keys,
        metrics=lambda: metrics,
        bounded=_bounded,
    )
    register_agent_routes(
        admin,
        registry=registry,
        control_transport=control_transport,
        gateway_agent_id=agent_id,
        bounded=_bounded,
    )
    install_admin(app, admin)

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        try:
            report = dict(await check())
        except Exception:
            report = {"overall": "unhealthy"}
        code = 200 if report.get("overall") == "healthy" else 503
        return JSONResponse(status_code=code, content=report)

    # HTTP metrics (Req 2.10): built after every route is registered so ``route`` is
    # bounded to the real templates plus ``_unmatched`` (Req 5.2). Added last, so it
    # is the outermost user middleware and times the whole call, audit included.
    templates = route_templates(app.routes)
    recorder = metrics_factory(templates) if metrics_factory is not None else NoopRecorder()
    metrics = GatewayMetrics(recorder)
    metrics.sse_streams(lambda: event_gate.open)
    app.state.metrics_recorder = recorder
    app.state.gateway_metrics = metrics
    app.state.route_templates = templates
    app.state.cursor_keys = cursor_keys
    app.add_middleware(HttpMetricsMiddleware, metrics=metrics, clock=clock)

    return app


__all__ = [
    "CANCEL_NOTIFY_TIMEOUT",
    "REDIS_SOCKET_TIMEOUT",
    "STORE_TIMEOUT",
    "HealthCheck",
    "MetricsFactory",
    "StoreUnavailable",
    "agent_view",
    "build_redis_client",
    "create_app",
    "format_heartbeat",
    "request_view",
]
