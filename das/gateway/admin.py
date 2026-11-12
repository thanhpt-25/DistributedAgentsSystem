"""Admin_API router (Requirements 11, 12, 13).

:func:`create_admin_router` returns the ``/v1/admin`` router with the
Admin_API_Key dependency attached at router level, so every endpoint added to it
is authorized before its handler runs (Req 11.2-11.5). Audit_Entry emission is
done by :class:`das.gateway.audit.AuditMiddleware`, which
:func:`install_admin` adds to the app together with the router (Req 11.8).

:func:`register_dlq_routes` adds the dead-letter endpoints:

- ``GET /v1/admin/dlq``                     200 page | 400 ``invalid_query`` | 503
- ``GET /v1/admin/dlq/{dead_letter_id}``    200 detail | 404 | 503
- ``POST /v1/admin/dlq/{id}/replay``        202 | 404 | 409 ``<code>`` + ``current_state`` | 503
  (a Request that failed by its budget: 409 ``{"error": "budget_exceeded"}``, Phase 4 Req 5.10)
- ``POST /v1/admin/dlq/{id}/discard``       200 (idempotent) | 404 | 409 | 503

:func:`register_agent_routes` adds drain/undrain (Req 17):

- ``POST /v1/admin/agents/{id}/drain``      202 ``{agent_id, status}`` | 400 ``invalid_request``
  | 404 | 409 ``agent_state`` | 503 | 504 ``agent_timeout``
- ``POST /v1/admin/agents/{id}/undrain``    202 | 404 | 409 | 503 | 504

Replay and Discard responses increment ``das_dlq_admin_actions_total{action,
outcome}`` (Req 3.9). Calls rejected by the authorization dependency (401/403)
never reach a handler, so they never count.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Literal, TypeVar

from fastapi import APIRouter, Depends, FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import SecretStr

from das.codec import AgentControlPayload, AgentControlReplyMessage, new_message
from das.config.schema import CAPABILITY_PATTERN, ClientLimitsConfig, TenantConfig
from das.gateway.audit import ADMIN_PREFIX, AuditMiddleware
from das.gateway.auth import admin_key_auth
from das.gateway.authz import authorize_route
from das.gateway.cursor_key import CursorKeyProvider
from das.models.dlq import (
    DEAD_LETTER_ID_PATTERN,
    DEAD_LETTER_REASONS,
    RECORD_STATUSES,
    DeadLetterReason,
    DeadLetterRecord,
    RecordStatus,
)
from das.observability import get_logger
from das.observability.logging import format_timestamp
from das.observability.metrics import DlqAction, DlqAdminOutcome, GatewayMetrics
from das.ops.cursor import InvalidCursor, PageCursor, decode_cursor, encode_cursor
from das.registry import AgentRegistry
from das.state.store import StateStore
from das.transport.base import RequestTimeout, Transport

T = TypeVar("T")
log = get_logger(__name__)

#: Awaits a store operation within a timeout; failures raise ``StoreUnavailable``
#: (mapped to 503 ``service_unavailable`` by the app, Req 12.12, 13.16).
Bounded = Callable[..., Awaitable[Any]]

#: Page_Cursor endpoint tag of ``GET /v1/admin/dlq`` (Req 19.8).
DLQ_CURSOR_ENDPOINT: Final = "dlq"
DEFAULT_LIMIT: Final = 20
MAX_LIMIT: Final = 100
MAX_REQUEST_ID_LENGTH: Final = 64
#: Bound on a Replay or Discard write (Req 13.16).
ADMIN_WRITE_TIMEOUT: Final = 2.0
#: Drain/undrain: the agent must confirm within this many seconds of the call (Req 17.1, 17.12).
AGENT_CONTROL_TIMEOUT: Final = 5.0
#: Bound on the Agent_Registry lookup that precedes a drain/undrain command.
AGENT_LOOKUP_TIMEOUT: Final = 2.0

AgentCommand = Literal["drain", "undrain"]

NOT_FOUND: Final = {"error": "not_found"}

_DLID_RE: Final = re.compile(DEAD_LETTER_ID_PATTERN)
_CAPABILITY_RE: Final = re.compile(CAPABILITY_PATTERN)
_LIMIT_RE: Final = re.compile(r"[1-9][0-9]{0,2}")
#: Record fields left out of list items and of the detail's record view.
_HEAVY_FIELDS: Final = frozenset({"envelope_json", "raw_b64"})
_TIMESTAMP_FIELDS: Final = ("dead_lettered_at", "status_changed_at", "discarded_at")


def create_admin_router(
    api_keys: Iterable[SecretStr | str],
    admin_api_keys: Iterable[SecretStr | str],
    *,
    client_limits: ClientLimitsConfig | None = None,
    rate_limit: Callable[..., Awaitable[None]] | None = None,
    tenants: Sequence[TenantConfig] = (),
) -> APIRouter:
    """``/v1/admin`` router guarded by :func:`admin_key_auth`, then by ``rate_limit`` (Phase 4 Req 2.5).

    The key check runs first, so a 401/403 never consumes a token; a 429 is still audited.
    """
    # Authenticate, then authorize against the Route_Scope_Table (Phase 5 Req 2.3). admin_key_auth still lets only an
    # Operator through: the DLQ routes become tenant-scoped in task 5.4, and a tenant's admin key reaches them then.
    dependencies = [
        Depends(admin_key_auth(api_keys, admin_api_keys, client_limits, tenants=tenants)),
        Depends(authorize_route()),
    ]
    if rate_limit is not None:
        dependencies.append(Depends(rate_limit))
    # Operator routes stay out of the schema a client key can read (Phase 4 Req 11.4).
    return APIRouter(prefix=ADMIN_PREFIX, dependencies=dependencies, include_in_schema=False)


def install_admin(
    app: FastAPI,
    router: APIRouter,
    now: Callable[[], datetime] | None = None,
) -> None:
    """Include the admin router and wrap the app with :class:`AuditMiddleware`.

    Register every admin endpoint on ``router`` before calling this.
    """
    app.include_router(router)
    if now is None:
        app.add_middleware(AuditMiddleware, prefix=ADMIN_PREFIX)
    else:
        app.add_middleware(AuditMiddleware, prefix=ADMIN_PREFIX, now=now)


# --------------------------------------------------------------------------- query


class InvalidQuery(ValueError):
    """A ``GET /v1/admin/dlq`` query parameter is invalid (Req 12.9)."""

    def __init__(self, parameter: str) -> None:
        super().__init__(parameter)
        self.parameter = parameter

    def body(self) -> dict[str, str]:
        return {"error": "invalid_query", "parameter": self.parameter}


@dataclass(frozen=True, slots=True)
class DlqQuery:
    """Validated query of ``GET /v1/admin/dlq``."""

    status: RecordStatus | None = None
    reason: DeadLetterReason | None = None
    request_id: str | None = None
    capability: str | None = None
    limit: int = DEFAULT_LIMIT
    cursor: str | None = None
    filters: dict[str, str] = field(default_factory=dict)


def _single(params: Mapping[str, Sequence[str]], name: str) -> str | None:
    values = params.get(name)
    if not values:
        return None
    if len(values) != 1:
        raise InvalidQuery(name)
    return values[0]


def parse_dlq_query(params: Mapping[str, Sequence[str]]) -> DlqQuery:
    """Validate the query (pure). Checks ``status``, ``reason``, ``request_id``,
    ``capability`` and ``limit`` in that order; the cursor is verified by the handler
    with :func:`decode_cursor` (needs the HMAC key)."""
    status_ = _single(params, "status")
    if status_ is not None and status_ not in RECORD_STATUSES:
        raise InvalidQuery("status")
    reason = _single(params, "reason")
    if reason is not None and reason not in DEAD_LETTER_REASONS:
        raise InvalidQuery("reason")
    request_id = _single(params, "request_id")
    if request_id is not None and not 1 <= len(request_id) <= MAX_REQUEST_ID_LENGTH:
        raise InvalidQuery("request_id")
    capability = _single(params, "capability")
    if capability is not None and _CAPABILITY_RE.fullmatch(capability) is None:
        raise InvalidQuery("capability")
    raw_limit = _single(params, "limit")
    limit = DEFAULT_LIMIT
    if raw_limit is not None:
        if _LIMIT_RE.fullmatch(raw_limit) is None or int(raw_limit) > MAX_LIMIT:
            raise InvalidQuery("limit")
        limit = int(raw_limit)
    cursor = _single(params, "cursor")
    filters = {
        k: v
        for k, v in (
            ("status", status_),
            ("reason", reason),
            ("request_id", request_id),
            ("capability", capability),
        )
        if v is not None
    }
    return DlqQuery(
        status=status_,
        reason=reason,
        request_id=request_id,
        capability=capability,
        limit=limit,
        cursor=cursor,
        filters=filters,
    )


# --------------------------------------------------------------------------- views


def record_view(rec: DeadLetterRecord) -> dict[str, Any]:
    """Dead_Letter_Record without the envelope/raw bytes; timestamps ISO 8601 UTC ms."""
    body = rec.model_dump(mode="json", exclude=set(_HEAVY_FIELDS))
    for name in _TIMESTAMP_FIELDS:
        value = getattr(rec, name)
        body[name] = None if value is None else format_timestamp(value)
    return body


def detail_view(
    rec: DeadLetterRecord,
    request_status: str | None,
    job_state: str | None,
    job_attempt: int | None,
    request_exists: bool | None,
) -> dict[str, Any]:
    """Body of ``GET /v1/admin/dlq/{id}`` (Req 12.7, 14.14)."""
    body = record_view(rec)
    if rec.reason == "malformed" or rec.envelope_json is None:
        body["raw_b64"] = rec.raw_b64
        body["truncated"] = rec.truncated
    else:
        body["message"] = json.loads(rec.envelope_json)
    body["request_status"] = request_status
    body["job_state"] = job_state
    body["job_attempt"] = job_attempt
    body["request_exists"] = request_exists
    return body


def _conflict(code: str, current_state: str | None) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"error": code, "current_state": current_state},
    )


def _not_found() -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=dict(NOT_FOUND))


# --------------------------------------------------------------------------- routes


def register_dlq_routes(
    router: APIRouter,
    *,
    store: StateStore,
    cursor_keys: CursorKeyProvider,
    metrics: Callable[[], GatewayMetrics],
    bounded: Bounded,
) -> None:
    """Register the DLQ endpoints on the admin ``router`` (Req 12, 13).

    ``metrics`` is called per response because the Gateway builds its recorder only
    after every route is registered.
    """

    @router.get("/dlq")
    async def list_dlq(request: Request) -> Any:
        params = {k: request.query_params.getlist(k) for k in request.query_params}
        try:
            query = parse_dlq_query(params)
        except InvalidQuery as exc:
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content=exc.body())
        after: tuple[int, str] | None = None
        if query.cursor is not None:
            key = await bounded(cursor_keys.get())
            try:
                cur = decode_cursor(
                    query.cursor, key, endpoint=DLQ_CURSOR_ENDPOINT, filters=query.filters
                )
            except InvalidCursor:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST, content=InvalidQuery("cursor").body()
                )
            after = (cur.s, cur.m)
        page = await bounded(
            store.list_dead_letters(
                status=query.status,
                reason=query.reason,
                request_id=query.request_id,
                capability=query.capability,
                limit=query.limit,
                after=after,
            )
        )
        body: dict[str, Any] = {"items": [record_view(r) for r in page.records]}
        if page.next_after is not None:
            key = await bounded(cursor_keys.get())
            score, member = page.next_after
            body["next_cursor"] = encode_cursor(
                PageCursor(e=DLQ_CURSOR_ENDPOINT, f=dict(query.filters), s=score, m=member), key
            )
        return body

    @router.get("/dlq/{dead_letter_id}")
    async def get_dlq(dead_letter_id: str) -> Any:
        if _DLID_RE.fullmatch(dead_letter_id) is None:
            return _not_found()
        rec = await bounded(store.get_dead_letter(dead_letter_id))
        if rec is None:
            return _not_found()
        request_status: str | None = None
        job_state: str | None = None
        job_attempt: int | None = None
        request_exists: bool | None = None
        if rec.request_id is not None:
            req_status, job = await bounded(store.get_request_job(rec.request_id, rec.job_id))
            request_exists = req_status is not None
            if req_status is not None:
                request_status = req_status.value
            if job is not None:
                job_state = job.state.value
                job_attempt = job.attempt
        return detail_view(rec, request_status, job_state, job_attempt, request_exists)

    async def _write(action: DlqAction, op: Awaitable[T]) -> T:
        try:
            return await bounded(op, ADMIN_WRITE_TIMEOUT)
        except Exception:
            metrics().dlq_admin_action(action, "service_unavailable")
            raise

    @router.post("/dlq/{dead_letter_id}/replay")
    async def replay_dlq(dead_letter_id: str) -> Any:
        if _DLID_RE.fullmatch(dead_letter_id) is None:
            metrics().dlq_admin_action("replay", "not_found")
            return _not_found()
        result = await _write("replay", store.replay_dead_letter(dead_letter_id))
        outcome: DlqAdminOutcome = result.outcome
        metrics().dlq_admin_action("replay", outcome)
        if result.outcome == "not_found":
            return _not_found()
        if result.outcome == "budget_exceeded":
            # No current_state: the body is exactly the one of Phase 4 Req 5.10.
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT, content={"error": "budget_exceeded"}
            )
        if not result.accepted:
            return _conflict(result.outcome, result.current_state)
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "dead_letter_id": dead_letter_id,
                "request_id": result.request_id,
                "job_id": result.job_id,
                "attempt": result.new_attempt,
            },
        )

    @router.post("/dlq/{dead_letter_id}/discard")
    async def discard_dlq(dead_letter_id: str) -> Any:
        if _DLID_RE.fullmatch(dead_letter_id) is None:
            metrics().dlq_admin_action("discard", "not_found")
            return _not_found()
        result = await _write("discard", store.discard_dead_letter(dead_letter_id))
        if result.outcome == "not_found":
            metrics().dlq_admin_action("discard", "not_found")
            return _not_found()
        if result.outcome == "record_not_pending":
            metrics().dlq_admin_action("discard", "record_not_pending")
            return _conflict("record_not_pending", result.status)
        # ``accepted`` and the idempotent ``already`` are both 200 (Req 3.9, 13.12).
        metrics().dlq_admin_action("discard", "accepted")
        return {
            "dead_letter_id": dead_letter_id,
            "status": "Discarded",
            "discarded_at": (
                None if result.discarded_at is None else format_timestamp(result.discarded_at)
            ),
        }


# --------------------------------------------------------------------------- drain


class InvalidRequest(ValueError):
    """A drain body parameter is invalid (Req 17.15)."""

    def __init__(self, parameter: str) -> None:
        super().__init__(parameter)
        self.parameter = parameter

    def body(self) -> dict[str, str]:
        return {"error": "invalid_request", "parameter": self.parameter}


def parse_drain_body(raw: bytes) -> bool:
    """``release_leases`` of a drain body (pure).

    An empty body, or one without ``release_leases``, means ``False``. Anything other
    than a JSON object, or a ``release_leases`` that is not a JSON boolean (``null``
    included), raises :class:`InvalidRequest` (Req 17.15).
    """
    if not raw.strip():
        return False
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise InvalidRequest("body") from None
    if not isinstance(data, dict):
        raise InvalidRequest("body")
    if "release_leases" not in data:
        return False
    value = data["release_leases"]
    if not isinstance(value, bool):
        raise InvalidRequest("release_leases")
    return value


def register_agent_routes(
    router: APIRouter,
    *,
    registry: AgentRegistry,
    control_transport: Transport | None,
    gateway_agent_id: str | None,
    bounded: Bounded,
) -> None:
    """Register drain/undrain on the admin ``router`` (Req 17.1, 17.8, 17.10-17.12, 17.15).

    - ``POST /v1/admin/agents/{agent_id}/drain``   body ``{release_leases?: bool}``
    - ``POST /v1/admin/agents/{agent_id}/undrain``

    Checks, in order, before any command is sent: body (drain only, 400), the
    Gateway's own ID (409), registry presence (404), Unavailable or role ``gateway``
    (409), RPC_Transport configured (503). The command is then sent once with
    ``request_reply``; no confirmation within :data:`AGENT_CONTROL_TIMEOUT` of
    receiving the call is a 504 ``agent_timeout``. The Gateway never writes the
    Agent_Registry here and never resends.
    """

    async def _control(
        agent_id: str, command: AgentCommand, release_leases: bool, started: float
    ) -> JSONResponse:
        if gateway_agent_id is not None and agent_id == gateway_agent_id:
            return _agent_state("gateway")
        loop = asyncio.get_running_loop()
        remaining = AGENT_CONTROL_TIMEOUT - (loop.time() - started)
        snap = await bounded(registry.snapshot(), min(AGENT_LOOKUP_TIMEOUT, max(remaining, 0.0)))
        record = snap.get(agent_id)
        if record is None:
            return _not_found()
        if record.role == "gateway":
            return _agent_state(record.status)
        if record.status == "Unavailable":
            return _agent_state(record.status)
        if control_transport is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": "service_unavailable"},
            )
        payload = AgentControlPayload(
            command=command, agent_id=agent_id, release_leases=release_leases
        )
        message = new_message(payload, correlation_id=agent_id)
        remaining = AGENT_CONTROL_TIMEOUT - (loop.time() - started)
        reply: Any = None
        try:
            if remaining <= 0:
                raise TimeoutError
            async with asyncio.timeout(remaining):
                reply = await control_transport.request_reply(agent_id, message, timeout=remaining)
        except (TimeoutError, RequestTimeout):
            log.warning("agent control timed out", agent_id=agent_id, command=command)
            return _agent_timeout()
        except Exception as exc:  # unreachable agent, transport error, bad reply bytes
            log.warning(
                "agent control failed", agent_id=agent_id, command=command,
                error=type(exc).__name__,
            )
            return _agent_timeout()
        if not isinstance(reply, AgentControlReplyMessage) or reply.payload.agent_id != agent_id:
            log.warning("agent control got no valid confirmation", agent_id=agent_id)
            return _agent_timeout()
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"agent_id": agent_id, "status": reply.payload.status},
        )

    @router.post("/agents/{agent_id}/drain")
    async def drain_agent(agent_id: str, request: Request) -> Any:
        started = asyncio.get_running_loop().time()
        try:
            release_leases = parse_drain_body(await request.body())
        except InvalidRequest as exc:
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content=exc.body())
        return await _control(agent_id, "drain", release_leases, started)

    @router.post("/agents/{agent_id}/undrain")
    async def undrain_agent(agent_id: str) -> Any:
        started = asyncio.get_running_loop().time()
        return await _control(agent_id, "undrain", False, started)


def _agent_state(current: str) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"error": "agent_state", "status": current},
    )


def _agent_timeout() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_504_GATEWAY_TIMEOUT, content={"error": "agent_timeout"}
    )


__all__ = [
    "ADMIN_WRITE_TIMEOUT",
    "AGENT_CONTROL_TIMEOUT",
    "InvalidRequest",
    "parse_drain_body",
    "register_agent_routes",
    "DLQ_CURSOR_ENDPOINT",
    "DlqQuery",
    "InvalidQuery",
    "create_admin_router",
    "detail_view",
    "install_admin",
    "parse_dlq_query",
    "record_view",
    "register_dlq_routes",
]
