"""``GET /v1/requests``: filtered, paginated Request listing (Requirement 15).

:func:`parse_list_query` validates the query without I/O (Req 15.2, 15.8) and
:func:`decode_list_cursor` verifies a Page_Cursor against the normalized filters
of the call (Req 19.8, 19.9). :func:`register_list_route` adds the endpoint to the
``/v1`` router, so API_Key / Admin_API_Key authorization runs first (Req 15.9, 20.1).

Handler flow: parse the query (400 ``invalid_query{parameter}``), verify the
cursor (400 ``invalid_query{parameter: "cursor"}``), check that the Request_Index
backfill completed (503 ``index_not_ready``, Req 15.10), then read ``limit + 1``
index entries and their fields in one bounded call (503 ``service_unavailable``
on failure, never a partial page, Req 15.13). Requests deleted between the index
read and the field read are skipped (Req 14.11), and a cursor whose position was
deleted resumes at the next existing Request (Req 15.12).
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from das.config.schema import TENANT_ID_PATTERN
from das.gateway.cursor_key import CursorKeyProvider
from das.gateway.openapi import LIST_PARAMETERS, LIST_RESPONSES
from das.models import RequestStatus
from das.observability.logging import format_timestamp
from das.ops.cursor import InvalidCursor, PageCursor, decode_cursor, encode_cursor
from das.state.store import IndexNotReady, RequestListItem, StateStore
from das.tenancy.scopes import Principal

#: Awaits a store operation within a timeout; failures raise ``StoreUnavailable``
#: (mapped to 503 ``service_unavailable`` by the app, Req 15.13).
Bounded = Callable[..., Awaitable[Any]]

#: Page_Cursor endpoint tag of ``GET /v1/requests`` (Req 19.8).
REQUESTS_CURSOR_ENDPOINT: Final = "requests"
DEFAULT_LIMIT: Final = 20
MAX_LIMIT: Final = 100
MAX_STATUS_VALUES: Final = 5

INDEX_NOT_READY: Final = {"error": "index_not_ready"}
_TENANT_RE: Final = re.compile(TENANT_ID_PATTERN)
#: The tenant filter of a cursor that lists every tenant (an Operator without ``tenant``).
ALL_TENANTS: Final = "*"

#: Canonical order of statuses in the normalized ``status`` filter.
_STATUS_ORDER: Final = (
    RequestStatus.PENDING,
    RequestStatus.RUNNING,
    RequestStatus.COMPLETED,
    RequestStatus.FAILED,
    RequestStatus.CANCELLED,
)
_STATUS_BY_NAME: Final = {s.value: s for s in _STATUS_ORDER}
_LIMIT_RE: Final = re.compile(r"[1-9][0-9]{0,2}")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MS: Final = timedelta(milliseconds=1)


class InvalidQuery(ValueError):
    """A ``GET /v1/requests`` query parameter is invalid (Req 15.8)."""

    def __init__(self, parameter: str) -> None:
        super().__init__(parameter)
        self.parameter = parameter

    def body(self) -> dict[str, str]:
        return {"error": "invalid_query", "parameter": self.parameter}


@dataclass(frozen=True, slots=True)
class ListQuery:
    """Validated query of ``GET /v1/requests``.

    ``filters`` is the normalized form bound into Page_Cursors: only the filters
    given, ``status`` as a de-duplicated list in canonical order and timestamps as
    UTC epoch milliseconds, so equivalent queries produce equal filters.
    """

    statuses: tuple[RequestStatus, ...] | None = None
    created_after_ms: int | None = None
    created_before_ms: int | None = None
    limit: int = DEFAULT_LIMIT
    cursor: str | None = None
    filters: dict[str, Any] = field(default_factory=dict)
    #: The ``tenant`` the Operator asked for; ``None`` when absent. Only an Operator may send it (Phase 5 Req 3.4).
    tenant: str | None = None


def _single(params: Mapping[str, Sequence[str]], name: str) -> str | None:
    values = params.get(name)
    if not values:
        return None
    if len(values) != 1:
        raise InvalidQuery(name)
    return values[0]


def _parse_statuses(raw: str) -> tuple[RequestStatus, ...]:
    chosen: set[RequestStatus] = set()
    for part in raw.split(","):
        st = _STATUS_BY_NAME.get(part)
        if st is None:
            raise InvalidQuery("status")
        chosen.add(st)
    if not 1 <= len(chosen) <= MAX_STATUS_VALUES:
        raise InvalidQuery("status")
    return tuple(s for s in _STATUS_ORDER if s in chosen)


def parse_timestamp_ms(raw: str) -> int:
    """ISO 8601 timestamp with a time zone → UTC epoch milliseconds (floored).

    Raises ``ValueError`` for anything else, including timestamps without a zone.
    """
    if not raw or raw != raw.strip():
        raise ValueError("empty or padded timestamp")
    moment = datetime.fromisoformat(raw)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("timestamp without time zone")
    return (moment.astimezone(UTC) - _EPOCH) // _ONE_MS


def _timestamp(params: Mapping[str, Sequence[str]], name: str) -> int | None:
    raw = _single(params, name)
    if raw is None:
        return None
    try:
        return parse_timestamp_ms(raw)
    except (ValueError, OverflowError) as exc:
        raise InvalidQuery(name) from exc


def parse_list_query(params: Mapping[str, Sequence[str]], *, allow_tenant: bool = False) -> ListQuery:
    """Validate the query (pure, Req 15.2, 15.8).

    ``tenant`` is accepted only with ``allow_tenant`` (an Operator): from anyone else it is an invalid parameter, and
    for an Operator it must be a valid Tenant_ID (Phase 5 Req 3.4). It is checked first.

    Checks ``status``, ``created_after``, ``created_before`` (and that
    ``created_after`` is earlier than ``created_before``, reported as
    ``created_after``), then ``limit``. The cursor is verified separately by
    :func:`decode_list_cursor`, which needs the HMAC key.
    """
    tenant = _single(params, "tenant") if "tenant" in params else None
    if "tenant" in params and (not allow_tenant or tenant is None or _TENANT_RE.fullmatch(tenant) is None):
        raise InvalidQuery("tenant")
    raw_status = _single(params, "status")
    statuses = _parse_statuses(raw_status) if raw_status is not None else None
    after_ms = _timestamp(params, "created_after")
    before_ms = _timestamp(params, "created_before")
    if after_ms is not None and before_ms is not None and after_ms >= before_ms:
        raise InvalidQuery("created_after")
    raw_limit = _single(params, "limit")
    limit = DEFAULT_LIMIT
    if raw_limit is not None:
        if _LIMIT_RE.fullmatch(raw_limit) is None or int(raw_limit) > MAX_LIMIT:
            raise InvalidQuery("limit")
        limit = int(raw_limit)
    cursor = _single(params, "cursor")
    filters: dict[str, Any] = {}
    if statuses is not None:
        filters["status"] = [s.value for s in statuses]
    if after_ms is not None:
        filters["created_after"] = after_ms
    if before_ms is not None:
        filters["created_before"] = before_ms
    return ListQuery(
        statuses=statuses,
        created_after_ms=after_ms,
        created_before_ms=before_ms,
        limit=limit,
        cursor=cursor,
        filters=filters,
        tenant=tenant,
    )


def scoped(query: ListQuery, principal: Principal) -> tuple[ListQuery, str | None]:
    """``query`` bound to the tenant the call may list, and that tenant (``None``: every tenant, an Operator only).

    The tenant goes into the cursor filters, so a cursor issued for one tenant fails for another (Req 3.3, 3.4)."""
    scope = query.tenant if principal.operator else principal.tenant
    return replace(query, filters={**query.filters, "tenant": scope or ALL_TENANTS}), scope


def decode_list_cursor(query: ListQuery, key: bytes) -> tuple[int, str] | None:
    """``(created_at_ms, request_id)`` to resume after, or ``None`` without a cursor.

    Raises :class:`InvalidQuery` (``cursor``) for any cursor not issued by the
    Gateway for ``GET /v1/requests`` with the same filters (Req 15.8, 19.9).
    """
    if query.cursor is None:
        return None
    try:
        cur = decode_cursor(
            query.cursor, key, endpoint=REQUESTS_CURSOR_ENDPOINT, filters=query.filters
        )
    except InvalidCursor as exc:
        raise InvalidQuery("cursor") from exc
    return cur.s, cur.m


def encode_list_cursor(query: ListQuery, position: tuple[int, str], key: bytes) -> str:
    score, member = position
    return encode_cursor(
        PageCursor(e=REQUESTS_CURSOR_ENDPOINT, f=dict(query.filters), s=score, m=member), key
    )


def _ms_to_iso(ms: int) -> str:
    return format_timestamp(_EPOCH + timedelta(milliseconds=ms))


def list_item_view(item: RequestListItem) -> dict[str, Any]:
    """One list entry (Req 15.3): no prompt, instruction, Job_Result or Final_Result."""
    return {
        "request_id": item.request_id,
        "status": item.status.value,
        "created_at": _ms_to_iso(item.created_at_ms),
        "finished_at": None if item.finished_at_ms is None else _ms_to_iso(item.finished_at_ms),
        "job_count": item.job_count,
        "token_usage": {
            "input": item.usage.input_tokens,
            "output": item.usage.output_tokens,
            "total": item.usage.total_tokens,
        },
    }


def _bad_query(exc: InvalidQuery) -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content=exc.body())


def register_list_route(
    router: APIRouter,
    *,
    store: StateStore,
    cursor_keys: CursorKeyProvider,
    bounded: Bounded,
) -> None:
    """Register ``GET /requests`` on the authenticated ``/v1`` ``router``."""

    @router.get("/requests", responses=LIST_RESPONSES, openapi_extra={"parameters": LIST_PARAMETERS})
    async def list_requests(request: Request) -> Any:
        params = {k: request.query_params.getlist(k) for k in request.query_params}
        principal: Principal = request.state.principal
        try:
            query, scope = scoped(parse_list_query(params, allow_tenant=principal.operator), principal)
        except InvalidQuery as exc:
            return _bad_query(exc)
        after: tuple[int, str] | None = None
        if query.cursor is not None:
            key = await bounded(cursor_keys.get())
            try:
                after = decode_list_cursor(query, key)
            except InvalidQuery as exc:
                return _bad_query(exc)
        if not await bounded(store.index_ready()):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=dict(INDEX_NOT_READY)
            )
        try:
            page = await bounded(
                store.list_requests(
                    statuses=query.statuses,
                    created_after_ms=query.created_after_ms,
                    created_before_ms=query.created_before_ms,
                    limit=query.limit,
                    after=after,
                    tenant=scope,
                )
            )
        except IndexNotReady:  # a non-default tenant before the Tenant_Backfill finished (Req 9.3)
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=dict(INDEX_NOT_READY)
            )
        body: dict[str, Any] = {"items": [list_item_view(it) for it in page.items]}
        if page.next_after is not None:
            key = await bounded(cursor_keys.get())
            body["next_cursor"] = encode_list_cursor(query, page.next_after, key)
        return body


__all__ = [
    "ALL_TENANTS",
    "DEFAULT_LIMIT",
    "INDEX_NOT_READY",
    "MAX_LIMIT",
    "REQUESTS_CURSOR_ENDPOINT",
    "InvalidQuery",
    "ListQuery",
    "decode_list_cursor",
    "encode_list_cursor",
    "list_item_view",
    "parse_list_query",
    "parse_timestamp_ms",
    "register_list_route",
    "scoped",
]
