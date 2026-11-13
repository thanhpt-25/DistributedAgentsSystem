"""``GET /v1/usage`` and ``GET /v1/admin/usage``: per-tenant Usage_Reports (Phase 5 Req 6.3, 6.4, 6.5).

The state scripts keep one rollup hash per tenant and UTC day. These routes read the hashes for the days of a range
(``from`` and ``to``, inclusive, by default the last seven days, at most 92) and turn them into the report with the
pure :func:`das.tenancy.usage.build_report`, pricing the models when the report is read.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from das.config.schema import TENANT_ID_PATTERN
from das.gateway.openapi import USAGE_PARAMETERS, USAGE_RESPONSES
from das.ops.cost import PriceTable
from das.state.store import StateStore
from das.tenancy.scopes import DEFAULT_TENANT, Principal
from das.tenancy.usage import UsageRangeError, build_report, parse_range

#: Awaits a store operation within a timeout; failures become 503 (see ``das.gateway.app``).
Bounded = Callable[..., Awaitable[Any]]

_TENANT_RE: Final = re.compile(TENANT_ID_PATTERN)


class InvalidUsageQuery(ValueError):
    """A usage query parameter is invalid (400 ``invalid_query`` naming it)."""

    def __init__(self, parameter: str) -> None:
        super().__init__(parameter)
        self.parameter = parameter


def _single(params: Mapping[str, Sequence[str]], name: str) -> str | None:
    values = params.get(name)
    if not values:
        return None
    if len(values) != 1:
        raise InvalidUsageQuery(name)
    return values[0]


def _bad_query(parameter: str) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"error": "invalid_query", "parameter": parameter}
    )


async def _reports(
    store: StateStore,
    bounded: Bounded,
    price_table: PriceTable | None,
    tenants: Sequence[str],
    params: Mapping[str, Sequence[str]],
) -> list[dict[str, Any]] | JSONResponse:
    """The report of each of ``tenants`` for the range in ``params``, or the 400 for a bad range. ``today`` is the
    current UTC day by Redis ``TIME``, the clock the rollups are keyed by."""
    try:
        from_, to = _single(params, "from"), _single(params, "to")
        today = datetime.fromtimestamp(await bounded(store.server_time()), UTC).date()
        day_range = parse_range(from_, to, today)
    except InvalidUsageQuery as exc:
        return _bad_query(exc.parameter)
    except UsageRangeError as exc:
        return _bad_query(exc.parameter)
    days = day_range.days()
    out = []
    for tenant in tenants:
        rollups = await bounded(store.read_usage(tenant, days))
        out.append(build_report(tenant, day_range, rollups, price_table))
    return out


def register_usage_route(
    router: APIRouter, *, store: StateStore, bounded: Bounded, price_table: PriceTable | None
) -> None:
    """Register ``GET /usage`` on the ``/v1`` router: the report of the caller's own tenant (Scope ``read``)."""

    @router.get("/usage", responses=USAGE_RESPONSES, openapi_extra={"parameters": USAGE_PARAMETERS})
    async def usage_report(request: Request) -> Any:
        principal: Principal = request.state.principal
        params = {k: request.query_params.getlist(k) for k in request.query_params}
        reports = await _reports(store, bounded, price_table, [principal.tenant], params)
        return reports if isinstance(reports, JSONResponse) else reports[0]


def register_admin_usage_route(
    router: APIRouter,
    *,
    store: StateStore,
    bounded: Bounded,
    price_table: PriceTable | None,
    tenants: Sequence[str],
) -> None:
    """Register ``GET /usage`` on the admin router (Operator): one tenant given by ``tenant``, or every tenant.

    ``tenants`` are the configured tenants; the default tenant is always reported too.
    """
    every = [DEFAULT_TENANT, *[t for t in tenants if t != DEFAULT_TENANT]]

    @router.get("/usage")
    async def admin_usage_report(request: Request) -> Any:
        params = {k: request.query_params.getlist(k) for k in request.query_params}
        try:
            tenant = _single(params, "tenant") if "tenant" in params else None
        except InvalidUsageQuery as exc:
            return _bad_query(exc.parameter)
        if "tenant" in params and (tenant is None or _TENANT_RE.fullmatch(tenant) is None):
            return _bad_query("tenant")
        reports = await _reports(store, bounded, price_table, [tenant] if tenant else every, params)
        if isinstance(reports, JSONResponse):
            return reports
        return reports[0] if tenant else {"tenants": reports}


__all__ = ["InvalidUsageQuery", "register_admin_usage_route", "register_usage_route"]
