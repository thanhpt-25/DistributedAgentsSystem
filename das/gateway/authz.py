"""Route authorization against the Route_Scope_Table (Phase 5 Req 2.2, 2.3, 2.4, 2.5).

:func:`authorize_route` is a router-level dependency that runs after authentication (which left the Principal on
``request.state``) and before the rate limit, so a refused call costs no token and touches no State_Store. It looks the
route up as ``(method, route template)`` in the table of :mod:`das.tenancy.scopes`. A route with no entry fails closed:
nobody, an Operator included, may use a route the table does not describe.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from fastapi import HTTPException, Request, status

from das.gateway.auth import FORBIDDEN_BODY, UNAUTHORIZED_BODY
from das.observability import get_logger
from das.tenancy.scopes import ROUTE_SCOPES, Principal, Requirement, authorize

log = get_logger(__name__)


def authorize_route(
    table: Mapping[tuple[str, str], Requirement] = ROUTE_SCOPES,
) -> Callable[..., Awaitable[None]]:
    """Dependency refusing a Principal that lacks what its route requires: 403 ``forbidden`` with a ``scope`` field
    naming the Scope, or ``operator``. A route without a table entry is refused without one."""

    async def require_scope(request: Request) -> None:
        principal = getattr(request.state, "principal", None)
        if not isinstance(principal, Principal):  # authentication did not run: never assume a role
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED, dict(UNAUTHORIZED_BODY), headers={"WWW-Authenticate": "ApiKey"}
            )
        template = getattr(request.scope.get("route"), "path_format", None)
        requirement = table.get((request.method, template)) if isinstance(template, str) else None
        if requirement is None:
            log.warning("route without a scope entry", method=request.method, route=template)
            raise HTTPException(status.HTTP_403_FORBIDDEN, dict(FORBIDDEN_BODY))
        if not authorize(principal, requirement):
            raise HTTPException(status.HTTP_403_FORBIDDEN, {**FORBIDDEN_BODY, "scope": str(requirement)})

    return require_scope


__all__ = ["authorize_route"]
