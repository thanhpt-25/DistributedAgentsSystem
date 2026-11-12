"""Scopes, Principal and the route table (Phase 5 Req 2.2, 2.5). Pure: no I/O, no logging.

A call is authenticated once into a :class:`Principal`; :func:`authorize` then decides, from one table
(:data:`ROUTE_SCOPES`), whether that Principal may use the route. The table lists every route explicitly, so it is
the one place to read what each route requires, and a test walks the application's routes against it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Literal

__all__ = ["DEFAULT_TENANT", "OPERATOR", "PUBLIC", "ROUTE_SCOPES", "Principal", "Requirement", "Scope", "authorize"]


class Scope(StrEnum):
    """What a key may do inside its tenant."""

    SUBMIT = "submit"
    READ = "read"
    ADMIN = "admin"


#: The platform role: not bound to one tenant, holds every Scope. Not a Scope a tenant key can be given.
OPERATOR: Final = "operator"
#: A route that needs no authentication (health checks).
PUBLIC: Final = "public"
#: The implicit tenant of every Request without a ``tenant`` field and of every key no tenant claims (Req 1.2, 3.1).
DEFAULT_TENANT: Final = "default"

Requirement = Scope | Literal["operator", "public"]


@dataclass(frozen=True, slots=True)
class Principal:
    """The result of authenticating a call (Req 2.1)."""

    tenant: str
    scopes: frozenset[Scope]
    operator: bool
    fingerprint: str


def authorize(principal: Principal, requirement: Requirement) -> bool:
    """Whether ``principal`` meets ``requirement``. An Operator holds every Scope and the operator role (Req 2.5)."""
    if requirement == PUBLIC:
        return True
    if requirement == OPERATOR:
        return principal.operator
    return principal.operator or requirement in principal.scopes


_TABLE: dict[tuple[str, str], Requirement] = {
    # submit
    ("POST", "/v1/requests"): Scope.SUBMIT,
    ("POST", "/v1/requests/{request_id}/cancel"): Scope.SUBMIT,
    # read
    ("GET", "/v1/requests"): Scope.READ,
    ("GET", "/v1/requests/{request_id}"): Scope.READ,
    ("GET", "/v1/requests/{request_id}/events"): Scope.READ,
    ("GET", "/v1/agents"): Scope.READ,
    ("GET", "/v1/usage"): Scope.READ,
    ("GET", "/v1/openapi.json"): Scope.READ,
    # admin: the caller's own tenant (records of no tenant and of other tenants are not visible, Req 5)
    ("GET", "/v1/admin/dlq"): Scope.ADMIN,
    ("GET", "/v1/admin/dlq/{dead_letter_id}"): Scope.ADMIN,
    ("POST", "/v1/admin/dlq/{dead_letter_id}/replay"): Scope.ADMIN,
    ("POST", "/v1/admin/dlq/{dead_letter_id}/discard"): Scope.ADMIN,
    # operator: the platform
    ("POST", "/v1/admin/agents/{agent_id}/drain"): OPERATOR,
    ("POST", "/v1/admin/agents/{agent_id}/undrain"): OPERATOR,
    ("GET", "/v1/admin/usage"): OPERATOR,
    # public
    ("GET", "/healthz"): PUBLIC,
}

#: ``(METHOD, route template) -> Requirement``, read-only. Templates are the application's ``path_format``.
ROUTE_SCOPES: Final = MappingProxyType(_TABLE)
