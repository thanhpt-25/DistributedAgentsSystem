"""Scopes, Principal and the route table (das-multi-tenant task 1.1; Phase 5 Req 2.2, 2.5)."""

from __future__ import annotations

import dataclasses
import itertools

import pytest

from das.tenancy.scopes import OPERATOR, PUBLIC, ROUTE_SCOPES, Principal, Scope, authorize

ALL_SCOPES = (Scope.SUBMIT, Scope.READ, Scope.ADMIN)
REQUIREMENTS = (*ALL_SCOPES, OPERATOR, PUBLIC)


def principal(scopes: tuple[Scope, ...] = (), *, operator: bool = False, tenant: str = "team-a") -> Principal:
    return Principal(tenant=tenant, scopes=frozenset(scopes), operator=operator, fingerprint="1a2b3c4d")


def subsets() -> list[tuple[Scope, ...]]:
    return [c for r in range(len(ALL_SCOPES) + 1) for c in itertools.combinations(ALL_SCOPES, r)]


# --------------------------------------------------------------------------- vocabulary


def test_the_scopes_are_exactly_submit_read_and_admin() -> None:
    assert {s.value for s in Scope} == {"submit", "read", "admin"}
    assert Scope("read") is Scope.READ


def test_operator_and_public_are_plain_strings_that_are_not_scopes() -> None:
    assert (OPERATOR, PUBLIC) == ("operator", "public")
    with pytest.raises(ValueError):
        Scope("operator")


def test_a_principal_is_immutable_and_hashable() -> None:
    p = principal((Scope.READ,))
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.tenant = "other"  # type: ignore[misc]
    assert len({p, principal((Scope.READ,))}) == 1


# --------------------------------------------------------------------------- authorize


@pytest.mark.parametrize("held", subsets(), ids=lambda s: "+".join(x.value for x in s) or "none")
@pytest.mark.parametrize("required", ALL_SCOPES)
def test_a_tenant_principal_passes_a_scope_only_when_it_holds_it(held: tuple[Scope, ...], required: Scope) -> None:
    assert authorize(principal(held), required) is (required in held)


@pytest.mark.parametrize("held", subsets(), ids=lambda s: "+".join(x.value for x in s) or "none")
def test_a_tenant_principal_never_passes_the_operator_requirement(held: tuple[Scope, ...]) -> None:
    assert authorize(principal(held), OPERATOR) is False


@pytest.mark.parametrize("held", subsets(), ids=lambda s: "+".join(x.value for x in s) or "none")
def test_public_needs_nothing(held: tuple[Scope, ...]) -> None:
    assert authorize(principal(held), PUBLIC) is True


@pytest.mark.parametrize("required", REQUIREMENTS)
def test_an_operator_holds_every_scope_and_the_operator_role(required: object) -> None:
    assert authorize(principal((), operator=True), required) is True  # type: ignore[arg-type]


# --------------------------------------------------------------------------- route table

EXPECTED: dict[tuple[str, str], object] = {
    ("POST", "/v1/requests"): Scope.SUBMIT,
    ("POST", "/v1/requests/{request_id}/cancel"): Scope.SUBMIT,
    ("GET", "/v1/requests"): Scope.READ,
    ("GET", "/v1/requests/{request_id}"): Scope.READ,
    ("GET", "/v1/requests/{request_id}/events"): Scope.READ,
    ("GET", "/v1/agents"): Scope.READ,
    ("GET", "/v1/usage"): Scope.READ,
    ("GET", "/v1/openapi.json"): Scope.READ,
    ("GET", "/v1/admin/dlq"): Scope.ADMIN,
    ("GET", "/v1/admin/dlq/{dead_letter_id}"): Scope.ADMIN,
    ("POST", "/v1/admin/dlq/{dead_letter_id}/replay"): Scope.ADMIN,
    ("POST", "/v1/admin/dlq/{dead_letter_id}/discard"): Scope.ADMIN,
    ("POST", "/v1/admin/agents/{agent_id}/drain"): OPERATOR,
    ("POST", "/v1/admin/agents/{agent_id}/undrain"): OPERATOR,
    ("GET", "/v1/admin/usage"): OPERATOR,
    ("GET", "/healthz"): PUBLIC,
}


def test_the_route_table_is_exactly_the_design_table() -> None:
    assert dict(ROUTE_SCOPES) == EXPECTED


def test_every_entry_is_an_upper_case_method_and_an_absolute_template() -> None:
    for method, template in ROUTE_SCOPES:
        assert method == method.upper() and method in {"GET", "POST"}
        assert template.startswith("/")


def test_only_the_health_route_is_public_and_no_v1_route_is() -> None:
    public = [key for key, req in ROUTE_SCOPES.items() if req == PUBLIC]
    assert public == [("GET", "/healthz")]


def test_the_route_table_cannot_be_modified_at_runtime() -> None:
    with pytest.raises(TypeError):
        ROUTE_SCOPES[("GET", "/v1/evil")] = PUBLIC  # type: ignore[index]
