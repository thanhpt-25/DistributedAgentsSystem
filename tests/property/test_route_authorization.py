# Feature: das-multi-tenant, Property 4: Every route is classified, and authorization is exact
"""Property 4: Every route is classified, and authorization is exact.

Every route registered on the Gateway application has an entry in the Route_Scope_Table, and every entry names a route
the application has. For any Principal (any tenant, any set of Scopes, Operator or not) and any route of the table, the
call passes the authorization check if and only if the Principal holds the Scope or role the table requires; a refusal
names that requirement and never runs the handler; an Operator holds every Scope and the operator role. A route the
table does not describe is refused for everyone.

**Validates: Requirements 2.2, 2.3, 2.4, 2.5**
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from hypothesis import given
from hypothesis import strategies as st
from starlette.exceptions import HTTPException as StarletteHTTPException

from das.gateway.app import create_app
from das.gateway.auth import api_key_auth
from das.gateway.authz import authorize_route
from das.registry import AgentRegistry
from das.state import StateStore
from das.tenancy.scopes import OPERATOR, PUBLIC, ROUTE_SCOPES, Principal, Scope, authorize
from tests.fixtures.routes import method_templates

KEY = "any-key-for-the-fixed-directory"
ENTRIES = sorted((m, t, req) for (m, t), req in ROUTE_SCOPES.items() if req != PUBLIC)


def concrete(template: str) -> str:
    """A path that matches ``template``: every ``{name}`` replaced by a value."""
    out = template
    while "{" in out:
        start, end = out.index("{"), out.index("}")
        out = out[:start] + "x" + out[end + 1 :]
    return out


# --------------------------------------------------------------------------- the table and the application agree


def real_routes() -> set[tuple[str, str]]:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    app = create_app(StateStore(redis), AgentRegistry(redis), None, ["k"], 10, admin_api_keys=["a"])
    return method_templates(app)  # refuses to return fewer routes than a Gateway has


def test_every_route_of_the_application_has_a_table_entry() -> None:
    assert sorted(r for r in real_routes() if r not in ROUTE_SCOPES) == []


def test_every_table_entry_names_a_route_the_application_has() -> None:
    """A stale entry would be a route someone believes is protected and that does not exist (or was renamed)."""
    routes = real_routes()
    stale = sorted((m, t) for (m, t), req in ROUTE_SCOPES.items() if req != PUBLIC and (m, t) not in routes)
    assert stale == [], f"table entries without a route: {stale}"


def test_the_requirements_are_the_scopes_the_roles_and_nothing_else() -> None:
    assert {req for req in ROUTE_SCOPES.values()} <= {*Scope, OPERATOR, PUBLIC}


# --------------------------------------------------------------------------- authorization is exact


class Holder:
    """A KeyDirectory whose Principal the property sets before each call."""

    principal: Principal | None = None

    def resolve(self, key: str | None) -> Principal | None:
        return self.principal


def stub_app(holder: Holder, ran: list[tuple[str, str]]) -> FastAPI:
    """One stub route for every non-public entry of the table, behind authentication and the real check."""
    app = FastAPI()

    @app.exception_handler(StarletteHTTPException)
    async def flat(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        body = exc.detail if isinstance(exc.detail, Mapping) else {"error": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=body)

    deps = [Depends(api_key_auth([], [], directory=holder)), Depends(authorize_route())]
    for method, template, _ in ENTRIES:

        def handler(method: str = method, template: str = template) -> dict[str, str]:
            ran.append((method, template))
            return {}

        app.add_api_route(template, handler, methods=[method], dependencies=deps)
    return app


principals = st.builds(
    Principal,
    tenant=st.sampled_from(["default", "team-a", "team-b"]),
    scopes=st.sets(st.sampled_from(list(Scope))).map(frozenset),
    operator=st.booleans(),
    fingerprint=st.just("abcd1234"),
)


async def call(app: FastAPI, method: str, path: str) -> httpx.Response:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        return await client.request(method, path, headers={"X-API-Key": KEY})


@given(principals, st.sampled_from(ENTRIES))
def test_a_call_passes_exactly_when_the_principal_meets_the_requirement(principal: Principal, entry: Any) -> None:
    method, template, requirement = entry
    holder, ran = Holder(), []
    holder.principal = principal
    reply = asyncio.run(call(stub_app(holder, ran), method, concrete(template)))
    if authorize(principal, requirement):
        assert reply.status_code == 200 and ran == [(method, template)]
    else:
        assert reply.status_code == 403 and ran == []  # the handler never ran
        assert reply.json() == {"error": "forbidden", "scope": str(requirement)}


@given(principals)
def test_an_operator_passes_every_route_and_a_tenant_key_never_passes_an_operator_route(principal: Principal) -> None:
    holder, ran = Holder(), []
    holder.principal = principal
    app = stub_app(holder, ran)
    outcomes = {(m, t): asyncio.run(call(app, m, concrete(t))).status_code == 200 for m, t, _ in ENTRIES}
    if principal.operator:
        assert all(outcomes.values())
    for (m, t), passed in outcomes.items():
        if ROUTE_SCOPES[(m, t)] == OPERATOR and not principal.operator:
            assert not passed, (m, t)
        if ROUTE_SCOPES[(m, t)] in Scope:
            assert passed == (principal.operator or ROUTE_SCOPES[(m, t)] in principal.scopes), (m, t)


@given(principals, st.sampled_from(["GET", "POST"]))
def test_a_route_the_table_does_not_describe_is_refused_for_everyone(principal: Principal, method: str) -> None:
    holder, ran = Holder(), []
    holder.principal = principal
    app = stub_app(holder, ran)
    guards = [Depends(api_key_auth([], [], directory=holder)), Depends(authorize_route())]
    app.add_api_route(
        "/v1/not-in-the-table", lambda: ran.append(("x", "x")) or {}, methods=[method], dependencies=guards
    )
    reply = asyncio.run(call(app, method, "/v1/not-in-the-table"))
    assert reply.status_code == 403 and ran == []
