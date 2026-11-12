"""The Gateway enforces the Route_Scope_Table on every route (das-multi-tenant tasks 4.2, 4.4, 4.5; Phase 5 Req 2.2,
2.3, 2.4, 2.5, 2.6, 2.7).

``authorize_route`` runs after authentication and before the rate limit. It looks the route up as ``(method,
template)`` in the table, fails closed when there is no entry, and refuses a Principal that lacks the Scope or role
with 403 ``forbidden`` and a ``scope`` field. The admin Audit_Entry gains ``tenant`` and ``operator``; the agents view
hides agent ids and hosts from a tenant that is not the default one."""

from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator, Iterator, Mapping
from typing import Any

import httpx
import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from das.config.schema import TenantConfig
from das.gateway.app import create_app
from das.gateway.auth import API_KEY_HEADER, api_key_auth
from das.gateway.authz import authorize_route
from das.observability.logging import configure_logging
from das.ops.keys import key_fingerprint
from das.registry import AgentInfo, AgentRegistry
from das.state import StateStore
from das.tenancy.scopes import OPERATOR, ROUTE_SCOPES, Scope

DEFAULT_KEY, READER, SUBMITTER = "default-key-0000001", "reader-key-00000001", "submitter-key-000001"
# Not named "operator": the log redactor masks every 8-character window of a key, which would mask that field name.
ADMINNER, ADMIN = "adminner-key-000001", "root-key-000000000001"
OTHER_TENANT_READER = "other-reader-key-0001"


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def team(tid: str, **keys_: list[str]) -> TenantConfig:
    entries = [{"fingerprint": fp(k), "scopes": scopes} for k, scopes in keys_.items()]
    return TenantConfig.model_validate(
        {"id": tid, "keys": entries, **({"settings": {"rate": 1, "burst": 1}} if tid == "team-a" else {})}
    )


TENANTS = [
    team("team-a", **{READER: ["read"], SUBMITTER: ["submit"], ADMINNER: ["read", "admin"]}),
    team("team-b", **{OTHER_TENANT_READER: ["read"]}),
]
ALL_KEYS = [DEFAULT_KEY, READER, SUBMITTER, ADMINNER, OTHER_TENANT_READER]


def h(key: str) -> dict[str, str]:
    return {API_KEY_HEADER: key}


# =========================================================================== authorize_route on its own


def bare_app(table: Mapping[tuple[str, str], Any], ran: list[str]) -> FastAPI:
    app = FastAPI()

    @app.exception_handler(StarletteHTTPException)
    async def flat(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        body = exc.detail if isinstance(exc.detail, Mapping) else {"error": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=body)

    deps = [Depends(api_key_auth(ALL_KEYS, [ADMIN], tenants=TENANTS)), Depends(authorize_route(table))]

    @app.post("/w/{item_id}", dependencies=deps)
    async def write(item_id: str) -> dict[str, str]:
        ran.append(f"write:{item_id}")
        return {}

    @app.get("/r", dependencies=deps)
    async def read() -> dict[str, str]:
        ran.append("read")
        return {}

    @app.get("/unlisted", dependencies=deps)
    async def unlisted() -> dict[str, str]:
        ran.append("unlisted")
        return {}

    return app


TABLE = {("POST", "/w/{item_id}"): Scope.SUBMIT, ("GET", "/r"): Scope.READ}


async def call(app: FastAPI, method: str, path: str, key: str | None) -> httpx.Response:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        return await c.request(method, path, headers=h(key) if key else {})


async def test_a_principal_with_the_scope_reaches_the_handler_and_the_template_is_the_key() -> None:
    ran: list[str] = []
    assert (await call(bare_app(TABLE, ran), "POST", "/w/anything", SUBMITTER)).status_code == 200
    assert (await call(bare_app(TABLE, ran), "GET", "/r", READER)).status_code == 200
    assert ran == ["write:anything", "read"]  # /w/anything was found as /w/{item_id}


async def test_a_principal_without_the_scope_gets_403_naming_it_and_the_handler_never_runs() -> None:
    ran: list[str] = []
    reply = await call(bare_app(TABLE, ran), "POST", "/w/x", READER)
    assert reply.status_code == 403 and reply.json() == {"error": "forbidden", "scope": "submit"}
    reply = await call(bare_app(TABLE, ran), "GET", "/r", SUBMITTER)
    assert reply.status_code == 403 and reply.json() == {"error": "forbidden", "scope": "read"}
    assert ran == []


async def test_an_operator_holds_every_scope() -> None:
    ran: list[str] = []
    for method, path in (("POST", "/w/x"), ("GET", "/r")):
        assert (await call(bare_app(TABLE, ran), method, path, ADMIN)).status_code == 200
    assert len(ran) == 2


async def test_an_operator_only_route_refuses_every_tenant_key_naming_the_operator_role() -> None:
    ran: list[str] = []
    table = {**TABLE, ("GET", "/r"): OPERATOR}
    reply = await call(bare_app(table, ran), "GET", "/r", ADMINNER)
    assert reply.status_code == 403 and reply.json() == {"error": "forbidden", "scope": "operator"}
    assert (await call(bare_app(table, ran), "GET", "/r", ADMIN)).status_code == 200


async def test_a_route_without_a_table_entry_fails_closed_for_everyone() -> None:
    ran: list[str] = []
    for key in (DEFAULT_KEY, ADMINNER, ADMIN):
        reply = await call(bare_app(TABLE, ran), "GET", "/unlisted", key)
        assert reply.status_code == 403 and reply.json() == {"error": "forbidden"}, key
    assert ran == []


async def test_the_method_is_part_of_the_key() -> None:
    ran: list[str] = []
    table = {("GET", "/w/{item_id}"): Scope.READ}  # only a GET is listed; the POST is not
    assert (await call(bare_app(table, ran), "POST", "/w/x", ADMIN)).status_code == 403


async def test_authentication_comes_first_so_an_unknown_key_is_401_not_403() -> None:
    ran: list[str] = []
    assert (await call(bare_app(TABLE, ran), "GET", "/unlisted", "unknown-key")).status_code == 401
    assert (await call(bare_app(TABLE, ran), "GET", "/r", None)).status_code == 401


async def test_without_a_principal_the_check_refuses_rather_than_assuming() -> None:
    app = FastAPI()
    app.get("/r", dependencies=[Depends(authorize_route(TABLE))])(lambda: {})
    reply = await call(app, "GET", "/r", "anything")
    assert reply.status_code in (401, 403)


# =========================================================================== the real application


@pytest.fixture
async def gateway(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    registry = AgentRegistry(fake_redis)
    for agent_id, role in (("worker-1", "worker"), ("manager-1", "manager")):
        await registry.register(AgentInfo(agent_id=agent_id, role=role, host=f"{agent_id}.internal"))
    app = create_app(state_store, registry, None, ALL_KEYS, 100, admin_api_keys=[ADMIN], tenants=TENANTS)
    app.state.table_probe = True
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        yield c


async def test_a_read_only_key_reads_and_cannot_submit_or_cancel(gateway: httpx.AsyncClient) -> None:
    assert (await gateway.get("/v1/agents", headers=h(DEFAULT_KEY))).status_code == 200  # default: submit and read
    refused = await gateway.post("/v1/requests", json={"prompt": "x"}, headers=h(READER))
    assert refused.status_code == 403 and refused.json() == {"error": "forbidden", "scope": "submit"}
    cancel = await gateway.post("/v1/requests/some-id/cancel", headers=h(READER))
    assert cancel.status_code == 403 and cancel.json() == {"error": "forbidden", "scope": "submit"}


async def test_a_submit_only_key_cannot_read(gateway: httpx.AsyncClient) -> None:
    for path in ("/v1/requests", "/v1/requests/some-id", "/v1/agents", "/v1/openapi.json"):
        reply = await gateway.get(path, headers=h(SUBMITTER))
        assert reply.status_code == 403 and reply.json() == {"error": "forbidden", "scope": "read"}, path


async def test_a_refused_call_is_decided_before_any_state_store_access() -> None:
    """403 comes from the table alone: the Gateway here has no usable store, and still answers 403 not 5xx."""
    ran: list[str] = []
    reply = await call(bare_app(TABLE, ran), "POST", "/w/x", READER)
    assert reply.status_code == 403 and ran == []


async def test_a_refusal_does_not_spend_a_rate_limit_token(gateway: httpx.AsyncClient) -> None:
    # team-a's burst is 1. Five refused calls must leave that one token for the allowed one.
    for _ in range(5):
        assert (await gateway.post("/v1/requests", json={"prompt": "x"}, headers=h(READER))).status_code == 403
    assert (await gateway.get("/v1/agents", headers=h(READER))).status_code == 200
    assert (await gateway.get("/v1/agents", headers=h(READER))).status_code == 429


async def test_a_tenant_admin_key_still_cannot_use_the_admin_router_until_it_is_tenant_scoped(
    gateway: httpx.AsyncClient,
) -> None:
    """The DLQ routes are not tenant-scoped yet (task 5.4): only an Operator may reach them until they are."""
    assert (await gateway.get("/v1/admin/dlq", headers=h(ADMINNER))).status_code == 403
    assert (await gateway.get("/v1/admin/dlq", headers=h(ADMIN))).status_code == 200


async def test_an_admin_route_without_a_table_entry_is_refused_even_to_an_operator() -> None:
    """The admin router fails closed too: a route the table does not describe is not served."""
    from das.gateway.admin import create_admin_router, install_admin

    app = FastAPI()
    router = create_admin_router(ALL_KEYS, [ADMIN], tenants=TENANTS)
    ran: list[str] = []

    @router.get("/dlq")  # listed: GET /v1/admin/dlq
    async def listed() -> dict[str, str]:
        ran.append("listed")
        return {}

    @router.get("/unlisted")  # not in the table
    async def unlisted() -> dict[str, str]:
        ran.append("unlisted")
        return {}

    install_admin(app, router)
    assert (await call(app, "GET", "/v1/admin/dlq", ADMIN)).status_code == 200
    assert (await call(app, "GET", "/v1/admin/unlisted", ADMIN)).status_code == 403
    assert ran == ["listed"]


async def test_every_route_of_the_application_has_an_entry_in_the_table(gateway: httpx.AsyncClient) -> None:
    app = gateway._transport.app  # type: ignore[attr-defined]
    missing = [
        (method, route.path_format)
        for route in app.routes
        if hasattr(route, "path_format")
        for method in getattr(route, "methods", None) or ()
        if method not in ("HEAD", "OPTIONS")
        and route.path_format.startswith("/v1")
        and (method, route.path_format) not in ROUTE_SCOPES
    ]
    assert missing == []


# =========================================================================== the Audit_Entry (task 4.4)


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    out = io.StringIO()
    configure_logging("gw-test", secrets=[ADMIN, ADMINNER, READER], stream=out)
    yield out
    configure_logging("test", stream=io.StringIO())


def audits(stream: io.StringIO) -> list[dict[str, Any]]:
    lines = [json.loads(line) for line in stream.getvalue().splitlines() if line]
    return [e for e in lines if e.get("message") == "audit"]


async def test_the_audit_entry_names_the_tenant_and_whether_the_caller_is_an_operator(
    gateway: httpx.AsyncClient, stream: io.StringIO
) -> None:
    await gateway.get("/v1/admin/dlq", headers=h(ADMIN))
    await gateway.get("/v1/admin/dlq", headers=h(ADMINNER))
    ok, refused = audits(stream)
    assert (ok["tenant"], ok["operator"], ok["http_status"], ok["key_fingerprint"]) == ("default", True, 200, fp(ADMIN))
    assert (refused["tenant"], refused["operator"], refused["http_status"]) == ("team-a", False, 403)
    assert refused["key_fingerprint"] == fp(ADMINNER)


async def test_an_unauthenticated_admin_call_has_no_tenant_and_no_operator_claim(
    gateway: httpx.AsyncClient, stream: io.StringIO
) -> None:
    await gateway.get("/v1/admin/dlq")
    await gateway.get("/v1/admin/dlq", headers=h("not-a-key"))
    for entry in audits(stream):
        assert (entry["http_status"], entry["tenant"], entry["operator"], entry["key_fingerprint"]) == (
            401,
            None,
            None,
            None,
        )


async def test_no_key_value_reaches_the_audit_log(gateway: httpx.AsyncClient, stream: io.StringIO) -> None:
    for key in (ADMIN, ADMINNER, "not-a-key"):
        await gateway.get("/v1/admin/dlq", headers=h(key))
    text = stream.getvalue()
    for key in (ADMIN, ADMINNER, READER, "not-a-key"):
        assert key not in text


# =========================================================================== the agents view (task 4.5)


def ids(reply: httpx.Response) -> set[str | None]:
    return {a.get("agent_id") for a in reply.json()}


async def test_an_operator_and_the_default_tenant_see_every_agent_in_full(gateway: httpx.AsyncClient) -> None:
    for key in (ADMIN, DEFAULT_KEY):
        reply = await gateway.get("/v1/agents", headers=h(key))
        assert ids(reply) == {"worker-1", "manager-1"}, key
        assert {a["host"] for a in reply.json()} == {"worker-1.internal", "manager-1.internal"}


async def test_another_tenant_sees_no_agent_id_and_no_host(gateway: httpx.AsyncClient) -> None:
    reply = await gateway.get("/v1/agents", headers=h(OTHER_TENANT_READER))
    agents = reply.json()
    assert len(agents) == 2
    for agent in agents:
        assert "agent_id" not in agent and "host" not in agent
        assert {"role", "capabilities", "status", "in_flight", "drained"} <= set(agent)  # what a tenant may know


async def test_a_tenant_key_that_is_also_read_scoped_for_admin_sees_the_restricted_view(
    gateway: httpx.AsyncClient,
) -> None:
    reply = await gateway.get("/v1/agents", headers=h(ADMINNER))
    assert all("agent_id" not in a and "host" not in a for a in reply.json())
