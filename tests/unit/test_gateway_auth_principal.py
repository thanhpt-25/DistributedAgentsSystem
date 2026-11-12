"""The Gateway authenticates to a Principal (das-multi-tenant task 4.1; Phase 5 Req 1.2, 1.3, 2.1, 4.1, 4.4).

``api_key_auth`` asks the KeyDirectory who the presented key is, leaves the Principal on ``request.state.principal`` and
the Client_Settings (now with the tenant layer) on ``request.state.client_settings``. A missing or unknown key is 401 as
before. ``admin_key_auth`` keeps its Phase 4 answers: 403 ``admin_disabled`` only when nothing in the configuration can
administer."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import Depends, FastAPI, Request

from das.config.schema import ClientLimitsConfig, ClientOverride, ClientSettingsConfig, TenantConfig
from das.gateway.app import create_app
from das.gateway.auth import API_KEY_HEADER, admin_key_auth, api_key_auth, audit_context
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore
from das.tenancy.scopes import DEFAULT_TENANT, Principal, Scope

API_KEY, TEAM_KEY, WRITER_KEY, OTHER_KEY, ADMIN_KEY = (
    "api-key-0000000001", "team-key-000000001", "writer-key-00000001", "other-key-00000001", "admin-key-0000001",
)  # fmt: skip


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def tenant(tid: str, *keys_: tuple[str, list[str]], **extra: Any) -> TenantConfig:
    entries = [{"fingerprint": fp(k), "scopes": scopes} for k, scopes in keys_]
    return TenantConfig.model_validate({"id": tid, "keys": entries, **extra})


TEAM_A = tenant("team-a", (TEAM_KEY, ["submit", "read"]), (WRITER_KEY, ["read", "admin"]))
ALL_KEYS = [API_KEY, TEAM_KEY, WRITER_KEY, OTHER_KEY]


def echo_app(**auth_kwargs: Any) -> FastAPI:
    """A route behind ``api_key_auth`` that reports what the dependency stored."""
    app = FastAPI()

    @app.get("/who", dependencies=[Depends(api_key_auth(**auth_kwargs))])
    async def who(request: Request) -> dict[str, Any]:
        principal: Principal = request.state.principal
        settings = request.state.client_settings
        return {
            "tenant": principal.tenant,
            "scopes": sorted(s.value for s in principal.scopes),
            "operator": principal.operator,
            "fingerprint": principal.fingerprint,
            "settings_fingerprint": settings.fingerprint,
            "rate": settings.rate,
            "audit_fingerprint": audit_context(request).get("key_fingerprint"),
        }

    return app


async def get(app: FastAPI, path: str, key: str | None) -> httpx.Response:
    headers = {} if key is None else {API_KEY_HEADER: key}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        return await c.get(path, headers=headers)


# =========================================================================== api_key_auth: the Principal


async def test_a_key_no_tenant_claims_is_a_default_tenant_key_with_submit_and_read() -> None:
    app = echo_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY], tenants=[TEAM_A])
    body = (await get(app, "/who", API_KEY)).json()
    assert (body["tenant"], body["scopes"], body["operator"]) == (DEFAULT_TENANT, ["read", "submit"], False)
    assert body["fingerprint"] == body["settings_fingerprint"] == body["audit_fingerprint"] == fp(API_KEY)


async def test_a_tenant_key_gets_its_tenant_and_exactly_the_scopes_listed() -> None:
    app = echo_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY], tenants=[TEAM_A])
    team = (await get(app, "/who", TEAM_KEY)).json()
    writer = (await get(app, "/who", WRITER_KEY)).json()
    assert (team["tenant"], team["scopes"], team["operator"]) == ("team-a", ["read", "submit"], False)
    assert (writer["tenant"], writer["scopes"], writer["operator"]) == ("team-a", ["admin", "read"], False)


async def test_an_admin_key_is_an_operator_in_the_default_tenant_with_every_scope() -> None:
    app = echo_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY], tenants=[TEAM_A])
    body = (await get(app, "/who", ADMIN_KEY)).json()
    assert (body["tenant"], body["scopes"], body["operator"]) == (DEFAULT_TENANT, ["admin", "read", "submit"], True)
    assert body["fingerprint"] == fp(ADMIN_KEY)


async def test_without_tenants_every_api_key_is_the_phase_4_default_key() -> None:
    app = echo_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY])
    for key in ALL_KEYS:
        body = (await get(app, "/who", key)).json()
        assert (body["tenant"], body["scopes"]) == (DEFAULT_TENANT, ["read", "submit"]), key


@pytest.mark.parametrize("key", [None, "", "unknown-key", ALL_KEYS[0] + " ", ALL_KEYS[0].upper()])
async def test_a_missing_or_unknown_key_is_401_and_the_route_never_runs(key: str | None) -> None:
    ran: list[str] = []
    app = FastAPI()

    @app.get("/who", dependencies=[Depends(api_key_auth(ALL_KEYS, [ADMIN_KEY], tenants=[TEAM_A]))])
    async def who() -> dict[str, str]:
        ran.append("handler")
        return {}

    reply = await get(app, "/who", key)
    assert reply.status_code == 401 and reply.json() == {"detail": {"error": "unauthorized"}}
    assert reply.headers["WWW-Authenticate"] == "ApiKey"
    assert ran == []


async def test_the_phase_4_call_shapes_still_work() -> None:
    assert (await get(echo_app(keys=[API_KEY]), "/who", API_KEY)).status_code == 200
    app = FastAPI()

    @app.get("/x", dependencies=[Depends(api_key_auth([API_KEY], [ADMIN_KEY], ClientLimitsConfig()))])
    async def x() -> dict[str, str]:
        return {}

    assert (await get(app, "/x", ADMIN_KEY)).status_code == 200


# =========================================================================== the settings layers


def limits_with(
    default_rate: float, override_for: str | None = None, override_rate: float | None = None
) -> ClientLimitsConfig:
    overrides = [ClientOverride(fingerprint=fp(override_for), rate=override_rate)] if override_for else []
    return ClientLimitsConfig(default=ClientSettingsConfig(rate=default_rate), overrides=overrides)


async def test_the_tenant_settings_layer_sits_between_the_default_and_a_key_override() -> None:
    team = tenant("team-a", (TEAM_KEY, ["submit", "read"]), (WRITER_KEY, ["read"]), settings={"rate": 7})
    app = echo_app(keys=ALL_KEYS, admin_keys=[], tenants=[team], client_limits=limits_with(60, WRITER_KEY, 3))
    rates = {k: (await get(app, "/who", k)).json()["rate"] for k in (API_KEY, TEAM_KEY, WRITER_KEY)}
    assert rates == {API_KEY: 60.0, TEAM_KEY: 7.0, WRITER_KEY: 3.0}  # default, tenant, then the key's own override


# =========================================================================== an injected directory


class FixedDirectory:
    """A KeyDirectory that is not the static one: Req 12.3 says a route handler cannot tell."""

    def __init__(self, principal: Principal | None) -> None:
        self.principal = principal
        self.asked: list[str | None] = []

    def resolve(self, key: str | None) -> Principal | None:
        self.asked.append(key)
        return self.principal


async def test_an_injected_directory_decides_who_a_key_is() -> None:
    chosen = Principal("team-z", frozenset({Scope.READ}), False, "deadbeef")
    directory = FixedDirectory(chosen)
    app = echo_app(keys=[], admin_keys=[], directory=directory)
    body = (await get(app, "/who", "any-key-at-all")).json()
    assert (body["tenant"], body["scopes"], body["fingerprint"]) == ("team-z", ["read"], "deadbeef")
    assert directory.asked == ["any-key-at-all"]


async def test_an_injected_directory_that_knows_nobody_means_401() -> None:
    assert (await get(echo_app(keys=[], admin_keys=[], directory=FixedDirectory(None)), "/who", "k")).status_code == 401


# =========================================================================== admin_key_auth


def admin_app(**kwargs: Any) -> FastAPI:
    """The real admin router with one stub route listed in the table: ``admin_key_auth`` authenticates, then
    ``authorize_route`` decides (Phase 5 Req 2.3). ``admin_key_auth`` alone no longer refuses a known key."""
    from starlette.exceptions import HTTPException as StarletteHTTPException

    from das.gateway.admin import create_admin_router, install_admin

    app = FastAPI()

    @app.exception_handler(StarletteHTTPException)
    async def flat(_: Request, exc: StarletteHTTPException) -> Any:
        from fastapi.responses import JSONResponse

        body = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=body)

    router = create_admin_router(
        kwargs["keys"], kwargs["admin_keys"], tenants=kwargs.get("tenants", ())
    )

    @router.get("/dlq")  # GET /v1/admin/dlq: needs Scope admin
    async def dlq(request: Request) -> dict[str, Any]:
        return {"operator": request.state.principal.operator, "tenant": request.state.principal.tenant}

    install_admin(app, router)
    return app


async def post_admin(app: FastAPI, key: str | None) -> httpx.Response:
    headers = {} if key is None else {API_KEY_HEADER: key}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        return await c.get("/v1/admin/dlq", headers=headers)


async def test_an_admin_key_reaches_the_admin_router_as_an_operator() -> None:
    reply = await post_admin(admin_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY], tenants=[TEAM_A]), ADMIN_KEY)
    assert reply.status_code == 200 and reply.json() == {"operator": True, "tenant": "default"}


async def test_a_tenant_key_with_the_admin_scope_reaches_the_admin_router() -> None:
    reply = await post_admin(admin_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY], tenants=[TEAM_A]), WRITER_KEY)
    assert reply.status_code == 200 and reply.json() == {"operator": False, "tenant": "team-a"}


async def test_an_api_key_without_the_admin_scope_is_forbidden_when_an_admin_exists() -> None:
    reply = await post_admin(admin_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY]), API_KEY)
    assert reply.status_code == 403 and reply.json() == {"error": "forbidden", "scope": "admin"}


async def test_without_any_admin_capability_the_answer_stays_admin_disabled() -> None:
    read_only = tenant("t", (TEAM_KEY, ["read"]))
    reply = await post_admin(admin_app(keys=ALL_KEYS, admin_keys=[], tenants=[read_only]), TEAM_KEY)
    assert reply.status_code == 403 and reply.json() == {"error": "admin_disabled"}


async def test_a_tenant_that_can_admin_makes_the_answer_forbidden_not_disabled() -> None:
    reply = await post_admin(admin_app(keys=ALL_KEYS, admin_keys=[], tenants=[TEAM_A]), API_KEY)
    assert reply.status_code == 403 and reply.json() == {"error": "forbidden", "scope": "admin"}


async def test_a_missing_key_on_the_admin_router_is_401() -> None:
    assert (await post_admin(admin_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY]), None)).status_code == 401


async def test_admin_key_auth_alone_authenticates_and_decides_nothing_else() -> None:
    """It no longer refuses a known key itself: that is the route table's job, one step later."""
    app = FastAPI()
    app.get("/x", dependencies=[Depends(admin_key_auth(ALL_KEYS, [ADMIN_KEY]))])(lambda: {})
    assert (await post_admin_at(app, "/x", API_KEY)).status_code == 200
    assert (await post_admin_at(app, "/x", "unknown")).status_code == 401


async def post_admin_at(app: FastAPI, path: str, key: str | None) -> httpx.Response:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        return await c.get(path, headers={} if key is None else {API_KEY_HEADER: key})


async def test_the_principal_is_known_even_when_the_admin_router_refuses_the_call() -> None:
    seen: dict[str, Any] = {}
    app = admin_app(keys=ALL_KEYS, admin_keys=[ADMIN_KEY])

    @app.middleware("http")
    async def spy(request: Request, call_next: Any) -> Any:
        response = await call_next(request)
        seen["fingerprint"] = audit_context(request).get("key_fingerprint")
        return response

    assert (await post_admin(app, API_KEY)).status_code == 403
    assert seen["fingerprint"] == fp(API_KEY)  # the Audit_Entry still names the key


# =========================================================================== the whole app


@pytest.fixture
async def gateway(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    team = tenant("team-a", (TEAM_KEY, ["submit", "read"]), settings={"rate": 1, "burst": 1})
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, ALL_KEYS, 10,
        admin_api_keys=[ADMIN_KEY], tenants=[team],
    )  # fmt: skip
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        yield c


async def test_create_app_applies_the_tenant_settings_to_the_rate_limit(gateway: httpx.AsyncClient) -> None:
    team = {API_KEY_HEADER: TEAM_KEY}
    assert (await gateway.get("/v1/agents", headers=team)).status_code == 200
    assert (await gateway.get("/v1/agents", headers=team)).status_code == 429  # the tenant's burst of 1
    other = {API_KEY_HEADER: API_KEY}
    for _ in range(3):
        assert (await gateway.get("/v1/agents", headers=other)).status_code == 200  # the default settings


async def test_create_app_accepts_every_key_kind_on_v1_and_refuses_unknown_ones(gateway: httpx.AsyncClient) -> None:
    for key in (API_KEY, TEAM_KEY, ADMIN_KEY):
        assert (await gateway.get("/v1/agents", headers={API_KEY_HEADER: key})).status_code == 200, key
    assert (await gateway.get("/v1/agents", headers={API_KEY_HEADER: "nope"})).status_code == 401
    assert (await gateway.get("/v1/agents")).status_code == 401
