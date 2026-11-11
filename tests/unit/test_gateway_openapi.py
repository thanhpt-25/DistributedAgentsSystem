"""``GET /v1/openapi.json``: the published schema, behind API_Key authentication
(das-client-api task 7.8; Phase 4 Req 11.4)."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from das.config.schema import ClientLimitsConfig, ClientSettingsConfig
from das.gateway.app import create_app
from das.registry import AgentRegistry

KEY = "client-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
AUTH = {"X-API-Key": KEY}
CLIENT_PATHS = {
    "/v1/requests",
    "/v1/requests/{request_id}",
    "/v1/requests/{request_id}/cancel",
    "/v1/requests/{request_id}/events",
    "/v1/agents",
}


def make_app(state_store, fake_redis, rate: float = 0.0):
    limits = ClientLimitsConfig(default=ClientSettingsConfig(rate=rate, burst=1))
    return create_app(
        state_store, AgentRegistry(fake_redis), None, [KEY], 50,
        admin_api_keys=[ADMIN_KEY], client_limits=limits,
    )


@pytest.fixture
async def schema(state_store, fake_redis) -> dict[str, Any]:
    app = make_app(state_store, fake_redis)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        r = await c.get("/v1/openapi.json", headers=AUTH)
    assert r.status_code == 200
    return r.json()


async def get(app, path: str, **headers: str) -> httpx.Response:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        return await c.get(path, headers=headers)


# --------------------------------------------------------------------------- access


async def test_the_schema_requires_an_api_key(state_store, fake_redis):
    app = make_app(state_store, fake_redis)
    r = await get(app, "/v1/openapi.json")
    assert (r.status_code, r.json()) == (401, {"error": "unauthorized"})
    r = await get(app, "/v1/openapi.json", **{"X-API-Key": "wrong-key-000000"})
    assert r.status_code == 401


async def test_an_admin_key_may_read_it_too(state_store, fake_redis):
    r = await get(make_app(state_store, fake_redis), "/v1/openapi.json", **{"X-API-Key": ADMIN_KEY})
    assert r.status_code == 200


async def test_the_rate_limit_applies(state_store, fake_redis, fake_redis_time):
    app = make_app(state_store, fake_redis, rate=1.0)
    assert (await get(app, "/v1/openapi.json", **AUTH)).status_code == 200
    assert (await get(app, "/v1/openapi.json", **AUTH)).status_code == 429


@pytest.mark.parametrize("path", ["/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect"])
async def test_no_unauthenticated_schema_or_docs_route_exists(state_store, fake_redis, path):
    r = await get(make_app(state_store, fake_redis), path, **AUTH)
    assert r.status_code == 404


# --------------------------------------------------------------------------- content


async def test_the_document_is_openapi_3_and_covers_the_client_routes(schema):
    assert schema["openapi"].startswith("3.")
    assert schema["info"]["title"] == "das-gateway"
    assert CLIENT_PATHS <= set(schema["paths"])


async def test_the_admin_api_and_the_schema_route_itself_are_not_published(schema):
    assert not any(p.startswith("/v1/admin") for p in schema["paths"])
    assert "/v1/openapi.json" not in schema["paths"]


async def test_submit_documents_every_status_it_can_return(schema):
    responses = schema["paths"]["/v1/requests"]["post"]["responses"]
    assert {"200", "202", "400", "401", "422", "429", "503"} <= set(responses)
    assert "idempotency_key_reused" in responses["422"]["description"]
    assert "quota_exceeded" in responses["429"]["description"]


async def test_the_events_route_documents_the_stream_and_its_refusals(schema):
    responses = schema["paths"]["/v1/requests/{request_id}/events"]["get"]["responses"]
    assert {"200", "401", "404", "429", "503"} <= set(responses)
    assert "text/event-stream" in responses["200"]["content"]
    assert "too_many_streams" in responses["429"]["description"]
    assert "too_many_streams" in responses["503"]["description"]


async def test_cancel_and_query_document_their_errors(schema):
    cancel = schema["paths"]["/v1/requests/{request_id}/cancel"]["post"]["responses"]
    assert {"202", "404", "409", "429", "503"} <= set(cancel)
    query = schema["paths"]["/v1/requests/{request_id}"]["get"]["responses"]
    assert {"200", "404", "429", "503"} <= set(query)


async def test_every_client_route_documents_auth_rate_limit_and_unavailability(schema):
    for path in CLIENT_PATHS:
        for operation in schema["paths"][path].values():
            assert {"401", "429", "503"} <= set(operation["responses"]), path


async def test_the_default_422_is_replaced_by_the_400_the_gateway_really_returns(schema):
    """Starlette's validation error becomes 400 ``invalid_body`` here, never 422 (v1 Req 1.4)."""
    text = json.dumps(schema)
    assert "HTTPValidationError" not in text
    assert "ValidationError" not in schema["components"].get("schemas", {})
    query = schema["paths"]["/v1/requests/{request_id}"]["get"]["responses"]
    assert "422" not in query and "400" in query


async def test_error_bodies_share_one_documented_shape(schema):
    error = schema["components"]["schemas"]["ErrorResponse"]
    assert error["required"] == ["error"]
    assert {"error", "parameter", "limit", "current_state"} <= set(error["properties"])
    body = schema["paths"]["/v1/requests"]["post"]["responses"]["429"]["content"]["application/json"]["schema"]
    assert body == {"$ref": "#/components/schemas/ErrorResponse"}


async def test_the_document_is_stable_across_calls(state_store, fake_redis):
    app = make_app(state_store, fake_redis)
    first = (await get(app, "/v1/openapi.json", **AUTH)).json()
    second = (await get(app, "/v1/openapi.json", **AUTH)).json()
    assert first == second
