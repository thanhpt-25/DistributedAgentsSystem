"""Unit tests for Admin_API authorization and audit (Requirements 11.2-11.8)."""

from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from das.gateway.admin import create_admin_router, install_admin
from das.gateway.app import create_app
from das.gateway.audit import AuditMiddleware
from das.observability.logging import configure_logging
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
FIXED = datetime(2025, 1, 2, 3, 4, 5, 678_000, tzinfo=UTC)


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    out = io.StringIO()
    configure_logging("gw-test", secrets=[API_KEY, ADMIN_KEY], stream=out)
    yield out
    configure_logging("test", stream=io.StringIO())


def audits(stream: io.StringIO) -> list[dict[str, Any]]:
    lines = [json.loads(line) for line in stream.getvalue().splitlines() if line]
    return [e for e in lines if e.get("message") == "audit"]


def admin_app(admin_keys: list[str], calls: list[str]) -> FastAPI:
    app = FastAPI()

    @app.exception_handler(StarletteHTTPException)
    async def _flat(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        body = exc.detail if isinstance(exc.detail, Mapping) else {"error": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=body, headers=exc.headers)

    router = create_admin_router([API_KEY], admin_keys)

    @router.get("/dlq")
    async def list_dlq() -> dict[str, Any]:
        calls.append("list")  # stands in for State_Store access
        return {"items": []}

    @router.post("/dlq/{dead_letter_id}/discard")
    async def discard(dead_letter_id: str) -> JSONResponse:
        calls.append(f"discard:{dead_letter_id}")
        if dead_letter_id == "missing":
            return JSONResponse(status_code=404, content={"error": "not_found"})
        return JSONResponse(status_code=200, content={"status": "Discarded"})

    install_admin(app, router, now=lambda: FIXED)
    return app


@pytest.fixture
def calls() -> list[str]:
    return []


async def _client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        yield c


@pytest.fixture
async def client(calls: list[str]) -> AsyncIterator[httpx.AsyncClient]:
    async for c in _client(admin_app([ADMIN_KEY], calls)):
        yield c


@pytest.mark.parametrize(
    "headers",
    [{}, {"X-API-Key": ""}, {"X-API-Key": "nope"}, {"X-API-Key": ADMIN_KEY[:-1]}],
)
async def test_unknown_key_is_401_without_handler(
    client: httpx.AsyncClient, calls: list[str], stream: io.StringIO, headers: dict[str, str]
) -> None:
    r = await client.post("/v1/admin/dlq/d1/discard", headers=headers)
    assert r.status_code == 401
    assert r.json() == {"error": "unauthorized"}
    assert calls == []
    [entry] = audits(stream)
    assert entry["http_status"] == 401
    assert entry["error_code"] == "unauthorized"
    assert entry["key_fingerprint"] is None
    assert entry["level"] == "info"


async def test_api_key_is_403_forbidden(
    client: httpx.AsyncClient, calls: list[str], stream: io.StringIO
) -> None:
    r = await client.get("/v1/admin/dlq", headers={"x-api-key": API_KEY})
    assert (r.status_code, r.json()) == (403, {"error": "forbidden"})
    assert calls == []
    [entry] = audits(stream)
    assert entry["key_fingerprint"] == key_fingerprint(API_KEY.encode())
    assert entry["error_code"] == "forbidden"


async def test_api_key_without_admin_keys_is_admin_disabled(
    calls: list[str], stream: io.StringIO
) -> None:
    async for c in _client(admin_app([], calls)):
        r = await c.get("/v1/admin/dlq", headers={"X-API-Key": API_KEY})
        assert (r.status_code, r.json()) == (403, {"error": "admin_disabled"})
        r = await c.get("/v1/admin/dlq", headers={"X-API-Key": ADMIN_KEY})
        assert (r.status_code, r.json()) == (401, {"error": "unauthorized"})
    assert calls == []
    assert [e["error_code"] for e in audits(stream)] == ["admin_disabled", "unauthorized"]


async def test_admin_key_reaches_handler_and_audits(
    client: httpx.AsyncClient, calls: list[str], stream: io.StringIO
) -> None:
    r = await client.post("/v1/admin/dlq/d1/discard", headers={"X-API-KEY": ADMIN_KEY})
    assert r.status_code == 200
    r = await client.post("/v1/admin/dlq/missing/discard", headers={"X-API-Key": ADMIN_KEY})
    assert (r.status_code, r.json()) == (404, {"error": "not_found"})
    r = await client.get("/v1/admin/dlq", headers={"X-API-Key": ADMIN_KEY})
    assert r.status_code == 200
    assert calls == ["discard:d1", "discard:missing", "list"]

    first, second, third = audits(stream)
    fp = key_fingerprint(ADMIN_KEY.encode())
    assert first == first | {
        "at": "2025-01-02T03:04:05.678Z",
        "action": "POST /v1/admin/dlq/{dead_letter_id}/discard",
        "target": "d1",
        "http_status": 200,
        "error_code": None,
        "key_fingerprint": fp,
    }
    assert (second["target"], second["http_status"], second["error_code"]) == (
        "missing",
        404,
        "not_found",
    )
    assert (third["action"], third["target"]) == ("GET /v1/admin/dlq", None)
    for value in (API_KEY, ADMIN_KEY):
        assert value[:8] not in stream.getvalue()


async def test_audit_is_emitted_before_response_start(stream: io.StringIO) -> None:
    async def app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 403, "headers": []})
        await send({"type": "http.response.body", "body": b'{"error":"forbidden"}'})

    seen: list[int] = []

    async def send(message: Any) -> None:
        if message["type"] == "http.response.start":
            seen.append(len(audits(stream)))

    async def receive() -> Any:
        return {"type": "http.request", "body": b""}

    scope = {"type": "http", "method": "GET", "path": "/v1/admin/x", "headers": []}
    await AuditMiddleware(app)(scope, receive, send)
    assert seen == [1]
    [entry] = audits(stream)
    assert entry["action"] == "GET _unmatched"
    assert entry["error_code"] == "forbidden"


async def test_non_admin_paths_are_not_audited(stream: io.StringIO) -> None:
    async def app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    async def noop(_: Any) -> None:
        return None

    async def receive() -> Any:
        return {"type": "http.request", "body": b""}

    for path in ("/v1/requests", "/v1/administrator", "/healthz"):
        scope = {"type": "http", "method": "GET", "path": path, "headers": []}
        await AuditMiddleware(app)(scope, receive, noop)
    assert audits(stream) == []


async def test_v1_accepts_admin_key_with_identical_response(
    state_store: StateStore, fake_redis: Any
) -> None:
    app = create_app(
        state_store,
        AgentRegistry(fake_redis),
        None,
        [API_KEY],
        max_prompt_length=10,
        admin_api_keys=[ADMIN_KEY],
    )
    async for c in _client(app):
        as_api = await c.get("/v1/requests/nope", headers={"X-API-Key": API_KEY})
        as_admin = await c.get("/v1/requests/nope", headers={"X-API-Key": ADMIN_KEY})
        assert (as_api.status_code, as_api.json()) == (as_admin.status_code, as_admin.json())
        assert as_api.status_code == 404
        a1 = await c.get("/v1/agents", headers={"X-API-Key": API_KEY})
        a2 = await c.get("/v1/agents", headers={"X-API-Key": ADMIN_KEY})
        assert (a1.status_code, a1.json()) == (a2.status_code, a2.json()) == (200, [])
        r = await c.get("/v1/agents", headers={"X-API-Key": "nope"})
        assert (r.status_code, r.json()) == (401, {"error": "unauthorized"})
