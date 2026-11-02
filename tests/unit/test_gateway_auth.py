"""Unit tests for das.gateway.auth (Requirements 1.3, 11.8)."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import Depends, FastAPI
from pydantic import SecretStr

from das.gateway.auth import API_KEY_HEADER, api_key_auth, is_valid_key

KEYS = [SecretStr("key-alpha"), SecretStr("key-beta")]


def _app(keys: list[SecretStr], calls: list[str]) -> FastAPI:
    app = FastAPI()
    auth = api_key_auth(keys)

    @app.post("/protected", dependencies=[Depends(auth)])
    async def protected() -> dict[str, str]:
        calls.append("handler")  # stands in for State_Store access
        return {"ok": "yes"}

    return app


@pytest.fixture
def calls() -> list[str]:
    return []


@pytest.fixture
async def client(calls: list[str]) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=_app(KEYS, calls))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.parametrize("key", ["key-alpha", "key-beta"])
async def test_any_configured_key_is_accepted(
    client: httpx.AsyncClient, calls: list[str], key: str
) -> None:
    resp = await client.post("/protected", headers={API_KEY_HEADER: key})
    assert resp.status_code == 200
    assert calls == ["handler"]


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {API_KEY_HEADER: ""},
        {API_KEY_HEADER: "wrong"},
        {API_KEY_HEADER: "key-alpha "},
        {API_KEY_HEADER: "key-alph"},
        {API_KEY_HEADER: "KEY-ALPHA"},
        {"Authorization": "key-alpha"},
    ],
)
async def test_missing_or_invalid_key_is_401_before_handler(
    client: httpx.AsyncClient, calls: list[str], headers: dict[str, str]
) -> None:
    resp = await client.post("/protected", headers=headers)
    assert resp.status_code == 401
    assert calls == []
    assert "key-alpha" not in resp.text and "key-beta" not in resp.text


async def test_header_name_is_case_insensitive(
    client: httpx.AsyncClient, calls: list[str]
) -> None:
    resp = await client.post("/protected", headers={"x-api-key": "key-beta"})
    assert resp.status_code == 200


async def test_no_configured_keys_rejects_everything() -> None:
    calls: list[str] = []
    app = _app([SecretStr("")], calls)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        for headers in ({}, {API_KEY_HEADER: ""}, {API_KEY_HEADER: "anything"}):
            assert (await c.post("/protected", headers=headers)).status_code == 401
    assert calls == []


async def test_key_values_never_logged(
    client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG):
        await client.post("/protected", headers={API_KEY_HEADER: "key-alpha"})
        await client.post("/protected", headers={API_KEY_HEADER: "bad-secret"})
    for secret in ("key-alpha", "key-beta", "bad-secret"):
        assert secret not in caplog.text


def test_is_valid_key_accepts_plain_strings_and_non_ascii() -> None:
    keys = [b"plain", "clé-ü".encode()]
    assert is_valid_key("plain", keys)
    assert is_valid_key("clé-ü", keys)
    assert not is_valid_key("cle-u", keys)
    assert not is_valid_key(None, keys)
    assert not is_valid_key("plain", [])
