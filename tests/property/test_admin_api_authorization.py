# Feature: das-observability-operations, Property 17: Phân quyền Admin_API và Audit_Entry
"""Property 17: Admin_API authorization and Audit_Entry.

For any set of API_Keys and Admin_API_Keys (including an empty Admin_API_Key set), any
``X-API-Key`` header (absent, empty, matching an API_Key, matching an Admin_API_Key,
arbitrary string) and any admin endpoint:

- HTTP status and error code follow the table: ``NONE`` → 401 ``unauthorized``;
  ``API`` with Admin_API_Keys → 403 ``forbidden``; ``API`` without → 403
  ``admin_disabled``; ``ADMIN`` → the handler.
- Every rejected call leaves the fakeredis contents unchanged and publishes no command.
- Every call emits exactly one Audit_Entry whose ``key_fingerprint`` equals
  ``sha256(key)[:8]`` when the key matched and null otherwise.
- ``classify_key`` calls ``compare_digest`` exactly ``len(api) + len(admin)`` times.

The app is the real :func:`das.gateway.app.create_app` on a fresh fakeredis per example.

**Validates: Requirements 11.2, 11.3, 11.4, 11.5, 11.7, 11.8**
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
from dataclasses import dataclass
from typing import Any
from unittest import mock

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given
from hypothesis import strategies as st

from das.gateway.app import create_app
from das.observability.logging import configure_logging
from das.ops.keys import classify_key
from das.registry import AgentRegistry
from das.state import StateStore
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

# Header-safe printable ASCII without whitespace (h11 strips surrounding whitespace).
_key_chars = st.characters(min_codepoint=0x21, max_codepoint=0x7E)
# Configured keys are secrets for the redactor (Req 11.9), which masks any 8-char
# substring of a key in log output. Draw them from characters that never appear in
# 8-char runs of Audit_Entry text (lowercase routes/fields, hex, digit timestamps), so
# redaction cannot coincidentally rewrite the fields under test.
_configured_key = st.text(
    st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZ!#$*+~^|"), min_size=16, max_size=32
)
_dlid = st.text(
    alphabet=st.characters(categories=("Ll", "Nd"), max_codepoint=0x7F, include_characters="-"),
    min_size=1,
    max_size=30,
)

# (method, path template, route template)
ENDPOINTS = (
    ("GET", "/v1/admin/dlq", "/v1/admin/dlq"),
    ("GET", "/v1/admin/dlq/{id}", "/v1/admin/dlq/{dead_letter_id}"),
    ("POST", "/v1/admin/dlq/{id}/replay", "/v1/admin/dlq/{dead_letter_id}/replay"),
    ("POST", "/v1/admin/dlq/{id}/discard", "/v1/admin/dlq/{dead_letter_id}/discard"),
)


@dataclass(frozen=True)
class Call:
    endpoint: tuple[str, str, str]
    target: str
    header: tuple[str, int | str]  # ("absent"|"empty"|"api"|"admin"|"strange", index/value)
    header_name: str


@dataclass(frozen=True)
class Scenario:
    api: tuple[str, ...]
    admin: tuple[str, ...]
    seeds: int
    calls: tuple[Call, ...]


_header_names = st.sampled_from(["X-API-Key", "x-api-key", "X-API-KEY", "x-Api-kEy"])


@st.composite
def scenarios(draw: st.DrawFn) -> Scenario:
    keys = draw(st.lists(_configured_key, unique=True, min_size=1, max_size=8))
    n_api = draw(st.integers(min_value=1, max_value=len(keys)))
    api, admin = tuple(keys[:n_api]), tuple(keys[n_api:])
    header = st.one_of(
        st.just(("absent", 0)),
        st.just(("empty", 0)),
        st.integers(0, len(api) - 1).map(lambda i: ("api", i)),
        st.text(_key_chars, min_size=1, max_size=40)
        .filter(lambda k: k not in keys)
        .map(lambda k: ("strange", k)),
    )
    if admin:
        header = st.one_of(header, st.integers(0, len(admin) - 1).map(lambda i: ("admin", i)))
    calls = draw(
        st.lists(
            st.builds(Call, st.sampled_from(ENDPOINTS), _dlid, header, _header_names),
            min_size=1,
            max_size=6,
        )
    )
    return Scenario(api, admin, draw(st.integers(0, 3)), tuple(calls))


async def _dump(r: FakeRedis) -> dict[str, tuple[str, Any, bool]]:
    out: dict[str, tuple[str, Any, bool]] = {}
    for key in sorted(await r.keys("*")):
        kind = await r.type(key)
        if kind == "string":
            val: Any = await r.get(key)
        elif kind == "hash":
            val = await r.hgetall(key)
        elif kind == "list":
            val = await r.lrange(key, 0, -1)
        elif kind == "set":
            val = sorted(await r.smembers(key))
        elif kind == "zset":
            val = await r.zrange(key, 0, -1, withscores=True)
        elif kind == "stream":
            val = await r.xrange(key)
        else:  # pragma: no cover
            val = None
        out[key] = (kind, val, (await r.pttl(key)) > 0)
    return out


def _audits(out: io.StringIO) -> list[dict[str, Any]]:
    lines = [json.loads(line) for line in out.getvalue().splitlines() if line]
    return [e for e in lines if e.get("message") == "audit"]


def _expected(sc: Scenario, call: Call) -> tuple[str | None, int | None, str | None]:
    """(presented key, expected auth status or None for handler, expected auth error)."""
    kind, val = call.header
    if kind == "absent":
        return None, 401, "unauthorized"
    if kind == "empty":
        return "", 401, "unauthorized"
    if kind == "strange":
        assert isinstance(val, str)
        return val, 401, "unauthorized"
    assert isinstance(val, int)
    if kind == "api":
        return sc.api[val], 403, ("forbidden" if sc.admin else "admin_disabled")
    return sc.admin[val], None, None


async def _run(sc: Scenario) -> None:
    out = io.StringIO()
    configure_logging("gw-prop17", secrets=[*sc.api, *sc.admin], stream=out)
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    broker = MemoryBroker()
    control = MemoryTransport(broker, agent_id="gateway")
    await control.start()
    try:
        store = StateStore(redis)
        for i in range(sc.seeds):
            await store.create_request(f"prompt {i}")
        app = create_app(
            store,
            AgentRegistry(redis),
            control,
            list(sc.api),
            max_prompt_length=50,
            admin_api_keys=list(sc.admin),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            for call in sc.calls:
                method, path_t, route_t = call.endpoint
                key, auth_status, auth_error = _expected(sc, call)
                headers = {} if key is None else {call.header_name: key}
                before = await _dump(redis)
                wire_before = len(broker.wire)
                audits_before = len(_audits(out))

                resp = await client.request(method, path_t.format(id=call.target), headers=headers)

                if auth_status is not None:
                    # Rejected by authorization: fixed body, no State_Store write, no command.
                    assert resp.status_code == auth_status, (call, resp.text)
                    expected_body = {"error": auth_error}
                    if auth_error == "forbidden":  # Phase 5 Req 2.3: the refusal names what the route requires
                        expected_body["scope"] = "admin" if "/dlq" in path_t else "operator"
                    assert resp.json() == expected_body
                    assert await _dump(redis) == before
                    assert len(broker.wire) == wire_before
                else:
                    # Reached the handler: never an authorization rejection.
                    assert resp.status_code not in (401, 403), (call, resp.text)
                    if path_t == "/v1/admin/dlq":
                        assert resp.status_code == 200, resp.text
                    else:  # no Dead_Letter_Record exists for the target
                        assert resp.status_code == 404, resp.text
                        assert resp.json().get("error") == "not_found"

                new = _audits(out)[audits_before:]
                assert len(new) == 1, (call, new)
                [entry] = new
                assert entry["level"] == "info"
                assert entry["action"] == f"{method} {route_t}"
                assert entry["target"] == (None if path_t == "/v1/admin/dlq" else call.target)
                assert entry["http_status"] == resp.status_code
                body = resp.json()
                expected_code = body.get("error") if isinstance(body, dict) else None
                assert entry["error_code"] == (
                    expected_code if isinstance(expected_code, str) else None
                )
                matched = call.header[0] in ("api", "admin")
                expected_fp = hashlib.sha256(key.encode()).hexdigest()[:8] if matched and key else None
                assert entry["key_fingerprint"] == expected_fp
    finally:
        await control.close()
        await redis.aclose()
        configure_logging("test", stream=io.StringIO())


@given(scenarios())
def test_admin_api_authorization_and_audit(sc: Scenario) -> None:
    asyncio.run(_run(sc))


@given(
    keys=st.lists(st.binary(min_size=1, max_size=24), unique=True, max_size=10),
    split=st.integers(min_value=0, max_value=10),
    candidate=st.one_of(st.none(), st.just(""), st.text(_key_chars, max_size=24)),
    pick=st.integers(min_value=0, max_value=20),
)
def test_classify_key_compares_every_key(
    keys: list[bytes], split: int, candidate: str | None, pick: int
) -> None:
    api, admin = keys[: min(split, len(keys))], keys[min(split, len(keys)) :]
    # Sometimes present one of the configured keys (when it is valid UTF-8 text).
    if keys and pick < len(keys):
        try:
            candidate = keys[pick].decode("utf-8")
        except UnicodeDecodeError:
            pass
    real = hmac.compare_digest
    with mock.patch("das.ops.keys.hmac.compare_digest", side_effect=real) as spy:
        classify_key(candidate, api, admin)
    assert spy.call_count == len(api) + len(admin)
