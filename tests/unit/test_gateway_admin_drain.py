"""Unit tests for the Admin_API drain/undrain endpoints (task 16.3).

Requirements 17.1, 17.8, 17.10, 17.11, 17.12, 17.15.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, ClassVar

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st

from das.codec import AgentControlReplyPayload, TypedMessage, new_message
from das.gateway import admin
from das.gateway.admin import InvalidRequest, parse_drain_body
from das.gateway.app import create_app
from das.registry import AgentRegistry
from das.registry.registry import AgentInfo, agent_key
from das.state import StateStore
from das.transport.base import RequestTimeout, TransportUnavailable

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
ADMIN = {"X-API-Key": ADMIN_KEY}
GATEWAY_ID = "gw-1"


class FakeControl:
    """Control Transport fake: records request_reply calls and answers per ``mode``."""

    name: ClassVar[str] = "fake"

    def __init__(self, mode: str = "reply", status: str = "Draining") -> None:
        self.mode = mode
        self.status = status
        self.calls: list[tuple[str, TypedMessage, float | None]] = []

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def publish(self, destination: str, message: TypedMessage,
                      headers: Mapping[str, str] | None = None) -> None: ...
    async def subscribe(self, *a: Any, **kw: Any) -> Any: ...
    async def acknowledge(self, *a: Any, **kw: Any) -> None: ...

    async def request_reply(
        self, destination: str, message: TypedMessage, timeout: float | None = None
    ) -> TypedMessage:
        self.calls.append((destination, message, timeout))
        if self.mode == "timeout":
            raise RequestTimeout(destination, timeout or 0)
        if self.mode == "unreachable":
            raise TransportUnavailable("grpc", "unreachable")
        if self.mode == "hang":
            await asyncio.sleep(3600)
        return new_message(
            AgentControlReplyPayload(agent_id=destination, status=self.status, in_flight=1)
        )


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


async def _setup(store: StateStore, control: FakeControl | None) -> tuple[AgentRegistry, Any]:
    registry = AgentRegistry(store.client)
    await registry.register(AgentInfo(agent_id="w1", role="worker", host="h"))
    await registry.register(AgentInfo(agent_id="m1", role="manager", host="h"))
    await registry.register(AgentInfo(agent_id="w-down", role="worker", host="h"))
    await registry.register(AgentInfo(agent_id="gw-2", role="gateway", host="h"))
    await store.client.hset(agent_key("w-down"), "status", "Unavailable")
    app = create_app(
        store, registry, control, [API_KEY], 10,
        admin_api_keys=[ADMIN_KEY], agent_id=GATEWAY_ID,
    )
    return registry, app


async def _snapshot_status(registry: AgentRegistry, aid: str) -> str | None:
    rec = (await registry.snapshot()).get(aid)
    return None if rec is None else rec.status


# --------------------------------------------------------------------------- body parsing


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(b"", False), (b"{}", False), (b'{"release_leases": true}', True),
     (b'{"release_leases": false}', False)],
)
def test_parse_drain_body_accepts(raw: bytes, expected: bool) -> None:
    assert parse_drain_body(raw) is expected


@given(
    st.one_of(
        st.none(), st.integers(), st.floats(allow_nan=False), st.text(),
        st.lists(st.booleans(), max_size=2), st.dictionaries(st.text(max_size=3), st.booleans(), max_size=2),
    )
)
def test_parse_drain_body_rejects_non_boolean_release_leases(value: Any) -> None:
    """**Validates: Requirements 17.15**"""
    import json

    with pytest.raises(InvalidRequest) as info:
        parse_drain_body(json.dumps({"release_leases": value}).encode())
    assert info.value.parameter == "release_leases"


# --------------------------------------------------------------------------- endpoints


async def test_drain_sends_command_and_returns_202(state_store: StateStore) -> None:
    control = FakeControl(status="Draining")
    registry, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post("/v1/admin/agents/m1/drain", headers=ADMIN, json={"release_leases": True})
    assert r.status_code == 202
    assert r.json() == {"agent_id": "m1", "status": "Draining"}
    [(dest, msg, timeout)] = control.calls
    assert dest == "m1" and msg.type == "agent_control"
    assert (msg.payload.command, msg.payload.agent_id, msg.payload.release_leases) == (
        "drain", "m1", True,
    )
    assert timeout is not None and 0 < timeout <= 5
    # The Gateway itself never writes the registry.
    assert await _snapshot_status(registry, "m1") == "Available"


async def test_drain_without_body_defaults_release_leases_false(state_store: StateStore) -> None:
    control = FakeControl()
    _, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post("/v1/admin/agents/w1/drain", headers=ADMIN)
    assert r.status_code == 202
    assert control.calls[0][1].payload.release_leases is False


async def test_undrain_sends_command(state_store: StateStore) -> None:
    control = FakeControl(status="Available")
    _, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post("/v1/admin/agents/w1/undrain", headers=ADMIN)
    assert r.status_code == 202
    assert r.json() == {"agent_id": "w1", "status": "Available"}
    assert control.calls[0][1].payload.command == "undrain"


@pytest.mark.parametrize("body", ['{"release_leases": "yes"}', '{"release_leases": null}',
                                  '{"release_leases": 1}'])
async def test_drain_invalid_release_leases_is_400(state_store: StateStore, body: str) -> None:
    control = FakeControl()
    _, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post("/v1/admin/agents/w1/drain", headers=ADMIN, content=body)
    assert r.status_code == 400
    assert r.json() == {"error": "invalid_request", "parameter": "release_leases"}
    assert control.calls == []


@pytest.mark.parametrize("command", ["drain", "undrain"])
async def test_unknown_agent_is_404(state_store: StateStore, command: str) -> None:
    control = FakeControl()
    _, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post(f"/v1/admin/agents/nobody/{command}", headers=ADMIN)
    assert r.status_code == 404 and r.json() == {"error": "not_found"}
    assert control.calls == []


@pytest.mark.parametrize(
    ("aid", "current"), [("w-down", "Unavailable"), ("gw-2", "Available"), (GATEWAY_ID, "gateway")]
)
@pytest.mark.parametrize("command", ["drain", "undrain"])
async def test_agent_state_conflicts_are_409(
    state_store: StateStore, aid: str, current: str, command: str
) -> None:
    control = FakeControl()
    _, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post(f"/v1/admin/agents/{aid}/{command}", headers=ADMIN)
    assert r.status_code == 409
    assert r.json() == {"error": "agent_state", "status": current}
    assert control.calls == []


@pytest.mark.parametrize("mode", ["timeout", "unreachable", "hang"])
async def test_no_confirmation_is_504_without_resend(
    state_store: StateStore, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.setattr(admin, "AGENT_CONTROL_TIMEOUT", 0.3)
    control = FakeControl(mode=mode)
    registry, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post("/v1/admin/agents/w1/drain", headers=ADMIN)
    assert r.status_code == 504 and r.json() == {"error": "agent_timeout"}
    assert len(control.calls) == 1
    assert await _snapshot_status(registry, "w1") == "Available"


async def test_no_rpc_transport_is_503(state_store: StateStore) -> None:
    _, app = await _setup(state_store, None)
    async with _client(app) as c:
        r = await c.post("/v1/admin/agents/w1/drain", headers=ADMIN)
    assert r.status_code == 503 and r.json() == {"error": "service_unavailable"}


async def test_drain_requires_admin_key(state_store: StateStore) -> None:
    control = FakeControl()
    _, app = await _setup(state_store, control)
    async with _client(app) as c:
        r = await c.post("/v1/admin/agents/w1/drain", headers={"X-API-Key": API_KEY})
    assert r.status_code == 403
    assert control.calls == []
