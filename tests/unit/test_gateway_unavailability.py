"""Gateway_API behaviour when the State_Store is unavailable or the Request is terminal.

- Submit, query and cancel return 503 ``{"error": "service_unavailable"}`` (no
  ``request_id``) within 5 s when Redis is unreachable, a write fails, or a call
  hangs (1.9).
- Cancel on a Completed, Failed or Cancelled Request returns 409 and leaves the
  stored state unchanged (1.8).

**Validates: Requirements 1.8, 1.9**
"""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from redis.exceptions import ReadOnlyError

from das.codec import MessageCodec
from das.gateway.app import build_redis_client, create_app
from das.models import FinalResult, JobPlan, JobReport, JobState, PlannedJob, RequestStatus
from das.registry import AgentRegistry
from das.state import StateStore
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

KEY = "test-api-key-123"
AUTH = {"X-API-Key": KEY}
UNAVAILABLE = {"error": "service_unavailable"}
#: Requirement 1.9 upper bound for the 503 response.
BOUND = 5.0
PLAN = JobPlan(jobs=[PlannedJob(job_id="a", instruction="research x", capabilities=["research"])])

CALLS = [
    ("POST", "/v1/requests"),
    ("GET", "/v1/requests/some-id"),
    ("POST", "/v1/requests/some-id/cancel"),
]


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


async def _timed(c: httpx.AsyncClient, method: str, path: str) -> tuple[httpx.Response, float]:
    start = time.monotonic()
    r = await c.request(method, path, json={"prompt": "hi"}, headers=AUTH)
    return r, time.monotonic() - start


def _assert_unavailable(r: httpx.Response, elapsed: float) -> None:
    assert r.status_code == 503
    assert r.json() == UNAVAILABLE
    assert "request_id" not in r.text
    assert elapsed < BOUND


def _closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def silent_redis() -> AsyncIterator[int]:
    """A TCP server that accepts connections but never answers (a hung Redis)."""
    writers: list[asyncio.StreamWriter] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        while await reader.read(4096):
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        for w in writers:
            w.close()
        server.close()
        await server.wait_closed()


class HangingStore:
    """A State_Store whose calls never complete."""

    async def _hang(self, *_: Any, **__: Any) -> Any:
        await asyncio.Event().wait()

    rate_check = submit_request = load_snapshot = cancel_request = _hang


class RaisingStore:
    """A State_Store whose calls raise arbitrary non-Redis errors."""

    async def _boom(self, *_: Any, **__: Any) -> Any:
        raise RuntimeError("unexpected")

    rate_check = submit_request = load_snapshot = cancel_request = _boom


@pytest.mark.parametrize(("method", "path"), CALLS)
async def test_unreachable_redis_returns_503(fake_redis, method, path):
    client = build_redis_client(f"redis://127.0.0.1:{_closed_port()}/0")
    app = create_app(StateStore(client), AgentRegistry(fake_redis), None, [KEY], 100)
    try:
        async with _client(app) as c:
            r, elapsed = await _timed(c, method, path)
    finally:
        await client.aclose()
    _assert_unavailable(r, elapsed)


async def test_hung_redis_connection_returns_503_within_bound(fake_redis, silent_redis):
    """Real client against a server that never replies: socket/overall timeouts bound the call."""
    client = build_redis_client(f"redis://127.0.0.1:{silent_redis}/0")
    app = create_app(StateStore(client), AgentRegistry(fake_redis), None, [KEY], 100)
    try:
        async with _client(app) as c:
            results = await asyncio.gather(*(_timed(c, m, p) for m, p in CALLS))
    finally:
        await client.aclose()
    for r, elapsed in results:
        _assert_unavailable(r, elapsed)


async def test_hanging_store_call_returns_503_within_bound(fake_redis):
    app = create_app(HangingStore(), AgentRegistry(fake_redis), None, [KEY], 100)  # type: ignore[arg-type]
    async with _client(app) as c:
        results = await asyncio.gather(*(_timed(c, m, p) for m, p in CALLS))
    for r, elapsed in results:
        _assert_unavailable(r, elapsed)


@pytest.mark.parametrize(("method", "path"), CALLS)
async def test_unexpected_store_error_returns_503(fake_redis, method, path):
    app = create_app(RaisingStore(), AgentRegistry(fake_redis), None, [KEY], 100)  # type: ignore[arg-type]
    async with _client(app) as c:
        r, elapsed = await _timed(c, method, path)
    _assert_unavailable(r, elapsed)


async def test_write_failure_returns_503_and_persists_nothing(fake_redis, monkeypatch):
    """Reads work but every script write is rejected (e.g. a read-only replica)."""
    store = StateStore(fake_redis)
    rid = await store.create_request("existing")
    before = await store.load_snapshot(rid, include_transitions=True)

    async def readonly(*_: Any, **__: Any) -> Any:
        raise ReadOnlyError("You can't write against a read only replica.")

    monkeypatch.setattr(fake_redis, "evalsha", readonly)
    monkeypatch.setattr(fake_redis, "eval", readonly)
    app = create_app(store, AgentRegistry(fake_redis), None, [KEY], 100)
    async with _client(app) as c:
        submit, t1 = await _timed(c, "POST", "/v1/requests")
        cancel, t2 = await _timed(c, "POST", f"/v1/requests/{rid}/cancel")
    _assert_unavailable(submit, t1)
    _assert_unavailable(cancel, t2)
    assert sorted(await fake_redis.keys("das:req:*")) == sorted(
        await fake_redis.keys(f"das:req:{rid}*")
    )
    assert await store.load_snapshot(rid, include_transitions=True) == before


# ---------------------------------------------------------------------- cancel on terminal


async def _seed_terminal(store: StateStore, status: RequestStatus) -> str:
    rid = await store.create_request("hello")
    if status is RequestStatus.CANCELLED:
        assert (await store.cancel_request(rid)).ok
        return rid
    lease = (await store.acquire_lease(rid, "m1", 30)).lease
    assert (await store.commit_plan(lease, PLAN)).ok
    if status is RequestStatus.COMPLETED:
        final = FinalResult(summary="done", jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED)])
        assert (await store.finalize(lease, final)).ok
    else:
        assert (await store.fail_request(lease, "boom")).ok
    return rid


async def _keyspace(r: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in sorted(await r.keys("*")):
        if k.startswith("das:client:") and k.endswith(":bucket"):
            continue  # every authenticated call takes a rate-limit token (Phase 4 Req 2.1)
        kind = await r.type(k)
        if kind == "hash":
            out[k] = await r.hgetall(k)
        elif kind == "zset":
            out[k] = await r.zrange(k, 0, -1, withscores=True)
        elif kind == "list":
            out[k] = await r.lrange(k, 0, -1)
        elif kind == "stream":
            out[k] = await r.xrange(k)
        elif kind == "string":
            out[k] = await r.get(k)
        else:
            out[k] = kind
    return out


@pytest.mark.parametrize(
    "status", [RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED]
)
async def test_cancel_terminal_returns_409_state_unchanged(state_store, fake_redis, status):
    rid = await _seed_terminal(state_store, status)
    before = await _keyspace(fake_redis)

    broker = MemoryBroker()
    control = MemoryTransport(broker, agent_id="gateway")
    await control.start()
    try:
        app = create_app(state_store, AgentRegistry(fake_redis), control, [KEY], 100)
        async with _client(app) as c:
            r = await c.post(f"/v1/requests/{rid}/cancel", headers=AUTH)
    finally:
        await control.close()

    assert r.status_code == 409
    assert r.json() == {"error": "request_terminal", "status": status.value}
    assert await state_store.get_status(rid) is status
    assert await _keyspace(fake_redis) == before
    assert broker.pending_messages("m1", MessageCodec()) == []
