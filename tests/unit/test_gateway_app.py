"""Smoke tests for the Gateway_API endpoints (Requirements 1, 11.4, 13.9)."""

from __future__ import annotations

import re

import httpx
import pytest

from das.codec import MessageCodec
from das.codec.envelope import ControlMessage
from das.gateway.app import create_app, format_heartbeat
from das.models import (
    FinalResult,
    JobPlan,
    JobReport,
    JobState,
    PlannedJob,
    RequestStatus,
)
from das.registry import AgentInfo, AgentRegistry
from das.state import StateStore
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

KEY = "test-api-key-123"
AUTH = {"X-API-Key": KEY}
PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id="a", instruction="research x", capabilities=["research"]),
        PlannedJob(job_id="b", instruction="sum", capabilities=["summarize"], depends_on=["a"]),
    ]
)


@pytest.fixture
async def broker():
    return MemoryBroker()


@pytest.fixture
async def control(broker):
    transport = MemoryTransport(broker, agent_id="gateway")
    await transport.start()
    yield transport
    await transport.close()


@pytest.fixture
async def client(state_store: StateStore, fake_redis, control):
    app = create_app(state_store, AgentRegistry(fake_redis), control, [KEY], max_prompt_length=10)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        yield c


async def test_submit_then_query_pending(client, state_store):
    r = await client.post("/v1/requests", json={"prompt": "hi"}, headers=AUTH)
    assert r.status_code == 202
    rid = r.json()["request_id"]
    assert (await state_store.get_status(rid)) is RequestStatus.PENDING

    r = await client.get(f"/v1/requests/{rid}", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "Pending"
    assert body["jobs"] == [] and body["transitions"] == []
    assert "final_result" not in body


@pytest.mark.parametrize(
    ("prompt", "expected"),
    [
        ("   ", {"error": "empty_prompt"}),
        ("x" * 11, {"error": "prompt_too_long", "max_length": 10}),
    ],
)
async def test_submit_invalid_prompt_400(client, fake_redis, prompt, expected):
    r = await client.post("/v1/requests", json={"prompt": prompt}, headers=AUTH)
    assert r.status_code == 400
    assert r.json() == expected
    assert await fake_redis.keys("das:req:*") == []


async def test_submit_malformed_body_400(client):
    r = await client.post("/v1/requests", json={"text": "x"}, headers=AUTH)
    assert r.status_code == 400
    assert r.json() == {"error": "invalid_body"}


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/v1/requests"),
        ("GET", "/v1/requests/x"),
        ("POST", "/v1/requests/x/cancel"),
        ("GET", "/v1/agents"),
    ],
)
@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_unauthorized_401_flat_body(client, fake_redis, method, path, headers):
    r = await client.request(method, path, json={"prompt": "hi"}, headers=headers)
    assert r.status_code == 401
    assert r.json() == {"error": "unauthorized"}
    assert await fake_redis.keys("das:req:*") == []


async def test_unknown_request_404(client):
    assert (await client.get("/v1/requests/nope", headers=AUTH)).status_code == 404
    r = await client.post("/v1/requests/nope/cancel", headers=AUTH)
    assert r.status_code == 404
    assert r.json() == {"error": "not_found"}


async def test_running_request_shows_jobs_and_ordered_transitions(client, state_store):
    rid = await state_store.create_request("hello")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.commit_plan(lease, PLAN)).ok

    body = (await client.get(f"/v1/requests/{rid}", headers=AUTH)).json()
    assert body["status"] == "Running"
    assert body["jobs"] == [{"job_id": "a", "state": "Waiting"}, {"job_id": "b", "state": "Waiting"}]
    assert [t["job_id"] for t in body["transitions"]] == ["a", "b"]
    assert all(t["to_state"] == "Waiting" for t in body["transitions"])
    assert "final_result" not in body


async def test_completed_request_returns_final_result(client, state_store):
    rid = await state_store.create_request("hello")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.commit_plan(lease, PLAN)).ok
    final = FinalResult(summary="done", jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED)])
    assert (await state_store.finalize(lease, final)).ok

    body = (await client.get(f"/v1/requests/{rid}", headers=AUTH)).json()
    assert body["status"] == "Completed"
    assert body["final_result"]["summary"] == "done"


async def test_cancel_then_conflict_and_owner_notified(client, state_store, broker):
    rid = await state_store.create_request("hello")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.commit_plan(lease, PLAN)).ok

    r = await client.post(f"/v1/requests/{rid}/cancel", headers=AUTH)
    assert r.status_code == 202
    assert r.json() == {"status": "Cancelled"}
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.CANCELLED
    assert all(rec.state is JobState.CANCELLED for rec in snap.jobs.values())

    sent = broker.pending_messages("m1", MessageCodec())
    assert len(sent) == 1 and isinstance(sent[0], ControlMessage)
    assert sent[0].payload.command == "cancel_request"
    assert sent[0].payload.request_id == rid
    assert sent[0].correlation_id == rid

    r = await client.post(f"/v1/requests/{rid}/cancel", headers=AUTH)
    assert r.status_code == 409
    assert r.json()["error"] == "request_terminal"


async def test_cancel_succeeds_when_owner_notify_fails(state_store, fake_redis):
    """The control command is best-effort: a disconnected transport doesn't fail the cancel."""
    transport = MemoryTransport(MemoryBroker(), agent_id="gateway")  # never started
    app = create_app(state_store, AgentRegistry(fake_redis), transport, [KEY], 10)
    rid = await state_store.create_request("hello")
    await state_store.acquire_lease(rid, "m1", 30)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        r = await c.post(f"/v1/requests/{rid}/cancel", headers=AUTH)
    assert r.status_code == 202
    assert (await state_store.get_status(rid)) is RequestStatus.CANCELLED


async def test_agents_listing_second_precision_utc(client, fake_redis):
    registry = AgentRegistry(fake_redis)
    await registry.register(AgentInfo(agent_id="w1", role="worker", host="h1", capabilities={"research"}))
    r = await client.get("/v1/agents", headers=AUTH)
    assert r.status_code == 200
    [agent] = r.json()
    assert agent["agent_id"] == "w1" and agent["role"] == "worker" and agent["host"] == "h1"
    assert agent["capabilities"] == ["research"] and agent["status"] == "Available"
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", agent["last_heartbeat"])


def test_format_heartbeat_truncates_to_seconds():
    assert format_heartbeat(0.999) == "1970-01-01T00:00:00Z"
    assert format_heartbeat(1_700_000_000.5) == "2023-11-14T22:13:20Z"


async def test_healthz(client):
    r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["overall"] == "healthy"


async def test_healthz_injected_unhealthy(state_store, fake_redis):
    async def unhealthy():
        return {"overall": "unhealthy", "redis": "disconnected"}

    app = create_app(state_store, AgentRegistry(fake_redis), None, [KEY], 10, health_check=unhealthy)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        r = await c.get("/healthz")
    assert r.status_code == 503
