"""Phase 2 extensions of the v1 Gateway endpoints and HTTP metrics.

Requirements 2.1, 2.3, 2.10, 2.13, 6.1, 9.8, 9.12, 9.13, 10.3, 10.7, 17.7, 19.6.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, settings
from hypothesis import strategies as st
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client.parser import text_string_to_metric_families

from das.clock import FakeClock
from das.config.schema import TracingConfig
from das.gateway.app import create_app
from das.gateway.http_metrics import route_templates
from das.models import JobPlan, PlannedJob, RequestStatus, TokenUsage
from das.models.dlq import ReplayHistoryEntry, RequestTransitionEntry
from das.models.usage import UsageRecord
from das.observability.metrics import HTTP_METHODS, MetricsRecorder
from das.observability.tracing import OtelTracer, build_tracer
from das.ops.cost import PriceEntry, PriceTable
from das.ops.usage import summarize_usage
from das.registry import AgentInfo, AgentRegistry
from das.state import StateStore
from das.state import keys

KEY = "test-api-key-123"
AUTH = {"X-API-Key": KEY}
PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id="a", instruction="research x", capabilities=["research"]),
        PlannedJob(job_id="b", instruction="sum", capabilities=["summarize"], depends_on=["a"]),
    ]
)
PLAN_USAGE = UsageRecord(
    operation="plan",
    provider="openai",
    model="gpt-x",
    usage=TokenUsage(input_tokens=1000, output_tokens=500, total_tokens=1500),
)
V1_REQUEST_KEYS = {"request_id", "status", "jobs", "transitions"}
V1_AGENT_KEYS = {"agent_id", "role", "host", "capabilities", "status", "last_heartbeat"}


class Recorders:
    """Captures the recorder built by ``create_app``'s metrics factory."""

    def __init__(self) -> None:
        self.recorder: MetricsRecorder | None = None
        self.templates: tuple[str, ...] = ()

    def __call__(self, templates: Sequence[str]) -> MetricsRecorder:
        self.templates = tuple(templates)
        self.recorder = MetricsRecorder(clock=FakeClock(), route_templates=templates)
        return self.recorder

    def value(self, sample_name: str, **labels: str) -> float:
        assert self.recorder is not None
        body, _ = self.recorder.render()
        for fam in text_string_to_metric_families(body.decode()):
            for s in fam.samples:
                if s.name == sample_name and s.labels == labels:
                    return s.value
        return 0.0


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


@pytest.fixture
async def client(state_store: StateStore, fake_redis, recorders: Recorders):
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [KEY], 10, metrics_factory=recorders
    )
    async with _client(app) as c:
        yield c


# --------------------------------------------------------------------------- submit


async def test_submitted_counter_only_on_ok(client, recorders: Recorders):
    assert (await client.post("/v1/requests", json={"prompt": "hi"}, headers=AUTH)).status_code == 202
    assert recorders.value("das_requests_submitted_total") == 1
    # 400 (validation), 400 (body), 401 (auth) never count (Req 2.1).
    await client.post("/v1/requests", json={"prompt": "   "}, headers=AUTH)
    await client.post("/v1/requests", json={"text": "x"}, headers=AUTH)
    await client.post("/v1/requests", json={"prompt": "hi"}, headers={"X-API-Key": "bad"})
    assert recorders.value("das_requests_submitted_total") == 1


async def test_submitted_counter_unchanged_on_store_failure(fake_redis, recorders: Recorders):
    class FailingStore(StateStore):
        async def create_request(self, prompt, trace_context=None):  # type: ignore[override]
            raise ConnectionError("down")

    app = create_app(
        FailingStore(fake_redis), AgentRegistry(fake_redis), None, [KEY], 10,
        metrics_factory=recorders,
    )
    async with _client(app) as c:
        r = await c.post("/v1/requests", json={"prompt": "hi"}, headers=AUTH)
    assert r.status_code == 503 and r.json() == {"error": "service_unavailable"}
    assert recorders.value("das_requests_submitted_total") == 0


@pytest.mark.parametrize("ratio", [1.0, 0.0])
async def test_submit_span_context_persisted_even_unsampled(state_store, fake_redis, ratio):
    mem = InMemorySpanExporter()
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317", sample_ratio=ratio)
    tracer = build_tracer(cfg, "gateway", "gw1", FakeClock(), exporter=mem, start_thread=False)
    assert isinstance(tracer, OtelTracer)
    app = create_app(state_store, AgentRegistry(fake_redis), None, [KEY], 10, tracer=tracer)
    async with _client(app) as c:
        rid = (await c.post("/v1/requests", json={"prompt": "hi"}, headers=AUTH)).json()[
            "request_id"
        ]
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and snap.trace_context is not None  # Req 6.1
    _, trace_id, _, flags = snap.trace_context.traceparent.split("-")
    while tracer.processor.queued:
        assert tracer.processor.export_once()
    spans = [s for s in mem.get_finished_spans() if s.name == "gateway.submit"]
    if ratio == 1.0:
        [span] = spans
        assert format(span.context.trace_id, "032x") == trace_id
        assert span.attributes["das.request_id"] == rid
    else:
        assert spans == []
    # The sampled bit travels with the persisted context (Req 6.4).
    assert bool(int(flags, 16) & 0x01) is (ratio == 1.0)


async def test_submit_without_tracing_persists_no_context(client, state_store):
    rid = (await client.post("/v1/requests", json={"prompt": "hi"}, headers=AUTH)).json()[
        "request_id"
    ]
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and snap.trace_context is None  # Req 6.13


# --------------------------------------------------------------------------- cancel


async def test_cancel_finished_metrics_only_on_ok(client, state_store, recorders: Recorders):
    rid = await state_store.create_request("hello")
    assert (await client.post(f"/v1/requests/{rid}/cancel", headers=AUTH)).status_code == 202
    labels = {"status": "Cancelled"}
    assert recorders.value("das_requests_finished_total", **labels) == 1
    assert recorders.value("das_request_duration_seconds_count", **labels) == 1
    # TERMINAL (409) leaves both unchanged (Req 2.13).
    assert (await client.post(f"/v1/requests/{rid}/cancel", headers=AUTH)).status_code == 409
    assert recorders.value("das_requests_finished_total", **labels) == 1
    assert recorders.value("das_request_duration_seconds_count", **labels) == 1


# --------------------------------------------------------------------------- query


async def _running_with_usage(state_store: StateStore) -> str:
    rid = await state_store.create_request("hello")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert lease is not None
    assert (await state_store.commit_plan(lease, PLAN, usage=[PLAN_USAGE])).ok
    return rid


async def test_query_adds_fields_and_keeps_v1_fields(client, state_store):
    rid = await state_store.create_request("hello")
    body = (await client.get(f"/v1/requests/{rid}", headers=AUTH)).json()
    assert V1_REQUEST_KEYS <= set(body)
    assert body["status"] == "Pending" and body["jobs"] == [] and body["transitions"] == []
    assert body["token_usage"] == summarize_usage([], []).model_dump(mode="json")
    assert body["token_usage"]["total"] == {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    assert body["estimated_cost"] is None  # no Price_Table (Req 10.7)
    assert body["replay_history"] == []


async def test_query_token_usage_from_committed_records(client, state_store):
    rid = await _running_with_usage(state_store)
    body = (await client.get(f"/v1/requests/{rid}", headers=AUTH)).json()
    expected = summarize_usage([PLAN_USAGE], ["a", "b"]).model_dump(mode="json")
    assert body["token_usage"] == expected
    assert set(body["token_usage"]["by_job"]) == {"a", "b"}
    assert [t["job_id"] for t in body["transitions"]] == ["a", "b"]  # v1 shape unchanged


async def test_estimated_cost_with_price_table(state_store, fake_redis):
    table = PriceTable(
        currency="EUR",
        models=[PriceEntry(provider="openai", model="gpt-x",
                           input_per_million=Decimal("2"), output_per_million=Decimal("4"))],
    )
    rid = await _running_with_usage(state_store)
    pending = await state_store.create_request("other")
    app = create_app(state_store, AgentRegistry(fake_redis), None, [KEY], 10, price_table=table)
    async with _client(app) as c:
        cost = (await c.get(f"/v1/requests/{rid}", headers=AUTH)).json()["estimated_cost"]
        empty = (await c.get(f"/v1/requests/{pending}", headers=AUTH)).json()["estimated_cost"]
    # 1000 * 2 / 1e6 + 500 * 4 / 1e6 = 0.004
    assert cost["amount"] == "0.004000" and cost["currency"] == "EUR"
    assert cost["complete"] is True and cost["unpriced_models"] == []
    assert cost["by_model"] == [{"provider": "openai", "model": "gpt-x", "amount": "0.004000"}]
    assert empty == {"amount": "0.000000", "currency": "EUR", "complete": True,
                     "unpriced_models": [], "by_model": []}


async def test_empty_price_table_marks_every_model_unpriced(state_store, fake_redis):
    rid = await _running_with_usage(state_store)
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [KEY], 10, price_table=PriceTable()
    )
    async with _client(app) as c:
        cost = (await c.get(f"/v1/requests/{rid}", headers=AUTH)).json()["estimated_cost"]
    assert cost["amount"] == "0.000000" and cost["complete"] is False
    assert cost["unpriced_models"] == [{"provider": "openai", "model": "gpt-x"}]


async def test_unreadable_usage_is_503_without_partial_summary(client, state_store, fake_redis):
    rid = await _running_with_usage(state_store)
    await fake_redis.rpush(keys.usage(rid), "{not json")
    r = await client.get(f"/v1/requests/{rid}", headers=AUTH)
    assert r.status_code == 503 and r.json() == {"error": "service_unavailable"}  # Req 9.12


async def test_replay_history_and_request_transition_entry(client, state_store, fake_redis):
    rid = await _running_with_usage(state_store)
    dlid = "0" * 31 + "1"
    at = datetime(2100, 1, 1, tzinfo=UTC)  # after the plan's transitions
    entry = RequestTransitionEntry(
        from_status=RequestStatus.FAILED, to_status=RequestStatus.PENDING,
        dead_letter_id=dlid, at=at,
    )
    history = ReplayHistoryEntry(dead_letter_id=dlid, at=at, error="job_failed")
    await fake_redis.rpush(keys.transitions(rid), entry.model_dump_json())
    await fake_redis.hset(
        keys.request(rid), keys.F_REPLAY_HISTORY, f"[{history.model_dump_json()}]"
    )
    body = (await client.get(f"/v1/requests/{rid}", headers=AUTH)).json()
    assert body["replay_history"] == [history.model_dump(mode="json")]
    assert [t.get("job_id") for t in body["transitions"]] == ["a", "b", None]
    assert body["transitions"][-1] == entry.model_dump(mode="json")
    assert body["transitions"][-1]["scope"] == "request"


# --------------------------------------------------------------------------- agents


async def test_agents_in_flight_and_drained(client, fake_redis):
    registry = AgentRegistry(fake_redis)
    for aid in ("w1", "w2", "m1"):
        role = "manager" if aid == "m1" else "worker"
        await registry.register(AgentInfo(agent_id=aid, role=role, host="h"))
    assert await registry.heartbeat("w1", "Draining", in_flight=0)
    assert await registry.heartbeat("w2", "Draining", in_flight=2)
    assert await registry.heartbeat("m1", "Available", in_flight=3)
    listing = {a["agent_id"]: a for a in (await client.get("/v1/agents", headers=AUTH)).json()}
    assert all(V1_AGENT_KEYS <= set(a) for a in listing.values())
    assert (listing["w1"]["status"], listing["w1"]["in_flight"], listing["w1"]["drained"]) == (
        "Draining", 0, True
    )
    assert (listing["w2"]["status"], listing["w2"]["in_flight"], listing["w2"]["drained"]) == (
        "Draining", 2, False
    )
    assert (listing["m1"]["status"], listing["m1"]["in_flight"], listing["m1"]["drained"]) == (
        "Available", 3, False
    )


# --------------------------------------------------------------------------- HTTP metrics


async def test_http_metrics_route_template_and_unmatched(client, state_store, recorders):
    rid = await state_store.create_request("hello")
    await client.get(f"/v1/requests/{rid}", headers=AUTH)
    await client.get("/v1/requests/other-id", headers=AUTH)  # 404 from the handler
    await client.get("/v1/requests/x", headers={})  # 401 from the auth dependency
    await client.get("/metrics")  # Ops path is not served on http_port
    template = "/v1/requests/{request_id}"
    assert template in recorders.templates
    get = {"route": template, "method": "GET"}
    assert recorders.value("das_http_requests_total", **get, status_code="200") == 1
    assert recorders.value("das_http_requests_total", **get, status_code="404") == 1
    assert recorders.value("das_http_requests_total", **get, status_code="401") == 1
    assert recorders.value("das_http_request_duration_seconds_count", **get) == 3
    unmatched = {"route": "_unmatched", "method": "GET", "status_code": "404"}
    assert recorders.value("das_http_requests_total", **unmatched) == 1
    body, _ = recorders.recorder.render()  # type: ignore[union-attr]
    assert rid not in body.decode()  # never a concrete path (Req 5.1)


@settings(max_examples=25, deadline=None)
@given(
    method=st.one_of(
        st.sampled_from(sorted(HTTP_METHODS - {"_other"})),
        st.from_regex(r"[A-Z]{3,10}", fullmatch=True),
    )
)
def test_http_method_normalized(method: str) -> None:
    async def scenario() -> None:
        redis = FakeRedis(server=FakeServer(), decode_responses=True)
        recorders = Recorders()
        app = create_app(
            StateStore(redis), AgentRegistry(redis), None, [KEY], 10, metrics_factory=recorders
        )
        async with _client(app) as c:
            r = await c.request(method, "/v1/agents", headers=AUTH)
        expected = method if method in HTTP_METHODS else "_other"
        labels = {"route": "/v1/agents", "method": expected, "status_code": str(r.status_code)}
        assert recorders.value("das_http_requests_total", **labels) == 1
        assert sum(
            recorders.value("das_http_requests_total", route="/v1/agents", method=m,
                            status_code=str(r.status_code))
            for m in HTTP_METHODS
        ) == 1

    asyncio.run(scenario())


def test_route_templates_include_v1_and_admin_routes(state_store, fake_redis):
    app = create_app(state_store, AgentRegistry(fake_redis), None, [KEY], 10)
    templates = route_templates(app.routes)
    assert {
        "/v1/requests",
        "/v1/requests/{request_id}",
        "/v1/requests/{request_id}/cancel",
        "/v1/agents",
        "/healthz",
    } <= set(templates)
    assert app.state.route_templates == templates
