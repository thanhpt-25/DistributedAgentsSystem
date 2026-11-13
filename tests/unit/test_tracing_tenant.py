"""The ``das.tenant`` Span attribute and the absence of key material (das-multi-tenant task 7.3; Phase 5 Req 8.3, 8.4).

``gateway.submit`` and ``manager.request`` carry ``das.tenant``; the Tracer's allow-list grows by exactly that name.
No metric label, Span attribute or index carries a Key_Fingerprint or a key value, and the global Request_Index holds no
tenant data."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from das.clock import FakeClock
from das.config.schema import TenantConfig, TracingConfig
from das.gateway.app import create_app
from das.manager.driver import DriverOutcome
from das.observability.tracing import ALLOWED_ATTRIBUTES, OtelTracer, build_tracer
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import ClientRequestSpec, StateStore
from tests.unit.test_gateway_v1_extensions import Recorders
from tests.unit.test_manager_driver import TIMEOUT, make_env
from tests.unit.test_manager_driver_observability import _spans, _tracer, _wire

A1, DEFAULT = "team-a-key-000000001", "default-key-00000001"
PHASE4_ATTRIBUTES = {
    "das.request_id", "das.job_id", "das.attempt", "das.capability", "das.agent_id", "das.role",
    "llm.provider", "llm.model", "llm.operation", "llm.input_tokens", "llm.output_tokens", "error.type",
}  # fmt: skip


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def test_the_allow_list_grows_by_exactly_the_tenant_attribute() -> None:
    assert ALLOWED_ATTRIBUTES - PHASE4_ATTRIBUTES == {"das.tenant"}
    assert PHASE4_ATTRIBUTES <= ALLOWED_ATTRIBUTES


# --------------------------------------------------------------------------- the Gateway


@pytest.fixture
async def traced(state_store: StateStore, fake_redis: Any) -> AsyncIterator[tuple[httpx.AsyncClient, Any, Any, Any]]:
    mem = InMemorySpanExporter()
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317", sample_ratio=1.0)
    tracer = build_tracer(cfg, "gateway", "gw1", FakeClock(), exporter=mem, start_thread=False)
    assert isinstance(tracer, OtelTracer)
    recorders = Recorders()
    keys = [{"fingerprint": fp(A1), "scopes": ["submit", "read"]}]
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [A1, DEFAULT], 1000,
        tenants=[TenantConfig.model_validate({"id": "team-a", "keys": keys})],
        tracer=tracer, metrics_factory=recorders,
    )  # fmt: skip
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        yield client, tracer, mem, recorders


def exported(tracer: OtelTracer, mem: InMemorySpanExporter, name: str | None = None) -> list[Any]:
    while tracer.processor.queued:
        assert tracer.processor.export_once()
    return [s for s in mem.get_finished_spans() if name is None or s.name == name]


async def test_gateway_submit_carries_the_tenant_of_the_key(traced: Any) -> None:
    client, tracer, mem, _ = traced
    await client.post("/v1/requests", json={"prompt": "p"}, headers={"X-API-Key": A1})
    await client.post("/v1/requests", json={"prompt": "p"}, headers={"X-API-Key": DEFAULT})
    spans = exported(tracer, mem, "gateway.submit")
    assert sorted(s.attributes["das.tenant"] for s in spans) == ["default", "team-a"]


async def test_a_refused_submit_still_names_the_tenant(traced: Any) -> None:
    """The attribute is set when the span opens, so a 400 is attributable too."""
    client, tracer, mem, _ = traced
    await client.post("/v1/requests", json={"prompt": ""}, headers={"X-API-Key": A1})
    [span] = exported(tracer, mem, "gateway.submit")
    assert span.attributes["das.tenant"] == "team-a"


# --------------------------------------------------------------------------- the Manager


async def test_manager_request_carries_the_tenant_of_the_request(state_store: StateStore, fake_redis: Any) -> None:
    exporter = InMemorySpanExporter()
    tracer = _tracer(exporter)
    env = await make_env(state_store, fake_redis, tweak=_wire(None, tracer))
    spec = ClientRequestSpec(fingerprint="cbabecbd", tenant="team-a")
    rid = (await state_store.submit_request("hello", spec)).request_id
    assert rid is not None
    acquired = await state_store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.ok and acquired.lease is not None
    assert await asyncio.wait_for(env.drivers.on_claimed(acquired.lease), TIMEOUT) is DriverOutcome.COMPLETED
    spans = _spans(tracer, exporter)
    [request] = [s for s in spans if s.name == "manager.request"]
    assert request.attributes["das.tenant"] == "team-a"
    others = [s for s in spans if s.name != "manager.request"]
    assert others and all("das.tenant" not in (s.attributes or {}) for s in others)  # exactly the two spans


async def test_manager_request_of_the_default_tenant(state_store: StateStore, fake_redis: Any) -> None:
    exporter = InMemorySpanExporter()
    tracer = _tracer(exporter)
    env = await make_env(state_store, fake_redis, tweak=_wire(None, tracer))
    _, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    [request] = [s for s in _spans(tracer, exporter) if s.name == "manager.request"]
    assert request.attributes["das.tenant"] == "default"


# --------------------------------------------------------------------------- no key material


async def redis_contents(state_store: StateStore, pattern: str) -> list[str]:
    """Every key name matching ``pattern`` with its members, scores or fields, as text."""
    redis = state_store.client
    out: list[str] = []
    for raw in await redis.keys(pattern):
        name = text(raw)
        readers = {
            "set": lambda: redis.smembers(name),
            "zset": lambda: redis.zrange(name, 0, -1, withscores=True),
            "hash": lambda: redis.hgetall(name),
            "string": lambda: redis.get(name),
        }
        read = readers.get(text(await redis.type(name)))
        out.append(f"{name} {(await read())!r}" if read else name)  # a type with no reader: the name alone
    return out


async def test_no_span_metric_or_index_carries_a_key_or_its_fingerprint(traced: Any, state_store: StateStore) -> None:
    client, tracer, mem, recorders = traced
    headers = {"X-API-Key": A1}
    rid = (await client.post("/v1/requests", json={"prompt": "p"}, headers=headers)).json()["request_id"]
    await client.get("/v1/requests", headers=headers)
    await client.get("/v1/usage", headers=headers)
    await client.post(f"/v1/requests/{rid}/cancel", headers=headers)
    await state_store.set_tenant_index_ready()
    await client.get("/v1/requests", headers=headers)

    spans = exported(tracer, mem)
    indexes = await redis_contents(state_store, "das:tenant:*")  # what Phase 5 adds: names, members, scores, fields
    assert spans and indexes  # there was something to look at
    surfaces = {
        "spans": repr([(s.name, dict(s.attributes or {})) for s in spans]),
        "metrics": recorders.recorder.render()[0].decode(),
        "tenant indexes": "\n".join(indexes),
    }
    for where, body in surfaces.items():
        for secret in (A1, fp(A1)):
            assert secret not in body, f"{where} carries key material"


async def test_the_global_request_index_holds_no_tenant_data(traced: Any, state_store: StateStore) -> None:
    client, _, _, _ = traced
    rid = (await client.post("/v1/requests", json={"prompt": "p"}, headers={"X-API-Key": A1})).json()["request_id"]
    contents = await redis_contents(state_store, "das:requests*")
    assert any(rid in c for c in contents)  # the index lists the request, by id alone
    assert all("team-a" not in c for c in contents)
