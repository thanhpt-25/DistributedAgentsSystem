# Feature: distributed-agent-system, Property 8: Rejected calls are non-mutating
"""Property 8: Rejected calls are non-mutating.

For any State_Store contents and any submit, query, cancel, or agent-listing call that
either carries a missing or non-configured API_Key (401) or targets a Request_ID absent
from the store (404), the State_Store contents after the call equal those before, and no
control Message is published. A 401 response contains only ``{"error": "unauthorized"}``.

The store is pre-populated through the real store/registry API (Pending, leased,
Running, Completed, Failed and Cancelled Requests plus registered agents) on a fresh
in-process fakeredis server per example; the full keyspace (every key's type, contents
and whether it carries an expiry) is compared before and after the rejected calls.

**Validates: Requirements 1.3, 1.5, 11.8**
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given
from hypothesis import strategies as st

from das.gateway.app import create_app
from das.models import FinalResult, JobPlan, JobReport, JobState, PlannedJob
from das.registry import AgentInfo, AgentRegistry
from das.state import StateStore
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

KEY = "configured-key-0123456789"
UNAUTHORIZED = {"error": "unauthorized"}
NOT_FOUND = {"error": "not_found"}
SEEDS = ("pending", "leased", "running", "completed", "failed", "cancelled")
PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id="a", instruction="research x", capabilities=["research"]),
        PlannedJob(job_id="b", instruction="sum", capabilities=["summarize"], depends_on=["a"]),
    ]
)

# Header-safe printable ASCII without surrounding whitespace (h11 strips it).
_key_chars = st.characters(min_codepoint=0x21, max_codepoint=0x7E)
_path_ids = st.text(
    alphabet=st.characters(categories=("Ll", "Lu", "Nd"), max_codepoint=0x7F, include_characters="-_"),
    min_size=1,
    max_size=40,
)


@dataclass(frozen=True)
class Call:
    method: str
    path: str  # may contain "{rid}" to be filled with an existing or arbitrary id
    headers: dict[str, str]
    body: Any
    expect: int


def _auth_headers() -> st.SearchStrategy[dict[str, str]]:
    wrong = st.text(_key_chars, min_size=1, max_size=40).filter(lambda k: k != KEY)
    return st.one_of(
        st.just({}),
        st.just({"X-API-Key": ""}),
        wrong.map(lambda k: {"X-API-Key": k}),
        st.just({"X-API-Key": KEY + "x"}),
        st.just({"X-API-Key": KEY[:-1]}),
    )


_bodies = st.one_of(
    st.none(),
    st.fixed_dictionaries({"prompt": st.text(max_size=20)}),
    st.just({"text": "x"}),
)


@st.composite
def unauthorized_calls(draw: st.DrawFn) -> Call:
    method, path = draw(
        st.sampled_from(
            [
                ("POST", "/v1/requests"),
                ("GET", "/v1/requests/{rid}"),
                ("POST", "/v1/requests/{rid}/cancel"),
                ("GET", "/v1/agents"),
            ]
        )
    )
    return Call(method, path, draw(_auth_headers()), draw(_bodies), 401)


@st.composite
def not_found_calls(draw: st.DrawFn) -> Call:
    method, path = draw(
        st.sampled_from([("GET", "/v1/requests/{missing}"), ("POST", "/v1/requests/{missing}/cancel")])
    )
    return Call(method, path, {"X-API-Key": KEY}, None, 404)


@dataclass(frozen=True)
class Scenario:
    seeds: tuple[str, ...]
    agents: tuple[str, ...]
    calls: tuple[tuple[Call, int, str], ...]  # (call, existing-id index, missing id)


@st.composite
def scenarios(draw: st.DrawFn) -> Scenario:
    seeds = tuple(draw(st.lists(st.sampled_from(SEEDS), max_size=6)))
    agents = tuple(draw(st.lists(st.sampled_from(["w1", "w2", "m1"]), unique=True, max_size=3)))
    calls = draw(
        st.lists(
            st.tuples(
                st.one_of(unauthorized_calls(), not_found_calls()),
                st.integers(min_value=0, max_value=10),
                _path_ids,
            ),
            min_size=1,
            max_size=6,
        )
    )
    return Scenario(seeds, agents, tuple(calls))


async def _seed(store: StateStore, registry: AgentRegistry, sc: Scenario) -> list[str]:
    rids: list[str] = []
    for kind in sc.seeds:
        rid = await store.create_request(f"prompt {kind}")
        rids.append(rid)
        if kind == "pending":
            continue
        lease = (await store.acquire_lease(rid, "m1", 300)).lease
        assert lease is not None
        if kind == "leased":
            continue
        assert (await store.commit_plan(lease, PLAN)).ok
        if kind == "completed":
            final = FinalResult(summary="done", jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED)])
            assert (await store.finalize(lease, final)).ok
        elif kind == "failed":
            assert (await store.fail_request(lease, "boom")).ok
        elif kind == "cancelled":
            await store.cancel_request(rid)
    for aid in sc.agents:
        role = "manager" if aid.startswith("m") else "worker"
        await registry.register(AgentInfo(agent_id=aid, role=role, host="h", capabilities={"research"}))
    return rids


async def _dump(r: FakeRedis) -> dict[str, tuple[str, Any, bool]]:
    """Every key with its type, full contents, and whether it has an expiry."""
    out: dict[str, tuple[str, Any, bool]] = {}
    for key in sorted(await r.keys("*")):
        if key.startswith("das:client:") and key.endswith(":bucket"):
            continue  # each authenticated call takes a rate-limit token (Phase 4 Req 2.1)
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
        else:  # pragma: no cover - unexpected type still compared by name
            val = None
        out[key] = (kind, val, (await r.pttl(key)) > 0)
    return out


async def _run(sc: Scenario) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    broker = MemoryBroker()
    control = MemoryTransport(broker, agent_id="gateway")
    await control.start()
    try:
        store = StateStore(redis)
        registry = AgentRegistry(redis)
        rids = await _seed(store, registry, sc)
        app = create_app(store, registry, control, [KEY], max_prompt_length=50)
        before = await _dump(redis)
        wire_before = len(broker.wire)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
            for call, idx, missing in sc.calls:
                existing = rids[idx % len(rids)] if rids else missing
                if missing in rids:
                    missing = missing + "-absent"
                path = call.path.format(rid=existing, missing=missing)
                kwargs: dict[str, Any] = {"headers": call.headers}
                if call.body is not None:
                    kwargs["json"] = call.body
                resp = await c.request(call.method, path, **kwargs)
                assert resp.status_code == call.expect, (call, path, resp.text)
                assert resp.json() == (UNAUTHORIZED if call.expect == 401 else NOT_FOUND)

        assert await _dump(redis) == before
        assert len(broker.wire) == wire_before
    finally:
        await control.close()
        await redis.aclose()


@given(scenarios())
def test_rejected_calls_are_non_mutating(sc: Scenario) -> None:
    asyncio.run(_run(sc))
