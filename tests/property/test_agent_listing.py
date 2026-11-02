"""Property test for the Gateway agent listing (task 16.7).

Builds random registry contents with the real ``AgentRegistry`` Lua scripts on
fakeredis (register, optional heartbeat, sweep, deregister under an injected
clock) and checks ``GET /v1/agents`` against an in-memory model.
"""

# Feature: distributed-agent-system, Property 34: Agent listing completeness

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given
from hypothesis import strategies as st

from das.gateway.app import create_app
from das.registry import AgentInfo, AgentRegistry
from das.registry.availability import MISSED_INTERVALS
from das.state import StateStore

KEY = "test-api-key-123"
AUTH = {"X-API-Key": KEY}
BASE = 1_700_000_000  # epoch seconds; times are BASE + milliseconds / 1000
ISO_SECONDS = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ")

CAPS = ["code", "search", "summarize", "translate", "research"]


@dataclass(frozen=True)
class AgentSpec:
    agent_id: str
    role: str
    host: str
    capabilities: frozenset[str]
    interval: int
    registered_ms: int
    heartbeat_ms: int | None  # extra heartbeat offset after registration
    deregistered: bool


millis = st.integers(min_value=0, max_value=60_000)


@st.composite
def agent_specs(draw) -> list[AgentSpec]:
    ids = draw(st.lists(st.from_regex(r"[a-z][a-z0-9-]{0,11}", fullmatch=True), unique=True, max_size=8))
    return [
        AgentSpec(
            agent_id=agent_id,
            role=draw(st.sampled_from(["worker", "manager", "gateway"])),
            host=draw(st.text(min_size=0, max_size=20)),
            capabilities=draw(st.frozensets(st.sampled_from(CAPS), max_size=4)),
            interval=draw(st.sampled_from([1, 2, 5, 10])),
            registered_ms=draw(millis),
            heartbeat_ms=draw(st.none() | millis),
            deregistered=draw(st.booleans()),
        )
        for agent_id in ids
    ]


def expected_heartbeat(ts: float) -> str:
    """Independent second-precision UTC rendering (truncation, not rounding)."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))


async def _scenario(specs: list[AgentSpec], sweep_ms: int | None) -> None:
    now = [0.0]

    async def clock() -> float:
        return now[0]

    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        registry = AgentRegistry(redis, time_source=clock)
        expected: dict[str, dict] = {}
        last_hb: dict[str, float] = {}

        for s in specs:
            now[0] = BASE + s.registered_ms / 1000
            await registry.register(
                AgentInfo(
                    agent_id=s.agent_id,
                    role=s.role,
                    host=s.host,
                    capabilities=s.capabilities,
                    heartbeat_interval=s.interval,
                )
            )
            last_hb[s.agent_id] = now[0]
        for s in specs:
            if s.heartbeat_ms is not None:
                now[0] = last_hb[s.agent_id] + s.heartbeat_ms / 1000
                assert await registry.heartbeat(s.agent_id)
                last_hb[s.agent_id] = now[0]

        swept_at = None
        if sweep_ms is not None:
            swept_at = BASE + sweep_ms / 1000
            await registry.sweep(swept_at)

        for s in specs:
            if s.deregistered:
                assert await registry.deregister(s.agent_id)
                continue
            stale = swept_at is not None and swept_at - last_hb[s.agent_id] >= MISSED_INTERVALS * s.interval
            expected[s.agent_id] = {
                "agent_id": s.agent_id,
                "role": s.role,
                "host": s.host,
                "capabilities": sorted(s.capabilities),
                "status": "Unavailable" if stale else "Available",
                "last_heartbeat": expected_heartbeat(last_hb[s.agent_id]),
                # Phase 2 additive fields (Req 17.7); never Draining here.
                "in_flight": 0,
                "drained": False,
            }

        app = create_app(StateStore(redis), registry, None, [KEY], max_prompt_length=10)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
            r = await c.get("/v1/agents", headers=AUTH)
    finally:
        await redis.aclose()

    assert r.status_code == 200
    listing = r.json()
    ids = [a["agent_id"] for a in listing]
    # Exactly one entry per registered (non-deregistered) agent, Unavailable included.
    assert len(ids) == len(set(ids))
    assert set(ids) == set(expected)
    for entry in listing:
        assert entry == expected[entry["agent_id"]]
        assert ISO_SECONDS.fullmatch(entry["last_heartbeat"])


@given(specs=agent_specs(), sweep_ms=st.none() | st.integers(min_value=0, max_value=200_000))
def test_agent_listing_completeness(specs: list[AgentSpec], sweep_ms: int | None) -> None:
    """**Validates: Requirements 11.4**"""
    asyncio.run(_scenario(specs, sweep_ms))
