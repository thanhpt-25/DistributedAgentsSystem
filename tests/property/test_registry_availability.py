"""Stateful property test for the Agent_Registry availability model (task 10.2).

Drives the real ``AgentRegistry`` Lua scripts on fakeredis with an injected
clock and compares every sweep against a plain in-memory model.
"""

# Feature: distributed-agent-system, Property 33: Registry availability model

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from das.registry import AgentInfo, AgentRegistry
from das.registry.availability import MISSED_INTERVALS

AGENT_IDS = ["a0", "a1", "a2", "a3", "a4"]
CAPS = ["code", "search", "summarize", "translate"]

# Integer times and intervals keep the 3 x interval boundary exact through Lua.
roles = st.sampled_from(["worker", "worker", "worker", "manager", "gateway"])
capability_sets = st.frozensets(st.sampled_from(CAPS), max_size=3)
intervals = st.sampled_from([1, 2, 5])


@dataclass
class ModelAgent:
    role: str
    capabilities: frozenset[str]
    interval: int
    last_heartbeat: float
    status: str = "Available"


class RegistryAvailabilityMachine(RuleBasedStateMachine):
    """**Validates: Requirements 11.2, 11.7**"""

    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.now = 1_000.0
        self.model: dict[str, ModelAgent] = {}

        async def clock() -> float:
            return self.now

        async def make_client() -> FakeRedis:
            return FakeRedis(server=FakeServer(), decode_responses=True)

        self.redis = self.run(make_client())
        self.registry = AgentRegistry(self.redis, time_source=clock)

    def run(self, coro):
        return self.loop.run_until_complete(coro)

    def teardown(self) -> None:
        self.run(self.redis.aclose())
        self.loop.close()

    # ------------------------------------------------------------------ helpers

    def model_advertised(self) -> frozenset[str]:
        caps: set[str] = set()
        for a in self.model.values():
            if a.role == "worker" and a.status == "Available":
                caps |= a.capabilities
        return frozenset(caps)

    # -------------------------------------------------------------------- rules

    @rule(agent_id=st.sampled_from(AGENT_IDS), role=roles, caps=capability_sets, interval=intervals)
    def register(self, agent_id, role, caps, interval):
        info = AgentInfo(
            agent_id=agent_id,
            role=role,
            host="h",
            capabilities=caps if role == "worker" else frozenset(),
            heartbeat_interval=interval,
        )
        stored = self.run(self.registry.register(info))
        assert stored == self.now
        self.model[agent_id] = ModelAgent(role, info.capabilities, interval, self.now)

    @rule(agent_id=st.sampled_from(AGENT_IDS))
    def heartbeat(self, agent_id):
        ok = self.run(self.registry.heartbeat(agent_id))
        assert ok == (agent_id in self.model)
        if ok:
            agent = self.model[agent_id]
            agent.last_heartbeat = self.now
            agent.status = "Available"
            # Req 11.7: a heartbeat restores Available immediately.
            snap = self.run(self.registry.snapshot())
            assert snap.get(agent_id).status == "Available"

    @rule(agent_id=st.sampled_from(AGENT_IDS))
    def deregister(self, agent_id):
        removed = self.run(self.registry.deregister(agent_id))
        assert removed == (agent_id in self.model)
        self.model.pop(agent_id, None)

    @rule(dt=st.integers(min_value=0, max_value=20))
    def advance(self, dt):
        self.now += dt

    @precondition(lambda self: bool(self.model))
    @rule(data=st.data())
    def advance_to_boundary(self, data):
        """Jump to just before, exactly at, or past an agent's staleness deadline."""
        agent_id = data.draw(st.sampled_from(sorted(self.model)))
        agent = self.model[agent_id]
        deadline = agent.last_heartbeat + MISSED_INTERVALS * agent.interval
        target = deadline + data.draw(st.sampled_from([-1, 0, 1]))
        self.now = max(self.now, target)

    @rule()
    def sweep(self):
        before = self.model_advertised()
        marked = self.run(self.registry.sweep())

        expected_marked = set()
        for agent_id, agent in self.model.items():
            stale = self.now - agent.last_heartbeat >= MISSED_INTERVALS * agent.interval
            if stale and agent.status == "Available":
                expected_marked.add(agent_id)
                agent.status = "Unavailable"
        assert set(marked) == expected_marked

        snap = self.run(self.registry.snapshot())
        assert {a.agent_id for a in snap.agents} == set(self.model)
        for record in snap.agents:
            agent = self.model[record.agent_id]
            stale = self.now - record.last_heartbeat >= MISSED_INTERVALS * agent.interval
            # Unavailable iff now - last_heartbeat >= 3 x interval (Req 11.2).
            assert (record.status == "Unavailable") == stale, record
            assert record.status == agent.status

        after = snap.advertised_capabilities()
        assert after == self.model_advertised()
        # A Capability disappears only when no Available Worker declares it.
        for cap in before - after:
            assert not any(
                cap in r.capabilities
                for r in snap.agents
                if r.role == "worker" and r.status == "Available"
            )

    # --------------------------------------------------------------- invariants

    @invariant()
    def snapshot_matches_model(self):
        snap = self.run(self.registry.snapshot())
        assert {a.agent_id: a.status for a in snap.agents} == {
            k: v.status for k, v in self.model.items()
        }
        for record in snap.agents:
            assert record.last_heartbeat == self.model[record.agent_id].last_heartbeat


RegistryAvailabilityMachine.TestCase.settings = settings(
    max_examples=100, stateful_step_count=30, deadline=None
)
TestRegistryAvailability = RegistryAvailabilityMachine.TestCase
