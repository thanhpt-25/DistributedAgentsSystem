"""Unit tests for the Agent_Registry and pure availability logic (task 10.1)."""

from __future__ import annotations

import time

import pytest
from hypothesis import given

from das.models import AgentRecord
from das.registry import (
    AGENTS_KEY,
    AgentInfo,
    AgentRegistry,
    RegistrySnapshot,
    advertised_capabilities,
    agent_key,
    capable_routing_capabilities,
    compute_status,
)
from tests.strategies import registry_snapshots


def rec(agent_id: str, caps: set[str], status: str = "Available", role: str = "worker"):
    return AgentRecord(
        agent_id=agent_id,
        role=role,
        host="h",
        capabilities=frozenset(caps),
        status=status,
        last_heartbeat=0.0,
    )


# ------------------------------------------------------------------ pure logic


def test_compute_status_threshold_is_three_intervals():
    assert compute_status(100.0, 5.0, 114.999) == "Available"
    assert compute_status(100.0, 5.0, 115.0) == "Unavailable"
    assert compute_status(100.0, 5.0, 200.0) == "Unavailable"
    with pytest.raises(ValueError):
        compute_status(0.0, 0.0, 1.0)


def test_advertised_capabilities_union_of_available_workers_only():
    agents = [
        rec("w1", {"research", "code"}),
        rec("w2", {"code"}, status="Unavailable"),
        rec("w3", {"summarize"}, status="Unavailable"),
        rec("m1", {"admin"}, role="manager"),
    ]
    assert advertised_capabilities(agents) == {"research", "code"}
    assert advertised_capabilities([]) == frozenset()


def test_dead_worker_drops_only_unshared_capabilities():
    alive = rec("w1", {"code"})
    dead = rec("w2", {"code", "research"}, status="Unavailable")
    assert advertised_capabilities([alive, dead]) == {"code"}


def test_capable_routing_capabilities():
    agents = [
        rec("w1", {"research", "code-review"}),
        rec("w2", {"research"}),  # lacks code-review: research queue unsafe
        rec("w3", {"code-review"}, status="Unavailable"),  # ignored
    ]
    assert capable_routing_capabilities(frozenset({"research", "code-review"}), agents) == {
        "code-review"
    }
    assert capable_routing_capabilities(frozenset({"research"}), agents) == {"research"}
    assert capable_routing_capabilities(frozenset({"summarize"}), agents) == frozenset()
    # No capable worker at all.
    assert capable_routing_capabilities(frozenset({"research", "x"}), agents) == frozenset()


@given(registry_snapshots())
def test_routing_capabilities_sound_on_generated_snapshots(snap: RegistrySnapshot):
    available = [a for a in snap.agents if a.role == "worker" and a.status == "Available"]
    for a in available:
        required = a.capabilities
        chosen = snap.capable_routing_capabilities(required)
        assert chosen <= required
        for cap in chosen:
            holders = [w for w in available if cap in w.capabilities]
            assert holders and all(required <= w.capabilities for w in holders)
    assert snap.advertised_capabilities() == frozenset().union(
        *(a.capabilities for a in available)
    )


# ------------------------------------------------------------------ Redis registry


class ManualTime:
    def __init__(self, now: float = 1_700_000_000.0) -> None:
        self.now = now

    async def __call__(self) -> float:
        return self.now


WORKER = AgentInfo(
    agent_id="w1",
    role="worker",
    host="host-a",
    grpc_endpoint="host-a:50051",
    capabilities=frozenset({"research", "code"}),
    concurrency_limit=4,
    heartbeat_interval=5,
)


async def test_register_snapshot_roundtrip_uses_server_time(fake_redis):
    reg = AgentRegistry(fake_redis)
    before = time.time()
    await reg.register(WORKER)
    await reg.register(AgentInfo(agent_id="m1", role="manager", host="host-b"))
    snap = await reg.snapshot()
    w = snap.get("w1")
    assert w is not None and w.status == "Available"
    assert w.capabilities == {"research", "code"}
    assert w.grpc_endpoint == "host-a:50051" and w.concurrency_limit == 4
    assert w.host == "host-a" and w.role == "worker"
    m = snap.get("m1")
    assert m is not None and m.grpc_endpoint is None and m.concurrency_limit is None
    assert abs(snap.server_time - before) < 5
    assert abs(w.last_heartbeat - before) < 5
    assert snap.advertised_capabilities() == {"research", "code"}


async def test_sweep_marks_stale_and_heartbeat_restores(fake_redis):
    clock = ManualTime()
    reg = AgentRegistry(fake_redis, time_source=clock)
    await reg.register(WORKER)
    await reg.register(
        AgentInfo(agent_id="w2", role="worker", host="h", capabilities=frozenset({"code"}))
    )

    clock.now += 14.9
    await reg.heartbeat("w2")
    assert await reg.sweep() == []

    clock.now += 0.1  # w1 now exactly 3 intervals old
    assert await reg.sweep() == ["w1"]
    snap = await reg.snapshot()
    assert snap.get("w1").status == "Unavailable"
    assert snap.advertised_capabilities() == {"code"}  # research dropped, code kept
    assert await reg.sweep() == []  # already marked

    assert await reg.heartbeat("w1") is True
    snap = await reg.snapshot()
    assert snap.get("w1").status == "Available"
    assert snap.get("w1").last_heartbeat == clock.now
    assert snap.advertised_capabilities() == {"research", "code"}


async def test_sweep_with_explicit_now(fake_redis):
    clock = ManualTime(1000.0)
    reg = AgentRegistry(fake_redis, time_source=clock)
    await reg.register(WORKER)
    assert await reg.sweep(now=1014.0) == []
    assert await reg.sweep(now=1015.0) == ["w1"]


async def test_heartbeat_unknown_agent_and_deregister(fake_redis):
    reg = AgentRegistry(fake_redis)
    assert await reg.heartbeat("ghost") is False
    assert (await reg.snapshot()).agents == ()

    await reg.register(WORKER)
    assert await reg.deregister("w1") is True
    assert await reg.deregister("w1") is False
    assert (await reg.snapshot()).agents == ()
    assert await fake_redis.exists(agent_key("w1"), AGENTS_KEY) == 0


async def test_reregister_resets_to_available(fake_redis):
    clock = ManualTime()
    reg = AgentRegistry(fake_redis, time_source=clock)
    await reg.register(WORKER)
    clock.now += 100
    assert await reg.sweep() == ["w1"]
    await reg.register(WORKER.model_copy(update={"capabilities": frozenset({"code"})}))
    w = (await reg.snapshot()).get("w1")
    assert w.status == "Available" and w.capabilities == {"code"}


async def test_sweep_uses_server_time_by_default(fake_redis):
    reg = AgentRegistry(fake_redis)
    await reg.register(WORKER)
    assert await reg.sweep() == []
    # Backdate the heartbeat; server-time sweep must notice.
    await fake_redis.hset(agent_key("w1"), "last_heartbeat", repr(time.time() - 60))
    assert await reg.sweep() == ["w1"]


# ------------------------------------------------------------------ Draining / in_flight (task 9.5)


def test_draining_worker_advertises_nothing():
    agents = [
        rec("w1", {"code", "research"}, status="Draining"),
        rec("w2", {"code"}),
    ]
    # research only held by a Draining worker: dropped (Req 17.6).
    assert advertised_capabilities(agents) == {"code"}
    assert capable_routing_capabilities(frozenset({"research"}), agents) == frozenset()


def test_agent_record_v1_json_defaults_in_flight():
    v1 = '{"agent_id":"w1","role":"worker","host":"h","status":"Available","last_heartbeat":1.0}'
    r = AgentRecord.model_validate_json(v1)
    assert r.in_flight == 0
    assert AgentRecord.model_validate({**r.model_dump(), "status": "Draining"}).status == "Draining"
    with pytest.raises(ValueError):
        AgentRecord.model_validate({**r.model_dump(), "in_flight": -1})


async def test_heartbeat_defaults_and_desired_status(fake_redis):
    clock = ManualTime()
    reg = AgentRegistry(fake_redis, time_source=clock)
    await reg.register(WORKER)
    assert (await reg.snapshot()).get("w1").in_flight == 0

    assert await reg.heartbeat("w1", desired_status="Draining", in_flight=3)
    w = (await reg.snapshot()).get("w1")
    assert w.status == "Draining" and w.in_flight == 3
    assert (await reg.snapshot()).advertised_capabilities() == frozenset()

    # Default call (v1 callers) goes back to Available with in_flight 0.
    assert await reg.heartbeat("w1")
    w = (await reg.snapshot()).get("w1")
    assert w.status == "Available" and w.in_flight == 0

    with pytest.raises(ValueError):
        await reg.heartbeat("w1", desired_status="Unavailable")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await reg.heartbeat("w1", in_flight=-1)


async def test_set_status_drain_and_undrain(fake_redis):
    clock = ManualTime()
    reg = AgentRegistry(fake_redis, time_source=clock)
    await reg.register(WORKER)
    hb_before = (await reg.snapshot()).get("w1").last_heartbeat

    clock.now += 2
    assert await reg.set_status("w1", "Draining", in_flight=2)
    w = (await reg.snapshot()).get("w1")
    assert w.status == "Draining" and w.in_flight == 2
    assert w.last_heartbeat == hb_before  # not a heartbeat

    assert await reg.set_status("w1", "Draining")  # idempotent, in_flight kept
    assert (await reg.snapshot()).get("w1").in_flight == 2

    assert await reg.set_status("w1", "Available", in_flight=0)
    w = (await reg.snapshot()).get("w1")
    assert w.status == "Available" and w.in_flight == 0

    assert await reg.set_status("ghost", "Draining") is False
    assert await fake_redis.exists(agent_key("ghost")) == 0
    with pytest.raises(ValueError):
        await reg.set_status("w1", "Unavailable")  # type: ignore[arg-type]


async def test_sweep_marks_stale_draining_unavailable(fake_redis):
    clock = ManualTime()
    reg = AgentRegistry(fake_redis, time_source=clock)
    await reg.register(WORKER)
    await reg.heartbeat("w1", desired_status="Draining", in_flight=1)
    clock.now += 14.9
    assert await reg.sweep() == []
    assert (await reg.snapshot()).get("w1").status == "Draining"
    clock.now += 0.1
    assert await reg.sweep() == ["w1"]
    assert (await reg.snapshot()).get("w1").status == "Unavailable"


async def test_reregister_after_drain_is_available(fake_redis):
    reg = AgentRegistry(fake_redis)
    await reg.register(WORKER)
    await reg.set_status("w1", "Draining", in_flight=4)
    await reg.register(WORKER)  # restart (Req 17.16)
    w = (await reg.snapshot()).get("w1")
    assert w.status == "Available" and w.in_flight == 0


async def test_snapshot_reads_v1_hash_without_in_flight(fake_redis):
    reg = AgentRegistry(fake_redis)
    await reg.register(WORKER)
    await fake_redis.hdel(agent_key("w1"), "in_flight")
    assert (await reg.snapshot()).get("w1").in_flight == 0
