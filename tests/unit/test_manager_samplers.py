"""Unit tests for QueueDepthSampler and RegistryGauges (task 11.2; Req 3.1, 3.2, 3.6, 3.7, 3.10)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

import pytest

from das.clock import FakeClock
from das.manager import samplers
from das.manager.samplers import (
    QueueDepthSampler,
    RegistryGauges,
    registry_counts,
    sampled_destinations,
    within,
)
from das.models import AgentRecord
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.registry.registry import RegistrySnapshot
from das.transport.base import QueueInspector, queue_name_for
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport


class ListLog:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict]] = []

    def warning(self, event: str, **fields) -> None:
        self.warnings.append((event, fields))

    def exception(self, event: str, **fields) -> None:  # pragma: no cover - not expected
        self.warnings.append((event, fields))


def agent(aid: str, role: str, status: str, caps: Iterable[str] = ()) -> AgentRecord:
    return AgentRecord(
        agent_id=aid, role=role, host="h", capabilities=frozenset(caps), status=status,
        last_heartbeat=0.0,
    )


class FakeRegistry:
    def __init__(self, agents: Iterable[AgentRecord] = ()) -> None:
        self.agents = tuple(agents)
        self.fail = False
        self.hang = False

    async def snapshot(self) -> RegistrySnapshot:
        if self.fail:
            raise ConnectionError("redis down")
        if self.hang:
            await asyncio.Event().wait()
        return RegistrySnapshot(agents=self.agents, server_time=0.0)


class FakeInspector:
    def __init__(self, depths: dict[str, int]) -> None:
        self.depths = depths
        self.failing: set[str] = set()
        self.hanging: set[str] = set()
        self.calls: list[str] = []

    async def ready_count(self, destination: str) -> int:
        self.calls.append(destination)
        if destination in self.failing:
            raise RuntimeError("broker error")
        if destination in self.hanging:
            await asyncio.Event().wait()
        return self.depths.get(destination, 0)


@pytest.fixture
def recorder() -> MetricsRecorder:
    return MetricsRecorder(clock=FakeClock(), log=ListLog())


@pytest.fixture
def log(monkeypatch) -> ListLog:
    fake = ListLog()
    monkeypatch.setattr(samplers, "log", fake)
    return fake


def value(rec: MetricsRecorder, name: str, labels: dict | None = None) -> float | None:
    return rec.registry.get_sample_value(name, labels or {})


# ---------------------------------------------------------------- helpers


def test_queue_names_match_broker_topology() -> None:
    assert queue_name_for("results") == "das.results"
    assert queue_name_for("dlq") == "das.dlq"
    assert queue_name_for("dlq-events") == "das.dlq.events"
    assert queue_name_for("jobs/web-search") == "das.jobs.web-search"


def test_sampled_destinations_cover_fixed_queues_and_capabilities() -> None:
    assert sampled_destinations({"b", "a"}) == ["results", "dlq", "jobs/a", "jobs/b"]
    assert sampled_destinations(()) == ["results", "dlq"]


async def test_within_returns_result_and_times_out_on_clock() -> None:
    clock = FakeClock()

    async def quick() -> int:
        return 7

    assert await within(clock, quick(), 5) == 7
    with pytest.raises(TimeoutError):
        await within(clock, asyncio.Event().wait(), 5)
    assert clock.sleeps[-1] == 5


def test_memory_transport_is_a_queue_inspector() -> None:
    assert isinstance(MemoryTransport(MemoryBroker()), QueueInspector)


# ---------------------------------------------------------------- queue depth


async def test_samples_results_dlq_and_advertised_capabilities(recorder, log) -> None:
    registry = FakeRegistry(
        [
            agent("w1", "worker", "Available", {"research"}),
            agent("w2", "worker", "Draining", {"code"}),  # not advertised
            agent("m1", "manager", "Available"),
        ]
    )
    inspector = FakeInspector({"results": 3, "dlq": 1, "jobs/research": 9})
    sampler = QueueDepthSampler(inspector, registry, ManagerMetrics(recorder), FakeClock())

    depths = await sampler.sample_once()

    assert depths == {"das.results": 3, "das.dlq": 1, "das.jobs.research": 9}
    assert inspector.calls == ["results", "dlq", "jobs/research"]
    assert value(recorder, "das_queue_depth", {"queue": "das.jobs.research"}) == 9
    assert value(recorder, "das_queue_depth", {"queue": "das.jobs.code"}) is None


async def test_failed_or_slow_queue_keeps_previous_value_and_counts(recorder, log) -> None:
    clock = FakeClock()
    registry = FakeRegistry([agent("w1", "worker", "Available", {"research"})])
    inspector = FakeInspector({"results": 3, "dlq": 1, "jobs/research": 9})
    sampler = QueueDepthSampler(inspector, registry, ManagerMetrics(recorder), clock)
    await sampler.sample_once()

    inspector.failing = {"results"}
    inspector.hanging = {"jobs/research"}
    inspector.depths["dlq"] = 4
    depths = await sampler.sample_once()

    assert depths == {"das.dlq": 4}  # other queues still sampled
    assert value(recorder, "das_queue_depth", {"queue": "das.results"}) == 3  # kept
    assert value(recorder, "das_queue_depth", {"queue": "das.jobs.research"}) == 9  # kept
    assert value(recorder, "das_queue_depth", {"queue": "das.dlq"}) == 4
    for queue in ("das.results", "das.jobs.research"):
        assert value(recorder, "das_queue_depth_sample_errors_total", {"queue": queue}) == 1
    assert value(recorder, "das_queue_depth_sample_errors_total", {"queue": "das.dlq"}) is None
    assert 5.0 in clock.sleeps  # the per-queue timeout ran on the injected clock
    assert [f["queue"] for e, f in log.warnings if e == "queue_depth_sample_failed"] == [
        "das.results",
        "das.jobs.research",
    ]


async def test_never_sampled_queue_has_no_series_and_warns_once_per_minute(recorder, log) -> None:
    clock = FakeClock()
    inspector = FakeInspector({})
    inspector.failing = {"dlq"}
    sampler = QueueDepthSampler(inspector, FakeRegistry(), ManagerMetrics(recorder), clock)

    start = clock.monotonic()
    for _ in range(3):
        await sampler.sample_once()
    assert clock.monotonic() - start < 60  # FakeClock timers advance time a little
    assert value(recorder, "das_queue_depth", {"queue": "das.dlq"}) is None
    assert value(recorder, "das_queue_depth_sample_errors_total", {"queue": "das.dlq"}) == 3
    assert len([e for e, _ in log.warnings if e == "queue_depth_sample_failed"]) == 1
    clock.advance(60)
    await sampler.sample_once()
    assert len([e for e, _ in log.warnings if e == "queue_depth_sample_failed"]) == 2


async def test_registry_failure_reuses_last_capabilities(recorder, log) -> None:
    registry = FakeRegistry([agent("w1", "worker", "Available", {"research"})])
    inspector = FakeInspector({"jobs/research": 2})
    sampler = QueueDepthSampler(inspector, registry, ManagerMetrics(recorder), FakeClock())
    await sampler.sample_once()
    registry.fail = True
    inspector.calls.clear()
    await sampler.sample_once()
    assert inspector.calls == ["results", "dlq", "jobs/research"]


async def test_sampler_works_with_memory_transport(recorder, log) -> None:
    broker = MemoryBroker()
    broker.publish_raw("results", b"x")
    broker.publish_raw("results", b"y")
    transport = MemoryTransport(broker)
    await transport.start()
    sampler = QueueDepthSampler(transport, FakeRegistry(), ManagerMetrics(recorder), FakeClock())
    assert await sampler.sample_once() == {"das.results": 2, "das.dlq": 0}


async def test_run_samples_immediately_then_every_interval(recorder, log) -> None:
    clock = FakeClock()
    inspector = FakeInspector({})
    sampler = QueueDepthSampler(
        inspector, FakeRegistry(), ManagerMetrics(recorder), clock, interval=15
    )
    task = asyncio.create_task(sampler.run())
    for _ in range(50):
        await asyncio.sleep(0)
        if inspector.calls.count("results") >= 3:
            break
    sampler.stop()
    await asyncio.wait_for(task, 1)
    assert inspector.calls[:2] == ["results", "dlq"]
    assert clock.sleeps.count(15) >= 2


# ---------------------------------------------------------------- registry gauges


def test_registry_counts_all_six_pairs_and_zero_capabilities() -> None:
    by_status, workers = registry_counts(
        [
            agent("w1", "worker", "Available", {"research", "code"}),
            agent("w2", "worker", "Unavailable", {"code", "math"}),
            agent("m1", "manager", "Draining"),
            agent("g1", "gateway", "Available"),
        ]
    )
    assert len(by_status) == 6
    assert by_status[("worker", "Available")] == 1
    assert by_status[("worker", "Unavailable")] == 1
    assert by_status[("manager", "Draining")] == 1
    assert sum(by_status.values()) == 3  # gateway not counted
    assert workers == {"research": 1, "code": 1, "math": 0}


async def test_registry_gauges_report_every_pair(recorder, log) -> None:
    registry = FakeRegistry(
        [
            agent("w1", "worker", "Available", {"research"}),
            agent("w2", "worker", "Draining", {"code"}),
        ]
    )
    gauges = RegistryGauges(registry, ManagerMetrics(recorder), FakeClock(), interval=5)
    assert await gauges.sample_once()
    for role in ("manager", "worker"):
        for status in ("Available", "Unavailable", "Draining"):
            expected = 1.0 if (role, status) in {("worker", "Available"), ("worker", "Draining")} else 0.0
            assert value(recorder, "das_registry_agents", {"role": role, "status": status}) == expected
    assert value(recorder, "das_capability_workers", {"capability": "research"}) == 1
    assert value(recorder, "das_capability_workers", {"capability": "code"}) == 0


async def test_capability_no_longer_declared_drops_to_zero(recorder, log) -> None:
    registry = FakeRegistry([agent("w1", "worker", "Available", {"research"})])
    gauges = RegistryGauges(registry, ManagerMetrics(recorder), FakeClock(), interval=5)
    await gauges.sample_once()
    registry.agents = ()
    await gauges.sample_once()
    assert value(recorder, "das_capability_workers", {"capability": "research"}) == 0


async def test_registry_failure_keeps_gauges_counts_and_rate_limits(recorder, log) -> None:
    clock = FakeClock()
    registry = FakeRegistry([agent("w1", "worker", "Available", {"research"})])
    gauges = RegistryGauges(registry, ManagerMetrics(recorder), clock, interval=5)
    await gauges.sample_once()

    registry.fail = True
    assert not await gauges.sample_once()
    registry.fail, registry.hang = False, True
    assert not await gauges.sample_once()  # > 2 s on the injected clock

    assert 2.0 in clock.sleeps
    assert value(recorder, "das_registry_sample_errors_total") == 2
    assert value(recorder, "das_registry_agents", {"role": "worker", "status": "Available"}) == 1
    assert value(recorder, "das_capability_workers", {"capability": "research"}) == 1
    assert len([e for e, _ in log.warnings if e == "registry_sample_failed"]) == 1
