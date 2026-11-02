# Feature: das-observability-operations, Property 1: Job transition được đếm tối đa một lần
"""Property 1: Job transitions are counted at most once.

For any 2..10 Manager_Agents and any interleaving of Lease acquisition, clock advances
(expiring Leases), driver progress (``commit_plan`` / ``apply_transition``), Job_Results,
Gateway cancels, and State_Store replies lost after the write succeeded, executed on
fakeredis with the real Lua scripts:

- the total ``das_job_transitions_total`` over every Manager is <= the number of Job
  transition entries in ``das:req:{rid}:transitions`` and no entry is counted twice;
- a write answered ``LEASE_LOST`` / ``REQUEST_TERMINAL`` / timeout / connection error
  changes neither the counter nor ``das_job_attempts``;
- every write answered ``OK`` adds exactly the entries it created (same capability and
  state), and one ``das_job_attempts`` observation per terminal entry.

Each Manager is a real ``RequestDriver`` advanced one production loop iteration per step
(``_wait`` / ``_finalize`` are the only overrides: they pause the driver). Faults are
injected in ``StateStore._run`` so the Lua script really runs (or not) underneath.
Redis ``TIME`` follows a fake server clock patched into fakeredis.

**Validates: Requirements 2.4, 2.5, 2.6**
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any
from unittest import mock

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from fakeredis.commands_mixins import server_mixin
from hypothesis import HealthCheck, event, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, precondition, rule
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from das.clock import FakeClock
from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.manager.driver import DriverDeps, RequestDriver, _Stop
from das.manager.planner import Planner, PlannerConfig
from das.manager.summarizer import Summarizer
from das.models import JobRecord, JobResultPayload, JobState, Lease, TokenUsage
from das.models.agent import AgentRecord
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.registry import RegistrySnapshot
from das.state import StateStore, StoreStatus, keys
from tests.fakes.llm_adapter import FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

START = 1_700_000_000.0
PARAMS = ModelParams(model="m-1", temperature=0.0, max_output_tokens=256)
#: Job_ID -> routing Capability (each Job has a single Capability).
CAPS = {"a": "search", "b": "write", "c": "search"}
PLAN_JSON = json.dumps(
    {
        "jobs": [
            {"job_id": "a", "instruction": "find facts", "capabilities": ["search"]},
            {"job_id": "b", "instruction": "write", "capabilities": ["write"],
             "depends_on": ["a"]},
            {"job_id": "c", "instruction": "more facts", "capabilities": ["search"]},
        ]
    }
)
COUNTED_WRITES = frozenset({"commit_plan", "apply_transition"})
TERMINAL = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.SKIPPED, JobState.CANCELLED})
FAULTS = (None, None, None, "drop", "lost_conn", "lost_timeout", "cancel")


class _Log:
    def warning(self, *_a: object, **_k: object) -> None: ...


class _Pause(Exception):
    """The driver reached the end of one loop iteration (or its final write)."""


class SteppedDriver(RequestDriver):
    """A production driver that pauses instead of sleeping or finalizing."""

    async def _wait(self, timeout: float) -> None:
        raise _Pause

    async def _finalize(self, finish: object) -> None:  # type: ignore[override]
        raise _Pause


class StubKeeper:
    """Never notices loss by itself: stale owners keep writing and are fenced by Lua."""

    def __init__(self, store: StateStore, lease: Lease) -> None:
        self._store, self.lease = store, lease
        self.is_lost = False

    async def release(self) -> None:
        await self._store.release_lease(self.lease)

    def stop_renewing(self) -> None: ...


class StaticRegistry:
    def __init__(self, machine: JobTransitionMachine) -> None:
        self._m = machine

    async def snapshot(self) -> RegistrySnapshot:
        worker = AgentRecord(agent_id="w1", role="worker", host="h",
                             capabilities=frozenset(CAPS.values()), status="Available",
                             last_heartbeat=self._m.now)
        return RegistrySnapshot(agents=(worker,), server_time=self._m.now)


class FaultyStore(StateStore):
    """Records the Job entries each write created and injects one fault on request.

    ``drop``: connection error before anything is sent. ``lost_conn`` / ``lost_timeout``:
    the script ran (and possibly wrote), then the reply is lost. ``cancel``: a Gateway
    cancel lands just before the script runs (it then answers ``REQUEST_TERMINAL``).
    """

    def __init__(self, client: Any, rid: str) -> None:
        super().__init__(client)
        self.rid = rid
        self.inject: str | None = None
        self.acked: list[tuple[int, dict[str, Any]]] = []  # OK-answered counted writes
        self.lost: list[tuple[int, dict[str, Any]]] = []
        self.rejected_writes = 0

    async def _entries(self) -> list[str]:
        return list(await self.client.lrange(keys.transitions(self.rid), 0, -1))

    async def _run(self, name: str, keys_: Sequence[str], args: Sequence[Any]) -> list[Any]:
        if name not in COUNTED_WRITES:
            return await super()._run(name, keys_, args)
        fault, self.inject = self.inject, None
        if fault == "drop":
            raise RedisConnectionError("injected: not sent")
        if fault == "cancel":  # Gateway cancel races in between status check and write
            await self.cancel_request(self.rid)
        before = await self._entries()
        reply = await super()._run(name, keys_, args)
        after = await self._entries()
        assert after[: len(before)] == before  # append-only
        created = [
            (len(before) + i, data)
            for i, raw in enumerate(after[len(before):])
            if (data := json.loads(raw)).get("scope") != "request"
        ]
        status = StoreStatus(str(reply[0]))
        if status is not StoreStatus.OK:
            assert not created, (name, status, created)  # rejected writes write nothing
            self.rejected_writes += 1
        event(f"{name} -> {status.value}" + (f" ({fault})" if fault else ""))
        if fault and created:
            event("reply lost after entries were written")
        if fault == "lost_conn":
            self.lost += created
            raise RedisConnectionError("injected: reply lost")
        if fault == "lost_timeout":
            self.lost += created
            raise RedisTimeoutError("injected: reply lost")
        self.acked += created
        return reply


def _transitions(rec: MetricsRecorder) -> Counter[tuple[str, str]]:
    out: Counter[tuple[str, str]] = Counter()
    for family in rec.registry.collect():
        if family.name == "das_job_transitions":
            for s in family.samples:
                if s.name.endswith("_total") and s.value:
                    out[(s.labels["capability"], s.labels["state"])] += int(s.value)
    return out


def _attempts(rec: MetricsRecorder) -> tuple[Counter[str], Counter[str]]:
    count: Counter[str] = Counter()
    total: Counter[str] = Counter()
    for family in rec.registry.collect():
        if family.name == "das_job_attempts":
            for s in family.samples:
                if s.name.endswith("_count") and s.value:
                    count[s.labels["capability"]] += int(s.value)
                elif s.name.endswith("_sum") and s.value:
                    total[s.labels["capability"]] += int(s.value)
    return count, total


class JobTransitionMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.now = START
        self._patch = mock.patch.object(
            server_mixin, "time", SimpleNamespace(time=lambda: self.now)
        )
        self._patch.start()
        self.loop = asyncio.new_event_loop()
        self.redis = FakeRedis(server=FakeServer(), decode_responses=True)
        self.gateway = StateStore(self.redis)
        self.rid = self.run(self.gateway.create_request("hello"))
        self.registry = StaticRegistry(self)
        self.broker = MemoryBroker(("dlq",))
        self.managers: list[str] = []
        self.stores: dict[str, FaultyStore] = {}
        self.recorders: dict[str, MetricsRecorder] = {}
        self.transports: dict[str, MemoryTransport] = {}
        # Live drivers: (driver, started). Dropped when the driver stops or pauses on
        # its final write.
        self.drivers: dict[str, list[Any]] = {}

    def run(self, coro):
        return self.loop.run_until_complete(coro)

    def teardown(self) -> None:
        try:
            self.run(self.redis.aclose())
            self.loop.close()
        finally:
            self._patch.stop()

    # ------------------------------------------------------------------ helpers
    def _driver(self, m: str, lease: Lease) -> SteppedDriver:
        store = self.stores[m]
        deps = DriverDeps(
            store=store,
            registry=self.registry,
            transport=self.transports[m],
            planner=Planner(LLMProvider(FakeAdapter(default=PLAN_JSON), PARAMS, retry_count=0)),
            summarizer=Summarizer(LLMProvider(FakeAdapter(default="sum"), PARAMS, retry_count=0)),
            planner_config=PlannerConfig(planning_retries=0),
            clock=FakeClock(),
            tick_period=0.01,
            metrics=ManagerMetrics(self.recorders[m]),
        )
        return SteppedDriver(lease, StubKeeper(store, lease), deps)  # type: ignore[arg-type]

    async def _jobs(self) -> dict[str, JobRecord]:
        raw = await self.redis.hgetall(keys.jobs(self.rid))
        return {k: JobRecord.model_validate_json(v) for k, v in raw.items()}

    # ------------------------------------------------------------------ rules
    @initialize(n=st.integers(min_value=2, max_value=10))
    def make_managers(self, n: int) -> None:
        self.managers = [f"m{i}" for i in range(n)]
        for m in self.managers:
            self.stores[m] = FaultyStore(self.redis, self.rid)
            self.recorders[m] = MetricsRecorder(clock=FakeClock(), log=_Log())
            self.transports[m] = MemoryTransport(self.broker, agent_id=m)
            self.run(self.transports[m].start())

    @rule(data=st.data(), duration=st.sampled_from([1.0, 2.0, 5.0, 30.0]))
    def acquire(self, data, duration: float) -> None:
        m = data.draw(st.sampled_from(self.managers), label="manager")
        res = self.run(self.stores[m].acquire_lease(self.rid, m, duration))
        if res.ok:
            self.drivers[m] = [self._driver(m, res.lease), False]  # replaces a stale one

    @precondition(lambda self: self.drivers)
    @rule(data=st.data(), fault=st.sampled_from(FAULTS))
    def drive(self, data, fault: str | None) -> None:
        """One production loop iteration of a (possibly stale) Manager's driver."""
        m = data.draw(st.sampled_from(sorted(self.drivers)), label="manager")
        driver, started = self.drivers[m]
        self.stores[m].inject = fault
        try:
            self.run(driver._loop() if started else driver._run())
        except _Pause:
            self.drivers[m][1] = True
        except _Stop:  # lease lost, Request terminal, abandoned
            del self.drivers[m]
        else:  # _loop returned: the driver is about to finalize
            del self.drivers[m]
        finally:
            self.stores[m].inject = None

    @rule(data=st.data(), ok=st.booleans(), stale=st.booleans())
    def deliver_result(self, data, ok: bool, stale: bool) -> None:
        jobs = self.run(self._jobs())
        live = sorted(j for j, r in jobs.items() if r.attempt >= 1)
        if not live:
            return
        job_id = data.draw(st.sampled_from(live), label="job")
        attempt = jobs[job_id].attempt - (1 if stale and jobs[job_id].attempt > 1 else 0)
        result = JobResultPayload(
            request_id=self.rid, job_id=job_id, attempt=attempt, worker_id="w1",
            status="Succeeded" if ok else "Failed", content=f"out-{job_id}",
            error=None if ok else "boom", usage=TokenUsage(input_tokens=1, output_tokens=1),
            duration_ms=5,
        )
        self.run(self.gateway.record_result_inbox(self.rid, result))

    @rule(dt=st.integers(min_value=1, max_value=24).map(lambda q: q * 0.25))
    def advance_clock(self, dt: float) -> None:
        self.now += dt

    @rule(roll=st.integers(min_value=0, max_value=9))
    def gateway_cancel(self, roll: int) -> None:
        if roll == 0:  # rare, so most runs get far before writes start hitting REQUEST_TERMINAL
            self.run(self.gateway.cancel_request(self.rid))

    # ------------------------------------------------------------------ invariants
    @invariant()
    def counted_exactly_the_acknowledged_entries(self) -> None:
        for m in self.managers:
            store, rec = self.stores[m], self.recorders[m]
            acked = [data for _, data in store.acked]
            expected = Counter((CAPS[d["job_id"]], d["to_state"]) for d in acked)
            assert _transitions(rec) == expected, (m, store.lost)
            terminal = [d for d in acked if JobState(d["to_state"]) in TERMINAL]
            if terminal:
                event("terminal entries counted")
            count, total = _attempts(rec)
            assert count == Counter(CAPS[d["job_id"]] for d in terminal), m
            exp_total: Counter[str] = Counter()
            for d in terminal:
                exp_total[CAPS[d["job_id"]]] += d["attempt"]
            assert +total == +exp_total, m

    @invariant()
    def counted_at_most_once_overall(self) -> None:
        raw = self.run(self.redis.lrange(keys.transitions(self.rid), 0, -1))
        stored = sum(1 for e in raw if json.loads(e).get("scope") != "request")
        indexes = [i for s in self.stores.values() for i, _ in s.acked]
        assert len(indexes) == len(set(indexes))  # no entry counted twice
        counted = sum(sum(_transitions(r).values()) for r in self.recorders.values())
        assert counted <= stored


JobTransitionMachine.TestCase.settings = settings(
    max_examples=200,
    stateful_step_count=80,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
TestJobTransitionCountedAtMostOnce = JobTransitionMachine.TestCase
