# Feature: distributed-agent-system, Property 27: Worker concurrency limit
"""Property 27: Worker concurrency limit.

For any concurrency limit ``k`` in 1..10, 1..5 Capabilities, and ``N`` Job
dispatches spread across those Capability queues, the Worker never executes
(or holds unacknowledged) more than ``k`` Jobs at once, across all its
subscriptions combined. While it is saturated the unconsumed dispatches stay
pending in the broker, and whenever fewer than ``k`` Jobs are executing and
work is queued, a new Job is taken (here: as soon as the freed slot is
released, before the next simulated completion). Every Job completes once the
blocked executions are released in an arbitrary order.

The executor stub blocks each Job on its own ``asyncio.Event``; the test
releases one currently executing Job at a time, chosen by Hypothesis (all
choices are drawn before the event loop starts).

**Validates: Requirements 5.3**
"""

from __future__ import annotations

import asyncio

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.codec import new_message
from das.models.results import JobDispatchPayload, JobResultPayload, TokenUsage
from das.registry import AgentRegistry
from das.worker.worker import Worker, WorkerSettings, jobs_destination
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

WORKER_ID = "worker-1"


class IdleClock:
    """Clock whose ``sleep`` never returns, so the heartbeat stays parked."""

    def now(self) -> float:
        return 0.0

    def monotonic(self) -> float:
        return 0.0

    async def sleep(self, seconds: float) -> None:
        await asyncio.Event().wait()


class BlockingExecutor:
    """``ExecutorLike`` stub: each Job blocks until the test releases it."""

    def __init__(self) -> None:
        self.gates: dict[str, asyncio.Event] = {}
        self.executing: set[str] = set()
        self.started: list[str] = []
        self.max_concurrent = 0

    async def execute(self, job: JobDispatchPayload, cancel: asyncio.Event) -> JobResultPayload:
        gate = self.gates.setdefault(job.job_id, asyncio.Event())
        self.executing.add(job.job_id)
        self.started.append(job.job_id)
        self.max_concurrent = max(self.max_concurrent, len(self.executing))
        try:
            await gate.wait()
        finally:
            self.executing.discard(job.job_id)
        return JobResultPayload(
            request_id=job.request_id,
            job_id=job.job_id,
            attempt=job.attempt,
            worker_id=WORKER_ID,
            status="Succeeded",
            content=f"done {job.job_id}",
            error=None,
            usage=TokenUsage.zero(),
            duration_ms=1,
        )

    def release(self, job_id: str) -> None:
        self.gates.setdefault(job_id, asyncio.Event()).set()


async def _until(predicate, timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


async def _quiesce(rounds: int = 25) -> None:
    """Yield enough times for any pending deliveries/handlers to run."""
    for _ in range(rounds):
        await asyncio.sleep(0)


@settings(max_examples=100, deadline=None)
@given(
    limit=st.integers(min_value=1, max_value=10),
    cap_count=st.integers(min_value=1, max_value=5),
    data=st.data(),
)
def test_worker_never_exceeds_concurrency_limit(limit: int, cap_count: int, data) -> None:
    caps = [f"cap-{i}" for i in range(cap_count)]
    # Which Capability queue each of the N dispatches goes to.
    targets = data.draw(
        st.lists(st.integers(min_value=0, max_value=cap_count - 1), min_size=0, max_size=20),
        label="targets",
    )
    n = len(targets)
    # Release choices, drawn up front (never inside the event loop): at step i
    # the Job released is sorted(executing)[picks[i] % len(executing)].
    picks = data.draw(
        st.lists(st.integers(min_value=0, max_value=limit - 1), min_size=n, max_size=n),
        label="picks",
    )

    async def scenario() -> None:
        redis = FakeRedis(server=FakeServer(), decode_responses=True)
        broker = MemoryBroker()
        queue = MemoryTransport(broker, agent_id=WORKER_ID)
        control = MemoryTransport(MemoryBroker(), agent_id=f"{WORKER_ID}-control")
        manager = MemoryTransport(broker, agent_id="manager-1")
        for t in (queue, control, manager):
            await t.start()
        executor = BlockingExecutor()
        worker = Worker(
            WorkerSettings(
                agent_id=WORKER_ID,
                host="host-a",
                capabilities=frozenset(caps),
                concurrency_limit=limit,
                shutdown_timeout=1.0,  # bounds teardown if an assertion fails mid-run
            ),
            queue=queue,
            control=control,
            registry=AgentRegistry(redis),
            executor=executor,
            clock=IdleClock(),
        )
        destinations = [jobs_destination(c) for c in caps]

        def unacked() -> int:
            return sum(broker.unacked_count(d) for d in destinations)

        def pending() -> int:
            return sum(len(broker.pending(d)) for d in destinations)

        max_unacked = 0
        max_slots = 0

        def observe() -> None:
            nonlocal max_unacked, max_slots
            max_unacked = max(max_unacked, unacked())
            max_slots = max(max_slots, worker.limiter.in_use)
            assert len(executor.executing) <= limit
            assert unacked() <= limit and worker.limiter.in_use <= limit

        await worker.start()
        try:
            for i, target in enumerate(targets):
                job_id = f"job-{i}"
                payload = JobDispatchPayload(
                    request_id="req-1",
                    job_id=job_id,
                    attempt=1,
                    instruction="do it",
                    required_capabilities=[caps[target]],
                )
                await manager.publish(
                    jobs_destination(caps[target]), new_message(payload, correlation_id="req-1")
                )
                observe()

            # Saturated (or all taken): exactly min(k, N) executing, rest pending.
            first = min(limit, n)
            await _until(lambda: len(executor.executing) == first)
            await _quiesce()
            observe()
            assert len(executor.executing) == first
            assert unacked() == first
            assert pending() == n - first
            assert broker.pending("results") == []

            # Release currently executing Jobs one at a time, in an arbitrary order.
            completed = 0
            while completed < n:
                running = sorted(executor.executing)
                executor.release(running[picks[completed] % len(running)])
                completed += 1
                remaining = n - completed
                expected = min(limit, remaining)
                await _until(
                    lambda: len(broker.pending("results")) == completed
                    and len(executor.executing) == expected
                    and unacked() == expected
                )
                await _quiesce()
                observe()
                # Work-conserving: a freed slot is refilled while work is queued.
                assert len(executor.executing) == expected
                assert pending() == remaining - expected

            assert executor.max_concurrent == min(limit, n)
            assert max_unacked <= limit and max_slots <= limit
            assert sorted(executor.started) == sorted(f"job-{i}" for i in range(n))
            assert len(set(executor.started)) == n  # each Job executed exactly once
            results = broker.pending_messages("results")
            assert sorted(m.payload.job_id for m in results) == sorted(executor.started)
            assert pending() == 0 and unacked() == 0
            assert worker.limiter.in_use == 0 and worker.in_flight == 0
        finally:
            await worker.stop()
            await redis.aclose()

    asyncio.run(scenario())
