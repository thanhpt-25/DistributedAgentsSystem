# Feature: distributed-agent-system, Property 26: Worker subscriptions match Capabilities
"""Property 26: Worker subscriptions match Capabilities.

For any Capability set of size 1..50, a started Worker subscribes to exactly the
destinations ``{jobs/c | c in Capabilities}``: one subscription per Capability, no
duplicates, no other ``jobs/`` destinations, all sharing the Worker's limiter. The
control transport carries only the Worker's own agent_id.

Runs the real Worker and AgentRegistry over MemoryTransport and a fresh in-process
fakeredis server per example.

**Validates: Requirements 5.2**
"""

from __future__ import annotations

import asyncio
from collections import Counter

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.registry import AgentRegistry
from das.worker.executor import JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport
from tests.strategies import capability_names

AGENT_ID = "worker-1"
PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)

capability_sets = st.frozensets(capability_names, min_size=1, max_size=50)


async def _check(caps: frozenset[str]) -> None:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    queue = MemoryTransport(MemoryBroker(), agent_id=AGENT_ID)
    control = MemoryTransport(MemoryBroker(), agent_id=f"{AGENT_ID}-control")
    provider = LLMProvider(FakeAdapter(), PARAMS, retry_count=0, timeout=10)
    worker = Worker(
        WorkerSettings(agent_id=AGENT_ID, host="host-a", capabilities=caps, concurrency_limit=2),
        queue=queue,
        control=control,
        registry=AgentRegistry(client),
        executor=JobExecutor(provider, AGENT_ID, job_timeout=30),
    )
    try:
        for t in (queue, control):
            await t.start()
        await worker.start()

        expected = {f"jobs/{c}" for c in caps}
        destinations = [s.destination for s in queue.subscriptions]
        # Set equality and multiset equality: exactly one subscription per Capability.
        assert set(destinations) == expected
        assert Counter(destinations) == Counter(expected)
        all_jobs = [
            s.destination
            for t in (queue, control)
            for s in t.subscriptions
            if s.destination.startswith("jobs/")
        ]
        assert sorted(all_jobs) == sorted(expected)
        assert all(s.options.limiter is worker.limiter for s in queue.subscriptions)
        assert set(worker.subscriptions) == caps
        assert [s.destination for s in control.subscriptions] == [AGENT_ID]
    finally:
        await worker.stop()  # also closes both transports
        await client.aclose()
    assert queue.subscriptions == [] and control.subscriptions == []


@settings(max_examples=100, deadline=None)
@given(capability_sets)
def test_worker_subscriptions_match_capabilities(caps: frozenset[str]) -> None:
    asyncio.run(_check(caps))
