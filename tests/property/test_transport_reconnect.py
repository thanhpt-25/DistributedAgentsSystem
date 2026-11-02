# Feature: distributed-agent-system, Property 31: Transport reconnect schedule
"""Property 31: Transport reconnect schedule.

For any maximum interval ``M`` in 1..300, the reconnect policy yields delays
``min(2^(k-1), M)`` for k = 1, 2, .... After a simulated reconnect, the fake
transport re-establishes every previously active subscription before
delivering any Message.

The in-memory transport uses :class:`FakeClock`, so backoff sleeps advance
time instantly and no example waits on a real clock.

**Validates: Requirements 8.6**
"""

from __future__ import annotations

import asyncio
import itertools
from datetime import UTC, datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from das.clock import FakeClock
from das.codec import new_message
from das.models.results import JobDispatchPayload
from das.transport.base import Delivery, Outcome, ReconnectPolicy
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

TS = datetime(2025, 1, 1, tzinfo=UTC)

# Integer and fractional caps across the whole allowed range.
_max_intervals = st.one_of(
    st.integers(min_value=1, max_value=300),
    st.floats(min_value=1.0, max_value=300.0, allow_nan=False, allow_infinity=False),
)


def _expected_delay(k: int, cap: float) -> float:
    return float(min(2 ** (k - 1), cap))


@settings(max_examples=200, deadline=None)
@given(cap=_max_intervals, attempts=st.integers(min_value=1, max_value=40))
def test_reconnect_policy_schedule(cap: float, attempts: int) -> None:
    policy = ReconnectPolicy(cap)
    delays = list(itertools.islice(policy.delays(), attempts))

    assert delays == [_expected_delay(k, cap) for k in range(1, attempts + 1)]
    assert [policy.delay(k) for k in range(1, attempts + 1)] == delays
    # Non-decreasing, starts at min(1, cap) = 1, never exceeds the cap.
    assert delays[0] == 1.0
    assert all(a <= b for a, b in itertools.pairwise(delays))
    assert all(d <= cap for d in delays)


def _dispatch(job_id: str):
    return new_message(
        JobDispatchPayload(
            request_id="r1",
            job_id=job_id,
            attempt=1,
            instruction="do it",
            required_capabilities=["research"],
        ),
        correlation_id="r1",
        message_id=f"dispatch-{job_id}",
        timestamp=TS,
    )


# One subscription spec: (destination index, cancelled before the drop?)
_sub_specs = st.lists(
    st.tuples(st.integers(min_value=0, max_value=3), st.booleans()),
    min_size=1,
    max_size=6,
)


@settings(max_examples=100, deadline=None)
@given(
    cap=_max_intervals,
    failures=st.integers(min_value=0, max_value=12),
    specs=_sub_specs,
    # Messages published per destination while the worker is disconnected.
    backlog=st.lists(st.integers(min_value=0, max_value=3), min_size=4, max_size=4),
)
def test_reconnect_restores_every_subscription_before_delivery(
    cap: float, failures: int, specs: list[tuple[int, bool]], backlog: list[int]
) -> None:
    async def scenario() -> None:
        clock = FakeClock()
        broker = MemoryBroker()
        sender = MemoryTransport(broker, agent_id="sender", clock=clock)
        worker = MemoryTransport(
            broker, agent_id="worker", clock=clock, reconnect_policy=ReconnectPolicy(cap)
        )
        await sender.start()
        await worker.start()

        violations: list[str] = []
        delivered: list[str] = []

        async def handler(delivery: Delivery) -> None:
            # At the moment of any delivery, every active subscription must be
            # attached to the broker and the delivery gate must be open.
            active = {s.id for s in worker.subscriptions}
            if not worker.registry.is_open or not active <= set(worker._consumers):
                violations.append(delivery.destination)
            delivered.append(delivery.destination)
            await worker.acknowledge(delivery, Outcome.ACK)

        subs = [await worker.subscribe(f"dest-{d}", handler) for d, _ in specs]
        for sub, (_, cancel) in zip(subs, specs, strict=True):
            if cancel:
                await sub.cancel()
        expected_active = [s.destination for s in subs if s.active]

        worker.simulate_drop()
        assert not worker.connected and not worker.registry.is_open

        served = {s.destination for s in subs if s.active}
        published = 0
        for d, count in enumerate(backlog):
            for i in range(count):
                await sender.publish(f"dest-{d}", _dispatch(f"j{d}-{i}"))
                published += 1 if f"dest-{d}" in served else 0
        await broker.settle()
        assert delivered == []  # nothing reaches a handler while down

        worker.events.clear()
        delays = await worker.reconnect_with_backoff(failures=failures)
        await broker.settle()

        # Delay schedule: one sleep per attempt following min(2^(k-1), M).
        expected = [_expected_delay(k, cap) for k in range(1, failures + 2)]
        assert delays == expected
        assert clock.sleeps == expected

        # Every previously active subscription (and no cancelled one) is
        # re-established, all before the gate reopens and before any delivery.
        kinds = [kind for kind, _ in worker.events]
        restored_at = kinds.index("restored")
        resubscribed = [dest for kind, dest in worker.events[:restored_at]]
        assert kinds[:restored_at] == ["resubscribe"] * restored_at
        assert resubscribed == expected_active
        assert "deliver" not in kinds[:restored_at]
        assert [s.destination for s in worker.subscriptions] == expected_active
        assert worker.connected and worker.registry.is_open

        # Queued Messages for served destinations are delivered after restore.
        assert violations == []
        assert len(delivered) == published
        assert kinds.count("deliver") == published

    asyncio.run(scenario())
