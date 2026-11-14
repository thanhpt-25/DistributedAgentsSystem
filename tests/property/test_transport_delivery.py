"""Property 2: delivery under any script of requeues, acknowledgements and rejects (das-scale-resilience task 13.3;
Phase 7 Req 6.2).

For any set of Messages, each with a script of zero to two requeues followed by an acknowledgement or a reject, every
Transport adapter delivers each Message at least once; the delivery count of the n-th delivery is n; a Message is never
delivered again after its acknowledgement or reject; and the dead-letter destination holds exactly the rejected
Messages, each once, with its reason. Run against the in-memory broker that models RabbitMQ's semantics, the HTTP
broker over fakeredis streams and the Kafka adapter over the in-memory cluster (the real RabbitMQ is covered by
``tests/integration/test_rabbitmq_broker.py``, the real Kafka by ``tests/integration/test_kafka_contract.py``)."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from das.transport.base import Delivery, Outcome
from tests.transport_contract import JOBS, job, until
from tests.unit.test_transport_behaviour import HttpRig, KafkaRig, MemoryRig

#: One Message: how many times it is requeued, then how it ends.
scripts = st.lists(
    st.tuples(st.integers(0, 2), st.sampled_from([Outcome.ACK, Outcome.REJECT])), min_size=1, max_size=5
)


async def scenario(make_rig: Callable[[], Any], script: list[tuple[int, Outcome]]) -> None:
    rig = make_rig()
    try:
        plan = {f"j{n}": [Outcome.REQUEUE] * requeues + [final] for n, (requeues, final) in enumerate(script)}
        remaining = {key: list(outcomes) for key, outcomes in plan.items()}
        deliveries: dict[str, list[Delivery]] = defaultdict(list)
        dead: list[Delivery] = []
        publisher, consumer = await rig.new("publisher"), await rig.new("consumer")

        async def work(delivery: Delivery) -> None:
            key = delivery.message.payload.job_id  # type: ignore[union-attr]
            deliveries[key].append(delivery)
            await consumer.acknowledge(delivery, remaining[key].pop(0))

        async def collect(delivery: Delivery) -> None:
            dead.append(delivery)
            await consumer.acknowledge(delivery)

        await consumer.subscribe(JOBS, work)
        await consumer.subscribe("dlq", collect)
        for n in range(len(script)):
            await publisher.publish(JOBS, job(n))

        rejected = [key for key, outcomes in plan.items() if outcomes[-1] is Outcome.REJECT]
        await until(lambda: all(not outcomes for outcomes in remaining.values()) and len(dead) >= len(rejected), 30)
        await asyncio.sleep(rig.redelivery_delay * 2 + 0.3)  # a Message that came back again would show now

        for key, outcomes in plan.items():
            got = deliveries[key]
            assert got, f"{key} was never delivered"
            assert len(got) == len(outcomes), f"{key}: {len(got)} deliveries for a script of {len(outcomes)}"
            assert [d.delivery_count for d in got] == list(range(len(outcomes))), f"{key}: delivery counts"
            assert [d.redelivered for d in got] == [n > 0 for n in range(len(outcomes))]
        dead_ids = sorted(d.message.payload.job_id for d in dead)  # type: ignore[union-attr]
        assert dead_ids == sorted(rejected), "the dead letters are exactly the rejected Messages, each once"
        assert all(d.headers["x-death-reason"] == "rejected" for d in dead)
    finally:
        await rig.close()


def run(make_rig: Callable[[], Any], script: list[tuple[int, Outcome]]) -> None:
    asyncio.run(scenario(make_rig, script))


FAST = settings(max_examples=60, deadline=None, suppress_health_check=list(HealthCheck))
SLOW = settings(max_examples=10, deadline=None, suppress_health_check=list(HealthCheck))


@FAST
@given(scripts)
def test_the_in_memory_broker_delivers_every_message_at_least_once_and_dead_letters_exactly_the_rejected(
    script: list[tuple[int, Outcome]],
) -> None:
    run(MemoryRig, script)


@SLOW
@given(scripts)
def test_the_http_transport_over_its_broker_does_the_same(script: list[tuple[int, Outcome]]) -> None:
    run(lambda: HttpRig(FakeRedis(server=FakeServer(), decode_responses=True)), script)


@SLOW
@given(scripts)
def test_the_kafka_transport_over_the_in_memory_cluster_does_the_same(script: list[tuple[int, Outcome]]) -> None:
    run(KafkaRig, script)
