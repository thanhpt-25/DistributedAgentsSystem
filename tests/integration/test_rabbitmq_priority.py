"""Message priority against a real RabbitMQ 4.1 (das-multi-tenant task 6.5; Phase 5 Req 7.5, 7.6).

The transport turns the reserved ``das-priority: high`` header into the AMQP ``priority`` property 9 on the existing
quorum queues. The spike behind this design (plan section 9, G0a) measured how the broker treats it; these tests pin
that behaviour so a broker upgrade that changes it fails here and not in production:

* delivery order from ``basic.get``: ``high`` overtakes a ``normal`` backlog about 2:1 and ``normal`` never starves;
* messages without the header stay FIFO;
* a requeued ``high`` message is redelivered ahead of the backlog;
* a dead-lettered message keeps its priority;
* no queue is declared with ``x-max-priority`` (a quorum queue refuses it).

The module is skipped when Docker is not reachable."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from das.transport.base import PRIORITY_HEADER, PRIORITY_HIGH, Delivery, Outcome
from das.transport.rabbitmq import HIGH_AMQP_PRIORITY
from tests.integration import test_rabbitmq_broker as base
from tests.integration.test_rabbitmq_broker import PASSWORD, USER, Broker, Inbox, cap, dispatch, get_queue, wait_for

pytestmark = pytest.mark.integration

# The fixtures of the Phase 1 broker tests: one throwaway container for this module, and a transport factory.
broker = base.broker
make_transport = base.make_transport

HIGH = {PRIORITY_HEADER: PRIORITY_HIGH}


async def publish_backlog(publisher, capability: str, normal: int, high: int) -> None:  # type: ignore[no-untyped-def]
    """``normal`` plain dispatches first, then ``high`` high-priority ones, all queued before anyone consumes."""
    for i in range(normal):
        await publisher.publish(f"jobs/{capability}", dispatch(capability, f"n{i:02d}"))
    for i in range(high):
        await publisher.publish(f"jobs/{capability}", dispatch(capability, f"h{i:02d}"), headers=HIGH)


async def consume(make_transport, capability: str, expected: int, ack: Outcome = Outcome.ACK) -> list[str]:  # type: ignore[no-untyped-def]
    worker = await make_transport()
    inbox = Inbox(ack)
    inbox.transport = worker
    await worker.subscribe(f"jobs/{capability}", inbox)
    await wait_for(lambda: len(inbox.deliveries) >= expected, timeout=30, what=f"{expected} deliveries")
    return inbox.job_ids()


async def test_high_overtakes_a_normal_backlog_about_two_to_one_and_normal_never_starves(make_transport) -> None:  # type: ignore[no-untyped-def]
    c = cap("weight")
    await publish_backlog(await make_transport(), c, normal=40, high=12)
    order = await consume(make_transport, c, expected=52)

    highs = [j for j in order if j.startswith("h")]
    normals = [j for j in order if j.startswith("n")]
    assert highs == [f"h{i:02d}" for i in range(12)]  # FIFO inside the class
    assert normals == [f"n{i:02d}" for i in range(40)]
    # Strict FIFO would deliver every high after the 40 normals. Instead all 12 come within the first ~18 deliveries
    # (the measured 2:1 weighting), and normal still gets its share meanwhile.
    last_high = max(i for i, j in enumerate(order) if j.startswith("h"))
    assert last_high < 22, order
    assert sum(1 for j in order[: last_high + 1] if j.startswith("n")) >= 4, order  # weighted, not strict priority


async def test_messages_without_the_header_stay_fifo(make_transport) -> None:  # type: ignore[no-untyped-def]
    c = cap("fifo")
    publisher = await make_transport()
    for i in range(8):
        await publisher.publish(f"jobs/{c}", dispatch(c, f"p{i}"))
    assert await consume(make_transport, c, expected=8) == [f"p{i}" for i in range(8)]


async def test_a_requeued_high_message_is_redelivered_ahead_of_the_backlog(make_transport) -> None:  # type: ignore[no-untyped-def]
    c = cap("requeue")
    publisher = await make_transport()
    for i in range(10):
        await publisher.publish(f"jobs/{c}", dispatch(c, f"n{i:02d}"))
    await publisher.publish(f"jobs/{c}", dispatch(c, "hX"), headers=HIGH)

    worker = await make_transport()
    seen: list[Delivery] = []

    async def handler(delivery: Delivery) -> None:
        seen.append(delivery)
        first_high = delivery.message.payload.job_id == "hX" and not delivery.redelivered
        await worker.acknowledge(delivery, Outcome.REQUEUE if first_high else Outcome.ACK)

    await worker.subscribe(f"jobs/{c}", handler)
    await wait_for(lambda: len(seen) >= 12, timeout=30, what="every delivery")
    ids = [d.message.payload.job_id for d in seen]
    assert ids[0] == "hX" and ids[1] == "hX", ids  # back at the front of the queue, not behind ten normals
    again = seen[1]
    assert again.redelivered and again.delivery_count == 1
    assert ids.count("hX") == 2 and sorted(j for j in ids if j != "hX") == [f"n{i:02d}" for i in range(10)]


async def dead_letter_priorities(broker: Broker, message_id: str, timeout: float = 30.0) -> list[int | None]:
    """The AMQP priority of every copy of ``message_id`` in ``das.dlq``, read without consuming it."""
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient(base_url=broker.management_url, auth=(USER, PASSWORD), timeout=10) as api:
        while True:
            reply = await api.post(
                "/api/queues/%2F/das.dlq/get",
                json={"count": 50, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": 1},
            )
            reply.raise_for_status()
            found = [
                m["properties"].get("priority") for m in reply.json() if m["properties"].get("message_id") == message_id
            ]
            if found or asyncio.get_running_loop().time() > deadline:
                return found
            await asyncio.sleep(0.5)


async def test_a_dead_lettered_message_keeps_its_priority(broker: Broker, make_transport) -> None:  # type: ignore[no-untyped-def]
    c = cap("dlq-prio")
    manager = await make_transport(max_attempts=2, apply_policy=True)  # delivery-limit 1: two deliveries, then the DLX
    await manager.publish(f"jobs/{c}", dispatch(c, "hdl"), headers=HIGH)
    await manager.publish(f"jobs/{c}", dispatch(c, "ndl"))
    for _ in range(2):  # a Worker that takes the dispatches and vanishes without acknowledging them
        worker = await make_transport()
        inbox = Inbox()
        await worker.subscribe(f"jobs/{c}", inbox)
        await wait_for(lambda: len(inbox.deliveries) >= 2, what="both dispatches")
        await worker.close()

    assert await dead_letter_priorities(broker, "m-hdl-1") == [HIGH_AMQP_PRIORITY]
    assert await dead_letter_priorities(broker, "m-ndl-1") == [0]


async def test_no_queue_is_declared_with_max_priority(broker: Broker, make_transport) -> None:  # type: ignore[no-untyped-def]
    c = cap("declare")
    await publish_backlog(await make_transport(), c, normal=2, high=2)
    queue = await get_queue(broker, f"das.jobs.{c}")
    assert queue["type"] == "quorum"
    assert "x-max-priority" not in queue["arguments"], queue["arguments"]
    assert queue["arguments"]["x-queue-type"] == "quorum"
