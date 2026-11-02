"""RabbitMQ adapter against a real broker (task 8.8).

Runs ``rabbitmq:4.1-management`` with testcontainers and checks the behaviour
the in-process fake cannot: Capability-queue routing, broker requeue of
unacknowledged deliveries on a connection drop, quorum ``delivery-limit``
dead-lettering through the ``das-jobs`` policy, and reconnect with
subscription restore. Connection drops are forced through the management API
(``DELETE /api/connections/{name}``).

The whole module is skipped when Docker is not reachable.

Requirements: 4.5, 4.7, 6.4, 6.6, 8.2, 8.6, 8.9
"""

from __future__ import annotations

import asyncio
import functools
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote

import aio_pika
import httpx
import pytest

from das.codec import new_message
from das.models.results import JobDispatchPayload
from das.transport.base import Delivery, Outcome, ReconnectPolicy
from das.transport.rabbitmq import (
    DEAD_LETTER_REASON_HEADER,
    ORIGINAL_DESTINATION_HEADER,
    POLICY_NAME,
    RabbitMQTransport,
    delivery_limit_for,
)

pytestmark = pytest.mark.integration

IMAGE = "rabbitmq:4.1-management"
USER = "das"
PASSWORD = "das-test"
TS = datetime(2025, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------- broker


def _docker_available() -> bool:
    try:
        import docker

        client = docker.from_env(timeout=5)
        try:
            return bool(client.ping())
        finally:
            client.close()
    except Exception:
        return False


@dataclass(frozen=True)
class Broker:
    amqp_url: str
    management_url: str

    def api(self) -> httpx.Client:
        return httpx.Client(base_url=self.management_url, auth=(USER, PASSWORD), timeout=10)


def _wait_ready(broker: Broker, deadline: float) -> None:
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with broker.api() as api:
                # aliveness of the AMQP side: the default vhost answers queue listing
                if api.get("/api/overview").status_code == 200 and (
                    api.get("/api/queues/%2F").status_code == 200
                ):
                    return
        except httpx.HTTPError as exc:
            last = exc
        time.sleep(0.5)
    raise RuntimeError(f"RabbitMQ did not become ready: {last}")


@pytest.fixture(scope="module")
def broker() -> Iterator[Broker]:
    if not _docker_available():
        pytest.skip("Docker is not available; RabbitMQ integration tests need a real broker")
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(IMAGE)
        .with_exposed_ports(5672, 15672)
        .with_env("RABBITMQ_DEFAULT_USER", USER)
        .with_env("RABBITMQ_DEFAULT_PASS", PASSWORD)
        # Faster management stats so new connections show up quickly.
        .with_env(
            "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "-rabbit collect_statistics_interval 500"
        )
    )
    container.start()
    try:
        host = container.get_container_host_ip()
        amqp_port = container.get_exposed_port(5672)
        mgmt_port = container.get_exposed_port(15672)
        b = Broker(
            amqp_url=f"amqp://{USER}:{PASSWORD}@{host}:{amqp_port}/",
            management_url=f"http://{host}:{mgmt_port}",
        )
        _wait_ready(b, time.monotonic() + 90)
        _wait_amqp(b.amqp_url, time.monotonic() + 60)
        yield b
    finally:
        container.stop()


def _wait_amqp(url: str, deadline: float) -> None:
    async def probe() -> None:
        connection = await aio_pika.connect(url, timeout=3)
        await connection.close()

    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            asyncio.run(probe())
            return
        except Exception as exc:
            last = exc
            time.sleep(0.5)
    raise RuntimeError(f"AMQP port did not accept connections: {last}")


# --------------------------------------------------------------------------- helpers


def cap(prefix: str) -> str:
    """Unique Capability per test, so each test gets fresh ``das.jobs.<cap>`` queues."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def dispatch(capability: str, job_id: str, attempt: int = 1):
    return new_message(
        JobDispatchPayload(
            request_id="req-it",
            job_id=job_id,
            attempt=attempt,
            instruction="do it",
            required_capabilities=[capability],
        ),
        correlation_id="req-it",
        message_id=f"m-{job_id}-{attempt}",
        timestamp=TS,
    )


async def wait_for(predicate: Callable[[], bool], timeout: float = 15.0, what: str = "") -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {what or predicate}")
        await asyncio.sleep(0.05)


class Inbox:
    """Handler recording deliveries; optionally acknowledges each one."""

    def __init__(self, ack: Outcome | None = None) -> None:
        self.ack = ack
        self.transport: RabbitMQTransport | None = None
        self.deliveries: list[Delivery] = []

    async def __call__(self, delivery: Delivery) -> None:
        self.deliveries.append(delivery)
        if self.ack is not None and self.transport is not None:
            await self.transport.acknowledge(delivery, self.ack)

    def job_ids(self) -> list[str]:
        return [d.message.payload.job_id for d in self.deliveries if d.message is not None]


async def drop_connections(broker: Broker, connection_name: str, timeout: float = 20.0) -> int:
    """Force-close every broker connection named ``connection_name``."""
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient(
        base_url=broker.management_url, auth=(USER, PASSWORD), timeout=10
    ) as api:
        while True:
            response = await api.get("/api/connections")
            response.raise_for_status()
            names = [
                c["name"]
                for c in response.json()
                if (c.get("client_properties") or {}).get("connection_name") == connection_name
                or c.get("user_provided_name") == connection_name
            ]
            if names:
                for name in names:
                    r = await api.delete(
                        f"/api/connections/{quote(name, safe='')}",
                        headers={"X-Reason": "das integration test: forced drop"},
                    )
                    assert r.status_code in (200, 204), r.text
                return len(names)
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"connection {connection_name!r} not listed by the broker")
            await asyncio.sleep(0.25)


async def get_queue(broker: Broker, queue: str) -> dict:
    async with httpx.AsyncClient(
        base_url=broker.management_url, auth=(USER, PASSWORD), timeout=10
    ) as api:
        response = await api.get(f"/api/queues/%2F/{quote(queue, safe='')}")
        response.raise_for_status()
        return response.json()


@pytest.fixture
async def make_transport(
    broker: Broker,
) -> AsyncIterator[Callable[..., Awaitable[RabbitMQTransport]]]:
    created: list[RabbitMQTransport] = []

    async def make(name: str | None = None, **kwargs) -> RabbitMQTransport:
        options = {
            "url": broker.amqp_url,
            "management_url": broker.management_url,
            "agent_id": name or "it",
            "poll_interval": 0.05,
            "reconnect_policy": ReconnectPolicy(1),
        }
        if name is not None:
            options["connect"] = functools.partial(
                aio_pika.connect, client_properties={"connection_name": name}
            )
        options.update(kwargs)
        transport = RabbitMQTransport(**options)
        await transport.start()
        created.append(transport)
        return transport

    yield make
    for transport in created:
        await transport.close()


# --------------------------------------------------------------------------- tests


async def test_capability_queues_route_by_capability(make_transport) -> None:
    """Req 4.5, 8.2: a dispatch reaches only queues of its routing Capability."""
    a, b = cap("alpha"), cap("beta")
    publisher = await make_transport()
    worker_a = await make_transport()
    worker_b = await make_transport()
    worker_ab = await make_transport()
    inbox_a, inbox_b, inbox_ab = Inbox(Outcome.ACK), Inbox(Outcome.ACK), Inbox(Outcome.ACK)
    for transport, inbox in ((worker_a, inbox_a), (worker_b, inbox_b), (worker_ab, inbox_ab)):
        inbox.transport = transport
    await worker_a.subscribe(f"jobs/{a}", inbox_a)
    await worker_b.subscribe(f"jobs/{b}", inbox_b)

    for i in range(3):
        await publisher.publish(f"jobs/{a}", dispatch(a, f"a{i}"))
        await publisher.publish(f"jobs/{b}", dispatch(b, f"b{i}"))
    await wait_for(lambda: len(inbox_a.deliveries) == 3 and len(inbox_b.deliveries) == 3)
    assert sorted(inbox_a.job_ids()) == ["a0", "a1", "a2"]
    assert sorted(inbox_b.job_ids()) == ["b0", "b1", "b2"]

    # A Worker with both Capabilities consumes from both queues (round robin).
    await worker_ab.subscribe(f"jobs/{a}", inbox_ab)
    await worker_ab.subscribe(f"jobs/{b}", inbox_ab)
    for sub in list(worker_a.subscriptions) + list(worker_b.subscriptions):
        await sub.cancel()
    await publisher.publish(f"jobs/{a}", dispatch(a, "a9"))
    await publisher.publish(f"jobs/{b}", dispatch(b, "b9"))
    await wait_for(lambda: len(inbox_ab.deliveries) == 2)
    assert sorted(inbox_ab.job_ids()) == ["a9", "b9"]
    assert {d.destination for d in inbox_ab.deliveries} == {f"jobs/{a}", f"jobs/{b}"}
    assert all(not d.redelivered and d.delivery_count == 0 for d in inbox_ab.deliveries)


async def test_unacked_dispatch_redelivered_after_connection_drop(
    broker: Broker, make_transport
) -> None:
    """Req 4.7, 6.4, 8.9: a drop returns the unacked dispatch, attempt unchanged."""
    c = cap("redeliver")
    publisher = await make_transport()
    worker_a = await make_transport("worker-a-" + c)
    held = Inbox()  # never acknowledges
    sub_a = await worker_a.subscribe(f"jobs/{c}", held)

    await publisher.publish(f"jobs/{c}", dispatch(c, "j1", attempt=1))
    await wait_for(lambda: len(held.deliveries) == 1, what="first delivery")
    first = held.deliveries[0]
    assert first.message.payload.attempt == 1
    assert not first.redelivered and first.delivery_count == 0
    await sub_a.cancel()  # worker A stops consuming but still holds the delivery

    await drop_connections(broker, "worker-a-" + c)
    await wait_for(lambda: not worker_a.connected, what="worker A to notice the drop")
    # Acknowledging after the drop is a stale no-op; the broker already requeued it.
    await worker_a.acknowledge(first)
    assert worker_a.stale_acknowledgements == 1

    worker_b = await make_transport()
    inbox_b = Inbox(Outcome.ACK)
    inbox_b.transport = worker_b
    await worker_b.subscribe(f"jobs/{c}", inbox_b)
    await wait_for(lambda: len(inbox_b.deliveries) == 1, what="redelivery to worker B")
    again = inbox_b.deliveries[0]
    assert again.message == first.message  # same Job, same attempt number
    assert again.message.payload.attempt == 1
    assert again.message.message_id == first.message.message_id
    assert again.redelivered
    # RabbitMQ 4.x quorum queues count returns in x-delivery-count.
    assert again.delivery_count == 1
    assert again.headers.get("x-delivery-count") == "1"

    await asyncio.sleep(0.5)
    assert (await get_queue(broker, f"das.jobs.{c}"))["messages"] == 0


@pytest.mark.parametrize("max_attempts", [1, 3])
async def test_delivery_limit_dead_letters_after_max_attempts(
    broker: Broker, make_transport, max_attempts: int
) -> None:
    """Req 6.6: ``delivery-limit = max_attempts - 1`` -> DLQ after max_attempts deliveries."""
    c = cap(f"dlq{max_attempts}")
    manager = await make_transport(max_attempts=max_attempts, apply_policy=True)
    job_id = f"dl-{c}"

    # The policy is in place and applies to the jobs queue.
    await manager.publish(f"jobs/{c}", dispatch(c, job_id))
    queue = await get_queue(broker, f"das.jobs.{c}")
    for _ in range(40):
        if queue.get("policy") == POLICY_NAME:
            break
        await asyncio.sleep(0.25)
        queue = await get_queue(broker, f"das.jobs.{c}")
    assert queue.get("policy") == POLICY_NAME
    assert queue["effective_policy_definition"]["delivery-limit"] == delivery_limit_for(
        max_attempts
    )

    events = Inbox(Outcome.ACK)
    events.transport = manager
    await manager.subscribe("dlq-events", events)

    # Each attempt: a fresh Worker connection receives the dispatch and closes
    # without acknowledging it.
    counts: list[int] = []
    for attempt in range(max_attempts):
        worker = await make_transport()
        inbox = Inbox()
        await worker.subscribe(f"jobs/{c}", inbox)
        await wait_for(lambda: len(inbox.deliveries) == 1, what=f"delivery {attempt + 1}")
        delivery = inbox.deliveries[0]
        assert delivery.message.payload.job_id == job_id
        assert delivery.message.payload.attempt == 1  # unacked returns keep the attempt
        counts.append(delivery.delivery_count)
        await worker.close()

    assert counts == list(range(max_attempts))
    await wait_for(
        lambda: job_id in events.job_ids(), timeout=20, what="dead-letter event"
    )
    dead = next(d for d in events.deliveries if d.message.payload.job_id == job_id)
    assert dead.headers[DEAD_LETTER_REASON_HEADER] == "delivery_limit"
    assert dead.headers[ORIGINAL_DESTINATION_HEADER] == f"jobs/{c}"

    # Not redelivered again: the jobs queue is empty, and the DLQ retains a copy.
    late = await make_transport()
    late_inbox = Inbox(Outcome.ACK)
    late_inbox.transport = late
    await late.subscribe(f"jobs/{c}", late_inbox)
    await asyncio.sleep(1.0)
    assert late_inbox.deliveries == []
    retained = Inbox(Outcome.ACK)
    retained.transport = late
    await late.subscribe("dlq", retained)
    await wait_for(lambda: job_id in retained.job_ids(), what="DLQ copy")


async def test_reconnect_restores_subscriptions(broker: Broker, make_transport) -> None:
    """Req 8.6: after a drop the adapter reconnects and resumes every subscription."""
    c1, c2 = cap("recon1"), cap("recon2")
    publisher = await make_transport()
    name = "worker-r-" + c1
    worker = await make_transport(name)
    inbox = Inbox(Outcome.ACK)
    inbox.transport = worker
    await worker.subscribe(f"jobs/{c1}", inbox)
    await worker.subscribe(f"jobs/{c2}", inbox)
    await worker.subscribe("results", Inbox())  # non-jobs destination, never fed

    await publisher.publish(f"jobs/{c1}", dispatch(c1, "before"))
    await wait_for(lambda: inbox.job_ids() == ["before"])

    await drop_connections(broker, name)
    await wait_for(lambda: not worker.connected, what="drop detected")
    # Published while the Worker is disconnected: delivered after restore.
    await publisher.publish(f"jobs/{c2}", dispatch(c2, "during"))
    await wait_for(lambda: worker.connected, timeout=20, what="reconnect")
    assert len(worker.subscriptions) == 3
    await wait_for(lambda: "during" in inbox.job_ids(), what="delivery after reconnect")

    await publisher.publish(f"jobs/{c1}", dispatch(c1, "after1"))
    await publisher.publish(f"jobs/{c2}", dispatch(c2, "after2"))
    await wait_for(lambda: {"after1", "after2"} <= set(inbox.job_ids()))
    assert sorted(inbox.job_ids()) == ["after1", "after2", "before", "during"]

    # A second drop is handled the same way.
    await drop_connections(broker, name)
    await wait_for(lambda: not worker.connected, what="second drop detected")
    await wait_for(lambda: worker.connected, timeout=20, what="second reconnect")
    await publisher.publish(f"jobs/{c1}", dispatch(c1, "after-second"))
    await wait_for(lambda: "after-second" in inbox.job_ids())
