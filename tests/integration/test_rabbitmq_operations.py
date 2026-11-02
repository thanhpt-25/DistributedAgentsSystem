"""Phase 2 RabbitMQ behaviour against a real broker (task 19.1).

Runs ``rabbitmq:4.1-management`` with testcontainers, loads the deployed topology
from ``deploy/rabbitmq/definitions.json`` (including policy ``das-dlq`` with
``delivery-limit: -1``, design D5) and checks what the in-process fakes cannot:

- ``RabbitMQTransport.ready_count`` matches the number of ready Messages (Req 3.1);
- ``das.dlq`` keeps a Message requeued more than 20 times while the State_Store
  is failing, so the Dead_Letter_Record is still created afterwards (Req 12.11, D5);
- a broker dead letter gets its Dead_Letter_Record within 5 s and then leaves
  ``das.dlq`` (Req 12.1, 13.14);
- the ``x-llm-provider`` / ``x-llm-model`` headers of a Job_Result survive the
  broker and reach the inbox entry (design D2).

The State_Store is fakeredis running the real Lua scripts. The whole module is
skipped when Docker is not reachable.

Requirements: 3.1, 12.1, 12.11, 13.14
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from das.codec import new_message
from das.manager.driver import (
    DLQ_ATTEMPTS_HEADER,
    DLQ_JOB_ID_HEADER,
    DLQ_LAST_ERROR_HEADER,
    DLQ_REASON_HEADER,
    DLQ_REASON_MAX_ATTEMPTS,
    DLQ_REQUEST_ID_HEADER,
)
from das.manager.ingest import DeadLetterRecorder, ResultIngest
from das.models import JobDispatchPayload, JobResultPayload, TokenUsage
from das.models.dlq import DeadLetterFields
from das.ops.dlq import dead_letter_id, dead_letter_identity
from das.state import INBOX_JOB_RESULT, StateStore
from das.transport.base import (
    Delivery,
    Outcome,
    ReconnectPolicy,
    SubscribeOptions,
    TransportError,
)
from das.transport.rabbitmq import RabbitMQTransport
from das.worker.executor import MODEL_HEADER, PROVIDER_HEADER
from tests.integration.test_rabbitmq_broker import (
    IMAGE,
    PASSWORD,
    USER,
    Broker,
    Inbox,
    _docker_available,
    _wait_amqp,
    _wait_ready,
    cap,
    dispatch,
    get_queue,
    wait_for,
)

pytestmark = pytest.mark.integration

DEFINITIONS = Path(__file__).resolve().parents[2] / "deploy" / "rabbitmq" / "definitions.json"
#: Quorum queues drop (no DLX on das.dlq) after this many returns without ``das-dlq``.
DEFAULT_QUORUM_DELIVERY_LIMIT = 20
RECORD_DEADLINE = 5.0  # Req 12.1


# --------------------------------------------------------------------------- broker


def _load_definitions(b: Broker) -> None:
    with b.api() as api:
        response = api.post("/api/definitions", json=json.loads(DEFINITIONS.read_text()))
        assert response.status_code in (200, 201, 204), response.text
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            queue = api.get("/api/queues/%2F/das.dlq").json()
            if queue.get("policy") == "das-dlq":
                return
            time.sleep(0.25)
    raise RuntimeError("policy das-dlq was not applied to das.dlq")


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
        .with_env(
            "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "-rabbit collect_statistics_interval 500"
        )
    )
    container.start()
    try:
        host = container.get_container_host_ip()
        b = Broker(
            amqp_url=f"amqp://{USER}:{PASSWORD}@{host}:{container.get_exposed_port(5672)}/",
            management_url=f"http://{host}:{container.get_exposed_port(15672)}",
        )
        _wait_ready(b, time.monotonic() + 90)
        _wait_amqp(b.amqp_url, time.monotonic() + 60)
        _load_definitions(b)
        yield b
    finally:
        container.stop()


@pytest.fixture
async def make_transport(
    broker: Broker,
) -> AsyncIterator[Callable[[], Awaitable[RabbitMQTransport]]]:
    created: list[RabbitMQTransport] = []

    async def make() -> RabbitMQTransport:
        transport = RabbitMQTransport(
            url=broker.amqp_url,
            management_url=broker.management_url,
            agent_id=f"it-{uuid.uuid4().hex[:6]}",
            poll_interval=0.05,
            reconnect_policy=ReconnectPolicy(1),
        )
        await transport.start()
        created.append(transport)
        return transport

    yield make
    for transport in created:
        await transport.close()


# --------------------------------------------------------------------------- helpers


class FailingStore:
    """Real StateStore whose first ``failures`` ``create_dead_letter`` calls raise
    (State_Store unreachable, Req 12.11)."""

    def __init__(self, store: StateStore, failures: int) -> None:
        self._store = store
        self.remaining = failures
        self.calls = 0

    async def create_dead_letter(self, fields: DeadLetterFields):  # noqa: ANN201
        self.calls += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise RedisConnectionError("injected redis failure")
        return await self._store.create_dead_letter(fields)


async def wait_ready_count(
    transport: RabbitMQTransport, destination: str, expected: int, timeout: float = 15.0
) -> None:
    last: list[int] = []

    async def check() -> bool:
        last.append(await transport.ready_count(destination))
        return last[-1] == expected

    deadline = asyncio.get_running_loop().time() + timeout
    while not await check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"ready_count({destination!r}) = {last[-1]}, want {expected}")
        await asyncio.sleep(0.1)


async def wait_queue_empty(broker: Broker, queue: str, timeout: float = 15.0) -> None:
    """Neither ready nor unacknowledged Messages left (management stats lag a bit)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        info = await get_queue(broker, queue)
        if info.get("messages") == 0 and info.get("messages_unacknowledged", 0) == 0:
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"{queue} still holds messages: {info.get('messages')}")
        await asyncio.sleep(0.25)


async def drain_dlq(transport: RabbitMQTransport) -> None:
    """ACK whatever an earlier test left in das.dlq so counts start at 0."""
    if await transport.ready_count("dlq") == 0:
        return
    inbox = Inbox(Outcome.ACK)
    inbox.transport = transport
    sub = await transport.subscribe("dlq", inbox)
    await wait_ready_count(transport, "dlq", 0)
    await sub.cancel()


def job_result(rid: str, job_id: str) -> JobResultPayload:
    return JobResultPayload(
        request_id=rid, job_id=job_id, attempt=1, worker_id="w1", status="Succeeded",
        content="x", usage=TokenUsage(), duration_ms=1,
    )


# --------------------------------------------------------------------------- tests


async def test_ready_count_matches_ready_messages(make_transport) -> None:
    """Req 3.1: ``ready_count`` counts ready Messages, not unacknowledged ones."""
    c = cap("depth")
    publisher = await make_transport()
    destination = f"jobs/{c}"
    for i in range(5):
        await publisher.publish(destination, dispatch(c, f"d{i}"))
    assert await publisher.ready_count(destination) == 5

    consumer = await make_transport()
    held = Inbox()  # never acknowledges
    sub = await consumer.subscribe(destination, held, SubscribeOptions(max_in_flight=2))
    await wait_for(lambda: len(held.deliveries) == 2, what="two deliveries")
    await sub.cancel()
    await wait_ready_count(publisher, destination, 3)  # 2 delivered, not yet acked

    for delivery in held.deliveries:
        await consumer.acknowledge(delivery, Outcome.ACK)
    assert await publisher.ready_count(destination) == 3

    # A missing queue fails only that sample; the transport stays usable.
    with pytest.raises(TransportError):
        await publisher.ready_count(f"jobs/{cap('missing')}")
    assert publisher.connected
    assert await publisher.ready_count(destination) == 3


async def test_dlq_policy_keeps_message_past_twenty_requeues(
    broker: Broker, make_transport, state_store: StateStore
) -> None:
    """Req 12.11, D5: with ``das-dlq`` the Message survives > 20 requeues on Redis
    failure, and the record is created once the State_Store recovers."""
    queue = await get_queue(broker, "das.dlq")
    assert queue["policy"] == "das-dlq"
    assert queue["effective_policy_definition"]["delivery-limit"] == -1

    manager = await make_transport()
    await drain_dlq(manager)
    failures = DEFAULT_QUORUM_DELIVERY_LIMIT + 5
    store = FailingStore(state_store, failures)
    recorder = DeadLetterRecorder(manager, store)  # type: ignore[arg-type]
    counts: list[int] = []

    async def handle(delivery: Delivery) -> None:
        counts.append(delivery.delivery_count)
        await recorder.handle(delivery)

    rid = await state_store.create_request("p")
    message = new_message(
        JobDispatchPayload(
            request_id=rid, job_id="a", attempt=3, instruction="do it",
            required_capabilities=["research"],
        ),
        correlation_id=rid,
    )
    await manager.publish(
        "dlq",
        message,
        {
            DLQ_REQUEST_ID_HEADER: rid,
            DLQ_JOB_ID_HEADER: "a",
            DLQ_ATTEMPTS_HEADER: "3",
            DLQ_LAST_ERROR_HEADER: "boom",
            DLQ_REASON_HEADER: DLQ_REASON_MAX_ATTEMPTS,
        },
    )
    await manager.subscribe("dlq", handle)

    dlid = dead_letter_id(dead_letter_identity(message, b""))
    deadline = asyncio.get_running_loop().time() + 60
    record = None
    while record is None:
        record = await state_store.get_dead_letter(dlid)
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"no record after {store.calls} deliveries, counts={counts}")
        await asyncio.sleep(0.1)

    assert store.calls == failures + 1
    assert len(counts) == failures + 1
    # RabbitMQ counted every requeue; the default limit (20) would have dropped it.
    assert counts[-1] > DEFAULT_QUORUM_DELIVERY_LIMIT
    assert record.status == "Pending" and record.reason == "retries_exhausted"
    assert record.request_id == rid and record.job_id == "a" and record.attempt == 3
    assert record.last_error == "boom"
    await wait_ready_count(manager, "dlq", 0)
    await wait_queue_empty(broker, "das.dlq")


async def test_broker_dead_letter_recorded_within_five_seconds(
    broker: Broker, make_transport, state_store: StateStore
) -> None:
    """Req 12.1, 13.14: a delivery-limit dead letter in das.dlq gets its
    Dead_Letter_Record within 5 s, then leaves the queue."""
    manager = await make_transport()
    await drain_dlq(manager)
    rid = await state_store.create_request("p")
    job_id = f"dl-{uuid.uuid4().hex[:8]}"
    message = new_message(
        JobDispatchPayload(
            request_id=rid, job_id=job_id, attempt=1, instruction="do it",
            required_capabilities=["research"],
        ),
        correlation_id=rid,
    )
    # das-jobs from definitions.json: delivery-limit 2 -> dead letter after 3 returns.
    await manager.publish("jobs/research", message)
    for attempt in range(3):
        worker = await make_transport()
        inbox = Inbox()
        await worker.subscribe("jobs/research", inbox)
        await wait_for(lambda: len(inbox.deliveries) == 1, what=f"delivery {attempt + 1}")
        await worker.close()
    await wait_ready_count(manager, "dlq", 1, timeout=20)

    dlid = dead_letter_id(dead_letter_identity(message, b""))
    assert await state_store.get_dead_letter(dlid) is None
    started = time.monotonic()
    await DeadLetterRecorder(manager, state_store).start()
    record = None
    while record is None and time.monotonic() - started <= RECORD_DEADLINE:
        record = await state_store.get_dead_letter(dlid)
        if record is None:
            await asyncio.sleep(0.05)
    elapsed = time.monotonic() - started
    assert record is not None, f"no Dead_Letter_Record within {RECORD_DEADLINE} s"
    assert elapsed <= RECORD_DEADLINE
    assert record.status == "Pending" and record.reason == "delivery_limit"
    assert record.request_id == rid and record.job_id == job_id and record.attempt == 1
    assert record.capabilities == ["research"]

    await wait_ready_count(manager, "dlq", 0)
    await wait_queue_empty(broker, "das.dlq")
    assert await manager.ready_count("jobs/research") == 0


async def test_llm_headers_cross_the_broker(make_transport, state_store: StateStore) -> None:
    """D2: Job_Result provider/model headers reach the inbox entry; missing -> unknown."""
    worker = await make_transport()
    manager = await make_transport()
    rid = await state_store.create_request("p")
    ingested: list[str] = []
    await ResultIngest(manager, state_store, on_ingested=ingested.append).start()

    await worker.publish(
        "results",
        new_message(job_result(rid, "a"), correlation_id=rid),
        headers={PROVIDER_HEADER: "anthropic", MODEL_HEADER: "claude-sonnet-4"},
    )
    await worker.publish("results", new_message(job_result(rid, "b"), correlation_id=rid))
    await wait_for(lambda: len(ingested) == 2, what="two ingested Job_Results")

    entries = await state_store.read_inbox(rid, after="0-0")
    by_job = {e.job_result().job_id: e for e in entries if e.kind == INBOX_JOB_RESULT}
    assert by_job["a"].meta_payload() == {"provider": "anthropic", "model": "claude-sonnet-4"}
    assert by_job["b"].meta_payload() == {"provider": "unknown", "model": "unknown"}
    await wait_ready_count(manager, "results", 0)
