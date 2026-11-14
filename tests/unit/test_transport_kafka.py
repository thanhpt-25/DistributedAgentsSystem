"""KafkaTransport beyond the shared contract (das-scale-resilience task 10.2; Phase 7 Req 3.2 to 3.8): when offsets are
committed, what a rebalance does to a message in flight, the retry topic and its delay, the delivery limit, the dead
letter headers, topic creation, how errors map, and how the real clients are configured."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from aiokafka.errors import CommitFailedError, KafkaConnectionError, KafkaError
from aiokafka.structs import TopicPartition

import das.transport.kafka as kafka
from das.codec import MessageCodec
from das.transport.base import (
    Delivery, Outcome, PublishNotConfirmed, ReconnectPolicy, SubscribeOptions, TransportUnavailable,
)
from das.transport.kafka import (
    COUNT_HEADER, NOT_BEFORE_HEADER, REPLY_TO_HEADER, AioKafkaFactory, KafkaSettings, KafkaTransport, topic_for,
)
from tests.fakes.kafka_cluster import FakeCluster
from tests.transport_contract import Recorder, job, until

JOBS = "jobs/work"
GROUP = f"das-{topic_for(JOBS)}"


class Clock:
    """A settable ``time.time`` so the retry delay does not need real waiting."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def cluster() -> FakeCluster:
    return FakeCluster(partitions=1)


@pytest.fixture
async def made() -> AsyncIterator[list[KafkaTransport]]:
    transports: list[KafkaTransport] = []
    yield transports
    for transport in transports:
        await transport.close()


async def make(
    cluster: FakeCluster, made: list[KafkaTransport], agent_id: str = "agent", clock: Clock | None = None, **extra: Any
) -> KafkaTransport:
    async def quick(seconds: float) -> None:
        await asyncio.sleep(0.05)

    settings = KafkaSettings(("fake:9092",), partitions=extra.pop("partitions", 1), **extra.pop("settings", {}))
    transport = KafkaTransport(
        settings, agent_id=agent_id, factory=cluster.factory(agent_id), sleep=quick,
        reconnect_policy=ReconnectPolicy(1.0), now=clock or Clock(), **extra,
    )
    await transport.start()
    made.append(transport)
    return transport


# --------------------------------------------------------------------------- settings and clients


def test_the_passwords_are_not_in_the_settings_repr() -> None:
    settings = KafkaSettings(("b:9092",), sasl_password="hunter2-secret", ssl_key_password="key-secret-pass")
    assert "hunter2-secret" not in repr(settings) and "key-secret-pass" not in repr(settings)


@pytest.mark.parametrize("bad", [
    {"bootstrap_servers": ()}, {"partitions": 0}, {"replication_factor": 0}, {"retry_delay": -1},
    {"delivery_limit": -1},
])
def test_settings_that_make_no_sense_are_refused(bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        KafkaSettings(**{"bootstrap_servers": ("b:9092",), **bad})


def test_topics_are_named_as_the_rabbitmq_queues_with_high_beside_them() -> None:
    assert topic_for("jobs/research") == "das.jobs.research" and topic_for("results") == "das.results"
    assert topic_for("dlq") == "das.dlq" and topic_for("dlq-events") == "das.dlq.events"
    assert topic_for("jobs/research", high=True) == "das.jobs.research.high"


def test_a_destination_becomes_a_topic_name_kafka_accepts() -> None:
    assert topic_for("control/ask") == "das.control.ask", "a slash is not allowed in a Kafka topic name"
    assert topic_for("control/worker 7:a") == "das.control.worker_7_a"
    assert topic_for("control/\u00e9t\u00e9") == "das.control._t_"


def test_a_destination_too_long_for_a_topic_name_is_refused() -> None:
    assert topic_for("jobs/" + "x" * 200)
    with pytest.raises(ValueError, match="too long"):
        topic_for("jobs/" + "x" * 245)


async def test_every_topic_the_adapter_uses_is_one_the_cluster_accepts(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    await t.subscribe("control/agent 1:x", Recorder(t))
    await t.publish("control/agent 1:x", job(1))
    assert "das.control.agent_1_x" in cluster.topics
    with pytest.raises(Exception, match="not a valid topic name"):
        cluster.append("das.control/ask", b"", [], None)


class Spy:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


def test_the_real_clients_get_an_idempotent_acks_all_producer_and_manual_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka, "AIOKafkaProducer", Spy)
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", Spy)
    factory = AioKafkaFactory(KafkaSettings(("a:9092", "b:9092")))
    producer = factory.producer()
    consumer, reply = factory.consumer("g", from_start=True), factory.consumer(None, from_start=False)
    assert producer.kwargs["enable_idempotence"] is True and producer.kwargs["acks"] == "all"
    assert producer.kwargs["bootstrap_servers"] == ["a:9092", "b:9092"]
    assert consumer.kwargs["enable_auto_commit"] is False and consumer.kwargs["auto_offset_reset"] == "earliest"
    assert consumer.kwargs["group_id"] == "g"
    assert reply.kwargs["auto_offset_reset"] == "latest" and reply.kwargs["group_id"] is None


def test_sasl_credentials_reach_the_clients_only_when_the_protocol_uses_them(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kafka, "AIOKafkaProducer", Spy)
    secured = AioKafkaFactory(KafkaSettings(
        ("b:9092",), security_protocol="SASL_PLAINTEXT", sasl_mechanism="SCRAM-SHA-512", sasl_username="das",
        sasl_password="pw-0123456789",
    )).producer().kwargs
    assert secured["sasl_mechanism"] == "SCRAM-SHA-512" and secured["sasl_plain_username"] == "das"
    assert secured["sasl_plain_password"] == "pw-0123456789" and secured["security_protocol"] == "SASL_PLAINTEXT"
    unused = KafkaSettings(("b:9092",), sasl_username="das", sasl_password="pw-0123456789")
    plain = AioKafkaFactory(unused).producer().kwargs
    assert "sasl_plain_password" not in plain and "ssl_context" not in plain


def test_tls_builds_a_verifying_context(monkeypatch: pytest.MonkeyPatch) -> None:
    import ssl

    monkeypatch.setattr(kafka, "AIOKafkaProducer", Spy)
    context = AioKafkaFactory(KafkaSettings(("b:9093",), security_protocol="SSL")).producer().kwargs["ssl_context"]
    assert isinstance(context, ssl.SSLContext) and context.verify_mode == ssl.CERT_REQUIRED


# --------------------------------------------------------------------------- topics (Req 3.2)


async def test_the_topics_of_a_subscription_are_created_once_with_the_configured_partitions(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made, partitions=5)
    await t.subscribe(JOBS, Recorder(t))
    await t.subscribe(JOBS, Recorder(t))
    names = [name for name, _ in cluster.created]
    assert sorted(names) == ["das.jobs.work", "das.jobs.work.high", "das.jobs.work.retry"]
    assert {parts for _, parts in cluster.created} == {5}


async def test_nothing_is_created_by_the_adapter_when_auto_create_is_off(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made, settings={"auto_create_topics": False})
    await t.publish(JOBS, job(1))
    assert [name for name, _ in cluster.created] == ["das.jobs.work"]  # only the broker's own auto-create on send


async def test_an_existing_topic_is_not_an_error(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    cluster.create_topic("das.jobs.work")
    t = await make(cluster, made)
    await t.publish(JOBS, job(1))
    assert len(cluster.records("das.jobs.work")) == 1


# --------------------------------------------------------------------------- publish


async def test_a_cluster_error_that_is_not_a_connection_one_is_publish_not_confirmed(
    cluster: FakeCluster, made: list[KafkaTransport], monkeypatch: pytest.MonkeyPatch
) -> None:
    t = await make(cluster, made)

    async def refuse(*args: Any, **kwargs: Any) -> None:
        raise KafkaError("too large")

    monkeypatch.setattr(t._producer, "send_and_wait", refuse)
    with pytest.raises(PublishNotConfirmed) as info:
        await t.publish(JOBS, job(1))
    assert info.value.destination == JOBS and t.connected is True


async def test_a_lost_connection_is_transport_unavailable_closes_the_gate_and_heals(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    cluster.down.add("agent")
    with pytest.raises(TransportUnavailable):
        await t.publish(JOBS, job(1))
    assert t.connected is False and t.registry.is_open is False
    cluster.down.discard("agent")
    await until(lambda: t.connected)
    assert t.registry.is_open


async def test_starting_against_a_cluster_that_is_down_is_transport_unavailable(cluster: FakeCluster) -> None:
    cluster.down.add("agent")
    t = KafkaTransport(KafkaSettings(("fake:9092",)), agent_id="agent", factory=cluster.factory("agent"))
    with pytest.raises(TransportUnavailable, match="cannot connect"):
        await t.start()
    assert t.connected is False


async def test_operations_before_start_raise_unavailable(cluster: FakeCluster) -> None:
    t = KafkaTransport(KafkaSettings(("fake:9092",)), factory=cluster.factory("a"))
    with pytest.raises(TransportUnavailable):
        await t.publish(JOBS, job(1))
    with pytest.raises(TransportUnavailable):
        await t.subscribe(JOBS, Recorder(t))


async def test_the_stored_bytes_are_the_codecs_encoding_and_priority_goes_to_the_high_topic(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    message = job(5)
    await t.publish(JOBS, message, headers={"das-priority": "high", "k": "v"})
    [record] = cluster.records("das.jobs.work.high")
    assert record.value == MessageCodec().encode(message) and cluster.records("das.jobs.work") == []
    assert dict(record.headers) == {"k": b"v"}, "the priority header is consumed, the others travel"


async def test_non_text_headers_are_a_type_error_and_nothing_is_sent(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    with pytest.raises(TypeError):
        await t.publish(JOBS, job(1), headers={"k": 1})  # type: ignore[dict-item]
    assert cluster.records("das.jobs.work") == []


# --------------------------------------------------------------------------- committing (Req 3.4, 3.6)


async def test_an_offset_is_committed_only_after_the_acknowledgement(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    await t.subscribe(JOBS, hold)
    await t.publish(JOBS, job(1))
    await until(lambda: len(held) == 1)
    await asyncio.sleep(0.5)
    assert cluster.committed(GROUP, "das.jobs.work") is None
    await t.acknowledge(held[0])
    assert cluster.committed(GROUP, "das.jobs.work") == 1


async def test_an_acknowledgement_out_of_order_never_commits_past_an_unfinished_message(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    await t.subscribe(JOBS, hold)
    for n in (1, 2, 3):
        await t.publish(JOBS, job(n))
    await until(lambda: len(held) == 3)
    await t.acknowledge(held[2])
    await t.acknowledge(held[1])
    assert cluster.committed(GROUP, "das.jobs.work") is None, "message 1 is still being worked on"
    await t.acknowledge(held[0])
    assert cluster.committed(GROUP, "das.jobs.work") == 3


async def test_a_handler_that_raises_leaves_its_message_uncommitted_and_gives_up_the_slot(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    seen: list[str] = []

    async def fail(delivery: Delivery) -> None:
        seen.append(delivery.message.payload.job_id)  # type: ignore[union-attr]
        raise RuntimeError("boom")

    await t.subscribe(JOBS, fail, SubscribeOptions(max_in_flight=1))
    await t.publish(JOBS, job(1))
    await t.publish(JOBS, job(2))
    await until(lambda: "j2" in seen)
    assert cluster.committed(GROUP, "das.jobs.work") is None and len(t.handler_errors) == 2


async def test_a_second_acknowledgement_is_a_no_op(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    t = await make(cluster, made)
    seen = Recorder(t)
    await t.subscribe(JOBS, seen)
    await t.publish(JOBS, job(1))
    await until(lambda: len(seen.deliveries) == 1)
    await t.acknowledge(seen.deliveries[0])
    assert cluster.committed(GROUP, "das.jobs.work") == 1


async def test_a_delivery_from_another_transport_is_refused(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    t = await make(cluster, made)
    with pytest.raises(ValueError, match="not produced"):
        await t.acknowledge(Delivery(None, JOBS, False, 0, {}, token=object()))


async def test_an_acknowledged_commit_that_could_not_be_made_is_made_when_the_cluster_is_back(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    await t.subscribe(JOBS, hold)
    await t.publish(JOBS, job(1))
    await until(lambda: len(held) == 1)
    cluster.down.add("agent")
    await t.acknowledge(held[0])  # the commit fails: the cluster is unreachable
    assert t.connected is False and cluster.committed(GROUP, "das.jobs.work") is None
    cluster.down.discard("agent")
    await until(lambda: cluster.committed(GROUP, "das.jobs.work") == 1)


# --------------------------------------------------------------------------- a rebalance (Req 3.6)


async def test_a_message_in_flight_at_a_rebalance_is_redelivered_and_the_late_acknowledgement_commits_nothing(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    first = await make(cluster, made, "first")
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    await first.subscribe(JOBS, hold)
    await first.publish(JOBS, job(1))
    await until(lambda: len(held) == 1)
    second = await make(cluster, made, "second")  # joins the group: a rebalance takes the partition from first
    pending: list[Delivery] = []

    async def hold_too(delivery: Delivery) -> None:
        pending.append(delivery)

    await second.subscribe(JOBS, hold_too)
    # one partition, two members: it goes to the first member of the group, so the message comes back to `first`
    await until(lambda: len(held) + len(pending) >= 2)
    stale = held[0]
    await first.acknowledge(stale)
    assert cluster.committed(GROUP, "das.jobs.work") is None, "a stale acknowledgement commits nothing"
    owner_deliveries = held[1:] + pending
    assert owner_deliveries, "the message was delivered again to its new owner"
    await (first if held[1:] else second).acknowledge(owner_deliveries[0])
    assert cluster.committed(GROUP, "das.jobs.work") == 1


async def test_what_is_acknowledged_is_committed_when_the_partition_is_taken_away(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    first = await make(cluster, made, "first")
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    await first.subscribe(JOBS, hold)
    for n in (1, 2):
        await first.publish(JOBS, job(n))
    await until(lambda: len(held) == 2)
    cluster.down.add("first")  # its acknowledgement of message 1 cannot be committed...
    await first.acknowledge(held[0])
    cluster.down.discard("first")
    assert cluster.committed(GROUP, "das.jobs.work") is None
    second = await make(cluster, made, "second")  # ...so the revoke commits it before the partition goes
    await second.subscribe(JOBS, Recorder(second))
    await until(lambda: cluster.committed(GROUP, "das.jobs.work") == 1)


# --------------------------------------------------------------------------- requeue, reject, the limit (Req 3.5)


async def test_a_requeue_goes_to_the_retry_topic_with_a_count_and_a_not_before_stamp(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    clock = Clock()
    t = await make(cluster, made, clock=clock, settings={"retry_delay": 5.0})
    seen = Recorder(t, Outcome.REQUEUE)
    await t.subscribe(JOBS, seen)
    await t.publish(JOBS, job(1))
    await until(lambda: len(cluster.records("das.jobs.work.retry")) == 1)
    [record] = cluster.records("das.jobs.work.retry")
    headers = {k: v.decode() for k, v in record.headers}
    assert headers[COUNT_HEADER] == "1" and headers[NOT_BEFORE_HEADER] == str(int((clock.now + 5.0) * 1000))
    await until(lambda: cluster.committed(GROUP, "das.jobs.work") == 1)  # the original, once the copy is stored


async def test_a_requeued_message_is_held_until_its_time_and_then_comes_back_marked(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    clock = Clock()
    t = await make(cluster, made, clock=clock, settings={"retry_delay": 60.0})
    seen = Recorder(t, Outcome.REQUEUE, Outcome.ACK)
    await t.subscribe(JOBS, seen)
    await t.publish(JOBS, job(1))
    await until(lambda: len(cluster.records("das.jobs.work.retry")) == 1)
    await asyncio.sleep(0.6)
    assert len(seen.deliveries) == 1, "not before its time"
    clock.now += 61
    await until(lambda: len(seen.deliveries) == 2)
    second = seen.deliveries[1]
    assert second.redelivered is True and second.delivery_count == 1
    assert COUNT_HEADER not in second.headers and NOT_BEFORE_HEADER not in second.headers


async def test_a_held_retry_does_not_let_the_commit_point_pass_it(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    clock = Clock()
    t = await make(cluster, made, clock=clock, settings={"retry_delay": 60.0})
    seen = Recorder(t, Outcome.REQUEUE)
    await t.subscribe(JOBS, seen)
    await t.publish(JOBS, job(1))
    await until(lambda: len(cluster.records("das.jobs.work.retry")) == 1)
    await t.publish(JOBS, job(2))
    await until(lambda: len(seen.deliveries) == 2)  # job 2 is acknowledged (the script ran out) while the retry waits
    await asyncio.sleep(0.5)
    assert cluster.committed(GROUP, "das.jobs.work.retry") is None, "the held retry is not committed"


async def test_a_message_past_the_delivery_limit_is_dead_lettered_with_its_reason_and_count(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made, settings={"retry_delay": 0.0, "delivery_limit": 2})
    seen = Recorder(t, Outcome.REQUEUE, Outcome.REQUEUE, Outcome.REQUEUE)
    await t.subscribe(JOBS, seen)
    await t.publish(JOBS, job(1))
    await until(lambda: len(cluster.records("das.dlq")) == 1)
    headers = {k: v.decode() for k, v in cluster.records("das.dlq")[0].headers}
    assert headers["x-death-reason"] == "delivery_limit" and headers["x-original-destination"] == JOBS
    assert headers["x-delivery-count"] == "3" and len(seen.deliveries) == 3


async def test_requeues_of_other_destinations_have_no_limit(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    t = await make(cluster, made, settings={"retry_delay": 0.0, "delivery_limit": 1})
    seen = Recorder(t, Outcome.REQUEUE, Outcome.REQUEUE, Outcome.REQUEUE, Outcome.ACK)
    await t.subscribe("results", seen)
    await t.publish("results", job(1))
    await until(lambda: len(seen.deliveries) == 4)
    assert cluster.records("das.dlq") == []


async def test_a_reject_is_copied_to_every_dead_letter_topic_and_the_original_is_committed(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    await t.subscribe(JOBS, Recorder(t, Outcome.REJECT))
    await t.publish(JOBS, job(7), headers={"x-custom": "kept"})
    await until(lambda: cluster.committed(GROUP, "das.jobs.work") == 1)
    for topic in ("das.dlq", "das.dlq.events"):
        [record] = cluster.records(topic)
        headers = {k: v.decode() for k, v in record.headers}
        assert headers["x-death-reason"] == "rejected" and headers["x-original-destination"] == JOBS
        assert headers["x-custom"] == "kept" and headers["x-delivery-count"] == "0"


async def test_a_dead_letter_that_is_rejected_again_is_dropped_not_looped(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    await t.subscribe("dlq", Recorder(t, Outcome.REJECT))
    await t.publish("dlq", job(1))
    await until(lambda: cluster.committed("das-das.dlq", "das.dlq") == 1)
    assert len(cluster.records("das.dlq")) == 1 and cluster.records("das.dlq.events") == []


async def test_a_failed_copy_leaves_the_original_uncommitted_so_it_is_not_lost(
    cluster: FakeCluster, made: list[KafkaTransport], monkeypatch: pytest.MonkeyPatch
) -> None:
    t = await make(cluster, made)
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    await t.subscribe(JOBS, hold)
    await t.publish(JOBS, job(1))
    await until(lambda: len(held) == 1)

    async def refuse(*args: Any, **kwargs: Any) -> None:
        raise KafkaError("refused")

    monkeypatch.setattr(t._producer, "send_and_wait", refuse)
    with pytest.raises(PublishNotConfirmed):
        await t.acknowledge(held[0], Outcome.REJECT)
    assert cluster.committed(GROUP, "das.jobs.work") is None


# --------------------------------------------------------------------------- what a handler sees


async def test_the_reserved_headers_never_reach_a_handler(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    caller, responder = await make(cluster, made, "caller"), await make(cluster, made, "responder")
    headers: list[dict[str, str]] = []

    async def answer(delivery: Delivery) -> Any:
        headers.append(dict(delivery.headers))
        return job(99)

    await responder.subscribe("control/ask", answer)
    await caller.request_reply("control/ask", job(1), timeout=5)
    assert headers and not {REPLY_TO_HEADER, COUNT_HEADER, NOT_BEFORE_HEADER, "das-correlation-id"} & set(headers[0])


async def test_a_message_that_cannot_be_decoded_is_delivered_with_its_parse_error(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    cluster.append("das.jobs.work", b"not a message", [], None)
    seen = Recorder(t, Outcome.REJECT)
    await t.subscribe(JOBS, seen)
    await until(lambda: len(seen.deliveries) == 1)
    [delivery] = seen.deliveries
    assert delivery.message is None and delivery.parse_error is not None and delivery.body == b"not a message"


async def test_each_call_gets_the_reply_to_its_own_request_over_one_reply_topic(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    caller, responder = await make(cluster, made, "caller"), await make(cluster, made, "responder")

    async def echo(delivery: Delivery) -> Any:
        return job(int(delivery.message.payload.job_id[1:]) + 100)  # type: ignore[union-attr]

    await responder.subscribe("control/ask", echo)
    replies = await asyncio.gather(*(caller.request_reply("control/ask", job(n), timeout=5) for n in (1, 2, 3)))
    assert [r.payload.job_id for r in replies] == ["j101", "j102", "j103"]  # type: ignore[union-attr]
    assert len([t for t in cluster.topics if t.startswith("das.reply.")]) == 1


async def test_the_reply_topic_is_removed_when_the_transport_closes(cluster: FakeCluster) -> None:
    caller = KafkaTransport(KafkaSettings(("f:1",)), factory=cluster.factory("c"))
    responder = KafkaTransport(KafkaSettings(("f:1",)), factory=cluster.factory("r"))
    await caller.start()
    await responder.start()

    async def echo(delivery: Delivery) -> Any:
        return job(1)

    await responder.subscribe("control/ask", echo)
    await caller.request_reply("control/ask", job(1), timeout=5)
    assert any(t.startswith("das.reply.") for t in cluster.topics)
    await caller.close()
    await responder.close()
    assert not any(t.startswith("das.reply.") for t in cluster.topics)


# --------------------------------------------------------------------------- close


async def test_closing_leaves_every_consumer_group_and_stops_the_tasks(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    await t.subscribe(JOBS, Recorder(t))
    await t.close()
    assert all(not group.members for group in cluster.groups.values())
    assert not t._tasks and not t._pollers
    with pytest.raises(TransportUnavailable):
        await t.publish(JOBS, job(1))


async def test_cancelling_a_subscription_stops_its_deliveries(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    t = await make(cluster, made)
    seen = Recorder(t)
    subscription = await t.subscribe(JOBS, seen)
    await subscription.cancel()
    await t.publish(JOBS, job(1))
    await asyncio.sleep(0.6)
    assert seen.deliveries == [] and subscription.active is False
    assert all(not group.members for group in cluster.groups.values())


def test_connection_errors_of_the_clients_are_what_the_adapter_treats_as_a_lost_connection() -> None:
    assert KafkaConnectionError in kafka.CONNECTION_ERRORS and OSError in kafka.CONNECTION_ERRORS


# --------------------------------------------------------------------------- tests added after the fault-injection pass


async def test_a_send_that_fails_on_a_connection_error_closes_the_gate(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    await t.publish(JOBS, job(1))  # the topic exists now, so the next failure is the send itself
    cluster.down.add("agent")
    with pytest.raises(TransportUnavailable):
        await t.publish(JOBS, job(2))
    assert t.connected is False and t.registry.is_open is False


async def test_a_topic_is_created_once_however_often_it_is_used(
    cluster: FakeCluster, made: list[KafkaTransport], monkeypatch: pytest.MonkeyPatch
) -> None:
    t = await make(cluster, made)
    calls: list[int] = []
    real = t._admin.create_topics

    async def counting(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        return await real(*args, **kwargs)

    monkeypatch.setattr(t._admin, "create_topics", counting)
    for n in range(3):
        await t.publish(JOBS, job(n))
    assert len(calls) == 1


async def test_topics_are_created_with_the_configured_replication_factor(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made, settings={"replication_factor": 3})
    await t.subscribe(JOBS, Recorder(t))
    assert {cluster.replication[name] for name in ("das.jobs.work", "das.jobs.work.high", "das.jobs.work.retry")} == {3}


async def test_the_high_priority_consumer_has_a_group_of_its_own(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    await t.subscribe(JOBS, Recorder(t))
    assert set(cluster.groups) == {GROUP, f"{GROUP}-high"}


async def test_a_revoke_forgets_what_was_in_flight_and_makes_older_deliveries_stale(
    cluster: FakeCluster, made: list[KafkaTransport]
) -> None:
    t = await make(cluster, made)
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    await t.subscribe(JOBS, hold)
    await t.publish(JOBS, job(1))
    await until(lambda: len(held) == 1)
    lane = next(iter(t._streams.values())).main
    partition = TopicPartition("das.jobs.work", 0)
    assert lane.tracker.in_flight(partition) == 1 and lane.generation.get(partition, 0) == 0
    await t._revoked(lane, [partition])
    assert lane.tracker.in_flight(partition) == 0 and lane.generation[partition] == 1
    await t.acknowledge(held[0])
    assert cluster.committed(GROUP, "das.jobs.work") is None, "the delivery is from before the revoke"


async def test_a_commit_refused_because_of_a_rebalance_is_dropped_not_retried_forever(
    cluster: FakeCluster, made: list[KafkaTransport], monkeypatch: pytest.MonkeyPatch
) -> None:
    t = await make(cluster, made)
    await t.subscribe(JOBS, Recorder(t))
    lane = next(iter(t._streams.values())).main
    partition = TopicPartition("das.jobs.work", 0)

    async def refuse(offsets: Any = None) -> None:
        raise CommitFailedError()

    monkeypatch.setattr(lane.consumer, "commit", refuse)
    lane.pending[partition] = 5
    await t._flush(lane)
    assert lane.pending == {}


async def test_polls_that_find_nothing_do_not_use_up_a_slot(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    t = await make(cluster, made)
    seen = Recorder(t)
    await t.subscribe(JOBS, seen, SubscribeOptions(max_in_flight=1))
    await asyncio.sleep(0.8)  # several polls that find nothing
    await t.publish(JOBS, job(1))
    await until(lambda: len(seen.deliveries) == 1)


async def test_a_failed_poll_gives_its_slot_back(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    t = await make(cluster, made)
    seen = Recorder(t)
    await t.subscribe(JOBS, seen, SubscribeOptions(max_in_flight=1))
    await asyncio.sleep(0.1)
    cluster.down.add("agent")
    await asyncio.sleep(0.5)
    cluster.down.discard("agent")
    await until(lambda: t.connected)
    await t.publish(JOBS, job(1))
    await until(lambda: len(seen.deliveries) == 1)


async def test_polling_pauses_while_the_connection_is_down(
    cluster: FakeCluster, made: list[KafkaTransport], monkeypatch: pytest.MonkeyPatch
) -> None:
    t = await make(cluster, made)
    await t.subscribe(JOBS, Recorder(t))
    stream = next(iter(t._streams.values()))
    polls: list[int] = []

    def count_polls(consumer: Any) -> None:
        real = consumer.getmany

        async def counting(*args: Any, **kwargs: Any) -> Any:
            polls.append(1)
            return await real(*args, **kwargs)

        monkeypatch.setattr(consumer, "getmany", counting)

    count_polls(stream.high.consumer)  # the high lane is polled first, so it is where a poll fails
    count_polls(stream.main.consumer)
    cluster.down.add("agent")
    await asyncio.sleep(0.7)  # a poll in flight notices, and the gate closes
    assert t.registry.is_open is False
    seen_so_far = len(polls)
    await asyncio.sleep(0.8)
    assert len(polls) == seen_so_far


async def test_starting_twice_keeps_the_same_clients(cluster: FakeCluster, made: list[KafkaTransport]) -> None:
    t = await make(cluster, made)
    producer, admin = t._producer, t._admin
    await t.start()
    assert t._producer is producer and t._admin is admin
