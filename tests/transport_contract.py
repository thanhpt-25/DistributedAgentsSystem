"""The behaviour every Transport adapter must have (das-scale-resilience task 9.1; Phase 7 Req 6.2, 2.1, 2.6 to 2.8).

A subclass supplies the ``rig`` fixture, an object with:

* ``await rig.new(agent_id)``: a started Transport on the shared broker (``agent_id`` names it);
* ``rig.supports_priority``: whether the broker has a high-priority class (the header is then consumed);
* ``await rig.drop(t)`` and ``await rig.reconnect(t)``: lose and regain ``t``'s connection;
* ``rig.redelivery_delay``: how long an unacknowledged Message takes to come back (seconds);
* ``await rig.close()``.

The tests use only the Transport protocol, so a new adapter is one rig away from the whole contract: publish and
subscribe round-trip, acknowledge, requeue, reject to the dead-letter destination, competing consumers, request-reply,
the in-flight limit, priority, and a dropped connection that comes back."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from das.codec import new_message
from das.codec.message_codec import TypedMessage
from das.models.results import JobDispatchPayload
from das.transport.base import (
    PRIORITY_HEADER, PRIORITY_HIGH, Delivery, Outcome, RequestTimeout, SubscribeOptions, Transport,
    TransportUnavailable,
)

JOBS = "jobs/contract"


def job(n: int = 1, request_id: str = "r") -> TypedMessage:
    return new_message(
        JobDispatchPayload(
            request_id=request_id, job_id=f"j{n}", attempt=1, instruction=f"do {n}", required_capabilities=["contract"]
        ),
        correlation_id=request_id,
    )


#: How long ``until`` waits by default; a rig on a real broker (a cold Kafka group takes seconds) raises it.
UNTIL_TIMEOUT = 8.0


async def until(condition: Callable[[], bool], timeout: float | None = None) -> None:
    async with asyncio.timeout(UNTIL_TIMEOUT if timeout is None else timeout):
        while not condition():
            await asyncio.sleep(0.01)


class Recorder:
    """A handler that records deliveries and settles each with the next outcome of a script (ACK by default)."""

    def __init__(self, transport: Transport, *outcomes: Outcome) -> None:
        self.transport = transport
        self.outcomes = list(outcomes)
        self.deliveries: list[Delivery] = []

    async def __call__(self, delivery: Delivery) -> None:
        self.deliveries.append(delivery)
        await self.transport.acknowledge(delivery, self.outcomes.pop(0) if self.outcomes else Outcome.ACK)

    def ids(self) -> list[str]:
        return [d.message.payload.job_id for d in self.deliveries if d.message is not None]  # type: ignore[union-attr]


class TransportContract:
    """Subclass and provide a ``rig`` fixture."""

    @pytest.fixture
    async def pair(self, rig: Any) -> tuple[Transport, Transport]:
        return await rig.new("publisher"), await rig.new("consumer")

    # --------------------------------------------------------------------- round trip, ack, requeue, reject

    async def test_a_published_message_reaches_the_subscriber_intact(self, pair: tuple[Transport, Transport]) -> None:
        publisher, consumer = pair
        seen = Recorder(consumer)
        await consumer.subscribe(JOBS, seen)
        message = job(1)
        await publisher.publish(JOBS, message)
        await until(lambda: len(seen.deliveries) == 1)
        [delivery] = seen.deliveries
        assert delivery.message == message and delivery.destination == JOBS and delivery.parse_error is None
        assert delivery.delivery_count == 0 and delivery.redelivered is False

    async def test_messages_published_before_anyone_subscribed_are_delivered(
        self, pair: tuple[Transport, Transport]
    ) -> None:
        publisher, consumer = pair
        await publisher.publish(JOBS, job(1))
        seen = Recorder(consumer)
        await consumer.subscribe(JOBS, seen)
        await until(lambda: seen.ids() == ["j1"])

    async def test_custom_headers_reach_the_subscriber(self, pair: tuple[Transport, Transport]) -> None:
        publisher, consumer = pair
        seen = Recorder(consumer)
        await consumer.subscribe(JOBS, seen)
        await publisher.publish(JOBS, job(1), headers={"x-custom": "value"})
        await until(lambda: len(seen.deliveries) == 1)
        assert seen.deliveries[0].headers["x-custom"] == "value"

    async def test_an_acknowledged_message_is_not_delivered_again(
        self, rig: Any, pair: tuple[Transport, Transport]
    ) -> None:
        publisher, consumer = pair
        seen = Recorder(consumer)
        await consumer.subscribe(JOBS, seen)
        await publisher.publish(JOBS, job(1))
        await until(lambda: len(seen.deliveries) == 1)
        await asyncio.sleep(rig.redelivery_delay * 2 + 0.3)
        assert len(seen.deliveries) == 1

    async def test_a_requeued_message_comes_back_marked_as_redelivered(self, pair: tuple[Transport, Transport]) -> None:
        publisher, consumer = pair
        seen = Recorder(consumer, Outcome.REQUEUE, Outcome.ACK)
        await consumer.subscribe(JOBS, seen)
        await publisher.publish(JOBS, job(1))
        await until(lambda: len(seen.deliveries) == 2)
        first, second = seen.deliveries
        assert first.redelivered is False and first.delivery_count == 0
        assert second.redelivered is True and second.delivery_count == 1 and second.message == first.message

    async def test_a_rejected_message_goes_to_the_dead_letter_destinations_with_its_reason(
        self, pair: tuple[Transport, Transport]
    ) -> None:
        publisher, consumer = pair
        await consumer.subscribe(JOBS, Recorder(consumer, Outcome.REJECT))
        dead = Recorder(consumer)
        await consumer.subscribe("dlq", dead)
        message = job(7)
        await publisher.publish(JOBS, message)
        await until(lambda: len(dead.deliveries) == 1)
        [letter] = dead.deliveries
        assert letter.message == message
        assert letter.headers["x-death-reason"] == "rejected" and letter.headers["x-original-destination"] == JOBS

    # --------------------------------------------------------------------- competing consumers, limits, priority

    async def test_each_message_goes_to_exactly_one_of_two_competing_consumers(self, rig: Any) -> None:
        publisher, first, second = await rig.new("publisher"), await rig.new("c1"), await rig.new("c2")
        one, two = Recorder(first), Recorder(second)
        await first.subscribe(JOBS, one)
        await second.subscribe(JOBS, two)
        for n in range(12):
            await publisher.publish(JOBS, job(n))
        await until(lambda: len(one.deliveries) + len(two.deliveries) >= 12)
        await asyncio.sleep(0.3)
        got = one.ids() + two.ids()
        assert sorted(got) == sorted(f"j{n}" for n in range(12)), "every message exactly once, none twice"

    async def test_max_in_flight_holds_back_the_next_message_until_one_is_settled(
        self, pair: tuple[Transport, Transport]
    ) -> None:
        publisher, consumer = pair
        held: list[Delivery] = []

        async def hold(delivery: Delivery) -> None:
            held.append(delivery)

        await consumer.subscribe(JOBS, hold, SubscribeOptions(max_in_flight=1))
        await publisher.publish(JOBS, job(1))
        await publisher.publish(JOBS, job(2))
        await until(lambda: len(held) == 1)
        await asyncio.sleep(0.4)
        assert len(held) == 1, "the second message must wait for a free slot"
        await consumer.acknowledge(held[0])
        await until(lambda: len(held) == 2)

    async def test_a_high_priority_message_is_delivered_before_earlier_normal_ones(self, rig: Any) -> None:
        if not rig.supports_priority:
            pytest.skip(f"{rig.name} has no priority class")
        publisher, consumer = await rig.new("publisher"), await rig.new("consumer")
        for n in (1, 2, 3):
            await publisher.publish(JOBS, job(n))
        await publisher.publish(JOBS, job(9), headers={PRIORITY_HEADER: PRIORITY_HIGH})
        seen = Recorder(consumer)
        await consumer.subscribe(JOBS, seen)
        await until(lambda: len(seen.deliveries) == 4)
        assert seen.ids()[0] == "j9"
        assert PRIORITY_HEADER not in seen.deliveries[0].headers  # consumed by the adapter, never delivered

    # --------------------------------------------------------------------- request-reply

    async def test_request_reply_returns_the_handlers_reply(self, pair: tuple[Transport, Transport]) -> None:
        caller, responder = pair
        answer = job(42, "answer")

        async def answer_it(delivery: Delivery) -> TypedMessage:
            return answer

        await responder.subscribe("control/ask", answer_it)
        assert await caller.request_reply("control/ask", job(1), timeout=5) == answer

    async def test_request_reply_without_a_reply_times_out_naming_the_destination(
        self, pair: tuple[Transport, Transport]
    ) -> None:
        caller, responder = pair

        async def stay_silent(delivery: Delivery) -> None:
            return None

        await responder.subscribe("control/ask", stay_silent)
        with pytest.raises(RequestTimeout) as info:
            await caller.request_reply("control/ask", job(1), timeout=1.0)
        assert info.value.destination == "control/ask"

    async def test_request_reply_with_nobody_listening_times_out(self, pair: tuple[Transport, Transport]) -> None:
        caller, _ = pair
        with pytest.raises(RequestTimeout):
            await caller.request_reply("control/nobody", job(1), timeout=1.0)

    # --------------------------------------------------------------------- a dropped connection

    async def test_publish_while_the_connection_is_down_raises_and_works_again_after_reconnect(
        self, rig: Any, pair: tuple[Transport, Transport]
    ) -> None:
        publisher, consumer = pair
        seen = Recorder(consumer)
        await consumer.subscribe(JOBS, seen)
        await rig.drop(publisher)
        with pytest.raises(TransportUnavailable):
            await publisher.publish(JOBS, job(1))
        await rig.reconnect(publisher)
        await publisher.publish(JOBS, job(2))
        await until(lambda: seen.ids() == ["j2"])

    async def test_a_subscriber_keeps_receiving_after_its_connection_comes_back(self, rig: Any) -> None:
        publisher, consumer = await rig.new("publisher"), await rig.new("consumer")
        seen = Recorder(consumer)
        await consumer.subscribe(JOBS, seen)
        await publisher.publish(JOBS, job(1))
        await until(lambda: seen.ids() == ["j1"])
        await rig.drop(consumer)
        await rig.reconnect(consumer)
        await publisher.publish(JOBS, job(2))
        await until(lambda: seen.ids() == ["j1", "j2"])
