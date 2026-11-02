"""Unit tests for das.transport.base and the in-memory fake transport (task 8.1).

Requirements: 8.1, 8.6, 8.7, 8.8
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import UTC, datetime

import pytest

from das.clock import FakeClock
from das.codec import MessageCodec, new_message
from das.models.results import ControlPayload, JobDispatchPayload
from das.transport.base import (
    TRANSPORT_OPERATIONS,
    Delivery,
    Outcome,
    PublishNotConfirmed,
    ReconnectPolicy,
    RequestTimeout,
    SubscribeOptions,
    Subscription,
    SubscriptionRegistry,
    Transport,
    TransportUnavailable,
    missing_operations,
)
from tests.fakes.memory_transport import (
    DEAD_LETTER_REASON_HEADER,
    ORIGINAL_DESTINATION_HEADER,
    MemoryBroker,
    MemoryTransport,
)

TS = datetime(2025, 1, 1, tzinfo=UTC)


def dispatch(job_id: str = "j1", attempt: int = 1):
    return new_message(
        JobDispatchPayload(
            request_id="r1",
            job_id=job_id,
            attempt=attempt,
            instruction="do it",
            required_capabilities=["research"],
        ),
        correlation_id="r1",
        message_id=f"dispatch-{job_id}-{attempt}",
        timestamp=TS,
    )


def control(command: str = "cancel_job"):
    return new_message(
        ControlPayload(command=command, request_id="r1", job_id="j1"),
        correlation_id="r1",
        message_id=f"control-{command}",
        timestamp=TS,
    )


class Recorder:
    """Handler that records deliveries and optionally settles them."""

    def __init__(self, transport: MemoryTransport, outcome: Outcome | None = Outcome.ACK):
        self.transport = transport
        self.outcome = outcome
        self.deliveries: list[Delivery] = []

    async def __call__(self, delivery: Delivery) -> None:
        self.deliveries.append(delivery)
        if self.outcome is not None:
            await self.transport.acknowledge(delivery, self.outcome)


class SimpleLimiter:
    """Minimal InFlightLimiterLike used to check the shared-limit contract."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.used = 0

    async def acquire(self) -> None:  # pragma: no cover - not used by the fake
        while not self.try_acquire():
            await asyncio.sleep(0.01)

    def try_acquire(self) -> bool:
        if self.used >= self.capacity:
            return False
        self.used += 1
        return True

    def release(self) -> None:
        self.used -= 1


async def started(broker: MemoryBroker, agent_id: str, **kwargs) -> MemoryTransport:
    transport = MemoryTransport(broker, agent_id=agent_id, **kwargs)
    await transport.start()
    return transport


# --------------------------------------------------------------------------- base


class TestReconnectPolicy:
    def test_default_schedule_doubles_and_caps_at_30(self):
        policy = ReconnectPolicy()
        delays = [policy.delay(k) for k in range(1, 9)]
        assert delays == [1, 2, 4, 8, 16, 30, 30, 30]

    def test_custom_cap(self):
        policy = ReconnectPolicy(max_interval=5)
        it = policy.delays()
        assert [next(it) for _ in range(5)] == [1, 2, 4, 5, 5]

    def test_cap_of_one_is_constant(self):
        assert [ReconnectPolicy(1).delay(k) for k in (1, 2, 50)] == [1, 1, 1]

    def test_large_attempt_does_not_overflow(self):
        assert ReconnectPolicy(300).delay(10_000) == 300

    @pytest.mark.parametrize("value", [0, 0.5, 301, True, "30"])
    def test_rejects_out_of_range_cap(self, value):
        with pytest.raises(ValueError, match="max_reconnect_interval"):
            ReconnectPolicy(value)

    @pytest.mark.parametrize("attempt", [0, -1])
    def test_rejects_non_positive_attempt(self, attempt):
        with pytest.raises(ValueError):
            ReconnectPolicy().delay(attempt)


class TestSubscribeOptions:
    def test_rejects_non_positive_max_in_flight(self):
        with pytest.raises(ValueError):
            SubscribeOptions(max_in_flight=0)

    def test_limiter_protocol(self):
        from das.transport.base import InFlightLimiterLike

        assert isinstance(SimpleLimiter(1), InFlightLimiterLike)


class TestSubscriptionRegistry:
    async def test_restore_reopens_gate_only_after_every_resubscribe(self):
        registry = SubscriptionRegistry()
        subs = [Subscription(f"d{i}", Recorder(None), SubscribeOptions()) for i in range(3)]
        for s in subs:
            registry.add(s)
        registry.mark_down()
        seen: list[tuple[str, bool]] = []

        async def resubscribe(sub: Subscription) -> None:
            seen.append((sub.destination, registry.is_open))

        await registry.restore(resubscribe)
        assert seen == [("d0", False), ("d1", False), ("d2", False)]
        assert registry.is_open

    async def test_failed_restore_keeps_gate_closed(self):
        registry = SubscriptionRegistry()
        registry.add(Subscription("d", Recorder(None), SubscribeOptions()))

        async def boom(_sub):
            raise ConnectionError("still down")

        with pytest.raises(ConnectionError):
            await registry.restore(boom)
        assert not registry.is_open

    async def test_cancelled_subscriptions_are_not_active(self):
        registry = SubscriptionRegistry()
        sub = Subscription("d", Recorder(None), SubscribeOptions())
        registry.add(sub)
        await sub.cancel()
        assert registry.active() == []


def test_missing_operations():
    class Partial:
        async def publish(self): ...
        async def acknowledge(self): ...

    assert missing_operations(Partial) == ["subscribe", "request_reply"]
    assert missing_operations(MemoryTransport) == []


def test_memory_transport_matches_protocol_signatures():
    assert isinstance(MemoryTransport(MemoryBroker()), Transport)
    for op in TRANSPORT_OPERATIONS:
        assert inspect.signature(getattr(MemoryTransport, op)) == inspect.signature(
            getattr(Transport, op)
        ), op


# --------------------------------------------------------------------------- fake


class TestMemoryTransport:
    async def test_publish_and_ack_round_trip_through_codec(self):
        broker = MemoryBroker()
        manager = await started(broker, "manager-1")
        worker = await started(broker, "worker-1")
        recorder = Recorder(worker)
        await worker.subscribe("jobs/research", recorder)

        message = dispatch()
        await manager.publish("jobs/research", message, headers={"x": "1"})
        await broker.settle()

        assert len(recorder.deliveries) == 1
        delivery = recorder.deliveries[0]
        assert delivery.message == message
        assert delivery.headers == {"x": "1"}
        assert delivery.delivery_count == 0 and not delivery.redelivered
        assert broker.wire_for("jobs/research")[0].body == MessageCodec().encode(message)
        assert broker.pending("jobs/research") == []
        assert broker.unacked_count("jobs/research") == 0

    async def test_competing_consumers_get_each_message_once(self):
        broker = MemoryBroker()
        sender = await started(broker, "m")
        workers = [await started(broker, f"w{i}") for i in range(3)]
        recorders = [Recorder(w) for w in workers]
        for w, r in zip(workers, recorders, strict=True):
            await w.subscribe("jobs/research", r)

        for i in range(9):
            await sender.publish("jobs/research", dispatch(f"j{i}"))
        await broker.settle()

        ids = [d.message.payload.job_id for r in recorders for d in r.deliveries]
        assert sorted(ids) == sorted(f"j{i}" for i in range(9))
        assert all(len(r.deliveries) == 3 for r in recorders)

    async def test_requeue_increments_delivery_count(self):
        broker = MemoryBroker()
        t = await started(broker, "w")
        seen: list[Delivery] = []

        async def handler(delivery: Delivery) -> None:
            seen.append(delivery)
            outcome = Outcome.REQUEUE if len(seen) < 3 else Outcome.ACK
            await t.acknowledge(delivery, outcome)

        await t.subscribe("q", handler)
        await t.publish("q", dispatch())
        await broker.settle()
        assert [d.delivery_count for d in seen] == [0, 1, 2]
        assert [d.redelivered for d in seen] == [False, True, True]
        assert broker.pending("q") == []

    async def test_reject_routes_to_dead_letter_destination(self):
        broker = MemoryBroker(dead_letter_destinations=("dlq", "dlq-events"))
        t = await started(broker, "w")
        await t.subscribe("jobs/research", Recorder(t, Outcome.REJECT))
        await t.publish("jobs/research", dispatch())
        await broker.settle()

        assert broker.pending("jobs/research") == []
        for dlq in ("dlq", "dlq-events"):
            [headers] = broker.pending_headers(dlq)
            assert headers[DEAD_LETTER_REASON_HEADER] == "rejected"
            assert headers[ORIGINAL_DESTINATION_HEADER] == "jobs/research"
            assert broker.pending_messages(dlq)[0] == dispatch()

    async def test_delivery_limit_dead_letters_after_limit_returns(self):
        broker = MemoryBroker()
        broker.set_delivery_limit("q", 2)  # max_attempts = 3
        t = await started(broker, "w")
        recorder = Recorder(t, Outcome.REQUEUE)
        await t.subscribe("q", recorder)
        await t.publish("q", dispatch())
        await broker.settle()

        assert len(recorder.deliveries) == 3
        [headers] = broker.pending_headers("dlq")
        assert headers[DEAD_LETTER_REASON_HEADER] == "delivery_limit"
        assert headers["x-delivery-count"] == "3"

    async def test_malformed_bytes_delivered_with_parse_error(self):
        broker = MemoryBroker()
        t = await started(broker, "w")
        recorder = Recorder(t, Outcome.REJECT)
        await t.subscribe("q", recorder)
        broker.publish_raw("q", b"not json")
        await broker.settle()

        [delivery] = recorder.deliveries
        assert delivery.message is None
        assert delivery.parse_error is not None
        assert delivery.body == b"not json"
        assert len(broker.pending("dlq")) == 1

    async def test_publish_while_down_raises_and_sends_nothing(self):
        broker = MemoryBroker()
        t = await started(broker, "m")
        t.simulate_drop()
        with pytest.raises(TransportUnavailable):
            await t.publish("q", dispatch())
        with pytest.raises(TransportUnavailable):
            await t.request_reply("w", control(), timeout=0.01)
        assert broker.wire == []
        assert broker.pending("q") == []

    async def test_publish_before_start_is_unavailable(self):
        with pytest.raises(TransportUnavailable):
            await MemoryTransport(MemoryBroker()).publish("q", dispatch())

    async def test_injected_publish_failures(self):
        broker = MemoryBroker()
        t = await started(broker, "m")
        t.fail_next_publishes(2)
        for _ in range(2):
            with pytest.raises(PublishNotConfirmed):
                await t.publish("q", dispatch())
        await t.publish("q", dispatch())
        assert len(broker.pending("q")) == 1

    async def test_drop_returns_unacked_to_another_consumer(self):
        broker = MemoryBroker()
        w1 = await started(broker, "w1")
        w2 = await started(broker, "w2")
        held = Recorder(w1, outcome=None)  # never acks
        await w1.subscribe("q", held)
        await w1.publish("q", dispatch())
        await broker.settle()
        assert len(held.deliveries) == 1 and broker.unacked_count("q") == 1

        taker = Recorder(w2)
        await w2.subscribe("q", taker)
        w1.simulate_drop()
        await broker.settle()

        [redelivery] = taker.deliveries
        assert redelivery.message == dispatch()  # attempt unchanged
        assert redelivery.delivery_count == 1 and redelivery.redelivered
        # A late ack from the dropped consumer is rejected while it is down.
        with pytest.raises(TransportUnavailable):
            await w1.acknowledge(held.deliveries[0])

    async def test_reconnect_restores_subscriptions_before_delivery(self):
        broker = MemoryBroker()
        sender = await started(broker, "m")
        w = await started(broker, "w")
        r1, r2 = Recorder(w), Recorder(w)
        await w.subscribe("a", r1)
        await w.subscribe("b", r2)
        w.simulate_drop()
        await sender.publish("a", dispatch("ja"))
        await sender.publish("b", dispatch("jb"))
        await broker.settle()
        assert r1.deliveries == [] and r2.deliveries == []

        w.events.clear()
        await w.simulate_reconnect()
        await broker.settle()

        kinds = [e[0] for e in w.events]
        assert kinds[:3] == ["resubscribe", "resubscribe", "restored"]
        assert sorted(kinds[3:]) == ["deliver", "deliver"]
        assert [d.message.payload.job_id for d in r1.deliveries] == ["ja"]
        assert [d.message.payload.job_id for d in r2.deliveries] == ["jb"]

    async def test_cancelled_subscription_not_restored(self):
        broker = MemoryBroker()
        w = await started(broker, "w")
        sub = await w.subscribe("a", Recorder(w))
        await w.subscribe("b", Recorder(w))
        await sub.cancel()
        w.simulate_drop()
        await w.simulate_reconnect()
        assert [s.destination for s in w.subscriptions] == ["b"]

    async def test_reconnect_with_backoff_uses_policy(self):
        clock = FakeClock()
        broker = MemoryBroker()
        w = await started(broker, "w", clock=clock, reconnect_policy=ReconnectPolicy(5))
        w.simulate_drop()
        delays = await w.reconnect_with_backoff(failures=4)
        assert delays == [1, 2, 4, 5, 5]
        assert clock.sleeps == delays
        assert w.connected

    async def test_max_in_flight_limits_unacked(self):
        broker = MemoryBroker()
        sender = await started(broker, "m")
        w = await started(broker, "w")
        held = Recorder(w, outcome=None)
        await w.subscribe("q", held, SubscribeOptions(max_in_flight=2))
        for i in range(5):
            await sender.publish("q", dispatch(f"j{i}"))
        await broker.settle()
        assert len(held.deliveries) == 2 and len(broker.pending("q")) == 3

        await w.acknowledge(held.deliveries[0])
        await broker.settle()
        assert len(held.deliveries) == 3

    async def test_shared_limiter_caps_total_across_subscriptions(self):
        broker = MemoryBroker()
        sender = await started(broker, "m")
        w = await started(broker, "w")
        limiter = SimpleLimiter(2)
        held = Recorder(w, outcome=None)
        options = SubscribeOptions(limiter=limiter)
        await w.subscribe("jobs/a", held, options)
        await w.subscribe("jobs/b", held, options)
        for dest in ("jobs/a", "jobs/b", "jobs/a", "jobs/b"):
            await sender.publish(dest, dispatch())
        await broker.settle()
        assert len(held.deliveries) == 2 and limiter.used == 2 and w.in_flight() == 2

        await w.acknowledge(held.deliveries[0])
        await broker.settle()
        assert len(held.deliveries) == 3 and limiter.used == 2

        w.simulate_drop()  # lost deliveries release their slots
        assert limiter.used == 0

    async def test_double_ack_is_counted_as_stale(self):
        broker = MemoryBroker()
        w = await started(broker, "w")
        recorder = Recorder(w)
        await w.subscribe("q", recorder)
        await w.publish("q", dispatch())
        await broker.settle()
        await w.acknowledge(recorder.deliveries[0])
        assert w.stale_acknowledgements == 1

    async def test_request_reply_returns_handler_reply(self):
        broker = MemoryBroker()
        gateway = await started(broker, "gateway")
        manager = await started(broker, "manager-1")
        reply = control("cancel_request")

        async def handler(delivery: Delivery):
            assert delivery.message == control()
            return reply

        await manager.subscribe("manager-1", handler)
        result = await gateway.request_reply("manager-1", control(), timeout=1)
        assert result == reply
        kinds = [r.kind for r in broker.wire_for("manager-1")]
        assert kinds == ["request", "reply"]

    async def test_request_reply_timeout_names_destination(self):
        broker = MemoryBroker()
        caller = await started(broker, "caller")
        callee = await started(broker, "callee")

        async def slow(_delivery):
            await asyncio.sleep(1)

        await callee.subscribe("callee", slow)
        with pytest.raises(RequestTimeout) as exc:
            await caller.request_reply("callee", control(), timeout=0.02)
        assert exc.value.destination == "callee"
        assert "callee" in str(exc.value)

    async def test_request_reply_no_subscriber_times_out(self):
        caller = await started(MemoryBroker(), "caller")
        with pytest.raises(RequestTimeout):
            await caller.request_reply("nobody", control(), timeout=0.01)

    async def test_request_reply_none_reply_times_out(self):
        broker = MemoryBroker()
        caller = await started(broker, "caller")
        callee = await started(broker, "callee")

        async def silent(_delivery):
            return None

        await callee.subscribe("callee", silent)
        with pytest.raises(RequestTimeout):
            await caller.request_reply("callee", control(), timeout=0.02)

    def test_request_reply_timeout_config_range(self):
        with pytest.raises(ValueError, match="request_reply_timeout"):
            MemoryTransport(MemoryBroker(), request_reply_timeout=0)
        with pytest.raises(ValueError, match="request_reply_timeout"):
            MemoryTransport(MemoryBroker(), request_reply_timeout=301)

    async def test_close_returns_unacked_and_stops_delivery(self):
        broker = MemoryBroker()
        sender = await started(broker, "m")
        w = await started(broker, "w")
        held = Recorder(w, outcome=None)
        await w.subscribe("q", held)
        await sender.publish("q", dispatch())
        await broker.settle()
        await w.close()
        assert len(broker.pending("q")) == 1
        assert w.subscriptions == []
        assert not w.connected

    async def test_handler_exception_is_recorded(self):
        broker = MemoryBroker()
        w = await started(broker, "w")

        async def bad(_delivery):
            raise RuntimeError("boom")

        await w.subscribe("q", bad)
        await w.publish("q", dispatch())
        await broker.settle()
        assert [str(e) for e in w.handler_errors] == ["boom"]
        assert broker.unacked_count("q") == 1
