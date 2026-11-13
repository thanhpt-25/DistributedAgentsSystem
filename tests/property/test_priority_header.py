# Feature: das-multi-tenant, Property 8: Priority header
"""Property 8: Priority header.

For any Request priority and any sequence of dispatches, retries, takeovers and Replays, every Job dispatch carries the
Priority_Header if and only if the Request's priority is ``high``, and the dispatch payload is the one produced without
priority (it has no priority field and the wire body no such word). The RabbitMQ adapter maps ``high`` to AMQP priority
9 and everything else to none, never lets the reserved header reach the wire, and leaves the body byte for byte as it
was (a fake channel).

**Validates: Requirements 7.3, 7.4, 7.5**
"""

from __future__ import annotations

import asyncio
from typing import Any

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import example, given
from hypothesis import strategies as st

from das.manager.driver import DriverOutcome
from das.manager.machine import MachineConfig
from das.models import JobDispatchPayload
from das.state import ClientRequestSpec, StateStore
from das.transport.base import PRIORITY_HEADER
from das.transport.rabbitmq import RabbitMQTransport
from tests.unit.test_manager_driver import (
    ONE_JOB,
    TIMEOUT,
    FakeWorker,
    _true,
    make_env,
    steal_lease,
    wait_until,
)
from tests.unit.test_rabbitmq_transport import FakeBroker, heartbeat
from tests.unit.test_state_phase2_dlq import fields

DL = "b" * 32
#: The dispatch's fields as Phase 5 shipped them. ``output_schema`` (Phase 6 Req 1.3) is on the wire only for a
#: Structured_Job, and these Requests have none; the point here is that no priority field was ever added.
PAYLOAD_FIELDS = set(JobDispatchPayload.model_fields) - {"output_schema"}
#: Req 7.5, stated here and not read from the adapter: 5 to 9 are the "high" class of a RabbitMQ quorum queue, and 9 is
#: the one the adapter sets (spike G0a).
AMQP_HIGH = 9


class ScheduledWorker(FakeWorker):
    """A worker whose first ``fail_n`` dispatches fail and whose later ones succeed, whatever the Job."""

    fail_n = 0

    @property  # type: ignore[override]
    def status(self) -> str:
        return "Failed" if len(self.dispatches) <= self.fail_n else "Succeeded"


async def scenario(
    priority: str | None, max_attempts: int, fail_n: int, takeover: bool, replay: bool
) -> tuple[list[Any], list[Any]]:
    """One Request through failures, retries, an optional takeover and an optional Replay. Returns what the Manager
    put on the wire for the Jobs and the dispatches the worker decoded."""
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    store = StateStore(redis)
    env = await make_env(
        store, redis, plan_outcomes=(ONE_JOB,), summary_outcomes=("done",) * 4,
        machine_config=MachineConfig(max_attempts=max_attempts, base_backoff=0.01, max_backoff=0.05),  # no real waits
        lease_duration=1.0 if takeover else 30.0,
    )  # fmt: skip
    env.worker.__class__ = ScheduledWorker
    env.worker.fail_n = fail_n  # type: ignore[attr-defined]
    spec = ClientRequestSpec(fingerprint="cbabecbd", tenant="team-a", priority=priority)
    rid = (await store.submit_request("hello", spec)).request_id
    assert rid is not None
    acquired = await store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.ok and acquired.lease is not None
    task = env.drivers.on_claimed(acquired.lease)

    if takeover:  # the first dispatch is never answered; another Manager takes the Request over and re-dispatches
        env.worker.mode = "hold"
        await wait_until(lambda: _true(len(env.worker.dispatches) >= 1))
        await steal_lease(redis, store, rid)
        env.worker.mode = "reply"
        stored = await store.load_snapshot(rid)
        assert stored is not None and stored.lease is not None
        assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
        task = type(env.drivers)(env.drivers.deps, 30.0).on_claimed(stored.lease)
    outcome = await asyncio.wait_for(task, TIMEOUT)

    if replay and outcome is DriverOutcome.FAILED:  # a Failed Request is replayed from its dead letter
        failed = await store.load_snapshot(rid)
        assert failed is not None
        attempt = failed.jobs["a"].attempt  # the record names the attempt that failed, or the Replay is stale
        assert (await store.create_dead_letter(fields(DL, rid=rid, attempt=attempt))).created
        assert (await store.replay_dead_letter(DL)).outcome == "accepted"
        again = await store.acquire_lease(rid, "m1", 30.0)
        assert again.ok and again.lease is not None
        await asyncio.wait_for(env.drivers.on_claimed(again.lease), TIMEOUT)

    wire = [r for r in env.broker.wire if r.destination.startswith("jobs/")]
    return wire, list(env.worker.dispatches)


priorities = st.sampled_from([None, "normal", "high"])


@example(priority="high", max_attempts=3, fail_n=2, takeover=True, replay=True)  # every kind of dispatch at once
@example(priority="normal", max_attempts=1, fail_n=3, takeover=False, replay=True)
@given(
    priority=priorities,
    max_attempts=st.integers(1, 3),
    fail_n=st.integers(0, 3),
    takeover=st.booleans(),
    replay=st.booleans(),
)
def test_every_dispatch_carries_the_header_if_and_only_if_the_request_is_high(
    priority: str | None, max_attempts: int, fail_n: int, takeover: bool, replay: bool
) -> None:
    wire, decoded = asyncio.run(scenario(priority, max_attempts, fail_n, takeover, replay))
    assert wire, "the Request was never dispatched"
    for record in wire:
        carried = record.headers.get(PRIORITY_HEADER)
        assert (carried == "high") == (priority == "high"), (priority, dict(record.headers))
        assert carried in (None, "high")  # the header takes no other value
        assert PRIORITY_HEADER.encode() not in record.body and b"priority" not in record.body  # not in the payload
    for message in decoded:
        assert set(message.payload.model_dump()) == PAYLOAD_FIELDS  # the payload is the one without priority


# --------------------------------------------------------------------------- the adapter

header_values = st.one_of(st.sampled_from(["high", "normal", "urgent", "", "HIGH", " high"]), st.text(max_size=8))
other_headers = st.dictionaries(
    st.text(alphabet="abcdefghijk-", min_size=1, max_size=6).filter(lambda k: k != PRIORITY_HEADER),
    st.text(max_size=6),
    max_size=3,
)


async def publish(headers: dict[str, str] | None) -> Any:
    broker = FakeBroker()
    transport = RabbitMQTransport(url="amqp://guest:guest@localhost:5672/", connect=broker.connect, poll_interval=0.01)
    await transport.start()
    try:
        await transport.publish("jobs/code", heartbeat(), headers=headers)
        return broker.published[-1][2]
    finally:
        await transport.close()


@example(value="high", others={})
@example(value="normal", others={"k": "v"})
@given(value=st.none() | header_values, others=other_headers)
def test_the_adapter_maps_high_to_amqp_priority_9_and_everything_else_to_none(
    value: str | None, others: dict[str, str]
) -> None:
    headers = {**others, **({} if value is None else {PRIORITY_HEADER: value})}
    sent = asyncio.run(publish(headers))
    assert sent.priority == (AMQP_HIGH if value == "high" else 0)
    assert PRIORITY_HEADER not in sent.headers  # the reserved header never reaches the wire, whatever its value
    assert dict(sent.headers) == others  # every other header is kept as it was
    plain = asyncio.run(publish(None))
    assert sent.body == plain.body  # the codec output does not depend on the header
