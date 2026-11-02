# Feature: das-observability-operations, Property 18: Mỗi dead letter đúng một Dead_Letter_Record
"""Property 18: every dead letter yields exactly one Dead_Letter_Record.

For any set of dead-lettered Messages (dispatch exhausted, delivery-limit, malformed
bytes of up to 2 x ``max_message_bytes``) and any delivery sequence with redeliveries,
duplicates and interleaving across 1-5 ``DeadLetterRecorder`` sharing one fakeredis
server, with Redis errors injected at random (each one leads to ``REQUEUE``) and
connection drops between record write and ACK:

- once every Message is ACKed, the number of Dead_Letter_Records equals the number
  of distinct Messages;
- each Message maps to exactly one record with the right ``reason``;
- ``das_dlq_records_created_total`` summed over every recorder equals the record count;
- for malformed, ``len(raw) == min(original_size, max_message_bytes)`` and
  ``truncated`` iff ``original_size > max_message_bytes``.

The recorders run the real ``create_dead_letter.lua`` (fakeredis[lua]) through
:class:`MemoryTransport` instances on one :class:`MemoryBroker`.

**Validates: Requirements 3.8, 12.2, 12.3**
"""

from __future__ import annotations

import asyncio
import base64
import random

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import HealthCheck, event, given, settings
from hypothesis import strategies as st
from prometheus_client.parser import text_string_to_metric_families
from redis.exceptions import ConnectionError as RedisConnectionError

from das.clock import FakeClock
from das.codec import MessageCodec, new_message
from das.manager.ingest import DEAD_LETTER_REASON_HEADER, DeadLetterRecorder
from das.models import JobDispatchPayload, JobResultPayload, TokenUsage
from das.models.dlq import DeadLetterFields
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.ops.dlq import LAST_ERROR_HEADER, dead_letter_id, dead_letter_identity
from das.state import StateStore
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

CODEC = MessageCodec()
#: ``x-death-reason`` header value -> expected Dead_Letter_Reason.
HEADER_REASONS = {
    "max_attempts": "retries_exhausted",
    "delivery_limit": "delivery_limit",
    "rejected": "malformed",
}


class FlakyStore:
    """Real StateStore whose ``create_dead_letter`` raises a Redis error before the
    write with probability ``p`` (State_Store unreachable, Req 12.11)."""

    def __init__(self, store: StateStore, rng: random.Random, p: float) -> None:
        self._store = store
        self._rng = rng
        self._p = p
        self.failures = 0

    async def create_dead_letter(self, fields: DeadLetterFields):  # noqa: ANN201
        await asyncio.sleep(0)  # let other recorders interleave
        if self._rng.random() < self._p:
            self.failures += 1
            raise RedisConnectionError("injected redis failure")
        return await self._store.create_dead_letter(fields)


def _parsed_body(kind: str, i: int) -> bytes:
    rid = f"req-{i}"
    if kind == "dispatch":
        payload = JobDispatchPayload(
            request_id=rid, job_id=f"j{i}", attempt=1 + i % 3, instruction="i",
            required_capabilities=["search"],
        )
    else:
        payload = JobResultPayload(
            request_id=rid, job_id=f"j{i}", attempt=1, worker_id="w1", status="Failed",
            content="", error="e", usage=TokenUsage(), duration_ms=1,
        )
    return CODEC.encode(new_message(payload, correlation_id=rid))


@st.composite
def dead_letters(draw: st.DrawFn, max_bytes: int) -> list[tuple[bytes, str]]:
    """Distinct dead-lettered Messages as ``(body, x-death-reason)``."""
    parsed = draw(
        st.lists(
            st.tuples(st.sampled_from(["dispatch", "result"]), st.sampled_from(list(HEADER_REASONS))),
            max_size=6,
        )
    )
    raws = draw(
        st.lists(
            # Leading NUL: never a valid envelope, never equal to a parsed body.
            st.binary(min_size=0, max_size=2 * max_bytes - 1).map(lambda b: b"\x00" + b),
            max_size=6,
            unique=True,
        )
    )
    out = [(_parsed_body(kind, i), header) for i, (kind, header) in enumerate(parsed)]
    out += [(raw, "rejected") for raw in raws]
    return out


def _created_total(recorder: MetricsRecorder) -> float:
    total = 0.0
    for family in text_string_to_metric_families(recorder.render()[0].decode()):
        for sample in family.samples:
            if sample.name == "das_dlq_records_created_total":
                total += sample.value
    return total


async def _scenario(data: st.DataObject, managers: int, max_bytes: int, p_fail: float) -> None:
    server = FakeServer()
    admin = FakeRedis(server=server, decode_responses=True)
    reader = StateStore(admin)
    rng = random.Random(data.draw(st.integers(0, 2**32 - 1), label="seed"))
    broker = MemoryBroker()
    transports: list[MemoryTransport] = []
    recorders: list[MetricsRecorder] = []
    clients: list[FakeRedis] = []
    for i in range(managers):
        client = FakeRedis(server=server, decode_responses=True)
        clients.append(client)
        transport = MemoryTransport(broker, agent_id=f"m{i}")
        await transport.start()
        metrics = MetricsRecorder(clock=FakeClock())
        await DeadLetterRecorder(
            transport,
            FlakyStore(StateStore(client), rng, p_fail),  # type: ignore[arg-type]
            ManagerMetrics(metrics),
            max_message_bytes=max_bytes,
        ).start()
        transports.append(transport)
        recorders.append(metrics)

    messages = data.draw(dead_letters(max_bytes), label="messages")
    # Each Message is delivered 1-3 times (broker redelivery / duplicate copies),
    # in a random interleaved order, optionally with connection drops in between.
    copies = [m for m in messages for _ in range(data.draw(st.integers(1, 3)))]
    order = data.draw(st.permutations(copies), label="order")
    drops = 0
    for body, header in order:
        headers = {DEAD_LETTER_REASON_HEADER: header}
        if header == "max_attempts":
            headers[LAST_ERROR_HEADER] = "boom"
        broker.publish_raw("dlq", body, headers)
        if data.draw(st.integers(0, 4)) == 0:
            # Drop a Manager's connection while its handlers are in flight: the
            # record may be written but the ACK fails, so the Message is redelivered.
            victim = transports[data.draw(st.integers(0, managers - 1))]
            await asyncio.sleep(0)
            victim.simulate_drop()
            drops += 1
        if data.draw(st.booleans()):
            await asyncio.sleep(0)
        for t in transports:
            if not t.connected and data.draw(st.booleans()):
                await t.simulate_reconnect()
    for t in transports:
        await t.simulate_reconnect()
    await broker.settle(timeout=20.0)
    for t in transports:
        await t.simulate_reconnect()
    await broker.settle(timeout=20.0)

    # Every Message has been ACKed.
    assert broker.pending("dlq") == [] and broker.unacked_count("dlq") == 0

    expected: dict[str, tuple[bytes, str]] = {}
    for body, header in messages:
        msg = None if body.startswith(b"\x00") else CODEC.decode(body)
        dlid = dead_letter_id(dead_letter_identity(msg, body))
        expected.setdefault(dlid, (body, HEADER_REASONS[header]))
    assert len(expected) == len(messages)

    page = await reader.list_dead_letters(limit=1000)
    assert page.next_after is None
    stored = {r.dead_letter_id: r for r in page.records}
    assert set(stored) == set(expected)
    for dlid, (body, reason) in expected.items():
        record = stored[dlid]
        assert record.reason == reason
        if reason == "malformed":
            assert record.raw_b64 is not None and record.original_size == len(body)
            raw = base64.b64decode(record.raw_b64)
            assert len(raw) == min(record.original_size, max_bytes)
            assert raw == body[:max_bytes]
            assert record.truncated is (record.original_size > max_bytes)
    assert sum(_created_total(r) for r in recorders) == len(stored)

    event(f"managers={managers}")
    event("redis failures injected" if p_fail > 0 else "no redis failures")
    event("connection drops" if drops else "no drops")
    event("truncated malformed" if any(r.truncated for r in stored.values()) else "no truncation")
    for t in transports:
        await t.close()
    for c in [admin, *clients]:
        await c.aclose()


@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(
    data=st.data(),
    managers=st.integers(1, 5),
    max_bytes=st.integers(1, 64),
    p_fail=st.sampled_from([0.0, 0.2, 0.5]),
)
def test_each_dead_letter_has_exactly_one_record(
    data: st.DataObject, managers: int, max_bytes: int, p_fail: float
) -> None:
    asyncio.run(_scenario(data, managers, max_bytes, p_fail))
