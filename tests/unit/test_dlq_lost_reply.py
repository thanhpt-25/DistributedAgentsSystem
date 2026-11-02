"""Dead_Letter_Record created but the State_Store reply lost (das-hardening-ci Req 3).

``ReplyDroppingRedis`` injects the fault at the Redis client (design H6): the
``EVALSHA`` of ``create_dead_letter.lua`` really runs on the shared ``FakeServer``,
then the client raises ``redis.exceptions.TimeoutError`` as if the reply never came.
redis-py's ``AsyncScript.__call__`` (used by ``StateStore._run``) calls
``client.evalsha``, so overriding ``evalsha`` covers the whole StateStore path.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

import pytest
import redis.exceptions
import structlog
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from prometheus_client.parser import text_string_to_metric_families
from structlog.testing import capture_logs

from das.clock import FakeClock
from das.codec import MessageCodec, new_message
from das.manager.ingest import DEAD_LETTER_REASON_HEADER, DeadLetterRecorder
from das.models import JobDispatchPayload
from das.models.dlq import DeadLetterFields
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.ops.dlq import dead_letter_id, dead_letter_identity
from das.state import StateStore
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

CODEC = MessageCodec()
#: Per-scenario limit (Req 3.4: the three scenarios together stay under 10 s).
SCENARIO_TIMEOUT = 3.0


@pytest.fixture(autouse=True)
def _default_structlog_config() -> Iterator[None]:
    """Isolate from global structlog state left by other tests.

    ``configure_logging(level=INFO)`` (e.g. in test_cli.py) installs an INFO
    filtering ``wrapper_class``; ``capture_logs`` only swaps processors, so the
    DEBUG event ``dead_letter_record_exists`` would be dropped. Reset to the
    structlog defaults (no level filtering) and restore the previous config.
    """
    saved = structlog.get_config()
    structlog.reset_defaults()
    try:
        yield
    finally:
        structlog.configure(**saved)


class ReplyDroppingRedis(FakeRedis):
    """FakeRedis that runs ``EVALSHA`` of ``target_sha`` for real, then raises
    ``TimeoutError`` on the first such call (the reply is lost after execution)."""

    def __init__(
        self,
        *,
        server: FakeServer,
        target_sha: str,
        on_drop: Callable[[], Awaitable[None]] | None = None,
        **kw: Any,
    ) -> None:
        super().__init__(server=server, **kw)
        self.target_sha = target_sha
        self.on_drop = on_drop
        self.armed = True  # True until the first reply is dropped
        self.executed = 0  # EVALSHA of target_sha that ran on the server
        self.dropped = 0  # replies dropped

    async def evalsha(self, sha: str, numkeys: int, *keys_and_args: Any) -> Any:
        # NoScriptError propagates before counting: the script did not run.
        result = await super().evalsha(sha, numkeys, *keys_and_args)
        if sha == self.target_sha:
            self.executed += 1
            if self.armed:
                self.armed = False
                self.dropped += 1
                if self.on_drop is not None:
                    await self.on_drop()
                raise redis.exceptions.TimeoutError("injected: reply lost after execution")
        return result


def create_dead_letter_sha() -> str:
    """SHA of ``create_dead_letter.lua`` as registered by ``StateStore``."""
    probe = FakeRedis(server=FakeServer(), decode_responses=True)
    return StateStore(probe)._scripts["create_dead_letter"].sha


def dl_fields(dlid: str = "d" * 32) -> DeadLetterFields:
    return DeadLetterFields(
        dead_letter_id=dlid,
        reason="delivery_limit",
        request_id=None,
        job_id="a",
        attempt=1,
        capabilities=["research"],
        last_error="delivery limit reached",
        message_id="m-1",
    )


async def test_reply_dropping_redis_runs_script_then_drops_first_reply() -> None:
    server = FakeServer()
    drops: list[int] = []

    async def on_drop() -> None:
        drops.append(1)

    client = ReplyDroppingRedis(
        server=server, target_sha=create_dead_letter_sha(), on_drop=on_drop, decode_responses=True
    )
    store = StateStore(client)
    fields = dl_fields()

    with pytest.raises(redis.exceptions.TimeoutError):
        await store.create_dead_letter(fields)

    assert client.executed == 1 and client.dropped == 1, "fault injection had no effect"
    assert drops == [1]
    # The script really ran: the record is visible through another client on the same server.
    other = StateStore(FakeRedis(server=server, decode_responses=True))
    assert await other.get_dead_letter(fields.dead_letter_id) is not None

    # The second call goes through: the record already exists.
    second = await store.create_dead_letter(fields)
    assert second.created is False
    assert client.executed == 2 and client.dropped == 1
    assert drops == [1]


# --------------------------------------------------------------------------- scenarios


class CountingRedis(FakeRedis):
    """Plain FakeRedis that counts ``EVALSHA`` calls of ``target_sha`` (no fault)."""

    def __init__(self, *, server: FakeServer, target_sha: str, **kw: Any) -> None:
        super().__init__(server=server, **kw)
        self.target_sha = target_sha
        self.executed = 0

    async def evalsha(self, sha: str, numkeys: int, *keys_and_args: Any) -> Any:
        result = await super().evalsha(sha, numkeys, *keys_and_args)
        if sha == self.target_sha:
            self.executed += 1
        return result


def _dispatch_body() -> bytes:
    payload = JobDispatchPayload(
        request_id="req-1", job_id="a", attempt=1, instruction="i", required_capabilities=["research"]
    )
    return CODEC.encode(new_message(payload, correlation_id="req-1"))


def _created_total(recorder: MetricsRecorder) -> float:
    """Sum of ``das_dlq_records_created_total`` over every label set of one recorder."""
    total = 0.0
    for family in text_string_to_metric_families(recorder.render()[0].decode()):
        for sample in family.samples:
            if sample.name == "das_dlq_records_created_total":
                total += sample.value
    return total


async def _recorder(
    broker: MemoryBroker, client: FakeRedis, agent_id: str
) -> tuple[MemoryTransport, DeadLetterRecorder, MetricsRecorder]:
    transport = MemoryTransport(broker, agent_id=agent_id)
    await transport.start()
    metrics = MetricsRecorder(clock=FakeClock())
    recorder = DeadLetterRecorder(transport, StateStore(client), ManagerMetrics(metrics))
    return transport, recorder, metrics


async def _lost_reply_scenario(case: str) -> None:
    """``case``: ``"same"`` (a), ``"other"`` (b) or ``"control"`` (no fault)."""
    server = FakeServer()
    sha = create_dead_letter_sha()
    reader_client = FakeRedis(server=server, decode_responses=True)
    reader = StateStore(reader_client)
    broker = MemoryBroker()
    body = _dispatch_body()
    dlid = dead_letter_id(dead_letter_identity(CODEC.decode(body), body))
    headers = {DEAD_LETTER_REASON_HEADER: "delivery_limit"}

    seen_after_drop: list[bool] = []
    recorder_a: DeadLetterRecorder | None = None

    async def on_drop() -> None:
        # The script ran and the reply is about to be lost: the record must exist now.
        seen_after_drop.append(await reader.get_dead_letter(dlid) is not None)
        if case == "other":
            assert recorder_a is not None
            await recorder_a.stop()  # stop consuming before the REQUEUE

    client_a: FakeRedis
    if case == "control":
        client_a = CountingRedis(server=server, target_sha=sha, decode_responses=True)
    else:
        client_a = ReplyDroppingRedis(server=server, target_sha=sha, on_drop=on_drop, decode_responses=True)
    transport_a, recorder_a, metrics_a = await _recorder(broker, client_a, "m-a")
    transports = [transport_a]
    clients: list[FakeRedis] = [reader_client, client_a]
    all_metrics = [metrics_a]
    client_b: CountingRedis | None = None

    with capture_logs() as logs:
        await recorder_a.start()
        broker.publish_raw("dlq", body, headers)
        await broker.settle()

        if case == "other":
            # A no longer consumes: the requeued Message waits for another Manager.
            assert broker.pending("dlq") == [body]
            client_b = CountingRedis(server=server, target_sha=sha, decode_responses=True)
            transport_b, recorder_b, metrics_b = await _recorder(broker, client_b, "m-b")
            transports.append(transport_b)
            clients.append(client_b)
            all_metrics.append(metrics_b)
            await recorder_b.start()
            await broker.settle()

    failed = [e for e in logs if e["event"] == "dead_letter_record_failed"]
    if case == "control":
        assert isinstance(client_a, CountingRedis)
        assert client_a.executed == 1 and failed == []
    else:
        # Fault took effect (Req 3.1), checked before anything else.
        assert isinstance(client_a, ReplyDroppingRedis)
        if client_a.executed < 1 or client_a.dropped != 1:
            pytest.fail(
                f"fault injection had no effect: executed={client_a.executed} dropped={client_a.dropped}"
            )
        assert seen_after_drop == [True], "fault injection had no effect: no record after the dropped reply"
        assert [e.get("error") for e in failed] == ["TimeoutError"], (
            f"fault injection had no effect: dead_letter_record_failed logs={failed}"
        )
        if case == "same":
            assert client_a.executed == 2  # the redelivery went back to A
        else:
            assert client_a.executed == 1 and client_b is not None
            assert client_b.executed == 1  # the redelivery went to B
            assert any(e["event"] == "dead_letter_record_exists" for e in logs)

    # The redelivery has been handled: nothing pending or unacked.
    assert broker.pending("dlq") == [] and broker.unacked_count("dlq") == 0

    # Req 3.2 / 3.6: exactly one record for this Message.
    page = await reader.list_dead_letters(limit=100)
    assert [r.dead_letter_id for r in page.records] == [dlid]

    # Req 3.3 / 3.6: counter summed over every participating Manager, read after settle.
    total = sum(_created_total(m) for m in all_metrics)
    if case == "control":
        assert total == 1
    else:
        assert total <= 1

    for t in transports:
        await t.close()
    for c in clients:
        await c.aclose()


async def test_lost_reply_redelivered_to_same_manager() -> None:
    """(a) The redelivery goes back to the Manager that got the timeout."""
    async with asyncio.timeout(SCENARIO_TIMEOUT):
        await _lost_reply_scenario("same")


async def test_lost_reply_redelivered_to_other_manager() -> None:
    """(b) A stops consuming before the REQUEUE; Manager B on the same State_Store gets it."""
    async with asyncio.timeout(SCENARIO_TIMEOUT):
        await _lost_reply_scenario("other")


async def test_reply_delivered_counts_exactly_once() -> None:
    """Control: no fault, so one record and the counter equals 1 (v2 Req 3.8 equality)."""
    async with asyncio.timeout(SCENARIO_TIMEOUT):
        await _lost_reply_scenario("control")
