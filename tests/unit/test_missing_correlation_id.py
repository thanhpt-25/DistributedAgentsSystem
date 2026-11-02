"""Missing correlation IDs on request-scoped Messages (task 17.5).

A Job dispatch or Job_Result Message that arrives with an empty correlation ID
is logged at warning level with its message type and sending agent ID, and is
still processed: the Worker executes the Job and publishes a result carrying the
payload's Request_ID; the Manager's ResultIngest still appends it to the inbox.
Runs over MemoryTransport + FakeAdapter + fakeredis, reading the structured
JSON log lines produced by ``configure_logging``.

Requirements: 13.6.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
from collections.abc import Iterator

import pytest
import structlog

from das.codec import new_message
from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.manager.ingest import SENDER_HEADER, ResultIngest
from das.models import JobDispatchPayload, JobResultPayload, TokenUsage
from das.observability import configure_logging
from das.registry import AgentRegistry
from das.state import INBOX_JOB_RESULT, StateStore
from das.worker.executor import JobExecutor
from das.worker.worker import SENDER_HEADER as WORKER_SENDER_HEADER
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)


@pytest.fixture
def log_stream() -> Iterator[io.StringIO]:
    """Route the real structured-logging pipeline into a buffer, then restore it."""
    stream = io.StringIO()
    root = logging.getLogger()
    old_level = root.level
    configure_logging(agent_id="test-agent", level="DEBUG", stream=stream)
    try:
        yield stream
    finally:
        for handler in list(root.handlers):
            if getattr(handler, "_das_handler", False):
                root.removeHandler(handler)
        root.setLevel(old_level)
        structlog.reset_defaults()


def _entries(stream: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


async def _until(predicate, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def test_worker_and_manager_agree_on_sender_header() -> None:
    assert WORKER_SENDER_HEADER == SENDER_HEADER


# --------------------------------------------------------------------- Worker


async def _worker(fake_redis, broker: MemoryBroker) -> Worker:
    queue = MemoryTransport(broker, agent_id="worker-1")
    control = MemoryTransport(MemoryBroker(), agent_id="worker-1-control")
    await queue.start()
    await control.start()
    provider = LLMProvider(FakeAdapter(default="done"), PARAMS, retry_count=0, timeout=10)
    worker = Worker(
        WorkerSettings(
            agent_id="worker-1", host="host-a", capabilities=frozenset({"research"}),
            concurrency_limit=1,
        ),
        queue=queue,
        control=control,
        registry=AgentRegistry(fake_redis),
        executor=JobExecutor(provider, "worker-1", job_timeout=10),
    )
    await worker.start()
    return worker


@pytest.mark.parametrize(
    ("headers", "expected_sender"),
    [({SENDER_HEADER: "manager-1"}, "manager-1"), (None, "unknown")],
)
async def test_worker_dispatch_without_correlation_id_warns_and_executes(
    fake_redis, log_stream: io.StringIO, headers, expected_sender
):
    broker = MemoryBroker()
    manager = MemoryTransport(broker, agent_id="manager-1")
    await manager.start()
    worker = await _worker(fake_redis, broker)
    try:
        payload = JobDispatchPayload(
            request_id="req-7", job_id="job-1", attempt=1, instruction="do it",
            required_capabilities=["research"],
        )
        await manager.publish(
            "jobs/research", new_message(payload, correlation_id=""), headers
        )
        await _until(lambda: broker.pending("results"))
        await broker.settle()
    finally:
        await worker.stop()

    warnings = [
        e for e in _entries(log_stream)
        if e["level"] == "warning" and e.get("message_type") == "job_dispatch"
    ]
    assert len(warnings) == 1
    assert warnings[0]["sender"] == expected_sender
    assert "correlation" in warnings[0]["message"]

    # Still processed: executed, result published with the payload's Request_ID, ACKed.
    [result] = broker.pending_messages("results")
    assert result.payload.status == "Succeeded" and result.payload.job_id == "job-1"
    assert result.correlation_id == "req-7"
    assert broker.pending("jobs/research") == [] and broker.unacked_count("jobs/research") == 0


# --------------------------------------------------------------------- Manager


def _result(rid: str) -> JobResultPayload:
    return JobResultPayload(
        request_id=rid, job_id="a", attempt=1, worker_id="worker-9", status="Succeeded",
        content="x", usage=TokenUsage(), duration_ms=1,
    )


async def test_result_ingest_without_correlation_id_warns_and_ingests(
    state_store: StateStore, log_stream: io.StringIO
):
    rid = await state_store.create_request("p")
    broker = MemoryBroker()
    manager = MemoryTransport(broker, agent_id="manager-1")
    worker = MemoryTransport(broker, agent_id="worker-9")
    await manager.start()
    await worker.start()
    woken: list[str] = []
    await ResultIngest(manager, state_store, on_ingested=woken.append).start()

    await worker.publish("results", new_message(_result(rid), correlation_id=""))
    await broker.settle()

    warnings = [
        e for e in _entries(log_stream)
        if e["level"] == "warning" and e["message"] == "message_missing_correlation_id"
    ]
    assert len(warnings) == 1
    assert warnings[0]["message_type"] == "job_result"
    assert warnings[0]["sender"] == "worker-9"

    # Still processed: appended to the Request's inbox, ACKed, owning driver woken.
    [entry] = await state_store.read_inbox(rid, after="0-0")
    assert entry.kind == INBOX_JOB_RESULT and entry.job_result() == _result(rid)
    assert broker.pending("results") == [] and broker.unacked_count("results") == 0
    assert woken == [rid]
