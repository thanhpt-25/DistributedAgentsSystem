# Feature: distributed-agent-system, Property 28: Worker execution contract
"""Property 28: Worker execution contract.

For any valid Job dispatch and any LLM outcome (success, error, or timeout):

- The Worker makes exactly one completion call per delivery whose messages
  contain the instruction and every dependency result content (labelled by
  its Job_ID).
- It publishes a Job_Result containing Job_ID, attempt, Worker_ID, status,
  content, token usage, and duration.
- It acknowledges the dispatch only after the Job_Result publish is
  confirmed, and doesn't acknowledge it when that publish fails.

Runs the real ``Worker`` + ``JobExecutor`` over ``MemoryTransport`` with a
fresh fakeredis registry per example.

**Validates: Requirements 5.4, 5.5**
"""

from __future__ import annotations

import asyncio

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.codec import new_message
from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload
from das.registry import AgentRegistry
from das.transport.base import Outcome
from das.worker.executor import JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import HANG, FakeAdapter, completion
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)
CAPABILITY = "research"
WORKER_ID = "worker-1"
JOB_TIMEOUT = 0.05  # only the "timeout" outcome ever waits on it


class RecordingTransport(MemoryTransport):
    """MemoryTransport that logs publishes and settlements in order."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.log: list[tuple[str, object]] = []

    async def publish(self, destination, message, headers=None):
        try:
            await super().publish(destination, message, headers)
        except Exception:
            self.log.append(("publish_failed", destination))
            raise
        self.log.append(("published", destination))

    async def acknowledge(self, delivery, outcome=Outcome.ACK):
        # Snapshot what is on the wire for "results" at the moment of settlement.
        on_wire = len(self.broker.pending("results"))
        self.log.append(("settle", (outcome, on_wire)))
        await super().acknowledge(delivery, outcome)


class Harness:
    """Local mirror of tests/unit/test_worker.py's Harness."""

    def __init__(self, fake_redis, adapter: FakeAdapter) -> None:
        self.broker = MemoryBroker()
        self.queue = RecordingTransport(self.broker, agent_id=WORKER_ID)
        self.control = MemoryTransport(MemoryBroker(), agent_id=f"{WORKER_ID}-control")
        self.manager = MemoryTransport(self.broker, agent_id="manager-1")
        self.registry = AgentRegistry(fake_redis)
        self.adapter = adapter
        provider = LLMProvider(adapter, PARAMS, retry_count=0, timeout=10)
        self.worker = Worker(
            WorkerSettings(
                agent_id=WORKER_ID,
                host="host-a",
                capabilities=frozenset({CAPABILITY}),
                concurrency_limit=1,
                shutdown_timeout=1,
            ),
            queue=self.queue,
            control=self.control,
            registry=self.registry,
            executor=JobExecutor(provider, WORKER_ID, job_timeout=JOB_TIMEOUT),
        )

    async def connect(self) -> None:
        for t in (self.queue, self.control, self.manager):
            await t.start()

    def results(self):
        return self.broker.pending_messages("results")


async def _until(predicate, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


# ------------------------------------------------------------------ strategies

_ident = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=12)
_text = st.text(min_size=1, max_size=40)

dispatches = st.builds(
    lambda rid, jid, attempt, instr, deps: JobDispatchPayload(
        request_id="req-" + rid,
        job_id="job-" + jid,
        attempt=attempt,
        instruction="instr:" + instr,
        required_capabilities=[CAPABILITY],
        dependency_results={f"dep-{k}": f"content:{v}" for k, v in deps.items()},
    ),
    _ident,
    _ident,
    st.integers(min_value=1, max_value=10),
    _text,
    st.dictionaries(_ident, _text, max_size=5),
)

outcomes = st.one_of(
    st.tuples(
        st.just("ok"),
        st.tuples(st.text(max_size=40), st.integers(0, 1000), st.integers(0, 1000)),
    ),
    st.tuples(st.just("permanent"), st.text(alphabet="abcdefgh ", min_size=1, max_size=20)),
    st.tuples(st.just("timeout"), st.none()),
)


def _materialize(kind: str, value):
    if kind == "ok":
        content, i, o = value
        return completion(content, input_tokens=i, output_tokens=o)
    if kind == "permanent":
        return PermanentLLMError("perm-" + value)
    return HANG


EXPECTED_STATUS = {"ok": "Succeeded", "permanent": "Failed", "timeout": "TimedOut"}


# ------------------------------------------------------------------ property


@settings(max_examples=100, deadline=None)
@given(job=dispatches, outcome=outcomes, fail_publish=st.booleans())
def test_worker_execution_contract(job: JobDispatchPayload, outcome, fail_publish: bool) -> None:
    kind, value = outcome
    asyncio.run(_scenario(job, kind, value, fail_publish))


async def _scenario(job: JobDispatchPayload, kind: str, value, fail_publish: bool) -> None:
    fake_redis = FakeRedis(server=FakeServer(), decode_responses=True)
    # Same outcome on every call, so a redelivery after a failed publish behaves alike.
    h = Harness(fake_redis, FakeAdapter(default=_materialize(kind, value)))
    try:
        await h.connect()
        if fail_publish:
            h.queue.fail_next_publishes(1)
        await h.worker.start()
        try:
            await h.manager.publish(
                f"jobs/{CAPABILITY}", new_message(job, correlation_id=job.request_id)
            )
            await _until(lambda: h.results())
            await h.broker.settle()
            _check(h, job, kind, value, fail_publish)
        finally:
            await h.worker.stop()
    finally:
        await fake_redis.aclose()


def _check(h: Harness, job: JobDispatchPayload, kind: str, value, fail_publish: bool) -> None:
    deliveries = 2 if fail_publish else 1

    # Exactly one completion call per delivery, containing instruction + every dependency.
    assert len(h.adapter.calls) == deliveries
    for call in h.adapter.calls:
        text = "\n".join(m.content for m in call.messages)
        assert job.instruction in text
        for dep_id, dep_content in job.dependency_results.items():
            assert dep_id in text
            assert dep_content in text
            assert f"{dep_id}:\n{dep_content}" in text  # labelled by its Job_ID

    # One Job_Result on the wire with the required fields.
    [result] = h.results()
    assert result.correlation_id == job.request_id
    p = result.payload
    assert (p.request_id, p.job_id, p.attempt, p.worker_id) == (
        job.request_id, job.job_id, job.attempt, WORKER_ID,
    )
    assert p.status == EXPECTED_STATUS[kind]
    assert p.duration_ms >= 0
    if kind == "ok":
        content, i, o = value
        assert p.content == content
        assert (p.usage.input_tokens, p.usage.output_tokens, p.usage.total_tokens) == (i, o, i + o)
    else:
        assert p.content == ""
        assert p.error
        assert p.usage.total_tokens == 0

    # ACK only after the result is confirmed on the wire; a failed publish is never ACKed.
    log = h.queue.log
    settles = [(i, entry[1]) for i, entry in enumerate(log) if entry[0] == "settle"]
    assert len(settles) == deliveries
    for idx, (outcome, on_wire) in settles:
        prev = log[idx - 1]
        if outcome is Outcome.ACK:
            assert prev == ("published", "results")
            assert on_wire == 1
        else:
            assert outcome is Outcome.REQUEUE
            assert prev == ("publish_failed", "results")
            assert on_wire == 0
    assert [s[1][0] for s in settles] == (
        [Outcome.REQUEUE, Outcome.ACK] if fail_publish else [Outcome.ACK]
    )

    # The dispatch left the queue only once the result was published.
    assert h.broker.pending(f"jobs/{CAPABILITY}") == []
    assert h.broker.unacked_count(f"jobs/{CAPABILITY}") == 0
