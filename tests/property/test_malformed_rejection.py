# Feature: distributed-agent-system, Property 29: Malformed messages are rejected without side effects
"""Property 29: Malformed messages are rejected without side effects.

*For any* byte payload that fails decoding or lacks a Job_ID or instruction, the
receiving agent acknowledges with ``REJECT`` (no requeue), leaves State_Store
contents unchanged, and processes the next valid Message normally.

Two receivers are exercised, each over MemoryTransport and a fresh in-process
fakeredis server per example:

- Worker dispatch handler (``jobs/<capability>``): a malformed or non-dispatch
  delivery is dead-lettered, never reaches the executor or the LLM, produces no
  Job_Result, and the valid dispatch queued behind it is still executed.
- Manager ``ResultIngest`` (``results``): an unparseable or non-result delivery is
  dead-lettered and changes no State_Store key; the valid Job_Result behind it is
  appended to its Request's inbox and nothing else changes.

The malformed-input strategies are copied from ``test_codec_malformed.py``
(Property 5), plus dispatches whose ``job_id`` / ``instruction`` is removed, empty,
or null, and valid Messages of the wrong type for the receiver.

**Validates: Requirements 5.9, 9.5**
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, note, settings
from hypothesis import strategies as st

from das.codec.envelope import ENVELOPE_FIELDS, MESSAGE_CLASSES, PAYLOAD_MODELS, new_message
from das.codec.message_codec import MessageCodec, ParseError
from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.manager.ingest import ResultIngest
from das.models.results import JobDispatchPayload, JobResultPayload
from das.registry import AgentRegistry
from das.state import INBOX_JOB_RESULT, StateStore
from das.state import keys as state_keys
from das.worker.executor import JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport
from tests.strategies import job_dispatch_payloads, job_result_payloads, messages

CAPABILITY = "search"
AGENT_ID = "worker-1"
PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)
_CODEC = MessageCodec()


# --------------------------------------------------------------------------- malformed bytes
# Copied from tests/property/test_codec_malformed.py (Property 5).


def _to_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


@st.composite
def _valid_objects(draw: st.DrawFn, types: tuple[str, ...] | None = None) -> dict[str, Any]:
    return json.loads(_CODEC.encode(draw(messages(types))))


@st.composite
def _truncated(draw: st.DrawFn) -> bytes:
    data = _CODEC.encode(draw(messages()))
    return data[: draw(st.integers(min_value=0, max_value=len(data) - 1))]


@st.composite
def _missing_envelope_field(draw: st.DrawFn) -> bytes:
    obj = draw(_valid_objects())
    del obj[draw(st.sampled_from(sorted(ENVELOPE_FIELDS)))]
    return _to_bytes(obj)


@st.composite
def _missing_payload_field(draw: st.DrawFn) -> bytes:
    obj = draw(_valid_objects())
    model = PAYLOAD_MODELS[obj["type"]]
    required = sorted(n for n, f in model.model_fields.items() if f.is_required())
    del obj["payload"][draw(st.sampled_from(required))]
    return _to_bytes(obj)


@st.composite
def _unknown_type(draw: st.DrawFn) -> bytes:
    obj = draw(_valid_objects())
    obj["type"] = draw(
        st.text(max_size=30).filter(lambda t: t not in MESSAGE_CLASSES)
        | st.integers()
        | st.none()
        | st.booleans()
    )
    return _to_bytes(obj)


@st.composite
def _unsupported_version(draw: st.DrawFn) -> bytes:
    obj = draw(_valid_objects())
    obj["schema_version"] = draw(
        st.integers().filter(lambda v: v not in MessageCodec.SUPPORTED_VERSIONS)
        | st.text(max_size=5)
        | st.booleans()
        | st.floats(allow_nan=False, allow_infinity=False)
    )
    return _to_bytes(obj)


_WRONG_BY_TYPE: dict[type, st.SearchStrategy[Any]] = {
    str: st.integers() | st.lists(st.integers(), max_size=3) | st.just({"x": 1}),
    int: st.text(max_size=10) | st.lists(st.integers(), max_size=3) | st.just({"x": 1}),
    list: st.text(max_size=10) | st.integers() | st.just({"x": 1}),
    dict: st.text(max_size=10) | st.integers() | st.lists(st.integers(), max_size=3),
    type(None): st.integers() | st.lists(st.integers(), max_size=3) | st.just({"x": 1}),
}


@st.composite
def _wrong_payload_type(draw: st.DrawFn) -> bytes:
    obj = draw(_valid_objects())
    payload = obj["payload"]
    name = draw(st.sampled_from(sorted(payload)))
    payload[name] = draw(_WRONG_BY_TYPE[type(payload[name])])
    return _to_bytes(obj)


@st.composite
def _dispatch_without_id_or_instruction(draw: st.DrawFn) -> bytes:
    """A job_dispatch whose job_id or instruction is removed, empty, or null (Req 5.9)."""
    obj = draw(_valid_objects(("job_dispatch",)))
    name = draw(st.sampled_from(["job_id", "instruction"]))
    how = draw(st.sampled_from(["delete", "empty", "null"]))
    if how == "delete":
        del obj["payload"][name]
    else:
        obj["payload"][name] = "" if how == "empty" else None
    return _to_bytes(obj)


undecodable_bytes = st.one_of(
    st.binary(max_size=512),
    _truncated(),
    _missing_envelope_field(),
    _missing_payload_field(),
    _unknown_type(),
    _unsupported_version(),
    _wrong_payload_type(),
    _dispatch_without_id_or_instruction(),
)


def _encoded(types: tuple[str, ...]) -> st.SearchStrategy[bytes]:
    return messages(types).map(_CODEC.encode)


#: Malformed for a Worker: undecodable, or a valid Message that is not a dispatch.
worker_malformed = undecodable_bytes | _encoded(("job_result", "heartbeat", "control"))
#: Malformed for ResultIngest: undecodable, or a valid Message that is not a result.
ingest_malformed = undecodable_bytes | _encoded(("job_dispatch", "heartbeat", "control"))


def _decodes_as(data: bytes, msg_type: str) -> bool:
    try:
        return _CODEC.decode(data).type == msg_type
    except ParseError:
        return False


# --------------------------------------------------------------------------- Worker


class _SpyExecutor:
    """Records every Job handed to the real executor."""

    def __init__(self, inner: JobExecutor) -> None:
        self._inner = inner
        self.jobs: list[JobDispatchPayload] = []

    async def execute(self, job: JobDispatchPayload, cancel: asyncio.Event) -> JobResultPayload:
        self.jobs.append(job)
        return await self._inner.execute(job, cancel)


async def _check_worker(bad: bytes, valid: JobDispatchPayload) -> None:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    broker = MemoryBroker()
    queue = MemoryTransport(broker, agent_id=AGENT_ID)
    control = MemoryTransport(MemoryBroker(), agent_id=f"{AGENT_ID}-control")
    adapter = FakeAdapter(default="done")
    provider = LLMProvider(adapter, PARAMS, retry_count=0, timeout=10)
    executor = _SpyExecutor(JobExecutor(provider, AGENT_ID, job_timeout=30))
    worker = Worker(
        WorkerSettings(
            agent_id=AGENT_ID, host="host-a", capabilities=frozenset({CAPABILITY}),
            concurrency_limit=2,
        ),
        queue=queue,
        control=control,
        registry=AgentRegistry(client),
        executor=executor,
    )
    try:
        for t in (queue, control):
            await t.start()
        await worker.start()

        destination = f"jobs/{CAPABILITY}"
        broker.publish_raw(destination, bad)
        valid_body = _CODEC.encode(new_message(valid, correlation_id=valid.request_id))
        broker.publish_raw(destination, valid_body)
        await broker.settle()

        # Malformed: REJECT -> dead letter, not requeued, nothing left unacked.
        assert broker.pending("dlq") == [bad]
        assert broker.pending(destination) == []
        assert broker.unacked_count(destination) == 0
        assert queue.handler_errors == []
        # Only the valid Job reached the executor and the LLM.
        assert [j.job_id for j in executor.jobs] == [valid.job_id]
        assert len(adapter.calls) == 1
        # Exactly one Job_Result, for the valid Job; consumption continued.
        [result] = broker.pending_messages("results")
        assert result.type == "job_result"
        assert result.payload.job_id == valid.job_id
        assert result.payload.request_id == valid.request_id
        assert result.payload.status == "Succeeded"
    finally:
        await worker.stop()
        await client.aclose()


@settings(deadline=None)
@given(bad=worker_malformed, valid=job_dispatch_payloads)
def test_worker_rejects_malformed_dispatch_without_side_effects(
    bad: bytes, valid: JobDispatchPayload
) -> None:
    assert not _decodes_as(bad, "job_dispatch")
    note(f"bad={bad!r}")
    valid = valid.model_copy(update={"required_capabilities": [CAPABILITY]})
    asyncio.run(_check_worker(bad, valid))


# --------------------------------------------------------------------------- ResultIngest


async def _keyspace(client: FakeRedis) -> dict[str, Any]:
    """Every key with its type and full contents."""
    snapshot: dict[str, Any] = {}
    for key in sorted(await client.keys("*")):
        kind = await client.type(key)
        if kind == "string":
            value: Any = await client.get(key)
        elif kind == "hash":
            value = await client.hgetall(key)
        elif kind == "list":
            value = await client.lrange(key, 0, -1)
        elif kind == "set":
            value = sorted(await client.smembers(key))
        elif kind == "zset":
            value = await client.zrange(key, 0, -1, withscores=True)
        elif kind == "stream":
            value = await client.xrange(key)
        else:
            raise AssertionError(f"unexpected key type {kind!r} for {key!r}")
        snapshot[key] = (kind, value)
    return snapshot


async def _check_ingest(bad: bytes, valid: JobResultPayload) -> None:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    store = StateStore(client)
    broker = MemoryBroker()
    manager = MemoryTransport(broker, agent_id="manager-1")
    try:
        await manager.start()
        rid = await store.create_request("prompt")
        valid = valid.model_copy(update={"request_id": rid})
        woken: list[str] = []
        await ResultIngest(manager, store, on_ingested=woken.append).start()
        before = await _keyspace(client)

        broker.publish_raw("results", bad)
        broker.publish_raw("results", _CODEC.encode(new_message(valid, correlation_id=rid)))
        await broker.settle()

        # Malformed: REJECT -> dead letter, not requeued, nothing left unacked.
        assert broker.pending("dlq") == [bad]
        assert broker.pending("results") == []
        assert broker.unacked_count("results") == 0
        assert manager.handler_errors == []
        # The valid result was ingested; it is the only inbox entry.
        [entry] = await store.read_inbox(rid, after="0-0")
        assert entry.kind == INBOX_JOB_RESULT
        assert entry.job_result() == valid
        assert woken == [rid]
        # Aside from the valid Request's inbox, the State_Store is unchanged.
        inbox_key = state_keys.inbox(rid)
        after = await _keyspace(client)
        after.pop(inbox_key)
        before.pop(inbox_key, None)
        assert after == before
    finally:
        await manager.close()
        await client.aclose()


@settings(deadline=None)
@given(bad=ingest_malformed, valid=job_result_payloads)
def test_result_ingest_rejects_malformed_without_state_change(
    bad: bytes, valid: JobResultPayload
) -> None:
    assert not _decodes_as(bad, "job_result")
    note(f"bad={bad!r}")
    asyncio.run(_check_ingest(bad, valid))
