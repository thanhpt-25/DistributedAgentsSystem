# Feature: distributed-agent-system, Property 40: Correlation ID propagation
"""Property 40: Correlation ID propagation.

*For any* received Message with correlation ID ``c``, every outbound Message
produced while handling it has ``correlation_id == c``, and every log entry
emitted during handling has ``request_id == c``. *For any* Request, every Job
dispatch, Job_Result, and control Message produced for it carries that
Request_ID as correlation ID.

Two scenarios, both over ``MemoryTransport`` with fakeredis (Lua) and the
structured logging pipeline writing to an in-memory stream:

- ``test_request_messages_and_logs_carry_request_id``: several concurrent
  Requests driven end to end by the real ``RequestDrivers`` (Planner,
  Summarizer, ResultIngest, DeadLetterIngest, ControlCommandHandler) and the
  real ``Worker`` + ``JobExecutor``. Plans, Job outcomes (success, failure up
  to dead-lettering) and cancellation are generated. Every request-scoped
  Message on either broker and every log entry is checked.
- ``test_worker_handlers_use_inbound_correlation_id``: the Worker's dispatch
  and ``cancel_job`` handlers given arbitrary correlation IDs (which may differ
  from the payload's ``request_id``).

**Validates: Requirements 13.4, 13.5**
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import structlog
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.codec import ControlMessage, JobDispatchMessage, JobResultMessage, new_message
from das.codec.message_codec import MessageCodec
from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.manager.driver import DriverDeps, RequestDrivers
from das.manager.ingest import ControlCommandHandler, DeadLetterIngest, ResultIngest
from das.manager.machine import MachineConfig
from das.manager.planner import Planner, PlannerConfig
from das.manager.summarizer import Summarizer
from das.models import ControlPayload
from das.models.results import JobDispatchPayload
from das.observability.logging import configure_logging
from das.registry import AgentRegistry
from das.state import StateStore
from das.worker.executor import JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import FakeAdapter, completion
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)
CAPS = ("search", "write")
WORKER_ID = "w1"
MANAGER_ID = "m1"
TIMEOUT = 15.0
REQUEST_SCOPED = (JobDispatchMessage, JobResultMessage, ControlMessage)

_TAG_RE = re.compile(r"TAG-r(\d+)-END")
_OUTCOME_RE = re.compile(r"#(ok|fail|hang)#")

# Log entries that are not emitted while handling a Request-scoped Message:
# Worker startup and shutdown. Everything else must carry a Request_ID.
_LIFECYCLE_MESSAGES = frozenset(
    {
        "registered in agent registry",
        "worker started",
        "worker stopped",
    }
)


# --------------------------------------------------------------------------- helpers


@contextmanager
def captured_logs() -> Iterator[io.StringIO]:
    stream = io.StringIO()
    root = logging.getLogger()
    old_level = root.level
    configure_logging(agent_id="agent-property-40", level="DEBUG", stream=stream)
    try:
        yield stream
    finally:
        for handler in list(root.handlers):
            if getattr(handler, "_das_handler", False):
                root.removeHandler(handler)
        root.setLevel(old_level)
        structlog.reset_defaults()


def parse_entries(stream: io.StringIO) -> list[dict]:
    """Entries emitted by DAS code (third-party stdlib loggers such as asyncio excluded)."""
    entries = [json.loads(line) for line in stream.getvalue().splitlines() if line]
    return [e for e in entries if str(e.get("logger_name", "")).startswith("das.")]


def wire_messages(*brokers: MemoryBroker) -> list:
    codec = MessageCodec()
    out = []
    for broker in brokers:
        for record in broker.wire:
            out.append((record, codec.decode(record.body)))
    return out


async def until(predicate, timeout: float = TIMEOUT) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


def _text(messages) -> str:
    return "\n".join(m.content for m in messages)


async def worker_llm(system, messages, params):
    """Outcome is chosen by a ``#ok#`` / ``#fail#`` / ``#hang#`` marker in the instruction."""
    match = _OUTCOME_RE.search(_text(messages))
    outcome = match.group(1) if match else "ok"
    if outcome == "fail":
        raise PermanentLLMError("scripted failure")
    if outcome == "hang":
        await asyncio.Event().wait()
    return completion("out")


def make_worker(fake_redis, broker: MemoryBroker, control_broker: MemoryBroker,
                adapter: FakeAdapter) -> tuple[Worker, MemoryTransport, MemoryTransport]:
    queue = MemoryTransport(broker, agent_id=WORKER_ID)
    control = MemoryTransport(control_broker, agent_id=WORKER_ID)
    provider = LLMProvider(adapter, PARAMS, retry_count=0, timeout=10)
    worker = Worker(
        WorkerSettings(
            agent_id=WORKER_ID,
            host="host-a",
            capabilities=frozenset(CAPS),
            concurrency_limit=16,
            heartbeat_interval=60,
        ),
        queue=queue,
        control=control,
        registry=AgentRegistry(fake_redis),
        executor=JobExecutor(provider, WORKER_ID, job_timeout=30),
    )
    return worker, queue, control


# --------------------------------------------------------------------------- strategies


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    capability: str
    depends_on: tuple[str, ...]
    outcome: str  # ok | fail | hang


@dataclass(frozen=True)
class RequestSpec:
    index: int
    jobs: tuple[JobSpec, ...]
    cancel: bool

    def plan_json(self) -> str:
        return json.dumps(
            {
                "jobs": [
                    {
                        "job_id": j.job_id,
                        "instruction": f"do {j.job_id} #{j.outcome}#",
                        "capabilities": [j.capability],
                        "depends_on": list(j.depends_on),
                    }
                    for j in self.jobs
                ]
            }
        )


@st.composite
def request_specs(draw: st.DrawFn, index: int) -> RequestSpec:
    cancel = draw(st.booleans())
    count = draw(st.integers(1, 3))
    jobs: list[JobSpec] = []
    for k in range(count):
        earlier = [j.job_id for j in jobs]
        deps = draw(st.lists(st.sampled_from(earlier), unique=True)) if earlier else []
        outcome = "hang" if cancel else draw(st.sampled_from(["ok", "ok", "fail"]))
        jobs.append(
            JobSpec(f"r{index}-j{k}", draw(st.sampled_from(CAPS)), tuple(deps), outcome)
        )
    return RequestSpec(index, tuple(jobs), cancel)


@st.composite
def scenarios(draw: st.DrawFn) -> list[RequestSpec]:
    n = draw(st.integers(1, 3))
    return [draw(request_specs(i)) for i in range(n)]


# --------------------------------------------------------------------------- scenario 1


async def _run_requests(specs: list[RequestSpec]) -> dict:
    fake_redis = FakeRedis(server=FakeServer(), decode_responses=True)
    store = StateStore(fake_redis)
    broker = MemoryBroker(("dlq",))
    control_broker = MemoryBroker()

    started_jobs: set[str] = set()

    async def worker_responder(system, messages, params):
        match = re.search(r"do (r\d+-j\d+)", _text(messages))
        if match:
            started_jobs.add(match.group(1))
        return await worker_llm(system, messages, params)

    worker, wq, wc = make_worker(fake_redis, broker, control_broker,
                                 FakeAdapter(default=worker_responder))
    await wq.start()
    await wc.start()
    await worker.start()

    plans = {s.index: s.plan_json() for s in specs}

    def planner_responder(system, messages, params):
        match = _TAG_RE.search(_text(messages))
        assert match, "planning prompt lost the Request tag"
        return completion(plans[int(match.group(1))])

    manager = MemoryTransport(broker, agent_id=MANAGER_ID)
    control = MemoryTransport(control_broker, agent_id=MANAGER_ID)
    gateway = MemoryTransport(control_broker, agent_id="gw")
    for t in (manager, control, gateway):
        await t.start()
    deps = DriverDeps(
        store=store,
        registry=AgentRegistry(fake_redis),
        transport=manager,
        planner=Planner(LLMProvider(FakeAdapter(default=planner_responder), PARAMS, retry_count=0)),
        summarizer=Summarizer(LLMProvider(FakeAdapter(default="final"), PARAMS, retry_count=0)),
        planner_config=PlannerConfig(planning_retries=0),
        machine_config=MachineConfig(max_attempts=2, base_backoff=0.001, max_backoff=0.002),
        control_transport=control,
        tick_period=0.01,
        finalize_retry_delay=0.01,
    )
    drivers = RequestDrivers(deps, 30.0)
    await ResultIngest(manager, store, on_ingested=drivers.notify).start()
    await DeadLetterIngest(manager, store, on_ingested=drivers.notify).start()
    await ControlCommandHandler(control, MANAGER_ID, drivers).start()

    rids: dict[int, str] = {}
    tasks = []
    try:
        for spec in specs:
            rid = await store.create_request(f"question TAG-r{spec.index}-END")
            rids[spec.index] = rid
            acquired = await store.acquire_lease(rid, MANAGER_ID, 30.0)
            assert acquired.ok and acquired.lease is not None
            tasks.append(drivers.on_claimed(acquired.lease))

        for spec in specs:
            if not spec.cancel:
                continue
            roots = {j.job_id for j in spec.jobs if not j.depends_on}
            await until(lambda roots=roots: bool(roots & started_jobs))
            rid = rids[spec.index]
            assert (await store.cancel_request(rid)).ok
            await gateway.publish(
                MANAGER_ID,
                new_message(ControlPayload(command="cancel_request", request_id=rid),
                            correlation_id=rid),
            )

        async with asyncio.timeout(TIMEOUT):
            await asyncio.gather(*tasks)
        # Cancelled Jobs publish their Cancelled results asynchronously.
        await until(lambda: worker.in_flight == 0)
        await broker.settle()
        await control_broker.settle()
    finally:
        await worker.stop()
        for t in (manager, control, gateway):
            await t.close()
        await fake_redis.aclose()

    return {"rids": rids, "brokers": (broker, control_broker)}


@settings(max_examples=100, deadline=None)
@given(specs=scenarios())
def test_request_messages_and_logs_carry_request_id(specs: list[RequestSpec]) -> None:
    """Property 40 for Manager drivers and the Worker over the fake transport."""
    with captured_logs() as stream:
        outcome = asyncio.run(_run_requests(specs))
    rids: dict[int, str] = outcome["rids"]
    owner_of_job = {j.job_id: rids[s.index] for s in specs for j in s.jobs}
    known = set(rids.values())

    # 13.4: every request-scoped Message carries its Request_ID as correlation ID.
    seen: dict[str, set[type]] = {rid: set() for rid in known}
    for record, message in wire_messages(*outcome["brokers"]):
        if not isinstance(message, REQUEST_SCOPED):
            continue
        rid = message.payload.request_id
        assert message.correlation_id == rid, (record.sender, record.destination, message)
        assert rid in known
        job_id = getattr(message.payload, "job_id", None)
        if job_id is not None:
            assert owner_of_job[job_id] == rid
        seen[rid].add(type(message))
    for spec in specs:
        rid = rids[spec.index]
        assert {JobDispatchMessage, JobResultMessage} <= seen[rid]
        if spec.cancel:
            assert ControlMessage in seen[rid]

    # 13.5: every log entry emitted while handling carries the Request_ID.
    entries = parse_entries(stream)
    logged_rids: set[str] = set()
    for entry in entries:
        if "job_id" in entry:
            assert entry.get("request_id") == owner_of_job[entry["job_id"]], entry
        if "request_id" in entry:
            assert entry["request_id"] in known, entry
            logged_rids.add(entry["request_id"])
        else:
            assert entry["message"] in _LIFECYCLE_MESSAGES, entry
    assert logged_rids == known


# --------------------------------------------------------------------------- scenario 2

_ids = st.text(min_size=1, max_size=20)


@dataclass(frozen=True)
class DispatchSpec:
    job_id: str
    correlation_id: str
    request_id: str
    outcome: str  # ok | fail | hang (cancelled)


@st.composite
def dispatch_specs(draw: st.DrawFn) -> list[DispatchSpec]:
    n = draw(st.integers(1, 3))
    out = []
    for k in range(n):
        c = draw(_ids)
        rid = draw(st.one_of(st.just(c), _ids))
        out.append(DispatchSpec(f"job-{k}", c, rid, draw(st.sampled_from(["ok", "fail", "hang"]))))
    return out


async def _run_worker(specs: list[DispatchSpec]) -> tuple[list, MemoryBroker, MemoryBroker]:
    fake_redis = FakeRedis(server=FakeServer(), decode_responses=True)
    broker, control_broker = MemoryBroker(), MemoryBroker()
    adapter = FakeAdapter(default=worker_llm)
    worker, wq, wc = make_worker(fake_redis, broker, control_broker, adapter)
    manager = MemoryTransport(broker, agent_id=MANAGER_ID)
    manager_control = MemoryTransport(control_broker, agent_id=MANAGER_ID)
    for t in (wq, wc, manager, manager_control):
        await t.start()
    await worker.start()
    try:
        for spec in specs:
            payload = JobDispatchPayload(
                request_id=spec.request_id, job_id=spec.job_id, attempt=1,
                instruction=f"do {spec.job_id} #{spec.outcome}#",
                required_capabilities=["search"],
            )
            await manager.publish("jobs/search",
                                  new_message(payload, correlation_id=spec.correlation_id))
        hanging = [s for s in specs if s.outcome == "hang"]
        await until(lambda: worker.in_flight >= len(hanging) and len(adapter.calls) == len(specs))
        for spec in hanging:
            cmd = ControlPayload(command="cancel_job", request_id=spec.request_id,
                                 job_id=spec.job_id)
            await manager_control.publish(WORKER_ID,
                                          new_message(cmd, correlation_id=spec.correlation_id))
        await until(lambda: len(broker.pending("results")) == len(specs))
        await broker.settle()
        await control_broker.settle()
        results = broker.pending_messages("results")
    finally:
        await worker.stop()
        for t in (manager, manager_control):
            await t.close()
        await fake_redis.aclose()
    return results, broker, control_broker


@settings(max_examples=100, deadline=None)
@given(specs=dispatch_specs())
def test_worker_handlers_use_inbound_correlation_id(specs: list[DispatchSpec]) -> None:
    """Property 40 for the Worker's dispatch and cancel_job handlers."""
    with captured_logs() as stream:
        results, _, _ = asyncio.run(_run_worker(specs))
    by_job = {s.job_id: s for s in specs}

    # Outbound Job_Results carry the dispatch's correlation ID.
    assert sorted(r.payload.job_id for r in results) == sorted(by_job)
    expected_status = {"ok": "Succeeded", "fail": "Failed", "hang": "Cancelled"}
    for result in results:
        spec = by_job[result.payload.job_id]
        assert result.correlation_id == spec.correlation_id
        assert result.payload.status == expected_status[spec.outcome]

    # Log entries emitted while handling use the inbound correlation ID.
    known = {s.correlation_id for s in specs}
    for entry in parse_entries(stream):
        if "job_id" in entry:
            assert entry.get("request_id") == by_job[entry["job_id"]].correlation_id, entry
        if "request_id" in entry:
            assert entry["request_id"] in known, entry
        else:
            assert entry["message"] in _LIFECYCLE_MESSAGES, entry
