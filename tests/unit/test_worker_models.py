"""A Job runs on the model of its routing Capability, and its result names the model that answered
(das-smarter-agents task 2.3; Req 2.2, 2.5)."""

from __future__ import annotations

import asyncio

import pytest

from das.clock import FakeClock
from das.codec import new_message
from das.llm.base import ModelParams
from das.llm.errors import AuthLLMError, PermanentLLMError
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload
from das.observability.metrics import MetricsRecorder, WorkerMetrics
from das.registry import AgentRegistry
from das.worker.executor import MODEL_HEADER, PROVIDER_HEADER, JobExecutor
from das.worker.worker import Worker, WorkerSettings
from tests.fakes.llm_adapter import FakeAdapter
from tests.fakes.memory_transport import MemoryBroker, MemoryTransport

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=64)
SMALL = {"summarize": "small-model"}


async def no_sleep(_: float) -> None:
    return None


def job(job_id: str = "a") -> JobDispatchPayload:
    return JobDispatchPayload(
        request_id="req-1", job_id=job_id, attempt=1, instruction="do it", required_capabilities=["summarize"]
    )


def executor(adapter: FakeAdapter, fallback: str | None = None, models: dict[str, str] | None = SMALL) -> JobExecutor:
    provider = LLMProvider(adapter, PARAMS, retry_count=0, timeout=1, sleep=no_sleep, fallback_model=fallback)
    return JobExecutor(provider, "w1", job_timeout=5, capability_models=models)


def used(adapter: FakeAdapter) -> list[str]:
    return [c.params.model for c in adapter.calls]


# --------------------------------------------------------------------------- choosing the model (Req 2.2)


async def test_the_routing_capabilitys_model_is_used_and_named_in_the_headers() -> None:
    adapter = FakeAdapter(["ok"])
    ex = executor(adapter)
    result = await ex.execute(job(), asyncio.Event(), routing_capability="summarize")
    assert used(adapter) == ["small-model"] and result.status == "Succeeded"
    assert ex.headers_for(result) == {PROVIDER_HEADER: "fake", MODEL_HEADER: "small-model"}


@pytest.mark.parametrize("routing", ["research", None])
async def test_without_a_model_for_the_capability_the_default_model_is_used(routing: str | None) -> None:
    adapter = FakeAdapter(["ok"])
    ex = executor(adapter)
    result = await ex.execute(job(), asyncio.Event(), routing_capability=routing)
    assert used(adapter) == ["fake-model"]
    assert ex.headers_for(result)[MODEL_HEADER] == "fake-model"


async def test_an_executor_without_capability_models_behaves_as_in_phase_5() -> None:
    adapter = FakeAdapter(["ok"])
    ex = executor(adapter, models=None)
    result = await ex.execute(job(), asyncio.Event(), routing_capability="summarize")
    assert used(adapter) == ["fake-model"]
    assert ex.headers_for(result) == ex.result_headers()


# --------------------------------------------------------------------------- the fallback (Req 2.5)


async def test_the_fallback_model_is_named_when_it_answered() -> None:
    adapter = FakeAdapter([PermanentLLMError("gone"), "ok"])
    ex = executor(adapter, fallback="backup-model")
    result = await ex.execute(job(), asyncio.Event(), routing_capability="summarize")
    assert used(adapter) == ["small-model", "backup-model"] and result.status == "Succeeded"
    assert ex.headers_for(result)[MODEL_HEADER] == "backup-model"


async def test_a_job_that_fails_names_the_model_that_failed() -> None:
    ex = executor(FakeAdapter([PermanentLLMError("gone")]))
    result = await ex.execute(job(), asyncio.Event(), routing_capability="summarize")
    assert result.status == "Failed" and "small-model" in (result.error or "")
    assert ex.headers_for(result)[MODEL_HEADER] == "small-model"


async def test_an_authentication_failure_does_not_fall_back_and_names_the_capability_model() -> None:
    adapter = FakeAdapter([AuthLLMError("bad key")])
    ex = executor(adapter, fallback="backup-model")
    result = await ex.execute(job(), asyncio.Event(), routing_capability="summarize")
    assert used(adapter) == ["small-model"] and result.status == "Failed"
    assert ex.headers_for(result)[MODEL_HEADER] == "small-model"


# --------------------------------------------------------------------------- a structured Job's correction


async def test_the_correction_of_a_structured_job_goes_to_the_model_that_answered_the_first_reply() -> None:
    """The Capability_Model failed for good and the fallback rescued the first reply; if that reply then fails the
    schema, asking the failed model again would just fail again."""
    schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
    structured = job().model_copy(update={"output_schema": schema})
    adapter = FakeAdapter([PermanentLLMError("gone"), "not json", '{"n": 1}'])
    ex = executor(adapter, fallback="backup-model")
    result = await ex.execute(structured, asyncio.Event(), routing_capability="summarize")
    assert used(adapter) == ["small-model", "backup-model", "backup-model"]
    assert result.status == "Succeeded" and ex.headers_for(result)[MODEL_HEADER] == "backup-model"


async def test_the_correction_stays_on_the_capability_model_when_that_model_answered() -> None:
    schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
    structured = job().model_copy(update={"output_schema": schema})
    adapter = FakeAdapter(["not json", '{"n": 1}'])
    ex = executor(adapter)
    result = await ex.execute(structured, asyncio.Event(), routing_capability="summarize")
    assert used(adapter) == ["small-model", "small-model"] and result.status == "Succeeded"


# --------------------------------------------------------------------------- the memory of models


async def test_headers_for_a_result_are_given_once_then_the_static_headers_apply() -> None:
    ex = executor(FakeAdapter(["ok"]))
    result = await ex.execute(job(), asyncio.Event(), routing_capability="summarize")
    assert ex.headers_for(result)[MODEL_HEADER] == "small-model"
    assert ex.headers_for(result)[MODEL_HEADER] == "fake-model"  # forgotten: the default, never a stale guess


async def test_models_nobody_asked_for_do_not_pile_up() -> None:
    ex = executor(FakeAdapter(default="ok"))
    for n in range(5000):
        await ex.execute(job(f"j{n}"), asyncio.Event(), routing_capability="summarize")
    assert len(ex._models) <= 4096  # bounded: a result that is never published must not leak memory


async def test_a_cancelled_job_leaves_nothing_behind() -> None:
    ex = executor(FakeAdapter())
    cancel = asyncio.Event()
    cancel.set()
    result = await ex.execute(job(), cancel, routing_capability="summarize")
    assert result.status == "Cancelled" and ex._models == {}
    assert ex.headers_for(result)[MODEL_HEADER] == "fake-model"


# --------------------------------------------------------------------------- the Worker publishes it


class Harness:
    def __init__(self, fake_redis, adapter: FakeAdapter, models: dict[str, str]) -> None:
        self.broker = MemoryBroker()
        self.queue = MemoryTransport(self.broker, agent_id="worker-1")
        self.control = MemoryTransport(MemoryBroker(), agent_id="worker-1-control")
        self.manager = MemoryTransport(self.broker, agent_id="manager-1")
        self.worker = Worker(
            WorkerSettings(agent_id="worker-1", host="h", capabilities=frozenset({"summarize", "research"}),
                           concurrency_limit=2),
            queue=self.queue,
            control=self.control,
            registry=AgentRegistry(fake_redis),
            executor=executor(adapter, models=models),
            clock=FakeClock(),
            metrics=WorkerMetrics(MetricsRecorder(clock=FakeClock())),
        )

    async def start(self) -> None:
        for t in (self.queue, self.control, self.manager):
            await t.start()
        await self.worker.start()

    async def dispatch(self, capability: str, job_id: str) -> None:
        payload = JobDispatchPayload(
            request_id="req-1", job_id=job_id, attempt=1, instruction="do it", required_capabilities=[capability]
        )
        await self.manager.publish(f"jobs/{capability}", new_message(payload, correlation_id="req-1"))


async def until(predicate, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_the_worker_picks_the_model_from_the_queue_a_job_came_from_and_publishes_it(fake_redis) -> None:
    adapter = FakeAdapter(default="ok")
    h = Harness(fake_redis, adapter, {"summarize": "small-model"})
    await h.start()
    try:
        await h.dispatch("summarize", "a")
        await h.dispatch("research", "b")
        await until(lambda: len(h.broker.wire_for("results")) == 2)
        by_job = {}
        for record in h.broker.wire_for("results"):
            by_job[record.headers[MODEL_HEADER]] = record.headers[PROVIDER_HEADER]
        assert set(by_job) == {"small-model", "fake-model"}  # each result names the model of its own queue
        assert sorted(used(adapter)) == ["fake-model", "small-model"]
    finally:
        await h.worker.stop()
