"""The CLI gives a Worker's executor the configured Capability_Models (das-smarter-agents task 2.3; Req 2.2)."""

from __future__ import annotations

import asyncio

from das.cli import _job_executor
from das.config.schema import LLMConfig
from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload
from tests.fakes.llm_adapter import FakeAdapter

PARAMS = ModelParams(model="big", temperature=0.0, max_output_tokens=64)


def job() -> JobDispatchPayload:
    return JobDispatchPayload(
        request_id="r", job_id="a", attempt=1, instruction="x", required_capabilities=["summarize"]
    )


def llm_config(**extra: object) -> LLMConfig:
    return LLMConfig(provider="ollama", base_url="http://ollama:11434", model="big", **extra)  # type: ignore[arg-type]


async def test_the_configured_capability_models_run_the_jobs_of_those_capabilities() -> None:
    adapter = FakeAdapter(default="ok")
    cfg = llm_config(capability_models={"summarize": "small"})
    executor = _job_executor(LLMProvider(adapter, PARAMS), "w1", 30, cfg)
    await executor.execute(job(), asyncio.Event(), routing_capability="summarize")
    await executor.execute(job(), asyncio.Event(), routing_capability="research")
    assert [c.params.model for c in adapter.calls] == ["small", "big"]


async def test_a_worker_without_llm_settings_or_models_uses_the_provider_model_throughout() -> None:
    for cfg in (None, llm_config()):
        adapter = FakeAdapter(default="ok")
        executor = _job_executor(LLMProvider(adapter, PARAMS), "w1", 30, cfg)
        await executor.execute(job(), asyncio.Event(), routing_capability="summarize")
        assert [c.params.model for c in adapter.calls] == ["big"]


async def test_the_job_timeout_is_passed_through() -> None:
    executor = _job_executor(LLMProvider(FakeAdapter(), PARAMS), "w1", 42, None)
    assert executor.job_timeout == 42
