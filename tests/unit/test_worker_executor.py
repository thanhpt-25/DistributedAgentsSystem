"""JobExecutor: completion prompt, timeout, cancel, provider failure (Req 5.4-5.8)."""
from __future__ import annotations

import asyncio
import time

import pytest

from das.clock import FakeClock
from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload, TokenUsage
from das.worker.executor import JobExecutor
from tests.fakes.llm_adapter import HANG, FakeAdapter, completion

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=256)


def _job(**overrides: object) -> JobDispatchPayload:
    data: dict[str, object] = {
        "request_id": "req-1",
        "job_id": "job-b",
        "attempt": 2,
        "instruction": "Summarize the findings",
        "required_capabilities": ["summarize"],
        "dependency_results": {"job-a": "finding A", "job-0": "finding zero"},
    }
    data.update(overrides)
    return JobDispatchPayload.model_validate(data)


def _executor(adapter: FakeAdapter, job_timeout: float = 5, clock: FakeClock | None = None,
              retry_count: int = 0) -> JobExecutor:
    provider = LLMProvider(adapter, PARAMS, retry_count=retry_count, timeout=1)
    return JobExecutor(provider, "worker-1", job_timeout=job_timeout, clock=clock)


async def test_succeeded_single_call_with_instruction_and_labelled_dependencies() -> None:
    clock = FakeClock()

    def respond(system, messages, params):
        clock.advance(1.5)
        return completion("summary", input_tokens=10, output_tokens=4)

    adapter = FakeAdapter([respond])
    result = await _executor(adapter, clock=clock).execute(_job(), asyncio.Event())

    assert len(adapter.calls) == 1
    text = "\n".join(m.content for m in adapter.calls[0].messages)
    assert "Summarize the findings" in text
    assert "job-a" in text and "finding A" in text
    assert "job-0" in text and "finding zero" in text

    assert result.status == "Succeeded"
    assert result.content == "summary"
    assert result.error is None
    assert result.usage == TokenUsage(input_tokens=10, output_tokens=4, total_tokens=14)
    assert (result.request_id, result.job_id, result.attempt, result.worker_id) == (
        "req-1", "job-b", 2, "worker-1"
    )
    assert result.duration_ms == 1500


async def test_no_dependencies_sends_only_instruction() -> None:
    adapter = FakeAdapter(["done"])
    result = await _executor(adapter).execute(_job(dependency_results={}), asyncio.Event())
    assert result.status == "Succeeded"
    assert adapter.calls[0].messages[0].content.strip().endswith("Summarize the findings")


async def test_provider_failure_is_failed_with_provider_model_reason() -> None:
    adapter = FakeAdapter([PermanentLLMError("invalid api key")])
    result = await _executor(adapter).execute(_job(), asyncio.Event())
    assert result.status == "Failed"
    assert result.content == ""
    assert result.usage == TokenUsage.zero()
    assert result.error is not None
    for part in ("fake", "fake-model", "invalid api key"):
        assert part in result.error


async def test_timeout_aborts_and_reports_duration() -> None:
    adapter = FakeAdapter([HANG])
    start = time.monotonic()
    result = await _executor(adapter, job_timeout=0.05).execute(_job(), asyncio.Event())
    assert time.monotonic() - start < 1
    assert result.status == "TimedOut"
    assert result.content == ""
    assert result.error is not None and "0.05s" in result.error


async def test_cancel_event_aborts_in_flight_call_promptly() -> None:
    adapter = FakeAdapter([HANG])
    cancel = asyncio.Event()
    task = asyncio.create_task(_executor(adapter, job_timeout=30).execute(_job(), cancel))
    await asyncio.sleep(0.01)
    assert len(adapter.calls) == 1
    start = time.monotonic()
    cancel.set()
    result = await asyncio.wait_for(task, 1)
    assert time.monotonic() - start < 1
    assert result.status == "Cancelled"
    assert result.content == ""
    # The provider call task was cancelled, not left running.
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert pending == []


async def test_cancel_already_set_skips_provider_call() -> None:
    adapter = FakeAdapter()
    cancel = asyncio.Event()
    cancel.set()
    result = await _executor(adapter).execute(_job(), cancel)
    assert result.status == "Cancelled"
    assert adapter.calls == []


async def test_outer_cancellation_propagates_and_stops_call() -> None:
    adapter = FakeAdapter([HANG])
    task = asyncio.create_task(_executor(adapter, job_timeout=30).execute(_job(), asyncio.Event()))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert pending == []


@pytest.mark.parametrize("bad", [0, -1, True])
def test_rejects_invalid_job_timeout(bad: object) -> None:
    provider = LLMProvider(FakeAdapter(), PARAMS)
    with pytest.raises(ValueError):
        JobExecutor(provider, "worker-1", job_timeout=bad)  # type: ignore[arg-type]
