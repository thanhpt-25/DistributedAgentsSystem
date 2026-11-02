"""Smoke tests for the Summarizer (Requirements 7.1, 7.6).

Detailed retry coverage lives in task 13.5.
"""
from __future__ import annotations

import pytest

from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.manager.summarizer import SummarizationFailed, Summarizer
from tests.fakes.llm_adapter import FakeAdapter, completion


async def _no_sleep(_: float) -> None:
    return None


def _provider(adapter: FakeAdapter) -> LLMProvider:
    return LLMProvider(
        adapter,
        ModelParams(model="m", temperature=0.0, max_output_tokens=256),
        retry_count=0,
        timeout=60,
        sleep=_no_sleep,
    )


async def test_summarize_uses_prompt_and_succeeded_contents_only() -> None:
    adapter = FakeAdapter([completion("final answer", input_tokens=7, output_tokens=3)])
    summarizer = Summarizer(_provider(adapter))

    result = await summarizer.summarize("What is X?", {"j1": "alpha", "j2": "beta"})

    assert result.content == "final answer"
    assert result.usage.input_tokens == 7
    assert len(adapter.calls) == 1
    (msg,) = adapter.calls[0].messages
    assert msg.role == "user"
    assert "What is X?" in msg.content
    assert "alpha" in msg.content and "beta" in msg.content
    assert "j1" in msg.content and "j2" in msg.content


async def test_summarize_raises_after_three_failed_attempts() -> None:
    adapter = FakeAdapter(default=PermanentLLMError("boom"))
    summarizer = Summarizer(_provider(adapter))

    with pytest.raises(SummarizationFailed) as info:
        await summarizer.summarize("p", {"j1": "c"})

    assert len(adapter.calls) == 3
    assert info.value.attempts == 3
    assert info.value.error.startswith("summarization failed:")
    assert "boom" in info.value.error


async def test_summarize_rejects_empty_succeeded() -> None:
    summarizer = Summarizer(_provider(FakeAdapter()))
    with pytest.raises(ValueError):
        await summarizer.summarize("p", {})


def test_usage_of_builds_summarize_record() -> None:
    summarizer = Summarizer(_provider(FakeAdapter()))
    rec = summarizer.usage_of(completion("x", input_tokens=2, output_tokens=5))
    assert (rec.operation, rec.provider, rec.model) == ("summarize", "fake", "m")
    assert rec.usage.total_tokens == 7 and rec.job_id is None


async def test_failed_summarization_has_no_usage() -> None:
    summarizer = Summarizer(_provider(FakeAdapter(default=PermanentLLMError("boom"))))
    with pytest.raises(SummarizationFailed) as info:
        await summarizer.summarize("p", {"j1": "c"})
    assert info.value.usages == ()  # no attempt returned a completion
