"""Planner edge paths: zero Capabilities and LLM_Provider failure (Requirements 3.10, 3.11)."""
from __future__ import annotations

import json

import pytest

from das.llm.base import ModelParams
from das.llm.errors import LLMCallFailed, PermanentLLMError, TransientLLMError
from das.llm.retry import LLMProvider
from das.manager.planner import (
    NO_CAPABILITIES_MESSAGE,
    NoCapabilities,
    Planner,
    PlanLLMFailed,
    PlannerConfig,
    llm_failure_message,
)
from tests.fakes.llm_adapter import FakeAdapter

PARAMS = ModelParams(model="planner-model", temperature=0.0, max_output_tokens=256)
CAPS = frozenset({"search", "write"})
BAD = json.dumps({"jobs": [{"job_id": "a", "instruction": "x", "capabilities": ["paint"]}]})
VALID = json.dumps({"jobs": [{"job_id": "a", "instruction": "x", "capabilities": ["search"]}]})


def make(outcomes: list, retry_count: int = 0) -> tuple[Planner, FakeAdapter, list[float]]:
    adapter = FakeAdapter(outcomes)
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    provider = LLMProvider(adapter, PARAMS, retry_count=retry_count, sleep=sleep)
    return Planner(provider), adapter, sleeps


def _assert_message(exc: LLMCallFailed, reason: str) -> None:
    msg = llm_failure_message(exc)
    assert f"provider={FakeAdapter.provider}" in msg
    assert f"model={PARAMS.model}" in msg
    assert f"reason={reason}" in msg


# --------------------------------------------------------------------------- 3.10


@pytest.mark.parametrize("retries", [0, 5])
async def test_zero_capabilities_makes_no_llm_call(retries: int) -> None:
    # The adapter would succeed if called; the Planner must not reach it.
    planner, adapter, sleeps = make([VALID] * 10)
    out = await planner.plan("do something", frozenset(), PlannerConfig(planning_retries=retries))
    assert isinstance(out, NoCapabilities)
    assert out.error_message == NO_CAPABILITIES_MESSAGE
    assert "Capabilities" in out.error_message
    assert adapter.calls == []
    assert sleeps == []


async def test_zero_capabilities_with_failing_adapter_still_no_call() -> None:
    planner, adapter, _ = make([PermanentLLMError("should not be raised")])
    out = await planner.plan("p", frozenset(), PlannerConfig())
    assert isinstance(out, NoCapabilities)
    assert adapter.calls == []


# --------------------------------------------------------------------------- 3.11


async def test_permanent_error_surfaces_provider_model_reason() -> None:
    planner, adapter, sleeps = make([PermanentLLMError("invalid api key")], retry_count=3)
    out = await planner.plan("p", CAPS, PlannerConfig())
    assert isinstance(out, PlanLLMFailed) and out.usages == ()
    exc = out.error
    assert (exc.provider, exc.model, exc.reason, exc.attempts) == (
        "fake", "planner-model", "invalid api key", 1,
    )
    assert len(adapter.calls) == 1  # permanent errors are not retried
    assert sleeps == []
    _assert_message(exc, "invalid api key")


async def test_exhausted_transient_retries_surface_provider_model_reason() -> None:
    planner, adapter, sleeps = make([TransientLLMError("rate limited")] * 3, retry_count=2)
    out = await planner.plan("p", CAPS, PlannerConfig())
    assert isinstance(out, PlanLLMFailed)
    exc = out.error
    assert exc.provider == "fake" and exc.model == "planner-model"
    assert exc.reason == "rate limited"
    assert exc.attempts == 3
    assert len(adapter.calls) == 3
    assert sleeps == [1.0, 2.0]
    _assert_message(exc, "rate limited")


async def test_error_on_corrected_plan_attempt_propagates() -> None:
    planner, adapter, _ = make([BAD, PermanentLLMError("context length exceeded")])
    out = await planner.plan("p", CAPS, PlannerConfig(planning_retries=2))
    assert isinstance(out, PlanLLMFailed)
    # The first attempt returned a completion: its usage is kept (Phase 2 Req 9.1).
    assert [(u.operation, u.provider, u.model) for u in out.usages] == [
        ("plan", "fake", "planner-model")
    ]
    assert len(adapter.calls) == 2
    # The failing call was the corrected-plan request (prompt, prior output, correction).
    assert [m.role for m in adapter.calls[1].messages] == ["user", "assistant", "user"]
    assert out.error.attempts == 1
    _assert_message(out.error, "context length exceeded")


async def test_transient_recovery_within_provider_does_not_fail_planning() -> None:
    # Transient errors retried successfully by the provider are invisible to the Planner.
    planner, adapter, sleeps = make([TransientLLMError("reset"), VALID], retry_count=1)
    out = await planner.plan("p", CAPS, PlannerConfig())
    assert out.attempts == 1  # one planning attempt, two adapter calls
    assert len(adapter.calls) == 2 and sleeps == [1.0]
