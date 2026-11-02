"""Smoke tests for the Planner (Requirements 3.1, 3.3-3.6, 3.10, 3.11)."""
from __future__ import annotations

import json

import pytest

from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.manager.planner import (
    NoCapabilities,
    PlanAccepted,
    Planner,
    PlanLLMFailed,
    PlannerConfig,
    PlanRejected,
    llm_failure_message,
)
from das.plan.validate import CheckFailure
from tests.fakes.llm_adapter import FakeAdapter, completion

PARAMS = ModelParams(model="m-1", temperature=0.0, max_output_tokens=256)
CAPS = frozenset({"search", "write"})


def _plan(*jobs: dict) -> str:
    return json.dumps({"jobs": list(jobs)})


VALID = "```json\n" + _plan(
    {"job_id": "a", "instruction": "find facts", "capabilities": ["search"]},
    {"job_id": "b", "instruction": "write it up", "capabilities": ["write"], "depends_on": ["a"]},
) + "\n```"
BAD = _plan({"job_id": "a", "instruction": "x", "capabilities": ["paint"], "depends_on": ["zz"]})


def make(outcomes: list) -> tuple[Planner, FakeAdapter]:
    adapter = FakeAdapter(outcomes)
    return Planner(LLMProvider(adapter, PARAMS, retry_count=0)), adapter


async def test_valid_first_attempt_prompt_contents() -> None:
    planner, adapter = make([VALID])
    out = await planner.plan("do research", CAPS, PlannerConfig(max_jobs=7))
    assert isinstance(out, PlanAccepted)
    assert out.attempts == 1
    assert [j.job_id for j in out.plan.jobs] == ["a", "b"]
    assert len(adapter.calls) == 1
    text = adapter.calls[0].messages[0].content
    assert "search" in text and "write" in text and "do research" in text
    assert "between 1 and 7 jobs" in text and '"$schema"' in text


async def test_correction_names_failed_checks_then_accepts() -> None:
    planner, adapter = make([BAD, VALID])
    out = await planner.plan("p", CAPS, PlannerConfig(planning_retries=2))
    assert isinstance(out, PlanAccepted) and out.attempts == 2
    msgs = adapter.calls[1].messages
    assert [m.role for m in msgs] == ["user", "assistant", "user"]
    assert msgs[1].content == BAD
    for check in (CheckFailure.UNADVERTISED_CAPABILITY, CheckFailure.UNKNOWN_DEPENDENCY):
        assert check.value in msgs[2].content


@pytest.mark.parametrize("retries", [0, 2])
async def test_rejected_after_retries_plus_one_attempts(retries: int) -> None:
    planner, adapter = make([BAD] * 10)
    out = await planner.plan("p", CAPS, PlannerConfig(planning_retries=retries))
    assert isinstance(out, PlanRejected)
    assert len(adapter.calls) == retries + 1 == out.attempts
    assert out.failed_checks == {CheckFailure.UNADVERTISED_CAPABILITY, CheckFailure.UNKNOWN_DEPENDENCY}
    assert "unadvertised_capability" in out.error_message


async def test_no_capabilities_makes_no_call() -> None:
    planner, adapter = make([VALID])
    out = await planner.plan("p", frozenset(), PlannerConfig())
    assert isinstance(out, NoCapabilities)
    assert adapter.calls == []


async def test_llm_call_failed_is_returned() -> None:
    planner, _ = make([PermanentLLMError("bad key")])
    out = await planner.plan("p", CAPS, PlannerConfig())
    assert isinstance(out, PlanLLMFailed) and out.usages == ()
    msg = llm_failure_message(out.error)
    assert out.error_message == msg
    assert "fake" in msg and "m-1" in msg and "bad key" in msg


def test_config_bounds() -> None:
    with pytest.raises(ValueError):
        PlannerConfig(planning_retries=6)
    with pytest.raises(ValueError):
        PlannerConfig(max_jobs=0)


# --------------------------------------------------------------------------- usage (Phase 2 Req 9.1)


async def test_every_attempt_with_a_completion_yields_one_plan_usage_record() -> None:
    planner, _ = make([completion(BAD, 3, 4), completion(VALID, 5, 6)])
    out = await planner.plan("p", CAPS, PlannerConfig(planning_retries=2))
    assert isinstance(out, PlanAccepted)
    assert [(u.operation, u.provider, u.model) for u in out.usages] == [
        ("plan", "fake", "m-1"), ("plan", "fake", "m-1"),
    ]
    assert [(u.usage.input_tokens, u.usage.output_tokens) for u in out.usages] == [(3, 4), (5, 6)]
    assert all(u.job_id is None and u.attempt is None for u in out.usages)


async def test_rejected_plan_keeps_usage_of_every_attempt() -> None:
    planner, _ = make([BAD] * 3)
    out = await planner.plan("p", CAPS, PlannerConfig(planning_retries=2))
    assert isinstance(out, PlanRejected) and len(out.usages) == 3


async def test_no_capabilities_has_no_usage() -> None:
    planner, _ = make([VALID])
    out = await planner.plan("p", frozenset(), PlannerConfig())
    assert isinstance(out, NoCapabilities) and out.usages == ()
