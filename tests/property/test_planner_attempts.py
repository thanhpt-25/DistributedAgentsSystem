# Feature: distributed-agent-system, Property 3: Bounded planning attempts with corrections
"""Property 3: Bounded planning attempts with corrections.

For any planning retry count ``r`` in 0..5, any non-empty advertised Capability set,
and any sequence of LLM outputs:

- The Planner makes ``min(k, r+1)`` LLM calls, where ``k`` is the 1-based index of the
  first valid output (infinity if none).
- Every planning prompt lists every advertised Capability.
- Every corrected-plan request names each check the previous output failed.
- When no valid output appears within ``r+1`` calls, the outcome reports the failed
  checks of the last attempt.

Outputs mix valid plans drawn against the advertised set, plans from
``invalid_plan_mutations`` (drawn in their own context, so usually invalid here),
code-fenced plans, and non-JSON text. Validity of each output in the fixed planning
context comes from ``validate_plan_text``, whose correctness is Property 2.

**Validates: Requirements 3.3, 3.5, 3.6**
"""
from __future__ import annotations

import asyncio
import json

from hypothesis import given, note, settings
from hypothesis import strategies as st

from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from das.manager.planner import Planner, PlanAccepted, PlannerConfig, PlanRejected
from das.plan.codec import print_plan
from das.plan.validate import validate_plan_text
from tests.fakes.llm_adapter import FakeAdapter
from tests.strategies import capability_names, invalid_plan_mutations, job_plans

PROMPT = "Summarize the quarterly report and draft a reply."


@st.composite
def planning_cases(draw: st.DrawFn) -> tuple[int, frozenset[str], int, list[str]]:
    retries = draw(st.integers(min_value=0, max_value=5))
    advertised = frozenset(draw(st.lists(capability_names, min_size=1, max_size=6, unique=True)))
    max_jobs = draw(st.integers(min_value=1, max_value=10))

    valid = job_plans(max_jobs=max_jobs, capabilities=advertised).map(print_plan)
    output = st.one_of(
        valid,
        valid.map(lambda text: f"```json\n{text}\n```"),
        invalid_plan_mutations(max_jobs=max_jobs).map(lambda case: json.dumps(case[0])),
        st.text(max_size=40),
    )
    # Long enough to cover every allowed attempt; extra entries must stay unused.
    outputs = draw(st.lists(output, min_size=retries + 1, max_size=retries + 3))
    return retries, advertised, max_jobs, outputs


@settings(max_examples=150, deadline=None)
@given(planning_cases())
def test_bounded_planning_attempts(case: tuple[int, frozenset[str], int, list[str]]) -> None:
    """**Validates: Requirements 3.3, 3.5, 3.6**"""
    retries, advertised, max_jobs, outputs = case
    adapter = FakeAdapter(outputs)
    llm = LLMProvider(
        adapter,
        ModelParams(model="fake-model", temperature=0.0, max_output_tokens=256),
        retry_count=0,
        timeout=60,
    )
    cfg = PlannerConfig(max_jobs=max_jobs, planning_retries=retries)

    outcome = asyncio.run(Planner(llm).plan(PROMPT, advertised, cfg))

    validations = [validate_plan_text(o, advertised, max_jobs) for o in outputs]
    first_valid = next((i + 1 for i, v in enumerate(validations) if v.plan is not None), None)
    expected_calls = min(first_valid, retries + 1) if first_valid else retries + 1
    note(f"first_valid={first_valid} expected_calls={expected_calls}")

    # min(k, r+1) LLM calls.
    assert len(adapter.calls) == expected_calls

    for index, call in enumerate(adapter.calls):
        # Each call carries the conversation so far: the planning prompt, then one
        # (previous output, corrected-plan request) pair per earlier attempt.
        assert len(call.messages) == 1 + 2 * index
        planning_prompt = call.messages[0]
        assert planning_prompt.role == "user"
        assert PROMPT in planning_prompt.content
        for cap in advertised:
            assert f"- {cap}\n" in planning_prompt.content

        for prev in range(index):
            echoed = call.messages[1 + 2 * prev]
            correction = call.messages[2 + 2 * prev]
            assert echoed.role == "assistant"
            assert echoed.content == outputs[prev]
            assert correction.role == "user"
            for check in validations[prev].failures:
                assert f"- {check.value}\n" in correction.content
            for cap in advertised:
                assert cap in correction.content

    if first_valid is not None and first_valid <= retries + 1:
        assert isinstance(outcome, PlanAccepted)
        assert outcome.attempts == first_valid
        assert outcome.plan == validations[first_valid - 1].plan
    else:
        assert isinstance(outcome, PlanRejected)
        assert outcome.attempts == retries + 1
        last = validations[retries]
        assert last.failures
        assert outcome.failed_checks == last.failures
        for check in last.failures:
            assert check.value in outcome.error_message
