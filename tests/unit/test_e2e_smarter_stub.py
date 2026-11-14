"""The fake LLM of the Phase 6 end-to-end tests behaves as the scenarios assume (das-smarter-agents task 7.3), checked
without Docker: its plans pass the real plan validator, its replan passes the real replan rules, and its replies follow
the Ollama wire format the real adapter reads. A stub that is wrong would make the end-to-end run fail for the wrong
reason, or pass for it."""

from __future__ import annotations

import json
from typing import Any

import pytest

from das.llm.adapters.ollama import OllamaAdapter
from das.manager.planner import build_replan_prompt
from das.models.plan import JobPlan
from das.plan.codec import parse_plan
from das.plan.replan import FailedJob, ReplanContext, SucceededJob, replan_issues
from das.plan.validate import validate_plan_text
from tests.integration.e2e import llm_stub_smart as stub
from tests.integration.e2e.llm_stub import PLANNER_MARK, WORKER_MARK

CAPABILITIES = {"research", "summarize", "code-review", "tool-calculator"}


def worker_body(*users: str, model: str = "fake-llm", **extra: Any) -> dict[str, Any]:
    messages: list[dict[str, Any]] = [{"role": "system", "content": WORKER_MARK + " ..."}]
    messages += [{"role": "user", "content": u} for u in users]
    return {"model": model, "messages": messages, **extra}


# --------------------------------------------------------------------------- the plans


@pytest.mark.parametrize("scenario", ["structured", "replan", "tool"])
def test_every_scenario_plan_passes_the_real_plan_validator(scenario: str) -> None:
    plan = stub.first_plan(f"do the thing [[scenario={scenario}]]")
    assert plan is not None
    result = validate_plan_text(json.dumps(plan), CAPABILITIES, 10)
    assert result.ok, result.issues


def test_a_prompt_without_a_marker_gets_the_base_stubs_plan() -> None:
    assert stub.first_plan("just a prompt") is None


def test_the_structured_plan_has_a_schema_and_the_tool_plan_requires_the_tool_capability() -> None:
    structured = stub.first_plan("[[scenario=structured]]")
    tool = stub.first_plan("[[scenario=tool]]")
    assert structured is not None and tool is not None
    assert structured["jobs"][0]["output_schema"] == stub.ANSWER_SCHEMA
    assert "tool-calculator" in tool["jobs"][0]["capabilities"]


def test_the_replan_keeps_what_succeeded_and_passes_the_real_replan_rules() -> None:
    prompt = "do it [[scenario=replan]]"
    first = parse_plan(json.dumps(stub.first_plan(prompt)))
    jobs = first.by_id()
    context = ReplanContext(
        succeeded=(SucceededJob(jobs["j2"], "fine"),),
        failed=(FailedJob(jobs["j1"], "scripted permanent failure"), FailedJob(jobs["j3"], None)),
    )
    replan_prompt = build_replan_prompt(prompt, frozenset(CAPABILITIES), 10, context)
    chosen = stub.planner_reply(replan_prompt)
    new = parse_plan(chosen)
    assert isinstance(new, JobPlan) and replan_issues(new, context, CAPABILITIES) == []
    ids = new.job_ids()
    assert "j2" in ids and "j1" not in ids and "j3" not in ids and {"r1", "r2"} <= set(ids)
    for job in new.jobs:  # the new Jobs cannot fail the same way; the kept one is unchanged, markers included
        if job.job_id in {"r1", "r2"}:
            assert "[[" not in job.instruction


# --------------------------------------------------------------------------- the replies


def test_the_missing_model_is_a_404() -> None:
    status, body = stub.chat(worker_body("hi", model="missing-model"))
    assert status == 404 and "not found" in body["error"]


def test_a_job_marked_to_fail_is_a_400() -> None:
    assert stub.chat(worker_body("Instruction:\n[research] [[fail]] x"))[0] == 400


def test_an_unmarked_worker_message_is_answered_like_the_base_stub() -> None:
    status, body = stub.chat(worker_body("Instruction:\n[research] hello"))
    assert status == 200 and body["message"]["content"].startswith("fake-llm worker output for:")


def test_the_structured_job_is_answered_in_prose_first_and_in_json_after_the_correction() -> None:
    first = stub.chat(worker_body(f"Instruction:\n[research] [[json]] x\n\n{stub.JSON_MARK}:\n{{}}"))[1]
    assert first["message"]["content"] == "The answer is forty two"
    users = [f"Instruction:\n...\n\n{stub.JSON_MARK}:\n{{}}", f"{stub.CORRECTION_MARK} the required JSON Schema at $"]
    second = stub.chat(worker_body(*users))[1]
    assert json.loads(second["message"]["content"]) == {"answer": 42}


def test_the_tool_job_asks_for_the_calculator_then_answers_from_its_result() -> None:
    tools = [{"type": "function", "function": {"name": "calculator"}}]
    asking = stub.chat(worker_body("Instruction:\n[[tool]]", tools=tools))[1]
    call = asking["message"]["tool_calls"][0]["function"]
    assert call == {"name": "calculator", "arguments": {"expression": "6*7"}}
    body = worker_body("Instruction:\n[[tool]]", tools=tools)
    body["messages"] += [{"role": "assistant", "content": "", "tool_calls": asking["message"]["tool_calls"]},
                         {"role": "tool", "content": "42", "tool_name": "calculator"}]
    assert stub.chat(body)[1]["message"]["content"] == "The result is 42"


def test_the_real_ollama_adapter_reads_the_stubs_tool_call() -> None:
    tools = [{"type": "function", "function": {"name": "calculator"}}]
    asking = stub.chat(worker_body("Instruction:\n[[tool]]", tools=tools))[1]
    turn = OllamaAdapter.parse_tool_response(asking)
    assert [(c.name, c.arguments) for c in turn.calls] == [("calculator", {"expression": "6*7"})]


def test_the_planner_message_goes_through_the_chat_endpoint() -> None:
    body = {"model": "fake-llm", "messages": [
        {"role": "system", "content": PLANNER_MARK + " ..."},
        {"role": "user", "content": "Advertised capabilities:\n- research\n- summarize\n\nRequest:\n[[scenario=tool]]"},
    ]}
    status, reply = stub.chat(body)
    assert status == 200 and '"tool-calculator"' in reply["message"]["content"]


# --------------------------------------------------------------------------- the Compose override


def test_the_override_environment_reaches_the_configuration_fields_it_names(tmp_path: Any) -> None:
    from pathlib import Path

    import yaml

    from das.config import load_config

    override = yaml.safe_load((Path(stub.__file__).parent / "docker-compose.smarter.e2e.yml").read_text())
    services = override["services"]
    base = {
        "role": "worker", "host": "10.0.0.7", "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"}, "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "ollama", "base_url": "http://llm-stub:11434", "model": "fake-llm"},
        "worker": {"capabilities": ["research", "summarize"]},
    }
    path = tmp_path / "worker.yaml"
    path.write_text(yaml.safe_dump(base), encoding="utf-8")
    worker = load_config(path, services["worker-research"]["environment"])
    assert worker.llm is not None
    assert worker.llm.capability_models == {"research": "missing-model"} and worker.llm.fallback_model == "fake-llm"
    assert worker.tools.enabled == ["calculator"]
    manager = load_config(path, services["manager"]["environment"])
    assert manager.limits.max_replans == 1 and manager.limits.max_attempts == 1
