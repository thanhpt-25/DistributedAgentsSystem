"""JobExecutor with an Output_Schema: validate, one corrective completion, fail
(das-smarter-agents task 1.3; Req 1.4 to 1.7)."""

from __future__ import annotations

import asyncio
import json
import time

from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload, TokenUsage
from das.worker.executor import JobExecutor, build_user_message
from tests.fakes.llm_adapter import HANG, FakeAdapter, completion

PARAMS = ModelParams(model="fake-model", temperature=0.0, max_output_tokens=256)
SCHEMA = {
    "type": "object",
    "properties": {"name": {"type": "string"}, "age": {"type": "integer", "minimum": 0}},
    "required": ["name", "age"],
    "additionalProperties": False,
}
GOOD = '{"name": "Ada", "age": 36}'
BAD = '{"name": "Ada", "age": -1}'


def job(schema: dict | None = SCHEMA) -> JobDispatchPayload:
    data: dict = {
        "request_id": "req-1", "job_id": "job-a", "attempt": 1, "instruction": "Describe the person",
        "required_capabilities": ["llm"], "dependency_results": {},
    }
    if schema is not None:
        data["output_schema"] = schema
    return JobDispatchPayload.model_validate(data)


def executor(adapter: FakeAdapter, job_timeout: float = 5) -> JobExecutor:
    return JobExecutor(LLMProvider(adapter, PARAMS, retry_count=0, timeout=1), "worker-1", job_timeout=job_timeout)


async def run(adapter: FakeAdapter, schema: dict | None = SCHEMA):  # noqa: ANN201
    return await executor(adapter).execute(job(schema), asyncio.Event())


# --------------------------------------------------------------------------- a valid first reply (Req 1.4, 1.7)


async def test_a_valid_reply_succeeds_with_canonical_json_after_one_call() -> None:
    adapter = FakeAdapter([completion(GOOD, 10, 4)])
    result = await run(adapter)
    assert len(adapter.calls) == 1
    assert (result.status, result.error) == ("Succeeded", None)
    assert result.content == '{"age":36,"name":"Ada"}'
    assert result.usage == TokenUsage(input_tokens=10, output_tokens=4, total_tokens=14)


async def test_the_first_prompt_shows_the_model_the_schema() -> None:
    adapter = FakeAdapter([GOOD])
    await run(adapter)
    prompt = adapter.calls[0].messages[0].content
    assert json.dumps(SCHEMA, sort_keys=True) in prompt
    assert "Describe the person" in prompt


async def test_a_job_without_a_schema_gets_exactly_the_phase_5_prompt_and_content() -> None:
    adapter = FakeAdapter(["just prose, not JSON"])
    result = await run(adapter, schema=None)
    assert adapter.calls[0].messages[0].content == build_user_message(job(None))
    assert "schema" not in adapter.calls[0].messages[0].content.lower()
    assert (result.status, result.content) == ("Succeeded", "just prose, not JSON")


async def test_a_reply_in_a_code_fence_is_accepted() -> None:
    result = await run(FakeAdapter(["```json\n" + GOOD + "\n```"]))
    assert result.status == "Succeeded" and result.content == '{"age":36,"name":"Ada"}'


# --------------------------------------------------------------------------- the corrective completion (Req 1.5)


async def test_an_invalid_reply_gets_exactly_one_correction_that_can_succeed() -> None:
    adapter = FakeAdapter([completion(BAD, 10, 4), completion(GOOD, 20, 5)])
    result = await run(adapter)
    assert len(adapter.calls) == 2
    assert result.status == "Succeeded" and result.content == '{"age":36,"name":"Ada"}'
    assert result.usage == TokenUsage(input_tokens=30, output_tokens=9, total_tokens=39)  # both calls (Req 1.5)


async def test_the_correction_shows_the_rejected_reply_and_names_the_path_and_reason() -> None:
    adapter = FakeAdapter([BAD, GOOD])
    await run(adapter)
    turns = adapter.calls[1].messages
    assert [m.role for m in turns] == ["user", "assistant", "user"]
    assert turns[1].content == BAD  # the model's own reply, so it can see what it said
    assert "$.age" in turns[2].content and "minimum" in turns[2].content


async def test_the_correction_message_never_quotes_the_rejected_content() -> None:
    secret = "hunter2-very-secret"
    adapter = FakeAdapter([json.dumps({"name": secret, "age": "old"}), GOOD])
    await run(adapter)
    assert secret not in adapter.calls[1].messages[-1].content


async def test_a_reply_that_is_not_json_is_corrected_too() -> None:
    adapter = FakeAdapter(["Sure, here you go!", GOOD])
    result = await run(adapter)
    assert result.status == "Succeeded" and len(adapter.calls) == 2
    assert "JSON" in adapter.calls[1].messages[-1].content


# --------------------------------------------------------------------------- still invalid (Req 1.6)


async def test_a_second_invalid_reply_fails_with_the_violation_and_spends_no_third_call() -> None:
    adapter = FakeAdapter([completion(BAD, 10, 4), completion('{"name": "Ada"}', 20, 5)])
    result = await run(adapter)
    assert len(adapter.calls) == 2
    assert result.status == "Failed" and result.content == ""
    assert result.error is not None and result.error.startswith("output_schema_violation: $: ")
    assert "age" in result.error  # the violation of the corrected reply, not of the first
    assert result.usage == TokenUsage(input_tokens=30, output_tokens=9, total_tokens=39)  # tokens were still spent


async def test_the_failure_text_never_contains_the_rejected_content() -> None:
    secret = "hunter2-very-secret"
    adapter = FakeAdapter([json.dumps({"name": 1, "age": 1, "note": secret})] * 2)
    result = await run(adapter)
    assert result.status == "Failed" and result.error is not None
    assert secret not in result.error


async def test_a_provider_failure_on_the_correction_fails_the_job_but_keeps_the_first_calls_tokens() -> None:
    adapter = FakeAdapter([completion(BAD, 10, 4), PermanentLLMError("model overloaded for good")])
    result = await run(adapter)
    assert result.status == "Failed"
    assert result.error is not None and "model overloaded for good" in result.error
    assert result.usage == TokenUsage(input_tokens=10, output_tokens=4, total_tokens=14)


# --------------------------------------------------------------------------- a bad schema reaching the worker


async def test_a_schema_that_is_invalid_fails_the_job_without_calling_the_model() -> None:
    adapter = FakeAdapter()
    result = await run(adapter, schema={"type": "nonsense"})
    assert adapter.calls == []
    assert result.status == "Failed"
    assert result.error is not None and result.error.startswith("output_schema_invalid: ")


# --------------------------------------------------------------------------- timeout and cancel cover both calls


async def test_the_job_timeout_covers_the_correction_too() -> None:
    adapter = FakeAdapter([BAD, HANG])
    start = time.monotonic()
    result = await executor(adapter, job_timeout=0.1).execute(job(), asyncio.Event())
    assert time.monotonic() - start < 1
    assert result.status == "TimedOut" and len(adapter.calls) == 2


async def test_cancel_during_the_correction_stops_it() -> None:
    adapter = FakeAdapter([BAD, HANG])
    cancel = asyncio.Event()
    task = asyncio.create_task(executor(adapter, job_timeout=30).execute(job(), cancel))
    for _ in range(100):
        if len(adapter.calls) == 2:
            break
        await asyncio.sleep(0.01)
    assert len(adapter.calls) == 2
    cancel.set()
    result = await asyncio.wait_for(task, 1)
    assert result.status == "Cancelled"
    assert [t for t in asyncio.all_tasks() if t is not asyncio.current_task()] == []
