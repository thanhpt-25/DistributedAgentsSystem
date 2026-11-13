"""The four Provider_Adapters' tool support (das-smarter-agents task 5.1; Phase 6 Req 5.1, 5.2).

Each adapter maps tool specs, the assistant's tool calls and the tool results to its provider's own format, and reads
the model's tool calls back as data. No network: stubbed SDK clients, ``httpx.MockTransport`` for Ollama."""

from __future__ import annotations

import json
from typing import Any

import anthropic
import botocore.exceptions as bexc
import httpx
import httpx2
import openai
import pytest
from anthropic.types import Message
from openai.types.chat import ChatCompletion

from das.llm.adapters.anthropic import AnthropicAdapter
from das.llm.adapters.bedrock import BedrockAdapter
from das.llm.adapters.ollama import OllamaAdapter
from das.llm.adapters.openai import OpenAIAdapter
from das.llm.base import ChatMessage, ModelParams
from das.llm.errors import AuthLLMError, PermanentLLMError, RateLimitedLLMError
from das.models.results import TokenUsage
from das.tools.types import AssistantTurn, ToolCall, ToolResult, ToolResults, ToolSpec, ToolTurn

SYSTEM = "You are a worker."
PARAMS = ModelParams(model="model-x", temperature=0.5, max_output_tokens=128)
SCHEMA = {"type": "object", "properties": {"expression": {"type": "string"}}, "required": ["expression"]}
TOOLS = [ToolSpec("calculator", "Evaluate arithmetic.", SCHEMA)]
CALL = ToolCall("c1", "calculator", {"expression": "2+3"})
TURNS = [
    ChatMessage(role="user", content="What is 2+3?"),
    AssistantTurn("Let me compute.", (CALL,)),
    ToolResults((ToolResult("c1", "5", False, "calculator"),)),
]
FAILED_TURNS = [*TURNS[:2], ToolResults((ToolResult("c1", "bad expression", True, "calculator"),))]
SILENT_TURNS = [TURNS[0], AssistantTurn("", (CALL,)), TURNS[2]]  # the model asked without saying anything


class Recorder:
    def __init__(self, result: Any = None, exc: BaseException | None = None) -> None:
        self.result, self.exc, self.calls = result, exc, []

    async def __call__(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if self.exc is not None:
            raise self.exc
        return self.result


class Ns:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)


def usage(i: int, o: int) -> TokenUsage:
    return TokenUsage(input_tokens=i, output_tokens=o, total_tokens=i + o)


# --------------------------------------------------------------------------- OpenAI


def openai_adapter(rec: Recorder) -> OpenAIAdapter:
    return OpenAIAdapter(client=Ns(chat=Ns(completions=Ns(create=rec))))


def openai_response(message: dict[str, Any]) -> ChatCompletion:
    return ChatCompletion.model_validate({
        "id": "x", "object": "chat.completion", "created": 1, "model": "model-x",
        "choices": [{"index": 0, "message": {"role": "assistant", **message}, "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 11, "completion_tokens": 4, "total_tokens": 15},
    })


def oai_call(call_id: str, name: str, arguments: str) -> dict[str, Any]:
    return {"id": call_id, "type": "function", "function": {"name": name, "arguments": arguments}}


def test_openai_request_carries_the_tools_the_calls_and_the_results() -> None:
    req = OpenAIAdapter.build_tool_request(SYSTEM, TURNS, TOOLS, PARAMS)
    assert req == {
        "model": "model-x",
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": "What is 2+3?"},
            {"role": "assistant", "content": "Let me compute.",
             "tool_calls": [oai_call("c1", "calculator", json.dumps({"expression": "2+3"}))]},
            {"role": "tool", "tool_call_id": "c1", "content": "5"},
        ],
        "tools": [{"type": "function", "function": {
            "name": "calculator", "description": "Evaluate arithmetic.", "parameters": SCHEMA}}],
        "temperature": 0.5,
        "max_completion_tokens": 128,
    }


def test_openai_a_silent_assistant_turn_has_null_content() -> None:
    messages = OpenAIAdapter.build_tool_request(SYSTEM, SILENT_TURNS, TOOLS, PARAMS)["messages"]
    assert messages[2]["content"] is None and messages[2]["tool_calls"]


def test_openai_several_results_become_several_tool_messages() -> None:
    two = [*TURNS[:2], ToolResults((ToolResult("c1", "5"), ToolResult("c2", "7")))]
    messages = OpenAIAdapter.build_tool_request(SYSTEM, two, TOOLS, PARAMS)["messages"]
    assert [m["tool_call_id"] for m in messages if m["role"] == "tool"] == ["c1", "c2"]


def test_openai_reads_text_calls_and_usage() -> None:
    resp = openai_response({"content": "Let me compute.", "tool_calls": [
        oai_call("a", "calculator", '{"expression": "2+3"}'), oai_call("b", "current_time", "{}")]})
    out = OpenAIAdapter.parse_tool_response(resp)
    assert out.text == "Let me compute." and out.usage == usage(11, 4)
    assert out.calls == (ToolCall("a", "calculator", {"expression": "2+3"}), ToolCall("b", "current_time", {}))


def test_openai_a_plain_answer_has_no_calls() -> None:
    out = OpenAIAdapter.parse_tool_response(openai_response({"content": "5"}))
    assert (out.text, out.calls) == ("5", ())


@pytest.mark.parametrize(("raw", "fragment"), [("not json", "JSON"), ("[1, 2]", "object"), ('"text"', "object")])
def test_openai_arguments_that_are_not_a_json_object_are_reported_on_the_call(raw: str, fragment: str) -> None:
    resp = openai_response({"content": None, "tool_calls": [oai_call("a", "t", raw)]})
    [call] = OpenAIAdapter.parse_tool_response(resp).calls
    assert call.arguments == {} and call.error is not None and fragment in call.error


async def test_openai_complete_with_tools_once_against_a_recorded_response() -> None:
    call = oai_call("a", "calculator", '{"expression": "1"}')
    rec = Recorder(openai_response({"content": None, "tool_calls": [call]}))
    out = await openai_adapter(rec).complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, timeout=7)
    assert isinstance(out, ToolTurn) and out.calls[0].name == "calculator"
    [sent] = rec.calls
    assert sent["timeout"] == 7 and sent["tools"][0]["function"]["name"] == "calculator"


async def test_openai_errors_are_classified_as_before() -> None:
    request = httpx2.Request("POST", "https://api.openai.com/v1/chat/completions")
    limited = openai.RateLimitError("slow down", response=httpx2.Response(429, request=request), body=None)
    with pytest.raises(RateLimitedLLMError):
        await openai_adapter(Recorder(exc=limited)).complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, 5)
    empty = openai_response({"content": "x"}).model_copy(update={"choices": []})
    with pytest.raises(PermanentLLMError):
        await openai_adapter(Recorder(empty)).complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, 5)


# --------------------------------------------------------------------------- Anthropic


def anthropic_adapter(rec: Recorder) -> AnthropicAdapter:
    return AnthropicAdapter(client=Ns(messages=Ns(create=rec)))


def anthropic_response(blocks: list[dict[str, Any]]) -> Message:
    return Message.model_validate({
        "id": "m", "type": "message", "role": "assistant", "model": "model-x", "content": blocks,
        "stop_reason": "tool_use", "stop_sequence": None, "usage": {"input_tokens": 30, "output_tokens": 9},
    })


def test_anthropic_request_carries_the_tools_the_calls_and_the_results() -> None:
    req = AnthropicAdapter.build_tool_request(SYSTEM, TURNS, TOOLS, PARAMS)
    assert req == {
        "model": "model-x",
        "system": SYSTEM,
        "messages": [
            {"role": "user", "content": "What is 2+3?"},
            {"role": "assistant", "content": [
                {"type": "text", "text": "Let me compute."},
                {"type": "tool_use", "id": "c1", "name": "calculator", "input": {"expression": "2+3"}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "c1", "content": "5", "is_error": False}]},
        ],
        "tools": [{"name": "calculator", "description": "Evaluate arithmetic.", "input_schema": SCHEMA}],
        "max_tokens": 128,
        "extra_body": {"temperature": 0.5},
    }


def test_anthropic_a_silent_assistant_turn_has_no_empty_text_block() -> None:
    assistant = AnthropicAdapter.build_tool_request(SYSTEM, SILENT_TURNS, TOOLS, PARAMS)["messages"][1]
    assert [b["type"] for b in assistant["content"]] == ["tool_use"]


def test_anthropic_a_failed_tool_is_marked_as_an_error() -> None:
    result = AnthropicAdapter.build_tool_request(SYSTEM, FAILED_TURNS, TOOLS, PARAMS)["messages"][2]["content"][0]
    assert result["is_error"] is True and result["content"] == "bad expression"


def test_anthropic_system_role_messages_are_folded_into_the_system_prompt() -> None:
    turns = [ChatMessage(role="system", content="Be brief."), *TURNS]
    req = AnthropicAdapter.build_tool_request(SYSTEM, turns, TOOLS, PARAMS)
    assert req["system"] == f"{SYSTEM}\n\nBe brief." and req["messages"][0]["role"] == "user"


def test_anthropic_reads_text_calls_and_usage() -> None:
    out = AnthropicAdapter.parse_tool_response(anthropic_response([
        {"type": "text", "text": "Let me compute."},
        {"type": "tool_use", "id": "t1", "name": "calculator", "input": {"expression": "2+3"}},
        {"type": "tool_use", "id": "t2", "name": "current_time", "input": {}},
    ]))
    assert out.text == "Let me compute." and out.usage == usage(30, 9)
    assert out.calls == (ToolCall("t1", "calculator", {"expression": "2+3"}), ToolCall("t2", "current_time", {}))


async def test_anthropic_complete_with_tools_once_against_a_recorded_response() -> None:
    rec = Recorder(anthropic_response([{"type": "tool_use", "id": "t", "name": "calculator", "input": {}}]))
    out = await anthropic_adapter(rec).complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, timeout=9)
    assert out.calls[0].id == "t" and rec.calls[0]["timeout"] == 9 and rec.calls[0]["tools"][0]["name"] == "calculator"


async def test_anthropic_refuses_an_empty_conversation_without_calling() -> None:
    rec = Recorder()
    only_system = [ChatMessage(role="system", content="x")]
    with pytest.raises(PermanentLLMError):
        await anthropic_adapter(rec).complete_with_tools_once(SYSTEM, only_system, TOOLS, PARAMS, 5)
    assert rec.calls == []


async def test_anthropic_an_authentication_error_is_an_auth_error() -> None:
    request = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    denied = anthropic.AuthenticationError("bad key", response=httpx2.Response(401, request=request), body=None)
    with pytest.raises(AuthLLMError):
        await anthropic_adapter(Recorder(exc=denied)).complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, 5)


# --------------------------------------------------------------------------- Bedrock


def bedrock_adapter(rec: Recorder) -> BedrockAdapter:
    return BedrockAdapter(region="us-east-1", client=Ns(converse=rec))


def bedrock_response(blocks: list[dict[str, Any]]) -> dict[str, Any]:
    return {"output": {"message": {"role": "assistant", "content": blocks}}, "stopReason": "tool_use",
            "usage": {"inputTokens": 25, "outputTokens": 5, "totalTokens": 30}}


def test_bedrock_request_carries_the_tools_the_calls_and_the_results() -> None:
    req = BedrockAdapter.build_tool_request(SYSTEM, TURNS, TOOLS, PARAMS)
    assert req == {
        "modelId": "model-x",
        "system": [{"text": SYSTEM}],
        "messages": [
            {"role": "user", "content": [{"text": "What is 2+3?"}]},
            {"role": "assistant", "content": [
                {"text": "Let me compute."},
                {"toolUse": {"toolUseId": "c1", "name": "calculator", "input": {"expression": "2+3"}}}]},
            {"role": "user", "content": [
                {"toolResult": {"toolUseId": "c1", "content": [{"text": "5"}], "status": "success"}}]},
        ],
        "toolConfig": {"tools": [{"toolSpec": {
            "name": "calculator", "description": "Evaluate arithmetic.", "inputSchema": {"json": SCHEMA}}}]},
        "inferenceConfig": {"maxTokens": 128, "temperature": 0.5},
    }


def test_bedrock_a_failed_tool_has_status_error() -> None:
    block = BedrockAdapter.build_tool_request(SYSTEM, FAILED_TURNS, TOOLS, PARAMS)["messages"][2]["content"][0]
    assert block["toolResult"]["status"] == "error"


def test_bedrock_a_silent_assistant_turn_has_no_empty_text_block() -> None:
    assistant = BedrockAdapter.build_tool_request(SYSTEM, SILENT_TURNS, TOOLS, PARAMS)["messages"][1]
    assert list(assistant["content"][0]) == ["toolUse"]


def test_bedrock_reads_text_calls_and_usage() -> None:
    out = BedrockAdapter.parse_tool_response(bedrock_response([
        {"text": "Let me compute."},
        {"toolUse": {"toolUseId": "t1", "name": "calculator", "input": {"expression": "2+3"}}},
    ]))
    assert out.text == "Let me compute." and out.usage == usage(25, 5)
    assert out.calls == (ToolCall("t1", "calculator", {"expression": "2+3"}),)


def test_bedrock_a_response_without_output_is_permanent() -> None:
    with pytest.raises(PermanentLLMError):
        BedrockAdapter.parse_tool_response({"usage": {}})


async def test_bedrock_complete_with_tools_once_against_a_recorded_response() -> None:
    rec = Recorder(bedrock_response([{"toolUse": {"toolUseId": "t", "name": "calculator", "input": {}}}]))
    out = await bedrock_adapter(rec).complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, timeout=5)
    assert out.calls[0].id == "t" and rec.calls[0]["toolConfig"]["tools"][0]["toolSpec"]["name"] == "calculator"


async def test_bedrock_access_denied_is_an_auth_error() -> None:
    denied = bexc.ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"},
                               "ResponseMetadata": {"HTTPStatusCode": 403}}, "Converse")
    with pytest.raises(AuthLLMError):
        await bedrock_adapter(Recorder(exc=denied)).complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, 5)


# --------------------------------------------------------------------------- Ollama


def ollama_adapter(handler: Any) -> OllamaAdapter:
    client = httpx.AsyncClient(base_url="http://ollama:11434", transport=httpx.MockTransport(handler))
    return OllamaAdapter("http://ollama:11434", client=client)


def ollama_message(message: dict[str, Any]) -> dict[str, Any]:
    return {"model": "model-x", "message": {"role": "assistant", **message}, "done": True,
            "prompt_eval_count": 18, "eval_count": 6}


def test_ollama_request_carries_the_tools_the_calls_and_the_results() -> None:
    req = OllamaAdapter.build_tool_request(SYSTEM, TURNS, TOOLS, PARAMS)
    assert req == {
        "model": "model-x",
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": "What is 2+3?"},
            {"role": "assistant", "content": "Let me compute.",
             "tool_calls": [{"function": {"name": "calculator", "arguments": {"expression": "2+3"}}}]},
            {"role": "tool", "content": "5", "tool_name": "calculator"},
        ],
        "tools": [{"type": "function", "function": {
            "name": "calculator", "description": "Evaluate arithmetic.", "parameters": SCHEMA}}],
        "stream": False,
        "options": {"temperature": 0.5, "num_predict": 128},
    }


def test_ollama_reads_text_and_calls_and_makes_up_the_ids_it_does_not_send() -> None:
    out = OllamaAdapter.parse_tool_response(ollama_message({"content": "Let me compute.", "tool_calls": [
        {"function": {"name": "calculator", "arguments": {"expression": "2+3"}}},
        {"function": {"name": "current_time", "arguments": {}}},
    ]}))
    assert out.text == "Let me compute." and out.usage == usage(18, 6)
    assert out.calls == (
        ToolCall("call-1", "calculator", {"expression": "2+3"}), ToolCall("call-2", "current_time", {}),
    )


def test_ollama_arguments_sent_as_a_json_string_are_read_too() -> None:
    out = OllamaAdapter.parse_tool_response(ollama_message({"content": "", "tool_calls": [
        {"function": {"name": "calculator", "arguments": '{"expression": "1"}'}}]}))
    assert out.calls[0].arguments == {"expression": "1"} and out.calls[0].error is None


def test_ollama_arguments_that_are_not_an_object_are_reported_on_the_call() -> None:
    out = OllamaAdapter.parse_tool_response(ollama_message({"content": "", "tool_calls": [
        {"function": {"name": "calculator", "arguments": "oops"}}]}))
    assert out.calls[0].error is not None and out.calls[0].arguments == {}


def test_ollama_a_response_without_a_message_is_permanent() -> None:
    with pytest.raises(PermanentLLMError):
        OllamaAdapter.parse_tool_response({"done": True})


async def test_ollama_complete_with_tools_once_against_a_recorded_response() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=ollama_message({"content": "", "tool_calls": [
            {"function": {"name": "calculator", "arguments": {"expression": "1"}}}]}))

    adapter = ollama_adapter(handler)
    out = await adapter.complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, timeout=5)
    await adapter.aclose()
    assert out.calls[0].name == "calculator"
    assert json.loads(seen[0].content) == OllamaAdapter.build_tool_request(SYSTEM, TURNS[:1], TOOLS, PARAMS)


async def test_ollama_a_401_is_an_auth_error() -> None:
    adapter = ollama_adapter(lambda request: httpx.Response(401, json={"error": "no"}))
    with pytest.raises(AuthLLMError):
        await adapter.complete_with_tools_once(SYSTEM, TURNS[:1], TOOLS, PARAMS, 5)
    await adapter.aclose()


# --------------------------------------------------------------------------- odd answers from providers


def test_openai_a_tool_call_of_another_kind_is_skipped_not_a_crash() -> None:
    custom = Ns(id="x", type="custom", custom=Ns(name="n", input="i"))  # no ``function``
    real = Ns(id="a", function=Ns(name="calculator", arguments="{}"))
    resp = Ns(choices=[Ns(message=Ns(content="hi", tool_calls=[custom, real]))],
              usage=Ns(prompt_tokens=1, completion_tokens=1, total_tokens=2))
    assert [c.id for c in OpenAIAdapter.parse_tool_response(resp).calls] == ["a"]


def test_openai_empty_arguments_mean_no_arguments_and_a_call_without_text_has_empty_text() -> None:
    resp = openai_response({"content": None, "tool_calls": [oai_call("a", "current_time", "")]})
    out = OpenAIAdapter.parse_tool_response(resp)
    assert out.text == "" and out.calls == (ToolCall("a", "current_time", {}),)


def test_ollama_entries_that_are_not_tool_calls_are_skipped() -> None:
    out = OllamaAdapter.parse_tool_response(ollama_message({"content": "", "tool_calls": [
        "oops", {"function": "x"}, {"function": {"name": "calculator", "arguments": {}}}]}))
    assert [c.name for c in out.calls] == ["calculator"]


def test_anthropic_a_tool_input_that_is_not_an_object_is_reported_on_the_call() -> None:
    block = Ns(type="tool_use", id="t", name="calculator", input=["x"])
    resp = Ns(content=[block], usage=Ns(input_tokens=1, output_tokens=1))
    [call] = AnthropicAdapter.parse_tool_response(resp).calls
    assert call.arguments == {} and call.error is not None


# --------------------------------------------------------------------------- all four


@pytest.mark.parametrize("adapter", [OpenAIAdapter, AnthropicAdapter, BedrockAdapter, OllamaAdapter])
def test_every_adapter_offers_tool_calls(adapter: type) -> None:
    for name in ("complete_with_tools_once", "build_tool_request", "parse_tool_response"):
        assert callable(getattr(adapter, name, None)), name
