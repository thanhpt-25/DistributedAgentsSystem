"""Unit tests for the Provider_Adapters (Requirements 10.1, 10.2, 10.3).

Each adapter is exercised against recorded provider responses with no real
network: stubbed SDK clients for OpenAI/Anthropic/Bedrock and
``httpx.MockTransport`` for Ollama. Covers request mapping, usage extraction,
and transient/permanent error classification.
"""
from __future__ import annotations

import json
from typing import Any

import anthropic
import botocore.exceptions as bexc
import httpx
import httpx2  # transport package used by the installed openai/anthropic SDKs
import openai
import pytest
from anthropic.types import Message
from openai.types.chat import ChatCompletion

from das.llm.adapters._common import error_for_status, split_system, usage
from das.llm.adapters.anthropic import AnthropicAdapter, classify_anthropic_error
from das.llm.adapters.bedrock import BedrockAdapter, classify_bedrock_error
from das.llm.adapters.ollama import OllamaAdapter, classify_ollama_error
from das.llm.adapters.openai import OpenAIAdapter, classify_openai_error
from das.llm.base import ChatMessage, Completion, ModelParams, ProviderAdapter
from das.llm.errors import PermanentLLMError, TransientLLMError
from das.models.results import TokenUsage

SYSTEM = "You are a planner."
MSGS = [
    ChatMessage(role="user", content="Plan a trip."),
    ChatMessage(role="assistant", content="Where to?"),
    ChatMessage(role="system", content="Be concise."),
    ChatMessage(role="user", content="Hanoi."),
]
PARAMS = ModelParams(model="model-x", temperature=0.7, max_output_tokens=256)


class _Recorder:
    """Async callable that records kwargs and returns (or raises) a canned value."""

    def __init__(self, result: Any = None, exc: BaseException | None = None) -> None:
        self.result, self.exc, self.calls = result, exc, []

    async def __call__(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if self.exc is not None:
            raise self.exc
        return self.result


class _Ns:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)


# ---------------------------------------------------------------- recorded responses

OPENAI_RESPONSE = {
    "id": "chatcmpl-123",
    "object": "chat.completion",
    "created": 1_700_000_000,
    "model": "model-x",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "Visit the Old Quarter."},
            "finish_reason": "stop",
            "logprobs": None,
        }
    ],
    "usage": {"prompt_tokens": 42, "completion_tokens": 7, "total_tokens": 49},
}

ANTHROPIC_RESPONSE = {
    "id": "msg_01",
    "type": "message",
    "role": "assistant",
    "model": "model-x",
    "content": [
        {"type": "text", "text": "Visit "},
        {"type": "text", "text": "the Old Quarter."},
    ],
    "stop_reason": "end_turn",
    "stop_sequence": None,
    "usage": {"input_tokens": 30, "output_tokens": 9},
}

BEDROCK_RESPONSE = {
    "output": {
        "message": {
            "role": "assistant",
            "content": [{"text": "Visit "}, {"text": "the Old Quarter."}],
        }
    },
    "stopReason": "end_turn",
    "usage": {"inputTokens": 25, "outputTokens": 5, "totalTokens": 30},
    "metrics": {"latencyMs": 120},
}

OLLAMA_RESPONSE = {
    "model": "model-x",
    "created_at": "2024-01-01T00:00:00Z",
    "message": {"role": "assistant", "content": "Visit the Old Quarter."},
    "done": True,
    "prompt_eval_count": 18,
    "eval_count": 6,
}


# ---------------------------------------------------------------- shared helpers


def test_usage_computes_total_when_missing() -> None:
    assert usage(3, 4) == TokenUsage(input_tokens=3, output_tokens=4, total_tokens=7)
    assert usage(None, None) == TokenUsage(input_tokens=0, output_tokens=0, total_tokens=0)
    assert usage(3, 4, 10).total_tokens == 10


def test_split_system_folds_system_role_messages() -> None:
    sys_prompt, turns = split_system(SYSTEM, MSGS)
    assert sys_prompt == "You are a planner.\n\nBe concise."
    assert [m.role for m in turns] == ["user", "assistant", "user"]


@pytest.mark.parametrize(
    ("status", "transient"),
    [(400, False), (401, False), (403, False), (404, False), (413, False), (422, False),
     (408, True), (429, True), (500, True), (502, True), (503, True), (529, True)],
)
def test_error_for_status(status: int, transient: bool) -> None:
    err = error_for_status("p", status, "boom")
    assert isinstance(err, TransientLLMError if transient else PermanentLLMError)
    assert f"HTTP {status}" in str(err) and "boom" in str(err)


# ---------------------------------------------------------------- OpenAI

_OAI_REQ = httpx2.Request("POST", "https://api.openai.com/v1/chat/completions")


def _oai_status(cls: type[openai.APIStatusError], status: int, msg: str) -> openai.APIStatusError:
    return cls(msg, response=httpx2.Response(status, request=_OAI_REQ), body=None)


def _openai_adapter(rec: _Recorder) -> OpenAIAdapter:
    return OpenAIAdapter(client=_Ns(chat=_Ns(completions=_Ns(create=rec))))


def test_openai_build_request() -> None:
    req = OpenAIAdapter.build_request(SYSTEM, MSGS, PARAMS)
    assert req == {
        "model": "model-x",
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": "Plan a trip."},
            {"role": "assistant", "content": "Where to?"},
            {"role": "system", "content": "Be concise."},
            {"role": "user", "content": "Hanoi."},
        ],
        "temperature": 0.7,
        "max_completion_tokens": 256,
    }
    assert OpenAIAdapter.build_request("", MSGS[:1], PARAMS)["messages"] == [
        {"role": "user", "content": "Plan a trip."}
    ]


async def test_openai_complete_once_against_recorded_response() -> None:
    rec = _Recorder(ChatCompletion.model_validate(OPENAI_RESPONSE))
    out = await _openai_adapter(rec).complete_once(SYSTEM, MSGS, PARAMS, timeout=12)
    assert out == Completion(
        content="Visit the Old Quarter.",
        usage=TokenUsage(input_tokens=42, output_tokens=7, total_tokens=49),
    )
    (call,) = rec.calls
    assert call["timeout"] == 12 and call["model"] == "model-x"


def test_openai_parse_response_rejects_empty_choices() -> None:
    resp = ChatCompletion.model_validate({**OPENAI_RESPONSE, "choices": []})
    with pytest.raises(PermanentLLMError):
        OpenAIAdapter.parse_response(resp)


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (_oai_status(openai.RateLimitError, 429, "rate limited"), True),
        (_oai_status(openai.InternalServerError, 503, "unavailable"), True),
        (openai.APITimeoutError(request=_OAI_REQ), True),
        (openai.APIConnectionError(request=_OAI_REQ), True),
        (_oai_status(openai.AuthenticationError, 401, "bad key"), False),
        (_oai_status(openai.PermissionDeniedError, 403, "forbidden"), False),
        (_oai_status(openai.BadRequestError, 400, "context_length_exceeded"), False),
        (_oai_status(openai.NotFoundError, 404, "model not found"), False),
    ],
)
async def test_openai_error_classification(exc: BaseException, transient: bool) -> None:
    expected = TransientLLMError if transient else PermanentLLMError
    assert isinstance(classify_openai_error(exc), expected)
    with pytest.raises(expected) as info:
        await _openai_adapter(_Recorder(exc=exc)).complete_once(SYSTEM, MSGS, PARAMS, timeout=5)
    assert info.value.__cause__ is exc


# ---------------------------------------------------------------- Anthropic

_ANT_REQ = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")


def _ant_status(cls: type[anthropic.APIStatusError], status: int, msg: str) -> anthropic.APIStatusError:
    return cls(msg, response=httpx2.Response(status, request=_ANT_REQ), body=None)


def _anthropic_adapter(rec: _Recorder) -> AnthropicAdapter:
    return AnthropicAdapter(client=_Ns(messages=_Ns(create=rec)))


def test_anthropic_build_request() -> None:
    req = AnthropicAdapter.build_request(SYSTEM, MSGS, PARAMS)
    assert req == {
        "model": "model-x",
        "system": "You are a planner.\n\nBe concise.",
        "messages": [
            {"role": "user", "content": "Plan a trip."},
            {"role": "assistant", "content": "Where to?"},
            {"role": "user", "content": "Hanoi."},
        ],
        "max_tokens": 256,
        "extra_body": {"temperature": 0.7},
    }
    assert "system" not in AnthropicAdapter.build_request("", MSGS[:1], PARAMS)


async def test_anthropic_complete_once_against_recorded_response() -> None:
    rec = _Recorder(Message.model_validate(ANTHROPIC_RESPONSE))
    out = await _anthropic_adapter(rec).complete_once(SYSTEM, MSGS, PARAMS, timeout=9)
    assert out == Completion(
        content="Visit the Old Quarter.",
        usage=TokenUsage(input_tokens=30, output_tokens=9, total_tokens=39),
    )
    assert rec.calls[0]["timeout"] == 9


async def test_anthropic_rejects_system_only_messages_without_calling() -> None:
    rec = _Recorder(Message.model_validate(ANTHROPIC_RESPONSE))
    with pytest.raises(PermanentLLMError):
        await _anthropic_adapter(rec).complete_once(
            SYSTEM, [ChatMessage(role="system", content="x")], PARAMS, timeout=5
        )
    assert rec.calls == []


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (_ant_status(anthropic.RateLimitError, 429, "rate limited"), True),
        (_ant_status(anthropic.InternalServerError, 529, "overloaded"), True),
        (anthropic.APITimeoutError(request=_ANT_REQ), True),
        (anthropic.APIConnectionError(request=_ANT_REQ), True),
        (_ant_status(anthropic.AuthenticationError, 401, "bad key"), False),
        (_ant_status(anthropic.PermissionDeniedError, 403, "forbidden"), False),
        (_ant_status(anthropic.BadRequestError, 400, "prompt is too long"), False),
        (_ant_status(anthropic.NotFoundError, 404, "model not found"), False),
    ],
)
async def test_anthropic_error_classification(exc: BaseException, transient: bool) -> None:
    expected = TransientLLMError if transient else PermanentLLMError
    assert isinstance(classify_anthropic_error(exc), expected)
    with pytest.raises(expected):
        await _anthropic_adapter(_Recorder(exc=exc)).complete_once(SYSTEM, MSGS, PARAMS, timeout=5)


# ---------------------------------------------------------------- Bedrock


def _client_error(code: str, status: int, msg: str = "m") -> bexc.ClientError:
    return bexc.ClientError(
        {"Error": {"Code": code, "Message": msg}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "Converse",
    )


def _bedrock_adapter(rec: _Recorder) -> BedrockAdapter:
    return BedrockAdapter(region="us-east-1", client=_Ns(converse=rec))


def test_bedrock_build_request() -> None:
    req = BedrockAdapter.build_request(SYSTEM, MSGS, PARAMS)
    assert req == {
        "modelId": "model-x",
        "system": [{"text": "You are a planner.\n\nBe concise."}],
        "messages": [
            {"role": "user", "content": [{"text": "Plan a trip."}]},
            {"role": "assistant", "content": [{"text": "Where to?"}]},
            {"role": "user", "content": [{"text": "Hanoi."}]},
        ],
        "inferenceConfig": {"maxTokens": 256, "temperature": 0.7},
    }


async def test_bedrock_complete_once_against_recorded_response() -> None:
    rec = _Recorder(BEDROCK_RESPONSE)
    out = await _bedrock_adapter(rec).complete_once(SYSTEM, MSGS, PARAMS, timeout=5)
    assert out == Completion(
        content="Visit the Old Quarter.",
        usage=TokenUsage(input_tokens=25, output_tokens=5, total_tokens=30),
    )
    assert rec.calls[0]["modelId"] == "model-x"


def test_bedrock_parse_response_missing_output() -> None:
    with pytest.raises(PermanentLLMError):
        BedrockAdapter.parse_response({"usage": {}})


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (_client_error("ThrottlingException", 429), True),
        (_client_error("ServiceUnavailableException", 503), True),
        (_client_error("InternalServerException", 500), True),
        (_client_error("ModelTimeoutException", 408), True),
        (bexc.ReadTimeoutError(endpoint_url="https://bedrock"), True),
        (bexc.EndpointConnectionError(endpoint_url="https://bedrock"), True),
        (_client_error("AccessDeniedException", 403), False),
        (_client_error("UnrecognizedClientException", 403), False),
        (_client_error("ValidationException", 400, "Input is too long"), False),
        (_client_error("ResourceNotFoundException", 404), False),
        (bexc.NoCredentialsError(), False),
    ],
)
async def test_bedrock_error_classification(exc: BaseException, transient: bool) -> None:
    expected = TransientLLMError if transient else PermanentLLMError
    assert isinstance(classify_bedrock_error(exc), expected)
    with pytest.raises(expected):
        await _bedrock_adapter(_Recorder(exc=exc)).complete_once(SYSTEM, MSGS, PARAMS, timeout=5)


def test_bedrock_reason_includes_code_status_and_message() -> None:
    reason = str(classify_bedrock_error(_client_error("ValidationException", 400, "bad model id")))
    assert "ValidationException" in reason and "HTTP 400" in reason and "bad model id" in reason


# ---------------------------------------------------------------- Ollama


def _ollama_adapter(handler: Any) -> OllamaAdapter:
    client = httpx.AsyncClient(base_url="http://ollama:11434", transport=httpx.MockTransport(handler))
    return OllamaAdapter("http://ollama:11434", client=client)


def test_ollama_build_request() -> None:
    req = OllamaAdapter.build_request(SYSTEM, MSGS[:1], PARAMS)
    assert req == {
        "model": "model-x",
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": "Plan a trip."},
        ],
        "stream": False,
        "options": {"temperature": 0.7, "num_predict": 256},
    }


async def test_ollama_complete_once_against_recorded_response() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=OLLAMA_RESPONSE)

    adapter = _ollama_adapter(handler)
    out = await adapter.complete_once(SYSTEM, MSGS, PARAMS, timeout=5)
    await adapter.aclose()
    assert out == Completion(
        content="Visit the Old Quarter.",
        usage=TokenUsage(input_tokens=18, output_tokens=6, total_tokens=24),
    )
    (req,) = seen
    assert req.method == "POST" and req.url.path == "/api/chat"
    assert json.loads(req.content) == OllamaAdapter.build_request(SYSTEM, MSGS, PARAMS)


@pytest.mark.parametrize(
    ("status", "transient"),
    [(429, True), (500, True), (503, True), (400, False), (404, False)],
)
async def test_ollama_http_status_classification(status: int, transient: bool) -> None:
    adapter = _ollama_adapter(lambda r: httpx.Response(status, json={"error": "model 'x' not found"}))
    expected = TransientLLMError if transient else PermanentLLMError
    with pytest.raises(expected) as info:
        await adapter.complete_once(SYSTEM, MSGS, PARAMS, timeout=5)
    assert f"HTTP {status}" in str(info.value) and "not found" in str(info.value)
    await adapter.aclose()


@pytest.mark.parametrize("exc_cls", [httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError])
async def test_ollama_transport_errors_are_transient(exc_cls: type[httpx.TransportError]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc_cls("boom", request=request)

    adapter = _ollama_adapter(handler)
    with pytest.raises(TransientLLMError):
        await adapter.complete_once(SYSTEM, MSGS, PARAMS, timeout=5)
    await adapter.aclose()


async def test_ollama_invalid_json_is_permanent() -> None:
    adapter = _ollama_adapter(lambda r: httpx.Response(200, content=b"not json"))
    with pytest.raises(PermanentLLMError):
        await adapter.complete_once(SYSTEM, MSGS, PARAMS, timeout=5)
    await adapter.aclose()


def test_ollama_parse_response_errors() -> None:
    with pytest.raises(PermanentLLMError):
        OllamaAdapter.parse_response({"done": True})
    assert isinstance(classify_ollama_error(ValueError("x")), PermanentLLMError)


# ---------------------------------------------------------------- common interface (Req 10.2)


def test_all_adapters_implement_provider_protocol() -> None:
    adapters = [
        OpenAIAdapter(client=_Ns()),
        AnthropicAdapter(client=_Ns()),
        BedrockAdapter(region="us-east-1", client=_Ns()),
        OllamaAdapter("http://ollama:11434", client=httpx.AsyncClient()),
    ]
    assert [a.provider for a in adapters] == ["openai", "anthropic", "bedrock", "ollama"]
    assert all(isinstance(a, ProviderAdapter) for a in adapters)
