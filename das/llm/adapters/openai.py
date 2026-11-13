"""OpenAI Provider_Adapter: Chat Completions via the async ``openai`` SDK (Req 10.2)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, ClassVar

import openai

from das.llm.adapters._common import (
    error_for_status,
    make_completion,
    make_tool_turn,
    parse_arguments,
    truncate,
)
from das.llm.base import ChatMessage, Completion, ModelParams
from das.llm.errors import LLMError, PermanentLLMError, TransientLLMError
from das.tools.types import AssistantTurn, ToolCall, ToolSpec, ToolTurn, Turn


def classify_openai_error(exc: BaseException) -> LLMError:
    """Map an ``openai`` SDK exception to a transient or permanent LLM error."""
    if isinstance(exc, openai.APIStatusError):
        return error_for_status("openai", exc.status_code, truncate(getattr(exc, "message", "") or str(exc)))
    if isinstance(exc, openai.APITimeoutError):
        return TransientLLMError("openai request timed out")
    if isinstance(exc, openai.APIConnectionError):
        return TransientLLMError(f"openai connection error: {truncate(str(exc))}")
    return PermanentLLMError(f"openai error {type(exc).__name__}: {truncate(str(exc))}")


class OpenAIAdapter:
    provider: ClassVar[str] = "openai"

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        *,
        client: Any = None,
    ) -> None:
        # SDK retries are disabled: LLMProvider owns the retry policy (Req 10.4).
        self._client = client if client is not None else openai.AsyncOpenAI(
            api_key=api_key, base_url=base_url, max_retries=0
        )

    @staticmethod
    def build_request(system: str, messages: Sequence[ChatMessage], params: ModelParams) -> dict[str, Any]:
        msgs: list[dict[str, str]] = []
        if system:
            msgs.append({"role": "system", "content": system})
        msgs.extend({"role": m.role, "content": m.content} for m in messages)
        return {
            "model": params.model,
            "messages": msgs,
            "temperature": params.temperature,
            "max_completion_tokens": params.max_output_tokens,
        }

    @staticmethod
    def parse_response(resp: Any) -> Completion:
        choices = getattr(resp, "choices", None) or []
        if not choices:
            raise PermanentLLMError("openai response contained no choices")
        content = getattr(choices[0].message, "content", None) or ""
        u = getattr(resp, "usage", None)
        return make_completion(
            content,
            getattr(u, "prompt_tokens", None),
            getattr(u, "completion_tokens", None),
            getattr(u, "total_tokens", None),
        )

    async def complete_once(
        self,
        system: str,
        messages: Sequence[ChatMessage],
        params: ModelParams,
        timeout: float,
    ) -> Completion:
        try:
            resp = await self._client.chat.completions.create(
                **self.build_request(system, messages, params), timeout=timeout
            )
        except openai.OpenAIError as exc:
            raise classify_openai_error(exc) from exc
        return self.parse_response(resp)

    @staticmethod
    def build_tool_request(
        system: str, turns: Sequence[Turn], tools: Sequence[ToolSpec], params: ModelParams
    ) -> dict[str, Any]:
        """A Chat Completions request with ``tools``, the assistant's tool calls and the tool results (Req 5.2)."""
        msgs: list[dict[str, Any]] = []
        if system:
            msgs.append({"role": "system", "content": system})
        for turn in turns:
            if isinstance(turn, ChatMessage):
                msgs.append({"role": turn.role, "content": turn.content})
            elif isinstance(turn, AssistantTurn):
                message: dict[str, Any] = {"role": "assistant", "content": turn.text or None}
                if turn.calls:
                    message["tool_calls"] = [
                        {"id": c.id, "type": "function",
                         "function": {"name": c.name, "arguments": json.dumps(c.arguments)}}
                        for c in turn.calls
                    ]
                msgs.append(message)
            else:
                msgs.extend({"role": "tool", "tool_call_id": r.call_id, "content": r.content} for r in turn.results)
        return {
            "model": params.model,
            "messages": msgs,
            "tools": [
                {"type": "function",
                 "function": {"name": t.name, "description": t.description, "parameters": t.parameters}}
                for t in tools
            ],
            "temperature": params.temperature,
            "max_completion_tokens": params.max_output_tokens,
        }

    @staticmethod
    def parse_tool_response(resp: Any) -> ToolTurn:
        choices = getattr(resp, "choices", None) or []
        if not choices:
            raise PermanentLLMError("openai response contained no choices")
        message = choices[0].message
        calls: list[ToolCall] = []
        for call in getattr(message, "tool_calls", None) or []:
            function = getattr(call, "function", None)
            if function is None:  # a kind of tool call this adapter never offers
                continue
            arguments, error = parse_arguments(getattr(function, "arguments", None))
            calls.append(ToolCall(str(call.id), str(function.name), arguments, error))
        u = getattr(resp, "usage", None)
        return make_tool_turn(
            getattr(message, "content", None) or "", calls,
            getattr(u, "prompt_tokens", None), getattr(u, "completion_tokens", None), getattr(u, "total_tokens", None),
        )

    async def complete_with_tools_once(
        self,
        system: str,
        turns: Sequence[Turn],
        tools: Sequence[ToolSpec],
        params: ModelParams,
        timeout: float,
    ) -> ToolTurn:
        try:
            resp = await self._client.chat.completions.create(
                **self.build_tool_request(system, turns, tools, params), timeout=timeout
            )
        except openai.OpenAIError as exc:
            raise classify_openai_error(exc) from exc
        return self.parse_tool_response(resp)

    async def ping(self) -> bool:
        try:
            await self._client.models.list(timeout=2.0)
        except Exception:
            return False
        return True

    async def aclose(self) -> None:
        await self._client.close()


__all__ = ["OpenAIAdapter", "classify_openai_error"]
