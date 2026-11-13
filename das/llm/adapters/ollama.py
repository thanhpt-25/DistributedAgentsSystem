"""Ollama Provider_Adapter: ``POST /api/chat`` via ``httpx`` with ``stream=false`` (Req 10.2)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

import httpx

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


def classify_ollama_error(exc: BaseException) -> LLMError:
    """Map an ``httpx`` exception to a transient or permanent LLM error."""
    if isinstance(exc, httpx.HTTPStatusError):
        detail = ""
        try:
            body = exc.response.json()
            detail = str(body.get("error", "")) if isinstance(body, dict) else ""
        except Exception:
            detail = exc.response.text if exc.response.is_closed else ""
        return error_for_status("ollama", exc.response.status_code, truncate(detail))
    if isinstance(exc, httpx.TimeoutException):
        return TransientLLMError("ollama request timed out")
    if isinstance(exc, httpx.TransportError):
        return TransientLLMError(f"ollama connection error: {type(exc).__name__}: {truncate(str(exc))}")
    return PermanentLLMError(f"ollama error {type(exc).__name__}: {truncate(str(exc))}")


class OllamaAdapter:
    provider: ClassVar[str] = "ollama"

    def __init__(self, base_url: str, *, client: httpx.AsyncClient | None = None) -> None:
        self._client = client if client is not None else httpx.AsyncClient(base_url=base_url.rstrip("/"))

    @staticmethod
    def build_request(system: str, messages: Sequence[ChatMessage], params: ModelParams) -> dict[str, Any]:
        msgs: list[dict[str, str]] = []
        if system:
            msgs.append({"role": "system", "content": system})
        msgs.extend({"role": m.role, "content": m.content} for m in messages)
        return {
            "model": params.model,
            "messages": msgs,
            "stream": False,
            "options": {
                "temperature": params.temperature,
                "num_predict": params.max_output_tokens,
            },
        }

    @staticmethod
    def parse_response(body: Any) -> Completion:
        if not isinstance(body, dict) or not isinstance(body.get("message"), dict):
            raise PermanentLLMError("ollama response is missing 'message'")
        if body.get("error"):
            raise PermanentLLMError(f"ollama error: {truncate(str(body['error']))}")
        return make_completion(
            str(body["message"].get("content") or ""),
            body.get("prompt_eval_count"),
            body.get("eval_count"),
        )

    async def complete_once(
        self,
        system: str,
        messages: Sequence[ChatMessage],
        params: ModelParams,
        timeout: float,
    ) -> Completion:
        try:
            resp = await self._client.post(
                "/api/chat", json=self.build_request(system, messages, params), timeout=timeout
            )
            resp.raise_for_status()
            body = resp.json()
        except httpx.HTTPError as exc:
            raise classify_ollama_error(exc) from exc
        except ValueError as exc:  # invalid JSON body
            raise PermanentLLMError(f"ollama returned invalid JSON: {truncate(str(exc))}") from exc
        return self.parse_response(body)

    @staticmethod
    def build_tool_request(
        system: str, turns: Sequence[Turn], tools: Sequence[ToolSpec], params: ModelParams
    ) -> dict[str, Any]:
        """A chat request with ``tools``, the assistant's ``tool_calls`` and ``tool`` messages for results (Req 5.2)."""
        msgs: list[dict[str, Any]] = []
        if system:
            msgs.append({"role": "system", "content": system})
        for turn in turns:
            if isinstance(turn, ChatMessage):
                msgs.append({"role": turn.role, "content": turn.content})
            elif isinstance(turn, AssistantTurn):
                message: dict[str, Any] = {"role": "assistant", "content": turn.text}
                if turn.calls:
                    message["tool_calls"] = [
                        {"function": {"name": c.name, "arguments": c.arguments}} for c in turn.calls
                    ]
                msgs.append(message)
            else:
                for r in turn.results:
                    reply: dict[str, Any] = {"role": "tool", "content": r.content}
                    if r.name:
                        reply["tool_name"] = r.name
                    msgs.append(reply)
        return {
            "model": params.model,
            "messages": msgs,
            "tools": [
                {"type": "function",
                 "function": {"name": t.name, "description": t.description, "parameters": t.parameters}}
                for t in tools
            ],
            "stream": False,
            "options": {"temperature": params.temperature, "num_predict": params.max_output_tokens},
        }

    @staticmethod
    def parse_tool_response(body: Any) -> ToolTurn:
        if not isinstance(body, dict) or not isinstance(body.get("message"), dict):
            raise PermanentLLMError("ollama response is missing 'message'")
        if body.get("error"):
            raise PermanentLLMError(f"ollama error: {truncate(str(body['error']))}")
        message = body["message"]
        calls: list[ToolCall] = []
        for number, raw in enumerate(message.get("tool_calls") or [], start=1):
            function = raw.get("function") if isinstance(raw, dict) else None
            if not isinstance(function, dict):
                continue
            arguments, error = parse_arguments(function.get("arguments"))
            calls.append(ToolCall(f"call-{number}", str(function.get("name", "")), arguments, error))  # no id is sent
        return make_tool_turn(
            str(message.get("content") or ""), calls, body.get("prompt_eval_count"), body.get("eval_count")
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
            resp = await self._client.post(
                "/api/chat", json=self.build_tool_request(system, turns, tools, params), timeout=timeout
            )
            resp.raise_for_status()
            body = resp.json()
        except httpx.HTTPError as exc:
            raise classify_ollama_error(exc) from exc
        except ValueError as exc:  # invalid JSON body
            raise PermanentLLMError(f"ollama returned invalid JSON: {truncate(str(exc))}") from exc
        return self.parse_tool_response(body)

    async def ping(self) -> bool:
        try:
            resp = await self._client.get("/api/tags", timeout=2.0)
        except Exception:
            return False
        return resp.status_code == 200

    async def aclose(self) -> None:
        await self._client.aclose()


__all__ = ["OllamaAdapter", "classify_ollama_error"]
