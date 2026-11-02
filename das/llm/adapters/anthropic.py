"""Anthropic Provider_Adapter: Messages API via the async ``anthropic`` SDK (Req 10.2)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

import anthropic

from das.llm.adapters._common import error_for_status, make_completion, split_system, truncate
from das.llm.base import ChatMessage, Completion, ModelParams
from das.llm.errors import LLMError, PermanentLLMError, TransientLLMError


def classify_anthropic_error(exc: BaseException) -> LLMError:
    """Map an ``anthropic`` SDK exception to a transient or permanent LLM error."""
    if isinstance(exc, anthropic.APIStatusError):
        return error_for_status(
            "anthropic", exc.status_code, truncate(getattr(exc, "message", "") or str(exc))
        )
    if isinstance(exc, anthropic.APITimeoutError):
        return TransientLLMError("anthropic request timed out")
    if isinstance(exc, anthropic.APIConnectionError):
        return TransientLLMError(f"anthropic connection error: {truncate(str(exc))}")
    return PermanentLLMError(f"anthropic error {type(exc).__name__}: {truncate(str(exc))}")


class AnthropicAdapter:
    provider: ClassVar[str] = "anthropic"

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        *,
        client: Any = None,
    ) -> None:
        # SDK retries are disabled: LLMProvider owns the retry policy (Req 10.4).
        self._client = client if client is not None else anthropic.AsyncAnthropic(
            api_key=api_key, base_url=base_url, max_retries=0
        )

    @staticmethod
    def build_request(system: str, messages: Sequence[ChatMessage], params: ModelParams) -> dict[str, Any]:
        sys_prompt, turns = split_system(system, messages)
        req: dict[str, Any] = {
            "model": params.model,
            "messages": [{"role": m.role, "content": m.content} for m in turns],
            "max_tokens": params.max_output_tokens,
            # This SDK version no longer types ``temperature`` on messages.create,
            # but the Messages API body still carries it.
            "extra_body": {"temperature": params.temperature},
        }
        if sys_prompt:
            req["system"] = sys_prompt
        return req

    @staticmethod
    def parse_response(resp: Any) -> Completion:
        blocks = getattr(resp, "content", None) or []
        text = "".join(
            getattr(b, "text", "") for b in blocks if getattr(b, "type", None) == "text"
        )
        u = getattr(resp, "usage", None)
        return make_completion(
            text, getattr(u, "input_tokens", None), getattr(u, "output_tokens", None)
        )

    async def complete_once(
        self,
        system: str,
        messages: Sequence[ChatMessage],
        params: ModelParams,
        timeout: float,
    ) -> Completion:
        req = self.build_request(system, messages, params)
        if not req["messages"]:
            raise PermanentLLMError("anthropic requires at least one user or assistant message")
        try:
            resp = await self._client.messages.create(**req, timeout=timeout)
        except anthropic.AnthropicError as exc:
            raise classify_anthropic_error(exc) from exc
        return self.parse_response(resp)

    async def ping(self) -> bool:
        try:
            await self._client.models.list(limit=1, timeout=2.0)
        except Exception:
            return False
        return True

    async def aclose(self) -> None:
        await self._client.close()


__all__ = ["AnthropicAdapter", "classify_anthropic_error"]
