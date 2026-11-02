"""OpenAI Provider_Adapter: Chat Completions via the async ``openai`` SDK (Req 10.2)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

import openai

from das.llm.adapters._common import error_for_status, make_completion, truncate
from das.llm.base import ChatMessage, Completion, ModelParams
from das.llm.errors import LLMError, PermanentLLMError, TransientLLMError


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

    async def ping(self) -> bool:
        try:
            await self._client.models.list(timeout=2.0)
        except Exception:
            return False
        return True

    async def aclose(self) -> None:
        await self._client.close()


__all__ = ["OpenAIAdapter", "classify_openai_error"]
