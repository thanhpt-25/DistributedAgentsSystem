"""AWS Bedrock Provider_Adapter: Converse API via ``aioboto3`` ``bedrock-runtime`` (Req 10.2).

Credentials come from the static key pair in Agent_Config when set, otherwise
from the default AWS credential chain.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from typing import Any, ClassVar

import aioboto3
import botocore.exceptions as bexc
from botocore.config import Config as BotoConfig

from das.llm.adapters._common import (
    AUTH_STATUSES,
    is_transient_status,
    make_completion,
    make_tool_turn,
    split_system,
    split_system_turns,
    truncate,
)
from das.llm.base import ChatMessage, Completion, ModelParams
from das.llm.errors import AuthLLMError, LLMError, PermanentLLMError, RateLimitedLLMError, TransientLLMError
from das.tools.types import AssistantTurn, ToolCall, ToolSpec, ToolTurn, Turn

#: Error codes that mean Bedrock is throttling the caller.
RATE_LIMIT_CODES: frozenset[str] = frozenset({"ThrottlingException", "TooManyRequestsException"})

#: Error codes for bad, expired or unauthorized credentials (Phase 6 Req 2.4).
AUTH_CODES: frozenset[str] = frozenset(
    {
        "AccessDeniedException",
        "UnrecognizedClientException",
        "ExpiredTokenException",
        "InvalidSignatureException",
        "InvalidClientTokenId",
        "UnauthorizedOperation",
    }
)

TRANSIENT_CODES: frozenset[str] = frozenset(
    {
        "ThrottlingException",
        "TooManyRequestsException",
        "ServiceUnavailableException",
        "InternalServerException",
        "ModelNotReadyException",
        "ModelTimeoutException",
        "RequestTimeout",
        "RequestTimeoutException",
    }
)


def classify_bedrock_error(exc: BaseException) -> LLMError:
    """Map a botocore exception to a transient or permanent LLM error."""
    if isinstance(exc, bexc.ClientError):
        err = exc.response.get("Error", {}) or {}
        code = str(err.get("Code", "") or "")
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        reason = f"bedrock {code or 'ClientError'}"
        if status:
            reason += f" (HTTP {status})"
        msg = err.get("Message")
        if msg:
            reason += f": {truncate(str(msg))}"
        if code in AUTH_CODES or status in AUTH_STATUSES:
            return AuthLLMError(reason)
        if code in RATE_LIMIT_CODES or status == 429:
            return RateLimitedLLMError(reason)
        if code in TRANSIENT_CODES or (status and is_transient_status(status)):
            return TransientLLMError(reason)
        return PermanentLLMError(reason)
    if isinstance(exc, (bexc.ConnectTimeoutError, bexc.ReadTimeoutError)):
        return TransientLLMError("bedrock request timed out")
    if isinstance(exc, (bexc.HTTPClientError, bexc.ConnectionError)):
        return TransientLLMError(f"bedrock connection error: {type(exc).__name__}")
    if isinstance(exc, (bexc.NoCredentialsError, bexc.PartialCredentialsError)):
        return AuthLLMError(f"bedrock credentials error: {type(exc).__name__}")
    return PermanentLLMError(f"bedrock error {type(exc).__name__}: {truncate(str(exc))}")


class BedrockAdapter:
    provider: ClassVar[str] = "bedrock"

    def __init__(
        self,
        region: str | None = None,
        *,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        read_timeout: float = 60,
        client: Any = None,
    ) -> None:
        self._session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region,
        )
        # SDK retries are disabled: LLMProvider owns the retry policy (Req 10.4).
        self._boto_config = BotoConfig(
            read_timeout=read_timeout,
            connect_timeout=min(10.0, read_timeout),
            retries={"total_max_attempts": 1, "mode": "standard"},
        )
        self._client = client
        self._stack = contextlib.AsyncExitStack()
        self._lock = asyncio.Lock()

    async def _runtime(self) -> Any:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    # types-aioboto3 overloads only accept AioConfig; aiobotocore also
                    # accepts a plain botocore Config at runtime.
                    self._client = await self._stack.enter_async_context(
                        self._session.client("bedrock-runtime", config=self._boto_config)  # type: ignore[call-overload]
                    )
        return self._client

    @staticmethod
    def build_request(system: str, messages: Sequence[ChatMessage], params: ModelParams) -> dict[str, Any]:
        sys_prompt, turns = split_system(system, messages)
        req: dict[str, Any] = {
            "modelId": params.model,
            "messages": [{"role": m.role, "content": [{"text": m.content}]} for m in turns],
            "inferenceConfig": {
                "maxTokens": params.max_output_tokens,
                "temperature": params.temperature,
            },
        }
        if sys_prompt:
            req["system"] = [{"text": sys_prompt}]
        return req

    @staticmethod
    def parse_response(resp: Any) -> Completion:
        try:
            blocks = resp["output"]["message"]["content"]
        except (KeyError, TypeError) as exc:
            raise PermanentLLMError("bedrock response is missing output.message.content") from exc
        text = "".join(b["text"] for b in blocks if isinstance(b, dict) and "text" in b)
        u = resp.get("usage") or {}
        return make_completion(
            text, u.get("inputTokens"), u.get("outputTokens"), u.get("totalTokens")
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
            raise PermanentLLMError("bedrock requires at least one user or assistant message")
        try:
            client = await self._runtime()
            resp = await client.converse(**req)
        except bexc.BotoCoreError as exc:
            raise classify_bedrock_error(exc) from exc
        except bexc.ClientError as exc:
            raise classify_bedrock_error(exc) from exc
        return self.parse_response(resp)

    @staticmethod
    def build_tool_request(
        system: str, turns: Sequence[Turn], tools: Sequence[ToolSpec], params: ModelParams
    ) -> dict[str, Any]:
        """A Converse request with ``toolConfig``, ``toolUse`` blocks for calls and ``toolResult`` blocks (Req 5.2)."""
        sys_prompt, rest = split_system_turns(system, turns)
        messages: list[dict[str, Any]] = []
        for turn in rest:
            if isinstance(turn, ChatMessage):
                messages.append({"role": turn.role, "content": [{"text": turn.content}]})
            elif isinstance(turn, AssistantTurn):
                blocks: list[dict[str, Any]] = [{"text": turn.text}] if turn.text else []
                blocks += [
                    {"toolUse": {"toolUseId": c.id, "name": c.name, "input": c.arguments}} for c in turn.calls
                ]
                messages.append({"role": "assistant", "content": blocks})
            else:
                messages.append({
                    "role": "user",
                    "content": [
                        {"toolResult": {"toolUseId": r.call_id, "content": [{"text": r.content}],
                                        "status": "error" if r.is_error else "success"}}
                        for r in turn.results
                    ],
                })
        req: dict[str, Any] = {
            "modelId": params.model,
            "messages": messages,
            "toolConfig": {"tools": [
                {"toolSpec": {"name": t.name, "description": t.description, "inputSchema": {"json": t.parameters}}}
                for t in tools
            ]},
            "inferenceConfig": {"maxTokens": params.max_output_tokens, "temperature": params.temperature},
        }
        if sys_prompt:
            req["system"] = [{"text": sys_prompt}]
        return req

    @staticmethod
    def parse_tool_response(resp: Any) -> ToolTurn:
        try:
            blocks = resp["output"]["message"]["content"]
        except (KeyError, TypeError) as exc:
            raise PermanentLLMError("bedrock response is missing output.message.content") from exc
        text = "".join(b["text"] for b in blocks if isinstance(b, dict) and "text" in b)
        calls = [
            ToolCall(str(b["toolUse"]["toolUseId"]), str(b["toolUse"]["name"]), dict(b["toolUse"].get("input") or {}))
            for b in blocks if isinstance(b, dict) and isinstance(b.get("toolUse"), dict)
        ]
        u = resp.get("usage") or {}
        return make_tool_turn(text, calls, u.get("inputTokens"), u.get("outputTokens"), u.get("totalTokens"))

    async def complete_with_tools_once(
        self,
        system: str,
        turns: Sequence[Turn],
        tools: Sequence[ToolSpec],
        params: ModelParams,
        timeout: float,
    ) -> ToolTurn:
        req = self.build_tool_request(system, turns, tools, params)
        if not req["messages"]:
            raise PermanentLLMError("bedrock requires at least one user or assistant message")
        try:
            client = await self._runtime()
            resp = await client.converse(**req)
        except bexc.BotoCoreError as exc:
            raise classify_bedrock_error(exc) from exc
        except bexc.ClientError as exc:
            raise classify_bedrock_error(exc) from exc
        return self.parse_tool_response(resp)

    async def ping(self) -> bool:
        """Connected when credentials resolve and the Bedrock control plane answers.

        ``AccessDeniedException`` still proves reachability and valid credentials.
        """
        try:
            # Stub gap: types-aioboto3 declares get_credentials() sync, but at runtime
            # boto3 delegates to AioSession.get_credentials, which is a coroutine.
            if await self._session.get_credentials() is None:  # type: ignore[misc]
                return False
            cfg = BotoConfig(connect_timeout=2, read_timeout=2, retries={"total_max_attempts": 1})
            # Same stub gap as in _runtime: botocore Config is valid at runtime.
            async with self._session.client("bedrock", config=cfg) as ctl:  # type: ignore[call-overload]
                await ctl.list_foundation_models(byOutputModality="TEXT")
        except bexc.ClientError as exc:
            return exc.response.get("Error", {}).get("Code") == "AccessDeniedException"
        except Exception:
            return False
        return True

    async def aclose(self) -> None:
        await self._stack.aclose()


__all__ = ["BedrockAdapter", "classify_bedrock_error"]
