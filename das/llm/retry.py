"""Retry wrapper around a Provider_Adapter (Requirements 10.1, 10.4, 10.5, 10.6).

``LLMProvider`` is the only LLM type agent code sees. It validates inputs,
enforces the per-call timeout, retries transient failures with capped
exponential backoff, and fails fast on permanent errors.

Phase 2 (Req 4, 6.6, 9.6): every call names its ``LLMOperation``; an optional
``LLMCallObserver`` hears about each scheduled retry and about the end of each
call that was not cancelled; each call runs inside an ``llm.completion`` Span
with one ``retry`` event per retry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace
from typing import Literal, Protocol, runtime_checkable

from das.clock import Clock, SystemClock
from das.llm.base import ROLES, ChatMessage, Completion, ModelParams, ProviderAdapter
from das.llm.errors import (
    AuthLLMError,
    LLMCallFailed,
    LLMInputError,
    PermanentLLMError,
    RateLimitedLLMError,
    TransientLLMError,
)
from das.models.results import TokenUsage
from das.models.usage import LLMOperation
from das.observability.logging import get_logger
from das.observability.tracing import NoopTracer, SpanHandle, Tracer

log = get_logger(__name__)

Sleep = Callable[[float], Awaitable[None]]

#: Outcome of one finished completion call (Req 4.1).
LLMOutcome = Literal["success", "rate_limited", "transient_error", "timeout", "non_transient_error"]


@runtime_checkable
class LLMCallObserver(Protocol):
    """Hears about LLM call progress; implemented by ``LLMMetricsObserver`` (Req 4).

    Phase 6: a call may run on another model than the provider's own (a Capability_Model, or the Fallback_Model).
    The provider then passes that model as the keyword ``model`` to ``on_retry_scheduled`` and ``on_finished``; it
    passes nothing when the provider's own model is used, so an observer written before Phase 6 keeps working. An
    observer may also implement ``on_fallback(operation, model)``, called with the model that failed (Req 2.6); the
    provider looks it up with ``getattr``, so it is optional and is not part of this protocol.
    """

    def on_retry_scheduled(self, operation: LLMOperation) -> None:
        """Called once per retry, right before the backoff sleep (Req 4.3)."""
        ...

    def on_finished(
        self,
        operation: LLMOperation,
        outcome: LLMOutcome,
        duration: float,
        usage: TokenUsage | None,
    ) -> None:
        """Called once per call that was not cancelled (Req 4.1, 4.2, 4.7).

        ``usage`` is set only for ``success`` with usage reported by the provider.
        """
        ...


def classify_outcome(exc: BaseException) -> LLMOutcome:
    """Map the error of the last attempt to its ``outcome`` label (Req 4.1)."""
    if isinstance(exc, RateLimitedLLMError):
        return "rate_limited"
    if isinstance(exc, TransientLLMError):
        return "transient_error"
    if isinstance(exc, TimeoutError):
        return "timeout"
    return "non_transient_error"

MIN_MESSAGES = 1
MAX_MESSAGES = 100
MIN_TEMPERATURE = 0.0
MAX_TEMPERATURE = 2.0
MIN_RETRY_COUNT = 0
MAX_RETRY_COUNT = 10
MIN_TIMEOUT = 1.0
MAX_TIMEOUT = 600.0
BACKOFF_CAP_SECONDS = 30.0


def retry_delay(k: int) -> float:
    """Wait before retry ``k`` (1-based): ``min(2^(k-1), 30)`` seconds."""
    if k < 1:
        raise ValueError("retry index k must be >= 1")
    # Avoid huge powers for large k; 2^5 = 32 already exceeds the cap.
    return BACKOFF_CAP_SECONDS if k > 6 else min(float(2 ** (k - 1)), BACKOFF_CAP_SECONDS)


def _validate_params(params: ModelParams) -> None:
    if not params.model:
        raise LLMInputError("model name must be non-empty")
    if not (MIN_TEMPERATURE <= params.temperature <= MAX_TEMPERATURE):
        raise LLMInputError(
            f"temperature {params.temperature} outside allowed range "
            f"{MIN_TEMPERATURE}-{MAX_TEMPERATURE}"
        )
    if params.max_output_tokens < 1:
        raise LLMInputError("max_output_tokens must be >= 1")


def _validate_messages(messages: Sequence[ChatMessage]) -> None:
    if not (MIN_MESSAGES <= len(messages) <= MAX_MESSAGES):
        raise LLMInputError(
            f"message count {len(messages)} outside allowed range {MIN_MESSAGES}-{MAX_MESSAGES}"
        )
    for i, m in enumerate(messages):
        if m.role not in ROLES:
            raise LLMInputError(f"messages[{i}].role {m.role!r} is not one of {sorted(ROLES)}")
        if not isinstance(m.content, str):
            raise LLMInputError(f"messages[{i}].content must be text")


class LLMProvider:
    """Validated, timed, retrying completion client bound to one adapter and model."""

    def __init__(
        self,
        adapter: ProviderAdapter,
        params: ModelParams,
        retry_count: int = 3,
        timeout: float = 60,
        sleep: Sleep = asyncio.sleep,
        clock: Clock | None = None,
        observer: LLMCallObserver | None = None,
        tracer: Tracer | None = None,
        fallback_model: str | None = None,
    ) -> None:
        if fallback_model is not None and not fallback_model.strip():
            raise LLMInputError("fallback_model must not be empty")
        if not (MIN_RETRY_COUNT <= retry_count <= MAX_RETRY_COUNT):
            raise LLMInputError(
                f"retry_count {retry_count} outside allowed range {MIN_RETRY_COUNT}-{MAX_RETRY_COUNT}"
            )
        if not (MIN_TIMEOUT <= timeout <= MAX_TIMEOUT):
            raise LLMInputError(
                f"timeout {timeout} outside allowed range {MIN_TIMEOUT:g}-{MAX_TIMEOUT:g} seconds"
            )
        _validate_params(params)
        self._adapter = adapter
        self._params = params
        self._retry_count = retry_count
        self._timeout = float(timeout)
        self._sleep = sleep
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._observer = observer
        self._tracer: Tracer = tracer if tracer is not None else NoopTracer()
        self._fallback_model = fallback_model.strip() if fallback_model is not None else None

    @property
    def provider(self) -> str:
        return self._adapter.provider

    @property
    def fallback_model(self) -> str | None:
        return self._fallback_model

    @property
    def params(self) -> ModelParams:
        return self._params

    @property
    def retry_count(self) -> int:
        return self._retry_count

    @property
    def timeout(self) -> float:
        return self._timeout

    async def ping(self) -> bool:
        return await self._adapter.ping()

    @property
    def observer(self) -> LLMCallObserver | None:
        return self._observer

    @observer.setter
    def observer(self, observer: LLMCallObserver | None) -> None:
        self._observer = observer

    @property
    def tracer(self) -> Tracer:
        return self._tracer

    @tracer.setter
    def tracer(self, tracer: Tracer | None) -> None:
        self._tracer = tracer if tracer is not None else NoopTracer()

    async def complete(
        self,
        system: str,
        messages: Sequence[ChatMessage],
        *,
        operation: LLMOperation,
        model: str | None = None,
    ) -> Completion:
        """Run one completion, retrying transient failures up to ``retry_count`` times.

        ``model`` runs this one call on another model than the provider's own (Phase 6 Req 2.2). After a
        permanent error that is not an authentication error, a configured Fallback_Model gets exactly one
        further call, when it differs from the model that failed (Req 2.3, 2.4). The Completion's ``model`` names
        the model that answered unless it is the provider's own (Req 2.5).

        Raises :class:`LLMInputError` for invalid arguments (no call is made or
        observed) and :class:`LLMCallFailed` on a permanent error or exhausted
        retries. ``asyncio.CancelledError`` propagates without ``on_finished``;
        retries already reported stay reported (Req 4.7).
        """
        _validate_messages(messages)
        msgs = tuple(messages)
        used = self._params if model is None else replace(self._params, model=model)
        try:
            return await self._attempt(system, msgs, used, operation)
        except LLMCallFailed as failure:
            fallback = self._fallback_model
            if fallback is None or not failure.permanent or failure.auth or fallback == used.model:
                raise
            self._notify_fallback(operation, used.model)
            return await self._attempt(system, msgs, replace(self._params, model=fallback), operation)

    async def _attempt(
        self, system: str, msgs: tuple[ChatMessage, ...], params: ModelParams, operation: LLMOperation
    ) -> Completion:
        """One model's turn: the call and its transient retries (the Phase 2 loop, bound to ``params``)."""
        attrs = {
            "llm.provider": self._adapter.provider,
            "llm.model": params.model,
            "llm.operation": operation,
        }
        with self._tracer.span("llm.completion", attributes=attrs) as span:
            start = self._clock.monotonic()
            attempts = 0
            while True:
                attempts += 1
                try:
                    async with asyncio.timeout(self._timeout):
                        completion = await self._adapter.complete_once(
                            system, msgs, params, self._timeout
                        )
                    if not isinstance(completion, Completion):
                        raise PermanentLLMError(
                            f"adapter returned {type(completion).__name__}, expected Completion"
                        )
                except PermanentLLMError as exc:
                    self._finish(operation, classify_outcome(exc), start, None, params)
                    raise self._failed(
                        str(exc) or type(exc).__name__, attempts, params,
                        permanent=True, auth=isinstance(exc, AuthLLMError),
                    ) from exc
                except TimeoutError as exc:
                    reason = f"timeout after {self._timeout:g}s"
                    cause: BaseException = exc
                except TransientLLMError as exc:
                    reason = str(exc) or type(exc).__name__
                    cause = exc
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Unexpected adapter failure: still one finished, non-retried call.
                    self._finish(operation, "non_transient_error", start, None, params)
                    raise
                else:
                    return self._succeeded(operation, completion, start, span, params)
                if attempts > self._retry_count:
                    self._finish(operation, classify_outcome(cause), start, None, params)
                    raise self._failed(reason, attempts, params) from cause
                span.add_event(
                    "retry", {"das.attempt": attempts, "error.type": type(cause).__name__}
                )
                self._notify_retry(operation, params)
                await self._sleep(retry_delay(attempts))

    def _succeeded(
        self, operation: LLMOperation, completion: Completion, start: float, span: SpanHandle, params: ModelParams
    ) -> Completion:
        if completion.usage_problems:
            log.warning(
                "llm_usage_invalid",
                operation=operation,
                provider=self._adapter.provider,
                model=params.model,
                fields=list(completion.usage_problems),
            )
        usage = completion.usage if completion.usage_reported else None
        if usage is not None:
            span.set_attributes(
                {"llm.input_tokens": usage.input_tokens, "llm.output_tokens": usage.output_tokens}
            )
        self._finish(operation, "success", start, usage, params)
        if params.model != self._params.model:  # Req 2.5: say which model answered, unless it is the usual one
            return replace(completion, model=params.model)
        return completion

    def _observer_model(self, params: ModelParams) -> dict[str, str]:
        """The ``model`` keyword for an observer, only when this call is not on the provider's own model."""
        return {} if params.model == self._params.model else {"model": params.model}

    def _notify_retry(self, operation: LLMOperation, params: ModelParams) -> None:
        if self._observer is None:
            return
        try:
            self._observer.on_retry_scheduled(operation, **self._observer_model(params))
        except Exception:  # observers must never break the call
            log.warning("llm_observer_failed", hook="on_retry_scheduled", exc_info=True)

    def _notify_fallback(self, operation: LLMOperation, failed_model: str) -> None:
        hook = getattr(self._observer, "on_fallback", None)
        if hook is None:
            return
        try:
            hook(operation, failed_model)
        except Exception:  # observers must never break the call
            log.warning("llm_observer_failed", hook="on_fallback", exc_info=True)

    def _finish(
        self,
        operation: LLMOperation,
        outcome: LLMOutcome,
        start: float,
        usage: TokenUsage | None,
        params: ModelParams,
    ) -> None:
        if self._observer is None:
            return
        duration = max(0.0, self._clock.monotonic() - start)
        try:
            self._observer.on_finished(operation, outcome, duration, usage, **self._observer_model(params))
        except Exception:  # observers must never break the call
            log.warning("llm_observer_failed", hook="on_finished", exc_info=True)

    def _failed(
        self, reason: str, attempts: int, params: ModelParams, *, permanent: bool = False, auth: bool = False
    ) -> LLMCallFailed:
        return LLMCallFailed(
            self._adapter.provider, params.model, reason, attempts, permanent=permanent, auth=auth
        )


__all__ = [
    "BACKOFF_CAP_SECONDS",
    "LLMCallObserver",
    "LLMOperation",
    "LLMOutcome",
    "LLMProvider",
    "Sleep",
    "classify_outcome",
    "retry_delay",
]
