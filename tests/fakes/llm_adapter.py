"""Scripted Provider_Adapter for tests.

Injected directly in code; it has no entry in the provider factory, so it can
never be selected by Agent_Config (Requirement 10.7).
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import ClassVar, Union

from das.llm.base import ChatMessage, Completion, ModelParams
from das.models.results import TokenUsage


class Hang:
    """Outcome that blocks until cancelled (simulates a call exceeding its timeout)."""


HANG = Hang()

Responder = Callable[[str, Sequence[ChatMessage], ModelParams], Union[Completion, Awaitable[Completion]]]
Outcome = Union[Completion, str, BaseException, Hang, Responder]


@dataclass(frozen=True)
class RecordedCall:
    system: str
    messages: tuple[ChatMessage, ...]
    params: ModelParams
    timeout: float


def completion(content: str = "ok", input_tokens: int = 1, output_tokens: int = 1) -> Completion:
    return Completion(
        content=content,
        usage=TokenUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
        ),
    )


class FakeAdapter:
    """Returns scripted outcomes in order.

    Each outcome is a ``Completion``, a ``str`` (wrapped as a Completion), an
    exception instance (raised), ``HANG`` (block until cancelled), or a callable
    ``(system, messages, params) -> Completion`` (sync or async). When the script
    runs out, ``default`` is used; if ``default`` is None an AssertionError is raised.
    """

    provider: ClassVar[str] = "fake"

    def __init__(self, outcomes: Iterable[Outcome] = (), default: Outcome | None = None,
                 healthy: bool = True) -> None:
        self._outcomes = list(outcomes)
        self._default = default
        self.healthy = healthy
        self.calls: list[RecordedCall] = []

    async def complete_once(self, system: str, messages: Sequence[ChatMessage],
                            params: ModelParams, timeout: float) -> Completion:
        self.calls.append(RecordedCall(system, tuple(messages), params, timeout))
        if self._outcomes:
            outcome = self._outcomes.pop(0)
        elif self._default is not None:
            outcome = self._default
        else:
            raise AssertionError("FakeAdapter script exhausted")
        if isinstance(outcome, Hang):
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, str):
            return completion(outcome)
        if isinstance(outcome, Completion):
            return outcome
        result = outcome(system, messages, params)
        if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
            result = await result
        return result

    async def ping(self) -> bool:
        return self.healthy


__all__ = ["HANG", "FakeAdapter", "Hang", "RecordedCall", "completion"]
