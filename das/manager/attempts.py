"""Per-attempt hook shared by the Planner and the Summarizer (Phase 2 Req 6.3, 6.10).

The driver passes ``on_attempt``: called with the 1-based attempt number, it returns a
context manager that wraps that attempt (the driver opens a ``manager.plan`` or
``manager.summarize`` Span). The yielded :class:`AttemptObserver` lets the caller
report a failed attempt that did not raise (``observer.fail(code)``). Planner and
Summarizer stay free of any tracing import.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Protocol


class AttemptObserver(Protocol):
    def fail(self, error_type: str) -> None: ...


AttemptHook = Callable[[int], AbstractContextManager[AttemptObserver]]


class _NoopObserver:
    def fail(self, error_type: str) -> None:
        return None


_NOOP = _NoopObserver()


@contextmanager
def no_attempt_hook(_attempt: int) -> Iterator[AttemptObserver]:
    yield _NOOP


__all__ = ["AttemptHook", "AttemptObserver", "no_attempt_hook"]
