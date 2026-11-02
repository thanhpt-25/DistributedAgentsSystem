"""Token bucket for per-key rate limits (Phase 4 Req 2.1-2.3). Pure.

``rate_check.lua`` implements the same arithmetic with Redis ``TIME``;
Property 1 checks that the two agree.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BucketState:
    """Tokens left and the Redis time (seconds) they were computed at."""

    tokens: float
    updated: float


@dataclass(frozen=True, slots=True)
class TakeResult:
    allowed: bool
    state: BucketState
    #: Seconds until one token is available; 0.0 when ``allowed``.
    retry_after: float


def take(state: BucketState | None, now: float, rate: float, burst: int) -> TakeResult:
    """Refill the bucket up to ``burst`` at ``rate`` tokens per second, then take one token.

    ``state is None`` is a full bucket. ``rate == 0`` disables the limit (Req 1.2).
    A ``now`` earlier than ``state.updated`` (clock stepped back) refills nothing.
    """
    if rate < 0:
        raise ValueError(f"rate must be >= 0, got {rate}")
    if burst < 1:
        raise ValueError(f"burst must be >= 1, got {burst}")
    if state is None:
        tokens, updated = float(burst), now
    else:
        elapsed = max(0.0, now - state.updated)
        tokens = min(float(burst), state.tokens + rate * elapsed)
        updated = max(now, state.updated)
    if rate == 0:
        return TakeResult(True, BucketState(tokens, updated), 0.0)
    if tokens >= 1.0:
        return TakeResult(True, BucketState(tokens - 1.0, updated), 0.0)
    return TakeResult(False, BucketState(tokens, updated), (1.0 - tokens) / rate)


def retry_after_header(seconds: float) -> int:
    """``Retry-After`` value: whole seconds rounded up, at least 1 (Req 2.2)."""
    return max(1, math.ceil(seconds))
