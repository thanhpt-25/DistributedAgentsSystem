"""Retry backoff schedule (Requirement 6.2). Pure."""
from __future__ import annotations

# Above this exponent 2^(N-1) dwarfs any permitted max_backoff; skip the float math.
_MAX_EXPONENT = 1000


def backoff(attempt: int, base: float, max_backoff: float) -> float:
    """Delay before re-dispatching a Job whose attempt ``attempt`` (N) failed.

    ``backoff(N) = min(base * 2^(N-1), max_backoff)``. ``attempt`` is 1-based.
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    if base <= 0 or max_backoff <= 0:
        raise ValueError("base and max_backoff must be positive")
    exponent = attempt - 1
    if exponent > _MAX_EXPONENT:
        return max_backoff
    return min(base * 2.0**exponent, max_backoff)
