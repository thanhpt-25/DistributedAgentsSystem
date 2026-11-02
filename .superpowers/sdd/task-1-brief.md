### Task 1: Token bucket (spec task 1.1)

**Files:**
- Create: `das/client/__init__.py`
- Create: `das/client/bucket.py`
- Test: `tests/unit/test_client_bucket.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `BucketState(tokens: float, updated: float)` (frozen dataclass)
  - `TakeResult(allowed: bool, state: BucketState, retry_after: float)` (frozen dataclass)
  - `take(state: BucketState | None, now: float, rate: float, burst: int) -> TakeResult`
  - `retry_after_header(seconds: float) -> int`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_client_bucket.py`:

```python
"""Token bucket (Phase 4 Req 2.1-2.3)."""
from __future__ import annotations

import pytest

from das.client.bucket import BucketState, TakeResult, retry_after_header, take


def test_first_call_starts_from_a_full_bucket() -> None:
    r = take(None, 100.0, rate=1.0, burst=2)
    assert r == TakeResult(True, BucketState(1.0, 100.0), 0.0)


def test_burst_is_used_up_then_denied_with_retry_after() -> None:
    r1 = take(None, 100.0, rate=1.0, burst=2)
    r2 = take(r1.state, 100.0, rate=1.0, burst=2)
    r3 = take(r2.state, 100.0, rate=1.0, burst=2)
    assert (r1.allowed, r2.allowed, r3.allowed) == (True, True, False)
    assert r3.state == BucketState(0.0, 100.0)
    assert r3.retry_after == pytest.approx(1.0)


def test_refill_over_time() -> None:
    empty = BucketState(0.0, 100.0)
    half = take(empty, 100.5, rate=1.0, burst=2)
    assert not half.allowed
    assert half.retry_after == pytest.approx(0.5)
    full_token = take(half.state, 101.0, rate=1.0, burst=2)
    assert full_token.allowed
    assert full_token.state.tokens == pytest.approx(0.0)


def test_refill_is_capped_at_burst() -> None:
    r = take(BucketState(0.0, 0.0), 100.0, rate=10.0, burst=2)
    assert r.allowed
    assert r.state == BucketState(1.0, 100.0)


def test_clock_stepping_back_refills_nothing() -> None:
    r = take(BucketState(0.0, 100.0), 50.0, rate=1.0, burst=2)
    assert not r.allowed
    assert r.state == BucketState(0.0, 100.0)


def test_rate_zero_disables_the_limit() -> None:
    state = None
    for _ in range(1000):
        r = take(state, 5.0, rate=0.0, burst=1)
        assert r.allowed
        assert r.retry_after == 0.0
        state = r.state


@pytest.mark.parametrize(("rate", "burst"), [(-0.1, 1), (1.0, 0), (1.0, -5)])
def test_invalid_arguments(rate: float, burst: int) -> None:
    with pytest.raises(ValueError):
        take(None, 0.0, rate=rate, burst=burst)


@pytest.mark.parametrize(("seconds", "header"), [(0.0, 1), (0.2, 1), (1.0, 1), (1.01, 2), (59.5, 60)])
def test_retry_after_header_rounds_up_to_at_least_one(seconds: float, header: int) -> None:
    assert retry_after_header(seconds) == header
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_bucket.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client'`

- [ ] **Step 3: Write minimal implementation**

Create `das/client/__init__.py`:

```python
"""Pure core for Phase 4 client API: limits, budgets, idempotency, events, webhooks.

Modules here are pure functions: no I/O, no clock reads, no randomness.
Time and random values are passed in by the I/O shell.
"""
```

Create `das/client/bucket.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_bucket.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_bucket.py && uv run mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Tick the spec task**

In `.kiro/specs/das-client-api/tasks.md` change `- [ ] 1.1 Implement the token bucket` to `- [x] 1.1 Implement the token bucket`.

---

