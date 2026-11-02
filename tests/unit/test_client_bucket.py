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
