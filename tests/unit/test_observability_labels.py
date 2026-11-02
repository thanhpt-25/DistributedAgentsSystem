"""Unit tests for das.observability.labels (Requirements 5.2, 5.3, 5.7)."""

from __future__ import annotations

from das.observability.labels import (
    OVERFLOW_VALUE,
    BoundResult,
    LabelBounder,
    LabelPolicy,
)


def _bounder(limit: int = 2) -> LabelBounder:
    policy = LabelPolicy(
        enumerated={"method": frozenset({"GET", "POST", "_other"})},
        dynamic_limits={"capability": limit, "model": limit},
    )
    return LabelBounder(policy)


def test_enumerated_value_inside_set_is_kept() -> None:
    result = _bounder().bound("das_http_requests_total", {"method": "GET"})
    assert result == BoundResult(labels={"method": "GET"}, overflowed=())


def test_enumerated_value_outside_set_becomes_other() -> None:
    result = _bounder().bound("das_http_requests_total", {"method": "TRACE"})
    assert result.labels == {"method": OVERFLOW_VALUE}
    assert result.overflowed == ("method",)


def test_dynamic_accepts_new_values_until_limit_then_overflows() -> None:
    b = _bounder(limit=2)
    m = "das_jobs_total"
    assert b.bound(m, {"capability": "a"}).labels == {"capability": "a"}
    assert b.bound(m, {"capability": "b"}).labels == {"capability": "b"}
    r = b.bound(m, {"capability": "c"})
    assert r.labels == {"capability": OVERFLOW_VALUE}
    assert r.overflowed == ("capability",)
    # Overflowed value is not recorded.
    assert b.seen[(m, "capability")] == {"a", "b"}


def test_dynamic_seen_values_keep_their_label_after_overflow() -> None:
    b = _bounder(limit=1)
    m = "das_jobs_total"
    b.bound(m, {"capability": "a"})
    b.bound(m, {"capability": "b"})
    r = b.bound(m, {"capability": "a"})
    assert r == BoundResult(labels={"capability": "a"}, overflowed=())


def test_dynamic_limits_are_per_metric_and_per_label() -> None:
    b = _bounder(limit=1)
    b.bound("m1", {"capability": "a"})
    assert b.bound("m2", {"capability": "b"}).labels == {"capability": "b"}
    assert b.bound("m1", {"model": "x"}).labels == {"model": "x"}
    assert b.bound("m1", {"capability": "b"}).labels == {"capability": OVERFLOW_VALUE}


def test_seen_is_never_reset_across_many_calls() -> None:
    b = _bounder(limit=2)
    for i in range(100):
        b.bound("m", {"capability": f"c{i}"})
    assert b.seen[("m", "capability")] == {"c0", "c1"}


def test_unknown_labels_pass_through_and_multiple_overflows_reported() -> None:
    b = _bounder(limit=1)
    b.bound("m", {"capability": "a"})
    r = b.bound("m", {"method": "BREW", "capability": "z", "route": "/v1/x"})
    assert r.labels == {
        "method": OVERFLOW_VALUE,
        "capability": OVERFLOW_VALUE,
        "route": "/v1/x",
    }
    assert set(r.overflowed) == {"method", "capability"}


def test_bound_does_not_mutate_input() -> None:
    labels = {"method": "BREW"}
    _bounder().bound("m", labels)
    assert labels == {"method": "BREW"}
