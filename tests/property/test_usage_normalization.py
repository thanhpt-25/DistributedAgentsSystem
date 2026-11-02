# Feature: das-observability-operations, Property 11: Chuẩn hóa token usage
"""Property 11: token usage normalization.

For any triple of arbitrary values (negative or positive int, float, bool,
string, ``None``) for input, output and total, ``normalize_usage`` returns a
``TokenUsage`` whose values are all int >= 0:

- every valid value (non-bool int >= 0) is kept, every invalid value becomes 0;
- when total is absent, total = normalized input + normalized output;
- ``problems`` lists exactly the invalid / missing fields (a missing total is
  derived, not a problem).

**Validates: Requirements 9.6, 9.7**
"""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.ops.usage import normalize_usage

valid_counts = st.integers(min_value=0, max_value=10**12)
raw_values = st.one_of(
    st.none(),
    valid_counts,
    st.integers(max_value=-1),
    st.floats(allow_nan=True, allow_infinity=True),
    st.booleans(),
    st.text(max_size=8),
    st.just("12"),
)


def _is_valid(value: object) -> bool:
    return type(value) is int and value >= 0


@settings(max_examples=300)
@given(i=raw_values, o=raw_values, t=raw_values)
def test_normalize_usage(i: object, o: object, t: object) -> None:
    result = normalize_usage(i, o, t)
    usage = result.usage

    for value in (usage.input_tokens, usage.output_tokens, usage.total_tokens):
        assert type(value) is int and value >= 0

    expected_i = i if _is_valid(i) else 0
    expected_o = o if _is_valid(o) else 0
    assert usage.input_tokens == expected_i
    assert usage.output_tokens == expected_o

    if t is None:
        assert usage.total_tokens == expected_i + expected_o
    else:
        assert usage.total_tokens == (t if _is_valid(t) else 0)

    expected_problems = [
        name
        for name, raw in (("input_tokens", i), ("output_tokens", o))
        if not _is_valid(raw)
    ]
    if t is not None and not _is_valid(t):
        expected_problems.append("total_tokens")
    assert list(result.problems) == expected_problems

    assert result.reported == (not (i is None and o is None and t is None))


@settings(max_examples=200)
@given(i=valid_counts, o=valid_counts)
def test_missing_total_is_sum_without_problems(i: int, o: int) -> None:
    result = normalize_usage(i, o, None)
    assert result.usage.total_tokens == i + o
    assert result.problems == ()
    assert result.reported
