# Feature: distributed-agent-system, Property 7: Prompt validation
"""Property 7: Prompt validation.

*For any* string ``s`` and configured maximum ``n`` in 1..1,000,000,
``validate_prompt(s, n)`` returns ``empty_prompt`` if ``s.strip() == ""``;
otherwise ``prompt_too_long`` carrying ``n`` if ``len(s) > n``; otherwise no error.

**Validates: Requirements 1.4**
"""

from __future__ import annotations

import sys

from hypothesis import given, note
from hypothesis import strategies as st

from das.gateway.validation import PromptError, validate_prompt

MAX_LEN_LIMIT = 1_000_000

# Every code point Python treats as whitespace (Zs, Zl, Zp, and the bidi
# whitespace controls such as \t, \n, \x1c-\x1f, \x85).
_WHITESPACE = "".join(chr(c) for c in range(sys.maxunicode + 1) if chr(c).isspace())

_whitespace_char = st.sampled_from(_WHITESPACE)
_any_char = st.characters(codec="utf-8") | _whitespace_char
_non_whitespace_char = st.characters(codec="utf-8").filter(lambda c: not c.isspace())

max_lengths = st.integers(min_value=1, max_value=MAX_LEN_LIMIT)


def _expected(s: str, n: int) -> PromptError | None:
    """Oracle written independently of ``str.strip``: whitespace-only iff every char is space."""
    if all(c.isspace() for c in s):
        return PromptError("empty_prompt")
    if len(s) > n:
        return PromptError("prompt_too_long", max_length=n)
    return None


@st.composite
def _blank_prompt(draw: st.DrawFn) -> str:
    """Empty or whitespace-only, possibly far longer than any small limit."""
    unit = draw(st.text(alphabet=_whitespace_char, max_size=8))
    return unit * draw(st.integers(min_value=0, max_value=64))


@st.composite
def _boundary_case(draw: st.DrawFn) -> tuple[str, int]:
    """A non-blank prompt whose length sits at ``n - 1``, ``n`` or ``n + 1``."""
    n = draw(st.integers(min_value=1, max_value=200) | max_lengths)
    length = draw(st.sampled_from([n - 1, n, n + 1]).filter(lambda k: k >= 1))
    # Mix whitespace in freely, but guarantee at least one non-whitespace char.
    anchor = draw(_non_whitespace_char)
    unit = draw(st.text(alphabet=_any_char, min_size=1, max_size=16))
    padding = (unit * (length // len(unit) + 1))[: length - 1]
    position = draw(st.integers(min_value=0, max_value=len(padding)))
    return padding[:position] + anchor + padding[position:], n


_prompts_and_limits = (
    st.tuples(st.text(alphabet=_any_char, max_size=300), st.integers(min_value=1, max_value=300))
    | st.tuples(_blank_prompt(), max_lengths | st.integers(min_value=1, max_value=4))
    | _boundary_case()
)


@given(case=_prompts_and_limits)
def test_validate_prompt_matches_specification(case: tuple[str, int]) -> None:
    s, n = case
    note(f"len={len(s)} n={n} blank={s.strip() == ''}")

    result = validate_prompt(s, n)

    assert result == _expected(s, n)
    if s.strip() == "":
        # Emptiness is checked before length, so blank prompts never report a length error.
        assert result == PromptError("empty_prompt")
        assert result.to_body() == {"error": "empty_prompt"}
    elif len(s) > n:
        assert result is not None
        assert result.to_body() == {"error": "prompt_too_long", "max_length": n}
    else:
        assert result is None


@given(ws=_whitespace_char, n=max_lengths)
def test_every_whitespace_character_alone_is_empty(ws: str, n: int) -> None:
    assert validate_prompt(ws, n) == PromptError("empty_prompt")
