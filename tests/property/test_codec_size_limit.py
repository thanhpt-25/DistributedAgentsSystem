# Feature: distributed-agent-system, Property 6: Message size limit
"""Property 6: Message size limit.

*For any* Message and any limit ``L``, ``encode`` returns bytes if and only if
the canonical serialization is at most ``L`` bytes, and otherwise raises
``MessageTooLarge`` without returning bytes. *For any* byte string longer than
``L``, ``decode`` raises a size-limit ``ParseError``.

**Validates: Requirements 9.6**
"""

from __future__ import annotations

import pytest
from hypothesis import given, note
from hypothesis import strategies as st

from das.codec.message_codec import (
    ROOT_FIELD,
    MessageCodec,
    MessageTooLarge,
    ParseError,
    TypedMessage,
)
from tests.strategies import messages

# Reference codec with the default 1 MiB limit; generated Messages fit well within it.
_REFERENCE = MessageCodec()

_SIZE_REASON = "exceeds the limit"


@st.composite
def _message_and_limit(draw: st.DrawFn) -> tuple[TypedMessage, bytes, int]:
    message = draw(messages())
    canonical = _REFERENCE.encode(message)
    size = len(canonical)
    # Concentrate limits around the boundary, plus arbitrary limits on either side.
    limit = draw(
        st.sampled_from([max(1, size - 1), size, size + 1])
        | st.integers(min_value=1, max_value=max(1, size - 1))
        | st.integers(min_value=size, max_value=size * 4)
    )
    return message, canonical, limit


@given(case=_message_and_limit())
def test_encode_respects_limit(case: tuple[TypedMessage, bytes, int]) -> None:
    message, canonical, limit = case
    note(f"size={len(canonical)} limit={limit}")
    codec = MessageCodec(max_bytes=limit)

    if len(canonical) <= limit:
        assert codec.encode(message) == canonical
    else:
        result: object = None
        with pytest.raises(MessageTooLarge) as excinfo:
            result = codec.encode(message)
        assert result is None
        assert excinfo.value.size == len(canonical)
        assert excinfo.value.limit == limit


@st.composite
def _oversized_input(draw: st.DrawFn) -> tuple[bytes, int]:
    """A byte string longer than the limit: random bytes or a valid encoding."""
    if draw(st.booleans()):
        data = _REFERENCE.encode(draw(messages()))
    else:
        data = draw(st.binary(min_size=2, max_size=2048))
    limit = draw(st.integers(min_value=1, max_value=len(data) - 1))
    return data, limit


@given(case=_oversized_input())
def test_decode_rejects_oversized_input(case: tuple[bytes, int]) -> None:
    data, limit = case
    note(f"len={len(data)} limit={limit}")
    codec = MessageCodec(max_bytes=limit)

    result: object = None
    with pytest.raises(ParseError) as excinfo:
        result = codec.decode(data)
    assert result is None
    # Size is checked first, so even a valid encoding fails on size alone.
    assert _SIZE_REASON in excinfo.value.reason
    assert excinfo.value.field == ROOT_FIELD
