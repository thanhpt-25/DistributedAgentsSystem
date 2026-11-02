# Feature: das-observability-operations, Property 28: Round-trip của Page_Cursor
"""Property 28: Round-trip của Page_Cursor.

For any valid ``PageCursor`` (endpoint ``requests`` or ``dlq``, any valid
normalized filters of that endpoint, any score and member) and any HMAC key:

- ``decode(encode(c)) == c`` and ``encode(decode(s)) == s``.
- ``len(s) <= 512`` and ``s`` only contains ``[A-Za-z0-9_.-]``.

Filters are generated within the Admin_API length limits (``status`` 1-5
distinct values, ``request_id`` 1-64 chars, ``capability`` per
``CAPABILITY_PATTERN``), which keeps every cursor well under 512 characters.

**Validates: Requirements 19.8**
"""

from __future__ import annotations

import re

from hypothesis import given
from hypothesis import strategies as st

from das.config.schema import CAPABILITY_PATTERN
from das.models.dlq import DEAD_LETTER_ID_PATTERN, DEAD_LETTER_REASONS, RECORD_STATUSES
from das.models.request import RequestStatus
from das.ops.cursor import MAX_CURSOR_LENGTH, PageCursor, decode_cursor, encode_cursor

_CURSOR_RE = re.compile(r"\A[A-Za-z0-9_.\-]+\Z")

_INT64 = st.integers(min_value=-(2**63), max_value=2**63 - 1)
_EPOCH_MS = st.integers(min_value=0, max_value=253_402_300_799_999)  # up to year 9999
_ASCII_ID = st.text(
    alphabet=st.characters(min_codepoint=0x21, max_codepoint=0x7E), min_size=1, max_size=64
)
_keys = st.binary(min_size=0, max_size=64)


@st.composite
def _request_filters(draw: st.DrawFn) -> dict[str, object]:
    statuses = draw(
        st.none()
        | st.lists(st.sampled_from([s.value for s in RequestStatus]), min_size=1, max_size=5, unique=True)
    )
    after = draw(st.none() | _EPOCH_MS)
    before = draw(st.none() | _EPOCH_MS)
    if after is not None and before is not None and after >= before:
        after, before = min(after, before), max(after, before) + 1
    return {"status": statuses, "created_after": after, "created_before": before}


_dlq_filters = st.fixed_dictionaries(
    {
        "status": st.none() | st.sampled_from(RECORD_STATUSES),
        "reason": st.none() | st.sampled_from(DEAD_LETTER_REASONS),
        "request_id": st.none() | _ASCII_ID,
        "capability": st.none() | st.from_regex(CAPABILITY_PATTERN, fullmatch=True),
    }
)

_request_cursors = st.builds(
    PageCursor,
    e=st.just("requests"),
    f=_request_filters(),
    s=_INT64,
    m=_ASCII_ID,
)
_dlq_cursors = st.builds(
    PageCursor,
    e=st.just("dlq"),
    f=_dlq_filters,
    s=_INT64,
    m=st.from_regex(DEAD_LETTER_ID_PATTERN, fullmatch=True),
)
page_cursors = _request_cursors | _dlq_cursors


@given(cursor=page_cursors, key=_keys)
def test_page_cursor_round_trip(cursor: PageCursor, key: bytes) -> None:
    s = encode_cursor(cursor, key)

    assert len(s) <= MAX_CURSOR_LENGTH
    assert _CURSOR_RE.fullmatch(s) is not None

    decoded = decode_cursor(s, key, endpoint=cursor.e, filters=cursor.f)
    assert decoded == cursor
    assert encode_cursor(decoded, key) == s
