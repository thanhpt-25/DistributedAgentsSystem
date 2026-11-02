"""Unit tests for das.ops.cursor (Req 19.8, 19.9)."""

from __future__ import annotations

import base64
import json
import re

import pytest

from das.ops.cursor import (
    MAX_CURSOR_LENGTH,
    InvalidCursor,
    PageCursor,
    _mac,
    decode_cursor,
    encode_cursor,
)

KEY = b"k" * 32
REQ_FILTERS: dict[str, object] = {
    "status": ["Completed", "Failed"],
    "created_after": 1_700_000_000_000,
    "created_before": None,
}
DLQ_FILTERS: dict[str, object] = {
    "status": "Pending",
    "reason": "malformed",
    "request_id": None,
    "capability": "summarize",
}


def _req_cursor() -> PageCursor:
    return PageCursor(e="requests", f=REQ_FILTERS, s=1_700_000_123_456, m="req-01HXYZ")


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _forge(body: dict[str, object], key: bytes = KEY) -> str:
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return f"{_b64(raw)}.{_b64(_mac(key, raw))}"


def test_round_trip_requests() -> None:
    c = _req_cursor()
    s = encode_cursor(c, KEY)
    assert len(s) <= MAX_CURSOR_LENGTH
    assert re.fullmatch(r"[A-Za-z0-9_.\-]+", s)
    decoded = decode_cursor(s, KEY, endpoint="requests", filters=REQ_FILTERS)
    assert decoded == c
    assert encode_cursor(decoded, KEY) == s


def test_round_trip_dlq_with_unicode_member() -> None:
    c = PageCursor(e="dlq", f=DLQ_FILTERS, s=0, m="dl-é😀")
    s = encode_cursor(c, KEY)
    assert decode_cursor(s, KEY, endpoint="dlq", filters=DLQ_FILTERS) == c


def test_encoding_is_deterministic_regardless_of_filter_order() -> None:
    reordered = dict(reversed(list(REQ_FILTERS.items())))
    a = encode_cursor(_req_cursor(), KEY)
    b = encode_cursor(PageCursor(e="requests", f=reordered, s=1_700_000_123_456, m="req-01HXYZ"), KEY)
    assert a == b
    assert decode_cursor(a, KEY, endpoint="requests", filters=reordered) == _req_cursor()


def test_rejects_too_long() -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor("a" * (MAX_CURSOR_LENGTH + 1), KEY, endpoint="requests", filters={})


@pytest.mark.parametrize("bad", ["", "abc def", "abc=.def", "a+b.c", "a/b.c"])
def test_rejects_bad_characters(bad: str) -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(bad, KEY, endpoint="requests", filters={})


@pytest.mark.parametrize("bad", ["nodot", "a.b.c", ".", "abc."])
def test_rejects_malformed_structure(bad: str) -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(bad, KEY, endpoint="requests", filters={})


def test_rejects_every_single_character_modification() -> None:
    s = encode_cursor(_req_cursor(), KEY)
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_."
    for i, ch in enumerate(s):
        for repl in alphabet:
            if repl == ch:
                continue
            tampered = s[:i] + repl + s[i + 1 :]
            with pytest.raises(InvalidCursor):
                decode_cursor(tampered, KEY, endpoint="requests", filters=REQ_FILTERS)


def test_rejects_wrong_key() -> None:
    s = encode_cursor(_req_cursor(), KEY)
    with pytest.raises(InvalidCursor):
        decode_cursor(s, b"other-key", endpoint="requests", filters=REQ_FILTERS)


def test_rejects_other_endpoint() -> None:
    s = encode_cursor(_req_cursor(), KEY)
    with pytest.raises(InvalidCursor):
        decode_cursor(s, KEY, endpoint="dlq", filters=REQ_FILTERS)


@pytest.mark.parametrize(
    "filters",
    [
        {**REQ_FILTERS, "status": ["Completed"]},
        {**REQ_FILTERS, "created_before": 1_800_000_000_000},
        {k: v for k, v in REQ_FILTERS.items() if k != "created_before"},
        {**REQ_FILTERS, "extra": None},
    ],
)
def test_rejects_other_filters(filters: dict[str, object]) -> None:
    s = encode_cursor(_req_cursor(), KEY)
    with pytest.raises(InvalidCursor):
        decode_cursor(s, KEY, endpoint="requests", filters=filters)


def test_rejects_bool_in_place_of_int_filter() -> None:
    c = PageCursor(e="requests", f={"created_after": 1}, s=1, m="r")
    s = encode_cursor(c, KEY)
    with pytest.raises(InvalidCursor):
        decode_cursor(s, KEY, endpoint="requests", filters={"created_after": True})


def test_rejects_signed_but_unsupported_version() -> None:
    s = _forge({"v": 2, "e": "requests", "f": {}, "s": 1, "m": "r"})
    with pytest.raises(InvalidCursor):
        decode_cursor(s, KEY, endpoint="requests", filters={})


@pytest.mark.parametrize(
    "body",
    [
        {"v": 1, "e": "requests", "f": {}, "s": "1", "m": "r"},
        {"v": 1, "e": "requests", "f": {}, "m": "r"},
        {"v": 1, "e": "requests", "f": {}, "s": 1, "m": "r", "x": 0},
        {"v": True, "e": "requests", "f": {}, "s": 1, "m": "r"},
    ],
)
def test_rejects_signed_but_invalid_shape(body: dict[str, object]) -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(_forge(body), KEY, endpoint="requests", filters={})


def test_rejects_signed_non_object_body() -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(_forge([1, 2]), KEY, endpoint="requests", filters={})  # type: ignore[arg-type]


def test_encode_rejects_oversized_cursor() -> None:
    with pytest.raises(ValueError):
        encode_cursor(PageCursor(e="dlq", f={}, s=0, m="x" * 600), KEY)
