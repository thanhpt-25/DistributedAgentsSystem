"""Signed Page_Cursor encoding and decoding (Req 19.8, 19.9).

Pure module: no I/O. The HMAC key is passed in by the caller (design D10).

Wire format::

    base64url(canonical_json(cursor)) "." base64url(hmac_sha256(key, canonical_json)[:16])

Both parts are unpadded base64url. ``decode_cursor`` checks, in order: total length
<= 512, alphabet ``[A-Za-z0-9_.-]``, MAC (``hmac.compare_digest``), ``v``, ``e`` and
``f``. Every failure raises :class:`InvalidCursor`.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from typing import Final, Literal

from pydantic import ValidationError

from das.models.base import DasModel

MAX_CURSOR_LENGTH = 512
MAC_LENGTH = 16
CURSOR_VERSION: Final = 1

_CURSOR_ALPHABET = re.compile(r"[A-Za-z0-9_.\-]+")

FilterValue = str | list[str] | int | None


class InvalidCursor(ValueError):
    """Raised for any Page_Cursor that was not produced by the Gateway for this call."""


class PageCursor(DasModel):
    """Position in a listing's sort order, bound to an endpoint and its filters."""

    v: Literal[1] = CURSOR_VERSION
    e: Literal["requests", "dlq"]
    f: dict[str, FilterValue]  # normalized filters of the call that produced the cursor
    s: int  # score (ms) of the last item on the page
    m: str  # Request_ID / Dead_Letter_ID of the last item on the page


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    padded = text + "=" * (-len(text) % 4)
    try:
        data = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InvalidCursor("cursor is not valid base64url") from exc
    # Reject non-canonical encodings (e.g. altered unused trailing bits) so that
    # changing any single character always invalidates the cursor.
    if _b64encode(data) != text:
        raise InvalidCursor("cursor is not canonical base64url")
    return data


def _mac(key: bytes, body: bytes) -> bytes:
    return hmac.new(key, body, hashlib.sha256).digest()[:MAC_LENGTH]


def encode_cursor(c: PageCursor, key: bytes) -> str:
    """Encode and sign ``c``. Raises ``ValueError`` if the result exceeds 512 characters."""
    body = _canonical_json(c.model_dump(mode="json"))
    token = f"{_b64encode(body)}.{_b64encode(_mac(key, body))}"
    if len(token) > MAX_CURSOR_LENGTH:
        raise ValueError("encoded cursor exceeds maximum length")
    return token


def decode_cursor(
    s: str, key: bytes, *, endpoint: str, filters: Mapping[str, object]
) -> PageCursor:
    """Verify and decode ``s`` for a call to ``endpoint`` with normalized ``filters``."""
    if not isinstance(s, str) or len(s) > MAX_CURSOR_LENGTH:
        raise InvalidCursor("cursor too long")
    if _CURSOR_ALPHABET.fullmatch(s) is None:
        raise InvalidCursor("cursor contains invalid characters")

    parts = s.split(".")
    if len(parts) != 2:
        raise InvalidCursor("cursor is malformed")
    body = _b64decode(parts[0])
    mac = _b64decode(parts[1])
    if not hmac.compare_digest(mac, _mac(key, body)):
        raise InvalidCursor("cursor signature mismatch")

    try:
        raw = json.loads(body)
    except (UnicodeDecodeError, ValueError) as exc:
        raise InvalidCursor("cursor body is not JSON") from exc
    if not isinstance(raw, dict):
        raise InvalidCursor("cursor body is not an object")
    if raw.get("v") != CURSOR_VERSION or type(raw.get("v")) is not int:
        raise InvalidCursor("unsupported cursor version")
    if raw.get("e") != endpoint:
        raise InvalidCursor("cursor was issued for another endpoint")

    try:
        cursor = PageCursor.model_validate(raw, strict=True)
    except ValidationError as exc:
        raise InvalidCursor("cursor body has invalid shape") from exc

    try:
        expected_filters = _canonical_json(dict(filters))
    except (TypeError, ValueError) as exc:
        raise InvalidCursor("filters are not comparable") from exc
    if _canonical_json(cursor.f) != expected_filters:
        raise InvalidCursor("cursor was issued for different filters")
    return cursor
