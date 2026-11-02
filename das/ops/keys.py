"""Classification of Admin_API and Gateway API keys (Requirement 11).

Pure module (Req 19.5): no I/O, no logging. Key values never leave this module
except through :func:`key_fingerprint`, which exposes only 8 hex characters of
the SHA-256 digest.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Sequence
from enum import Enum

__all__ = ["KeyClass", "classify_key", "key_fingerprint"]


class KeyClass(Enum):
    """Result of classifying a presented ``X-API-Key`` value."""

    NONE = 0
    API = 1
    ADMIN = 2


def _scan(presented: bytes, keys: Sequence[bytes]) -> tuple[bool, int]:
    """Compare ``presented`` against every key, without early exit.

    Returns the OR of all comparisons and the index of the (last) matching key,
    or ``-1`` when none matched. The index is selected arithmetically so every
    iteration performs the same work regardless of where the match is.
    """
    matched = False
    index = -1
    for i, key in enumerate(keys):
        hit = hmac.compare_digest(presented, key)
        matched |= hit
        # Branch-free select: hit is 0/1, so this keeps ``index`` or takes ``i``.
        index = index + (i - index) * int(hit)
    return matched, index


def classify_key(
    candidate: str | None,
    api: Sequence[bytes],
    admin: Sequence[bytes],
) -> tuple[KeyClass, int | None]:
    """Classify ``candidate`` against configured API_Keys and Admin_API_Keys.

    ``hmac.compare_digest`` is called exactly ``len(api) + len(admin)`` times,
    with no early exit, and results are combined with OR, so timing does not
    depend on matching prefixes or on the position of the matching key
    (Req 11.7). A missing or empty candidate never matches (Req 11.3).

    Returns ``(KeyClass, index)`` where ``index`` is the position of the matched
    key in ``admin`` (for ``ADMIN``) or ``api`` (for ``API``), and ``None`` for
    ``NONE``. ``ADMIN`` wins if a value is somehow present in both lists.
    """
    presented = candidate.encode("utf-8") if candidate else b""
    api_hit, api_index = _scan(presented, api)
    admin_hit, admin_index = _scan(presented, admin)

    if not presented:
        return KeyClass.NONE, None
    if admin_hit:
        return KeyClass.ADMIN, admin_index
    if api_hit:
        return KeyClass.API, api_index
    return KeyClass.NONE, None


def key_fingerprint(key: bytes) -> str:
    """Key_Fingerprint: first 8 hex characters of ``sha256(key)``."""
    return hashlib.sha256(key).hexdigest()[:8]
