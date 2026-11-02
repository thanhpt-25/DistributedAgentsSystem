"""Unit tests for das.ops.keys (Requirements 11.2-11.7)."""

from __future__ import annotations

import hashlib
import hmac

import pytest

from das.ops import keys as keys_mod
from das.ops.keys import KeyClass, classify_key, key_fingerprint

API = [b"api-key-0000000000000001", b"api-key-0000000000000002"]
ADMIN = [b"admin-key-00000000000001", b"admin-key-00000000000002"]


@pytest.mark.parametrize(
    ("candidate", "expected"),
    [
        ("api-key-0000000000000001", (KeyClass.API, 0)),
        ("api-key-0000000000000002", (KeyClass.API, 1)),
        ("admin-key-00000000000001", (KeyClass.ADMIN, 0)),
        ("admin-key-00000000000002", (KeyClass.ADMIN, 1)),
        ("unknown-key-000000000000", (KeyClass.NONE, None)),
        ("", (KeyClass.NONE, None)),
        (None, (KeyClass.NONE, None)),
    ],
)
def test_classify_key_table(candidate: str | None, expected: tuple[KeyClass, int | None]) -> None:
    assert classify_key(candidate, API, ADMIN) == expected


def test_shared_prefix_and_case_do_not_match() -> None:
    assert classify_key("api-key-000000000000000", API, ADMIN) == (KeyClass.NONE, None)
    assert classify_key("api-key-00000000000000011", API, ADMIN) == (KeyClass.NONE, None)
    assert classify_key("API-KEY-0000000000000001", API, ADMIN) == (KeyClass.NONE, None)


def test_empty_admin_list_still_classifies_api() -> None:
    assert classify_key("api-key-0000000000000001", API, []) == (KeyClass.API, 0)
    assert classify_key("admin-key-00000000000001", API, []) == (KeyClass.NONE, None)
    assert classify_key("x", [], []) == (KeyClass.NONE, None)


def test_empty_candidate_never_matches_empty_configured_key() -> None:
    assert classify_key("", [b""], [b""]) == (KeyClass.NONE, None)


def test_admin_wins_when_present_in_both_lists() -> None:
    assert classify_key("dup-key-0000000000000000", [b"dup-key-0000000000000000"],
                        [b"x" * 16, b"dup-key-0000000000000000"]) == (KeyClass.ADMIN, 1)


@pytest.mark.parametrize(
    "candidate",
    ["api-key-0000000000000001", "admin-key-00000000000002", "nope", "", None],
)
def test_compare_digest_called_for_every_key(
    monkeypatch: pytest.MonkeyPatch, candidate: str | None
) -> None:
    calls: list[tuple[bytes, bytes]] = []
    real = hmac.compare_digest

    def spy(a: bytes, b: bytes) -> bool:
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(keys_mod.hmac, "compare_digest", spy)
    classify_key(candidate, API, ADMIN)
    assert len(calls) == len(API) + len(ADMIN)
    assert [b for _, b in calls] == [*API, *ADMIN]


def test_key_fingerprint() -> None:
    key = b"admin-key-00000000000001"
    fp = key_fingerprint(key)
    assert fp == hashlib.sha256(key).hexdigest()[:8]
    assert len(fp) == 8
    assert all(c in "0123456789abcdef" for c in fp)
    assert key_fingerprint(b"other-key-0000000000001") != fp
