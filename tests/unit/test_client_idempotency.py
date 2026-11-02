"""Idempotency_Key validation and Body_Hash (Phase 4 Req 4)."""
from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from das.client.budget import Budget
from das.client.idempotency import body_hash, idem_key_hash, validate_idempotency_key


@pytest.mark.parametrize("value", ["a", "abc-123_XYZ", "!" + "~" * 127, "a" * 128])
def test_valid_keys(value: str) -> None:
    assert validate_idempotency_key(value)


@pytest.mark.parametrize("value", ["", "a" * 129, "a b", "é", "a\n", "\n", "\x7f", "tab\tkey"])
def test_invalid_keys(value: str) -> None:
    assert not validate_idempotency_key(value)


def test_body_hash_known_value() -> None:
    expected = hashlib.sha256(b'{"budget":null,"prompt":"hi"}').hexdigest()
    assert body_hash("hi", None) == expected


def test_body_hash_ignores_decimal_spelling_and_empty_budget() -> None:
    assert body_hash("p", Budget(max_cost=Decimal("1.50"))) == body_hash("p", Budget(max_cost=Decimal("1.5")))
    assert body_hash("p", Budget()) == body_hash("p", None)


def test_body_hash_changes_with_prompt_and_budget() -> None:
    base = body_hash("p", Budget(max_total_tokens=10))
    assert body_hash("q", Budget(max_total_tokens=10)) != base
    assert body_hash("p", Budget(max_total_tokens=11)) != base
    assert body_hash("p", None) != base


def test_body_hash_accepts_lone_surrogates() -> None:
    # JSON "\ud800" decodes to a lone surrogate; hashing must not raise.
    assert len(body_hash("\ud800", None)) == 64


def test_idem_key_hash() -> None:
    assert idem_key_hash("k") == hashlib.sha256(b"k").hexdigest()
