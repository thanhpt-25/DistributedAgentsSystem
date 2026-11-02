"""Webhook signing, verification and retry schedule (Phase 4 Req 8.4, 8.6, 9.7)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from das.client.webhook import next_due, sign, verify, webhook_body

FIXTURE: dict[str, Any] = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures" / "webhook_vectors.json").read_text()
)
VECTORS: list[dict[str, Any]] = FIXTURE["vectors"]
REJECT: list[dict[str, Any]] = FIXTURE["reject"]
ACCEPT: list[dict[str, Any]] = FIXTURE["accept"]
SECRET = b"whsec_test_0123456789"
TS = 1793000000
BODY = b'{"data":{"status":"Completed"},"event_id":"1793000000000-0","request_id":"r1","type":"request.completed"}'


@pytest.mark.parametrize("v", VECTORS)
def test_sign_matches_vectors(v: dict[str, Any]) -> None:
    assert sign(v["secret"].encode(), v["timestamp"], v["body"].encode()) == v["signature"]


@pytest.mark.parametrize("v", VECTORS)
def test_verify_accepts_vectors(v: dict[str, Any]) -> None:
    ts = v["timestamp"]
    assert verify(v["secret"].encode(), str(ts), v["body"].encode(), v["signature"], now=ts, tolerance=300)


def test_webhook_body_is_canonical_json() -> None:
    assert webhook_body("r1", "1793000000000-0", "request.completed", {"status": "Completed"}) == BODY


def test_verify_rejects_changes() -> None:
    good = sign(SECRET, TS, BODY)
    assert verify(SECRET, str(TS), BODY, good, now=TS, tolerance=300)
    assert not verify(SECRET, str(TS), BODY + b" ", good, now=TS, tolerance=300)
    assert not verify(SECRET, str(TS + 1), BODY, good, now=TS, tolerance=300)
    assert not verify(b"wrong", str(TS), BODY, good, now=TS, tolerance=300)
    assert not verify(SECRET, str(TS), BODY, good.removeprefix("v1="), now=TS, tolerance=300)
    flipped = good[:-1] + ("1" if good[-1] == "0" else "0")
    assert not verify(SECRET, str(TS), BODY, flipped, now=TS, tolerance=300)


def test_verify_tolerance_window() -> None:
    good = sign(SECRET, TS, BODY)
    assert verify(SECRET, str(TS), BODY, good, now=TS + 300, tolerance=300)
    assert verify(SECRET, str(TS), BODY, good, now=TS - 300, tolerance=300)
    assert not verify(SECRET, str(TS), BODY, good, now=TS + 301, tolerance=300)
    assert not verify(SECRET, str(TS), BODY, good, now=TS - 301, tolerance=300)


@pytest.mark.parametrize(
    "ts", ["", "12a", "+1793000000", " 1793000000", "1793000000 ", "1.5", "١٢٣", "9" * 20, "9" * 400, "1" * 5000],
)
def test_verify_rejects_malformed_timestamps(ts: str) -> None:
    # Float ``now``, as production passes: an int would keep the comparison in int arithmetic.
    assert not verify(SECRET, ts, BODY, sign(SECRET, TS, BODY), now=float(TS), tolerance=300)


@pytest.mark.parametrize("v", REJECT, ids=lambda v: v["reason"])
def test_verify_rejects_vectors(v: dict[str, Any]) -> None:
    assert not verify(
        v["secret"].encode(), v["timestamp"], v["body"].encode(), v["signature"],
        now=v["now"], tolerance=v["tolerance"],
    )


@pytest.mark.parametrize("v", ACCEPT, ids=lambda v: v["reason"])
def test_verify_accepts_header_vectors(v: dict[str, Any]) -> None:
    assert verify(
        v["secret"].encode(), v["timestamp"], v["body"].encode(), v["signature"],
        now=v["now"], tolerance=v["tolerance"],
    )


def test_verify_rejects_nan_now() -> None:
    assert not verify(SECRET, str(TS), BODY, sign(SECRET, TS, BODY), now=float("nan"), tolerance=300)


def test_verify_handles_non_ascii_signature() -> None:
    assert not verify(SECRET, str(TS), BODY, "v1=é", now=TS, tolerance=300)


@pytest.mark.parametrize(("rand", "delay"), [(0.0, 24.0), (0.5, 30.0), (1.0, 36.0)])
def test_next_due_jitter(rand: float, delay: float) -> None:
    assert next_due(1000.0, 1, 30.0, 3600.0, rand) == pytest.approx(1000.0 + delay)


def test_next_due_is_capped_by_max_backoff() -> None:
    assert next_due(1000.0, 10, 30.0, 3600.0, 1.0) == pytest.approx(1000.0 + 3600.0 * 1.2)


@pytest.mark.parametrize("rand", [-0.1, 1.1])
def test_next_due_rejects_rand_out_of_range(rand: float) -> None:
    with pytest.raises(ValueError):
        next_due(1000.0, 1, 30.0, 3600.0, rand)
