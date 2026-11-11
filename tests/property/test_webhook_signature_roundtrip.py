# Feature: das-client-api, Property 8: Webhook signature round trip
"""Property 8: Webhook signature round trip.

For any secret, timestamp and body, ``verify(secret, ts, body, sign(secret, ts, body), now, tol)`` holds when
``|now - ts| <= tol``. It fails when ``|now - ts| > tol``, or when the body, timestamp, signature or secret is
changed. The Client_Package's ``verify_webhook`` agrees with ``das.client.webhook.verify`` on every generated
case, and the two copies of ``sign`` and ``verify`` (server and client) never disagree.

**Validates: Requirements 8.4, 9.7, 9.8**
"""
from __future__ import annotations

import math
from fractions import Fraction
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

import das.client.webhook as server
import das_client.webhook as client
from das_client import Event, InvalidSignature, verify_webhook

secrets = st.binary(min_size=1, max_size=64)
timestamps = st.integers(min_value=0, max_value=2**40)
bodies = st.binary(max_size=2048)
tolerances = st.integers(min_value=0, max_value=1000)
# Multiples of 1/8: sums and differences with a timestamp stay exact in a float, so the oracle can be exact too.
offsets = st.integers(min_value=-16_000, max_value=16_000).map(lambda n: n / 8)

IMPLEMENTATIONS = (server, client)


def outcome(fn: Any, *args: Any) -> tuple[str, Any]:
    """What a call did: its value, or the type of the error it raised (the copies must agree on both)."""
    try:
        return "value", fn(*args)
    except Exception as exc:
        return "raised", type(exc).__name__


# --------------------------------------------------------------------------- the two copies are one function


@given(secret=secrets, ts=st.integers(min_value=0, max_value=2**63), body=bodies)
def test_both_copies_sign_identically(secret: bytes, ts: int, body: bytes) -> None:
    assert server.sign(secret, ts, body) == client.sign(secret, ts, body)


@given(
    secret=secrets,
    ts=timestamps,
    body=bodies,
    now=st.floats(allow_nan=True, allow_infinity=True),
    tol=st.integers(min_value=-5, max_value=1000),
    case=st.sampled_from(["valid", "arbitrary"]),
    ts_text=st.text(alphabet=st.sampled_from("0123456789+-. \t٣²abc"), max_size=25),
    signature_text=st.text(max_size=80),
)
def test_both_copies_agree_on_any_header_text(
    secret: bytes, ts: int, body: bytes, now: float, tol: int, case: str, ts_text: str, signature_text: str
) -> None:
    """Hostile headers (signs, spaces, non-ASCII digits, NaN clocks, odd tolerances): same answer, or same error."""
    if case == "valid":
        ts_text, signature_text = str(ts), server.sign(secret, ts, body)
    args = (secret, ts_text, body, signature_text, now, tol)
    assert outcome(server.verify, *args) == outcome(client.verify, *args)


# --------------------------------------------------------------------------- the round trip


@given(secret=secrets, ts=timestamps, body=bodies, tol=tolerances, offset=offsets)
def test_a_signature_verifies_exactly_within_the_tolerance(
    secret: bytes, ts: int, body: bytes, tol: int, offset: float
) -> None:
    now = ts + offset
    signature = server.sign(secret, ts, body)
    within = abs(Fraction(now) - ts) <= tol  # an exact oracle, independent of the code under test
    for impl in IMPLEMENTATIONS:
        assert impl.verify(secret, str(ts), body, signature, now, tol) is within


@given(
    secret=secrets,
    ts=timestamps,
    body=bodies,
    tol=st.integers(min_value=1, max_value=1000),
    side=st.sampled_from([-1, 1]),
    edge=st.integers(min_value=-1, max_value=1),
)
def test_the_tolerance_boundary_is_inclusive(
    secret: bytes, ts: int, body: bytes, tol: int, side: int, edge: int
) -> None:
    """``|now - ts| == tol`` is accepted, one second beyond is not."""
    now = float(ts + side * (tol + edge))  # tol >= 1, so the distance is tol + edge >= 0
    signature = server.sign(secret, ts, body)
    for impl in IMPLEMENTATIONS:
        assert impl.verify(secret, str(ts), body, signature, now, tol) is (edge <= 0)


@given(secret=secrets, ts=timestamps, body=bodies, now=st.sampled_from([math.nan, math.inf, -math.inf]), tol=tolerances)
def test_a_clock_that_is_not_a_finite_number_never_verifies(
    secret: bytes, ts: int, body: bytes, now: float, tol: int
) -> None:
    """NaN must fail the window check, not skip it; infinity is never within any tolerance."""
    signature = server.sign(secret, ts, body)
    for impl in IMPLEMENTATIONS:
        assert not impl.verify(secret, str(ts), body, signature, now, tol)


# --------------------------------------------------------------------------- any change is rejected


@given(secret=secrets, ts=timestamps, body=bodies, tol=tolerances, offset=offsets, data=st.data())
def test_changing_the_body_or_the_secret_is_rejected(
    secret: bytes, ts: int, body: bytes, tol: int, offset: float, data: st.DataObject
) -> None:
    now = float(ts) + max(-tol, min(tol, offset))  # inside the window, so only the change can make it fail
    signature = server.sign(secret, ts, body)
    other_body = data.draw(bodies.filter(lambda b: b != body))
    # HMAC zero-pads a short key to the block size, so b"k" and b"k\x00" are the *same* key. A secret that differs
    # only by trailing NULs is not a different secret, so it is not generated.
    other_secret = data.draw(secrets.filter(lambda s: s.rstrip(b"\x00") != secret.rstrip(b"\x00")))
    for impl in IMPLEMENTATIONS:
        assert impl.verify(secret, str(ts), body, signature, now, tol)  # the unchanged input is accepted
        assert not impl.verify(secret, str(ts), other_body, signature, now, tol)
        assert not impl.verify(other_secret, str(ts), body, signature, now, tol)


@given(
    secret=secrets,
    ts=timestamps,
    body=bodies,
    kind=st.sampled_from(["replace", "append", "truncate", "upper"]),
    data=st.data(),
)
def test_changing_any_part_of_the_signature_is_rejected(
    secret: bytes, ts: int, body: bytes, kind: str, data: st.DataObject
) -> None:
    signature = server.sign(secret, ts, body)
    if kind == "replace":
        i = data.draw(st.integers(min_value=0, max_value=len(signature) - 1))
        replacement = data.draw(st.sampled_from("0123456789abcdefxyz!=").filter(lambda c: c != signature[i]))
        changed = signature[:i] + replacement + signature[i + 1 :]
    elif kind == "append":
        changed = signature + data.draw(st.text(min_size=1, max_size=3))
    elif kind == "truncate":
        changed = signature[: data.draw(st.integers(min_value=0, max_value=len(signature) - 1))]
    else:
        changed = signature.upper()
    assert changed != signature
    for impl in IMPLEMENTATIONS:
        assert not impl.verify(secret, str(ts), body, changed, float(ts), 300)


@given(secret=secrets, ts=timestamps, body=bodies, tol=st.integers(min_value=1, max_value=1000), data=st.data())
def test_changing_the_timestamp_is_rejected_even_inside_the_window(
    secret: bytes, ts: int, body: bytes, tol: int, data: st.DataObject
) -> None:
    """The signature covers the timestamp: a different one that is still in the window must fail on its own."""
    other = data.draw(st.integers(min_value=max(0, ts - tol), max_value=ts + tol).filter(lambda t: t != ts))
    signature = server.sign(secret, ts, body)
    for impl in IMPLEMENTATIONS:
        assert impl.verify(secret, str(ts), body, signature, float(ts), tol)
        assert not impl.verify(secret, str(other), body, signature, float(ts), tol)


# --------------------------------------------------------------------------- verify_webhook agrees with the server

event_ids = st.from_regex(r"[0-9]{1,13}-[0-9]{1,5}", fullmatch=True)
known_types = st.sampled_from(["request.completed", "request.failed", "request.cancelled", "job.transition"])
event_types = known_types | st.text(min_size=1, max_size=20)
json_values = st.none() | st.booleans() | st.integers() | st.text(max_size=20)
event_data = st.dictionaries(st.text(max_size=10), json_values, max_size=5)


@given(
    secret=secrets,
    ts=timestamps,
    tol=tolerances,
    offset=offsets,
    request_id=st.text(max_size=30),
    event_id=event_ids,
    event_type=event_types,
    payload=event_data,
    change=st.sampled_from(["none", "body", "timestamp", "signature", "secret"]),
)
def test_verify_webhook_accepts_exactly_what_the_server_verify_accepts(
    secret: bytes,
    ts: int,
    tol: int,
    offset: float,
    request_id: str,
    event_id: str,
    event_type: str,
    payload: dict[str, Any],
    change: str,
) -> None:
    body = server.webhook_body(request_id, event_id, event_type, payload)
    now = ts + offset
    signature = server.sign(secret, ts, body)
    sent_body, sent_secret, sent_ts, sent_signature = body, secret, ts, signature
    if change == "body":
        sent_body = body + b" "  # still valid JSON, so only the signature can reject it
    elif change == "timestamp":
        sent_ts = ts + 1
    elif change == "signature":
        sent_signature = signature[:-1] + ("0" if signature[-1] != "0" else "1")
    elif change == "secret":
        sent_secret = secret + b"!"

    accepted = server.verify(sent_secret, str(sent_ts), sent_body, sent_signature, now, tol)
    if change != "none":
        assert not accepted  # any change is rejected, whatever the clock says

    headers = {"DAS-Webhook-Timestamp": str(sent_ts), "DAS-Webhook-Signature": sent_signature}
    if accepted:
        event = verify_webhook(sent_body, headers, sent_secret, tolerance=tol, now=now)
        assert event == Event(event_type, event_id, payload, request_id)
    else:
        with pytest.raises(InvalidSignature):
            verify_webhook(sent_body, headers, sent_secret, tolerance=tol, now=now)
