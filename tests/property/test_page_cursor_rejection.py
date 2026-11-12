# Feature: das-observability-operations, Property 29: Từ chối Page_Cursor không hợp lệ
"""Property 29: invalid Page_Cursors are rejected.

For any valid cursor ``s`` (any endpoint, filters, position and HMAC key):

- every string obtained by changing one character of ``s``, every random string,
  every string longer than 512 characters, a cursor signed with another key, a cursor
  issued for the other endpoint and a cursor issued with filters other than the
  call's are rejected by ``decode_cursor`` with ``InvalidCursor``;
- ``GET /v1/admin/dlq`` and ``GET /v1/requests`` (the real ``create_app`` on
  fakeredis, with data that a valid cursor would page through) answer HTTP 400
  ``{"error": "invalid_query", "parameter": "cursor"}`` with no item.

The unmodified cursor is accepted (positive control), so each rejection is caused
by the modification alone.

HMAC treats keys that differ only by trailing NUL bytes as the same key (keys are
zero-padded to the block size), so "another key" is generated to differ after
stripping trailing NULs; the Gateway's keys are UTF-8 secrets or hex and never end
in NUL.

**Validates: Requirements 12.9, 15.8, 19.9**
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from das.gateway.admin import parse_dlq_query
from das.gateway.app import create_app
from das.gateway.listing import parse_list_query
from das.models.dlq import DEAD_LETTER_REASONS, RECORD_STATUSES, DeadLetterFields
from das.models.request import RequestStatus
from das.ops.cursor import (
    MAX_CURSOR_LENGTH,
    InvalidCursor,
    PageCursor,
    decode_cursor,
    encode_cursor,
)
from das.registry import AgentRegistry
from das.state import StateStore
from tests.fixtures.redis_time import FakeRedisTime
from tests.property.test_page_cursor_roundtrip import (
    _dlq_filters,
    _request_filters,
    page_cursors,
)

CURSOR_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-"
OTHER = {"requests": "dlq", "dlq": "requests"}

# Characters a single-character edit may introduce: the cursor alphabet plus
# characters outside it (padding, standard base64, whitespace, non-ASCII).
_replacement = st.sampled_from(CURSOR_ALPHABET) | st.sampled_from("=+/ ~%&#?\x00é😀")
_any_text = st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=600)
_alphabet_text = st.text(alphabet=CURSOR_ALPHABET, max_size=600)
_random_strings = _any_text | _alphabet_text
_too_long_extra = st.text(alphabet=CURSOR_ALPHABET, min_size=1, max_size=200)
_keys = st.binary(min_size=0, max_size=64)


def _canon(f: dict[str, Any]) -> str:
    return json.dumps(f, sort_keys=True, separators=(",", ":"))


def _filters_for(endpoint: str) -> st.SearchStrategy[dict[str, Any]]:
    return _request_filters() if endpoint == "requests" else _dlq_filters


@st.composite
def _one_char_edit(draw: st.DrawFn, token: str) -> str:
    i = draw(st.integers(0, len(token) - 1))
    c = draw(_replacement.filter(lambda ch: ch != token[i]))
    return token[:i] + c + token[i + 1 :]


@st.composite
def _too_long(draw: st.DrawFn, token: str) -> str:
    """``token`` extended past 512 characters, or a random alphabet string > 512."""
    if draw(st.booleans()):
        pad = draw(_too_long_extra)
        out = token + pad * (MAX_CURSOR_LENGTH // len(pad) + 1)
    else:
        out = draw(st.text(alphabet=CURSOR_ALPHABET, min_size=513, max_size=1200))
    assert len(out) > MAX_CURSOR_LENGTH
    return out


def _other_key(key: bytes) -> st.SearchStrategy[bytes]:
    return _keys.filter(lambda k: k.rstrip(b"\0") != key.rstrip(b"\0"))


def _rejects(s: str, key: bytes, endpoint: str, filters: dict[str, Any]) -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(s, key, endpoint=endpoint, filters=filters)


# --------------------------------------------------------------------------- pure


@given(cursor=page_cursors, key=_keys, data=st.data())
def test_decode_rejects_every_invalid_cursor(
    cursor: PageCursor, key: bytes, data: st.DataObject
) -> None:
    e, f = cursor.e, cursor.f
    s = encode_cursor(cursor, key)
    assert decode_cursor(s, key, endpoint=e, filters=f) == cursor  # positive control

    # Any single-character edit.
    _rejects(data.draw(_one_char_edit(s), label="edited"), key, e, f)

    # Random strings (a forged valid MAC has negligible probability).
    rnd = data.draw(_random_strings, label="random")
    assume(rnd != s)
    _rejects(rnd, key, e, f)

    # Longer than 512 characters.
    _rejects(data.draw(_too_long(s), label="too_long"), key, e, f)

    # Signed with another key.
    other_key = data.draw(_other_key(key), label="other_key")
    _rejects(encode_cursor(cursor, other_key), key, e, f)

    # Issued for the other endpoint, with exactly the filters of the call.
    _rejects(s, key, OTHER[e], f)
    _rejects(encode_cursor(cursor.model_copy(update={"e": OTHER[e]}), key), key, e, f)

    # Issued with filters other than the call's.
    call_filters = data.draw(_filters_for(e), label="call_filters")
    assume(_canon(call_filters) != _canon(f))
    _rejects(s, key, e, call_filters)


# --------------------------------------------------------------------------- endpoints

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
SECRET = "cursor-secret-for-property-29"
SECRET_KEY = SECRET.encode("utf-8")
T0 = 1_700_000_000_000
HEADERS = {"requests": {"X-API-Key": API_KEY}, "dlq": {"X-API-Key": ADMIN_KEY}}
PATHS = {"requests": "/v1/requests", "dlq": "/v1/admin/dlq"}
# Resume position after every seeded item in (score desc, member desc) order.
RESUME = (T0 + 10_000, "~" * 32)

_request_params = st.fixed_dictionaries(
    {},
    optional={
        "status": st.lists(
            st.sampled_from([s.value for s in RequestStatus]), min_size=1, max_size=5, unique=True
        ).map(",".join),
        "created_after": st.just("2023-11-14T22:13:19Z"),
        "created_before": st.just("2100-01-01T00:00:00Z"),
        "limit": st.integers(1, 100).map(str),
    },
)
_dlq_params = st.fixed_dictionaries(
    {},
    optional={
        "status": st.sampled_from(RECORD_STATUSES),
        "reason": st.sampled_from(DEAD_LETTER_REASONS),
        "request_id": st.sampled_from(("r1", "r2")),
        "capability": st.sampled_from(("research", "code")),
        "limit": st.integers(1, 100).map(str),
    },
)


def _normalized(endpoint: str, params: dict[str, str]) -> dict[str, Any]:
    raw = {k: [v] for k, v in params.items()}
    parse = parse_list_query if endpoint == "requests" else parse_dlq_query
    # Phase 5 Req 3.3: a cursor is bound to the tenant it was issued for. The listing call here is a default-tenant key,
    # and the DLQ call is an Operator listing every tenant.
    return {**parse(raw).filters, "tenant": "default" if endpoint == "requests" else "*"}


@st.composite
def _scenario(draw: st.DrawFn) -> tuple[str, dict[str, str], str, list[tuple[str, str]]]:
    """Endpoint, call params, the valid cursor of the call, and labelled bad cursors."""
    e = draw(st.sampled_from(("requests", "dlq")))
    params_st = _request_params if e == "requests" else _dlq_params
    params = draw(params_st, label="params")
    f = _normalized(e, params)
    s, m = RESUME
    valid = encode_cursor(PageCursor(e=e, f=f, s=s, m=m), SECRET_KEY)  # type: ignore[arg-type]

    bad: list[tuple[str, str]] = [
        ("edited", draw(_one_char_edit(valid), label="edited")),
        ("too_long", draw(_too_long(valid), label="too_long")),
        (
            "other_key",
            encode_cursor(
                PageCursor(e=e, f=f, s=s, m=m),  # type: ignore[arg-type]
                draw(_other_key(SECRET_KEY), label="other_key"),
            ),
        ),
        (
            "other_endpoint",
            encode_cursor(PageCursor(e=OTHER[e], f=f, s=s, m=m), SECRET_KEY),  # type: ignore[arg-type]
        ),
    ]
    rnd = draw(_random_strings, label="random")
    if rnd != valid:
        bad.append(("random", rnd))
    other_params = draw(params_st, label="other_params")
    other_f = _normalized(e, other_params)
    if _canon(other_f) != _canon(f):
        bad.append(
            (
                "other_filters",
                encode_cursor(PageCursor(e=e, f=other_f, s=s, m=m), SECRET_KEY),  # type: ignore[arg-type]
            )
        )
    return e, params, valid, bad


async def _seed(store: StateStore) -> None:
    with FakeRedisTime() as clock:
        for i, (rid, cap) in enumerate((("r1", "research"), ("r2", "code"), (None, None))):
            clock.set_ms(T0 + i)
            await store.create_request(f"prompt {i}")
            await store.create_dead_letter(
                DeadLetterFields(
                    dead_letter_id=f"{i:032x}",
                    reason=DEAD_LETTER_REASONS[i % len(DEAD_LETTER_REASONS)],  # type: ignore[arg-type]
                    request_id=rid,
                    job_id=None if rid is None else "a",
                    attempt=None if rid is None else 1,
                    capabilities=None if cap is None else [cap],
                    last_error="boom",
                    message_id=f"m-{i}",
                )
            )
    await store.set_index_ready()


async def _run(e: str, params: dict[str, str], valid: str, bad: list[tuple[str, str]]) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        store = StateStore(redis)
        await _seed(store)
        app = create_app(
            store,
            AgentRegistry(redis),
            None,
            [API_KEY],
            10,
            admin_api_keys=[ADMIN_KEY],
            cursor_secret=SECRET,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            ok = await client.get(PATHS[e], params={**params, "cursor": valid}, headers=HEADERS[e])
            assert ok.status_code == 200, (e, params, ok.text)  # positive control
            for kind, cursor in bad:
                resp = await client.get(
                    PATHS[e], params={**params, "cursor": cursor}, headers=HEADERS[e]
                )
                assert resp.status_code == 400, (kind, e, params, cursor, resp.text)
                assert resp.json() == {"error": "invalid_query", "parameter": "cursor"}, (
                    kind,
                    resp.text,
                )
    finally:
        await redis.aclose()


@settings(max_examples=100, deadline=None)
@given(scenario=_scenario())
def test_endpoints_reject_invalid_cursor(
    scenario: tuple[str, dict[str, str], str, list[tuple[str, str]]],
) -> None:
    asyncio.run(_run(*scenario))
