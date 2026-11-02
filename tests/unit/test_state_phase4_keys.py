"""Unit tests for Phase 4 State_Store keys, Lua helpers and ``rate_check``
(das-client-api tasks 4.1, Phase 4 Req 2.1-2.4)."""

from __future__ import annotations

import math

import pytest

from das.state import StateStore, keys
from das.state.scripts import _read

START = 1000.0


def _helper_script(client, body: str):
    """Register a test-only script that runs ``body`` after the shared helpers."""
    return client.register_script(_read("_common") + "\n" + body)


# ------------------------------------------------------------------ keys


def test_phase4_key_builders():
    assert keys.client_bucket("fp") == "das:client:fp:bucket"
    assert keys.client_active("fp") == "das:client:fp:active"
    assert keys.client_idem("fp", "h") == "das:client:fp:idem:h"
    assert keys.client_streams("fp") == "das:client:fp:streams"
    assert keys.events("r") == "das:req:r:events"
    assert keys.webhook("r") == "das:req:r:webhook"
    assert keys.WEBHOOKS_DUE == "das:webhooks:due"


def test_request_keys_end_with_events_and_webhook():
    ks = keys.request_keys("r")
    assert ks[-2:] == ["das:req:r:events", "das:req:r:webhook"]
    assert len(ks) == len(set(ks)) == 11


def test_phase4_request_fields_are_reserved():
    fields = (keys.F_CLIENT_FP, keys.F_BUDGET, keys.F_WEBHOOK_URL, keys.F_WEBHOOK_SECRET_NAME,
              keys.F_EVENTS_MAX_LEN)
    assert fields == ("client_fp", "budget", "webhook_url", "webhook_secret_name", "events_max_len")
    for f in fields:
        assert f in keys.RESERVED_REQUEST_FIELDS


def test_default_events_max_len():
    from das.state import DEFAULT_EVENTS_MAX_LEN

    assert DEFAULT_EVENTS_MAX_LEN == 1000


# ------------------------------------------------------------------ Lua helpers


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("key_prefix('das:req:r1')", "das"),
        ("events_key('das:req:r1')", "das:req:r1:events"),
        ("webhook_key('das:req:r1')", "das:req:r1:webhook"),
        ("outbox_key('das:req:r1')", "das:webhooks:due"),
        ("active_key('das:req:r1', 'cbabecbd')", "das:client:cbabecbd:active"),
        ("idem_key('das:req:r1', 'cbabecbd', 'h1')", "das:client:cbabecbd:idem:h1"),
        ("fmt_double(0.1 + 0.2)", "0.30000000000000004"),
    ],
)
async def test_lua_key_helpers(fake_redis, expr, expected):
    assert await _helper_script(fake_redis, f"return {expr}")(keys=[], args=[]) == expected


async def test_lua_key_helpers_match_python(fake_redis):
    req = keys.request("r1")
    script = _helper_script(
        fake_redis,
        "return {events_key(KEYS[1]), webhook_key(KEYS[1]), outbox_key(KEYS[1]),"
        " active_key(KEYS[1], 'f'), idem_key(KEYS[1], 'f', 'h')}",
    )
    assert await script(keys=[req], args=[]) == [
        keys.events("r1"), keys.webhook("r1"), keys.WEBHOOKS_DUE,
        keys.client_active("f"), keys.client_idem("f", "h"),
    ]


# ------------------------------------------------------------------ rate_check


async def test_rate_check_bucket_sequence(state_store: StateStore, fake_redis_time):
    fake_redis_time.set(START)
    assert await state_store.rate_check("fp", 1.0, 2) is None
    assert await state_store.rate_check("fp", 1.0, 2) is None
    assert await state_store.rate_check("fp", 1.0, 2) == pytest.approx(1.0)
    fake_redis_time.advance(0.5)
    assert await state_store.rate_check("fp", 1.0, 2) == pytest.approx(0.5)
    fake_redis_time.advance(0.5)
    assert await state_store.rate_check("fp", 1.0, 2) is None


async def test_rate_check_zero_rate_never_touches_the_bucket(state_store: StateStore, fake_redis_time):
    fake_redis_time.set(START)
    for _ in range(1000):
        assert await state_store.rate_check("fp", 0.0, 1) is None
    assert await state_store.client.exists(keys.client_bucket("fp")) == 0


async def test_rate_check_bucket_ttl(state_store: StateStore, fake_redis_time):
    fake_redis_time.set(START)
    assert await state_store.rate_check("fp", 1.0, 2) is None
    pttl = await state_store.client.pttl(keys.client_bucket("fp"))
    assert 0 < pttl <= math.ceil(2 / 1 * 1000) + 60_000


async def test_rate_check_rejects_bad_arguments(fake_redis):
    from das.state.scripts import script_source

    script = fake_redis.register_script(script_source("rate_check"))
    for args in (["x", "2"], ["-1", "2"], ["1", "0"], ["1", "1.5"]):
        with pytest.raises(Exception, match="rate_check"):
            await script(keys=["das:client:fp:bucket"], args=args)
    assert await fake_redis.exists("das:client:fp:bucket") == 0
