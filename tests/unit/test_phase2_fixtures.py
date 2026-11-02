"""Sanity checks for Phase 2 shared test fixtures and strategies (task 1.1)."""

from __future__ import annotations

import base64
import json
import re
from decimal import Decimal

import pytest
from hypothesis import given

from das.clock import FakeClock
from das.codec.message_codec import MessageCodec
from tests.fixtures.redis_time import FakeRedisTime
from tests.fixtures.v1_codec import V1_ENVELOPE_FIELDS, V1ParseError, V1ReferenceCodec
from tests.strategies import (
    dead_letter_records,
    messages,
    price_tables,
    trace_contexts,
    usage_records,
)

_TRACEPARENT = re.compile(r"\A00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}\Z")


# ------------------------------------------------------------------ V1ReferenceCodec
@given(messages())
def test_v1_reference_matches_current_codec(message) -> None:
    ref = V1ReferenceCodec()
    data = ref.encode(message)
    assert data == MessageCodec().encode(message)
    assert ref.decode(data) == message
    assert set(json.loads(data)) == V1_ENVELOPE_FIELDS


def test_v1_reference_rejects_extra_field_and_version_2() -> None:
    obj = {
        "type": "heartbeat",
        "schema_version": 1,
        "message_id": "m",
        "correlation_id": "",
        "timestamp": "2025-01-01T00:00:00.000Z",
        "payload": {"agent_id": "a", "role": "worker", "capabilities": []},
    }
    ref = V1ReferenceCodec()
    ref.decode(json.dumps(obj).encode())
    with pytest.raises(V1ParseError) as exc:
        ref.decode(json.dumps({**obj, "trace_context": None}).encode())
    assert exc.value.field == "trace_context"
    with pytest.raises(V1ParseError) as exc:
        ref.decode(json.dumps({**obj, "schema_version": 2}).encode())
    assert exc.value.field == "schema_version"


# ------------------------------------------------------------------ FakeRedisTime
async def test_fake_redis_time_drives_time_and_lua(fake_redis, fake_redis_time) -> None:
    fake_redis_time.set_ms(1_700_000_000_123)
    assert await fake_redis.time() == (1_700_000_000, 123_000)
    lua = "local t = redis.call('TIME'); return t[1] * 1000 + math.floor(t[2] / 1000)"
    assert await fake_redis.eval(lua, 0) == 1_700_000_000_123
    fake_redis_time.advance(2.5)
    assert await fake_redis.eval(lua, 0) == 1_700_000_002_623
    assert fake_redis_time.now_ms() == 1_700_000_002_623


async def test_fake_redis_time_follows_clock(fake_redis, fake_redis_time_factory) -> None:
    clock = FakeClock(start=1_000.0)
    t = fake_redis_time_factory(clock)
    clock.advance(5)
    assert await fake_redis.time() == (1_005, 0)
    with pytest.raises(RuntimeError):
        t.advance(1)


def test_fake_redis_time_uninstall_restores_real_time() -> None:
    from fakeredis.commands_mixins import server_mixin

    original = server_mixin.time
    with FakeRedisTime(start=1.0):
        assert server_mixin.time is not original
    assert server_mixin.time is original


# ------------------------------------------------------------------ strategies
@given(trace_contexts())
def test_trace_contexts_are_w3c_shaped(tc) -> None:
    assert _TRACEPARENT.match(tc["traceparent"])
    tid, pid = tc["traceparent"].split("-")[1:3]
    assert set(tid) != {"0"} and set(pid) != {"0"}
    ts = tc["tracestate"]
    if ts is not None:
        members = ts.split(",")
        assert len(ts) <= 512 and 1 <= len(members) <= 32
        keys = [m.split("=", 1)[0] for m in members]
        assert len(keys) == len(set(keys))


@given(usage_records(job_id_pool=["a", "b"]))
def test_usage_records_shape(r) -> None:
    assert r["operation"] in {"plan", "summarize", "job"}
    assert all(v >= 0 for v in r["usage"].values())
    if r["operation"] == "job":
        assert r["job_id"] in {"a", "b"} and r["attempt"] >= 1
    else:
        assert r["job_id"] is None and r["attempt"] is None


@given(price_tables())
def test_price_tables_valid(t) -> None:
    pairs = [(e["provider"], e["model"]) for e in t["models"]]
    assert len(pairs) == len(set(pairs))
    for e in t["models"]:
        for p in (e["input_per_million"], e["output_per_million"]):
            assert Decimal(0) <= p <= Decimal(1_000_000)
            assert -p.as_tuple().exponent <= 6


@given(dead_letter_records())
def test_dead_letter_records_consistent(r) -> None:
    assert re.fullmatch(r"[0-9a-f]{32}", r["dead_letter_id"])
    assert (r["discarded_at"] is not None) == (r["status"] == "Discarded")
    assert (r["status_changed_at"] is None) == (r["status"] == "Pending")
    if r["reason"] == "malformed":
        assert r["envelope_json"] is None and r["request_id"] is None
        raw = base64.b64decode(r["raw_b64"])
        assert r["original_size"] >= len(raw)
        assert r["truncated"] == (r["original_size"] > len(raw))
    else:
        env = json.loads(r["envelope_json"])
        assert env["payload"]["job_id"] == r["job_id"]
        assert env["correlation_id"] == r["request_id"]
