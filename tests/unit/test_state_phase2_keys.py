"""Unit tests for Phase 2 State_Store keys, Lua helpers, create_request and
acquire_lease (das-observability-operations task 9.1)."""

from __future__ import annotations

import json

import pytest

from das.models import RequestStatus, TokenUsage
from das.observability.trace_context import TraceContext
from das.state import LEASE_KIND_INITIAL, LEASE_KIND_TAKEOVER, StateStore, keys
from das.state.scripts import _read
from tests.unit.test_state_store import PLAN

TP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def _helper_script(client, body: str):
    """Register a test-only script that runs ``body`` after the shared helpers."""
    return client.register_script(_read("_common") + "\n" + body)


# ------------------------------------------------------------------ keys


def test_phase2_key_layout():
    assert keys.usage("r") == "das:req:r:usage"
    assert keys.pending_replays("r") == "das:req:r:pending_replays"
    assert keys.REQUESTS_INDEX == "das:requests:index"
    assert keys.requests_by_status("Pending") == "das:requests:by_status:Pending"
    assert keys.REQUESTS_INDEX_READY == "das:requests:index:ready"
    assert keys.REQUESTS_INDEX_BACKFILL_LOCK == "das:requests:index:backfill_lock"
    assert keys.REQUESTS_FINISHED == "das:requests:finished"
    assert keys.REQUESTS_INBOX_PURGE == "das:requests:inbox_purge"
    assert keys.dlq("d") == "das:dlq:d"
    assert keys.DLQ_INDEX == "das:dlq:index"
    assert keys.dlq_by_status("Pending") == "das:dlq:by_status:Pending"
    assert keys.dlq_by_reason("malformed") == "das:dlq:by_reason:malformed"
    assert keys.DLQ_TERMINAL == "das:dlq:terminal"
    assert keys.GATEWAY_CURSOR_KEY == "das:gateway:cursor_key"


def test_request_keys_include_usage_and_pending_replays():
    ks = keys.request_keys("r")
    assert keys.usage("r") in ks and keys.pending_replays("r") in ks
    assert len(ks) == len(set(ks)) == 11  # + Phase 4 events, webhook


def test_phase2_request_fields_are_reserved():
    for f in (keys.F_TRACE_CONTEXT, keys.F_FINISHED_AT, keys.F_JOB_COUNT, keys.F_USAGE_TOTAL,
              keys.F_REPLAY_HISTORY):
        assert f in keys.RESERVED_REQUEST_FIELDS


# ------------------------------------------------------------------ create_request


async def test_create_request_indexes_pending_by_created_ms(fake_redis_time, fake_redis):
    fake_redis_time.set_ms(1_700_000_000_500)
    store = StateStore(fake_redis)
    rid = await store.create_request("hello")
    assert await fake_redis.zscore(keys.REQUESTS_INDEX, rid) == 1_700_000_000_500
    assert await fake_redis.zscore(keys.requests_by_status("Pending"), rid) == 1_700_000_000_500
    snap = await store.load_snapshot(rid)
    assert snap.trace_context is None
    assert await fake_redis.hget(keys.request(rid), keys.F_TRACE_CONTEXT) is None


async def test_create_request_persists_trace_context(state_store: StateStore):
    ctx = TraceContext(traceparent=TP, tracestate="vendor=x")
    rid = await state_store.create_request("hello", trace_context=ctx)
    snap = await state_store.load_snapshot(rid)
    assert snap.trace_context == ctx
    no_state = await state_store.create_request("p", TraceContext(traceparent=TP))
    raw = await state_store.client.hget(keys.request(no_state), keys.F_TRACE_CONTEXT)
    assert json.loads(raw) == {"traceparent": TP}


async def test_malformed_stored_trace_context_reads_as_none(state_store: StateStore):
    rid = await state_store.create_request("hello")
    await state_store.client.hset(keys.request(rid), keys.F_TRACE_CONTEXT, "{not json")
    assert (await state_store.load_snapshot(rid)).trace_context is None


async def test_snapshot_phase2_defaults_for_v1_request(state_store: StateStore):
    rid = await state_store.create_request("hello")
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.finished_at is None and snap.job_count is None
    assert snap.usage_totals == TokenUsage()
    assert snap.replay_history == [] and snap.request_transitions == []
    assert (await state_store.load_snapshot(rid)).request_transitions is None


async def test_delete_request_removes_index_entries(state_store: StateStore):
    rid = await state_store.create_request("hello")
    await state_store.delete_request(rid)
    r = state_store.client
    assert await r.zscore(keys.REQUESTS_INDEX, rid) is None
    assert await r.zscore(keys.requests_by_status("Pending"), rid) is None


# ------------------------------------------------------------------ acquire_lease kind


async def test_acquire_kind_initial_then_reacquire_by_same_agent(state_store: StateStore):
    rid = await state_store.create_request("p")
    first = await state_store.acquire_lease(rid, "m1", 30)
    assert first.ok and first.kind == LEASE_KIND_INITIAL
    assert await state_store.client.hget(keys.lease(rid), keys.F_LAST_OWNER) == "m1"
    assert (await state_store.release_lease(first.lease)).ok
    again = await state_store.acquire_lease(rid, "m1", 30)
    assert again.ok and again.kind == LEASE_KIND_INITIAL
    assert again.lease.epoch == first.lease.epoch + 1


async def test_acquire_kind_takeover_after_release_by_other(state_store: StateStore):
    rid = await state_store.create_request("p")
    first = await state_store.acquire_lease(rid, "m1", 30)
    assert (await state_store.release_lease(first.lease)).ok
    # last_owner survives release.
    assert await state_store.client.hget(keys.lease(rid), keys.F_LAST_OWNER) == "m1"
    taken = await state_store.acquire_lease(rid, "m2", 30)
    assert taken.ok and taken.kind == LEASE_KIND_TAKEOVER
    assert await state_store.client.hget(keys.lease(rid), keys.F_LAST_OWNER) == "m2"


async def test_acquire_kind_takeover_after_expiry(fake_redis_time, fake_redis):
    store = StateStore(fake_redis)
    rid = await store.create_request("p")
    assert (await store.acquire_lease(rid, "m1", 5)).kind == LEASE_KIND_INITIAL
    held = await store.acquire_lease(rid, "m2", 5)
    assert not held.ok and held.kind is None
    fake_redis_time.advance(6.0)
    taken = await store.acquire_lease(rid, "m2", 5)
    assert taken.ok and taken.kind == LEASE_KIND_TAKEOVER


# ------------------------------------------------------------------ _common.lua helpers


async def test_index_move_moves_between_status_sets(fake_redis):
    script = _helper_script(
        fake_redis, "index_move(ARGV[1], KEYS[1], KEYS[2], tonumber(ARGV[2])) return 1"
    )
    pending, running = keys.requests_by_status("Pending"), keys.requests_by_status("Running")
    await fake_redis.zadd(pending, {"r": 10})
    await script(keys=[pending, running], args=["r", 10])
    assert await fake_redis.zscore(pending, "r") is None
    assert await fake_redis.zscore(running, "r") == 10
    # Same source and target: member stays.
    await script(keys=[running, running], args=["r", 10])
    assert await fake_redis.zscore(running, "r") == 10


async def test_mark_finished_stamps_hash_and_retention_sets(fake_redis):
    script = _helper_script(
        fake_redis,
        "mark_finished(KEYS[1], KEYS[2], KEYS[3], ARGV[1], tonumber(ARGV[2])) return 1",
    )
    await script(
        keys=[keys.request("r"), keys.REQUESTS_FINISHED, keys.REQUESTS_INBOX_PURGE],
        args=["r", 1_700_000_000_500],
    )
    assert await fake_redis.hget(keys.request("r"), keys.F_FINISHED_AT) == "1700000000500"
    assert await fake_redis.zscore(keys.REQUESTS_FINISHED, "r") == 1_700_000_000_500
    assert await fake_redis.zscore(keys.REQUESTS_INBOX_PURGE, "r") == 1_700_000_000_500


async def test_append_usage_pushes_verbatim_and_sums_totals(fake_redis):
    script = _helper_script(fake_redis, "return append_usage(KEYS[1], KEYS[2], ARGV[1])")
    rec1 = json.dumps({"operation": "plan", "provider": "p", "model": "m",
                       "usage": {"input_tokens": 3, "output_tokens": 4, "total_tokens": 7}})
    rec2 = {"operation": "summarize", "provider": "p", "model": "m",
            "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}}
    n = await script(keys=[keys.usage("r"), keys.request("r")], args=[json.dumps([rec1, rec2])])
    assert n == 2
    stored = await fake_redis.lrange(keys.usage("r"), 0, -1)
    assert stored[0] == rec1 and json.loads(stored[1]) == rec2
    h = await fake_redis.hgetall(keys.request("r"))
    assert (h["usage_input"], h["usage_output"], h["usage_total"]) == ("4", "6", "10")
    assert await script(keys=[keys.usage("r"), keys.request("r")], args=["[]"]) == 0


async def test_usage_totals_read_into_snapshot(state_store: StateStore):
    rid = await state_store.create_request("p")
    await state_store.client.hset(
        keys.request(rid), mapping={"usage_input": 4, "usage_output": 6, "usage_total": 10,
                                    keys.F_JOB_COUNT: 2, keys.F_FINISHED_AT: 1234}
    )
    snap = await state_store.load_snapshot(rid)
    assert snap.usage_totals == TokenUsage(input_tokens=4, output_tokens=6, total_tokens=10)
    assert snap.job_count == 2 and snap.finished_at == 1234


@pytest.mark.parametrize("tag", ["", "a" * 32])
async def test_put_job_tag_sets_dead_letter_id(fake_redis, tag: str):
    script = _helper_script(
        fake_redis, "return put_job(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], ARGV[4], KEYS[3])"
    )
    rec = json.dumps({"job": {"job_id": "a"}, "state": "Waiting", "attempt": 0})
    written = await script(
        keys=[keys.jobs("r"), keys.transitions("r"), keys.request("r")],
        args=["a", rec, "2024-01-01T00:00:00.000Z", tag],
    )
    assert written == 1
    [entry] = await fake_redis.lrange(keys.transitions("r"), 0, -1)
    data = json.loads(entry)
    assert data.get("dead_letter_id") == (tag or None)
    assert ("dead_letter_id" in data) == bool(tag)


async def test_request_level_transitions_are_split(state_store: StateStore):
    rid = await state_store.create_request("p")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.commit_plan(lease, PLAN)).ok
    req_entry = {"scope": "request", "from_status": "Failed", "to_status": "Pending",
                 "dead_letter_id": "b" * 32, "at": "2024-01-01T00:00:00.000Z"}
    await state_store.client.rpush(keys.transitions(rid), json.dumps(req_entry))
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert len(snap.transitions) == 2  # the two Waiting entries of the plan
    [rt] = snap.request_transitions
    assert rt.from_status is RequestStatus.FAILED and rt.to_status is RequestStatus.PENDING
    assert len(await state_store.get_transitions(rid)) == 2
    assert len(await state_store.get_request_transitions(rid)) == 1
