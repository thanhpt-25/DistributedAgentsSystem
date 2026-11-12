"""Unit tests for the retention and Request_Index scripts
(das-observability-operations task 9.4)."""

from __future__ import annotations

import itertools

import pytest

from das.models import RequestStatus
from das.models.dlq import DeadLetterFields
from das.state import PURGE_DELETED, PURGE_KEEP, PURGE_STAMPED, StateStore, keys
from tests.unit.test_state_phase2_dlq import everything, failed_request, running_with_failed_job

T0 = 1_700_000_000_000
TTL = 60_000


def ids(*names: str):
    it = iter(names)
    return lambda: next(it)


def counter_ids(prefix: str = "r"):
    c = itertools.count()
    return lambda: f"{prefix}{next(c):03d}"


async def global_index_members(store: StateStore, rid: str) -> list[str]:
    """Global zsets that still mention ``rid``."""
    r = store.client
    zsets = [
        keys.REQUESTS_INDEX,
        *(keys.requests_by_status(s.value) for s in RequestStatus),
        keys.REQUESTS_FINISHED,
        keys.REQUESTS_INBOX_PURGE,
        keys.REQUESTS_PENDING,
        keys.LEASES_EXPIRY,
    ]
    return [z for z in zsets if await r.zscore(z, rid) is not None]


# ------------------------------------------------------------------ purge_request


async def test_purge_request_keeps_until_ttl_then_deletes_everything(
    fake_redis_time, state_store: StateStore
):
    fake_redis_time.set_ms(T0)
    rid = await failed_request(state_store)
    await state_store.record_inbox_event(rid, "job_result", "{}")
    r = state_store.client
    finished = int(await r.hget(keys.request(rid), keys.F_FINISHED_AT))
    assert finished == T0

    fake_redis_time.set_ms(T0 + TTL - 1)
    assert await state_store.purge_request(rid, TTL) == PURGE_KEEP
    assert await state_store.get_status(rid) is RequestStatus.FAILED

    fake_redis_time.set_ms(T0 + TTL)
    assert await state_store.purge_request(rid, TTL) == PURGE_DELETED
    for k in keys.request_keys(rid):
        assert await r.exists(k) == 0, k
    assert await global_index_members(state_store, rid) == []
    # Everything of the Request is gone. What stays is its tenant's Usage_Rollup (Phase 5 Req 6): usage for
    # chargeback outlives the Request it describes and expires on its own (usage.retention_days).
    assert {k: v for k, v in (await everything(state_store)).items() if ":usage:" not in k} == {}
    # A second sweeper finds nothing to delete (Req 14.10: counted once).
    assert await state_store.purge_request(rid, TTL) == PURGE_KEEP


async def test_purge_request_never_deletes_non_terminal(fake_redis_time, state_store):
    fake_redis_time.set_ms(T0)
    pending = await state_store.create_request("p")
    running, _lease = await running_with_failed_job(state_store)
    r = state_store.client
    # Stale candidates (e.g. left by a Replay that reopened the Request).
    await r.zadd(keys.REQUESTS_FINISHED, {pending: 0, running: 0, "gone": 0})
    await r.zadd(keys.REQUESTS_INBOX_PURGE, {"gone": 0})
    fake_redis_time.set_ms(T0 + 10 * TTL)
    for rid in (pending, running, "gone"):
        assert await state_store.purge_request(rid, TTL) == PURGE_KEEP
    assert await state_store.get_status(pending) is RequestStatus.PENDING
    assert await state_store.get_status(running) is RequestStatus.RUNNING
    assert await r.zcard(keys.REQUESTS_FINISHED) == 0
    assert await r.zcard(keys.REQUESTS_INBOX_PURGE) == 0


async def test_purge_request_stamps_missing_finished_at(fake_redis_time, state_store):
    fake_redis_time.set_ms(T0)
    rid = await state_store.create_request("x")
    assert (await state_store.cancel_request(rid)).ok
    r = state_store.client
    await r.hdel(keys.request(rid), keys.F_FINISHED_AT)  # v1 data

    fake_redis_time.set_ms(T0 + 10 * TTL)
    assert await state_store.purge_request(rid, TTL) == PURGE_STAMPED
    assert int(await r.hget(keys.request(rid), keys.F_FINISHED_AT)) == T0 + 10 * TTL
    assert await r.zscore(keys.REQUESTS_FINISHED, rid) == T0 + 10 * TTL
    assert await r.zscore(keys.REQUESTS_INBOX_PURGE, rid) == T0 + 10 * TTL
    assert await state_store.purge_request(rid, TTL) == PURGE_KEEP
    fake_redis_time.set_ms(T0 + 11 * TTL)
    assert await state_store.purge_request(rid, TTL) == PURGE_DELETED


async def test_finished_candidates_order_and_bound(fake_redis_time, state_store):
    store = StateStore(state_store.client, id_factory=ids("b", "a", "c"))
    fake_redis_time.set_ms(T0)
    b = await store.create_request("1")
    a = await store.create_request("2")
    await store.cancel_request(b)
    await store.cancel_request(a)  # same finished_at ms: Request_ID ascending
    fake_redis_time.set_ms(T0 + 5)
    c = await store.create_request("3")
    await store.cancel_request(c)
    assert await store.finished_candidates(T0 + TTL, TTL, 10) == ["a", "b"]
    assert await store.finished_candidates(T0 + TTL + 5, TTL, 10) == ["a", "b", "c"]
    assert await store.finished_candidates(T0 + TTL + 5, TTL, 1) == ["a"]
    assert await store.inbox_candidates(T0 + TTL - 1, TTL, 10) == []
    assert await store.inbox_candidates(T0 + TTL + 5, TTL, 2) == ["a", "b"]
    with pytest.raises(ValueError):
        await store.finished_candidates(T0, TTL, 0)


# ------------------------------------------------------------------ purge_inbox


async def test_purge_inbox_only_terminal_after_ttl(fake_redis_time, state_store):
    fake_redis_time.set_ms(T0)
    rid = await failed_request(state_store)
    running = await state_store.create_request("still running")
    await state_store.record_inbox_event(rid, "job_result", "{}")
    await state_store.record_inbox_event(running, "job_result", "{}")
    r = state_store.client
    await r.zadd(keys.REQUESTS_INBOX_PURGE, {running: 0})  # stale

    fake_redis_time.set_ms(T0 + TTL - 1)
    assert await state_store.purge_inbox(rid, TTL) == PURGE_KEEP
    assert await r.exists(keys.inbox(rid)) == 1

    fake_redis_time.set_ms(T0 + TTL)
    assert await state_store.purge_inbox(running, TTL) == PURGE_KEEP
    assert await r.exists(keys.inbox(running)) == 1
    assert await state_store.purge_inbox(rid, TTL) == PURGE_DELETED
    assert await r.exists(keys.inbox(rid)) == 0
    # Every other key of the Request stays (Req 14.7).
    assert await state_store.get_status(rid) is RequestStatus.FAILED
    assert await r.exists(keys.jobs(rid), keys.transitions(rid)) == 2
    assert await r.zscore(keys.REQUESTS_FINISHED, rid) is not None
    assert await r.zcard(keys.REQUESTS_INBOX_PURGE) == 0
    assert await state_store.purge_inbox(rid, TTL) == PURGE_KEEP


# ------------------------------------------------------------------ purge_dead_letter


def _dl(dlid: str, reason: str = "retries_exhausted") -> DeadLetterFields:
    return DeadLetterFields(dead_letter_id=dlid, reason=reason, message_id=f"m-{dlid[:4]}")


async def test_purge_dead_letter_never_pending_and_terminal_after_ttl(
    fake_redis_time, state_store
):
    pending, discarded = "1" * 32, "2" * 32
    fake_redis_time.set_ms(T0)
    await state_store.create_dead_letter(_dl(pending))
    await state_store.create_dead_letter(_dl(discarded, "malformed"))
    await state_store.discard_dead_letter(discarded)
    r = state_store.client

    fake_redis_time.set_ms(T0 + 100 * TTL)
    assert await state_store.purge_dead_letter(pending, TTL) == PURGE_KEEP
    assert await state_store.get_dead_letter(pending) is not None
    assert await state_store.dlq_terminal_candidates(T0 + 100 * TTL, TTL, 10) == [discarded]

    fake_redis_time.set_ms(T0 + TTL - 1)
    assert await state_store.purge_dead_letter(discarded, TTL) == PURGE_KEEP
    fake_redis_time.set_ms(T0 + TTL)
    assert await state_store.purge_dead_letter(discarded, TTL) == PURGE_DELETED
    assert await state_store.get_dead_letter(discarded) is None
    for z in (
        keys.DLQ_INDEX,
        keys.DLQ_TERMINAL,
        keys.dlq_by_status("Discarded"),
        keys.dlq_by_reason("malformed"),
    ):
        assert await r.zscore(z, discarded) is None, z
    assert (await state_store.list_dead_letters()).records[0].dead_letter_id == pending
    assert await state_store.purge_dead_letter(discarded, TTL) == PURGE_KEEP


# ------------------------------------------------------------------ index_request


async def test_index_request_backfills_v1_request(fake_redis_time, state_store):
    r = state_store.client
    fake_redis_time.set_ms(T0 + 9000)
    # Requests as written by v1 scripts: no index entries, no finished_at.
    await r.hset(
        keys.request("v1done"),
        mapping={"status": "Completed", "prompt": "x", "created_at": "1700000000.123456"},
    )
    await r.hset(
        keys.request("v1run"),
        mapping={"status": "Running", "prompt": "y", "created_at": "1700000001.5"},
    )
    await r.zadd(keys.requests_by_status("Pending"), {"v1run": 1})  # stale status
    assert await state_store.index_request("v1done")
    assert await state_store.index_request("v1run")
    assert not await state_store.index_request("missing")

    assert await r.zscore(keys.REQUESTS_INDEX, "v1done") == T0 + 123
    assert await r.zscore(keys.requests_by_status("Completed"), "v1done") == T0 + 123
    assert int(await r.hget(keys.request("v1done"), keys.F_FINISHED_AT)) == T0 + 9000
    assert await r.zscore(keys.REQUESTS_FINISHED, "v1done") == T0 + 9000
    assert await r.zscore(keys.requests_by_status("Running"), "v1run") == T0 + 1500
    assert await r.zscore(keys.requests_by_status("Pending"), "v1run") is None
    assert await r.hget(keys.request("v1run"), keys.F_FINISHED_AT) is None

    before = await everything(state_store)
    fake_redis_time.advance(10)
    assert await state_store.index_request("v1done")  # idempotent
    assert await everything(state_store) == before


async def test_index_ready_flag(state_store):
    assert not await state_store.index_ready()
    await state_store.set_index_ready()
    assert await state_store.index_ready()


# ------------------------------------------------------------------ list_requests


async def _populate(fake_redis_time, store: StateStore) -> list[tuple[int, str]]:
    """10 Requests over 4 ms values (ties), mixed statuses; returns (ms, rid) desc."""
    out = []
    for i in range(10):
        fake_redis_time.set_ms(T0 + (i // 3))
        rid = await store.create_request(f"p{i}")
        out.append((T0 + i // 3, rid))
        if i % 3 == 1:
            await store.cancel_request(rid)
    return sorted(out, reverse=True)


async def _all_pages(store: StateStore, limit: int, **kw) -> list[str]:
    seen: list[str] = []
    after = None
    for _ in range(100):
        page = await store.list_requests(limit=limit, after=after, **kw)
        assert len(page.items) <= limit
        seen += [it.request_id for it in page.items]
        if page.next_after is None:
            return seen
        after = page.next_after
    raise AssertionError("pagination did not end")


async def test_list_requests_order_pagination_and_filters(fake_redis_time, state_store):
    store = StateStore(state_store.client, id_factory=counter_ids())
    expected = await _populate(fake_redis_time, store)
    order = [rid for _, rid in expected]
    statuses = {rid: await store.get_status(rid) for rid in order}

    for limit in (1, 3, 4, 10, 50):
        assert await _all_pages(store, limit) == order

    both = [RequestStatus.CANCELLED, RequestStatus.PENDING, RequestStatus.CANCELLED]
    assert await _all_pages(store, 2, statuses=both) == order
    cancelled = [rid for rid in order if statuses[rid] is RequestStatus.CANCELLED]
    assert await _all_pages(store, 2, statuses=[RequestStatus.CANCELLED]) == cancelled
    assert await _all_pages(store, 2, statuses=[RequestStatus.RUNNING]) == []

    window = [rid for ms, rid in expected if T0 + 1 <= ms < T0 + 3]
    assert (
        await _all_pages(store, 2, created_after_ms=T0 + 1, created_before_ms=T0 + 3) == window
    )

    page = await store.list_requests(limit=20)
    assert page.next_after is None
    first = page.items[0]
    assert first.request_id == order[0]
    assert first.created_at_ms == expected[0][0]
    assert first.job_count == 0
    assert first.usage.total_tokens == 0
    by_id = {it.request_id: it for it in page.items}
    for rid in cancelled:
        assert by_id[rid].finished_at_ms is not None
    for rid in set(order) - set(cancelled):
        assert by_id[rid].finished_at_ms is None


async def test_list_requests_cursor_survives_deleted_position(fake_redis_time, state_store):
    store = StateStore(state_store.client, id_factory=counter_ids())
    expected = await _populate(fake_redis_time, store)
    order = [rid for _, rid in expected]
    page = await store.list_requests(limit=3)
    assert [it.request_id for it in page.items] == order[:3]
    assert page.next_after == expected[2]
    await store.delete_request(order[2])  # the cursor position disappears
    # A Request created after the walk started does not show up later (Req 15.7).
    fake_redis_time.set_ms(T0 + 100)
    await store.create_request("new")
    rest = await store.list_requests(limit=50, after=page.next_after)
    assert [it.request_id for it in rest.items] == order[3:]
    assert rest.next_after is None


async def test_list_requests_rejects_bad_arguments(state_store):
    with pytest.raises(ValueError):
        await state_store.list_requests(limit=0)
    with pytest.raises(ValueError):
        await state_store.list_requests(statuses=[])
    empty = await state_store.list_requests()
    assert empty.items == [] and empty.next_after is None
