"""Event_Stream connection slots and event reads of the SSE endpoint
(das-client-api task 7.4; Phase 4 Req 7.1, 7.3, 7.4, 7.7, 7.8)."""

from __future__ import annotations

from das.state import StateStore, keys
from tests.unit.test_state_phase4_events import events, submitted

FP = "cbabecbd"
OTHER_FP = "0badf00d"
TTL = 60.0


async def members(store: StateStore, fp: str = FP) -> set[str]:
    return {str(m) for m in await store.client.zrange(keys.client_streams(fp), 0, -1)}


# --------------------------------------------------------------------------- slots


async def test_acquire_until_the_limit_then_refuse(state_store: StateStore, fake_redis_time):
    assert await state_store.acquire_stream(FP, "a", 2, TTL)
    assert await state_store.acquire_stream(FP, "b", 2, TTL)
    assert not await state_store.acquire_stream(FP, "c", 2, TTL)
    assert await members(state_store) == {"a", "b"}  # a refusal registers nothing


async def test_release_frees_a_slot(state_store: StateStore, fake_redis_time):
    assert await state_store.acquire_stream(FP, "a", 1, TTL)
    assert not await state_store.acquire_stream(FP, "b", 1, TTL)
    await state_store.release_stream(FP, "a")
    assert await state_store.acquire_stream(FP, "b", 1, TTL)


async def test_releasing_an_unknown_slot_is_harmless(state_store: StateStore, fake_redis_time):
    await state_store.release_stream(FP, "never-acquired")
    assert await members(state_store) == set()


async def test_expired_slots_are_pruned_by_the_next_acquire(state_store: StateStore, fake_redis_time):
    """A crashed Gateway's slots expire on their own (Req 7.8)."""
    assert await state_store.acquire_stream(FP, "a", 1, TTL)
    fake_redis_time.advance(TTL + 1)
    assert await state_store.acquire_stream(FP, "b", 1, TTL)
    assert await members(state_store) == {"b"}


async def test_renew_keeps_a_live_slot_counted(state_store: StateStore, fake_redis_time):
    assert await state_store.acquire_stream(FP, "a", 2, TTL)
    assert await state_store.acquire_stream(FP, "b", 2, TTL)
    fake_redis_time.advance(40)
    await state_store.renew_stream(FP, "a", TTL)  # a lives to t=100, b expires at t=60
    fake_redis_time.advance(40)
    assert await state_store.acquire_stream(FP, "c", 2, TTL)  # b pruned
    assert not await state_store.acquire_stream(FP, "d", 2, TTL)  # a and c remain
    assert await members(state_store) == {"a", "c"}


async def test_renew_re_adds_a_pruned_slot_of_a_live_stream(state_store: StateStore, fake_redis_time):
    assert await state_store.acquire_stream(FP, "a", 5, TTL)
    fake_redis_time.advance(TTL + 1)
    assert await state_store.acquire_stream(FP, "b", 5, TTL)  # prunes a
    await state_store.renew_stream(FP, "a", TTL)
    assert await members(state_store) == {"a", "b"}


async def test_slots_are_per_client(state_store: StateStore, fake_redis_time):
    assert await state_store.acquire_stream(FP, "a", 1, TTL)
    assert await state_store.acquire_stream(OTHER_FP, "a", 1, TTL)


async def test_the_key_expires_once_every_slot_is_gone(state_store: StateStore, fake_redis_time):
    assert await state_store.acquire_stream(FP, "a", 1, TTL)
    ttl_ms = await state_store.client.pttl(keys.client_streams(FP))
    assert 0 < ttl_ms <= TTL * 1000 + 60_000


# --------------------------------------------------------------------------- event reads


async def test_read_events_returns_entries_after_the_cursor_in_order(state_store: StateStore):
    rid = await submitted(state_store)
    assert (await state_store.cancel_request(rid)).ok
    stored = await events(state_store, rid)

    everything = await state_store.read_events(rid, "0-0")
    assert [(eid, f["type"]) for eid, f in everything] == [(sid, t) for sid, t, _ in stored]
    after_first = await state_store.read_events(rid, stored[0][0])
    assert [eid for eid, _ in after_first] == [stored[1][0]]
    assert await state_store.read_events(rid, stored[-1][0]) == []


async def test_read_events_honours_the_count(state_store: StateStore):
    rid = await submitted(state_store)
    assert (await state_store.cancel_request(rid)).ok
    page = await state_store.read_events(rid, "0-0", count=1)
    assert len(page) == 1


async def test_read_events_of_an_unknown_request_is_empty(state_store: StateStore):
    assert await state_store.read_events("nope", "0-0") == []


async def test_first_event_id_is_the_oldest_retained_entry(state_store: StateStore):
    rid = await submitted(state_store)
    assert (await state_store.cancel_request(rid)).ok
    stored = await events(state_store, rid)
    assert await state_store.first_event_id(rid) == stored[0][0]
    await state_store.client.delete(keys.events(rid))
    assert await state_store.first_event_id(rid) is None
