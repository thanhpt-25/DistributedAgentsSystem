"""The Tenant_Backfill script and its store calls (das-multi-tenant task 3.8; Phase 5 Req 9.2).

Phase 4 data has the global indexes but no tenant twins and no ``tenant`` field. The tests build such data by
letting the Phase 5 scripts write it and then removing everything Phase 5 added; the backfill must put back exactly
what the scripts wrote (Property 7), changing nothing when run again or beside writers."""

from __future__ import annotations

from typing import Any

from das.models import JobRecord, JobState
from das.state import StateStore, TransitionBatch, keys
from tests.unit.test_state_phase2_dlq import DL1, DL2, everything, fields
from tests.unit.test_state_phase2_writes import FINAL
from tests.unit.test_state_store import PLAN
from tests.unit.test_state_tenant_scripts import A, lease_of, submit

DL3 = "3" * 32
DL4 = "4" * 32


def twins(snapshot: dict[str, Any]) -> dict[str, Any]:
    """The tenant keys a backfill is responsible for: indexes, status sets, active sets, DLQ twins. Not the flag,
    the lock, the retention setting or the usage rollups."""
    return {
        k: v
        for k, v in snapshot.items()
        if k.startswith("das:tenant:")
        and ":usage:" not in k
        and not k.startswith("das:tenant:index:")
        and k != keys.TENANT_USAGE_RETENTION
    }


async def failed_job(store: StateStore, rid: str) -> None:
    lease = await lease_of(store, rid)
    assert (await store.commit_plan(lease, PLAN)).ok
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    skipped = JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed")
    assert (await store.apply_transition(lease, TransitionBatch(jobs={"a": failed, "b": skipped}))).ok


async def phase_4_data(store: StateStore) -> dict[str, Any]:
    """Requests in every status and Dead_Letter_Records in every status, written by the Phase 5 scripts, then
    stripped of everything Phase 5 added. Returns what the twins looked like before the strip."""
    pending = await store.create_request("pending")
    running = await store.create_request("running")
    await store.commit_plan(await lease_of(store, running), PLAN)
    done = await store.create_request("done")
    done_lease = await lease_of(store, done)
    await store.commit_plan(done_lease, PLAN)
    await store.finalize(done_lease, FINAL)
    cancelled = await store.create_request("cancelled")
    await store.cancel_request(cancelled, prices=None)
    failing = await store.create_request("failing")
    await failed_job(store, failing)
    await store.create_dead_letter(fields(DL1, rid=failing))
    await store.create_dead_letter(fields(DL2, rid=failing, reason="malformed"))
    await store.create_dead_letter(fields(DL3, rid=pending))
    await store.create_dead_letter(fields(DL4, rid=None))  # no Request: belongs to no tenant
    assert (await store.discard_dead_letter(DL2)).outcome == "accepted"
    expected = twins(await everything(store))
    r = store.client
    for key in [k for k in await r.keys("das:tenant:*") if k in expected]:
        await r.delete(key)
    for dlid in (DL1, DL2, DL3, DL4):
        await r.hdel(keys.dlq(dlid), "tenant")
    assert twins(await everything(store)) == {}
    return expected


async def backfill(store: StateStore, batch: int = 2) -> int:
    done = 0
    async for rids in store.scan_request_index(batch):
        done += await store.tenant_backfill_requests(rids)
    async for dlids in store.scan_dead_letter_index(batch):
        done += await store.tenant_backfill_dead_letters(dlids)
    return done


# =========================================================================== completeness and idempotence


async def test_the_backfill_rebuilds_exactly_what_the_scripts_wrote(state_store: StateStore) -> None:
    expected = await phase_4_data(state_store)
    assert keys.tenant_requests("default") in expected and keys.tenant_dlq_index("default") in expected
    await backfill(state_store)
    assert twins(await everything(state_store)) == expected


async def test_the_batch_size_does_not_change_the_result(state_store: StateStore) -> None:
    expected = await phase_4_data(state_store)
    await backfill(state_store, batch=1000)
    assert twins(await everything(state_store)) == expected


async def test_running_it_again_changes_nothing(state_store: StateStore) -> None:
    await phase_4_data(state_store)
    await backfill(state_store)
    once = await everything(state_store)
    await backfill(state_store)
    assert await everything(state_store) == once


async def test_the_backfill_counts_what_it_filed(state_store: StateStore) -> None:
    await phase_4_data(state_store)
    # 5 Requests and the 3 records that have one; the record without a Request is skipped.
    assert await backfill(state_store) == 5 + 3


# =========================================================================== what it does to each item


async def test_terminal_requests_are_not_active_and_the_others_are(state_store: StateStore) -> None:
    await phase_4_data(state_store)
    await backfill(state_store)
    active = {str(m) for m in await state_store.client.smembers(keys.tenant_active("default"))}
    non_terminal = {
        str(m)
        for status in ("Pending", "Running")
        for m in await state_store.client.zrange(keys.requests_by_status(status), 0, -1)
    }
    assert active == non_terminal and active


async def test_a_dead_letter_record_gets_the_default_tenant_only_with_an_existing_request(
    state_store: StateStore,
) -> None:
    await phase_4_data(state_store)
    await backfill(state_store)
    r = state_store.client
    assert [await r.hget(keys.dlq(d), "tenant") for d in (DL1, DL2, DL3)] == ["default"] * 3
    assert await r.hget(keys.dlq(DL4), "tenant") is None
    assert await r.zscore(keys.tenant_dlq_index("default"), DL4) is None


async def test_a_request_with_a_tenant_field_is_filed_under_it_not_under_default(state_store: StateStore) -> None:
    rid = await submit(state_store, A)
    r = state_store.client
    for key in await r.keys("das:tenant:*"):
        await r.delete(key)
    await backfill(state_store)
    assert await r.zscore(keys.tenant_requests(A), rid) is not None
    assert await r.zscore(keys.tenant_requests("default"), rid) is None
    assert await r.sismember(keys.tenant_active(A), rid)


async def test_an_existing_record_tenant_is_kept(state_store: StateStore) -> None:
    rid = await submit(state_store, A)
    await failed_job(state_store, rid)
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    r = state_store.client
    for key in await r.keys("das:tenant:*"):
        await r.delete(key)
    await backfill(state_store)
    assert await r.hget(keys.dlq(DL1), "tenant") == A  # HSETNX: not overwritten
    assert await r.zscore(keys.tenant_dlq_index(A), DL1) is not None
    assert await r.zscore(keys.tenant_dlq_index("default"), DL1) is None


async def test_a_stale_status_entry_is_removed(state_store: StateStore) -> None:
    rid = await state_store.create_request("x")
    await state_store.cancel_request(rid, prices=None)
    expected = twins(await everything(state_store))
    # An older version moved the Request without touching the twins: it still sits in Pending's twin.
    await state_store.client.zadd(keys.tenant_requests_by_status("default", "Pending"), {rid: 1})
    await backfill(state_store)
    assert twins(await everything(state_store)) == expected


async def test_a_terminal_request_left_in_the_active_set_by_an_older_version_is_removed(
    state_store: StateStore,
) -> None:
    rid = await state_store.create_request("x")
    await state_store.cancel_request(rid, prices=None)
    expected = twins(await everything(state_store))
    # An older version finished it without touching the tenant's active set.
    await state_store.client.sadd(keys.tenant_active("default"), rid)
    await backfill(state_store)
    assert twins(await everything(state_store)) == expected
    assert not await state_store.client.sismember(keys.tenant_active("default"), rid)


async def test_ids_that_are_not_in_the_index_or_no_longer_exist_are_skipped(state_store: StateStore) -> None:
    rid = await state_store.create_request("x")
    assert await state_store.tenant_backfill_requests(["no-such-request"]) == 0
    assert await state_store.tenant_backfill_dead_letters(["f" * 32]) == 0
    r = state_store.client
    await r.delete(keys.REQUESTS_INDEX)  # a Request that left the global index is not filed
    for key in await r.keys("das:tenant:*"):
        await r.delete(key)
    assert await state_store.tenant_backfill_requests([rid]) == 0
    assert twins(await everything(state_store)) == {}


async def test_an_empty_store_backfills_nothing(state_store: StateStore) -> None:
    assert await backfill(state_store) == 0
    assert twins(await everything(state_store)) == {}


# =========================================================================== beside writers


async def test_writers_between_batches_converge_to_the_same_result(state_store: StateStore) -> None:
    first = await state_store.create_request("first")
    second = await state_store.create_request("second")
    third = await state_store.create_request("third")
    r = state_store.client
    for key in await r.keys("das:tenant:*"):
        await r.delete(key)
    await state_store.tenant_backfill_requests([first])
    # A Phase 5 writer finalizes a request the backfill has not seen yet, and one it has.
    await state_store.cancel_request(second, prices=None)
    await state_store.cancel_request(first, prices=None)
    await state_store.tenant_backfill_requests([second, third])
    for status in ("Pending", "Running", "Completed", "Failed", "Cancelled"):
        global_members = {str(m) for m in await r.zrange(keys.requests_by_status(status), 0, -1)}
        twin_members = {str(m) for m in await r.zrange(keys.tenant_requests_by_status("default", status), 0, -1)}
        assert twin_members == global_members, status
    assert {str(m) for m in await r.smembers(keys.tenant_active("default"))} == {third}


async def test_a_request_purged_after_it_was_listed_is_skipped(state_store: StateStore) -> None:
    rid = await state_store.create_request("x")
    batches = [b async for b in state_store.scan_request_index(10)]
    assert batches == [[rid]]
    await state_store.cancel_request(rid, prices=None)
    assert await state_store.purge_request(rid, ttl_ms=0) == "DELETED"
    assert await state_store.tenant_backfill_requests([rid]) == 0
    assert twins(await everything(state_store)) == {}


# =========================================================================== the lock and the flag


async def test_the_backfill_lock_is_not_the_phase_2_lock(state_store: StateStore) -> None:
    assert await state_store.acquire_backfill_lock("m1", 60_000)
    assert await state_store.acquire_tenant_backfill_lock("m1", 60_000)
    assert await state_store.client.exists(keys.REQUESTS_INDEX_BACKFILL_LOCK, keys.TENANT_INDEX_BACKFILL_LOCK) == 2


async def test_the_tenant_lock_is_set_nx_with_an_expiry_and_checked_by_owner(state_store: StateStore) -> None:
    r = state_store.client
    assert await state_store.acquire_tenant_backfill_lock("m1", 60_000)
    assert not await state_store.acquire_tenant_backfill_lock("m2", 60_000)
    assert 0 < await r.pttl(keys.TENANT_INDEX_BACKFILL_LOCK) <= 60_000
    assert not await state_store.renew_tenant_backfill_lock("m2", 90_000)
    assert await state_store.renew_tenant_backfill_lock("m1", 90_000)
    assert await r.pttl(keys.TENANT_INDEX_BACKFILL_LOCK) > 60_000
    assert not await state_store.release_tenant_backfill_lock("m2")
    assert await r.get(keys.TENANT_INDEX_BACKFILL_LOCK) == "m1"
    assert await state_store.release_tenant_backfill_lock("m1")
    assert await r.exists(keys.TENANT_INDEX_BACKFILL_LOCK) == 0
