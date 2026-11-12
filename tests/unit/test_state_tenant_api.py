"""The tenant-aware StateStore API (das-multi-tenant task 3.5; Phase 5 Req 3.3, 3.4, 5.3, 6.3, 7.3, 9.3).

Reads: the snapshot carries tenant and priority; ``list_requests`` and ``list_dead_letters`` take a tenant and read
its twin keys (``None`` is an Operator and reads the global keys); a non-default tenant is refused until the
Tenant_Backfill has set ``das:tenant:index:ready``; ``read_usage`` returns one tenant's rollups by UTC day."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from das.models import RequestStatus
from das.state import ClientRequestSpec, IndexNotReady, StateStore, keys
from tests.unit.test_state_phase2_dlq import DL1, DL2, fields
from tests.unit.test_state_phase2_writes import FINAL, usage
from tests.unit.test_state_tenant_scripts import A, B, FP, running, running_with_failed_job, submit

DL3 = "3" * 32


async def ids(store: StateStore, tenant: str | None, **kw: object) -> list[str]:
    page = await store.list_requests(tenant=tenant, **kw)  # type: ignore[arg-type]
    return [item.request_id for item in page.items]


async def dl_ids(store: StateStore, tenant: str | None, **kw: object) -> list[str]:
    page = await store.list_dead_letters(tenant=tenant, **kw)  # type: ignore[arg-type]
    return [r.dead_letter_id for r in page.records]


# =========================================================================== snapshot


async def test_the_snapshot_carries_the_tenant_and_the_priority(state_store: StateStore) -> None:
    rid = await submit(state_store, A, priority="high")
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and (snap.tenant, snap.priority) == (A, "high")


async def test_a_request_without_the_fields_is_default_and_normal(state_store: StateStore) -> None:
    legacy = await state_store.create_request("hello")
    normal = await submit(state_store, A)
    for rid, tenant in ((legacy, "default"), (normal, A)):
        snap = await state_store.load_snapshot(rid)
        assert snap is not None and (snap.tenant, snap.priority) == (tenant, "normal")


# =========================================================================== list_requests


async def two_tenants(store: StateStore) -> tuple[list[str], list[str]]:
    a = [await submit(store, A) for _ in range(3)]
    b = [await submit(store, B) for _ in range(2)]
    await store.set_tenant_index_ready()
    return a, b


async def test_a_tenant_lists_only_its_own_requests(state_store: StateStore) -> None:
    a, b = await two_tenants(state_store)
    assert sorted(await ids(state_store, A)) == sorted(a)
    assert sorted(await ids(state_store, B)) == sorted(b)
    assert sorted(await ids(state_store, None)) == sorted(a + b)  # an Operator sees every tenant


async def test_the_status_filter_reads_the_tenants_status_sets(state_store: StateStore) -> None:
    ra, la = await running(state_store, A)
    rb, _ = await running(state_store, B)
    await state_store.finalize(la, FINAL)
    await state_store.set_tenant_index_ready()
    assert await ids(state_store, A, statuses=[RequestStatus.COMPLETED]) == [ra]
    assert await ids(state_store, B, statuses=[RequestStatus.COMPLETED]) == []
    assert await ids(state_store, B, statuses=[RequestStatus.RUNNING]) == [rb]
    assert await ids(state_store, A, statuses=[RequestStatus.RUNNING]) == []


async def test_a_tenant_listing_pages_without_gaps_or_leaks(state_store: StateStore) -> None:
    a, b = await two_tenants(state_store)
    first = await state_store.list_requests(tenant=A, limit=2)
    assert len(first.items) == 2 and first.next_after is not None
    second = await state_store.list_requests(tenant=A, limit=2, after=first.next_after)
    paged = [i.request_id for i in first.items + second.items]
    assert sorted(paged) == sorted(a) and second.next_after is None
    assert not set(paged) & set(b)


async def test_a_tenant_with_no_requests_lists_nothing(state_store: StateStore) -> None:
    await two_tenants(state_store)
    assert await ids(state_store, "team-c") == []


async def test_before_the_backfill_a_non_default_tenant_is_refused(state_store: StateStore) -> None:
    await submit(state_store, A)
    with pytest.raises(IndexNotReady):
        await state_store.list_requests(tenant=A)


async def test_before_the_backfill_the_default_tenant_and_an_operator_read_the_global_index(
    state_store: StateStore,
) -> None:
    legacy = await state_store.create_request("old")
    other = await submit(state_store, A)
    assert sorted(await ids(state_store, "default")) == sorted([legacy, other])  # Req 9.3: the global index
    assert sorted(await ids(state_store, None)) == sorted([legacy, other])


async def test_after_the_backfill_the_default_tenant_reads_its_own_twin(state_store: StateStore) -> None:
    legacy = await state_store.create_request("old")
    await submit(state_store, A)
    await state_store.set_tenant_index_ready()
    assert await ids(state_store, "default") == [legacy]


async def test_the_flag_is_read_from_the_store(state_store: StateStore) -> None:
    assert await state_store.tenant_index_ready() is False
    await state_store.set_tenant_index_ready()
    assert await state_store.tenant_index_ready() is True
    assert await state_store.client.get(keys.TENANT_INDEX_READY) == "1"


# =========================================================================== list_dead_letters


async def dead_letters(store: StateStore) -> None:
    ra, _ = await running_with_failed_job(store, A)
    rb, _ = await running_with_failed_job(store, B)
    assert (await store.create_dead_letter(fields(DL1, rid=ra))).created
    assert (await store.create_dead_letter(fields(DL2, rid=rb, reason="malformed"))).created
    assert (await store.create_dead_letter(fields(DL3, rid=None))).created  # no Request: no tenant
    await store.set_tenant_index_ready()


async def test_a_tenant_lists_only_the_dead_letters_of_its_own_requests(state_store: StateStore) -> None:
    await dead_letters(state_store)
    assert await dl_ids(state_store, A) == [DL1]
    assert await dl_ids(state_store, B) == [DL2]


async def test_an_operator_lists_every_dead_letter_including_those_without_a_request(state_store: StateStore) -> None:
    await dead_letters(state_store)
    assert sorted(await dl_ids(state_store, None)) == sorted([DL1, DL2, DL3])


async def test_the_status_and_reason_filters_read_the_tenants_twins(state_store: StateStore) -> None:
    await dead_letters(state_store)
    assert await dl_ids(state_store, A, status="Pending") == [DL1]
    assert await dl_ids(state_store, A, status="Discarded") == []
    assert await dl_ids(state_store, B, reason="malformed") == [DL2]
    assert await dl_ids(state_store, A, reason="malformed") == []


async def test_a_discard_moves_the_record_in_the_tenants_status_listing(state_store: StateStore) -> None:
    await dead_letters(state_store)
    assert (await state_store.discard_dead_letter(DL1)).outcome == "accepted"
    assert await dl_ids(state_store, A, status="Pending") == []
    assert await dl_ids(state_store, A, status="Discarded") == [DL1]


async def test_dead_letter_listing_is_refused_for_a_non_default_tenant_before_the_backfill(
    state_store: StateStore,
) -> None:
    ra, _ = await running_with_failed_job(state_store, A)
    await state_store.create_dead_letter(fields(DL1, rid=ra))
    with pytest.raises(IndexNotReady):
        await state_store.list_dead_letters(tenant=A)
    assert await dl_ids(state_store, None) == [DL1]
    assert await dl_ids(state_store, "default") == [DL1]  # the global index until the flag is set


# =========================================================================== read_usage


async def today(store: StateStore) -> date:
    return datetime.fromtimestamp(await store.server_time_ms() / 1000, UTC).date()


async def test_read_usage_returns_one_tenants_rollup_for_each_requested_day(state_store: StateStore) -> None:
    _, lease = await running(state_store, A, plan_usage=[usage("plan", i=3, o=2)])
    await state_store.finalize(lease, FINAL, usage=[usage("summarize", i=4, o=1)])
    await running(state_store, B)
    d = await today(state_store)
    got = await state_store.read_usage(A, [d])
    assert list(got) == [d]
    assert got[d]["submitted"] == "1" and got[d]["finished/Completed"] == "1"
    assert sum(int(v) for k, v in got[d].items() if k.startswith("tokens/in/")) == 7
    other = await state_store.read_usage(B, [d])
    assert other[d]["submitted"] == "1" and "finished/Completed" not in other[d]


async def test_read_usage_gives_an_empty_mapping_for_a_day_without_a_rollup(state_store: StateStore) -> None:
    await submit(state_store, A)
    d = await today(state_store)
    earlier = date.fromordinal(d.toordinal() - 3)
    got = await state_store.read_usage(A, [earlier, d])
    assert got[earlier] == {} and got[d]["submitted"] == "1"
    assert await state_store.read_usage("team-c", [d]) == {d: {}}


async def test_read_usage_keeps_the_order_of_the_days(state_store: StateStore) -> None:
    await submit(state_store, A)
    d = await today(state_store)
    days = [date.fromordinal(d.toordinal() - n) for n in range(6, -1, -1)]
    assert list(await state_store.read_usage(A, days)) == days


async def test_read_usage_of_no_days_is_empty(state_store: StateStore) -> None:
    assert await state_store.read_usage(A, []) == {}


async def test_request_tenant_is_the_tenant_none_for_a_missing_request_and_default_without_a_field(
    state_store: StateStore,
) -> None:
    named = await submit(state_store, A)
    legacy = await state_store.create_request("no tenant field")
    assert await state_store.request_tenant(named) == A
    assert await state_store.request_tenant(legacy) == "default"  # absent field: the default tenant
    assert await state_store.request_tenant("no-such-request") is None  # not "default": there is no such Request


async def test_dead_letter_tenant_is_the_records_tenant_and_none_for_no_tenant_or_no_record(
    state_store: StateStore,
) -> None:
    ra, _ = await running_with_failed_job(state_store, A)
    await state_store.create_dead_letter(fields(DL1, rid=ra))
    await state_store.create_dead_letter(fields(DL2, rid=None))
    assert await state_store.dead_letter_tenant(DL1) == A
    assert await state_store.dead_letter_tenant(DL2) is None  # a record of no tenant
    assert await state_store.dead_letter_tenant("f" * 32) is None  # no such record


async def test_the_submit_spec_refuses_an_invalid_tenant() -> None:
    with pytest.raises(ValueError):
        ClientRequestSpec(fingerprint=FP, tenant="Bad Tenant")
