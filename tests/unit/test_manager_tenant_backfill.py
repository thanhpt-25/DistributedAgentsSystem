"""TenantIndexBackfill, the Manager task that fills the tenant indexes (das-multi-tenant task 3.8; Phase 5 Req 9.2).

It follows the Phase 2 ``IndexBackfill``: a renewed ``SET NX PX`` lock, one batched pass, then the ready flag. It
starts only once the Phase 2 Request_Index is ready, because it copies from the global index."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from das.clock import FakeClock
from das.manager import retention
from das.manager.retention import TenantIndexBackfill
from das.state import StateStore, keys
from tests.unit.test_manager_retention import ListLog, _client
from tests.unit.test_state_phase2_dlq import DL1, everything, fields
from tests.unit.test_state_tenant_backfill import failed_job, twins


@pytest.fixture
def log(monkeypatch: pytest.MonkeyPatch) -> ListLog:
    fake = ListLog()
    monkeypatch.setattr(retention, "log", fake)
    return fake


async def phase_4_store(fake_redis_time: Any) -> tuple[StateStore, dict[str, Any]]:
    """A store with Phase 4 data: two Requests, one with a dead letter, no tenant twins."""
    store = StateStore(await _client(fake_redis_time))
    await store.set_index_ready()  # the Phase 2 index is done
    first = await store.create_request("one")
    failing = await store.create_request("two")
    await failed_job(store, failing)
    await store.create_dead_letter(fields(DL1, rid=failing))
    expected = twins(await everything(store))
    for key in [k for k in await store.client.keys("das:tenant:*") if k in expected]:
        await store.client.delete(key)
    await store.client.hdel(keys.dlq(DL1), "tenant")
    assert first and twins(await everything(store)) == {}
    return store, expected


async def test_it_files_requests_and_dead_letters_and_sets_the_flag(fake_redis_time: Any, log: ListLog) -> None:
    store, expected = await phase_4_store(fake_redis_time)
    await asyncio.wait_for(TenantIndexBackfill(store, FakeClock(), "manager-1").run(), 2)
    assert await store.tenant_index_ready()
    assert twins(await everything(store)) == expected
    assert await store.client.exists(keys.TENANT_INDEX_BACKFILL_LOCK) == 0  # released
    assert [event for event, _ in log.infos] == ["tenant_index_backfilled"]


async def test_it_does_not_start_before_the_phase_2_index_is_ready(fake_redis_time: Any, log: ListLog) -> None:
    store = StateStore(await _client(fake_redis_time))
    await store.create_request("v1")
    backfill = TenantIndexBackfill(store, FakeClock(), "manager-1")
    assert not await backfill.run_once()
    assert not await store.tenant_index_ready()
    assert await store.client.exists(keys.TENANT_INDEX_BACKFILL_LOCK) == 0  # it did not even take the lock
    await store.set_index_ready()
    assert await backfill.run_once()
    assert await store.tenant_index_ready()


async def test_it_does_nothing_once_ready(fake_redis_time: Any, log: ListLog) -> None:
    store, _ = await phase_4_store(fake_redis_time)
    await store.set_tenant_index_ready()
    assert await TenantIndexBackfill(store, FakeClock(), "manager-1").run_once()
    assert twins(await everything(store)) == {}  # nothing was filed
    assert log.infos == []


async def test_it_waits_while_another_agent_holds_the_lock(fake_redis_time: Any, log: ListLog) -> None:
    store, _ = await phase_4_store(fake_redis_time)
    assert await store.acquire_tenant_backfill_lock("other", 60_000)
    backfill = TenantIndexBackfill(store, FakeClock(), "manager-1")
    assert not await backfill.run_once()
    assert not await store.tenant_index_ready() and twins(await everything(store)) == {}

    task = asyncio.create_task(backfill.run())
    await asyncio.sleep(0)
    await store.set_tenant_index_ready()  # the other agent finished
    await asyncio.wait_for(task, 2)
    assert twins(await everything(store)) == {}
    assert await store.client.get(keys.TENANT_INDEX_BACKFILL_LOCK) == "other"  # not ours to release


class SlowStore:
    """Delegates to a StateStore; each Request batch takes 15 s of clock time."""

    def __init__(self, store: StateStore, clock: FakeClock) -> None:
        self._store = store
        self._clock = clock
        self.renewals: list[tuple[str, int]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)

    async def tenant_backfill_requests(self, rids: Any) -> int:
        for _ in range(15):
            await asyncio.sleep(0)
            self._clock.advance(1)
        return await self._store.tenant_backfill_requests(rids)

    async def renew_tenant_backfill_lock(self, owner: str, ttl_ms: int) -> bool:
        self.renewals.append((owner, ttl_ms))
        return await self._store.renew_tenant_backfill_lock(owner, ttl_ms)


async def test_it_renews_its_own_lock_every_20_seconds(fake_redis_time: Any, log: ListLog) -> None:
    store, _ = await phase_4_store(fake_redis_time)
    clock = FakeClock()
    slow = SlowStore(store, clock)
    assert await TenantIndexBackfill(slow, clock, "manager-1").run_once()  # type: ignore[arg-type]
    assert slow.renewals and all(x == ("manager-1", 60_000) for x in slow.renewals)
    assert 20.0 in clock.sleeps
    assert await store.tenant_index_ready()


class FlakyStore:
    """The first Request batch fails, as a lost Redis connection would."""

    def __init__(self, store: StateStore) -> None:
        self._store = store
        self.failures = 1

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)

    async def tenant_backfill_requests(self, rids: Any) -> int:
        if self.failures:
            self.failures -= 1
            raise ConnectionError("redis went away")
        return await self._store.tenant_backfill_requests(rids)


async def test_a_store_error_is_logged_and_the_next_attempt_finishes_the_job(
    fake_redis_time: Any, log: ListLog
) -> None:
    store, expected = await phase_4_store(fake_redis_time)
    backfill = TenantIndexBackfill(FlakyStore(store), FakeClock(), "manager-1")  # type: ignore[arg-type]
    await asyncio.wait_for(backfill.run(), 2)
    assert await store.tenant_index_ready()
    assert twins(await everything(store)) == expected
    assert [event for event, _ in log.warnings] == ["tenant_index_backfill_failed"]
    assert await store.client.exists(keys.TENANT_INDEX_BACKFILL_LOCK) == 0  # released after the failure too
