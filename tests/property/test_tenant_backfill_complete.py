# Feature: das-multi-tenant, Property 7: Tenant_Backfill is idempotent and complete
"""Property 7: Tenant_Backfill is idempotent and complete.

For any Phase 4 data set (Requests with and without ``client_fp``, in any status, and Dead_Letter_Records with and
without an existing Request) and any interleaving of the backfill batches with writers, once the backfill reports ready
the default tenant's twins equal what the Phase 5 scripts would have written, running the backfill again changes
nothing, and records without a Request stay out of every twin.

Two Worlds receive the same operations. ``reference`` is Phase 5 throughout. ``subject`` has the same data set stripped
of everything Phase 5 added, as Phase 4 left it, then takes the same later operations (the writers) while the backfill
works through it a batch at a time. At the end they must hold the same keys. The one difference is by design: a record
whose Request was purged before the backfill reached it has no tenant, so it is in no twin (Req 5.2).

**Validates: Requirements 5.1, 5.2, 5.5, 9.1, 9.2**
"""

from __future__ import annotations

import asyncio
import itertools
from typing import Any

from hypothesis import example, given
from hypothesis import strategies as st

from das.state import StateStore, keys
from tests.fixtures.redis_time import FakeRedisTime
from tests.property.tenant_ops import EVERY_KIND, START_MS, World, steady_operations
from tests.unit.test_state_phase2_dlq import everything

Op = tuple[Any, ...]
STEADY_EVERY_KIND = [op for op in EVERY_KIND if op[0] != "tick"]


def counter() -> Any:
    ids = itertools.count(1)
    return lambda: f"req-{next(ids):06d}"


async def strip_to_phase_4(store: StateStore) -> None:
    """Remove everything Phase 5 added to the keys the backfill rebuilds: the twins, and the dead letters' tenant."""
    r = store.client
    for key in await r.keys("das:tenant:*"):
        if ":usage:" not in key and key != keys.TENANT_USAGE_RETENTION:
            await r.delete(key)
    for dlid in await r.zrange(keys.DLQ_INDEX, 0, -1):
        await r.hdel(keys.dlq(dlid), "tenant")


async def work_items(store: StateStore, batch: int) -> list[tuple[str, list[str]]]:
    """The backfill's batches over the global indexes as they are now, Requests first."""
    return [("requests", b) async for b in store.scan_request_index(batch)] + [
        ("dlq", b) async for b in store.scan_dead_letter_index(batch)
    ]


async def run_item(store: StateStore, item: tuple[str, list[str]]) -> None:
    kind, ids = item
    if kind == "requests":
        await store.tenant_backfill_requests(ids)
    else:
        await store.tenant_backfill_dead_letters(ids)


#: Streams carry entry ids taken from the wall clock, which differ between two Worlds; the backfill never touches them.
STREAM_SUFFIXES = (":inbox", ":events")


def without_orphans(snapshot: dict[str, Any], orphans: set[str]) -> dict[str, Any]:
    """The reference with the records that have no Request filed under no tenant, as the backfill leaves them."""
    out: dict[str, Any] = {}
    for key, value in snapshot.items():
        if key.endswith(STREAM_SUFFIXES):
            continue
        if key.startswith("das:tenant:") and ":dlq:" in key:
            value = [(member, score) for member, score in value if member not in orphans]
            if not value:
                continue
        elif key.startswith("das:dlq:") and key.rsplit(":", 1)[-1] in orphans:
            value = {k: v for k, v in value.items() if k != "tenant"}
        out[key] = value
    return out


async def run(before: list[Op], during: list[Op], batch: int) -> None:
    with FakeRedisTime() as clock:
        clock.set_ms(START_MS)
        reference = World(clock, phase4=True, ids=counter())
        subject = World(clock, phase4=True, ids=counter())
        for op in before:  # the Phase 4 data set
            await reference.apply(*op)
            await subject.apply(*op)
        await strip_to_phase_4(subject.store)
        # Records whose Request is already gone belong to no tenant, whatever the backfill does (known independently of
        # its result, so a backfill that files them anyway cannot hide itself).
        gone = {
            str(dlid)
            for dlid in await subject.store.client.zrange(keys.DLQ_INDEX, 0, -1)
            if not await subject.store.client.exists(
                keys.request(await subject.store.client.hget(keys.dlq(dlid), "request_id"))
            )
        }

        pending = await work_items(subject.store, batch)
        for op in during:  # writers beside the backfill: one batch after each operation
            await reference.apply(*op)
            await subject.apply(*op)
            if pending:
                await run_item(subject.store, pending.pop(0))
        for item in pending:
            await run_item(subject.store, item)

        r = subject.store.client
        orphans = {
            str(dlid)
            for dlid in await r.zrange(keys.DLQ_INDEX, 0, -1)
            if await r.hget(keys.dlq(dlid), "tenant") is None
        }
        assert gone <= orphans, gone - orphans  # none of them was given a tenant
        for dlid in orphans:  # records without a Request stay out of every twin
            request_id = await r.hget(keys.dlq(dlid), "request_id")
            assert request_id and await r.exists(keys.request(request_id)) == 0, dlid
            for twin in await r.keys("das:tenant:*:dlq:*"):
                assert await r.zscore(twin, dlid) is None, (twin, dlid)

        done = without_orphans(await everything(subject.store), set())  # streams left out, nothing else changed
        assert done == without_orphans(await everything(reference.store), orphans)

        for item in await work_items(subject.store, batch):  # running it again changes nothing
            await run_item(subject.store, item)
        assert without_orphans(await everything(subject.store), set()) == done


@example(STEADY_EVERY_KIND, STEADY_EVERY_KIND, 2)
@given(steady_operations, steady_operations, st.integers(1, 6))
def test_the_backfill_is_complete_and_idempotent_beside_writers(before: list[Op], during: list[Op], batch: int) -> None:
    asyncio.run(run(before, during, batch))
