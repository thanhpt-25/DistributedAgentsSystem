"""Phase 4 data under Phase 5 code: the upgrade (das-multi-tenant task 8.2; Phase 5 Req 9.1, 9.2, 9.3, 9.4).

The data is what a Phase 4 fleet leaves in Redis: Requests and Dead_Letter_Records with the global indexes, no
``tenant`` field and no ``das:tenant:*`` key. (It is built, as in the backfill tests, by letting the Phase 5 scripts
write it and removing everything Phase 5 added.) The Phase 5 Gateway and Manager then run over it, through the real
``TenantIndexBackfill`` and the real Gateway, in the order an upgrade goes."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from das.clock import FakeClock
from das.config.schema import TenantConfig
from das.gateway.app import create_app
from das.manager.retention import TenantIndexBackfill
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import IndexNotReady, StateStore, keys
from tests.unit.test_state_phase2_dlq import DL1, DL2, everything
from tests.unit.test_state_tenant_backfill import DL3, DL4, phase_4_data

DEFAULT, A, OP = "default-key-00000001", "team-a-key-000000001", "root-key-0000000001"
TENANT_NAMESPACE = "das:tenant:"
#: Fields the Phase 5 scripts add to a hash and a Phase 4 reader never looks at.
PHASE_5_FIELDS = ("tenant", "priority")


def h(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


async def phase_4_store(store: StateStore) -> set[str]:
    """Phase 4 data with every Phase 5 key gone (usage rollups too: Phase 4 never wrote any), and its Request ids."""
    await phase_4_data(store)
    for key in await store.client.keys(f"{TENANT_NAMESPACE}*"):
        await store.client.delete(key)
    await store.set_index_ready()  # the Phase 2 index is done, as on any Phase 4 fleet
    page = await store.list_requests(tenant=None, limit=100)
    assert await store.client.keys(f"{TENANT_NAMESPACE}*") == []
    return {item.request_id for item in page.items}


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    team_a = TenantConfig.model_validate(
        {"id": "team-a", "keys": [{"fingerprint": key_fingerprint(A.encode()), "scopes": ["submit", "read"]}]}
    )
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [DEFAULT, A], 1000, admin_api_keys=[OP], tenants=[team_a]
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        yield client


async def listed(gw: httpx.AsyncClient, key: str) -> tuple[int, set[str]]:
    """The status of the listing and every Request id over all its pages."""
    seen: set[str] = set()
    cursor: str | None = None
    for _ in range(20):
        params: dict[str, Any] = {"limit": 2, **({"cursor": cursor} if cursor else {})}
        reply = await gw.get("/v1/requests", params=params, headers=h(key))
        if reply.status_code != 200:
            return reply.status_code, seen
        seen |= {item["request_id"] for item in reply.json()["items"]}
        cursor = reply.json().get("next_cursor")
        if cursor is None:
            return 200, seen
    raise AssertionError("a listing did not end")


async def upgrade(store: StateStore) -> None:
    """The Manager's Tenant_Backfill, to completion: it files every old Request and sets the flag."""
    await asyncio.wait_for(TenantIndexBackfill(store, FakeClock(), "manager-1").run(), 5)
    assert await store.tenant_index_ready()


# --------------------------------------------------------------------------- Req 9.3: before the flag


async def test_before_the_flag_the_default_tenant_reads_the_global_index_and_others_are_refused(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    old = await phase_4_store(state_store)
    assert len(old) == 5
    assert not await state_store.tenant_index_ready()
    assert await listed(gw, DEFAULT) == (200, old)  # correct, although no twin exists yet
    assert await listed(gw, OP) == (200, old)
    reply = await gw.get("/v1/requests", headers=h(A))
    assert (reply.status_code, reply.json()) == (503, {"error": "index_not_ready"})


# --------------------------------------------------------------------------- Req 9.2, 9.3: after the backfill


async def test_after_the_backfill_old_requests_belong_to_the_default_tenant(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    old = await phase_4_store(state_store)
    await upgrade(state_store)
    assert await listed(gw, DEFAULT) == (200, old)  # now read from the tenant's own twin, and the same
    assert await listed(gw, OP) == (200, old)
    assert await listed(gw, A) == (200, set())  # a configured tenant starts empty: nothing old is its own
    for rid in old:  # and an old Request is still the default tenant's to read
        assert (await gw.get(f"/v1/requests/{rid}", headers=h(DEFAULT))).status_code == 200
        assert (await gw.get(f"/v1/requests/{rid}", headers=h(A))).status_code == 404


async def test_new_requests_are_filed_under_their_tenant_and_old_ones_stay_default(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    old = await phase_4_store(state_store)
    await upgrade(state_store)
    created = (await gw.post("/v1/requests", json={"prompt": "new"}, headers=h(A))).json()["request_id"]
    assert await listed(gw, A) == (200, {created})
    assert await listed(gw, DEFAULT) == (200, old)  # the tenant's Request is not in the default tenant's list
    assert await listed(gw, OP) == (200, old | {created})


# --------------------------------------------------------------------------- Req 9.1, 9.2: writers beside the upgrade


async def test_writes_between_the_upgrade_and_the_backfill_end_up_in_the_right_indexes(
    state_store: StateStore,
) -> None:
    """The new release already writes tenant keys for every Request it handles (Req 9.1) while the backfill has not
    run; the backfill then reconciles (Req 9.2): each tenant index equals the global one it mirrors."""
    old = await phase_4_store(state_store)
    victim = sorted(old)[0]
    await state_store.cancel_request(victim, prices=None)  # an old Request, changed by the new scripts
    fresh = await state_store.create_request("created after the upgrade, before the backfill")
    await upgrade(state_store)

    r = state_store.client
    members = set(await r.zrange(keys.tenant_requests("default"), 0, -1))
    assert members == old | {fresh}  # every old Request and the new one, treated as default (Req 9.1)
    assert set(await r.zrange(keys.REQUESTS_INDEX, 0, -1)) == members
    for status in ("Pending", "Running", "Completed", "Failed", "Cancelled"):
        twin = set(await r.zrange(keys.tenant_requests_by_status("default", status), 0, -1))
        assert twin == set(await r.zrange(keys.requests_by_status(status), 0, -1)), status
    assert victim in await r.zrange(keys.tenant_requests_by_status("default", "Cancelled"), 0, -1)
    active = await r.smembers(keys.tenant_active("default"))  # the Tenant_Active_Set: non-terminal Requests
    assert victim not in active and fresh in active  # the cancelled one left it, the new one is in


async def test_dead_letters_follow_the_same_rule_across_the_upgrade(state_store: StateStore) -> None:
    await phase_4_store(state_store)

    async def listed_records(tenant: str | None) -> set[str]:
        return {r.dead_letter_id for r in (await state_store.list_dead_letters(tenant=tenant, limit=100)).records}

    everything_there = {DL1, DL2, DL3, DL4}
    assert await listed_records("default") == everything_there  # before the flag: the global index
    with pytest.raises(IndexNotReady):
        await listed_records("team-a")
    await upgrade(state_store)
    # A record belongs to the tenant of its Request; DL4 has none, so it is in no tenant's index (and the Operator's).
    assert await listed_records("default") == {DL1, DL2, DL3}
    assert await listed_records(None) == everything_there
    assert await listed_records("team-a") == set()


async def test_the_backfill_run_twice_leaves_the_indexes_as_one_run_does(state_store: StateStore) -> None:
    await phase_4_store(state_store)
    await upgrade(state_store)
    once = await everything(state_store)
    await upgrade(state_store)
    assert await everything(state_store) == once


# --------------------------------------------------------------------------- Req 9.4: Phase 4 readers and rollback


def seen_by_phase_4(snapshot: dict[str, Any]) -> dict[str, Any]:
    """What a Phase 4 reader can see: every key outside the tenant namespace, without the fields the new scripts add."""
    out: dict[str, Any] = {}
    for key, value in snapshot.items():
        if key.startswith(TENANT_NAMESPACE):
            continue
        out[key] = {k: v for k, v in value.items() if k not in PHASE_5_FIELDS} if isinstance(value, dict) else value
    return out


async def test_the_backfill_changes_nothing_a_phase_4_reader_reads(state_store: StateStore) -> None:
    await phase_4_store(state_store)
    before = seen_by_phase_4(await everything(state_store))
    await upgrade(state_store)
    assert seen_by_phase_4(await everything(state_store)) == before  # it only adds tenant keys (and fields)


async def test_a_phase_4_reader_gets_the_same_answers_with_every_tenant_key_gone(state_store: StateStore) -> None:
    """Rollback: the Phase 4 code ignores ``das:tenant:*``, so removing it all changes nothing it reads."""
    await phase_4_store(state_store)
    await upgrade(state_store)
    await state_store.create_request("after the upgrade")
    page = await state_store.list_requests(tenant=None, limit=100)  # the global index, as Phase 4 reads it
    snapshots = {i.request_id: await state_store.load_snapshot(i.request_id) for i in page.items}
    assert len(snapshots) == 6
    for key in await state_store.client.keys(f"{TENANT_NAMESPACE}*"):
        await state_store.client.delete(key)
    again = await state_store.list_requests(tenant=None, limit=100)
    assert [i.request_id for i in again.items] == [i.request_id for i in page.items]
    for rid, snap in snapshots.items():
        assert await state_store.load_snapshot(rid) == snap
