# Feature: das-multi-tenant, Property 1: Tenant isolation
"""Property 1: Tenant isolation.

For any sequence of Request and Dead_Letter operations (the model of :mod:`tests.property.tenant_ops`, run against the
real State_Store) interleaved with calls through the real Gateway by Principals of two tenants, the default tenant and
an Operator, no Principal of tenant ``t`` observes a Request_ID, field, count, cursor position or Dead_Letter_Record of
another tenant. A cursor issued to one tenant is rejected for another. Every cross-tenant access answers 404 with the
body of a missing resource, and a refused mutation leaves the whole store as it was.

Each call is checked against the model of who owns what: get, events, cancel, the listing (paged, so every page is
seen), a cursor taken to another scope, the Dead_Letter list, get, replay and discard, and the usage report. Whatever a
call returns is also scanned for the ids of Requests and records the caller may not see.

**Validates: Requirements 3.2, 3.3, 3.4, 3.6, 5.3**
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from hypothesis import example, given
from hypothesis import strategies as st

from das.config.schema import ClientLimitsConfig, ClientSettingsConfig, TenantConfig
from das.gateway.app import create_app
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from tests.fixtures.redis_time import FakeRedisTime
from tests.property.tenant_ops import KINDS, START_MS, TENANTS, Rec, World
from tests.unit.test_state_phase2_dlq import everything

NOT_FOUND = {"error": "not_found"}
BAD_CURSOR = {"error": "invalid_query", "parameter": "cursor"}
UNKNOWN_DLID = "f" * 32
PAGE = 2  # small, so a listing of a few Requests spans several pages
MAX_PAGES = 40
WINDOW_DAYS = 7  # the default window of the usage report


@dataclass(frozen=True)
class Who:
    key: str
    tenant: str
    operator: bool
    admin: bool

    @property
    def scope(self) -> str:
        """What a listing cursor of this Principal is bound to: its tenant, or every tenant."""
        return "*" if self.operator else self.tenant

    def sees(self, tenant: str) -> bool:
        return self.operator or self.tenant == tenant


TEAM_A = Who("team-a-key-000000001", "team-a", False, True)
TEAM_B = Who("team-b-key-000000001", "team-b", False, True)
DEFAULT = Who("default-key-00000001", "default", False, False)  # an API_Key of the Default_Tenant: no admin Scope
OPERATOR = Who("root-key-0000000001", "default", True, True)
PRINCIPALS = (TEAM_A, TEAM_B, DEFAULT, OPERATOR)


def tenant_config(tenant: str, who: Who) -> TenantConfig:
    keys = [{"fingerprint": key_fingerprint(who.key.encode()), "scopes": ["submit", "read", "admin"]}]
    return TenantConfig.model_validate({"id": tenant, "keys": keys})


def headers(who: Who) -> dict[str, str]:
    return {"X-API-Key": who.key}


class Harness:
    """The real Gateway over the world's store, and the checks every call must pass."""

    def __init__(self, world: World, client: httpx.AsyncClient) -> None:
        self.world, self.client = world, client

    # ------------------------------------------------------------------ what the model says

    def request_for(self, target: int) -> tuple[str, Rec | None]:
        """A Request the world made (or an unknown id for the last slot), and its record."""
        recs = self.world.recs
        index = target % (len(recs) + 1)
        return (recs[index].rid, recs[index]) if index < len(recs) else (uuid.uuid4().hex, None)

    def dead_letters(self) -> list[tuple[str, Rec]]:
        """Every record ever filed with the Request it belongs to (a replayed Request that fails again has two)."""
        return [(dlid, rec) for rec in self.world.recs for dlid in rec.dlids]

    def dead_letter_for(self, target: int) -> tuple[str, Rec | None]:
        owned = self.dead_letters()
        index = target % (len(owned) + 1)
        return owned[index] if index < len(owned) else (UNKNOWN_DLID, None)

    def foreign_ids(self, who: Who) -> set[str]:
        out: set[str] = set()
        for rec in self.world.recs:
            if not who.sees(rec.tenant):
                out.update({rec.rid, *rec.dlids})
        return out

    def leak_check(self, who: Who, reply: httpx.Response) -> None:
        for leaked in self.foreign_ids(who):
            assert leaked not in reply.text, f"{who.tenant} was shown {leaked}: {reply.request.url}"

    async def call(self, who: Who, method: str, url: str, **kw: Any) -> httpx.Response:
        reply = await self.client.request(method, url, headers=headers(who), **kw)
        self.leak_check(who, reply)
        return reply

    # ------------------------------------------------------------------ the probes

    async def get(self, who: Who, target: int) -> None:
        rid, rec = self.request_for(target)
        reply = await self.call(who, "GET", f"/v1/requests/{rid}")
        if rec is not None and rec.status != "Purged" and who.sees(rec.tenant):
            assert reply.status_code == 200 and reply.json()["request_id"] == rid
        else:
            assert (reply.status_code, reply.json()) == (404, NOT_FOUND)  # foreign, purged and unknown look alike

    async def events(self, who: Who, target: int) -> None:
        rid, rec = self.request_for(target)
        if rec is not None and rec.status != "Purged" and who.sees(rec.tenant):
            return  # a stream of one's own Request does not end by itself
        reply = await self.call(who, "GET", f"/v1/requests/{rid}/events")
        assert (reply.status_code, reply.json()) == (404, NOT_FOUND)

    async def cancel(self, who: Who, target: int) -> None:
        rid, rec = self.request_for(target)
        if rec is not None and who.sees(rec.tenant):
            return await self.get(who, target)  # cancelling one's own Request would desynchronise the model
        before = await everything(self.world.store)
        reply = await self.call(who, "POST", f"/v1/requests/{rid}/cancel")
        assert (reply.status_code, reply.json()) == (404, NOT_FOUND)
        assert await everything(self.world.store) == before  # nothing was written for the foreign caller

    async def pages(self, who: Who, path: str, item_key: str) -> list[str]:
        """Every page of a listing, followed by its cursors, as the list of ``item_key`` values."""
        seen: list[str] = []
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            params: dict[str, Any] = {"limit": PAGE, **({"cursor": cursor} if cursor else {})}
            reply = await self.call(who, "GET", path, params=params)
            assert reply.status_code == 200, (who.tenant, str(reply.request.url), reply.text)
            body = reply.json()
            seen.extend(item[item_key] for item in body["items"])
            cursor = body.get("next_cursor")
            if cursor is None:
                return seen
        raise AssertionError("a listing did not end")

    async def listing(self, who: Who) -> None:
        seen = await self.pages(who, "/v1/requests", "request_id")
        expected = {r.rid for r in self.world.recs if r.status != "Purged" and who.sees(r.tenant)}
        assert len(seen) == len(set(seen)) and set(seen) == expected  # no gap, no repeat, no foreign Request

    async def cursor(self, who: Who, other: Who, path: str = "/v1/requests") -> None:
        if path != "/v1/requests" and not (who.admin and other.admin):
            return  # the Dead_Letter routes need the admin Scope to be reached at all
        first = await self.call(who, "GET", path, params={"limit": 1})
        cursor = first.json().get("next_cursor")
        if cursor is None:
            return  # fewer than two items visible: nothing to page past
        reply = await self.call(other, "GET", path, params={"limit": 1, "cursor": cursor})
        if other.scope == who.scope:
            assert reply.status_code == 200
        else:
            assert (reply.status_code, reply.json()) == (400, BAD_CURSOR)  # a position of another scope is refused

    async def submit(self, who: Who) -> None:
        """A Request created through the Gateway belongs to the tenant of the key that submitted it."""
        reply = await self.call(who, "POST", "/v1/requests", json={"prompt": "from the gateway"})
        assert reply.status_code == 202, reply.text
        self.world.adopt(reply.json()["request_id"], who.tenant)

    async def dlq_list(self, who: Who) -> None:
        if not who.admin:
            reply = await self.call(who, "GET", "/v1/admin/dlq")
            assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": "admin"})
            return
        seen = await self.pages(who, "/v1/admin/dlq", "dead_letter_id")
        expected = {dlid for dlid, rec in self.dead_letters() if who.sees(rec.tenant)}  # a record outlives its Request
        assert len(seen) == len(set(seen)) and set(seen) == expected  # exactly its own records, never a foreign one

    async def dlq_item(self, who: Who, target: int, action: str) -> None:
        dlid, rec = self.dead_letter_for(target)
        if not who.admin:
            return
        if rec is not None and who.sees(rec.tenant):
            if action == "get":
                reply = await self.call(who, "GET", f"/v1/admin/dlq/{dlid}")
                assert reply.status_code == 200 and reply.json()["dead_letter_id"] == dlid
            return  # replay and discard of one's own record would desynchronise the model
        before = await everything(self.world.store)
        method, suffix = ("GET", "") if action == "get" else ("POST", f"/{action}")
        reply = await self.call(who, method, f"/v1/admin/dlq/{dlid}{suffix}")
        assert (reply.status_code, reply.json()) == (404, NOT_FOUND)
        assert await everything(self.world.store) == before

    async def usage(self, who: Who) -> None:
        reply = await self.call(who, "GET", "/v1/usage")
        assert reply.status_code == 200, reply.text
        report = reply.json()
        assert report["tenant"] == who.tenant  # an Operator's own tenant is the default one
        today = datetime.fromtimestamp(self.world.clock.now(), UTC).date()
        days = [(today - timedelta(days=n)).strftime("%Y%m%d") for n in range(WINDOW_DAYS)]
        expected = self.world.expected

        def total(name: str) -> int:
            return sum(expected[(who.tenant, day, name)] for day in days)

        requests = report["totals"]["requests"]
        assert requests["submitted"] == total("submitted")
        for status in ("Completed", "Failed", "Cancelled"):
            assert requests["finished"][status] == total(f"finished/{status}")


PROBES = (
    "submit", "get", "events", "cancel", "list", "cursor", "dlq_list", "dlq_cursor", "dlq_get", "dlq_replay",
    "dlq_discard", "usage",
)  # fmt: skip


async def probe(harness: Harness, what: str, p: int, target: int) -> None:
    who = PRINCIPALS[p % len(PRINCIPALS)]
    if what in ("cursor", "dlq_cursor"):
        path = "/v1/requests" if what == "cursor" else "/v1/admin/dlq"
        return await harness.cursor(who, PRINCIPALS[target % len(PRINCIPALS)], path)
    if what.startswith("dlq_") and what != "dlq_list":
        return await harness.dlq_item(who, target, what.removeprefix("dlq_"))
    handlers = {
        "submit": lambda: harness.submit(who),
        "get": lambda: harness.get(who, target), "events": lambda: harness.events(who, target),
        "cancel": lambda: harness.cancel(who, target), "list": lambda: harness.listing(who),
        "dlq_list": lambda: harness.dlq_list(who), "usage": lambda: harness.usage(who),
    }  # fmt: skip
    await handlers[what]()


async def run(ops: list[tuple[Any, ...]]) -> None:
    with FakeRedisTime() as clock:
        clock.set_ms(START_MS)
        world = World(clock)
        await world.store.set_index_ready()  # a fresh store: both backfills are trivially complete, and every
        await world.store.set_tenant_index_ready()  # Request below is indexed by its tenant as it is created
        app = create_app(
            world.store, AgentRegistry(world.store.client), None,
            [TEAM_A.key, TEAM_B.key, DEFAULT.key], 10_000,
            admin_api_keys=[OPERATOR.key],
            tenants=[tenant_config("team-a", TEAM_A), tenant_config("team-b", TEAM_B)],
            client_limits=ClientLimitsConfig(default=ClientSettingsConfig(rate=0)),  # never throttle the probes
        )  # fmt: skip
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
            harness = Harness(world, client)
            for op in ops:
                if op[0] == "world":
                    await world.apply(*op[1:])
                else:
                    await probe(harness, *op[1:])


world_op = st.tuples(
    st.just("world"), st.sampled_from(KINDS), st.integers(0, 7), st.sampled_from(TENANTS),
    st.integers(1, 5), st.integers(1, 4),
)  # fmt: skip
probe_op = st.tuples(st.just("probe"), st.sampled_from(PROBES), st.integers(0, 3), st.integers(0, 9))
operations = st.lists(st.one_of(world_op, probe_op), min_size=1, max_size=30)

#: Requests of all three tenants with a dead letter each, then every probe by every Principal at several targets: a
#: sequence the property always runs, so a hole in one check cannot hide behind the luck of the random draw.
EVERY_PROBE = [
    *[("world", "create", i, TENANTS[i % 3], 1, 1) for i in range(6)],
    *[("world", "fail_dlq", i, TENANTS[i % 3], 1 + i % 5, 1 + i % 4) for i in range(3)],
    *[("probe", what, p, t) for what in PROBES for p in range(len(PRINCIPALS)) for t in range(0, 9, 2)],
]


@example(EVERY_PROBE)
@given(operations)
def test_no_principal_observes_another_tenants_requests_records_counts_or_cursors(ops: list[tuple[Any, ...]]) -> None:
    asyncio.run(run(ops))
