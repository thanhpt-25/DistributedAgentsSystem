# Feature: das-observability-operations, Property 27: Phân trang danh sách Request đầy đủ
"""Property 27: complete pagination of ``GET /v1/requests``.

For any set of Requests (up to 300, with duplicate ``created_at`` ms) whose status and
existence do not change while it is walked, any valid filter (``status``,
``created_after`` inclusive, ``created_before`` exclusive) and any ``limit`` from 1 to
100 (or the default 20):

- concatenating the pages obtained by following ``next_cursor`` from the first page
  yields exactly the reference list: filter, then sort by (created_at desc,
  Request_ID desc), every Request exactly once; every page holds at most ``limit``
  items and ``next_cursor`` is present iff matching Requests remain after the page;
- Requests created between two pages (newer created_at) never show up on later pages;
- when the Request at the cursor position is deleted between two pages, the next page
  starts at the next existing Request and no error is returned.

Requests are created and moved through their statuses with the real Lua scripts on
fakeredis (Lua) under a controlled Redis ``TIME``; the Gateway is the real
:func:`das.gateway.app.create_app`.

**Validates: Requirements 15.1, 15.4, 15.6, 15.7, 15.12**
"""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, given, note, settings
from hypothesis import strategies as st

from das.config.schema import ClientLimitsConfig, ClientSettingsConfig
from das.gateway.app import create_app
from das.models import FinalResult, RequestStatus
from das.registry import AgentRegistry
from das.state import StateStore
from tests.fixtures.redis_time import FakeRedisTime
from tests.unit.test_state_store import PLAN

API_KEY = "client-api-key-0001"
NO_RATE_LIMIT = ClientLimitsConfig(default=ClientSettingsConfig(rate=0.0))
AUTH = {"X-API-Key": API_KEY}
SECRET = "cursor-secret-for-property-27"
T0 = 1_700_000_000_000
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
STATUSES = list(RequestStatus)
COMPLETED = FinalResult(summary="done", jobs=[])
#: Redis time for status changes and for Requests created mid-walk: newer than any
#: seeded created_at (offsets are at most 20 ms).
LATER_MS = T0 + 10_000
TZ_OFFSETS = (0, 7 * 60, -5 * 60 - 30)  # minutes; exercises normalization to UTC


@dataclass(frozen=True)
class Req:
    rid: str
    offset_ms: int  # created_at = T0 + offset_ms (small span => many ties)
    status: RequestStatus


@dataclass(frozen=True)
class Query:
    statuses: tuple[RequestStatus, ...] | None
    after_ms: int | None
    before_ms: int | None
    tz_minutes: int
    limit: int | None  # None => default 20

    def _iso(self, ms: int) -> str:
        tz = timezone(timedelta(minutes=self.tz_minutes))
        return (EPOCH + timedelta(milliseconds=ms)).astimezone(tz).isoformat(
            timespec="milliseconds"
        )

    def params(self) -> dict[str, str]:
        out: dict[str, str] = {}
        if self.statuses is not None:
            out["status"] = ",".join(s.value for s in self.statuses)
        if self.after_ms is not None:
            out["created_after"] = self._iso(self.after_ms)
        if self.before_ms is not None:
            out["created_before"] = self._iso(self.before_ms)
        if self.limit is not None:
            out["limit"] = str(self.limit)
        return out

    def matches(self, r: Req) -> bool:
        ms = T0 + r.offset_ms
        return (
            (self.statuses is None or r.status in self.statuses)
            and (self.after_ms is None or ms >= self.after_ms)
            and (self.before_ms is None or ms < self.before_ms)
        )


@dataclass(frozen=True)
class Boundary:
    """What happens to the store between two pages of a walk."""

    delete_cursor_item: bool  # delete the last Request of the page just returned
    new_requests: tuple[RequestStatus, ...]  # created now, i.e. newer than every page


_rid = st.text(alphabet="0123456789abcdef", min_size=8, max_size=8)


@st.composite
def requests_(draw: st.DrawFn) -> list[Req]:
    n = draw(st.one_of(st.integers(0, 40), st.integers(0, 300)))
    ids = draw(st.lists(_rid, min_size=n, max_size=n, unique=True))
    span = draw(st.integers(0, 20))  # few distinct timestamps => duplicate created_at
    return [
        Req(rid, draw(st.integers(0, span)), draw(st.sampled_from(STATUSES))) for rid in ids
    ]


def _window(pair: tuple[int | None, int | None]) -> tuple[int | None, int | None]:
    a, b = pair
    if a is not None and b is not None and a >= b:
        a, b = b, (a + 1 if a == b else a)
    return a, b


queries = st.builds(
    lambda statuses, window, tz, limit: Query(statuses, *_window(window), tz, limit),
    st.none() | st.lists(st.sampled_from(STATUSES), min_size=1, max_size=5).map(tuple),
    st.tuples(st.none() | st.integers(T0 - 2, T0 + 22), st.none() | st.integers(T0 - 2, T0 + 22)),
    st.sampled_from(TZ_OFFSETS),
    st.none() | st.integers(1, 100) | st.integers(1, 5),
)

boundaries = st.builds(
    Boundary,
    delete_cursor_item=st.booleans(),
    new_requests=st.lists(st.sampled_from(STATUSES), max_size=2).map(tuple),
)


class Seeder:
    def __init__(self, store: StateStore, clock: FakeRedisTime) -> None:
        self.store = store
        self.clock = clock
        self.new_ids = (f"new{n:05d}" for n in itertools.count())
        #: Model of the Requests that exist: rid -> Req (the test's reference).
        self.model: dict[str, Req] = {}

    async def move_to(self, rid: str, status: RequestStatus) -> None:
        """Drive a Pending Request to ``status`` through the real scripts."""
        s = self.store
        if status is RequestStatus.PENDING:
            return
        if status is RequestStatus.CANCELLED:
            assert (await s.cancel_request(rid)).ok
            return
        lease = (await s.acquire_lease(rid, "m1", 3600)).lease
        assert lease is not None
        assert (await s.commit_plan(lease, PLAN)).ok
        if status is RequestStatus.COMPLETED:
            assert (await s.finalize(lease, COMPLETED)).ok
        elif status is RequestStatus.FAILED:
            assert (await s.fail_request(lease, "job a failed")).ok

    async def seed(self, reqs: list[Req]) -> None:
        ids = iter(r.rid for r in sorted(reqs, key=lambda x: x.offset_ms))
        self.store._id_factory = lambda: next(ids)
        # Create in created_at order so Redis TIME never moves backwards.
        for r in sorted(reqs, key=lambda x: x.offset_ms):
            self.clock.set_ms(T0 + r.offset_ms)
            assert await self.store.create_request("secret prompt") == r.rid
            self.model[r.rid] = r
        self.clock.set_ms(LATER_MS)
        for r in reqs:
            await self.move_to(r.rid, r.status)
        await self.store.set_index_ready()

    async def create_new(self, status: RequestStatus) -> None:
        self.store._id_factory = lambda: next(self.new_ids)
        now = self.clock.now_ms() + 1
        self.clock.set_ms(now)
        rid = await self.store.create_request("late prompt")
        await self.move_to(rid, status)
        self.model[rid] = Req(rid, now - T0, status)

    async def delete(self, rid: str) -> None:
        await self.store.delete_request(rid)
        del self.model[rid]


async def _walk(
    client: httpx.AsyncClient,
    seeder: Seeder,
    q: Query,
    expected: list[str],
    perturb: list[Boundary],
) -> None:
    limit = 20 if q.limit is None else q.limit
    seen: list[str] = []
    params = q.params()
    for page_no in itertools.count():
        resp = await client.get("/v1/requests", params=params, headers=AUTH)
        assert resp.status_code == 200, (q, resp.text)
        body = resp.json()
        items = [i["request_id"] for i in body["items"]]
        assert len(items) <= limit, (q, len(items))
        seen += items
        # next_cursor present iff matching Requests remain after this page (Req 15.4).
        assert ("next_cursor" in body) == (len(seen) < len(expected)), (q, seen, body)
        if "next_cursor" not in body:
            break
        assert items, "a page with next_cursor must not be empty"
        assert page_no <= len(expected), "pagination does not terminate"
        # Between pages: delete the cursor position (Req 15.12) and/or create newer
        # Requests (Req 15.7). Neither changes what remains after the cursor.
        b = perturb[page_no % len(perturb)] if perturb else None
        event("multi-page walk")
        if b is not None:
            if b.delete_cursor_item:
                event("cursor position deleted between pages")
                await seeder.delete(items[-1])
            for status in b.new_requests:
                event("Request created between pages")
                await seeder.create_new(status)
        params = {**q.params(), "cursor": body["next_cursor"]}
    assert seen == expected, q


async def _run(reqs: list[Req], qs: list[Query], perturb: list[Boundary]) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        store = StateStore(redis)
        with FakeRedisTime() as clock:
            seeder = Seeder(store, clock)
            await seeder.seed(reqs)
            app = create_app(
                store,
                AgentRegistry(redis),
                None,
                [API_KEY],
                10,
                cursor_secret=SECRET,
                client_limits=NO_RATE_LIMIT,  # a walk makes hundreds of calls at one fake instant
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://gw"
            ) as client:
                for q in qs:
                    # Reference: the modelled Requests that exist now (earlier walks
                    # may have deleted or added some), filtered, (created_at, id) desc.
                    ordered = sorted(
                        seeder.model.values(), key=lambda r: (r.offset_ms, r.rid), reverse=True
                    )
                    expected = [r.rid for r in ordered if q.matches(r)]
                    note(f"query={q.params()} expected={len(expected)}")
                    await _walk(client, seeder, q, expected, perturb)
    finally:
        await redis.aclose()


@settings(max_examples=100)
@given(
    reqs=requests_(),
    qs=st.lists(queries, min_size=1, max_size=3),
    perturb=st.lists(boundaries, max_size=3),
)
def test_request_list_pagination_is_complete(
    reqs: list[Req], qs: list[Query], perturb: list[Boundary]
) -> None:
    asyncio.run(_run(reqs, qs, perturb))
