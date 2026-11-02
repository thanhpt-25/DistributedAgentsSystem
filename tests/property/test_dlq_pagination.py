# Feature: das-observability-operations, Property 19: Phân trang DLQ đầy đủ
"""Property 19: complete DLQ pagination.

For any set of Dead_Letter_Records (up to 300, with duplicate ``dead_lettered_at``)
that does not change while it is walked, any combination of the ``status`` /
``reason`` / ``request_id`` / ``capability`` filters and any ``limit`` from 1 to 100,
concatenating the pages of ``GET /v1/admin/dlq`` obtained by following
``next_cursor`` from the first page yields exactly the reference list: filter, then
sort by (dead_lettered_at desc, Dead_Letter_ID desc). Every page holds at most
``limit`` items, and ``next_cursor`` is present iff more matching items follow.

Records are created through the real ``create_dead_letter.lua`` /
``discard_dead_letter.lua`` on fakeredis (Lua) with a controlled Redis ``TIME``; the
Gateway is the real :func:`das.gateway.app.create_app` called with an Admin_API_Key.

**Validates: Requirements 12.4, 12.5, 12.6**
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.gateway.app import create_app
from das.models.dlq import DEAD_LETTER_REASONS, RECORD_STATUSES, DeadLetterFields
from das.registry import AgentRegistry
from das.state import StateStore, keys
from tests.fixtures.redis_time import FakeRedisTime

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
ADMIN = {"X-API-Key": ADMIN_KEY}
T0 = 1_700_000_000_000

REQUEST_IDS = ("r1", "r2", "r3")
CAPABILITIES = ("research", "review", "code")


@dataclass(frozen=True)
class Rec:
    dlid: str
    offset_ms: int  # dead_lettered_at = T0 + offset_ms (small range => many ties)
    reason: str
    status: str
    request_id: str | None
    capabilities: tuple[str, ...] | None


@dataclass(frozen=True)
class Query:
    status: str | None
    reason: str | None
    request_id: str | None
    capability: str | None
    limit: int | None  # None => default 20

    def params(self) -> dict[str, str]:
        out = {
            k: v
            for k, v in (
                ("status", self.status),
                ("reason", self.reason),
                ("request_id", self.request_id),
                ("capability", self.capability),
            )
            if v is not None
        }
        if self.limit is not None:
            out["limit"] = str(self.limit)
        return out

    def matches(self, r: Rec) -> bool:
        return (
            (self.status is None or r.status == self.status)
            and (self.reason is None or r.reason == self.reason)
            and (self.request_id is None or r.request_id == self.request_id)
            and (self.capability is None or self.capability in (r.capabilities or ()))
        )


_dlid = st.text(alphabet="0123456789abcdef", min_size=32, max_size=32)


@st.composite
def records(draw: st.DrawFn) -> list[Rec]:
    n = draw(st.one_of(st.integers(0, 40), st.integers(0, 300)))
    ids = draw(st.lists(_dlid, min_size=n, max_size=n, unique=True))
    span = draw(st.integers(0, 20))  # few distinct timestamps => duplicate scores
    out = []
    for dlid in ids:
        out.append(
            Rec(
                dlid=dlid,
                offset_ms=draw(st.integers(0, span)),
                reason=draw(st.sampled_from(DEAD_LETTER_REASONS)),
                status=draw(st.sampled_from(RECORD_STATUSES)),
                request_id=draw(st.one_of(st.none(), st.sampled_from(REQUEST_IDS))),
                capabilities=draw(
                    st.one_of(
                        st.none(),
                        st.lists(st.sampled_from(CAPABILITIES), min_size=1, max_size=3, unique=True)
                        .map(tuple),
                    )
                ),
            )
        )
    return out


queries = st.builds(
    Query,
    status=st.one_of(st.none(), st.sampled_from(RECORD_STATUSES)),
    reason=st.one_of(st.none(), st.sampled_from(DEAD_LETTER_REASONS)),
    request_id=st.one_of(st.none(), st.sampled_from((*REQUEST_IDS, "nobody"))),
    capability=st.one_of(st.none(), st.sampled_from((*CAPABILITIES, "absent-cap"))),
    limit=st.one_of(st.none(), st.integers(1, 100), st.integers(1, 5)),
)


async def _seed(store: StateStore, clock: FakeRedisTime, recs: list[Rec]) -> None:
    r = store.client
    # Create in timestamp order so Redis TIME never needs to move backwards.
    for rec in sorted(recs, key=lambda x: x.offset_ms):
        clock.set_ms(T0 + rec.offset_ms)
        created = await store.create_dead_letter(
            DeadLetterFields(
                dead_letter_id=rec.dlid,
                reason=rec.reason,  # type: ignore[arg-type]
                request_id=rec.request_id,
                job_id=None if rec.request_id is None else "a",
                attempt=None if rec.request_id is None else 1,
                capabilities=None if rec.capabilities is None else list(rec.capabilities),
                last_error="boom",
                message_id=f"m-{rec.dlid[:6]}",
            )
        )
        assert created.created and created.dead_lettered_at_ms == T0 + rec.offset_ms
    clock.set_ms(T0 + 60_000)
    for rec in recs:
        if rec.status == "Discarded":
            assert (await store.discard_dead_letter(rec.dlid)).outcome == "accepted"
        elif rec.status == "Replayed":
            # A real Replay needs a Request/Job/attempt setup per record; apply only the
            # Dead_Letter_Record part of replay_dead_letter.lua (lines setting status and
            # moving the dlid between the by_status zsets), which is all listing reads.
            ms = T0 + 60_000
            await r.hset(
                keys.dlq(rec.dlid), mapping={"status": "Replayed", "status_changed_at": str(ms)}
            )
            await r.zrem(keys.dlq_by_status("Pending"), rec.dlid)
            await r.zadd(keys.dlq_by_status("Replayed"), {rec.dlid: T0 + rec.offset_ms})
            await r.zadd(keys.DLQ_TERMINAL, {rec.dlid: ms})


async def _walk(client: httpx.AsyncClient, q: Query, expected: list[str]) -> None:
    limit = 20 if q.limit is None else q.limit
    seen: list[str] = []
    params = q.params()
    pages = 0
    while True:
        resp = await client.get("/v1/admin/dlq", params=params, headers=ADMIN)
        assert resp.status_code == 200, (q, resp.text)
        body = resp.json()
        items = [i["dead_letter_id"] for i in body["items"]]
        assert len(items) <= limit, (q, len(items))
        seen += items
        pages += 1
        # next_cursor present iff matching records remain after this page (Req 12.5).
        assert ("next_cursor" in body) == (len(seen) < len(expected)), (q, len(seen), body)
        if "next_cursor" not in body:
            break
        assert items, "a page with next_cursor must not be empty"
        assert pages <= len(expected) + 1, "pagination does not terminate"
        params = {**q.params(), "cursor": body["next_cursor"]}
    assert seen == expected, q


async def _run(recs: list[Rec], qs: list[Query]) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        store = StateStore(redis)
        with FakeRedisTime() as clock:
            await _seed(store, clock, recs)
        app = create_app(
            store, AgentRegistry(redis), None, [API_KEY], 10, admin_api_keys=[ADMIN_KEY]
        )
        ordered = sorted(recs, key=lambda x: (x.offset_ms, x.dlid), reverse=True)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:
            for q in qs:
                await _walk(client, q, [r.dlid for r in ordered if q.matches(r)])
    finally:
        await redis.aclose()


@settings(max_examples=100)
@given(recs=records(), qs=st.lists(queries, min_size=1, max_size=4))
def test_dlq_pagination_is_complete(recs: list[Rec], qs: list[Query]) -> None:
    asyncio.run(_run(recs, qs))
