# Feature: distributed-agent-system, Property 9: Submission creates unique Pending Requests
"""Property 9: Submission creates unique Pending Requests.

*For any* sequence of valid prompts submitted with a valid API_Key, every
returned Request_ID is distinct, and each maps in the State_Store to a Request
with status Pending and the submitted prompt.

Prompts are submitted through the real Gateway_API (``httpx.ASGITransport``),
sequentially or concurrently, against a State_Store whose ``id_factory`` may
deliberately re-issue already-used ids to exercise collision regeneration.

**Validates: Requirements 1.1**
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable

import httpx
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, note
from hypothesis import strategies as st

from das.gateway.app import create_app
from das.models import RequestStatus
from das.registry import AgentRegistry
from das.state import StateStore, keys

KEY = "prop-api-key-123"
AUTH = {"X-API-Key": KEY}
MAX_COLLISIONS = 20

_prompt_char = st.characters(codec="utf-8")


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[int, list[str], bool, list[bool] | None]:
    max_len = draw(st.integers(min_value=1, max_value=64))
    prompt = st.text(_prompt_char, min_size=1, max_size=max_len).filter(lambda s: s.strip() != "")
    prompts = draw(st.lists(prompt, min_size=1, max_size=12))
    concurrent = draw(st.booleans())
    # None -> default uuid factory; otherwise a schedule of "re-issue a used id" flags.
    collide = draw(st.none() | st.lists(st.booleans(), max_size=MAX_COLLISIONS))
    return max_len, prompts, concurrent, collide


def _colliding_factory(flags: list[bool]) -> Callable[[], str]:
    """Return fresh ids, but re-issue an already-issued id whenever the next flag is True."""
    issued: list[str] = []
    schedule = iter(flags)

    def factory() -> str:
        if next(schedule, False) and issued:
            return issued[len(issued) // 2]
        rid = uuid.uuid4().hex
        issued.append(rid)
        return rid

    return factory


async def _run(max_len: int, prompts: list[str], concurrent: bool, collide: list[bool] | None) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        if collide is None:
            store = StateStore(redis)
        else:
            # Each flag yields at most one collision, so this bound guarantees allocation succeeds.
            store = StateStore(
                redis, id_factory=_colliding_factory(collide), max_id_attempts=MAX_COLLISIONS + 1
            )
        app = create_app(store, AgentRegistry(redis), None, [KEY], max_prompt_length=max_len)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as client:

            async def submit(p: str) -> httpx.Response:
                return await client.post("/v1/requests", json={"prompt": p}, headers=AUTH)

            if concurrent:
                responses = await asyncio.gather(*(submit(p) for p in prompts))
            else:
                responses = [await submit(p) for p in prompts]

        assert [r.status_code for r in responses] == [202] * len(prompts)
        ids = [r.json()["request_id"] for r in responses]
        note(f"ids={ids}")
        assert len(set(ids)) == len(ids), "Request_IDs must be distinct"

        for rid, prompt in zip(ids, prompts, strict=True):
            snap = await store.load_snapshot(rid)
            assert snap is not None
            assert snap.status is RequestStatus.PENDING
            assert snap.prompt == prompt
            assert await redis.zscore(keys.REQUESTS_PENDING, rid) is not None

        prefix = keys.request("")
        request_records = [k for k in await redis.keys(f"{prefix}*") if ":" not in k[len(prefix):]]
        assert len(request_records) == len(prompts)
        assert await redis.zcard(keys.REQUESTS_PENDING) == len(prompts)
    finally:
        await redis.aclose()


@given(scenarios())
def test_submission_creates_unique_pending_requests(
    scenario: tuple[int, list[str], bool, list[bool] | None],
) -> None:
    asyncio.run(_run(*scenario))
