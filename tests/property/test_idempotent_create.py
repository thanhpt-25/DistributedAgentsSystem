# Feature: das-client-api, Property 2: Idempotent create
"""Property 2: Idempotent create.

*For any* set of concurrent submits that share the Idempotency-Key ``"k"``, drawn
over two client fingerprints and two Body_Hashes, each fingerprint that appears gets
exactly one Request. Every submit of that fingerprint with the creator's Body_Hash
returns the creator's Request_ID (``created`` once, ``replayed`` otherwise), every
submit with the other Body_Hash returns ``idempotency_conflict``, and the
fingerprint's Active_Set holds exactly that one Request. The same key under two
fingerprints never collides.

Runs the real ``create_request.lua`` on a fresh in-process fakeredis server per example.

**Validates: Requirements 4.2, 4.3, 4.4, 4.6, 4.7**
"""

from __future__ import annotations

import asyncio

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given, note
from hypothesis import strategies as st

from das.state import ClientRequestSpec, CreateResult, StateStore, keys

FINGERPRINTS = ("fp-a", "fp-b")
BODY_HASHES = ("body-1", "body-2")
KEY_HASH = "k"

submits = st.lists(
    st.tuples(st.sampled_from(FINGERPRINTS), st.sampled_from(BODY_HASHES)), min_size=2, max_size=8
)


async def _run(plan: list[tuple[str, str]]) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        store = StateStore(redis)

        async def submit(fp: str, body: str) -> CreateResult:
            spec = ClientRequestSpec(fingerprint=fp, idempotency_key_hash=KEY_HASH, body_hash=body)
            return await store.submit_request(f"prompt for {body}", spec)

        results = await asyncio.gather(*(submit(fp, body) for fp, body in plan))
        note(f"results={results}")

        created_rids: list[str] = []
        for fp in {fp for fp, _ in plan}:
            mine = [(body, res) for (f, body), res in zip(plan, results, strict=True) if f == fp]
            creators = [(body, res) for body, res in mine if res.outcome == "created"]
            assert len(creators) == 1, f"{fp}: expected exactly one created, got {creators}"
            creator_body, creator = creators[0]
            assert creator.request_id is not None
            created_rids.append(creator.request_id)
            for body, res in mine:
                if body == creator_body:
                    assert res.outcome in ("created", "replayed")
                    assert res.request_id == creator.request_id
                else:
                    assert res == CreateResult("idempotency_conflict")
            assert await redis.smembers(keys.client_active(fp)) == {creator.request_id}

        assert len(set(created_rids)) == len(created_rids)
        prefix = keys.request("")
        hashes = [k for k in await redis.keys(f"{prefix}*") if ":" not in k[len(prefix):]]
        assert sorted(hashes) == sorted(keys.request(r) for r in created_rids)
    finally:
        await redis.aclose()


@given(submits)
def test_idempotent_create(plan: list[tuple[str, str]]) -> None:
    asyncio.run(_run(plan))
