# Feature: das-client-api, Property 1: Token bucket bound
"""Property 1: Token bucket bound.

For any ``rate`` (0 or 0.001-1000 tokens/s), ``burst`` (1-50) and non-decreasing
sequence of call times:

- ``rate_check.lua`` makes the same allow/deny decision as the pure
  ``das.client.bucket.take`` at every call;
- for every window ``[t_i, t_j]`` of calls the number admitted is at most
  ``floor(burst + rate * (t_j - t_i))`` (Phase 4 Req 2.3);
- two ``StateStore`` instances on one Redis (two Gateway replicas), called
  alternately, decide exactly as a single bucket would (Phase 4 Req 2.4).

Runs the real Lua script on a fresh in-process fakeredis server per example, with
Redis ``TIME`` driven by :class:`FakeRedisTime`. Times are drawn as integer
microseconds so Redis ``TIME`` and the model see identical values.

**Validates: Requirements 2.3, 2.4**
"""

from __future__ import annotations

import asyncio
import math

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given
from hypothesis import strategies as st

from das.client.bucket import BucketState, take
from das.state import StateStore
from tests.fixtures.redis_time import FakeRedisTime

START_US = 1_700_000_000_000_000
FP = "cbabecbd"

rates = st.one_of(st.just(0.0), st.floats(0.001, 1000, allow_nan=False, allow_infinity=False))
bursts = st.integers(1, 50)
# Gaps up to 3 s, with zero gaps common so bursts of simultaneous calls occur.
gaps = st.lists(st.one_of(st.just(0), st.integers(0, 3_000_000)), min_size=1, max_size=40)


def _times(gap_list: list[int]) -> list[int]:
    out, t = [], START_US
    for g in gap_list:
        t += g
        out.append(t)
    return out


async def _check(rate: float, burst: int, times_us: list[int]) -> None:
    server = FakeServer()
    clients = [FakeRedis(server=server, decode_responses=True) for _ in range(2)]
    try:
        stores = [StateStore(c) for c in clients]
        model: BucketState | None = None
        admitted: list[bool] = []
        with FakeRedisTime() as rt:
            for i, us in enumerate(times_us):
                sec, usec = divmod(us, 1_000_000)
                rt.set(us / 1_000_000)
                assert await clients[0].time() == (sec, usec)
                now = sec + usec / 1_000_000  # server_time() in _common.lua
                got = await stores[i % 2].rate_check(FP, rate, burst)
                expected = take(model, now, rate, burst)
                model = expected.state
                assert (got is None) is expected.allowed, (i, us, got, expected)
                if got is not None:
                    assert math.isclose(got, expected.retry_after, rel_tol=1e-12, abs_tol=1e-12)
                admitted.append(got is None)
        if rate == 0:
            assert all(admitted)
            return
        # Req 2.3: no window admits more than burst + rate * width calls. The 1e-9
        # slack absorbs float rounding of the refill sum.
        for i in range(len(times_us)):
            count = 0
            for j in range(i, len(times_us)):
                count += admitted[j]
                width = (times_us[j] - times_us[i]) / 1_000_000
                assert count <= math.floor(burst + rate * width + 1e-9), (i, j, count)
    finally:
        for c in clients:
            await c.aclose()


@given(rates, bursts, gaps)
def test_token_bucket_bound(rate: float, burst: int, gap_list: list[int]) -> None:
    """**Validates: Requirements 2.3, 2.4**"""
    asyncio.run(_check(rate, burst, _times(gap_list)))
