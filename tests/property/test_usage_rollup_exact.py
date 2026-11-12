# Feature: das-multi-tenant, Property 3: Usage rollup is exact
"""Property 3: Usage rollup is exact.

For any sequence of operations that append UsageRecords (plan, Job results, summarize, Replay), per tenant, UTC day and
``{provider}/{model}``, the rollup's ``tokens/in``, ``tokens/out`` and ``tokens/total`` equal the sums over the records
appended that day, ``submitted`` equals the Requests created that day, and ``finished/{status}`` equals the terminal
writes that day. A duplicate result discarded by the reducer adds nothing, and a rejected write adds nothing. Every
rollup hash expires within ``usage.retention_days`` (400 by default).

**Validates: Requirements 6.1, 6.2**
"""

from __future__ import annotations

import asyncio
from typing import Any

from hypothesis import example, given

from tests.fixtures.redis_time import FakeRedisTime
from tests.property.tenant_ops import EVERY_KIND, START_MS, World, operations


async def run(ops: list[tuple[Any, ...]]) -> None:
    with FakeRedisTime() as clock:
        clock.set_ms(START_MS)
        world = World(clock)
        await world.check_usage()
        for op in ops:
            await world.apply(*op)
            await world.check_usage()


@example(EVERY_KIND)
@given(operations)
def test_the_rollup_equals_the_sums_over_what_each_day_appended(ops: list[tuple[Any, ...]]) -> None:
    asyncio.run(run(ops))
