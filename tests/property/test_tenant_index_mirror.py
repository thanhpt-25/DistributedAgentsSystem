# Feature: das-multi-tenant, Property 2: Tenant_Index mirrors the global index
"""Property 2: Tenant_Index mirrors the global index.

For any sequence of create, plan commit, finalize, cancel, reopen, replay and purge over several tenants, after every
operation, for each tenant ``t`` and status ``s``, the tenant's ``by_status:{s}`` zset equals the members of the global
``by_status:{s}`` whose Request belongs to ``t``, ``das:tenant:{t}:requests`` equals the members of
``das:requests:index`` belonging to ``t``, and ``das:tenant:{t}:active`` equals the tenant's non-terminal Requests.
Requests without ``tenant`` are in the ``default`` tenant's sets; the Dead_Letter twins follow the same rule. Operations
rejected with ``LEASE_LOST`` or ``REQUEST_TERMINAL`` change no tenant key.

**Validates: Requirements 3.1, 3.5, 3.9**
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
        await world.check_mirror()
        for op in ops:
            await world.apply(*op)
            await world.check_mirror()  # after every operation, not only at the end


@example(EVERY_KIND)
@given(operations)
def test_the_tenant_indexes_mirror_the_global_ones_after_every_operation(ops: list[tuple[Any, ...]]) -> None:
    asyncio.run(run(ops))
