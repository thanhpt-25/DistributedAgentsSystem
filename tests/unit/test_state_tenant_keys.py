"""Tenant keys and Lua helpers (das-multi-tenant task 3.1; Phase 5 Req 3.5, 5.1, 6.1).

The helpers are exercised in isolation (a test-only script that runs after ``_common.lua``), as the Phase 4 helpers
are. Wiring them into the Request and DLQ scripts is task 3.2 to 3.4."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from das.state import keys
from das.state.scripts import _read

T = "team-a"
RID = "r1"
REQ = keys.request(RID)


def _helper_script(client, body: str):
    """Register a test-only script that runs ``body`` after the shared helpers."""
    return client.register_script(_read("_common") + "\n" + body)


# --------------------------------------------------------------------------- Python keys


def test_tenant_key_builders() -> None:
    assert keys.DEFAULT_TENANT == "default"
    assert keys.tenant_requests(T) == "das:tenant:team-a:requests"
    assert keys.tenant_requests_by_status(T, "Running") == "das:tenant:team-a:requests:by_status:Running"
    assert keys.tenant_active(T) == "das:tenant:team-a:active"
    assert keys.tenant_dlq_index(T) == "das:tenant:team-a:dlq:index"
    assert keys.tenant_dlq_by_status(T, "Pending") == "das:tenant:team-a:dlq:by_status:Pending"
    assert keys.tenant_dlq_by_reason(T, "max_attempts") == "das:tenant:team-a:dlq:by_reason:max_attempts"
    assert keys.tenant_usage(T, "20261112") == "das:tenant:team-a:usage:20261112"
    assert keys.TENANT_INDEX_READY == "das:tenant:index:ready"
    assert keys.TENANT_INDEX_BACKFILL_LOCK == "das:tenant:index:backfill_lock"


def test_the_request_hash_gains_two_reserved_fields() -> None:
    assert (keys.F_TENANT, keys.F_PRIORITY) == ("tenant", "priority")
    assert keys.F_TENANT in keys.RESERVED_REQUEST_FIELDS and keys.F_PRIORITY in keys.RESERVED_REQUEST_FIELDS


def test_tenant_keys_are_not_per_request_keys() -> None:
    assert len(keys.request_keys("r")) == 11  # unchanged: the tenant keys are not removed with a Request


GLOBAL_TO_TWIN = [
    (keys.REQUESTS_INDEX, keys.tenant_requests(T)),
    (keys.requests_by_status("Running"), keys.tenant_requests_by_status(T, "Running")),
    (keys.requests_by_status("Pending"), keys.tenant_requests_by_status(T, "Pending")),
    (keys.DLQ_INDEX, keys.tenant_dlq_index(T)),
    (keys.dlq_by_status("Pending"), keys.tenant_dlq_by_status(T, "Pending")),
    (keys.dlq_by_reason("max_attempts"), keys.tenant_dlq_by_reason(T, "max_attempts")),
]
NO_TWIN = [
    keys.REQUESTS_PENDING,
    keys.REQUESTS_FINISHED,
    keys.REQUESTS_INBOX_PURGE,
    keys.REQUESTS_INDEX_READY,
    keys.LEASES_EXPIRY,
    keys.DLQ_TERMINAL,
    keys.AGENTS,
    keys.WEBHOOKS_DUE,
]


@pytest.mark.parametrize(("global_key", "twin"), GLOBAL_TO_TWIN)
def test_the_python_twin_of_a_global_index_key(global_key: str, twin: str) -> None:
    assert keys.tenant_twin(global_key, T) == twin


@pytest.mark.parametrize("global_key", NO_TWIN)
def test_infrastructure_keys_have_no_twin(global_key: str) -> None:
    with pytest.raises(ValueError):
        keys.tenant_twin(global_key, T)


# --------------------------------------------------------------------------- Lua: tenant_of and tenant_twin


async def test_tenant_of_defaults_when_the_field_is_absent_or_empty(fake_redis) -> None:
    script = _helper_script(fake_redis, "return tenant_of(KEYS[1])")
    assert await script(keys=[REQ], args=[]) == "default"  # no hash at all
    await fake_redis.hset(REQ, mapping={"status": "Pending"})
    assert await script(keys=[REQ], args=[]) == "default"
    await fake_redis.hset(REQ, "tenant", T)
    assert await script(keys=[REQ], args=[]) == T
    await fake_redis.hset(REQ, "tenant", "")
    assert await script(keys=[REQ], args=[]) == "default"


@pytest.mark.parametrize(("global_key", "twin"), GLOBAL_TO_TWIN)
async def test_the_lua_twin_equals_the_python_twin(fake_redis, global_key: str, twin: str) -> None:
    assert await _helper_script(fake_redis, "return tenant_twin(KEYS[1], ARGV[1])")(keys=[global_key], args=[T]) == twin


@pytest.mark.parametrize("global_key", NO_TWIN)
async def test_the_lua_twin_is_nil_for_infrastructure_keys(fake_redis, global_key: str) -> None:
    assert await _helper_script(fake_redis, "return tenant_twin(KEYS[1], ARGV[1])")(keys=[global_key], args=[T]) is None


# --------------------------------------------------------------------------- Lua: index_move


MOVE = "index_move(ARGV[1], KEYS[2], KEYS[3], tonumber(ARGV[2]), KEYS[1]) return 1"
MOVE_LEGACY = "index_move(ARGV[1], KEYS[1], KEYS[2], tonumber(ARGV[2])) return 1"
ADD_WITH_TENANT = "index_move(ARGV[1], nil, KEYS[2], tonumber(ARGV[2]), KEYS[1]) return 1"


async def zset(client, key: str) -> list[tuple[str, float]]:
    return [(m, s) for m, s in await client.zrange(key, 0, -1, withscores=True)]


async def test_index_move_without_a_request_key_behaves_as_before_and_touches_no_twin(fake_redis) -> None:
    pending, running = keys.requests_by_status("Pending"), keys.requests_by_status("Running")
    await fake_redis.zadd(pending, {RID: 5})
    await _helper_script(fake_redis, MOVE_LEGACY)(keys=[pending, running], args=[RID, "5"])
    assert await zset(fake_redis, pending) == [] and await zset(fake_redis, running) == [(RID, 5.0)]
    assert [k async for k in fake_redis.scan_iter("das:tenant:*")] == []


async def test_index_move_with_a_request_key_moves_the_request_in_the_tenants_twins_too(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"tenant": T})
    pending, running = keys.requests_by_status("Pending"), keys.requests_by_status("Running")
    await fake_redis.zadd(pending, {RID: 5})
    await fake_redis.zadd(keys.tenant_requests_by_status(T, "Pending"), {RID: 5})
    await _helper_script(fake_redis, MOVE)(keys=[REQ, pending, running], args=[RID, "5"])
    assert await zset(fake_redis, running) == [(RID, 5.0)] and await zset(fake_redis, pending) == []
    assert await zset(fake_redis, keys.tenant_requests_by_status(T, "Running")) == [(RID, 5.0)]
    assert await zset(fake_redis, keys.tenant_requests_by_status(T, "Pending")) == []


async def test_a_request_without_a_tenant_moves_in_the_default_tenants_twins(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"status": "Pending"})
    running = keys.requests_by_status("Running")
    await _helper_script(fake_redis, ADD_WITH_TENANT)(keys=[REQ, running], args=[RID, "7"])
    assert await zset(fake_redis, keys.tenant_requests_by_status("default", "Running")) == [(RID, 7.0)]


async def test_adding_to_the_index_also_adds_to_the_tenants_requests_zset(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"tenant": T})
    await _helper_script(fake_redis, ADD_WITH_TENANT)(keys=[REQ, keys.REQUESTS_INDEX], args=[RID, "9"])
    assert await zset(fake_redis, keys.REQUESTS_INDEX) == [(RID, 9.0)]
    assert await zset(fake_redis, keys.tenant_requests(T)) == [(RID, 9.0)]


async def test_moving_to_the_same_key_does_not_remove_and_the_score_is_refreshed_in_both(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"tenant": T})
    running = keys.requests_by_status("Running")
    twin = keys.tenant_requests_by_status(T, "Running")
    await fake_redis.zadd(running, {RID: 1})
    await fake_redis.zadd(twin, {RID: 1})
    await _helper_script(fake_redis, MOVE)(keys=[REQ, running, running], args=[RID, "2"])
    assert await zset(fake_redis, running) == [(RID, 2.0)] and await zset(fake_redis, twin) == [(RID, 2.0)]


async def test_an_empty_from_key_skips_the_removal(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"tenant": T})
    running = keys.requests_by_status("Running")
    await _helper_script(fake_redis, MOVE)(keys=[REQ, "", running], args=[RID, "3"])
    assert await zset(fake_redis, keys.tenant_requests_by_status(T, "Running")) == [(RID, 3.0)]


# --------------------------------------------------------------------------- Lua: Active_Set helpers


ACTIVE_ADD = "client_active_add(KEYS[1], ARGV[1]) return 1"
ACTIVE_REMOVE = "client_active_remove(KEYS[1], ARGV[1]) return 1"


async def test_active_add_and_remove_maintain_the_tenant_set_and_the_key_set(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"tenant": T, "client_fp": "cbabecbd"})
    await _helper_script(fake_redis, ACTIVE_ADD)(keys=[REQ], args=[RID])
    assert await fake_redis.smembers(keys.tenant_active(T)) == {RID}
    assert await fake_redis.smembers(keys.client_active("cbabecbd")) == {RID}
    await _helper_script(fake_redis, ACTIVE_ADD)(keys=[REQ], args=[RID])  # idempotent
    assert await fake_redis.scard(keys.tenant_active(T)) == 1
    await _helper_script(fake_redis, ACTIVE_REMOVE)(keys=[REQ], args=[RID])
    assert await fake_redis.smembers(keys.tenant_active(T)) == set()
    assert await fake_redis.smembers(keys.client_active("cbabecbd")) == set()


async def test_a_request_without_client_fp_is_still_in_its_tenants_set_but_in_no_key_set(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"tenant": T})
    await _helper_script(fake_redis, ACTIVE_ADD)(keys=[REQ], args=[RID])
    assert await fake_redis.smembers(keys.tenant_active(T)) == {RID}
    assert [k async for k in fake_redis.scan_iter("das:client:*")] == []


async def test_a_request_without_a_tenant_is_in_the_default_tenants_set(fake_redis) -> None:
    await fake_redis.hset(REQ, mapping={"status": "Pending"})
    await _helper_script(fake_redis, ACTIVE_ADD)(keys=[REQ], args=[RID])
    assert await fake_redis.smembers(keys.tenant_active("default")) == {RID}


# --------------------------------------------------------------------------- Lua: utc_day and usage_bump


def epoch_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp() * 1000)


@pytest.mark.parametrize(
    "iso",
    [
        "1970-01-01T00:00:00",
        "2024-02-29T23:59:59.999000",
        "2024-03-01T00:00:00",
        "2026-11-12T08:00:00",
        "2026-12-31T23:59:59.999000",
        "2027-01-01T00:00:00",
        "2100-02-28T12:00:00",
    ],
)
async def test_utc_day_matches_the_calendar(fake_redis, iso: str) -> None:
    ms = epoch_ms(iso)
    expected = datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y%m%d")
    assert await _helper_script(fake_redis, "return utc_day(tonumber(ARGV[1]))")(keys=[], args=[str(ms)]) == expected


BUMP = "usage_bump(KEYS[1], ARGV[1], tonumber(ARGV[2]), ARGV[3]) return 1"
FOUR_HUNDRED_DAYS = 400 * 86_400


async def test_usage_bump_adds_to_the_tenants_hash_of_the_current_utc_day(fake_redis, fake_redis_time) -> None:
    fake_redis_time.set_ms(epoch_ms("2026-11-12T08:00:00"))
    await fake_redis.hset(REQ, mapping={"tenant": T})
    script = _helper_script(fake_redis, BUMP)
    await script(keys=[REQ], args=["submitted", "1", "400"])
    await script(keys=[REQ], args=["submitted", "1", "400"])
    await script(keys=[REQ], args=["tokens/in/openai/gpt-4o", "1200", "400"])
    day = await fake_redis.hgetall(keys.tenant_usage(T, "20261112"))
    assert day == {"submitted": "2", "tokens/in/openai/gpt-4o": "1200"}


async def test_usage_bump_rolls_over_at_midnight_utc(fake_redis, fake_redis_time) -> None:
    await fake_redis.hset(REQ, mapping={"tenant": T})
    script = _helper_script(fake_redis, BUMP)
    fake_redis_time.set_ms(epoch_ms("2026-11-12T23:59:59.999000"))
    await script(keys=[REQ], args=["submitted", "1", "400"])
    fake_redis_time.set_ms(epoch_ms("2026-11-13T00:00:00"))
    await script(keys=[REQ], args=["submitted", "1", "400"])
    assert await fake_redis.hget(keys.tenant_usage(T, "20261112"), "submitted") == "1"
    assert await fake_redis.hget(keys.tenant_usage(T, "20261113"), "submitted") == "1"


async def test_usage_bump_sets_the_retention_expiry_and_defaults_to_400_days(fake_redis, fake_redis_time) -> None:
    fake_redis_time.set_ms(epoch_ms("2026-11-12T08:00:00"))
    await fake_redis.hset(REQ, mapping={"tenant": T})
    day = keys.tenant_usage(T, "20261112")
    await _helper_script(fake_redis, BUMP)(keys=[REQ], args=["submitted", "1", "30"])
    assert 30 * 86_400 - 5 <= await fake_redis.ttl(day) <= 30 * 86_400
    await _helper_script(fake_redis, BUMP)(keys=[REQ], args=["submitted", "1", ""])  # no retention given
    assert FOUR_HUNDRED_DAYS - 5 <= await fake_redis.ttl(day) <= FOUR_HUNDRED_DAYS


async def test_a_request_without_a_tenant_is_counted_for_the_default_tenant(fake_redis, fake_redis_time) -> None:
    fake_redis_time.set_ms(epoch_ms("2026-11-12T08:00:00"))
    await fake_redis.hset(REQ, mapping={"status": "Pending"})
    await _helper_script(fake_redis, BUMP)(keys=[REQ], args=["finished/Completed", "1", "400"])
    assert await fake_redis.hgetall(keys.tenant_usage("default", "20261112")) == {"finished/Completed": "1"}
