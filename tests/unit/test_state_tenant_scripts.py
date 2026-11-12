"""The Request, DLQ and usage scripts keep the tenant keys in step (das-multi-tenant tasks 3.2, 3.3, 3.4;
Phase 5 Req 3.1, 3.5, 3.7, 3.9, 5.1, 5.2, 6.1, 6.6, 7.3).

Each test drives the real store path against the Lua scripts on fakeredis. Property 2 (the tenant indexes mirror the
global ones, task 3.6) and Property 3 (the usage rollup is exact, task 3.7) generalise what is checked here by
example."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from das.models import JobRecord, JobState, Lease
from das.state import ClientRequestSpec, StateStore, StoreStatus, TransitionBatch, keys
from tests.unit.test_state_phase2_dlq import DL1, DL2, everything, fields
from tests.unit.test_state_phase2_dlq import FINAL as FAILED_FINAL
from tests.unit.test_state_phase2_writes import FINAL, usage
from tests.unit.test_state_store import PLAN, result

FP = "cbabecbd"
A, B = "team-a", "team-b"


def ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=UTC).timestamp() * 1000)


async def submit(store: StateStore, tenant: str | None = A, **kw: Any) -> str:
    res = await store.submit_request("hello", ClientRequestSpec(fingerprint=FP, tenant=tenant, **kw))
    assert res.request_id is not None, res
    return res.request_id


async def lease_of(store: StateStore, rid: str) -> Lease:
    lease = (await store.acquire_lease(rid, "m1", 30)).lease
    assert lease is not None
    return lease


async def running(store: StateStore, tenant: str | None = A, plan_usage: Any = ()) -> tuple[str, Lease]:
    rid = await submit(store, tenant)
    lease = await lease_of(store, rid)
    assert (await store.commit_plan(lease, PLAN, plan_usage)).ok
    return rid, lease


async def members(store: StateStore, key: str) -> set[str]:
    return {str(m) for m in await store.client.zrange(key, 0, -1)}


async def by_status(store: StateStore, tenant: str, status: str) -> set[str]:
    return await members(store, keys.tenant_requests_by_status(tenant, status))


async def tenant_keys(store: StateStore) -> dict[str, Any]:
    return {k: v for k, v in (await everything(store)).items() if k.startswith("das:tenant:")}


async def active(store: StateStore, tenant: str) -> set[str]:
    return {str(m) for m in await store.client.smembers(keys.tenant_active(tenant))}


async def day(store: StateStore, tenant: str, yyyymmdd: str) -> dict[str, str]:
    return await store.client.hgetall(keys.tenant_usage(tenant, yyyymmdd))


# =========================================================================== create (task 3.2)


async def test_a_submitted_request_stores_its_tenant_and_joins_the_tenants_indexes(state_store: StateStore) -> None:
    rid = await submit(state_store)
    r = state_store.client
    assert await r.hget(keys.request(rid), "tenant") == A
    assert await r.hget(keys.request(rid), "priority") is None  # normal is not stored
    score = await r.zscore(keys.REQUESTS_INDEX, rid)
    assert await r.zscore(keys.tenant_requests(A), rid) == score
    assert await r.zscore(keys.tenant_requests_by_status(A, "Pending"), rid) == score
    assert await active(state_store, A) == {rid}
    assert await r.smembers(keys.client_active(FP)) == {rid}  # the per-key set (Phase 4) is unchanged


async def test_a_high_priority_is_stored_and_a_normal_one_is_not(state_store: StateStore) -> None:
    high = await submit(state_store, priority="high")
    normal = await submit(state_store, priority="normal")
    assert await state_store.client.hget(keys.request(high), "priority") == "high"
    assert await state_store.client.hget(keys.request(normal), "priority") is None


async def test_a_request_created_without_a_tenant_belongs_to_the_default_tenant(state_store: StateStore) -> None:
    rid = await state_store.create_request("hello")
    assert await state_store.client.hget(keys.request(rid), "tenant") is None
    assert await members(state_store, keys.tenant_requests("default")) == {rid}
    assert await by_status(state_store, "default", "Pending") == {rid}
    assert await active(state_store, "default") == {rid}


async def test_a_submit_for_the_default_tenant_stores_it_by_name(state_store: StateStore) -> None:
    rid = await submit(state_store, tenant="default")
    assert await state_store.client.hget(keys.request(rid), "tenant") == "default"
    assert await by_status(state_store, "default", "Pending") == {rid}


async def test_the_tenant_quota_refuses_the_next_submit_and_writes_nothing(state_store: StateStore) -> None:
    for _ in range(2):
        await submit(state_store, tenant_max_active=2)
    before = await everything(state_store)
    res = await state_store.submit_request("hello", ClientRequestSpec(fingerprint=FP, tenant=A, tenant_max_active=2))
    assert (res.outcome, res.limit, res.request_id) == ("tenant_quota_exceeded", 2, None)
    assert await everything(state_store) == before


async def test_the_tenant_quota_counts_only_its_own_tenant_and_zero_means_none(state_store: StateStore) -> None:
    await submit(state_store, A, tenant_max_active=1)
    full = await state_store.submit_request("x", ClientRequestSpec(fingerprint=FP, tenant=A, tenant_max_active=1))
    other = await state_store.submit_request("x", ClientRequestSpec(fingerprint=FP, tenant=B, tenant_max_active=1))
    assert (full.outcome, other.outcome) == ("tenant_quota_exceeded", "created")
    for _ in range(3):
        await submit(state_store, A, tenant_max_active=0)  # 0 = no quota, whatever is active


async def test_a_finished_request_frees_a_place_in_the_tenant_quota(state_store: StateStore) -> None:
    _, lease = await running(state_store)
    spec = ClientRequestSpec(fingerprint=FP, tenant=A, tenant_max_active=1)
    assert (await state_store.submit_request("x", spec)).outcome == "tenant_quota_exceeded"
    assert (await state_store.finalize(lease, FINAL)).ok
    assert (await state_store.submit_request("x", spec)).outcome == "created"


async def test_an_idempotent_replay_is_answered_even_when_the_tenant_is_full(state_store: StateStore) -> None:
    spec = ClientRequestSpec(
        fingerprint=FP, tenant=A, tenant_max_active=1, idempotency_key_hash="k" * 8, body_hash="b" * 8
    )
    first = await state_store.submit_request("hello", spec)
    again = await state_store.submit_request("hello", spec)
    assert (first.outcome, again.outcome, again.request_id) == ("created", "replayed", first.request_id)


async def test_the_per_key_quota_still_applies_beside_the_tenant_quota(state_store: StateStore) -> None:
    await submit(state_store, max_active_requests=1)
    res = await state_store.submit_request("x", ClientRequestSpec(fingerprint=FP, tenant=A, max_active_requests=1))
    assert (res.outcome, res.limit) == ("quota_exceeded", 1)


@pytest.mark.parametrize(
    ("tenant", "priority"),
    [("Bad Tenant", None), ("UPPER", None), ("-x", None), ("a" * 33, None), (A, "urgent"), (A, "normal ")],
)
async def test_an_invalid_tenant_or_priority_is_refused_before_any_write(
    state_store: StateStore, tenant: str, priority: str | None
) -> None:
    before = await everything(state_store)
    with pytest.raises(ValueError):
        await state_store.submit_request("x", ClientRequestSpec(fingerprint=FP, tenant=tenant, priority=priority))
    assert await everything(state_store) == before


async def test_the_script_itself_refuses_a_bad_tenant_and_writes_nothing(state_store: StateStore) -> None:
    before = await everything(state_store)
    with pytest.raises(Exception, match="tenant"):
        await state_store._run(
            "create_request",
            [keys.request("r9"), keys.REQUESTS_PENDING, keys.REQUESTS_INDEX, keys.requests_by_status("Pending")],
            ["r9", "p", "", FP, "", "", "", "", "", "0", "0", "", "Not A Tenant", "0", ""],
        )
    assert await everything(state_store) == before


# =========================================================================== lifecycle (task 3.2)


async def test_commit_plan_moves_the_request_to_the_running_twin(state_store: StateStore) -> None:
    rid, _ = await running(state_store)
    assert await by_status(state_store, A, "Pending") == set()
    assert await by_status(state_store, A, "Running") == {rid}
    assert await members(state_store, keys.tenant_requests(A)) == {rid}  # the index twin keeps it
    assert await active(state_store, A) == {rid}


async def test_finalize_completed_moves_the_twin_and_leaves_the_active_set(state_store: StateStore) -> None:
    rid, lease = await running(state_store)
    assert (await state_store.finalize(lease, FINAL)).ok
    assert await by_status(state_store, A, "Running") == set()
    assert await by_status(state_store, A, "Completed") == {rid}
    assert await active(state_store, A) == set()
    assert await state_store.client.smembers(keys.client_active(FP)) == set()


async def test_a_failed_request_is_in_the_failed_twin(state_store: StateStore) -> None:
    rid, lease = await running(state_store)
    assert (await state_store.fail_request(lease, "boom", FAILED_FINAL)).ok
    assert await by_status(state_store, A, "Failed") == {rid} and await active(state_store, A) == set()


async def test_cancelling_a_pending_request_moves_it_to_the_cancelled_twin(state_store: StateStore) -> None:
    rid = await submit(state_store)
    assert (await state_store.cancel_request(rid, prices=None)).ok
    assert await by_status(state_store, A, "Pending") == set()
    assert await by_status(state_store, A, "Cancelled") == {rid}
    assert await active(state_store, A) == set()


async def test_cancelling_a_running_request_moves_it_to_the_cancelled_twin(state_store: StateStore) -> None:
    rid, _ = await running(state_store)
    assert (await state_store.cancel_request(rid, prices=None)).ok
    assert await by_status(state_store, A, "Running") == set()
    assert await by_status(state_store, A, "Cancelled") == {rid}


async def test_a_replay_reopens_the_request_in_the_pending_twin_and_the_active_set(state_store: StateStore) -> None:
    rid, lease = await running_with_failed_job(state_store)
    assert (await state_store.fail_request(lease, "job a failed", FAILED_FINAL)).ok
    assert await by_status(state_store, A, "Failed") == {rid} and await active(state_store, A) == set()
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    assert (await state_store.replay_dead_letter(DL1)).outcome == "accepted"
    assert await by_status(state_store, A, "Failed") == set()
    assert await by_status(state_store, A, "Pending") == {rid}
    assert await active(state_store, A) == {rid}  # back in, even beyond the quota
    assert (await state_store.reopen_request(await lease_of(state_store, rid))).ok
    assert await by_status(state_store, A, "Pending") == set()
    assert await by_status(state_store, A, "Running") == {rid}


async def test_purging_a_request_removes_every_twin_entry_and_its_active_member(
    state_store: StateStore, fake_redis_time: Any
) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    rid, lease = await running(state_store)
    assert (await state_store.finalize(lease, FINAL)).ok
    fake_redis_time.set_ms(ms("2026-11-12T08:00:10"))
    assert await state_store.purge_request(rid, ttl_ms=0) == "DELETED"
    assert await members(state_store, keys.tenant_requests(A)) == set()
    for status in ("Pending", "Running", "Completed", "Failed", "Cancelled"):
        assert await by_status(state_store, A, status) == set()
    assert await active(state_store, A) == set()


async def test_rejected_operations_change_no_tenant_key(state_store: StateStore) -> None:
    rid, lease = await running(state_store)
    stranger = lease.model_copy(update={"owner": "someone-else"})
    before = await tenant_keys(state_store)
    assert (await state_store.commit_plan(stranger, PLAN)).status is StoreStatus.LEASE_LOST
    assert (await state_store.finalize(stranger, FINAL)).status is StoreStatus.LEASE_LOST
    assert await tenant_keys(state_store) == before
    assert (await state_store.finalize(lease, FINAL)).ok
    after_finish = await tenant_keys(state_store)
    assert (await state_store.finalize(lease, FINAL)).status in {StoreStatus.REQUEST_TERMINAL, StoreStatus.LEASE_LOST}
    assert (await state_store.cancel_request(rid, prices=None)).status is StoreStatus.TERMINAL
    assert await tenant_keys(state_store) == after_finish


async def test_two_tenants_never_share_an_index_entry(state_store: StateStore) -> None:
    a1, a_lease = await running(state_store, A)
    b1 = await submit(state_store, B)
    a2 = await submit(state_store, A)
    assert (await state_store.finalize(a_lease, FINAL)).ok
    assert await members(state_store, keys.tenant_requests(A)) == {a1, a2}
    assert await members(state_store, keys.tenant_requests(B)) == {b1}
    assert await by_status(state_store, A, "Completed") == {a1}
    assert await by_status(state_store, B, "Completed") == set()
    assert await active(state_store, A) == {a2} and await active(state_store, B) == {b1}


async def test_the_tenant_indexes_add_up_to_the_global_ones(state_store: StateStore) -> None:
    ra, la = await running(state_store, A)
    rb, _ = await running(state_store, B)
    rd = await state_store.create_request("legacy")  # no tenant field: the default tenant
    await state_store.finalize(la, FINAL)
    await state_store.cancel_request(rb, prices=None)
    tenants = [A, B, "default"]
    assert await members(state_store, keys.REQUESTS_INDEX) == {ra, rb, rd}
    union: set[str] = set()
    for t in tenants:
        union |= await members(state_store, keys.tenant_requests(t))
    assert union == {ra, rb, rd}
    for status in ("Pending", "Running", "Completed", "Failed", "Cancelled"):
        expected = await members(state_store, keys.requests_by_status(status))
        got: set[str] = set()
        for t in tenants:
            got |= await by_status(state_store, t, status)
        assert got == expected, status


# =========================================================================== dead letters (task 3.3)


async def running_with_failed_job(store: StateStore, tenant: str | None = A) -> tuple[str, Lease]:
    rid = await submit(store, tenant)
    lease = await lease_of(store, rid)
    assert (await store.commit_plan(lease, PLAN)).ok
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    skipped = JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed")
    assert (await store.apply_transition(lease, TransitionBatch(jobs={"a": failed, "b": skipped}))).ok
    return rid, lease


async def dlq_twins(store: StateStore, tenant: str) -> dict[str, set[str]]:
    out = {"index": await members(store, keys.tenant_dlq_index(tenant))}
    for s in ("Pending", "Replayed", "Discarded"):
        out[s] = await members(store, keys.tenant_dlq_by_status(tenant, s))
    out["reason"] = await members(store, keys.tenant_dlq_by_reason(tenant, "retries_exhausted"))
    return out


async def test_a_dead_letter_of_an_existing_request_is_filed_under_the_requests_tenant(state_store: StateStore) -> None:
    rid, _ = await running_with_failed_job(state_store)
    assert (await state_store.create_dead_letter(fields(DL1, rid=rid))).created
    assert await state_store.client.hget(keys.dlq(DL1), "tenant") == A
    twins = await dlq_twins(state_store, A)
    assert twins == {"index": {DL1}, "Pending": {DL1}, "Replayed": set(), "Discarded": set(), "reason": {DL1}}
    assert await members(state_store, keys.DLQ_INDEX) == {DL1}  # the global index is unchanged


async def test_a_dead_letter_without_a_request_belongs_to_no_tenant(state_store: StateStore) -> None:
    await state_store.create_dead_letter(fields(DL1, rid=None))
    await state_store.create_dead_letter(fields(DL2, rid="no-such-request"))
    for dlid in (DL1, DL2):
        assert await state_store.client.hget(keys.dlq(dlid), "tenant") is None
    assert await state_store.client.keys("das:tenant:*:dlq:*") == []


async def test_a_dead_letter_of_a_request_without_a_tenant_field_is_filed_under_default(
    state_store: StateStore,
) -> None:
    rid = await state_store.create_request("legacy")
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    assert await state_store.client.hget(keys.dlq(DL1), "tenant") == "default"
    assert (await dlq_twins(state_store, "default"))["index"] == {DL1}


async def test_a_redelivered_dead_letter_changes_nothing(state_store: StateStore) -> None:
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    before = await tenant_keys(state_store)
    assert not (await state_store.create_dead_letter(fields(DL1, rid=rid))).created
    assert await tenant_keys(state_store) == before


async def test_discarding_moves_the_record_in_the_tenants_status_twins(state_store: StateStore) -> None:
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    assert (await state_store.discard_dead_letter(DL1)).outcome == "accepted"
    twins = await dlq_twins(state_store, A)
    assert (twins["Pending"], twins["Discarded"], twins["index"]) == (set(), {DL1}, {DL1})


async def test_replaying_moves_the_record_to_replayed_in_the_twins(state_store: StateStore) -> None:
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    assert (await state_store.replay_dead_letter(DL1)).outcome == "accepted"
    twins = await dlq_twins(state_store, A)
    assert (twins["Pending"], twins["Replayed"]) == (set(), {DL1})


async def test_a_reverted_record_returns_to_pending_in_the_twins(state_store: StateStore) -> None:
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    assert (await state_store.replay_dead_letter(DL1)).outcome == "accepted"
    assert (await state_store.cancel_request(rid, prices=None)).ok  # a Replay never applied is returned to Pending
    twins = await dlq_twins(state_store, A)
    assert (twins["Pending"], twins["Replayed"]) == ({DL1}, set())


async def test_purging_a_dead_letter_removes_every_twin_entry(state_store: StateStore, fake_redis_time: Any) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    await state_store.discard_dead_letter(DL1)
    fake_redis_time.set_ms(ms("2026-11-12T08:00:10"))
    assert await state_store.purge_dead_letter(DL1, ttl_ms=0) == "DELETED"
    empty = {"index": set(), "Pending": set(), "Replayed": set(), "Discarded": set(), "reason": set()}
    assert await dlq_twins(state_store, A) == empty


async def test_a_record_from_before_phase_5_gets_no_twin_entries_when_its_status_changes(
    state_store: StateStore,
) -> None:
    r = state_store.client
    legacy = {"status": "Pending", "reason": "retries_exhausted", "dead_lettered_at": "1000"}
    await r.hset(keys.dlq(DL1), mapping=legacy)
    await r.zadd(keys.DLQ_INDEX, {DL1: 1000})
    await r.zadd(keys.dlq_by_status("Pending"), {DL1: 1000})
    assert (await state_store.discard_dead_letter(DL1)).outcome == "accepted"
    assert await r.keys("das:tenant:*:dlq:*") == []  # nothing invented, nothing to clean up later


# =========================================================================== usage (task 3.4)


async def test_planning_usage_is_rolled_up_per_model_and_day(state_store: StateStore, fake_redis_time: Any) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    await running(state_store, A, plan_usage=[usage("plan", i=100, o=40), usage("plan", i=7, o=3)])
    assert await day(state_store, A, "20261112") == {
        "submitted": "1",
        "tokens/in/openai/gpt": "107",
        "tokens/out/openai/gpt": "43",
        "tokens/total/openai/gpt": "150",
    }


async def test_two_models_and_two_days_are_kept_apart(state_store: StateStore, fake_redis_time: Any) -> None:
    other = usage("plan", i=1, o=1).model_copy(update={"provider": "anthropic", "model": "claude"})
    fake_redis_time.set_ms(ms("2026-11-12T23:59:59"))
    _, lease = await running(state_store, A, plan_usage=[usage("plan", i=10, o=0), other])
    fake_redis_time.set_ms(ms("2026-11-13T00:00:01"))
    assert (await state_store.finalize(lease, FINAL, usage=[usage("summarize", i=5, o=5)])).ok
    first, second = await day(state_store, A, "20261112"), await day(state_store, A, "20261113")
    assert first["tokens/in/openai/gpt"] == "10" and first["tokens/in/anthropic/claude"] == "1"
    assert second == {
        "tokens/in/openai/gpt": "5", "tokens/out/openai/gpt": "5", "tokens/total/openai/gpt": "10",
        "finished/Completed": "1",
    }  # fmt: skip


async def test_a_job_result_is_counted_once_even_when_it_is_delivered_twice(
    state_store: StateStore, fake_redis_time: Any
) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    rid, lease = await running(state_store)
    batch = TransitionBatch(results=[result(rid, "a")], usage=[usage("job", i=5, o=6, job_id="a", attempt=1)])
    assert (await state_store.apply_transition(lease, batch)).ok
    assert (await state_store.apply_transition(lease, batch)).ok  # duplicate: the first result is kept, no new usage
    assert (await day(state_store, A, "20261112"))["tokens/total/openai/gpt"] == "11"


async def test_finished_counters_follow_the_terminal_status(state_store: StateStore, fake_redis_time: Any) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    _, done = await running(state_store, A)
    _, broken = await running(state_store, A)
    cancelled = await submit(state_store, A)
    await state_store.finalize(done, FINAL)
    await state_store.fail_request(broken, "boom", FAILED_FINAL)
    await state_store.cancel_request(cancelled, prices=None)
    counters = await day(state_store, A, "20261112")
    assert {k: v for k, v in counters.items() if k != "submitted"} == {
        "finished/Completed": "1", "finished/Failed": "1", "finished/Cancelled": "1",
    }  # fmt: skip
    assert counters["submitted"] == "3"


async def test_a_replay_adds_the_new_attempts_usage_and_one_more_terminal_count(
    state_store: StateStore, fake_redis_time: Any
) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    rid, lease = await running_with_failed_job(state_store)
    assert (await state_store.fail_request(lease, "job a failed", FAILED_FINAL)).ok
    await state_store.create_dead_letter(fields(DL1, rid=rid))
    await state_store.replay_dead_letter(DL1)
    lease2 = await lease_of(state_store, rid)
    await state_store.reopen_request(lease2)
    # The owner applies the accepted Replay; only then may the Request finalize (Phase 2 Req 13.15).
    assert (await state_store.apply_transition(lease2, TransitionBatch(replays_applied=[DL1]))).ok
    assert (await state_store.finalize(lease2, FINAL, usage=[usage("summarize", i=2, o=2)])).ok
    counters = await day(state_store, A, "20261112")
    assert (counters["submitted"], counters["finished/Failed"], counters["finished/Completed"]) == ("1", "1", "1")
    assert counters["tokens/total/openai/gpt"] == "4"  # only the new attempt's usage


async def test_a_request_without_a_tenant_is_counted_for_the_default_tenant(
    state_store: StateStore, fake_redis_time: Any
) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    await state_store.create_request("legacy")
    assert await day(state_store, "default", "20261112") == {"submitted": "1"}


async def test_the_retention_is_a_cluster_wide_setting_with_a_400_day_default(
    state_store: StateStore, fake_redis_time: Any
) -> None:
    fake_redis_time.set_ms(ms("2026-11-12T08:00:00"))
    await submit(state_store)
    assert 400 * 86_400 - 5 <= await state_store.client.ttl(keys.tenant_usage(A, "20261112")) <= 400 * 86_400
    await state_store.set_usage_retention(30)
    assert await state_store.client.get(keys.TENANT_USAGE_RETENTION) == "30"
    await submit(state_store)  # every write refreshes the expiry with the current setting
    assert 30 * 86_400 - 5 <= await state_store.client.ttl(keys.tenant_usage(A, "20261112")) <= 30 * 86_400


@pytest.mark.parametrize("bad", [0, -1, 3651, 29])
async def test_a_retention_outside_the_documented_range_is_refused(state_store: StateStore, bad: int) -> None:
    with pytest.raises(ValueError):
        await state_store.set_usage_retention(bad)
    assert await state_store.client.get(keys.TENANT_USAGE_RETENTION) is None
