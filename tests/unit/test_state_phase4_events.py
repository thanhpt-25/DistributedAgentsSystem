"""Unit tests for the Event_Stream and Webhook_Delivery enqueue written by the
state-changing scripts (das-client-api tasks 4.5, 4.7; Phase 4 Req 6.1-6.5, 8.2)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from das.client.budget import Budget
from das.client.events import (
    TERMINAL_EVENT_TYPES,
    planned_event_data,
    terminal_event_data,
    transition_event_data,
)
from das.models import (
    FinalResult,
    JobPlan,
    JobRecord,
    JobReport,
    JobState,
    Lease,
    PlannedJob,
    RequestStatus,
    TokenUsage,
)
from das.models.usage import UsageRecord
from das.state import (
    ClientRequestSpec,
    StateStore,
    StoreStatus,
    TransitionBatch,
    WebhookState,
    keys,
)
from tests.unit.test_state_phase2_dlq import fields

PROMPT = "PROMPT-SECRET-7f3a"
INSTR_A = "INSTR-SECRET-alpha"
INSTR_B = "INSTR-SECRET-beta"
PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id="a", instruction=INSTR_A, capabilities=["research"]),
        PlannedJob(job_id="b", instruction=INSTR_B, capabilities=["summarize"], depends_on=["a"]),
    ]
)
FINAL = FinalResult(summary="done", jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED)])
FP = "cbabecbd"
DLID = "e" * 32
URL = "https://hooks.example.test/das"
SECRET_NAME = "hook-secret"


def usage_rec(i: int, o: int) -> UsageRecord:
    return UsageRecord(
        operation="summarize", provider="openai", model="gpt",
        usage=TokenUsage(input_tokens=i, output_tokens=o, total_tokens=i + o),
    )


async def events(store: StateStore, rid: str) -> list[tuple[str, str, dict[str, Any]]]:
    out = []
    for sid, f in await store.client.xrange(keys.events(rid)):
        out.append((sid, f["type"], json.loads(f["data"])))
    return out


async def types(store: StateStore, rid: str) -> list[str]:
    return [t for _, t, _ in await events(store, rid)]


async def submitted(
    store: StateStore, webhook: bool = False, events_max_len: int = 1000, budget: Budget | None = None
) -> str:
    spec = ClientRequestSpec(
        fingerprint=FP,
        budget=budget,
        webhook_url=URL if webhook else None,
        webhook_secret_name=SECRET_NAME if webhook else None,
        events_max_len=events_max_len,
    )
    res = await store.submit_request(PROMPT, spec)
    assert res.request_id is not None
    return res.request_id


async def leased(store: StateStore, rid: str) -> Lease:
    lease = (await store.acquire_lease(rid, "m1", 30)).lease
    assert lease is not None
    return lease


async def running(
    store: StateStore, webhook: bool = False, events_max_len: int = 1000, budget: Budget | None = None
):
    rid = await submitted(store, webhook, events_max_len, budget)
    lease = await leased(store, rid)
    assert (await store.commit_plan(lease, PLAN)).ok
    return rid, lease


async def job_entries(store: StateStore, rid: str):
    snap = await store.load_snapshot(rid, include_transitions=True)
    assert snap is not None and snap.transitions is not None
    return snap.transitions


async def assert_transitions_mirror_log(store: StateStore, rid: str) -> None:
    evs = [d for _, t, d in await events(store, rid) if t == "job.transition"]
    assert evs == [transition_event_data(e) for e in await job_entries(store, rid)]


# ------------------------------------------------------------------ commit_plan


async def test_commit_plan_appends_planned_then_one_transition_per_job(state_store: StateStore):
    rid, _ = await running(state_store)
    evs = await events(state_store, rid)
    assert [t for _, t, _ in evs] == ["request.created", "request.planned", "job.transition", "job.transition"]
    assert evs[1][2] == planned_event_data(PLAN)
    entries = await job_entries(state_store, rid)
    assert [d for _, _, d in evs[2:]] == [transition_event_data(e) for e in entries]
    assert all(d["from"] is None and d["to"] == "Waiting" for _, _, d in evs[2:])
    assert evs[2][2]["worker_id"] is None


# ------------------------------------------------------------------ apply_transition


async def test_apply_transition_appends_one_event_per_state_change(state_store: StateStore):
    rid, lease = await running(state_store)
    snap = await state_store.load_snapshot(rid)
    jobs = {
        "a": snap.jobs["a"].model_copy(update={"state": JobState.DISPATCHED, "attempt": 1, "worker_id": "w1"}),
        "b": snap.jobs["b"].model_copy(update={"state": JobState.SKIPPED}),
    }
    before = len(await events(state_store, rid))
    assert (await state_store.apply_transition(lease, TransitionBatch(jobs=jobs))).ok
    evs = await events(state_store, rid)
    assert [t for _, t, _ in evs[before:]] == ["job.transition", "job.transition"]
    assert evs[before][2]["worker_id"] == "w1" and evs[before][2]["from"] == "Waiting"
    await assert_transitions_mirror_log(state_store, rid)

    # Same states again: no transition entry, no event.
    count = len(evs)
    assert (await state_store.apply_transition(lease, TransitionBatch(jobs=jobs))).ok
    assert len(await events(state_store, rid)) == count


async def test_lease_lost_batch_appends_nothing(state_store: StateStore):
    rid, lease = await running(state_store)
    snap = await state_store.load_snapshot(rid)
    jobs = {"a": snap.jobs["a"].model_copy(update={"state": JobState.READY})}
    stale = lease.model_copy(update={"epoch": lease.epoch + 1})
    before = await events(state_store, rid)
    res = await state_store.apply_transition(stale, TransitionBatch(jobs=jobs))
    assert res.status is StoreStatus.LEASE_LOST
    assert await events(state_store, rid) == before


# ------------------------------------------------------------------ finalize


async def test_finalize_completed_appends_terminal_event(state_store: StateStore):
    rid, lease = await running(state_store)
    assert (await state_store.finalize(lease, FINAL, usage=[usage_rec(3, 4), usage_rec(1, 1)])).ok
    evs = await events(state_store, rid)
    assert evs[-1][1] == "request.completed"
    records = await state_store.load_usage(rid)
    assert evs[-1][2] == terminal_event_data(RequestStatus.COMPLETED, None, records, None, None)
    assert evs[-1][2]["usage"] == {"input_tokens": 4, "output_tokens": 5, "total_tokens": 9}
    assert "budget" not in evs[-1][2]


async def test_finalize_with_budget_view_splices_it(state_store: StateStore):
    rid, lease = await running(state_store)
    view = {"limits": {"max_total_tokens": 100}, "used": {"total_tokens": 7, "cost": None}, "exceeded": False}
    assert (await state_store.finalize(lease, FINAL, usage=[usage_rec(3, 4)], budget_view=view)).ok
    _, t, data = (await events(state_store, rid))[-1]
    assert t == "request.completed" and data["budget"] == view


async def test_finalize_failed_carries_error(state_store: StateStore):
    rid, lease = await running(state_store)
    assert (await state_store.fail_request(lease, "budget_exceeded")).ok
    _, t, data = (await events(state_store, rid))[-1]
    assert t == "request.failed"
    assert data == terminal_event_data(RequestStatus.FAILED, "budget_exceeded", [], None, None)


async def test_finalize_rejects_malformed_budget_json_before_writing(state_store: StateStore):
    from das.state.scripts import script_source
    from das.state.store import finalize_keys

    rid, lease = await running(state_store)
    script = state_store.client.register_script(script_source("finalize"))
    before = await state_store.load_snapshot(rid, include_transitions=True)
    before_events = await events(state_store, rid)
    args = [rid, lease.owner, lease.epoch, "Completed", FINAL.model_dump_json(), "", "", "{bad"]
    with pytest.raises(Exception):  # noqa: B017
        await script(keys=finalize_keys(rid, RequestStatus.COMPLETED), args=args)
    assert await state_store.load_snapshot(rid, include_transitions=True) == before
    assert await events(state_store, rid) == before_events


# ------------------------------------------------------------------ cancel


async def test_cancel_appends_transitions_then_cancelled(state_store: StateStore):
    rid, _ = await running(state_store, budget=Budget(max_total_tokens=5))
    before = len(await events(state_store, rid))
    view = {"limits": {"max_total_tokens": 5}, "used": {"total_tokens": 0, "cost": None}, "exceeded": False}
    assert (await state_store.cancel_request(rid, prices=None)).ok
    evs = await events(state_store, rid)
    assert [t for _, t, _ in evs[before:]] == ["job.transition", "job.transition", "request.cancelled"]
    assert all(d["to"] == "Cancelled" for _, _, d in evs[before:-1])
    assert evs[-1][2] == {**terminal_event_data(RequestStatus.CANCELLED, None, [], None, None), "budget": view}
    await assert_transitions_mirror_log(state_store, rid)


async def test_cancel_pending_request_without_budget(state_store: StateStore):
    rid = await submitted(state_store)
    assert (await state_store.cancel_request(rid)).ok
    evs = await events(state_store, rid)
    assert [t for _, t, _ in evs] == ["request.created", "request.cancelled"]
    assert "budget" not in evs[-1][2]


class RacingStore(StateStore):
    """Before the first ``cancel_request`` script run, a concurrent owner adds a
    UsageRecord and changes Job ``a``, so that run returns CONFLICT."""

    def __init__(self, client: Any) -> None:
        super().__init__(client)
        self.cancel_runs = 0

    async def _run(self, name: str, keys_: Any, args: Any) -> list[Any]:
        if name == "cancel_request":
            self.cancel_runs += 1
            if self.cancel_runs == 1:
                rid = args[0]
                rec = usage_rec(3, 4)
                r = self.client
                await r.rpush(keys.usage(rid), rec.model_dump_json(exclude_none=True))
                await r.hincrby(keys.request(rid), "usage_input", 3)
                await r.hincrby(keys.request(rid), "usage_output", 4)
                await r.hincrby(keys.request(rid), "usage_total", 7)
                snap = await self.load_snapshot(rid)
                ready = snap.jobs["a"].model_copy(update={"state": JobState.READY})
                await r.hset(keys.jobs(rid), "a", ready.model_dump_json())
        return await super()._run(name, keys_, args)


async def test_cancel_recomputes_budget_used_after_conflict(state_store: StateStore):
    store = RacingStore(state_store.client)
    rid, _ = await running(store, budget=Budget(max_total_tokens=100))
    assert (await store.cancel_request(rid)).ok
    assert store.cancel_runs == 2  # one CONFLICT, then OK
    _, t, data = (await events(store, rid))[-1]
    assert t == "request.cancelled"
    assert data["usage"]["total_tokens"] == 7
    assert data["budget"]["used"]["total_tokens"] == data["usage"]["total_tokens"]
    assert data["budget"] == {
        "limits": {"max_total_tokens": 100}, "used": {"total_tokens": 7, "cost": None}, "exceeded": False,
    }


async def test_budget_view_json_rejects_nan():
    from das.state.store import _budget_view_json

    with pytest.raises(ValueError):
        _budget_view_json({"used": float("nan")})


@pytest.mark.parametrize("bad", ["{bad", "[]", "[1]", "  [ ]", "7"])
async def test_cancel_rejects_malformed_budget_json_before_writing(state_store: StateStore, bad: str):
    from das.state.scripts import script_source
    from das.state.store import cancel_request_keys

    rid, _ = await running(state_store)
    snap = await state_store.load_snapshot(rid)
    args = [rid, "[]", bad]
    for jid, rec in snap.jobs.items():
        args += [jid, rec.model_dump_json(), rec.model_copy(update={"state": JobState.CANCELLED}).model_dump_json()]
    script = state_store.client.register_script(script_source("cancel_request"))
    before_events = await events(state_store, rid)
    with pytest.raises(Exception):  # noqa: B017
        await script(keys=cancel_request_keys(rid), args=args)
    assert await state_store.get_status(rid) is RequestStatus.RUNNING
    assert await events(state_store, rid) == before_events


# ------------------------------------------------------------------ replay + reopen


async def failed_with_dead_letter(store: StateStore, webhook: bool = False) -> tuple[str, Lease]:
    rid, lease = await running(store, webhook)
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    skipped = JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed")
    assert (await store.apply_transition(lease, TransitionBatch(jobs={"a": failed, "b": skipped}))).ok
    assert (await store.fail_request(lease, "job a failed")).ok
    await store.client.delete(keys.lease(rid))
    assert (await store.create_dead_letter(fields(DLID, rid))).created
    return rid, lease


async def apply_replay(store: StateStore, rid: str, lease: Lease) -> None:
    """The owner applies the accepted Replay: Job a Failed -> Ready, pending_replays cleared."""
    snap = await store.load_snapshot(rid)
    ready = snap.jobs["a"].model_copy(update={"state": JobState.READY})
    batch = TransitionBatch(jobs={"a": ready}, tag=DLID, replays_applied=[DLID])
    assert (await store.apply_transition(lease, batch)).ok


async def reopen_and_finish(store: StateStore, rid: str) -> None:
    assert (await store.replay_dead_letter(DLID)).outcome == "accepted"
    lease = await leased(store, rid)
    assert (await store.reopen_request(lease)).ok
    await apply_replay(store, rid, lease)
    assert (await store.finalize(lease, FINAL)).ok


async def test_reopen_appends_reopened_and_second_finalize_adds_terminal(state_store: StateStore):
    rid, _ = await failed_with_dead_letter(state_store)
    assert (await state_store.replay_dead_letter(DLID)).outcome == "accepted"
    evs = await events(state_store, rid)
    assert [t for _, t, _ in evs[-2:]] == ["request.failed", "request.reopened"]
    assert evs[-1][2] == {"status": "Pending"}
    lease = await leased(state_store, rid)
    assert (await state_store.reopen_request(lease)).ok
    assert await events(state_store, rid) == evs  # reopen_request appends nothing (Req 6.4)
    await apply_replay(state_store, rid, lease)
    assert (await state_store.finalize(lease, FINAL)).ok
    ts = await types(state_store, rid)
    assert ts[-4:] == ["request.failed", "request.reopened", "job.transition", "request.completed"]
    await assert_transitions_mirror_log(state_store, rid)
    assert sum(t in ("request.failed", "request.completed") for t in ts) == 2


async def test_replay_then_cancel_before_resumption(state_store: StateStore):
    """Req 6.4: the Replay records the reopen, so a cancel before a Manager resumes
    the Request still leaves request.reopened between the two Terminal_Events."""
    rid, _ = await failed_with_dead_letter(state_store)
    assert (await state_store.replay_dead_letter(DLID)).outcome == "accepted"
    assert (await state_store.cancel_request(rid)).ok
    ts = await types(state_store, rid)
    assert ts[-3:] == ["request.failed", "request.reopened", "request.cancelled"]
    reopen_at = len(ts) - 1 - ts[::-1].index("request.reopened")
    assert sum(t in TERMINAL_EVENT_TYPES for t in ts[reopen_at + 1:]) == 1
    assert ts.count("request.reopened") == 1


async def test_replay_of_running_request_appends_no_event(state_store: StateStore):
    rid, lease = await running(state_store)
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    assert (await state_store.apply_transition(lease, TransitionBatch(jobs={"a": failed}))).ok
    assert (await state_store.create_dead_letter(fields(DLID, rid))).created
    before = await events(state_store, rid)
    assert (await state_store.replay_dead_letter(DLID)).outcome == "accepted"
    assert await state_store.get_status(rid) is RequestStatus.RUNNING
    assert await events(state_store, rid) == before


async def test_event_data_never_contains_prompt_or_instructions(state_store: StateStore):
    rid, _ = await failed_with_dead_letter(state_store, webhook=True)
    await reopen_and_finish(state_store, rid)
    raw = [f["data"] for _, f in await state_store.client.xrange(keys.events(rid))]
    hook = await state_store.client.hget(keys.webhook(rid), "data")
    for text in [*raw, hook]:
        for secret in (PROMPT, INSTR_A, INSTR_B):
            assert secret not in text


# ------------------------------------------------------------------ webhook


async def test_finalize_with_webhook_target_enqueues_delivery(fake_redis_time, state_store: StateStore):
    fake_redis_time.set_ms(1_700_000_000_000)
    rid, lease = await running(state_store, webhook=True)
    fake_redis_time.set_ms(1_700_000_012_345)
    assert (await state_store.finalize(lease, FINAL)).ok
    assert await state_store.get_webhook(rid) == WebhookState("pending", 0, None)
    score = await state_store.client.zscore(keys.WEBHOOKS_DUE, rid)
    assert score == pytest.approx(1_700_000_012.345)
    h = await state_store.client.hgetall(keys.webhook(rid))
    last_id, last_type, last_data = (await events(state_store, rid))[-1]
    assert h["event_id"] == last_id
    assert h["type"] == last_type == "request.completed"
    assert json.loads(h["data"]) == last_data
    assert (h["url"], h["secret_name"], h["claim"], h["last_status"]) == (URL, SECRET_NAME, "0", "")


async def test_cancel_with_webhook_target_enqueues_delivery(state_store: StateStore):
    rid, _ = await running(state_store, webhook=True)
    assert (await state_store.cancel_request(rid)).ok
    h = await state_store.client.hgetall(keys.webhook(rid))
    assert h["type"] == "request.cancelled" and h["state"] == "pending"
    assert await state_store.client.zscore(keys.WEBHOOKS_DUE, rid) is not None


async def test_no_webhook_target_writes_no_delivery(state_store: StateStore):
    rid, lease = await running(state_store)
    assert (await state_store.finalize(lease, FINAL)).ok
    assert await state_store.get_webhook(rid) is None
    assert not await state_store.client.exists(keys.webhook(rid))
    assert await state_store.client.zscore(keys.WEBHOOKS_DUE, rid) is None
    legacy = await state_store.create_request("p")
    assert (await state_store.cancel_request(legacy)).ok
    assert await state_store.get_webhook(legacy) is None
    assert await state_store.client.zcard(keys.WEBHOOKS_DUE) == 0


async def test_finalizing_reopened_request_resets_delivery(state_store: StateStore):
    rid, _ = await failed_with_dead_letter(state_store, webhook=True)
    r = state_store.client
    await r.hset(keys.webhook(rid), mapping={"state": "delivered", "attempts": 3, "last_status": "200",
                                             "claim": 2, "stale_field": "x"})
    await r.zrem(keys.WEBHOOKS_DUE, rid)
    await reopen_and_finish(state_store, rid)
    assert await state_store.get_webhook(rid) == WebhookState("pending", 0, None)
    h = await r.hgetall(keys.webhook(rid))
    assert "stale_field" not in h and h["claim"] == "0" and h["type"] == "request.completed"
    assert h["event_id"] == (await events(state_store, rid))[-1][0]
    assert await r.zscore(keys.WEBHOOKS_DUE, rid) is not None


async def test_get_webhook_maps_last_status(state_store: StateStore):
    rid, lease = await running(state_store, webhook=True)
    assert (await state_store.finalize(lease, FINAL)).ok
    await state_store.client.hset(keys.webhook(rid), mapping={"state": "failed", "attempts": 5, "last_status": "503"})
    assert await state_store.get_webhook(rid) == WebhookState("failed", 5, "503")


async def test_purge_removes_outbox_entry(fake_redis_time, state_store: StateStore):
    fake_redis_time.set_ms(1_700_000_000_000)
    rid, lease = await running(state_store, webhook=True)
    assert (await state_store.finalize(lease, FINAL)).ok
    assert await state_store.client.zscore(keys.WEBHOOKS_DUE, rid) is not None
    fake_redis_time.advance(120)
    assert await state_store.purge_request(rid, 60_000) == "DELETED"
    assert await state_store.client.zscore(keys.WEBHOOKS_DUE, rid) is None
    assert not await state_store.client.exists(keys.webhook(rid), keys.events(rid))


async def test_delete_request_removes_outbox_entry(state_store: StateStore):
    rid, lease = await running(state_store, webhook=True)
    assert (await state_store.finalize(lease, FINAL)).ok
    assert await state_store.client.zscore(keys.WEBHOOKS_DUE, rid) is not None
    await state_store.delete_request(rid)
    assert await state_store.client.zscore(keys.WEBHOOKS_DUE, rid) is None
    assert await state_store.next_webhook_due() is None
    assert not await state_store.client.exists(keys.webhook(rid), keys.events(rid))


# ------------------------------------------------------------------ rejected calls


async def side_effects(store: StateStore, rid: str) -> tuple[Any, ...]:
    """Event_Stream, Active_Set and Webhook_Outbox, compared before and after a rejected call."""
    r = store.client
    return (
        await r.xlen(keys.events(rid)),
        await r.xrange(keys.events(rid)),
        await r.smembers(keys.client_active(FP)),
        await r.zrange(keys.WEBHOOKS_DUE, 0, -1, withscores=True),
    )


async def test_cancel_terminal_changes_nothing(state_store: StateStore):
    rid, lease = await running(state_store, webhook=True)
    await submitted(state_store)  # keeps the Active_Set non-empty
    assert (await state_store.finalize(lease, FINAL)).ok
    before = await side_effects(state_store, rid)
    res = await state_store.cancel_request(rid)
    assert res.status is StoreStatus.TERMINAL and res.request_status is RequestStatus.COMPLETED
    assert await side_effects(state_store, rid) == before


async def test_cancel_not_found_changes_nothing(state_store: StateStore):
    rid, lease = await running(state_store, webhook=True)
    assert (await state_store.finalize(lease, FINAL)).ok
    await submitted(state_store)
    before = await side_effects(state_store, "missing"), await side_effects(state_store, rid)
    assert (await state_store.cancel_request("missing")).status is StoreStatus.NOT_FOUND
    assert (await side_effects(state_store, "missing"), await side_effects(state_store, rid)) == before
    assert not await state_store.client.exists(keys.events("missing"))


async def test_cancel_script_conflict_changes_nothing(state_store: StateStore):
    from das.state.scripts import script_source
    from das.state.store import cancel_request_keys

    rid, _ = await running(state_store, webhook=True)
    snap = await state_store.load_snapshot(rid)
    args = [rid, "[]", ""]
    for jid, rec in snap.jobs.items():
        stale = rec.model_copy(update={"attempt": rec.attempt + 1}).model_dump_json()  # not the stored JSON
        args += [jid, stale, rec.model_copy(update={"state": JobState.CANCELLED}).model_dump_json()]
    script = state_store.client.register_script(script_source("cancel_request"))
    before = await side_effects(state_store, rid)
    assert list(await script(keys=cancel_request_keys(rid), args=args)) == ["CONFLICT"]
    assert await side_effects(state_store, rid) == before
    assert await state_store.get_status(rid) is RequestStatus.RUNNING


async def test_finalize_replay_pending_changes_nothing(state_store: StateStore):
    rid, lease = await running(state_store, webhook=True)
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    assert (await state_store.apply_transition(lease, TransitionBatch(jobs={"a": failed}))).ok
    assert (await state_store.create_dead_letter(fields(DLID, rid))).created
    assert (await state_store.replay_dead_letter(DLID)).outcome == "accepted"
    before = await side_effects(state_store, rid)
    assert (await state_store.finalize(lease, FINAL)).status is StoreStatus.REPLAY_PENDING
    assert await side_effects(state_store, rid) == before


async def test_finalize_completed_without_final_result_changes_nothing(state_store: StateStore):
    rid, lease = await running(state_store, webhook=True)
    before = await side_effects(state_store, rid)
    assert (await state_store.finalize(lease, None)).status is StoreStatus.INVALID_STATE
    assert await side_effects(state_store, rid) == before
    assert await state_store.get_status(rid) is RequestStatus.RUNNING


# ------------------------------------------------------------------ trimming


async def test_stream_is_trimmed_to_events_max_len(state_store: StateStore):
    rid, lease = await running(state_store, events_max_len=100)  # 4 events so far
    snap = await state_store.load_snapshot(rid)
    rec = snap.jobs["a"]
    states = [JobState.READY, JobState.WAITING]
    for i in range(146):
        rec = rec.model_copy(update={"state": states[i % 2]})
        assert (await state_store.apply_transition(lease, TransitionBatch(jobs={"a": rec}))).ok
    assert len(await state_store.get_transitions(rid)) == 148  # 150 events appended
    assert await state_store.client.xlen(keys.events(rid)) == 100
