# Feature: das-observability-operations, Property 20: Thứ tự kiểm tra Replay và Discard
"""Property 20: Replay and Discard check order.

For any generated state — record (absent, Pending, Replayed or Discarded, every
reason), Request (absent, missing from the record, or any Request_Status), Job (absent,
missing from the record, or any Job_State), record attempt vs. the Job's attempt count
(smaller, equal, larger, unknown) and Lease (none, expired or unexpired):

- the outcome and current state of ``replay_dead_letter.lua`` and
  ``discard_dead_letter.lua`` (real scripts on fakeredis) equal those of
  :func:`das.ops.replay.decide_replay` / :func:`das.ops.replay.decide_discard`;
- every rejection (and the idempotent Discard) leaves every key unchanged;
- an accepted Replay/Discard makes the changes of Req 13.1-13.3 / 13.11.

**Validates: Requirements 13.6, 13.7, 13.8, 13.11, 13.13**
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, given, note
from hypothesis import strategies as st

from das.models import JobRecord, JobState, RequestStatus
from das.models.dlq import DEAD_LETTER_REASONS, RECORD_STATUSES
from das.ops.replay import RequestView, decide_discard, decide_replay
from das.state import INBOX_REPLAY, StateStore, TransitionBatch, keys
from tests.unit.test_state_phase2_dlq import FINAL, everything, fields
from tests.unit.test_state_store import PLAN

DLID = "d" * 32
ORIGINAL_DISCARDED_AT = 1_600_000_000_123


@dataclass(frozen=True)
class Scenario:
    record: str | None  # None = no record, else Record_Status
    reason: str
    request: str  # "none" (record has no request_id), "missing", or a Request_Status
    job: str  # "none" (record has no job_id), "missing", or a Job_State
    job_attempt: int
    attempt_delta: int | None  # record attempt = job_attempt + delta; None = unknown
    lease: str  # "none", "expired", "active"


def biased(passing: st.SearchStrategy, failing: st.SearchStrategy) -> st.SearchStrategy:
    """Mostly values that pass a check, so every later check (and acceptance) is reached."""
    return st.sampled_from([True, True, True, False]).flatmap(
        lambda ok: passing if ok else failing
    )


scenarios = st.builds(
    Scenario,
    record=biased(st.just("Pending"), st.none() | st.sampled_from(RECORD_STATUSES)),
    reason=biased(
        st.sampled_from(["retries_exhausted", "delivery_limit"]),
        st.sampled_from(DEAD_LETTER_REASONS),
    ),
    request=biased(
        st.sampled_from(["Running", "Failed"]),
        st.sampled_from(["none", "missing", *(s.value for s in RequestStatus)]),
    ),
    job=biased(
        st.just("Failed"), st.sampled_from(["none", "missing", *(s.value for s in JobState)])
    ),
    job_attempt=st.integers(min_value=1, max_value=3),
    attempt_delta=biased(st.just(0), st.none() | st.sampled_from([-1, 0, 1])),
    lease=st.sampled_from(["none", "expired", "active"]),
)


def record_attempt(sc: Scenario) -> int | None:
    if sc.attempt_delta is None:
        return None
    return max(1, sc.job_attempt + sc.attempt_delta)


async def build(store: StateStore, sc: Scenario) -> str:
    """Write the scenario into Redis; returns the Request_ID (maybe nonexistent)."""
    r = store.client
    rid = "no-such-request"
    if sc.request not in ("none", "missing"):
        rid = await store.create_request("hello")
        lease = (await store.acquire_lease(rid, "m1", 30)).lease
        assert (await store.commit_plan(lease, PLAN)).ok
        if sc.job not in ("none", "missing"):
            job = JobRecord(
                job=PLAN.jobs[0], state=JobState(sc.job), attempt=sc.job_attempt, reason="x"
            )
            assert (await store.apply_transition(lease, TransitionBatch(jobs={"a": job}))).ok
        if sc.request == "Failed":
            assert (await store.fail_request(lease, "job a failed", FINAL)).ok
        elif sc.request != "Running":
            await r.hset(keys.request(rid), "status", sc.request)
        await r.delete(keys.lease(rid))
        if sc.lease != "none":
            now = await store.server_time()
            exp = now + 60 if sc.lease == "active" else now - 60
            await r.hset(
                keys.lease(rid), mapping={"owner": "m9", "epoch": "7", "expires_at": f"{exp:.6f}"}
            )
    if sc.record is not None:
        job_id = {"none": None, "missing": "zz"}.get(sc.job, "a")
        f = fields(
            DLID,
            None if sc.request == "none" else rid,
            reason=sc.reason,
            job_id=job_id,
            attempt=record_attempt(sc),
        )
        assert (await store.create_dead_letter(f)).created
        if sc.record != "Pending":
            await r.hset(keys.dlq(DLID), "status", sc.record)
            if sc.record == "Discarded":
                await r.hset(keys.dlq(DLID), "discarded_at", str(ORIGINAL_DISCARDED_AT))
    return rid


def model_inputs(sc: Scenario):
    rec = (
        None
        if sc.record is None
        else SimpleNamespace(status=sc.record, reason=sc.reason, attempt=record_attempt(sc))
    )
    if sc.request in ("none", "missing"):
        return rec, None, None, sc.lease == "active"
    req = RequestView(RequestStatus(sc.request))
    job = None
    if sc.job not in ("none", "missing"):
        job = JobRecord(job=PLAN.jobs[0], state=JobState(sc.job), attempt=sc.job_attempt)
    return rec, req, job, sc.lease == "active"


def non_dlq(snapshot: dict) -> dict:
    return {k: v for k, v in snapshot.items() if not k.startswith(keys.dlq(""))}


async def check_replay(sc: Scenario) -> None:
    store = StateStore(FakeRedis(server=FakeServer(), decode_responses=True))
    try:
        rid = await build(store, sc)
        expected = decide_replay(*model_inputs(sc))
        before = await everything(store)
        res = await store.replay_dead_letter(DLID)
        note(f"expected={expected} got={res}")
        event(expected.outcome)
        assert res.outcome == expected.outcome
        if not expected.accepted:
            assert res.current_state == expected.current_state
            assert await everything(store) == before
            return
        assert res.new_attempt == expected.new_attempt
        assert res.request_status_before is expected.request_status_before
        r = store.client
        rec = await store.get_dead_letter(DLID)
        assert rec.status == "Replayed" and rec.status_changed_at is not None
        assert await r.zscore(keys.dlq_by_status("Pending"), DLID) is None
        assert await r.zscore(keys.dlq_by_status("Replayed"), DLID) is not None
        entries = await store.read_inbox(rid)
        assert [e.kind for e in entries] == [INBOX_REPLAY]
        assert entries[0].payload() == {
            "dead_letter_id": DLID, "job_id": "a", "attempt": record_attempt(sc),
        }
        assert await r.smembers(keys.pending_replays(rid)) == {DLID}
        # The Gateway never touches Job state or Job_Results (Req 13.3, 13.4).
        assert await r.hgetall(keys.jobs(rid)) == before[keys.jobs(rid)]
        assert (await r.hgetall(keys.results(rid))) == before.get(keys.results(rid), {})
        snap = await store.load_snapshot(rid, include_transitions=True)
        if expected.request_status_before is RequestStatus.FAILED:
            # Req 13.2: Failed -> Pending, error/final_result moved to replay history.
            assert snap.status is RequestStatus.PENDING
            assert snap.error is None and snap.final_result is None
            assert snap.finished_at is None
            [hist] = snap.replay_history
            assert hist.dead_letter_id == DLID and hist.final_result == FINAL
            last = snap.request_transitions[-1]
            assert (last.from_status, last.to_status, last.dead_letter_id) == (
                RequestStatus.FAILED, RequestStatus.PENDING, DLID,
            )
            assert rid in await store.claimable_request_ids()
        else:
            # Req 13.3: a Running Request stays Running; its Lease owner applies the event.
            assert snap.status is RequestStatus.RUNNING
            assert snap.replay_history == []
            assert snap.request_transitions == []
    finally:
        await store.client.aclose()


async def check_discard(sc: Scenario) -> None:
    store = StateStore(FakeRedis(server=FakeServer(), decode_responses=True))
    try:
        await build(store, sc)
        rec, *_ = model_inputs(sc)
        expected = decide_discard(rec)
        before = await everything(store)
        res = await store.discard_dead_letter(DLID)
        note(f"expected={expected} got={res}")
        event(expected.outcome)
        assert res.outcome == expected.outcome
        if expected.outcome == "record_not_pending":
            assert res.status == expected.current_state
        if expected.outcome != "accepted":
            if expected.outcome == "already":
                assert res.discarded_at is not None
                assert int(res.discarded_at.timestamp() * 1000) == ORIGINAL_DISCARDED_AT
            assert await everything(store) == before
            return
        # Req 13.11: record Discarded with a discard time; nothing else changes.
        stored = await store.get_dead_letter(DLID)
        assert stored.status == "Discarded" and res.status == "Discarded"
        assert stored.discarded_at == res.discarded_at is not None
        r = store.client
        assert await r.zscore(keys.dlq_by_status("Pending"), DLID) is None
        assert await r.zscore(keys.dlq_by_status("Discarded"), DLID) is not None
        assert non_dlq(await everything(store)) == non_dlq(before)
    finally:
        await store.client.aclose()


@given(scenarios)
def test_replay_check_order_matches_decide_replay(sc: Scenario) -> None:
    asyncio.run(check_replay(sc))


@given(scenarios)
def test_discard_check_order_matches_decide_discard(sc: Scenario) -> None:
    asyncio.run(check_discard(sc))
