"""``replan.lua`` and ``StateStore.replan``: swap the unfinished part of a plan atomically
(das-smarter-agents task 3.2; Phase 6 Req 3.4, 3.6, 3.7).

A Request with plan a -> b where a Succeeded and b Failed is replanned into a -> c: ``a`` and its result are kept, ``b``
leaves the plan, ``c`` is new and Waiting, and the Request stays Running."""

from __future__ import annotations

import json

import pytest
import redis.exceptions

from das.client.events import EVENT_TYPES, replanned_event_data, transition_event_data
from das.models import JobPlan, JobRecord, JobResultPayload, JobState, PlannedJob, RequestStatus, TokenUsage
from das.models.usage import UsageRecord
from das.state import StateStore, StoreStatus, TransitionBatch, keys
from das.state.store import _write_result, replan_keys
from tests.unit.test_state_phase2_dlq import DL1, everything, fields
from tests.unit.test_state_phase4_events import PLAN, events, job_entries, leased, running, submitted

A, B = PLAN.jobs
C = PlannedJob(job_id="c", instruction="do c instead", capabilities=["research"], depends_on=["a"])
NEW_PLAN = JobPlan(jobs=[A, C])


def result(rid: str, job_id: str, attempt: int, status: str) -> JobResultPayload:
    return JobResultPayload(
        request_id=rid, job_id=job_id, attempt=attempt, worker_id="w1", status=status,  # type: ignore[arg-type]
        content=f"out-{job_id}-{attempt}", error=None if status == "Succeeded" else "boom",
        usage=TokenUsage(input_tokens=2, output_tokens=3, total_tokens=5), duration_ms=1,
    )


def plan_usage(i: int = 10, o: int = 4) -> UsageRecord:
    return UsageRecord(
        operation="plan", provider="openai", model="gpt",
        usage=TokenUsage(input_tokens=i, output_tokens=o, total_tokens=i + o),
    )


async def finished(store: StateStore, b_state: JobState = JobState.FAILED):
    """A Running Request whose job a Succeeded (attempt 1) and whose job b ended in ``b_state`` (attempt 3)."""
    rid, lease = await running(store)
    batch = TransitionBatch(
        jobs={
            "a": JobRecord(job=A, state=JobState.SUCCEEDED, attempt=1),
            "b": JobRecord(job=B, state=b_state, attempt=3),
        },
        results=[result(rid, "a", 1, "Succeeded"), result(rid, "b", 3, "Failed")],
    )
    assert (await store.apply_transition(lease, batch)).ok
    return rid, lease


async def do_replan(store: StateStore, lease, plan: JobPlan = NEW_PLAN, **kw):  # noqa: ANN001, ANN201
    args = {"kept": ["a"], "failed": ["b"], "previous_replans": 0, **kw}
    return await store.replan(lease, plan, [plan_usage()], **args)


# --------------------------------------------------------------------------- the swap (Req 3.4)


async def test_the_unfinished_part_is_replaced_and_the_request_stays_running(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    out = await do_replan(state_store, lease)
    assert out.ok and out.at is not None
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and snap.status is RequestStatus.RUNNING
    assert snap.plan == NEW_PLAN
    assert list(snap.jobs) == ["a", "c"]  # b is gone, a and c are in the plan's order
    assert snap.jobs["a"].state is JobState.SUCCEEDED and snap.jobs["a"].attempt == 1  # untouched
    assert snap.jobs["c"].state is JobState.WAITING and snap.jobs["c"].attempt == 0
    assert snap.job_count == 2 and snap.replans == 1


async def test_the_kept_job_keeps_its_result_and_the_replaced_jobs_results_stay_as_history(
    state_store: StateStore,
) -> None:
    rid, lease = await finished(state_store)
    assert (await do_replan(state_store, lease)).ok
    snap = await state_store.load_snapshot(rid)
    assert snap is not None
    assert snap.results[("a", 1)].content == "out-a-1"
    assert ("b", 3) in snap.results  # audit: what the failed job produced is not erased


async def test_the_new_job_gets_a_waiting_transition_and_event_and_the_log_still_mirrors_the_stream(
    state_store: StateStore,
) -> None:
    rid, lease = await finished(state_store)
    assert (await do_replan(state_store, lease)).ok
    entries = await job_entries(state_store, rid)
    assert [(e.job_id, e.to_state) for e in entries][-1] == ("c", JobState.WAITING)
    evs = [d for _, t, d in await events(state_store, rid) if t == "job.transition"]
    assert evs == [transition_event_data(e) for e in entries]  # one event per logged transition, as always


async def test_a_replanned_event_names_the_failed_and_the_new_jobs(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    assert (await do_replan(state_store, lease)).ok
    evs = await events(state_store, rid)
    replanned = [d for _, t, d in evs if t == "request.replanned"]
    assert replanned == [replanned_event_data(1, ["b"], ["c"])]
    assert replanned[0] == {"replans": 1, "failed_jobs": ["b"], "new_jobs": ["c"]}
    order = [t for _, t, _ in evs]
    assert order[-2:] == ["request.replanned", "job.transition"]  # the event, then the new job's transition


async def test_the_planning_usage_is_recorded_with_the_request(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    before = await state_store.load_snapshot(rid, include_usage=True)
    assert before is not None
    assert (await do_replan(state_store, lease)).ok
    after = await state_store.load_snapshot(rid, include_usage=True)
    assert after is not None and after.usage is not None and before.usage is not None
    assert len(after.usage) == len(before.usage) + 1
    assert after.usage[-1].operation == "plan" and after.usage[-1].usage.input_tokens == 10
    assert after.usage_totals.input_tokens == before.usage_totals.input_tokens + 10


async def test_the_request_stays_in_its_running_indexes(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    assert (await do_replan(state_store, lease)).ok
    r = state_store.client
    assert rid in await r.zrange(keys.requests_by_status("Running"), 0, -1)
    assert rid not in await r.zrange(keys.requests_by_status("Failed"), 0, -1)
    assert rid in await r.smembers(keys.tenant_active("default"))


async def test_a_skipped_job_is_replaced_too(state_store: StateStore) -> None:
    rid, lease = await finished(state_store, b_state=JobState.SKIPPED)
    assert (await do_replan(state_store, lease)).ok
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and list(snap.jobs) == ["a", "c"]


async def test_a_second_replan_counts_two(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    assert (await do_replan(state_store, lease)).ok
    d = PlannedJob(job_id="d", instruction="and again", capabilities=["research"], depends_on=["a"])
    second = JobPlan(jobs=[A, d])
    early = await state_store.replan(lease, second, [], kept=["a"], failed=["c"], previous_replans=1)
    assert early.status is StoreStatus.INVALID_STATE  # c is still Waiting: not finished, so not replaceable
    batch = TransitionBatch(jobs={"c": JobRecord(job=C, state=JobState.FAILED, attempt=3)})
    assert (await state_store.apply_transition(lease, batch)).ok
    assert (await state_store.replan(lease, second, [], kept=["a"], failed=["c"], previous_replans=1)).ok
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and snap.replans == 2 and list(snap.jobs) == ["a", "d"]
    assert [data for _, t, data in await events(state_store, rid) if t == "request.replanned"][-1]["replans"] == 2


async def test_a_request_never_replanned_reads_zero(state_store: StateStore) -> None:
    rid, _ = await running(state_store)
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and snap.replans == 0


async def test_replans_cannot_be_set_through_apply_transition(state_store: StateStore) -> None:
    _, lease = await running(state_store)
    with pytest.raises(ValueError):
        await state_store.apply_transition(lease, TransitionBatch(request_fields={keys.F_REPLANS: "5"}))


# --------------------------------------------------------------------------- refusals write nothing (Req 3.4)


async def refused_and_unchanged(store: StateStore, call) -> object:  # noqa: ANN001
    before = await everything(store)
    out = await call()
    assert await everything(store) == before
    return out


async def test_a_stale_lease_is_refused(state_store: StateStore) -> None:
    _, lease = await finished(state_store)
    stale = lease.model_copy(update={"epoch": lease.epoch + 1})
    out = await refused_and_unchanged(state_store, lambda: do_replan(state_store, stale))
    assert out.status is StoreStatus.LEASE_LOST  # type: ignore[attr-defined]


async def test_a_terminal_request_is_refused(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    assert (await state_store.cancel_request(rid)).ok
    out = await refused_and_unchanged(state_store, lambda: do_replan(state_store, lease))
    assert out.status in (StoreStatus.REQUEST_TERMINAL, StoreStatus.LEASE_LOST)  # type: ignore[attr-defined]


async def test_a_pending_request_is_refused(state_store: StateStore) -> None:
    """Only its status is wrong: it has no jobs yet, so a plan of one new job would otherwise be fine."""
    rid = await submitted(state_store)
    lease = await leased(state_store, rid)
    only_new = JobPlan(jobs=[PlannedJob(job_id="n", instruction="x", capabilities=["research"])])
    out = await refused_and_unchanged(
        state_store, lambda: state_store.replan(lease, only_new, [], kept=[], failed=[], previous_replans=0)
    )
    assert out.status is StoreStatus.INVALID_STATE and out.detail == "Pending"  # type: ignore[attr-defined]


@pytest.mark.parametrize("state", [JobState.DISPATCHED, JobState.READY, JobState.WAITING])
async def test_a_request_with_jobs_still_in_flight_is_refused(state_store: StateStore, state: JobState) -> None:
    _, lease = await finished(state_store, b_state=state)
    out = await refused_and_unchanged(state_store, lambda: do_replan(state_store, lease))
    assert out.status is StoreStatus.INVALID_STATE and "jobs_active" in (out.detail or "")  # type: ignore[attr-defined]


async def test_a_wrong_previous_count_is_refused(state_store: StateStore) -> None:
    _, lease = await finished(state_store)
    out = await refused_and_unchanged(state_store, lambda: do_replan(state_store, lease, previous_replans=1))
    assert out.status is StoreStatus.INVALID_STATE and "replans" in (out.detail or "")  # type: ignore[attr-defined]


async def test_a_new_job_that_reuses_the_id_of_a_replaced_job_is_refused(state_store: StateStore) -> None:
    """Results are keyed by (job_id, attempt): a new ``b`` would meet the old ``b``'s results and be taken for a
    duplicate."""
    _, lease = await finished(state_store)
    reused = PlannedJob(job_id="b", instruction="again", capabilities=["research"], depends_on=["a"])
    out = await refused_and_unchanged(
        state_store, lambda: do_replan(state_store, lease, JobPlan(jobs=[A, reused]))
    )
    assert out.status is StoreStatus.INVALID_STATE and "invalid_plan" in (out.detail or "")  # type: ignore[attr-defined]


async def test_a_plan_that_drops_a_succeeded_job_is_refused(state_store: StateStore) -> None:
    _, lease = await finished(state_store)
    out = await refused_and_unchanged(
        state_store,
        lambda: state_store.replan(lease, JobPlan(jobs=[C]), [], kept=[], failed=["b"], previous_replans=0),
    )
    assert out.status is StoreStatus.INVALID_STATE and "invalid_plan" in (out.detail or "")  # type: ignore[attr-defined]


async def raw_replan(store: StateStore, rid: str, lease, plan: JobPlan, new: list[PlannedJob]):  # noqa: ANN001, ANN201
    """Call the script with ``new`` as its new jobs, whatever ``plan`` says: the Python side derives them from the plan,
    so the script's own checks of a plan that disagrees with them can only be reached by hand."""
    event = json.dumps(replanned_event_data(1, ["b"], [job.job_id for job in new]))
    args: list = [rid, lease.owner, lease.epoch, plan.model_dump_json(), "[]", event, 0]
    for job in new:
        args += [job.job_id, JobRecord(job=job, state=JobState.WAITING).model_dump_json()]
    before = await everything(store)
    out = _write_result(await store._run("replan", replan_keys(rid), args))
    if not out.ok:
        assert await everything(store) == before  # a refusal writes nothing
    return out


STRAY = PlannedJob(job_id="z", instruction="stray", capabilities=["research"])


async def test_a_plan_that_lists_more_jobs_than_the_new_and_kept_ones_is_refused(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    out = await raw_replan(state_store, rid, lease, JobPlan(jobs=[A, C, C]), [C])  # c listed twice, sent once
    assert out.status is StoreStatus.INVALID_STATE and "does not list exactly" in (out.detail or "")


async def test_a_plan_that_lists_a_job_that_is_neither_kept_nor_new_is_refused(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    out = await raw_replan(state_store, rid, lease, JobPlan(jobs=[A, STRAY]), [C])  # right size, wrong job
    assert out.status is StoreStatus.INVALID_STATE and "neither kept nor new" in (out.detail or "")


async def test_a_plan_that_drops_a_succeeded_job_is_refused_even_when_its_size_adds_up(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    out = await raw_replan(state_store, rid, lease, JobPlan(jobs=[C, C]), [C])  # a is missing, c is there twice
    assert out.status is StoreStatus.INVALID_STATE and "missing from the plan" in (out.detail or "")


async def test_a_plan_with_no_new_job_is_refused_by_the_script_too(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    out = await raw_replan(state_store, rid, lease, JobPlan(jobs=[A]), [])
    assert out.status is StoreStatus.INVALID_STATE and "no new job" in (out.detail or "")


async def test_a_plan_that_grows_is_stored_with_its_new_job_count(state_store: StateStore) -> None:
    """Two new jobs replace the one that failed: the stored count must follow the plan, not stay at the old two."""
    rid, lease = await finished(state_store)
    d = PlannedJob(job_id="d", instruction="and this", capabilities=["research"], depends_on=["a"])
    assert (await do_replan(state_store, lease, JobPlan(jobs=[A, C, d]))).ok
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and snap.job_count == 3 and list(snap.jobs) == ["a", "c", "d"]


async def test_malformed_input_aborts_before_any_write(state_store: StateStore) -> None:
    rid, lease = await finished(state_store)
    before = await everything(state_store)
    args = [rid, lease.owner, lease.epoch, NEW_PLAN.model_dump_json(), "not json", "{}", 0, "c", "{}"]
    # Real Redis answers a script error with ResponseError; fakeredis lets the JSON decoder's ValueError through.
    with pytest.raises((redis.exceptions.ResponseError, ValueError)):
        await state_store._run("replan", replan_keys(rid), args)
    assert await everything(state_store) == before


# --------------------------------------------------------------------------- dead letters of replaced jobs (Req 3.7)


async def test_a_dead_letter_of_a_replaced_job_stays_and_cannot_be_replayed_into_the_new_plan(
    state_store: StateStore,
) -> None:
    rid, lease = await finished(state_store)
    assert (await state_store.create_dead_letter(fields(DL1, rid=rid, job_id="b", attempt=3))).created
    assert (await do_replan(state_store, lease)).ok
    assert (await state_store.get_dead_letter(DL1)).status == "Pending"  # the record is history: it stays
    before = await everything(state_store)
    out = await state_store.replay_dead_letter(DL1)
    assert out.outcome != "accepted"  # the Job it names is no longer in the plan
    assert await everything(state_store) == before  # and the refusal changed nothing, the new plan included
    snap = await state_store.load_snapshot(rid)
    assert snap is not None and snap.plan == NEW_PLAN and "b" not in snap.jobs


async def test_the_event_type_is_known_and_its_data_is_exactly_three_fields() -> None:
    assert "request.replanned" in EVENT_TYPES
    assert json.dumps(replanned_event_data(2, ["x"], ["y"]), sort_keys=True) == (
        '{"failed_jobs": ["x"], "new_jobs": ["y"], "replans": 2}'
    )
