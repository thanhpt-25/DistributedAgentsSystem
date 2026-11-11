"""``replay_dead_letter.lua`` refuses a Request that failed by its budget and changes nothing
(das-client-api task 7.8; Phase 4 Req 5.10)."""

from __future__ import annotations

import pytest

from das.models import RequestStatus
from das.state import StateStore
from tests.unit.test_state_phase2_dlq import DL1, FINAL, everything, fields, running_with_failed_job


async def failed_with(store: StateStore, error: str) -> str:
    rid, lease = await running_with_failed_job(store)
    assert (await store.fail_request(lease, error, FINAL)).ok
    await store.create_dead_letter(fields(DL1, rid))
    return rid


@pytest.mark.parametrize("error", ["budget_exceeded", "budget_unenforceable"])
async def test_replay_of_a_budget_failed_request_is_refused_without_a_write(
    state_store: StateStore, fake_redis_time, error: str
):
    rid = await failed_with(state_store, error)
    before = await everything(state_store)
    result = await state_store.replay_dead_letter(DL1)
    assert (result.outcome, result.current_state, result.accepted) == ("budget_exceeded", None, False)
    assert await everything(state_store) == before
    assert await state_store.get_status(rid) is RequestStatus.FAILED
    assert (await state_store.get_dead_letter(DL1)).status == "Pending"


async def test_replay_of_an_ordinary_failure_is_still_accepted(state_store: StateStore, fake_redis_time):
    rid = await failed_with(state_store, "job a failed")
    result = await state_store.replay_dead_letter(DL1)
    assert result.accepted
    assert await state_store.get_status(rid) is RequestStatus.PENDING
