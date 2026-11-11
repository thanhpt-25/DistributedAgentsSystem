"""``POST /v1/admin/dlq/{id}/replay`` on a Request that failed by its budget
(das-client-api task 7.8; Phase 4 Req 5.10)."""

from __future__ import annotations

import pytest

from das.models import RequestStatus
from das.state import StateStore
from tests.unit.test_gateway_admin_dlq import ADMIN, _client, action, build
from tests.unit.test_gateway_v1_extensions import Recorders
from tests.unit.test_state_phase2_dlq import DL1, FINAL, everything, fields, running_with_failed_job


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


async def failed_with(store: StateStore, error: str) -> str:
    rid, lease = await running_with_failed_job(store)
    assert (await store.fail_request(lease, error, FINAL)).ok
    await store.create_dead_letter(fields(DL1, rid))
    return rid


@pytest.mark.parametrize("error", ["budget_exceeded", "budget_unenforceable"])
async def test_replay_of_a_budget_failed_request_is_409_budget_exceeded(
    state_store: StateStore, fake_redis_time, recorders: Recorders, error: str
):
    rid = await failed_with(state_store, error)
    before = await everything(state_store)
    async with _client(build(state_store, recorders)) as c:
        r = await c.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert (r.status_code, r.json()) == (409, {"error": "budget_exceeded"})  # no current_state (Req 5.10)
    assert action(recorders, "replay", "budget_exceeded") == 1
    assert await state_store.get_status(rid) is RequestStatus.FAILED
    assert (await state_store.get_dead_letter(DL1)).status == "Pending"
    assert await everything(state_store) == before


async def test_replay_of_an_ordinary_failure_is_still_202(
    state_store: StateStore, fake_redis_time, recorders: Recorders
):
    rid = await failed_with(state_store, "job a failed")
    async with _client(build(state_store, recorders)) as c:
        r = await c.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert r.status_code == 202
    assert action(recorders, "replay", "accepted") == 1
    assert await state_store.get_status(rid) is RequestStatus.PENDING
