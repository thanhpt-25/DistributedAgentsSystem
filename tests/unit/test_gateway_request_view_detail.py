"""``GET /v1/requests/{id}?detail=true`` adds each Job's instruction and Worker answer; the default is unchanged."""

from __future__ import annotations

from das.state import StateStore
from tests.unit.test_gateway_request_view_client import AUTH, client_for
from tests.unit.test_state_phase6_replan import finished


async def test_detail_shows_what_each_worker_answered(state_store: StateStore, fake_redis) -> None:
    rid, _ = await finished(state_store)
    async with client_for(state_store, fake_redis) as c:
        plain = (await c.get(f"/v1/requests/{rid}", headers=AUTH)).json()
        body = (await c.get(f"/v1/requests/{rid}?detail=true", headers=AUTH)).json()
    assert set(plain["jobs"][0]) == {"job_id", "state"}
    a, b = body["jobs"]
    assert a["instruction"] and a["attempt"] == 1 and a["result"]["status"] == "Succeeded"
    assert a["result"]["content"] is not None
    assert b["attempt"] == 3 and b["result"]["status"] == "Failed"
    assert "worker_id" not in a["result"]  # carried once, on the Job


async def test_a_job_nobody_ran_has_no_result(state_store: StateStore, fake_redis) -> None:
    rid = await state_store.create_request("hello")
    async with client_for(state_store, fake_redis) as c:
        body = (await c.get(f"/v1/requests/{rid}?detail=true", headers=AUTH)).json()
    assert all(j["result"] is None and j["attempt"] == 0 for j in body["jobs"])
