"""``GET /v1/requests/{id}`` carries ``replans`` (das-smarter-agents task 3.1; Phase 6 Req 3.6): 0 for a Request that
was never replanned, then the number of times the Manager replaced the Jobs that failed."""

from __future__ import annotations

from das.state import StateStore
from tests.unit.test_gateway_request_view_client import client_for, view
from tests.unit.test_state_phase6_replan import NEW_PLAN, do_replan, finished


async def test_a_plain_request_reports_zero(state_store: StateStore, fake_redis) -> None:
    rid = await state_store.create_request("hello")
    async with client_for(state_store, fake_redis) as c:
        assert (await view(c, rid))["replans"] == 0


async def test_a_replanned_request_reports_the_count_and_the_new_jobs(state_store: StateStore, fake_redis) -> None:
    rid, lease = await finished(state_store)
    assert (await do_replan(state_store, lease)).ok
    async with client_for(state_store, fake_redis) as c:
        body = await view(c, rid)
    assert body["replans"] == 1 and body["status"] == "Running"
    assert [j["job_id"] for j in body["jobs"]] == NEW_PLAN.job_ids()


async def test_the_other_fields_of_the_view_are_unchanged(state_store: StateStore, fake_redis) -> None:
    """The new field is an addition: every field a Phase 5 client reads is still there."""
    rid = await state_store.create_request("hello")
    async with client_for(state_store, fake_redis) as c:
        body = await view(c, rid)
    phase_5 = {
        "request_id", "status", "priority", "jobs", "transitions", "token_usage", "estimated_cost",
        "replay_history", "budget", "webhook",
    }
    assert phase_5 <= set(body) and set(body) - phase_5 == {"replans"}
