"""Gateway unit tests completing task 13.9 (das-observability-operations).

Golden snapshot of v1 response shapes (Req 19.6, 9.13), DLQ inspect after a real
retention purge (Req 12.7, 14.14), admin counter outcomes not covered elsewhere
(Req 3.9) and list items with a plan and usage (Req 15.3).
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from das.gateway.app import create_app
from das.models import FinalResult, JobReport, JobState, TokenUsage
from das.models.usage import UsageRecord
from das.registry import AgentInfo, AgentRegistry
from das.state import PURGE_DELETED, StateStore
from tests.unit.test_gateway_v1_extensions import PLAN, PLAN_USAGE, Recorders
from tests.unit.test_state_phase2_dlq import (
    DL1,
    T0,
    everything,
    failed_request,
    fields,
    running_with_failed_job,
)

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
AUTH = {"X-API-Key": API_KEY}
ADMIN = {"X-API-Key": ADMIN_KEY}


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


def build(store: StateStore, recorders: Recorders | None = None, **kw: Any) -> Any:
    return create_app(
        store, AgentRegistry(store.client), None, [API_KEY], 10,
        admin_api_keys=[ADMIN_KEY], metrics_factory=recorders, **kw,
    )


# --------------------------------------------------------------------------- golden v1

def shape(value: Any) -> Any:
    """JSON value -> type tree (dict keys kept, lists reduced to their first element)."""
    if isinstance(value, dict):
        return {k: shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [shape(value[0])] if value else []
    if value is None:
        return "null"
    return type(value).__name__


def assert_extends(golden: Any, actual: Any, path: str = "$") -> None:
    """Every v1 key of ``golden`` is in ``actual`` with the same type (Req 19.6)."""
    if isinstance(golden, dict):
        assert isinstance(actual, dict), path
        missing = set(golden) - set(actual)
        assert not missing, f"{path}: v1 keys removed {missing}"
        for k, v in golden.items():
            assert_extends(v, actual[k], f"{path}.{k}")
    elif isinstance(golden, list):
        assert isinstance(actual, list), path
        if golden:
            assert actual, f"{path}: list unexpectedly empty"
            assert_extends(golden[0], actual[0], f"{path}[0]")
    else:
        assert golden == actual, f"{path}: type {actual} != v1 {golden}"


# Golden v1 response shapes (Phase 1 contract), captured from the v1 Gateway.
V1_SUBMIT = {"request_id": "str"}
V1_TRANSITION = {
    "job_id": "str", "from_state": "null", "to_state": "str",
    "attempt": "int", "worker_id": "null", "at": "str",
}
V1_COMPLETED = {
    "request_id": "str",
    "status": "str",
    "jobs": [{"job_id": "str", "state": "str"}],
    "transitions": [V1_TRANSITION],
    "final_result": {
        "summary": "str",
        "jobs": [{"job_id": "str", "state": "str", "worker_id": "str",
                  "input_tokens": "int", "output_tokens": "int"}],
        "failures": [],
        "summarizer_usage": {"input_tokens": "int", "output_tokens": "int",
                             "total_tokens": "int"},
        "error": "null",
    },
}
V1_FAILED = {
    "request_id": "str", "status": "str",
    "jobs": [{"job_id": "str", "state": "str"}],
    "transitions": [V1_TRANSITION],
    "error": "str",
}
V1_AGENT = {
    "agent_id": "str", "role": "str", "host": "str", "capabilities": ["str"],
    "status": "str", "last_heartbeat": "str",
}
PHASE2_REQUEST_KEYS = {"token_usage", "estimated_cost", "replay_history"}
#: Phase 4 Req 11.1: the only change for a client that sends nothing new.
PHASE4_REQUEST_KEYS = {"budget", "webhook"}
PHASE5_REQUEST_KEYS = {"priority"}  # das-multi-tenant Req 7.3


async def test_golden_v1_shapes_kept_with_default_config(state_store: StateStore) -> None:
    rid_done = await state_store.create_request("hello")
    lease = (await state_store.acquire_lease(rid_done, "m1", 30)).lease
    assert (await state_store.commit_plan(lease, PLAN)).ok
    final = FinalResult(
        summary="done",
        jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED, worker_id="w1",
                        input_tokens=3, output_tokens=4)],
    )
    assert (await state_store.finalize(lease, final)).ok
    rid_failed = await failed_request(state_store)
    await AgentRegistry(state_store.client).register(
        AgentInfo(agent_id="w1", role="worker", host="h1", capabilities={"research"})
    )
    async with _client(build(state_store)) as c:
        submit = await c.post("/v1/requests", json={"prompt": "hi"}, headers=AUTH)
        done = await c.get(f"/v1/requests/{rid_done}", headers=AUTH)
        failed = await c.get(f"/v1/requests/{rid_failed}", headers=AUTH)
        agents = await c.get("/v1/agents", headers=AUTH)
        cancel = await c.post(f"/v1/requests/{submit.json()['request_id']}/cancel", headers=AUTH)
        again = await c.post(f"/v1/requests/{rid_done}/cancel", headers=AUTH)
        errors = [
            (await c.get("/v1/requests/nope", headers=AUTH)),
            (await c.post("/v1/requests/nope/cancel", headers=AUTH)),
            (await c.post("/v1/requests", json={"prompt": " "}, headers=AUTH)),
            (await c.post("/v1/requests", json={"prompt": "x" * 11}, headers=AUTH)),
            (await c.post("/v1/requests", json={"text": "x"}, headers=AUTH)),
            (await c.get("/v1/agents")),
        ]

    assert (submit.status_code, shape(submit.json())) == (202, V1_SUBMIT)
    assert done.status_code == 200 and failed.status_code == 200
    assert_extends(V1_COMPLETED, shape(done.json()))
    assert_extends(V1_FAILED, shape(failed.json()))
    # Phases 2, 4 and 5 only add fields (Req 9.13, 19.6; Phase 4 Req 11.1; Phase 5 Req 7.3).
    added = PHASE2_REQUEST_KEYS | PHASE4_REQUEST_KEYS | PHASE5_REQUEST_KEYS
    assert set(done.json()) - set(V1_COMPLETED) == added
    assert set(failed.json()) - set(V1_FAILED) == added
    assert done.json()["budget"] is None and done.json()["webhook"] is None
    assert agents.status_code == 200
    assert_extends([V1_AGENT], shape(agents.json()))
    assert set(agents.json()[0]) - set(V1_AGENT) == {"in_flight", "drained"}
    assert (cancel.status_code, cancel.json()) == (202, {"status": "Cancelled"})
    assert (again.status_code, again.json()) == (
        409, {"error": "request_terminal", "status": "Completed"}
    )
    assert [(r.status_code, r.json()) for r in errors] == [
        (404, {"error": "not_found"}),
        (404, {"error": "not_found"}),
        (400, {"error": "empty_prompt"}),
        (400, {"error": "prompt_too_long", "max_length": 10}),
        (400, {"error": "invalid_body"}),
        (401, {"error": "unauthorized"}),
    ]


# --------------------------------------------------------------------------- DLQ inspect


async def test_inspect_after_retention_purge(fake_redis_time, state_store: StateStore) -> None:
    fake_redis_time.set_ms(T0)
    rid = await failed_request(state_store)
    await state_store.create_dead_letter(
        fields(rid=rid, envelope_json=json.dumps({"schema_version": 1, "payload": {}}))
    )
    async with _client(build(state_store)) as c:
        before = (await c.get(f"/v1/admin/dlq/{DL1}", headers=ADMIN)).json()
        assert (before["request_exists"], before["request_status"], before["job_state"],
                before["job_attempt"]) == (True, "Failed", "Failed", 1)
        ttl = 60_000
        fake_redis_time.set_ms(T0 + ttl)
        assert await state_store.purge_request(rid, ttl) == PURGE_DELETED
        r = await c.get(f"/v1/admin/dlq/{DL1}", headers=ADMIN)
    assert r.status_code == 200  # Req 14.14
    body = r.json()
    assert (body["request_exists"], body["request_status"], body["job_state"],
            body["job_attempt"]) == (False, None, None, None)
    # The record itself is untouched by the Request purge.
    assert (body["dead_letter_id"], body["request_id"], body["status"]) == (DL1, rid, "Pending")
    assert body["message"] == {"schema_version": 1, "payload": {}}


# --------------------------------------------------------------------------- admin counter


async def test_replay_request_state_and_job_state_outcomes(state_store: StateStore) -> None:
    recorders = Recorders()
    rid, _ = await running_with_failed_job(state_store, attempt=2)
    pending = await state_store.create_request("p")
    dl_job, dl_req = "4" * 32, "6" * 32
    await state_store.create_dead_letter(fields(dl_job, rid, job_id="b", attempt=2))
    await state_store.create_dead_letter(fields(dl_req, pending))
    async with _client(build(state_store, recorders)) as c:
        for dlid, code, current in ((dl_job, "job_state", "Skipped"),
                                    (dl_req, "request_state", "Pending")):
            before = await everything(state_store)
            r = await c.post(f"/v1/admin/dlq/{dlid}/replay", headers=ADMIN)
            assert (r.status_code, r.json()) == (409, {"error": code, "current_state": current})
            assert await everything(state_store) == before
    for outcome in ("job_state", "request_state"):
        assert recorders.value(
            "das_dlq_admin_actions_total", action="replay", outcome=outcome
        ) == 1
    assert recorders.value(
        "das_dlq_admin_actions_total", action="replay", outcome="accepted"
    ) == 0


# --------------------------------------------------------------------------- list items


async def test_list_item_with_plan_and_usage_has_no_content(state_store: StateStore) -> None:
    rid = await state_store.create_request("secret prompt")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.commit_plan(lease, PLAN, usage=[PLAN_USAGE])).ok
    summ = UsageRecord(
        operation="summarize", provider="openai", model="gpt-x",
        usage=TokenUsage(input_tokens=7, output_tokens=3, total_tokens=10),
    )
    final = FinalResult(summary="secret summary",
                        jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED)])
    assert (await state_store.finalize(lease, final, usage=[summ])).ok
    await state_store.set_index_ready()
    async with _client(build(state_store)) as c:
        r = await c.get("/v1/requests", headers=AUTH)
    assert r.status_code == 200
    [item] = r.json()["items"]
    assert set(item) == {
        "request_id", "status", "created_at", "finished_at", "job_count", "token_usage",
    }
    assert (item["request_id"], item["status"], item["job_count"]) == (rid, "Completed", 2)
    assert item["finished_at"] is not None
    assert item["token_usage"] == {"input": 1007, "output": 503, "total": 1510}
    text = r.text
    for content in ("secret prompt", "secret summary", "research x", "instruction"):
        assert content not in text
