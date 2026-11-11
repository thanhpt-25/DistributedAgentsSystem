"""One case per row of the design's Error Handling table (das-client-api task 7.10;
Phase 4 Req 2.2, 2.5, 3.2, 4.1, 4.4, 5.1, 5.2, 7.7 and the rows' own notes).

The rows are also covered in depth next to their feature (limits, submit, events, webhooks, replay); this
file is the one place that shows the whole table through the real app. Each case also checks the row's
*side effects*: a rejection stores nothing, takes no slot and leaves the Active_Set alone."""

from __future__ import annotations

import io
import json
from typing import Any

import httpx
import pytest

from das.clock import FakeClock
from das.config.schema import BudgetConfig, EventsConfig
from das.models import RequestStatus
from das.observability.logging import configure_logging
from das.state import keys
from tests.unit.test_gateway_admin_dlq import ADMIN, _client, build
from tests.unit.test_gateway_admin_replay_budget import failed_with
from tests.unit.test_gateway_events import cancelled_request, make_app
from tests.unit.test_gateway_events import stream as open_stream
from tests.unit.test_gateway_limits import AUTH, KEY, Recorders, fp, limits
from tests.unit.test_gateway_submit_client import IDEM, active, make_client, settings, submit
from tests.unit.test_gateway_v1_extensions import Recorders as AdminRecorders
from tests.unit.test_gateway_webhooks import Harness, pending_delivery
from tests.unit.test_state_phase2_dlq import DL1


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


def rejected(recorders: Recorders, reason: str) -> float:
    return recorders.value("das_client_rejections_total", reason=reason)


async def down(*_: Any, **__: Any) -> Any:
    raise ConnectionError("redis down")


# --------------------------------------------------------------------------- row 1: bucket empty


async def test_bucket_empty_is_429_with_retry_after_before_any_other_handling(
    state_store, fake_redis, fake_redis_time, recorders
):
    async for c in make_client(state_store, fake_redis, recorders, limits(rate=1.0, burst=1)):
        assert (await submit(c)).status_code == 202  # takes the only token
        # The next request is both rate-limited and invalid (blank prompt, bad key, bad budget): 429 wins.
        r = await submit(c, "  ", headers={IDEM: ""}, budget={"max_cost": 0})
        assert (r.status_code, r.json()) == (429, {"error": "rate_limited"})
        assert int(r.headers["Retry-After"]) >= 1
        assert len(await active(fake_redis)) == 1
        assert rejected(recorders, "rate_limited") == 1


# --------------------------------------------------------------------------- row 2: Active_Set full


async def test_active_set_full_is_429_quota_exceeded_with_limit_after_the_idempotency_check(
    state_store, fake_redis, fake_redis_time, recorders
):
    async for c in make_client(state_store, fake_redis, recorders, settings(max_active_requests=1)):
        first = await submit(c, headers={IDEM: "k-1"})
        assert first.status_code == 202
        # A replay is answered although the set is full (the idempotency check comes first)...
        assert (await submit(c, headers={IDEM: "k-1"})).status_code == 200
        # ...a new Request is not.
        r = await submit(c, "another")
        assert (r.status_code, r.json()) == (429, {"error": "quota_exceeded", "limit": 1})
        assert await active(fake_redis) == {first.json()["request_id"]}
        assert rejected(recorders, "quota_exceeded") == 1


# --------------------------------------------------------------------------- rows 3, 5, 6: 400 bodies


@pytest.mark.parametrize(
    ("headers", "body", "expected", "reason"),
    [
        pytest.param({IDEM: ""}, {}, {"error": "invalid_request", "parameter": "Idempotency-Key"}, None, id="bad-key"),
        pytest.param(
            {},
            {"budget": {"max_cost": 0}},
            {"error": "invalid_request", "parameter": "budget.max_cost"},
            None,
            id="invalid-budget-field",
        ),
        pytest.param(
            {},
            {"budget": {"max_total_tokens": 1001}},
            {"error": "budget_above_limit", "parameter": "budget.max_total_tokens", "limit": 1000},
            "budget_above_limit",
            id="budget-above-key-default",
        ),
    ],
)
async def test_a_bad_submit_is_a_400_that_stores_nothing(
    state_store, fake_redis, fake_redis_time, recorders, headers, body, expected, reason
):
    capped = settings(default_budget=BudgetConfig(max_total_tokens=1000))
    async for c in make_client(state_store, fake_redis, recorders, capped):
        r = await submit(c, headers=headers, **body)
        assert (r.status_code, r.json()) == (400, expected)
        assert await active(fake_redis) == set()
        if reason:
            assert rejected(recorders, reason) == 1


# --------------------------------------------------------------------------- row 4: key reuse


async def test_key_reused_with_another_body_is_422(state_store, fake_redis, fake_redis_time, recorders):
    async for c in make_client(state_store, fake_redis, recorders):
        first = await submit(c, "one", headers={IDEM: "k-1"})
        r = await submit(c, "two", headers={IDEM: "k-1"})
        assert (r.status_code, r.json()) == (422, {"error": "idempotency_key_reused"})
        assert await active(fake_redis) == {first.json()["request_id"]}  # nothing was created
        assert rejected(recorders, "idempotency_key_reused") == 1


# --------------------------------------------------------------------------- row 7: stream caps


async def test_stream_cap_per_key_is_429(state_store, fake_redis, fake_redis_time, recorders):
    rid, _ = await cancelled_request(state_store)
    for i in range(2):
        assert await state_store.acquire_stream(fp(KEY), f"other-{i}", 2, 600.0)
    app = make_app(state_store, fake_redis, recorders, settings(max_streams=2))
    r = await open_stream(app, rid)
    assert (r.status_code, r.json()) == (429, {"error": "too_many_streams"})
    assert app.state.event_gate.open == 0  # the replica slot taken first is given back
    assert await fake_redis.zcard(keys.client_streams(fp(KEY))) == 2
    assert rejected(recorders, "too_many_streams") == 1


async def test_stream_cap_per_replica_is_503(state_store, fake_redis, fake_redis_time, recorders):
    rid, _ = await cancelled_request(state_store)
    app = make_app(state_store, fake_redis, recorders, events_config=EventsConfig(max_connections=1))
    assert app.state.event_gate.try_acquire()  # another stream holds the replica's only slot
    r = await open_stream(app, rid)
    assert (r.status_code, r.json()) == (503, {"error": "too_many_streams"})
    assert await fake_redis.zcard(keys.client_streams(fp(KEY))) == 0
    assert rejected(recorders, "too_many_streams") == 1


# --------------------------------------------------------------------------- row 8: Redis unreachable (fail closed)


async def test_redis_down_for_the_rate_check_is_503_not_an_open_gate(
    state_store, fake_redis, fake_redis_time, recorders, monkeypatch
):
    async for c in make_client(state_store, fake_redis, recorders, limits(rate=1.0, burst=5)):
        monkeypatch.setattr(state_store, "rate_check", down)
        for r in (await submit(c), await c.get("/v1/agents", headers=AUTH)):
            assert (r.status_code, r.json()) == (503, {"error": "service_unavailable"})
        assert await active(fake_redis) == set()


async def test_redis_down_for_create_is_503_and_creates_nothing(
    state_store, fake_redis, fake_redis_time, recorders, monkeypatch
):
    async for c in make_client(state_store, fake_redis, recorders):
        with monkeypatch.context() as m:
            m.setattr(state_store, "submit_request", down)
            r = await submit(c, headers={IDEM: "k-1"})
        assert (r.status_code, r.json()) == (503, {"error": "service_unavailable"})
        assert await active(fake_redis) == set()
        assert (await submit(c, headers={IDEM: "k-1"})).status_code == 202  # the key was not burned by the 503


# --------------------------------------------------------------------------- row 9: budget-failed replay


@pytest.mark.parametrize("error", ["budget_exceeded", "budget_unenforceable"])
async def test_replay_of_a_budget_failed_request_is_409_budget_exceeded(state_store, fake_redis_time, error):
    rid = await failed_with(state_store, error)
    async with _client(build(state_store, AdminRecorders())) as c:
        r = await c.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert (r.status_code, r.json()) == (409, {"error": "budget_exceeded"})
    assert await state_store.get_status(rid) is RequestStatus.FAILED
    assert (await state_store.get_dead_letter(DL1)).status == "Pending"


# --------------------------------------------------------------------------- row 10: webhook secret removed


async def test_webhook_secret_removed_after_enqueue_fails_the_delivery_and_logs_once(
    state_store, fake_redis_time, respx_mock
):
    rid, *_ = await pending_delivery(state_store)
    out = io.StringIO()
    configure_logging("client-errors-test", stream=out)
    try:
        async with httpx.AsyncClient(follow_redirects=False) as client:
            harness = Harness(state_store, client, FakeClock(), secrets={})  # the secret is gone
            assert await harness.sender.deliver_one() is True
    finally:
        configure_logging("test", stream=io.StringIO())
    state = await state_store.get_webhook(rid)
    assert (state.state, state.last_status) == ("failed", "secret_missing")
    warnings = [x for x in out.getvalue().splitlines() if json.loads(x).get("level") == "warning"]
    assert len(warnings) == 1  # logged once per delivery
    assert len(respx_mock.calls) == 0  # nothing was sent
