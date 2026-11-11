"""The Client_Package against the real Gateway_API app: every client method and error mapping
(das-client-api task 9.6; Phase 4 Req 9.10). Runs each test once per client (async and blocking).

A failure here means the client and the server no longer agree on a request, a response field, an error body or a
stream, which is exactly what this test exists to catch."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import httpx
import pytest

import das.gateway.app as gateway_app
from das.config.schema import BudgetConfig
from das.state import keys
from das_client import (
    AuthError,
    Budget,
    BudgetAboveLimit,
    BudgetView,
    CancelResult,
    Conflict,
    DasError,
    Event,
    IdempotencyKeyReused,
    InvalidRequest,
    InvalidSignature,
    NotFound,
    Page,
    QuotaExceeded,
    RateLimited,
    ServiceUnavailable,
    Submitted,
    verify_webhook,
)
from tests.unit.test_gateway_limits import limits
from tests.unit.test_gateway_submit_client import settings
from tests.unit.test_gateway_webhooks import SECRET, Harness, pending_delivery
from tests.unit.test_state_phase4_events import FINAL, PLAN, leased

UTC = dt.timezone.utc


async def finish(state_store: Any, request_id: str) -> str:
    """Drive a submitted Request to Completed through the store, as a Manager would. Returns its first event id."""
    first = (await state_store.client.xrange(keys.events(request_id), count=1))[0][0]
    lease = await leased(state_store, request_id)
    assert (await state_store.commit_plan(lease, PLAN)).ok
    assert (await state_store.finalize(lease, FINAL)).ok
    return str(first)


# --------------------------------------------------------------------------- submit and get


async def test_submit_then_get_reads_the_request_back(contract) -> None:
    gateway = contract()
    submitted = await gateway.call("submit", "hello")
    assert isinstance(submitted, Submitted) and submitted.replayed is False
    view = await gateway.call("get", submitted.request_id)
    assert (view.request_id, view.status, view.is_terminal) == (submitted.request_id, "Pending", False)
    assert (view.budget, view.webhook, view.final_result, view.error) == (None, None, None, None)


async def test_a_finished_request_shows_its_final_result(contract, state_store) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "hello")).request_id
    await finish(state_store, rid)
    view = await gateway.call("get", rid)
    assert (view.status, view.is_terminal) == ("Completed", True)
    assert view.final_result["summary"] == "done"
    assert {job.job_id for job in view.jobs} == {"a", "b"}


async def test_the_same_key_replays_and_a_generated_key_does_not(contract) -> None:
    gateway = contract()
    first = await gateway.call("submit", "same", idempotency_key="key-0001")
    again = await gateway.call("submit", "same", idempotency_key="key-0001")
    assert (first.replayed, again.replayed, again.request_id) == (False, True, first.request_id)
    other = await gateway.call("submit", "same")  # a fresh generated key is a new request
    assert other.request_id != first.request_id and other.replayed is False
    page = await gateway.call("list_requests")
    assert {item.request_id for item in page.items} == {first.request_id, other.request_id}


async def test_the_same_key_with_another_prompt_is_key_reuse(contract) -> None:
    gateway = contract()
    await gateway.call("submit", "one", idempotency_key="key-0001")
    with pytest.raises(IdempotencyKeyReused) as caught:
        await gateway.call("submit", "two", idempotency_key="key-0001")
    assert caught.value.status_code == 422


# --------------------------------------------------------------------------- budget


async def test_a_token_budget_is_accepted_and_shown_back(contract) -> None:
    gateway = contract()
    submitted = await gateway.call("submit", "p", budget=Budget(max_total_tokens=500))
    view = await gateway.call("get", submitted.request_id)
    assert view.budget == BudgetView(Budget(500, None), 0, None, False)


async def test_a_cost_budget_comes_back_as_a_decimal(contract) -> None:
    """Only the limits are checked: this Gateway has no Price_Table, so a ``max_cost`` budget is reported
    ``exceeded`` (unenforceable, Phase 4 Req 5.6) and the Manager would fail the Request before planning."""
    gateway = contract()
    submitted = await gateway.call("submit", "p", budget=Budget(max_total_tokens=500, max_cost=Decimal("1.5")))
    view = await gateway.call("get", submitted.request_id)
    assert isinstance(view.budget, BudgetView)
    assert view.budget.limits == Budget(500, Decimal("1.5"))
    assert view.budget.used_total_tokens == 0


async def test_an_invalid_budget_names_the_parameter(contract) -> None:
    gateway = contract()
    with pytest.raises(InvalidRequest) as caught:
        await gateway.call("submit", "p", budget=Budget(max_total_tokens=0))
    assert (caught.value.status_code, caught.value.parameter) == (400, "budget.max_total_tokens")


async def test_a_budget_above_the_keys_default_reports_the_limit(contract) -> None:
    gateway = contract(settings(default_budget=BudgetConfig(max_total_tokens=1000)))
    with pytest.raises(BudgetAboveLimit) as caught:
        await gateway.call("submit", "p", budget=Budget(max_total_tokens=1001))
    assert (caught.value.parameter, caught.value.limit) == ("budget.max_total_tokens", 1000)
    ok = await gateway.call("submit", "p", budget=Budget(max_total_tokens=1000))  # at the limit is allowed
    assert isinstance(ok, Submitted)


# --------------------------------------------------------------------------- limits and the other client errors


async def test_a_full_quota_is_quota_exceeded_until_a_request_ends(contract) -> None:
    gateway = contract(settings(max_active_requests=1))
    first = await gateway.call("submit", "one")
    with pytest.raises(QuotaExceeded) as caught:
        await gateway.call("submit", "two")
    assert (caught.value.status_code, caught.value.limit) == (429, 1)
    assert (await gateway.call("cancel", first.request_id)).status == "Cancelled"
    assert isinstance(await gateway.call("submit", "two"), Submitted)  # a finished request frees its slot


async def test_an_empty_bucket_is_rate_limited_with_a_retry_after(contract) -> None:
    gateway = contract(limits(rate=1.0, burst=1))
    await gateway.call("submit", "one")
    with pytest.raises(RateLimited) as caught:
        await gateway.call("submit", "two")
    assert caught.value.status_code == 429 and caught.value.retry_after is not None and caught.value.retry_after >= 1


async def test_a_wrong_api_key_is_an_auth_error(contract) -> None:
    gateway = contract(key="not-a-real-key-00001")
    for name, args in (("submit", ("p",)), ("get", ("r-1",)), ("list_requests", ())):
        with pytest.raises(AuthError) as caught:
            await gateway.call(name, *args)
        assert caught.value.status_code == 401


async def test_an_unknown_request_is_not_found_on_every_method(contract) -> None:
    gateway = contract()
    for name in ("get", "cancel"):
        with pytest.raises(NotFound):
            await gateway.call(name, "no-such-request")
    with pytest.raises(NotFound):
        await gateway.events("no-such-request")


async def test_a_blank_prompt_is_an_invalid_request(contract) -> None:
    gateway = contract()
    with pytest.raises(InvalidRequest) as caught:
        await gateway.call("submit", "   ")
    assert caught.value.status_code == 400


async def test_an_unavailable_store_is_service_unavailable(contract, state_store, monkeypatch) -> None:
    async def down(*args: Any, **kwargs: Any) -> Any:
        raise ConnectionError("redis down")

    monkeypatch.setattr(state_store, "submit_request", down)
    with pytest.raises(ServiceUnavailable) as caught:
        await contract().call("submit", "p")
    assert (caught.value.status_code, caught.value.code) == (503, "service_unavailable")


async def test_a_retry_after_a_503_creates_exactly_one_request(contract, state_store, monkeypatch) -> None:
    gateway = contract(max_retries=2)
    original, calls = state_store.submit_request, {"n": 0}

    async def down_once(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("redis down")
        return await original(*args, **kwargs)

    monkeypatch.setattr(state_store, "submit_request", down_once)
    submitted = await gateway.call("submit", "p")
    assert calls["n"] == 2  # the first call failed with a 503, the client retried
    page = await gateway.call("list_requests")
    assert [item.request_id for item in page.items] == [submitted.request_id]


# --------------------------------------------------------------------------- cancel and list


async def test_cancel_then_a_second_cancel_conflicts(contract) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p")).request_id
    assert await gateway.call("cancel", rid) == CancelResult("Cancelled")
    assert (await gateway.call("get", rid)).status == "Cancelled"
    with pytest.raises(Conflict) as caught:
        await gateway.call("cancel", rid)
    assert caught.value.status_code == 409


async def test_list_requests_pages_to_the_end_without_repeats(contract) -> None:
    gateway = contract()
    created = [(await gateway.call("submit", f"prompt {i}")).request_id for i in range(5)]
    seen: list[str] = []
    cursor, pages = None, 0
    while True:
        page = await gateway.call("list_requests", limit=2, cursor=cursor)
        assert isinstance(page, Page) and len(page.items) <= 2
        seen += [item.request_id for item in page.items]
        pages += 1
        cursor = page.next_cursor
        if cursor is None:
            break
    assert pages == 3 and len(seen) == len(set(seen)) and set(seen) == set(created)


async def test_list_requests_filters_by_status_and_time(contract) -> None:
    gateway = contract()
    ids = [(await gateway.call("submit", f"prompt {i}")).request_id for i in range(3)]
    await gateway.call("cancel", ids[0])
    cancelled = await gateway.call("list_requests", status="Cancelled")
    assert [item.request_id for item in cancelled.items] == [ids[0]]
    both = await gateway.call("list_requests", status=["Cancelled", "Pending"])
    assert {item.request_id for item in both.items} == set(ids)
    long_ago = dt.datetime(2000, 1, 1, tzinfo=UTC)
    assert len((await gateway.call("list_requests", created_after=long_ago)).items) == 3
    assert (await gateway.call("list_requests", created_before=long_ago)).items == ()
    with pytest.raises(InvalidRequest) as caught:
        await gateway.call("list_requests", limit=0)
    assert caught.value.parameter == "limit"


# --------------------------------------------------------------------------- events and wait


async def test_events_run_from_a_position_to_the_terminal_event(contract, state_store) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p")).request_id
    first_id = await finish(state_store, rid)
    events = await gateway.events(rid, last_event_id=first_id)  # ends by itself after the terminal event
    kinds = [event.type for event in events]
    assert kinds[0] == "snapshot" and kinds[-1] == "request.completed" and "request.planned" in kinds
    assert events[0].data["status"] == "Completed" and "final_result" not in events[0].data
    assert events[-1].is_terminal and events[-1].data["status"] == "Completed"
    ids = [e.event_id for e in events[1:]]
    assert ids == sorted(ids) and all(i > first_id for i in ids)  # in order, and only after the given position


async def test_events_of_a_request_that_is_already_over_are_just_its_snapshot(contract, state_store) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p")).request_id
    await finish(state_store, rid)
    events = await gateway.events(rid)
    assert [(e.type, e.data["status"]) for e in events] == [("snapshot", "Completed")]


async def test_a_cancelled_request_streams_its_terminal_event(contract, state_store) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p")).request_id
    first = (await state_store.client.xrange(keys.events(rid), count=1))[0][0]
    await gateway.call("cancel", rid)
    events = await gateway.events(rid, last_event_id=first)
    assert [e.type for e in events] == ["snapshot", "request.cancelled"]
    assert all(isinstance(e, Event) and e.request_id == rid for e in events)


async def test_wait_returns_the_final_view_with_its_result(contract, state_store) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p")).request_id
    await finish(state_store, rid)
    view = await gateway.call("wait", rid, timeout=30.0)
    assert (view.status, view.final_result["summary"]) == ("Completed", "done")


async def test_wait_polls_when_the_stream_cannot_be_opened(contract, state_store) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p")).request_id
    await finish(state_store, rid)
    gateway.transport.fail_events = 1  # the only stream connection fails; max_retries is 0
    view = await gateway.call("wait", rid, timeout=30.0)
    assert (gateway.transport.fail_events, view.status, view.final_result["summary"]) == (0, "Completed", "done")


# --------------------------------------------------------------------------- the client notices a changed server


async def test_a_response_that_lost_a_field_the_client_needs_is_an_error(contract, monkeypatch) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p")).request_id
    original = gateway_app.request_view

    def without_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
        body = original(*args, **kwargs)
        body.pop("status")
        return body

    monkeypatch.setattr(gateway_app, "request_view", without_status)
    with pytest.raises(DasError) as caught:
        await gateway.call("get", rid)
    assert type(caught.value) is DasError  # a malformed answer, not one of the typed API errors


async def test_a_newer_server_adding_fields_does_not_break_the_client(contract, monkeypatch) -> None:
    gateway = contract()
    rid = (await gateway.call("submit", "p", budget=Budget(max_total_tokens=10))).request_id
    original = gateway_app.request_view

    def with_extras(*args: Any, **kwargs: Any) -> dict[str, Any]:
        body = original(*args, **kwargs)
        body["brand_new_field"] = {"x": 1}
        body["budget"]["brand_new_budget_field"] = True
        body["jobs"] = [*body["jobs"], {"job_id": "x", "state": "Waiting", "brand_new_job_field": 1}]
        return body

    monkeypatch.setattr(gateway_app, "request_view", with_extras)
    view = await gateway.call("get", rid)
    assert (view.request_id, view.status, view.budget.limits.max_total_tokens) == (rid, "Pending", 10)
    assert [job.job_id for job in view.jobs] == ["x"]


# --------------------------------------------------------------------------- a delivery the Gateway really signed


async def test_a_delivery_signed_by_the_real_sender_verifies_with_the_client(
    state_store, fake_redis_time, respx_mock
) -> None:
    """Not per client: ties the server's signing to ``verify_webhook`` through a real ``WebhookSender``."""
    rid, event_id, event_type, data = await pending_delivery(state_store)
    route = respx_mock.post("https://hooks.example.test/das").mock(return_value=httpx.Response(204))
    async with httpx.AsyncClient(follow_redirects=False) as http:
        from das.clock import FakeClock

        harness = Harness(state_store, http, FakeClock())
        assert await harness.sender.deliver_one() is True
    request = route.calls.last.request
    now = float(request.headers["das-webhook-timestamp"])  # the sender's clock, not ours
    event = verify_webhook(request.content, request.headers, SECRET, now=now)
    assert event == Event(event_type, event_id, data, rid) and event.type == "request.cancelled" and event.is_terminal
    with pytest.raises(InvalidSignature):
        verify_webhook(request.content + b" ", request.headers, SECRET, now=now)  # one changed byte
    with pytest.raises(InvalidSignature):
        verify_webhook(request.content, request.headers, "another-secret-0123456789", now=now)
