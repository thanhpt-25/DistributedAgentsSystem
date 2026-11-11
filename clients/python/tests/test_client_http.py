"""Request lines, headers, idempotency keys and client lifecycle (Phase 4 Req 9.2, 9.3)."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from decimal import Decimal
from typing import Any

import httpx
import pytest

from conftest import API_KEY, BASE, call
from das_client import AsyncDasClient, Budget, CancelResult, DasClient, DasError, Page, Submitted

SUBMIT = f"{BASE}/v1/requests"
ACCEPTED = httpx.Response(202, json={"request_id": "r-1"})
VIEW = {"request_id": "r-1", "status": "Pending"}
# datetime.UTC only exists from Python 3.11, and the client supports 3.10.
UTC = dt.timezone.utc


def sent(route: Any, index: int = -1) -> httpx.Request:
    return route.calls[index].request


def closer(kind: str) -> str:
    return "close" if kind == "sync" else "aclose"


# --------------------------------------------------------------------------- submit and its key


async def test_submit_posts_the_prompt_with_a_key_and_the_api_key(make_client, respx_mock) -> None:
    route = respx_mock.post(SUBMIT).mock(return_value=ACCEPTED)
    result = await call(make_client(), "submit", "hello")
    assert result == Submitted("r-1", False)
    request = sent(route)
    assert json.loads(request.content) == {"prompt": "hello"}  # no budget field without a budget
    assert request.headers["x-api-key"] == API_KEY
    assert request.headers["content-type"] == "application/json"
    assert uuid.UUID(request.headers["idempotency-key"]).version == 4


async def test_a_generated_key_is_reused_by_every_retry_of_that_call(make_client, respx_mock, timing) -> None:
    route = respx_mock.post(SUBMIT).mock(
        side_effect=[httpx.ConnectError("x"), httpx.Response(503), httpx.Response(429), ACCEPTED]
    )
    await call(make_client(), "submit", "hello")
    keys = {sent(route, i).headers["idempotency-key"] for i in range(4)}
    assert route.call_count == 4 and len(keys) == 1


async def test_each_call_gets_its_own_generated_key(make_client, respx_mock) -> None:
    route = respx_mock.post(SUBMIT).mock(return_value=ACCEPTED)
    client = make_client()
    for _ in range(3):
        await call(client, "submit", "hello")
    assert len({sent(route, i).headers["idempotency-key"] for i in range(3)}) == 3


@pytest.mark.parametrize("key", ["my-key-0001", "!~" * 64, ""])
async def test_an_explicit_key_is_sent_unchanged(make_client, respx_mock, key) -> None:
    route = respx_mock.post(SUBMIT).mock(return_value=ACCEPTED)
    await call(make_client(), "submit", "hello", idempotency_key=key)
    assert sent(route).headers["idempotency-key"] == key  # the server judges it; an empty one is never replaced


async def test_a_replay_is_marked(make_client, respx_mock) -> None:
    replayed = httpx.Response(200, json={"request_id": "r-1"}, headers={"Idempotent-Replayed": "true"})
    respx_mock.post(SUBMIT).mock(return_value=replayed)
    assert await call(make_client(), "submit", "hello", idempotency_key="k") == Submitted("r-1", True)


@pytest.mark.parametrize(
    ("budget", "expected"),
    [
        (None, {"prompt": "p"}),
        (Budget(), {"prompt": "p"}),  # an empty Budget is no Budget
        (Budget(max_total_tokens=500), {"prompt": "p", "budget": {"max_total_tokens": 500}}),
        (Budget(max_cost=Decimal("1.5")), {"prompt": "p", "budget": {"max_cost": "1.5"}}),
        (
            Budget(max_total_tokens=5, max_cost="2.50"),
            {"prompt": "p", "budget": {"max_total_tokens": 5, "max_cost": "2.50"}},
        ),
    ],
)
async def test_the_budget_is_sent_only_when_it_has_a_limit(make_client, respx_mock, budget, expected) -> None:
    route = respx_mock.post(SUBMIT).mock(return_value=ACCEPTED)
    await call(make_client(), "submit", "p", budget=budget)
    assert json.loads(sent(route).content) == expected


@pytest.mark.parametrize("body", [{}, {"request_id": 5}, [], "x"])
async def test_a_success_without_a_request_id_is_a_das_error(make_client, respx_mock, body) -> None:
    respx_mock.post(SUBMIT).mock(return_value=httpx.Response(202, json=body))
    with pytest.raises(DasError):
        await call(make_client(), "submit", "hello")


async def test_a_success_with_a_body_that_is_not_json_is_a_das_error(make_client, respx_mock) -> None:
    respx_mock.post(SUBMIT).mock(return_value=httpx.Response(202, content=b"<html>proxy</html>"))
    with pytest.raises(DasError):
        await call(make_client(), "submit", "hello")


# --------------------------------------------------------------------------- the other methods


async def test_get(make_client, respx_mock) -> None:
    route = respx_mock.get(f"{BASE}/v1/requests/r-1").mock(return_value=httpx.Response(200, json=VIEW))
    view = await call(make_client(), "get", "r-1")
    assert (view.request_id, view.status) == ("r-1", "Pending")
    assert sent(route).headers["x-api-key"] == API_KEY
    assert not sent(route).content


async def test_cancel(make_client, respx_mock) -> None:
    route = respx_mock.post(f"{BASE}/v1/requests/r-1/cancel").mock(
        return_value=httpx.Response(202, json={"status": "Cancelled"})
    )
    assert await call(make_client(), "cancel", "r-1") == CancelResult("Cancelled")
    assert sent(route).headers["x-api-key"] == API_KEY


@pytest.mark.parametrize(
    ("request_id", "raw_path"),
    [
        ("a/b", b"/v1/requests/a%2Fb"),
        ("a?b=1", b"/v1/requests/a%3Fb%3D1"),
        ("a#b", b"/v1/requests/a%23b"),
        # httpx would fold a literal ".." and leave /v1/requests/, so dot-only ids are encoded too.
        ("..", b"/v1/requests/%2E%2E"),
        (".", b"/v1/requests/%2E"),
    ],
)
async def test_a_request_id_cannot_change_the_path(make_client, respx_mock, request_id, raw_path) -> None:
    route = respx_mock.route(method="GET", url__startswith=f"{BASE}/v1/requests/").mock(
        return_value=httpx.Response(200, json=VIEW)
    )
    await call(make_client(), "get", request_id)
    assert sent(route).url.raw_path == raw_path


@pytest.mark.parametrize("method", ["get", "cancel"])
async def test_an_empty_request_id_is_rejected_before_sending(make_client, respx_mock, method) -> None:
    route = respx_mock.route().mock(return_value=httpx.Response(200, json=VIEW))
    with pytest.raises(ValueError, match="request_id"):
        await call(make_client(), method, "")
    assert route.call_count == 0


async def test_list_without_filters_sends_no_query(make_client, respx_mock) -> None:
    route = respx_mock.get(SUBMIT).mock(return_value=httpx.Response(200, json={"items": []}))
    assert await call(make_client(), "list_requests") == Page((), None)
    assert sent(route).url.query == b""


@pytest.mark.parametrize(
    ("kwargs", "query"),
    [
        ({"status": "Completed"}, {"status": "Completed"}),
        ({"status": ["Completed", "Failed"]}, {"status": "Completed,Failed"}),
        ({"status": ("Running",)}, {"status": "Running"}),
        ({"limit": 25}, {"limit": "25"}),
        ({"cursor": "abc.def"}, {"cursor": "abc.def"}),
        ({"created_after": "2026-11-11T10:00:00Z"}, {"created_after": "2026-11-11T10:00:00Z"}),
        (
            {"created_after": dt.datetime(2026, 11, 11, 10, 0, tzinfo=UTC)},
            {"created_after": "2026-11-11T10:00:00+00:00"},
        ),
        (
            {"created_before": dt.datetime(2026, 11, 11, 19, 0, tzinfo=dt.timezone(dt.timedelta(hours=9)))},
            {"created_before": "2026-11-11T10:00:00+00:00"},  # converted to UTC
        ),
    ],
)
async def test_list_sends_only_the_given_filters(make_client, respx_mock, kwargs, query) -> None:
    route = respx_mock.get(SUBMIT).mock(return_value=httpx.Response(200, json={"items": []}))
    await call(make_client(), "list_requests", **kwargs)
    assert dict(sent(route).url.params) == query


async def test_list_rejects_a_naive_datetime_before_sending_anything(make_client, respx_mock) -> None:
    route = respx_mock.get(SUBMIT).mock(return_value=httpx.Response(200, json={"items": []}))
    with pytest.raises(ValueError, match="time zone"):
        await call(make_client(), "list_requests", created_after=dt.datetime(2026, 11, 11, 10, 0))
    assert route.call_count == 0


async def test_list_returns_the_page_and_its_cursor(make_client, respx_mock) -> None:
    usage = {"input": 1, "output": 1, "total": 2}
    item = {"request_id": "r-1", "status": "Completed", "job_count": 1, "token_usage": usage}
    respx_mock.get(SUBMIT).mock(return_value=httpx.Response(200, json={"items": [item], "next_cursor": "next"}))
    page = await call(make_client(), "list_requests", limit=1)
    assert [i.request_id for i in page.items] == ["r-1"] and page.next_cursor == "next"


# --------------------------------------------------------------------------- construction and lifecycle


async def test_a_trailing_slash_on_the_base_url_is_tolerated(kind, respx_mock) -> None:
    route = respx_mock.get(f"{BASE}/v1/requests/r-1").mock(return_value=httpx.Response(200, json=VIEW))
    client = (DasClient if kind == "sync" else AsyncDasClient)(f"{BASE}/", API_KEY)
    try:
        await call(client, "get", "r-1")
    finally:
        await call(client, closer(kind))
    assert route.call_count == 1


async def test_the_api_key_is_not_in_the_repr(make_client) -> None:
    text = repr(make_client())
    assert BASE in text and API_KEY not in text


async def test_an_injected_http_client_is_used_and_left_open(kind, respx_mock) -> None:
    route = respx_mock.get(f"{BASE}/v1/requests/r-1").mock(return_value=httpx.Response(200, json=VIEW))
    http: Any = httpx.Client() if kind == "sync" else httpx.AsyncClient()
    client = (DasClient if kind == "sync" else AsyncDasClient)(BASE, API_KEY, http_client=http)
    await call(client, "get", "r-1")
    await call(client, closer(kind))
    assert route.call_count == 1
    assert not http.is_closed  # the caller owns it
    await call(http, closer(kind))


async def test_an_owned_http_client_is_closed_by_close(kind) -> None:
    client: Any = (DasClient if kind == "sync" else AsyncDasClient)(BASE, API_KEY)
    inner = client._http
    assert not inner.is_closed
    await call(client, closer(kind))
    assert inner.is_closed


async def test_the_context_manager_closes_an_owned_client(kind) -> None:
    if kind == "sync":
        with DasClient(BASE, API_KEY) as client:
            inner = client._http
        assert inner.is_closed
    else:
        async with AsyncDasClient(BASE, API_KEY) as aclient:
            inner = aclient._http
        assert inner.is_closed


async def test_the_per_request_timeout_is_the_configured_one(make_client, respx_mock) -> None:
    route = respx_mock.get(f"{BASE}/v1/requests/r-1").mock(return_value=httpx.Response(200, json=VIEW))
    await call(make_client(timeout=7.5), "get", "r-1")
    assert sent(route).extensions["timeout"]["read"] == 7.5
