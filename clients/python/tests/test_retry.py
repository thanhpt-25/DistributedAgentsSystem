"""Retry policy: connection errors, 503 and 429 are retried with jittered backoff (Phase 4 Req 9.4)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from conftest import BASE, call
from das_client import (
    AuthError,
    Conflict,
    DasError,
    IdempotencyKeyReused,
    InvalidRequest,
    NotFound,
    QuotaExceeded,
    RateLimited,
    ServiceUnavailable,
    TransportFailure,
)
from das_client._transport import retry_delay

URL = f"{BASE}/v1/requests/r-1"
VIEW = {"request_id": "r-1", "status": "Pending"}
UNAVAILABLE = {"error": "service_unavailable"}
LIMITED = {"error": "rate_limited"}


# --------------------------------------------------------------------------- the delay, pure


@pytest.mark.parametrize(
    ("attempt", "ceiling"), [(0, 0.5), (1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (5, 8.0), (50, 8.0), (10_000, 8.0)]
)
def test_backoff_doubles_from_half_a_second_and_stops_at_eight(attempt: int, ceiling: float) -> None:
    assert retry_delay(attempt, None, lambda: 1.0) == ceiling


@pytest.mark.parametrize("rand", [0.0, 0.25, 0.5, 1.0])
def test_full_jitter_scales_the_whole_delay(rand: float) -> None:
    assert retry_delay(2, None, lambda: rand) == 2.0 * rand


@pytest.mark.parametrize(
    ("retry_after", "expected"), [(None, 1.0), (0.0, 1.0), (0.5, 1.0), (1.0, 1.0), (5.0, 5.0), (3600.0, 3600.0)]
)
def test_retry_after_is_a_floor_not_a_replacement(retry_after: float | None, expected: float) -> None:
    assert retry_delay(1, retry_after, lambda: 1.0) == expected


def test_the_floor_applies_even_when_jitter_is_zero() -> None:
    assert retry_delay(0, 4.0, lambda: 0.0) == 4.0


# --------------------------------------------------------------------------- what is retried


async def test_503_then_success(make_client, respx_mock, timing) -> None:
    ok = httpx.Response(200, json=VIEW)
    route = respx_mock.get(URL).mock(side_effect=[httpx.Response(503, json=UNAVAILABLE)] * 2 + [ok])
    view = await call(make_client(), "get", "r-1")
    assert view.request_id == "r-1"
    assert route.call_count == 3
    assert timing.sleeps == [0.5, 1.0]


async def test_a_connection_error_is_retried(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(side_effect=[httpx.ConnectError("refused"), httpx.Response(200, json=VIEW)])
    assert (await call(make_client(), "get", "r-1")).request_id == "r-1"
    assert (route.call_count, timing.sleeps) == (2, [0.5])


@pytest.mark.parametrize(
    "failure", [httpx.ConnectTimeout("t"), httpx.ReadTimeout("t"), httpx.RemoteProtocolError("p")]
)
async def test_timeouts_and_protocol_errors_are_retried(make_client, respx_mock, timing, failure) -> None:
    route = respx_mock.get(URL).mock(side_effect=[failure, httpx.Response(200, json=VIEW)])
    await call(make_client(), "get", "r-1")
    assert route.call_count == 2


async def test_429_is_retried(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(side_effect=[httpx.Response(429, json=LIMITED), httpx.Response(200, json=VIEW)])
    await call(make_client(), "get", "r-1")
    assert (route.call_count, timing.sleeps) == (2, [0.5])


@pytest.mark.parametrize(
    ("status", "body", "error"),
    [
        (400, {"error": "invalid_request", "parameter": "x"}, InvalidRequest),
        (401, {"error": "unauthorized"}, AuthError),
        (404, {"error": "not_found"}, NotFound),
        (409, {"error": "budget_exceeded"}, Conflict),
        (422, {"error": "idempotency_key_reused"}, IdempotencyKeyReused),
        (500, {"error": "boom"}, DasError),
    ],
)
async def test_other_statuses_are_not_retried(make_client, respx_mock, timing, status, body, error) -> None:
    route = respx_mock.get(URL).mock(return_value=httpx.Response(status, json=body))
    with pytest.raises(error):
        await call(make_client(), "get", "r-1")
    assert (route.call_count, timing.sleeps) == (1, [])


# --------------------------------------------------------------------------- limits and the final error


async def test_default_is_three_retries_then_the_mapped_error(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(return_value=httpx.Response(503, json=UNAVAILABLE))
    with pytest.raises(ServiceUnavailable):
        await call(make_client(), "get", "r-1")
    assert route.call_count == 4  # the first try and 3 retries
    assert timing.sleeps == [0.5, 1.0, 2.0]


async def test_max_retries_is_configurable(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(return_value=httpx.Response(503))
    with pytest.raises(ServiceUnavailable):
        await call(make_client(max_retries=5), "get", "r-1")
    assert route.call_count == 6
    assert timing.sleeps == [0.5, 1.0, 2.0, 4.0, 8.0]


async def test_zero_retries_means_one_attempt(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(return_value=httpx.Response(503))
    with pytest.raises(ServiceUnavailable):
        await call(make_client(max_retries=0), "get", "r-1")
    assert (route.call_count, timing.sleeps) == (1, [])


async def test_a_429_after_the_retries_is_rate_limited_with_its_retry_after(make_client, respx_mock, timing) -> None:
    respx_mock.get(URL).mock(return_value=httpx.Response(429, json=LIMITED, headers={"Retry-After": "2"}))
    with pytest.raises(RateLimited) as caught:
        await call(make_client(), "get", "r-1")
    assert (caught.value.status_code, caught.value.retry_after) == (429, 2.0)


async def test_quota_exceeded_after_the_retries_keeps_its_type_and_limit(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(return_value=httpx.Response(429, json={"error": "quota_exceeded", "limit": 7}))
    with pytest.raises(QuotaExceeded) as caught:
        await call(make_client(), "get", "r-1")
    assert (caught.value.limit, route.call_count) == (7, 4)


async def test_a_transport_failure_after_the_retries_is_typed_and_chained(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(TransportFailure) as caught:
        await call(make_client(), "get", "r-1")
    assert route.call_count == 4 and len(timing.sleeps) == 3
    assert isinstance(caught.value, DasError)
    assert isinstance(caught.value.__cause__, httpx.ConnectError)
    assert caught.value.status_code is None


# --------------------------------------------------------------------------- Retry-After on 429


@pytest.mark.parametrize(
    ("header", "expected_first_sleep"),
    [("5", 5.0), ("0", 0.5), ("1", 1.0), ("120", 120.0), ("garbage", 0.5), ("1.5", 0.5)],
)
async def test_429_waits_at_least_the_retry_after(
    make_client, respx_mock, timing, header, expected_first_sleep
) -> None:
    limited = httpx.Response(429, json=LIMITED, headers={"Retry-After": header})
    respx_mock.get(URL).mock(side_effect=[limited, httpx.Response(200, json=VIEW)])
    await call(make_client(), "get", "r-1")
    assert timing.sleeps == [expected_first_sleep]


async def test_retry_after_is_a_floor_for_every_retry_of_the_call(make_client, respx_mock, timing) -> None:
    limited = httpx.Response(429, headers={"Retry-After": "3"})
    respx_mock.get(URL).mock(side_effect=[limited] * 3 + [httpx.Response(200, json=VIEW)])
    await call(make_client(), "get", "r-1")
    assert timing.sleeps == [3.0, 3.0, 3.0]  # backoff 0.5, 1.0, 2.0 are all below the floor


async def test_a_retry_after_on_503_is_not_a_floor(make_client, respx_mock, timing) -> None:
    unavailable = httpx.Response(503, headers={"Retry-After": "30"})
    respx_mock.get(URL).mock(side_effect=[unavailable, httpx.Response(200, json=VIEW)])
    await call(make_client(), "get", "r-1")
    assert timing.sleeps == [0.5]  # Req 9.4 gives the floor to 429 only


async def test_jitter_comes_from_the_injected_random_source(make_client, respx_mock, timing) -> None:
    timing.rand_value = 0.25
    respx_mock.get(URL).mock(side_effect=[httpx.Response(503)] * 2 + [httpx.Response(200, json=VIEW)])
    await call(make_client(), "get", "r-1")
    assert timing.sleeps == [0.125, 0.25]


# --------------------------------------------------------------------------- every method retries


@pytest.mark.parametrize(
    ("method", "args", "verb", "path", "ok"),
    [
        ("submit", ("hello",), "POST", "/v1/requests", httpx.Response(202, json={"request_id": "r-1"})),
        ("get", ("r-1",), "GET", "/v1/requests/r-1", httpx.Response(200, json=VIEW)),
        ("cancel", ("r-1",), "POST", "/v1/requests/r-1/cancel", httpx.Response(202, json={"status": "Cancelled"})),
        ("list_requests", (), "GET", "/v1/requests", httpx.Response(200, json={"items": []})),
    ],
)
async def test_each_method_retries_503(make_client, respx_mock, timing, method, args, verb, path, ok: Any) -> None:
    route = respx_mock.route(method=verb, url=f"{BASE}{path}").mock(side_effect=[httpx.Response(503), ok])
    await call(make_client(), method, *args)
    assert (route.call_count, timing.sleeps) == (2, [0.5])
