"""Typed errors and the status-to-error mapping (Phase 4 Req 9.5)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from das_client import (
    AuthError,
    BudgetAboveLimit,
    Conflict,
    DasError,
    IdempotencyKeyReused,
    InvalidRequest,
    NotFound,
    QuotaExceeded,
    RateLimited,
    ServiceUnavailable,
)
from das_client._transport import error_from_response, parse_retry_after

ABOVE = {"error": "budget_above_limit", "parameter": "budget.max_cost", "limit": "2.5"}


def err(status: int, body: Any = None, headers: dict[str, str] | None = None) -> DasError:
    raw = body if isinstance(body, bytes) else (b"" if body is None else json.dumps(body).encode())
    return error_from_response(status, headers or {}, raw)


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (401, {"error": "unauthorized"}, AuthError),
        (403, {"error": "forbidden"}, AuthError),
        (404, {"error": "not_found"}, NotFound),
        (409, {"error": "budget_exceeded"}, Conflict),
        (409, {"current_state": "Completed"}, Conflict),
        (422, {"error": "idempotency_key_reused"}, IdempotencyKeyReused),
        (400, ABOVE, BudgetAboveLimit),
        (400, {"error": "invalid_request", "parameter": "Idempotency-Key"}, InvalidRequest),
        (400, {"error": "invalid_body"}, InvalidRequest),
        (400, {"error": "invalid_query", "parameter": "limit"}, InvalidRequest),
        (429, {"error": "quota_exceeded", "limit": 2}, QuotaExceeded),
        (429, {"error": "rate_limited"}, RateLimited),
        (429, {"error": "too_many_streams"}, RateLimited),
        (503, {"error": "service_unavailable"}, ServiceUnavailable),
        (503, {"error": "too_many_streams"}, ServiceUnavailable),
    ],
)
def test_status_maps_to_the_typed_error(status: int, body: dict[str, Any], expected: type[DasError]) -> None:
    error = err(status, body)
    assert type(error) is expected
    assert isinstance(error, DasError)
    assert (error.status_code, error.code, error.body) == (status, body.get("error"), body)


@pytest.mark.parametrize("status", [400, 401, 404, 409, 422, 429, 500, 503])
@pytest.mark.parametrize("raw", [b"", b"not json", b"[1, 2]", b'"text"', b"null", b"\xff\xfe"])
def test_an_unusable_body_still_maps_by_status(status: int, raw: bytes) -> None:
    error = err(status, raw)
    assert error.status_code == status
    assert error.body == {} and error.code is None


@pytest.mark.parametrize("status", [402, 418, 500, 502, 504])
def test_an_unmapped_status_is_a_plain_das_error(status: int) -> None:
    error = err(status, {"error": "teapot"})
    assert type(error) is DasError
    assert (error.status_code, error.code) == (status, "teapot")


def test_another_422_is_not_reported_as_key_reuse() -> None:
    assert type(err(422, {"error": "something_else"})) is DasError


def test_budget_above_limit_carries_the_parameter_and_limit() -> None:
    error = err(400, {"error": "budget_above_limit", "parameter": "budget.max_total_tokens", "limit": 1000})
    assert isinstance(error, BudgetAboveLimit)
    assert (error.parameter, error.limit) == ("budget.max_total_tokens", 1000)


def test_invalid_request_carries_the_parameter_when_present() -> None:
    with_parameter = err(400, {"error": "invalid_request", "parameter": "budget.max_cost"})
    without = err(400, {"error": "invalid_body"})
    assert isinstance(with_parameter, InvalidRequest) and with_parameter.parameter == "budget.max_cost"
    assert isinstance(without, InvalidRequest) and without.parameter is None


@pytest.mark.parametrize(
    ("body", "limit"), [({"limit": 2}, 2), ({}, None), ({"limit": "2"}, None), ({"limit": True}, None)]
)
def test_quota_exceeded_limit_is_only_an_integer(body: dict[str, Any], limit: int | None) -> None:
    error = err(429, {"error": "quota_exceeded", **body})
    assert isinstance(error, QuotaExceeded) and error.limit == limit


@pytest.mark.parametrize(
    ("header", "seconds"), [("1", 1.0), ("30", 30.0), ("0", 0.0), (" 7 ", 7.0), ("999999", 999999.0)]
)
def test_retry_after_is_whole_seconds(header: str, seconds: float) -> None:
    assert parse_retry_after(header) == seconds
    error = err(429, {"error": "rate_limited"}, {"retry-after": header})
    assert isinstance(error, RateLimited) and error.retry_after == seconds


@pytest.mark.parametrize(
    "header", [None, "", "abc", "-1", "1.5", "Wed, 21 Oct 2026 07:28:00 GMT", "1e9", "nan", "٣"]
)
def test_a_malformed_retry_after_is_ignored(header: str | None) -> None:
    assert parse_retry_after(header) is None


def test_error_text_has_the_status_and_code_but_nothing_else() -> None:
    error = err(400, {"error": "invalid_request", "parameter": "budget.max_cost", "prompt": "PRIVATE PROMPT TEXT"})
    text = f"{error} {error!r}"
    assert "400" in text and "invalid_request" in text
    assert "PRIVATE PROMPT TEXT" not in text


def test_a_hostile_code_cannot_flood_the_message() -> None:
    error = err(500, {"error": "x" * 10_000})
    assert len(str(error)) < 300
