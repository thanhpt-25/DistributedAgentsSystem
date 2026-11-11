"""Parts of the HTTP clients that need no I/O, shared by the sync and async classes (Phase 4 Req 9.3-9.5).

Everything here is a plain function, so retry and error rules are tested without a network.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable, Iterable, Mapping
from typing import Any
from urllib.parse import quote

import httpx

from .errors import (
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
from .models import Budget

#: Statuses worth another try; connection errors are retried too (Req 9.4).
RETRYABLE_STATUSES = frozenset({429, 503})
BASE_DELAY = 0.5
MAX_DELAY = 8.0
#: ``2 ** 5 * BASE_DELAY`` already exceeds the cap; clamping the exponent keeps huge attempt counts cheap.
_MAX_EXPONENT = 5
#: Longest ``Retry-After`` we read; more digits than this is not a delay a server means.
_MAX_RETRY_AFTER_DIGITS = 10


# --------------------------------------------------------------------------- retry


def retry_delay(attempt: int, retry_after: float | None, rand: Callable[[], float]) -> float:
    """Seconds to wait before retry number ``attempt`` (0 for the first): full jitter, ``Retry-After`` as a floor."""
    ceiling = min(MAX_DELAY, BASE_DELAY * 2 ** min(attempt, _MAX_EXPONENT))
    delay = ceiling * rand()
    return delay if retry_after is None else max(delay, retry_after)


def next_delay(
    attempt: int,
    max_retries: int,
    rand: Callable[[], float],
    *,
    status: int | None = None,
    retry_after: float | None = None,
) -> float | None:
    """Wait before the next try, or ``None`` when the call must fail now.

    ``status`` is ``None`` for a connection error. Only a 429 gets the ``Retry-After`` floor (Req 9.4).
    """
    if attempt >= max_retries or (status is not None and status not in RETRYABLE_STATUSES):
        return None
    return retry_delay(attempt, retry_after if status == 429 else None, rand)


def parse_retry_after(value: str | None) -> float | None:
    """Whole seconds from a ``Retry-After`` header; an HTTP date or anything malformed is ignored."""
    if value is None:
        return None
    text = value.strip()
    if not (text.isascii() and text.isdigit()) or len(text) > _MAX_RETRY_AFTER_DIGITS:
        return None
    return float(int(text))


# --------------------------------------------------------------------------- errors


def _json_object(body: bytes) -> dict[str, Any]:
    try:
        data = json.loads(body)
    except (ValueError, RecursionError):
        return {}
    return data if isinstance(data, dict) else {}


def _text(obj: dict[str, Any], key: str) -> str | None:
    value = obj.get(key)
    return value if isinstance(value, str) else None


def _count(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def error_from_response(status: int, headers: Mapping[str, str], body: bytes) -> DasError:
    """The typed error for an error response (Req 9.5). Works from the status when the body is unusable."""
    obj = _json_object(body)
    code, parameter = _text(obj, "error"), _text(obj, "parameter")
    common: dict[str, Any] = {"status_code": status, "code": code, "body": obj}
    if status in (401, 403):
        return AuthError(**common)
    if status == 404:
        return NotFound(**common)
    if status == 409:
        return Conflict(**common)
    if status == 422 and code == "idempotency_key_reused":
        return IdempotencyKeyReused(**common)
    if status == 400:
        if code == "budget_above_limit":
            return BudgetAboveLimit(parameter=parameter, limit=obj.get("limit"), **common)
        return InvalidRequest(parameter=parameter, **common)
    if status == 429:
        if code == "quota_exceeded":
            return QuotaExceeded(limit=_count(obj.get("limit")), **common)
        lowered = {k.lower(): v for k, v in headers.items()}
        return RateLimited(retry_after=parse_retry_after(lowered.get("retry-after")), **common)
    if status == 503:
        return ServiceUnavailable(**common)
    return DasError(**common)


def parse_json(response: httpx.Response) -> Any:
    """The JSON body of a success response; a body that is not JSON is a :class:`DasError`."""
    try:
        return response.json()
    except (ValueError, RecursionError):
        raise DasError("unexpected response: the body is not JSON", status_code=response.status_code) from None


# --------------------------------------------------------------------------- requests


def path_segment(request_id: str) -> str:
    """A Request id as one URL path segment. ``.`` and ``..`` are encoded so httpx cannot fold them away."""
    if not isinstance(request_id, str) or not request_id:
        raise ValueError("request_id must be a non-empty string")
    segment = quote(request_id, safe="")
    return segment.replace(".", "%2E") if set(segment) == {"."} else segment


def submit_body(prompt: str, budget: Budget | None) -> dict[str, Any]:
    body: dict[str, Any] = {"prompt": prompt}
    limits = budget.to_json() if budget is not None else None
    if limits is not None:
        body["budget"] = limits
    return body


def _timestamp(value: dt.datetime | str) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must have a time zone (the server rejects a timestamp without one)")
    return value.astimezone(dt.timezone.utc).isoformat()  # datetime.UTC needs Python 3.11


def list_params(
    *,
    status: str | Iterable[str] | None = None,
    created_after: dt.datetime | str | None = None,
    created_before: dt.datetime | str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> dict[str, str]:
    """Query parameters for ``GET /v1/requests``: only the filters that were given."""
    params: dict[str, str] = {}
    if status is not None:
        params["status"] = status if isinstance(status, str) else ",".join(status)
    if created_after is not None:
        params["created_after"] = _timestamp(created_after)
    if created_before is not None:
        params["created_before"] = _timestamp(created_before)
    if limit is not None:
        params["limit"] = str(limit)
    if cursor is not None:
        params["cursor"] = cursor
    return params


#: Seconds between polls of ``get`` once streaming has failed, or while a finished stream's Request is not yet
#: reported finished (Req 9.6).
POLL_INTERVAL = 1.0


def events_path(request_id: str) -> str:
    return f"/v1/requests/{path_segment(request_id)}/events"


def stream_headers(api_key: str, last_event_id: str | None) -> dict[str, str]:
    headers = {"X-API-Key": api_key, "Accept": "text/event-stream", "Cache-Control": "no-cache"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    return headers


def is_event_stream(response: httpx.Response) -> bool:
    return response.headers.get("content-type", "").lower().startswith("text/event-stream")


def is_replay(response: httpx.Response) -> bool:
    """True for a submit answered from an earlier one (``Idempotent-Replayed: true``)."""
    return response.headers.get("idempotent-replayed") == "true"
