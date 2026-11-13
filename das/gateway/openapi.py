"""The OpenAPI document the Gateway publishes at ``GET /v1/openapi.json`` (Phase 4 Req 11.4).

FastAPI derives paths and request bodies; the routes return raw JSON responses, so their error
statuses are documented here and passed as ``responses=``. Two corrections make the document match
what the Gateway really does:

- FastAPI adds a ``422`` validation response to every route with parameters. This app turns a
  validation error into ``400 invalid_body``, so that entry is replaced by a ``400``.
- The admin routes are not in the document (they are created with ``include_in_schema=False``): it
  is readable with an ordinary API key.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Final

from fastapi import FastAPI
from pydantic import BaseModel

ERROR_SCHEMA: Final = "ErrorResponse"
_ERROR_REF: Final = {"$ref": f"#/components/schemas/{ERROR_SCHEMA}"}
_VALIDATION_SCHEMAS: Final = ("HTTPValidationError", "ValidationError")

Responses = dict[int | str, dict[str, Any]]


class ErrorResponse(BaseModel):
    """Body of every error: a stable ``error`` code, plus the fields some codes add."""

    error: str
    parameter: str | None = None
    limit: int | str | None = None
    current_state: str | None = None
    #: The Scope or role a refused call lacked (`forbidden`; Phase 5 Req 2.3).
    scope: str | None = None
    #: `tenant` on a `quota_exceeded` that is the tenant's limit, not the key's (Phase 5 Req 3.7).
    limit_scope: str | None = None


class SubmitAccepted(BaseModel):
    request_id: str


def _error(description: str) -> dict[str, Any]:
    return {"model": ErrorResponse, "description": description}


#: Every ``/v1`` route: the key check, the rate limit and the State_Store (Phase 4 Req 2.2, 2.5).
V1_RESPONSES: Final[Responses] = {
    401: _error("Missing or invalid API key (`unauthorized`)."),
    403: _error("The key lacks the Scope this route needs (`forbidden`, with `scope` naming it)."),
    429: _error("Rate limit reached (`rate_limited`). `Retry-After` gives the wait in whole seconds."),
    503: _error("The State_Store is unavailable (`service_unavailable`)."),
}

SUBMIT_RESPONSES: Final[Responses] = {
    202: {"model": SubmitAccepted, "description": "Request created."},
    200: {
        "model": SubmitAccepted,
        "description": (
            "A submit with the same `Idempotency-Key` and body was already accepted: the original "
            "`request_id`, with header `Idempotent-Replayed: true`. No new Request was created."
        ),
    },
    400: _error(
        "Invalid prompt (`empty_prompt`, `prompt_too_long`), body (`invalid_body`), `Idempotency-Key`, `budget` or "
        "`priority` (`invalid_request` with `parameter`), a budget above the key's limit "
        "(`budget_above_limit` with `parameter` and `limit`), or a `priority` above the tenant's `max_priority` "
        "(`priority_above_limit`, with `limit`). Nothing is created."
    ),
    422: _error("The `Idempotency-Key` was already used with another body (`idempotency_key_reused`)."),
    429: _error(
        "Rate limit reached (`rate_limited`, with `Retry-After`) or the key already has its maximum of "
        "active Requests (`quota_exceeded`, with `limit`); when the limit is the tenant's own, the same body "
        "carries `limit_scope: \"tenant\"`."
    ),
}

LIST_RESPONSES: Final[Responses] = {
    400: _error(
        "A query parameter is invalid, or the cursor is not valid for this caller (`invalid_query`, with `parameter`)."
    ),
    503: _error(
        "The State_Store is unavailable (`service_unavailable`), or the per-tenant index is still being built "
        "(`index_not_ready`)."
    ),
}

USAGE_RESPONSES: Final[Responses] = {
    200: {"description": "The Usage_Report of the caller's tenant: one entry per day, totals, and the estimated cost."},
    400: _error("`from` or `to` is invalid, inverted, or spans more than 92 days (`invalid_query`, with `parameter`)."),
}


def _query(name: str, description: str, **schema: Any) -> dict[str, Any]:
    return {
        "name": name,
        "in": "query",
        "required": False,
        "description": description,
        "schema": {"type": "string", **schema},
    }


#: ``GET /v1/requests`` reads its query itself, so FastAPI does not know the parameters.
LIST_PARAMETERS: Final[list[dict[str, Any]]] = [
    _query("status", "Comma-separated Request statuses to keep (at most five)."),
    _query("created_after", "Only Requests created at or after this ISO 8601 time (with zone).", format="date-time"),
    _query("created_before", "Only Requests created before this ISO 8601 time (with zone).", format="date-time"),
    _query("limit", "Page size.", pattern="^[0-9]+$"),
    _query("cursor", "`next_cursor` of the previous page. A cursor is valid only for the caller scope that got it."),
    _query("tenant", "Operator only: list one tenant's Requests. Any other caller gets `invalid_query`."),
]

USAGE_PARAMETERS: Final[list[dict[str, Any]]] = [
    _query("from", "First UTC day of the report, `YYYY-MM-DD`. Default: six days before `to`.", format="date"),
    _query("to", "Last UTC day of the report, `YYYY-MM-DD`. Default: today by the State_Store clock.", format="date"),
]

QUERY_RESPONSES: Final[Responses] = {404: _error("Unknown Request (`not_found`).")}

CANCEL_RESPONSES: Final[Responses] = {
    404: _error("Unknown Request (`not_found`)."),
    409: _error("The Request is already terminal (`request_terminal`, with `status`)."),
}

EVENTS_RESPONSES: Final[Responses] = {
    200: {
        "description": (
            "A Server-Sent Events stream: a `snapshot` event first, then the Request's events after "
            "`Last-Event-ID`, with `: keepalive` comments while idle. It ends after the terminal event."
        ),
        "content": {"text/event-stream": {"schema": {"type": "string"}}},
    },
    404: _error("Unknown Request (`not_found`)."),
    429: _error(
        "Rate limit reached (`rate_limited`, with `Retry-After`) or the key already has its maximum of "
        "open streams (`too_many_streams`)."
    ),
    503: _error(
        "The State_Store is unavailable (`service_unavailable`) or this Gateway replica already has its "
        "maximum of open streams (`too_many_streams`)."
    ),
}


def published_schema(app: FastAPI) -> dict[str, Any]:
    """The document to serve: FastAPI's schema with the validation response corrected.

    A copy, so FastAPI's own cached schema is left as generated.
    """
    schema = copy.deepcopy(app.openapi())
    for item in schema.get("paths", {}).values():
        for operation in item.values():
            responses = operation.get("responses") if isinstance(operation, dict) else None
            if not responses or "422" not in responses:
                continue
            if "HTTPValidationError" not in json.dumps(responses["422"]):
                continue  # a documented 422 of ours (idempotency_key_reused) stays
            del responses["422"]
            responses.setdefault(
                "400",
                {
                    "description": "Invalid body or parameter (`invalid_body`).",
                    "content": {"application/json": {"schema": _ERROR_REF}},
                },
            )
    components = schema.get("components", {}).get("schemas", {})
    for name in _VALIDATION_SCHEMAS:
        components.pop(name, None)
    return schema


__all__ = [
    "CANCEL_RESPONSES",
    "ERROR_SCHEMA",
    "EVENTS_RESPONSES",
    "LIST_PARAMETERS",
    "LIST_RESPONSES",
    "QUERY_RESPONSES",
    "SUBMIT_RESPONSES",
    "USAGE_PARAMETERS",
    "USAGE_RESPONSES",
    "V1_RESPONSES",
    "ErrorResponse",
    "SubmitAccepted",
    "published_schema",
]
