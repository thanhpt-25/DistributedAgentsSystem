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


class SubmitAccepted(BaseModel):
    request_id: str


def _error(description: str) -> dict[str, Any]:
    return {"model": ErrorResponse, "description": description}


#: Every ``/v1`` route: the key check, the rate limit and the State_Store (Phase 4 Req 2.2, 2.5).
V1_RESPONSES: Final[Responses] = {
    401: _error("Missing or invalid API key (`unauthorized`)."),
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
        "Invalid prompt (`empty_prompt`, `prompt_too_long`), body (`invalid_body`), `Idempotency-Key` or "
        "`budget` (`invalid_request` with `parameter`), or a budget above the key's limit "
        "(`budget_above_limit` with `parameter` and `limit`)."
    ),
    422: _error("The `Idempotency-Key` was already used with another body (`idempotency_key_reused`)."),
    429: _error(
        "Rate limit reached (`rate_limited`, with `Retry-After`) or the key already has its maximum of "
        "active Requests (`quota_exceeded`, with `limit`)."
    ),
}

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
    "QUERY_RESPONSES",
    "SUBMIT_RESPONSES",
    "V1_RESPONSES",
    "ErrorResponse",
    "SubmitAccepted",
    "published_schema",
]
