"""Parsing and request building for ``POST /v1/requests`` (Phase 4 Req 3.1, 4.1, 5.1, 5.2). Pure.

The Gateway handler stays thin: it calls these helpers, then ``StateStore.submit_request``.
Every problem the client can cause is an :class:`InvalidRequest` (400 ``invalid_request`` with the
offending ``parameter``) or a :class:`das.client.budget.BudgetError` (400 ``budget_above_limit``).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Final

from pydantic import Field, StrictInt, ValidationError

from das.client.budget import Budget, BudgetError, decimal_text
from das.client.idempotency import body_hash, idem_key_hash, validate_idempotency_key
from das.client.settings import ClientSettings
from das.config.schema import BudgetConfig, ClientLimitsConfig, EventsConfig
from das.state.store import ClientRequestSpec

IDEMPOTENCY_KEY_PARAMETER: Final = "Idempotency-Key"


class InvalidRequest(ValueError):
    """A client-supplied header or body field is invalid (400 ``invalid_request``)."""

    def __init__(self, parameter: str) -> None:
        super().__init__(f"invalid {parameter}")
        self.parameter = parameter

    def to_body(self) -> dict[str, str]:
        return {"error": "invalid_request", "parameter": self.parameter}


class BudgetBody(BudgetConfig):
    """The ``budget`` object of the submit body (Phase 4 Req 5.1).

    Same ranges as a key's ``default_budget``, but ``max_total_tokens`` must be a JSON integer:
    ``true``, ``"5"`` and ``5.5`` are wrong types, not values to coerce.
    """

    max_total_tokens: Annotated[StrictInt, Field(ge=1)] | None = None


def parse_idempotency_key(value: str | None) -> str | None:
    """The ``Idempotency-Key`` header, or ``None`` when absent (Req 4.1)."""
    if value is None:
        return None
    if not validate_idempotency_key(value):
        raise InvalidRequest(IDEMPOTENCY_KEY_PARAMETER)
    return value


def parse_budget(raw: Any) -> Budget | None:
    """The client's own Budget from the body's ``budget`` value (Req 5.1).

    ``None`` when absent or when no field is set: an empty Budget is no Budget. The parameter of
    a rejection is ``budget`` for a non-object and ``budget.<field>`` for a bad or unknown field.
    """
    if raw is None:
        return None
    try:
        body = BudgetBody.model_validate(raw)
    except ValidationError as exc:
        path = "".join(f".{part}" for part in exc.errors()[0]["loc"])
        raise InvalidRequest(f"budget{path}") from None
    budget = Budget(max_total_tokens=body.max_total_tokens, max_cost=body.max_cost)
    return None if budget.is_empty() else budget


def above_limit_body(error: BudgetError) -> dict[str, Any]:
    """400 body of ``budget_above_limit``; ``max_cost`` limits are decimal text, as in the Request view."""
    limit = error.limit
    return {
        "error": "budget_above_limit",
        "parameter": error.parameter,
        "limit": decimal_text(limit) if isinstance(limit, Decimal) else limit,
    }


def client_spec(
    settings: ClientSettings,
    *,
    prompt: str,
    effective_budget: Budget | None,
    client_budget: Budget | None,
    idempotency_key: str | None,
    limits: ClientLimitsConfig,
    events: EventsConfig,
) -> ClientRequestSpec:
    """The Phase 4 fields stored with a new Request (Req 3.1, 4.2, 5.4, 6.5, 8.1).

    The Body_Hash covers the Budget as the client sent it, not the merged one, so a later change
    of the key's default does not turn a retry into a conflict (Req 4.3, 4.4).
    """
    keyed = idempotency_key is not None
    return ClientRequestSpec(
        fingerprint=settings.fingerprint,
        budget=effective_budget,
        webhook_url=settings.webhook_url,
        webhook_secret_name=settings.webhook_secret_name,
        idempotency_key_hash=idem_key_hash(idempotency_key) if idempotency_key is not None else None,
        body_hash=body_hash(prompt, client_budget) if keyed else None,
        idempotency_ttl=limits.idempotency_ttl,
        max_active_requests=settings.max_active_requests,
        events_max_len=events.max_len,
    )


__all__ = [
    "BudgetBody",
    "InvalidRequest",
    "above_limit_body",
    "client_spec",
    "parse_budget",
    "parse_idempotency_key",
]
