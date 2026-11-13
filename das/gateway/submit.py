"""Parsing and request building for ``POST /v1/requests`` (Phase 4 Req 3.1, 4.1, 5.1, 5.2). Pure.

The Gateway handler stays thin: it calls these helpers, then ``StateStore.submit_request``.
Every problem the client can cause is an :class:`InvalidRequest` (400 ``invalid_request`` with the
offending ``parameter``) or a :class:`das.client.budget.BudgetError` (400 ``budget_above_limit``).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Final, cast, get_args

from pydantic import Field, StrictInt, ValidationError

from das.client.budget import Budget, BudgetError, decimal_text
from das.client.idempotency import body_hash, idem_key_hash, validate_idempotency_key
from das.client.settings import ClientSettings
from das.config.schema import BudgetConfig, ClientLimitsConfig, EventsConfig, PriorityClass
from das.state.store import ClientRequestSpec

IDEMPOTENCY_KEY_PARAMETER: Final = "Idempotency-Key"
#: The classes in rising order (the order of the ``PriorityClass`` literal): ``normal`` 0, ``high`` 1.
_PRIORITY_RANK: Final = {name: rank for rank, name in enumerate(get_args(PriorityClass))}


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


class PriorityAboveLimit(ValueError):
    """The body asks for a priority above the tenant's ``max_priority`` (400 ``priority_above_limit``)."""

    def __init__(self, limit: str) -> None:
        super().__init__(f"priority above {limit}")
        self.limit = limit

    def to_body(self) -> dict[str, str]:
        return {"error": "priority_above_limit", "limit": self.limit}


def parse_priority(raw: Any, settings: ClientSettings) -> PriorityClass:
    """The priority of a new Request (Phase 5 Req 7.1, 7.2): the body's, else the tenant's default.

    Anything but the strings ``normal`` and ``high`` is a 400 ``invalid_request`` naming ``priority`` (``null`` counts
    as absent). A value above ``settings.max_priority`` is :class:`PriorityAboveLimit`; a lower one is always allowed.
    """
    if raw is None:
        return settings.default_priority
    if not isinstance(raw, str) or raw not in _PRIORITY_RANK:  # type first: a list or object is unhashable
        raise InvalidRequest("priority")
    if _PRIORITY_RANK[raw] > _PRIORITY_RANK[settings.max_priority]:
        raise PriorityAboveLimit(settings.max_priority)
    return cast(PriorityClass, raw)


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
    priority: PriorityClass = "normal",
) -> ClientRequestSpec:
    """The Phase 4 fields stored with a new Request (Req 3.1, 4.2, 5.4, 6.5, 8.1), and its tenant (Phase 5 Req 3.1).

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
        tenant=settings.tenant,  # Phase 5 Req 3.1, 3.7
        tenant_max_active=settings.tenant_max_active,
        priority=priority,  # the store keeps it only when ``high`` (Phase 5 Req 7.3)
    )


__all__ = [
    "BudgetBody",
    "InvalidRequest",
    "PriorityAboveLimit",
    "above_limit_body",
    "client_spec",
    "parse_budget",
    "parse_idempotency_key",
    "parse_priority",
]
