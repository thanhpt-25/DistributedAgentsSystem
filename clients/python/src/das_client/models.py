"""Response models (Phase 4 Req 9.9).

Each model's ``from_json`` takes the fields it knows and ignores the rest, so a newer server does not break
an older client. A missing optional field is ``None``; a field the client needs but cannot read is a
:class:`~das_client.errors.DasError`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import DasError

TERMINAL_STATUSES = frozenset({"Completed", "Failed", "Cancelled"})
TERMINAL_EVENT_TYPES = frozenset({"request.completed", "request.failed", "request.cancelled"})


# --------------------------------------------------------------------------- reading helpers


def _invalid(what: str, why: str) -> DasError:
    return DasError(f"unexpected response: {what} {why}")


def _obj(data: Any, what: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise _invalid(what, "is not an object")
    return data


def _str(data: dict[str, Any], key: str, what: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise _invalid(what, f"has no text field {key!r}")
    return value


def _opt_str(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key)
    return value if isinstance(value, str) else None


def _int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _opt_int(data: dict[str, Any], key: str) -> int | None:
    value = data.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _opt_dict(data: dict[str, Any], key: str) -> dict[str, Any] | None:
    value = data.get(key)
    return value if isinstance(value, dict) else None


def _list(data: dict[str, Any], key: str, what: str, *, required: bool = False) -> list[Any]:
    value = data.get(key)
    if value is None and not required:
        return []
    if not isinstance(value, list):
        raise _invalid(what, f"has no list field {key!r}")
    return value


def _decimal(value: Any, what: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise _invalid(what, "is not a number")
    try:
        return Decimal(str(value))
    except InvalidOperation:
        raise _invalid(what, "is not a number") from None


# --------------------------------------------------------------------------- models


@dataclass(frozen=True, slots=True)
class Budget:
    """Limits for one Request. ``max_cost`` may be a ``Decimal`` or its text; both are sent as text."""

    max_total_tokens: int | None = None
    max_cost: Decimal | str | None = None

    def to_json(self) -> dict[str, Any] | None:
        """Request-body form, or ``None`` when no limit is set (an empty Budget is no Budget)."""
        body: dict[str, Any] = {}
        if self.max_total_tokens is not None:
            body["max_total_tokens"] = self.max_total_tokens
        if self.max_cost is not None:
            body["max_cost"] = format(self.max_cost, "f") if isinstance(self.max_cost, Decimal) else self.max_cost
        return body or None

    @classmethod
    def from_json(cls, data: Any) -> Budget:
        data = _obj(data, "budget limits")
        return cls(_opt_int(data, "max_total_tokens"), _decimal(data.get("max_cost"), "budget max_cost"))


@dataclass(frozen=True, slots=True)
class BudgetView:
    """The ``budget`` object of a Request view: its limits, what was used, and whether it was exceeded."""

    limits: Budget
    used_total_tokens: int
    used_cost: Decimal | None
    exceeded: bool

    @classmethod
    def from_json(cls, data: Any) -> BudgetView:
        data = _obj(data, "budget")
        used = _obj(data.get("used") or {}, "budget usage")
        return cls(
            Budget.from_json(data.get("limits") or {}),
            _int(used, "total_tokens"),
            _decimal(used.get("cost"), "budget cost"),
            data.get("exceeded") is True,
        )


@dataclass(frozen=True, slots=True)
class WebhookState:
    """Delivery state of the Request's webhook: ``pending``, ``delivered`` or ``failed``."""

    state: str
    attempts: int
    last_status: str | None

    @classmethod
    def from_json(cls, data: Any) -> WebhookState:
        data = _obj(data, "webhook")
        return cls(_str(data, "state", "webhook"), _int(data, "attempts"), _opt_str(data, "last_status"))


@dataclass(frozen=True, slots=True)
class JobView:
    job_id: str
    state: str

    @classmethod
    def from_json(cls, data: Any) -> JobView:
        data = _obj(data, "job")
        return cls(_str(data, "job_id", "job"), _str(data, "state", "job"))


@dataclass(frozen=True, slots=True)
class RequestView:
    """``GET /v1/requests/{id}``. ``status`` stays plain text so a new server status does not break parsing."""

    request_id: str
    status: str
    jobs: tuple[JobView, ...] = ()
    transitions: tuple[dict[str, Any], ...] = ()
    final_result: Any = None
    error: str | None = None
    token_usage: dict[str, Any] | None = None
    estimated_cost: dict[str, Any] | None = None
    replay_history: tuple[dict[str, Any], ...] = ()
    budget: BudgetView | None = None
    webhook: WebhookState | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @classmethod
    def from_json(cls, data: Any) -> RequestView:
        data = _obj(data, "request view")
        budget, webhook = data.get("budget"), data.get("webhook")
        return cls(
            request_id=_str(data, "request_id", "request view"),
            status=_str(data, "status", "request view"),
            jobs=tuple(JobView.from_json(j) for j in _list(data, "jobs", "request view")),
            transitions=tuple(t for t in _list(data, "transitions", "request view") if isinstance(t, dict)),
            final_result=data.get("final_result"),
            error=_opt_str(data, "error"),
            token_usage=_opt_dict(data, "token_usage"),
            estimated_cost=_opt_dict(data, "estimated_cost"),
            replay_history=tuple(h for h in _list(data, "replay_history", "request view") if isinstance(h, dict)),
            budget=BudgetView.from_json(budget) if budget is not None else None,
            webhook=WebhookState.from_json(webhook) if webhook is not None else None,
        )


@dataclass(frozen=True, slots=True)
class Event:
    """One item of an event stream, or the event of a webhook delivery.

    ``type`` is ``"snapshot"`` for the first item of a stream (``data`` is then the Request view, without its
    final result) and otherwise one of the server's event types, kept as text so a new type does not break
    parsing. ``event_id`` is ``None`` only for a snapshot of a Request that has no event yet.
    """

    type: str
    event_id: str | None
    data: dict[str, Any]
    request_id: str | None

    @property
    def is_terminal(self) -> bool:
        return self.type in TERMINAL_EVENT_TYPES

    @classmethod
    def from_envelope(cls, obj: Any) -> Event | None:
        """The event of an ``{request_id, event_id, type, data}`` object, or ``None`` if it is not one."""
        if not isinstance(obj, dict):
            return None
        request_id, event_id, event_type, data = (obj.get(k) for k in ("request_id", "event_id", "type", "data"))
        if not (isinstance(request_id, str) and isinstance(event_id, str) and isinstance(event_type, str)):
            return None
        return cls(event_type, event_id, data, request_id) if isinstance(data, dict) else None


@dataclass(frozen=True, slots=True)
class Submitted:
    """The answer to ``submit``. ``replayed`` is true when the idempotency key matched an earlier submit."""

    request_id: str
    replayed: bool = False

    @classmethod
    def from_json(cls, data: Any, *, replayed: bool = False) -> Submitted:
        return cls(_str(_obj(data, "submit answer"), "request_id", "submit answer"), replayed)


@dataclass(frozen=True, slots=True)
class CancelResult:
    status: str

    @classmethod
    def from_json(cls, data: Any) -> CancelResult:
        return cls(_str(_obj(data, "cancel answer"), "status", "cancel answer"))


@dataclass(frozen=True, slots=True)
class RequestSummary:
    """One entry of ``list_requests``: no prompt, instruction or result."""

    request_id: str
    status: str
    created_at: str | None
    finished_at: str | None
    job_count: int
    token_usage: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, data: Any) -> RequestSummary:
        data = _obj(data, "list entry")
        return cls(
            _str(data, "request_id", "list entry"),
            _str(data, "status", "list entry"),
            _opt_str(data, "created_at"),
            _opt_str(data, "finished_at"),
            _int(data, "job_count"),
            _opt_dict(data, "token_usage") or {},
        )


@dataclass(frozen=True, slots=True)
class Page:
    """One page of ``list_requests``; pass ``next_cursor`` back as ``cursor`` for the next, until it is ``None``."""

    items: tuple[RequestSummary, ...]
    next_cursor: str | None = None

    @classmethod
    def from_json(cls, data: Any) -> Page:
        data = _obj(data, "list page")
        items = _list(data, "items", "list page", required=True)
        return cls(tuple(RequestSummary.from_json(i) for i in items), _opt_str(data, "next_cursor"))
