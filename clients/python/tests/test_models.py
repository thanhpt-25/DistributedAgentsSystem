"""Response models read what they know and ignore the rest (Phase 4 Req 9.9)."""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from typing import Any

import pytest

from das_client import (
    Budget,
    BudgetView,
    CancelResult,
    DasError,
    JobView,
    Page,
    RequestSummary,
    RequestView,
    WebhookState,
)

BUDGET: dict[str, Any] = {
    "limits": {"max_total_tokens": 100, "max_cost": "2.50"},
    "used": {"total_tokens": 7, "cost": "0.0007"},
    "exceeded": False,
}
FULL_VIEW: dict[str, Any] = {
    "request_id": "r-1",
    "status": "Completed",
    "jobs": [{"job_id": "a", "state": "Succeeded"}, {"job_id": "b", "state": "Succeeded"}],
    "transitions": [{"job_id": "a", "from": "Running", "to": "Succeeded", "at": "2026-11-11T10:00:00Z"}],
    "final_result": {"text": "done"},
    "token_usage": {"input_tokens": 3, "output_tokens": 4, "total_tokens": 7},
    "estimated_cost": {"total": "0.0007", "currency": "USD"},
    "replay_history": [{"replayed_at": "2026-11-11T10:01:00Z"}],
    "budget": BUDGET,
    "webhook": {"state": "delivered", "attempts": 2, "last_status": "200"},
}


def test_a_full_view_parses_every_field() -> None:
    view = RequestView.from_json(FULL_VIEW)
    assert (view.request_id, view.status, view.error) == ("r-1", "Completed", None)
    assert view.jobs == (JobView("a", "Succeeded"), JobView("b", "Succeeded"))
    assert view.final_result == {"text": "done"}
    assert view.token_usage is not None and view.token_usage["total_tokens"] == 7
    assert view.budget == BudgetView(Budget(100, Decimal("2.50")), 7, Decimal("0.0007"), False)
    assert view.webhook == WebhookState("delivered", 2, "200")
    assert view.is_terminal


def test_a_minimal_view_needs_only_id_and_status() -> None:
    view = RequestView.from_json({"request_id": "r-1", "status": "Pending"})
    assert (view.jobs, view.transitions, view.replay_history) == ((), (), ())
    assert (view.final_result, view.error, view.budget, view.webhook, view.estimated_cost) == (None,) * 5
    assert not view.is_terminal


@pytest.mark.parametrize(
    ("status", "terminal"),
    [
        ("Pending", False),
        ("Running", False),
        ("Completed", True),
        ("Failed", True),
        ("Cancelled", True),
        ("Paused", False),
    ],
)
def test_is_terminal(status: str, terminal: bool) -> None:
    assert RequestView.from_json({"request_id": "r", "status": status}).is_terminal is terminal


def test_null_optional_fields_are_accepted() -> None:
    data = {**FULL_VIEW, "budget": None, "webhook": None, "estimated_cost": None, "final_result": None, "error": None}
    view = RequestView.from_json(data)
    assert (view.budget, view.webhook, view.estimated_cost, view.final_result, view.error) == (None,) * 5


def test_budget_view_with_a_null_cost() -> None:
    budget = {"limits": {"max_total_tokens": 5}, "used": {"total_tokens": 9, "cost": None}, "exceeded": True}
    view = RequestView.from_json({**FULL_VIEW, "budget": budget})
    assert view.budget == BudgetView(Budget(5, None), 9, None, True)


def test_unknown_fields_are_ignored_at_every_level() -> None:
    budget = {
        "limits": {"max_total_tokens": 1, "new_limit": 4},
        "used": {"total_tokens": 1, "cost": None, "new_used": 5},
        "exceeded": False,
        "new_budget": 3,
    }
    data = {
        **FULL_VIEW,
        "new_top": 1,
        "jobs": [{"job_id": "a", "state": "Succeeded", "new_job": 2}],
        "budget": budget,
        "webhook": {"state": "pending", "attempts": 0, "last_status": None, "new_hook": 6},
    }
    view = RequestView.from_json(data)
    assert view.jobs == (JobView("a", "Succeeded"),)
    assert view.budget is not None and view.budget.limits == Budget(1, None)
    assert view.webhook == WebhookState("pending", 0, None)
    assert not hasattr(view, "new_top")


def test_an_unknown_status_string_is_kept_as_text() -> None:
    assert RequestView.from_json({"request_id": "r", "status": "Quarantined"}).status == "Quarantined"


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"status": "Pending"},
        {"request_id": "r"},
        {"request_id": 5, "status": "Pending"},
        {"request_id": "r", "status": None},
        [],
        "x",
        None,
        5,
    ],
)
def test_a_view_without_id_or_status_is_a_das_error(data: Any) -> None:
    with pytest.raises(DasError):
        RequestView.from_json(data)


@pytest.mark.parametrize(
    "patch",
    [
        {"jobs": "nope"},
        {"jobs": [5]},
        {"jobs": [{"state": "x"}]},
        {"budget": "x"},
        {"budget": {**BUDGET, "limits": {"max_cost": "abc"}}},
        {"webhook": [1]},
    ],
)
def test_a_malformed_nested_field_is_a_das_error_not_a_crash(patch: dict[str, Any]) -> None:
    with pytest.raises(DasError):
        RequestView.from_json({**FULL_VIEW, **patch})


def test_models_are_frozen_and_slotted() -> None:
    view = RequestView.from_json({"request_id": "r", "status": "Pending"})
    with pytest.raises(dataclasses.FrozenInstanceError):
        view.status = "Completed"  # type: ignore[misc]
    assert not hasattr(view, "__dict__")


def test_cancel_result() -> None:
    assert CancelResult.from_json({"status": "Cancelled", "extra": 1}) == CancelResult("Cancelled")
    with pytest.raises(DasError):
        CancelResult.from_json({})


def test_a_summary_and_a_page() -> None:
    usage = {"input": 1, "output": 2, "total": 3}
    item = {
        "request_id": "r-1",
        "status": "Completed",
        "created_at": "2026-11-11T10:00:00Z",
        "finished_at": None,
        "job_count": 2,
        "token_usage": usage,
        "new": 1,
    }
    page = Page.from_json({"items": [item], "next_cursor": "abc", "new": 2})
    assert page.next_cursor == "abc"
    assert page.items == (RequestSummary("r-1", "Completed", "2026-11-11T10:00:00Z", None, 2, usage),)
    assert Page.from_json({"items": []}) == Page((), None)
    with pytest.raises(DasError):
        Page.from_json({"items": "x"})
    with pytest.raises(DasError):
        Page.from_json({})


@pytest.mark.parametrize(
    ("budget", "expected"),
    [
        (Budget(), None),
        (Budget(max_total_tokens=5), {"max_total_tokens": 5}),
        (Budget(max_cost=Decimal("1.5")), {"max_cost": "1.5"}),
        (Budget(max_cost=Decimal("1E+2")), {"max_cost": "100"}),
        (Budget(max_cost="2.50"), {"max_cost": "2.50"}),
        (Budget(max_total_tokens=5, max_cost=Decimal("0.000001")), {"max_total_tokens": 5, "max_cost": "0.000001"}),
    ],
)
def test_budget_serialises_for_the_request_body(budget: Budget, expected: dict[str, Any] | None) -> None:
    assert budget.to_json() == expected
