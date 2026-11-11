"""``decide_replay`` refuses a Request that failed by its budget
(das-client-api task 7.8; Phase 4 Req 5.10; check order of das-observability-operations Req 13.7)."""

from __future__ import annotations

import pytest

from das.models import JobState, RequestStatus
from das.ops.replay import REPLAY_CHECK_ORDER, RequestView, decide_replay
from tests.unit.test_ops_replay import _jobrec, _rec

BUDGET_ERRORS = ("budget_exceeded", "budget_unenforceable")


def failed(error: str | None) -> RequestView:
    return RequestView(RequestStatus.FAILED, error)


def test_the_refused_errors_are_the_ones_the_manager_stores() -> None:
    """``das.ops`` cannot import the Manager, so the strings are repeated; this keeps them in step."""
    from das.manager.machine import BUDGET_EXCEEDED, BUDGET_UNENFORCEABLE
    from das.ops.replay import BUDGET_FAILURES

    assert BUDGET_FAILURES == {BUDGET_EXCEEDED, BUDGET_UNENFORCEABLE} == set(BUDGET_ERRORS)


@pytest.mark.parametrize("error", BUDGET_ERRORS)
def test_a_budget_failed_request_cannot_be_replayed(error: str) -> None:
    d = decide_replay(_rec(), failed(error), _jobrec(), lease_active=False)
    assert (d.outcome, d.current_state, d.accepted) == ("budget_exceeded", None, False)
    assert d.new_attempt is None and d.request_status_before is None


@pytest.mark.parametrize("error", [None, "", "job a failed", "budget", "Budget_Exceeded", "budget_exceeded: tokens"])
def test_any_other_failure_is_still_replayable(error: str | None) -> None:
    d = decide_replay(_rec(), failed(error), _jobrec(), lease_active=False)
    assert d.accepted and d.request_status_before is RequestStatus.FAILED


def test_the_view_defaults_to_no_error() -> None:
    assert RequestView(RequestStatus.FAILED).error is None
    assert decide_replay(_rec(), RequestView(RequestStatus.FAILED), _jobrec(), False).accepted


def test_a_running_request_is_not_refused_whatever_its_error_field() -> None:
    d = decide_replay(_rec(), RequestView(RequestStatus.RUNNING, "budget_exceeded"), _jobrec(), True)
    assert d.accepted


# --- where the new check sits in the order -------------------------------------


def test_the_check_comes_after_request_state_and_before_job_state() -> None:
    order = list(REPLAY_CHECK_ORDER)
    assert order.index("request_state") < order.index("budget_exceeded") < order.index("job_state")
    assert len(order) == len(set(order))


@pytest.mark.parametrize(
    ("rec", "req", "job", "lease", "outcome"),
    [
        # Earlier checks win over the budget check.
        (None, failed("budget_exceeded"), _jobrec(), False, "not_found"),
        (_rec("Replayed"), failed("budget_exceeded"), _jobrec(), False, "record_not_pending"),
        (_rec(reason="malformed"), failed("budget_exceeded"), _jobrec(), False, "not_replayable"),
        (_rec(), None, None, False, "request_gone"),
        (_rec(), RequestView(RequestStatus.COMPLETED, "budget_exceeded"), _jobrec(), False, "request_state"),
        (_rec(), RequestView(RequestStatus.CANCELLED, "budget_exceeded"), _jobrec(), False, "request_state"),
        # The budget check wins over every later one.
        (_rec(), failed("budget_exceeded"), None, False, "budget_exceeded"),
        (_rec(), failed("budget_exceeded"), _jobrec(JobState.SUCCEEDED), False, "budget_exceeded"),
        (_rec(attempt=1), failed("budget_unenforceable"), _jobrec(), False, "budget_exceeded"),
        (_rec(), failed("budget_exceeded"), _jobrec(), True, "budget_exceeded"),
    ],
)
def test_check_order_around_the_budget_check(rec, req, job, lease, outcome) -> None:  # type: ignore[no-untyped-def]
    assert decide_replay(rec, req, job, lease).outcome == outcome
