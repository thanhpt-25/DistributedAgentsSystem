"""Unit tests for das.ops.retention.retention_decision (Req 14.3, 14.4, 14.9)."""

from __future__ import annotations

import pytest

from das.models.request import RequestStatus
from das.ops.retention import retention_decision

TTL = 3_600_000
NOW = 10_000_000_000


@pytest.mark.parametrize("status", [None, RequestStatus.PENDING, RequestStatus.RUNNING])
def test_missing_or_non_terminal_is_kept(status: RequestStatus | None) -> None:
    # Even with an ancient finished_at, non-terminal Requests are never deleted.
    assert retention_decision(status, 0, NOW, TTL) == "keep"
    assert retention_decision(status, None, NOW, TTL) == "keep"


@pytest.mark.parametrize(
    "status", [RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED]
)
def test_terminal_ttl_boundary(status: RequestStatus) -> None:
    assert retention_decision(status, NOW - TTL, NOW, TTL) == "delete"  # exactly at TTL
    assert retention_decision(status, NOW - TTL - 1, NOW, TTL) == "delete"
    assert retention_decision(status, NOW - TTL + 1, NOW, TTL) == "keep"
    assert retention_decision(status, NOW, NOW, TTL) == "keep"


def test_terminal_without_finished_at_is_stamped() -> None:
    assert retention_decision(RequestStatus.COMPLETED, None, NOW, TTL) == "stamp"
