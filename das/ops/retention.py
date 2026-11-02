"""Retention decisions for finished Requests (Req 14).

Pure module (Req 19.5): no clock, no I/O. ``purge_request.lua`` mirrors this decision
with its own ``TIME``; the shell uses it to pick candidates.
"""

from __future__ import annotations

from typing import Literal

from das.models.request import TERMINAL_REQUEST, RequestStatus

__all__ = ["RetentionDecision", "retention_decision"]

RetentionDecision = Literal["delete", "keep", "stamp"]


def retention_decision(
    status: RequestStatus | None,
    finished_at_ms: int | None,
    now_ms: int,
    ttl_ms: int,
) -> RetentionDecision:
    """Decide what retention does with one Request.

    - missing or non-terminal Request → ``keep`` (Req 14.3)
    - terminal without ``finished_at`` → ``stamp`` (set it to ``now``; Req 14.9)
    - terminal with ``finished_at + ttl <= now`` → ``delete`` (Req 14.4)
    - otherwise → ``keep``
    """
    if status is None or status not in TERMINAL_REQUEST:
        return "keep"
    if finished_at_ms is None:
        return "stamp"
    if finished_at_ms + ttl_ms <= now_ms:
        return "delete"
    return "keep"
