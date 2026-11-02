"""Request lifecycle status and Lease models."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from das.models.base import DasModel


class RequestStatus(StrEnum):
    PENDING = "Pending"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


TERMINAL_REQUEST: frozenset[RequestStatus] = frozenset(
    {RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED}
)


def is_terminal_request(status: RequestStatus) -> bool:
    return status in TERMINAL_REQUEST


class Lease(DasModel):
    """Time-bounded exclusive claim of one Manager_Agent on one Request (Req 2.3).

    ``epoch`` is the fencing token incremented on every acquisition; ``expires_at``
    is Redis server time in seconds.
    """

    request_id: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    epoch: int = Field(ge=1)
    expires_at: float
