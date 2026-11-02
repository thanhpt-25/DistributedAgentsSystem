"""Job state, persisted Job record, and transition-log entry (Req 13.7)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field, SerializerFunctionWrapHandler, field_validator, model_serializer

from das.models.base import DasModel, to_utc_ms
from das.models.plan import PlannedJob


class JobState(StrEnum):
    WAITING = "Waiting"
    READY = "Ready"
    DISPATCHED = "Dispatched"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    TIMED_OUT = "TimedOut"
    SKIPPED = "Skipped"
    CANCELLED = "Cancelled"


# TimedOut is intentionally non-terminal: it records a timed-out attempt that leads
# either to a re-dispatch or to Failed once max attempts are reached.
TERMINAL_JOB: frozenset[JobState] = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.SKIPPED, JobState.CANCELLED}
)


def is_terminal_job(state: JobState) -> bool:
    return state in TERMINAL_JOB


def _drop_none(data: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    """Omit Phase 2 optional fields when unset so persisted JSON stays identical to v1."""
    for name in fields:
        if data.get(name, 0) is None:
            del data[name]
    return data


class JobRecord(DasModel):
    """Persisted state of one Job of a Request."""

    job: PlannedJob
    state: JobState
    attempt: int = Field(default=0, ge=0)  # 0 = never dispatched; first dispatch = 1
    ready_since: float | None = None
    publish_failures: int = Field(default=0, ge=0)
    worker_id: str | None = None
    reason: str | None = None  # last error / skip / cancel reason
    # Phase 2 (Req 13.4): set by a Replay; ``None`` means ``cfg.max_attempts`` applies.
    attempt_limit: int | None = Field(default=None, ge=1)

    @model_serializer(mode="wrap")
    def _omit_new_none(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        return _drop_none(handler(self), ("attempt_limit",))


class TransitionEntry(DasModel):
    """One Job state change, appended in the same atomic update as the change."""

    job_id: str = Field(min_length=1)
    from_state: JobState | None
    to_state: JobState
    attempt: int = Field(ge=0)
    worker_id: str | None = None
    at: datetime
    # Phase 2 (Req 13.4): Dead_Letter_ID of the Replay that caused this change.
    dead_letter_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")

    @model_serializer(mode="wrap")
    def _omit_new_none(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        return _drop_none(handler(self), ("dead_letter_id",))

    @field_validator("at")
    @classmethod
    def _utc_ms(cls, value: datetime) -> datetime:
        return to_utc_ms(value)
