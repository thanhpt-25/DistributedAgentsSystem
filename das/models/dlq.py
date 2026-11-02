"""Dead_Letter_Record and Replay-related models (Req 12, 13).

``DeadLetterFields`` is what the Manager derives from a dead-lettered Message
(:func:`das.ops.dlq.build_record`); ``DeadLetterRecord`` adds the lifecycle
fields that ``create_dead_letter.lua`` and the admin scripts maintain.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final, Literal

from pydantic import Field, field_validator

from das.models.base import DasModel, to_utc_ms
from das.models.request import RequestStatus
from das.models.results import FinalResult

DeadLetterReason = Literal["retries_exhausted", "delivery_limit", "malformed"]
RecordStatus = Literal["Pending", "Replayed", "Discarded"]

DEAD_LETTER_REASONS: Final[tuple[DeadLetterReason, ...]] = (
    "retries_exhausted",
    "delivery_limit",
    "malformed",
)
RECORD_STATUSES: Final[tuple[RecordStatus, ...]] = ("Pending", "Replayed", "Discarded")
MAX_LAST_ERROR_LENGTH: Final = 4096
DEAD_LETTER_ID_PATTERN: Final = r"^[0-9a-f]{32}$"


def _opt_utc_ms(value: datetime | None) -> datetime | None:
    return None if value is None else to_utc_ms(value)


class DeadLetterFields(DasModel):
    """Fields parsed from a dead-lettered Message (Req 12.1, 12.3).

    Each Request/Job field is ``None`` when it could not be parsed.
    """

    dead_letter_id: str = Field(pattern=DEAD_LETTER_ID_PATTERN)
    reason: DeadLetterReason
    request_id: str | None = None
    job_id: str | None = None
    attempt: int | None = Field(default=None, ge=1)
    capabilities: list[str] | None = None
    last_error: str | None = Field(default=None, max_length=MAX_LAST_ERROR_LENGTH)
    message_id: str | None = None
    # malformed only
    parse_reason: str | None = None
    parse_field: str | None = None
    original_size: int | None = Field(default=None, ge=0)
    raw_b64: str | None = None
    truncated: bool = False
    # parseable only: canonical JSON of the original envelope (incl. payload)
    envelope_json: str | None = None


class DeadLetterRecord(DeadLetterFields):
    """A persisted Dead_Letter_Record (``das:dlq:{dlid}``)."""

    status: RecordStatus
    dead_lettered_at: datetime
    status_changed_at: datetime | None = None
    discarded_at: datetime | None = None

    @field_validator("dead_lettered_at")
    @classmethod
    def _utc_ms(cls, value: datetime) -> datetime:
        return to_utc_ms(value)

    @field_validator("status_changed_at", "discarded_at")
    @classmethod
    def _opt_utc_ms(cls, value: datetime | None) -> datetime | None:
        return _opt_utc_ms(value)


class ReplayHistoryEntry(DasModel):
    """State of a Failed Request saved when a Replay reopened it (Req 13.2)."""

    dead_letter_id: str = Field(pattern=DEAD_LETTER_ID_PATTERN)
    at: datetime
    error: str | None = None
    final_result: FinalResult | None = None
    finished_at: datetime | None = None

    @field_validator("at")
    @classmethod
    def _utc_ms(cls, value: datetime) -> datetime:
        return to_utc_ms(value)

    @field_validator("finished_at")
    @classmethod
    def _opt_utc_ms(cls, value: datetime | None) -> datetime | None:
        return _opt_utc_ms(value)


class RequestTransitionEntry(DasModel):
    """Request-level transition stored in the same list as Job ``TransitionEntry``."""

    scope: Literal["request"] = "request"
    from_status: RequestStatus
    to_status: RequestStatus
    dead_letter_id: str | None = Field(default=None, pattern=DEAD_LETTER_ID_PATTERN)
    at: datetime

    @field_validator("at")
    @classmethod
    def _utc_ms(cls, value: datetime) -> datetime:
        return to_utc_ms(value)
