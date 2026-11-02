"""Common base for DAS pydantic models.

Every shared model is immutable (``frozen=True``) and rejects unknown fields
(``extra="forbid"``) so that wire payloads and persisted records have an exact shape.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict


class DasModel(BaseModel):
    """Frozen, strict-shape base model."""

    model_config = ConfigDict(frozen=True, extra="forbid")


def to_utc_ms(value: datetime) -> datetime:
    """Normalize a timezone-aware datetime to UTC truncated to millisecond precision.

    Naive datetimes are rejected because the system's timestamps are UTC by contract.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware (UTC)")
    value = value.astimezone(UTC)
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)
