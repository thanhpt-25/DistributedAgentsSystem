"""Common base for DAS pydantic models.

Every shared model is immutable (``frozen=True``) and rejects unknown fields
(``extra="forbid"``) so that wire payloads and persisted records have an exact shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, SerializerFunctionWrapHandler, model_serializer


class DasModel(BaseModel):
    """Frozen, strict-shape base model."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class WithOutputSchema(DasModel):
    """A Job or its dispatch, optionally carrying an Output_Schema (Phase 6 Req 1.1, 1.3).

    The field is left out of every serialization while it is ``None``, so plans, stored records and wire messages
    that do not use structured output keep exactly the bytes they had before the field existed.
    """

    output_schema: dict[str, Any] | None = None

    @model_serializer(mode="wrap")
    def _omit_absent_output_schema(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if data.get("output_schema") is None:
            data.pop("output_schema", None)
        return data


def to_utc_ms(value: datetime) -> datetime:
    """Normalize a timezone-aware datetime to UTC truncated to millisecond precision.

    Naive datetimes are rejected because the system's timestamps are UTC by contract.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware (UTC)")
    value = value.astimezone(UTC)
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)
