"""Usage_Record and Token_Usage_Summary models (Req 9)."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from das.models.base import DasModel
from das.models.results import TokenUsage

LLMOperation = Literal["plan", "summarize", "job"]
LLM_OPERATIONS: tuple[LLMOperation, ...] = ("plan", "job", "summarize")


class UsageRecord(DasModel):
    """Token usage of exactly one LLM call (Req 9.1-9.3).

    ``usage`` is already normalized (non-negative, total filled in).
    ``job_id`` / ``attempt`` are set only for operation ``job``.
    """

    operation: LLMOperation
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    usage: TokenUsage
    job_id: str | None = None
    attempt: int | None = Field(default=None, ge=1)


class ModelUsage(DasModel):
    """Token usage aggregated for one provider/model pair."""

    provider: str
    model: str
    usage: TokenUsage


class TokenUsageSummary(DasModel):
    """Token_Usage_Summary of a Request (Req 9.8, 9.9)."""

    total: TokenUsage
    by_operation: dict[LLMOperation, TokenUsage]
    by_model: list[ModelUsage]
    by_job: dict[str, TokenUsage]
