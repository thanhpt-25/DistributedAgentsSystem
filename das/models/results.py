"""Message payloads, token usage, and Final_Result models (Req 5.5, 7.5)."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from das.models.base import DasModel
from das.models.job import JobState


class TokenUsage(DasModel):
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)

    @classmethod
    def zero(cls) -> TokenUsage:
        return cls()


JobResultStatus = Literal["Succeeded", "Failed", "TimedOut", "Cancelled"]


class JobResultPayload(DasModel):
    """Job_Result published by a Worker for one attempt of a Job (Req 5.5)."""

    request_id: str = Field(min_length=1)
    job_id: str = Field(min_length=1)
    attempt: int = Field(ge=1)
    worker_id: str = Field(min_length=1)
    status: JobResultStatus
    content: str
    error: str | None = None
    usage: TokenUsage
    duration_ms: int = Field(ge=0)


class JobDispatchPayload(DasModel):
    """Job dispatch sent by a Manager; ``dependency_results`` maps dep Job_ID to content."""

    request_id: str = Field(min_length=1)
    job_id: str = Field(min_length=1)
    attempt: int = Field(ge=1)
    instruction: str = Field(min_length=1)
    required_capabilities: list[str] = Field(min_length=1)
    dependency_results: dict[str, str] = Field(default_factory=dict)


class ControlPayload(DasModel):
    command: Literal["cancel_job", "cancel_request"]
    request_id: str = Field(min_length=1)
    job_id: str | None = None


class HeartbeatPayload(DasModel):
    agent_id: str = Field(min_length=1)
    role: Literal["manager", "worker", "gateway"]
    capabilities: list[str] = Field(default_factory=list)


class JobReport(DasModel):
    """Per-Job entry of a Final_Result (Req 7.5). ``worker_id`` is None if no worker ran it."""

    job_id: str
    state: JobState
    worker_id: str | None = None
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)


class FailureEntry(DasModel):
    """Entry for a Job that ended Failed, Skipped, or Cancelled (Req 7.2)."""

    job_id: str
    state: JobState
    reason: str


class FinalResult(DasModel):
    summary: str | None = None
    jobs: list[JobReport]
    failures: list[FailureEntry] = Field(default_factory=list)
    summarizer_usage: TokenUsage = Field(default_factory=TokenUsage)
    error: str | None = None
