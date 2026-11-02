"""Runs one Job attempt as one LLM completion (Requirements 5.4-5.8)."""
from __future__ import annotations

import asyncio

from das.clock import Clock, SystemClock
from das.llm.base import ChatMessage, Completion
from das.llm.errors import LLMCallFailed
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload, JobResultPayload, JobResultStatus, TokenUsage

#: Transport headers naming the LLM that produced a Job_Result (design D2, Req 9.2).
#: They travel outside the codec bytes so a v1 Manager still decodes the payload.
PROVIDER_HEADER = "x-llm-provider"
MODEL_HEADER = "x-llm-model"

SYSTEM_PROMPT = (
    "You are a worker agent in a distributed system. Complete the assigned job "
    "instruction. Results of the jobs it depends on are provided for context."
)


def build_user_message(job: JobDispatchPayload) -> str:
    """Instruction followed by every dependency result, labelled by its Job_ID (5.4)."""
    parts = [f"Instruction:\n{job.instruction}"]
    for dep_id in sorted(job.dependency_results):
        parts.append(f"Result of dependency job {dep_id}:\n{job.dependency_results[dep_id]}")
    return "\n\n".join(parts)


class JobExecutor:
    """Executes a decoded Job dispatch and builds its Job_Result payload."""

    def __init__(
        self,
        provider: LLMProvider,
        worker_id: str,
        job_timeout: float = 300,
        clock: Clock | None = None,
    ) -> None:
        if not worker_id:
            raise ValueError("worker_id must be non-empty")
        if isinstance(job_timeout, bool) or not job_timeout > 0:
            raise ValueError("job_timeout must be a positive number of seconds")
        self._provider = provider
        self._worker_id = worker_id
        self._job_timeout = float(job_timeout)
        self._clock: Clock = clock or SystemClock()

    @property
    def job_timeout(self) -> float:
        return self._job_timeout

    def result_headers(self) -> dict[str, str]:
        """Headers published with every Job_Result of this executor, whatever its status (D2).

        Token usage itself is already normalized by the adapter (``normalize_usage``,
        D13), so the payload never carries negative counts (Req 9.6).
        """
        return {
            PROVIDER_HEADER: str(self._provider.provider),
            MODEL_HEADER: str(self._provider.params.model),
        }

    async def execute(self, job: JobDispatchPayload, cancel: asyncio.Event) -> JobResultPayload:
        """Run the completion, racing it against the timeout and the cancel event.

        If the calling task itself is cancelled (shutdown), the provider call is
        cancelled too and ``CancelledError`` propagates.
        """
        start = self._clock.monotonic()

        if cancel.is_set():
            return self._result(job, "Cancelled", start, error="job cancelled")

        messages = [ChatMessage(role="user", content=build_user_message(job))]
        call = asyncio.create_task(self._provider.complete(SYSTEM_PROMPT, messages, operation="job"))
        cancelled = asyncio.create_task(cancel.wait())
        try:
            try:
                async with asyncio.timeout(self._job_timeout):
                    done, _ = await asyncio.wait(
                        {call, cancelled}, return_when=asyncio.FIRST_COMPLETED
                    )
            except TimeoutError:
                return self._result(
                    job, "TimedOut", start,
                    error=f"job timed out after {self._job_timeout:g}s",
                )
            if cancelled in done:
                return self._result(job, "Cancelled", start, error="job cancelled")
            try:
                completion: Completion = call.result()
            except LLMCallFailed as exc:
                return self._result(
                    job, "Failed", start,
                    error=(
                        f"LLM call failed: provider={exc.provider} model={exc.model} "
                        f"reason={exc.reason}"
                    ),
                )
            except Exception as exc:  # noqa: BLE001 - any other failure fails the attempt
                return self._result(
                    job, "Failed", start,
                    error=(
                        f"LLM call failed: provider={self._provider.provider} "
                        f"model={self._provider.params.model} "
                        f"reason={type(exc).__name__}: {exc}"
                    ),
                )
            return self._result(
                job, "Succeeded", start, content=completion.content, usage=completion.usage
            )
        finally:
            for task in (call, cancelled):
                if not task.done():
                    task.cancel()
            # Reap both tasks (retrieves any exception) without masking an
            # outer cancellation of this coroutine.
            await asyncio.gather(call, cancelled, return_exceptions=True)

    def _result(
        self,
        job: JobDispatchPayload,
        status: JobResultStatus,
        start: float,
        *,
        content: str = "",
        usage: TokenUsage | None = None,
        error: str | None = None,
    ) -> JobResultPayload:
        elapsed = self._clock.monotonic() - start
        return JobResultPayload(
            request_id=job.request_id,
            job_id=job.job_id,
            attempt=job.attempt,
            worker_id=self._worker_id,
            status=status,
            content=content,
            error=error,
            usage=usage or TokenUsage.zero(),
            duration_ms=max(0, int(round(elapsed * 1000))),
        )


__all__ = [
    "MODEL_HEADER",
    "PROVIDER_HEADER",
    "SYSTEM_PROMPT",
    "JobExecutor",
    "build_user_message",
]
