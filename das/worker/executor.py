"""Runs one Job attempt as one LLM completion (Requirements 5.4-5.8)."""
from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import replace

from das.clock import Clock, SystemClock
from das.llm.base import ChatMessage, Completion
from das.llm.errors import LLMCallFailed
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload, JobResultPayload, JobResultStatus, TokenUsage
from das.structured.schema import check_schema
from das.structured.validate import Valid, Violation, parse_and_validate

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
    if job.output_schema is not None:  # Phase 6 Req 1.4: only a Structured_Job's prompt changes
        parts.append(
            "Reply with only a JSON value that matches this JSON Schema, and nothing else:\n"
            + json.dumps(job.output_schema, sort_keys=True)
        )
    return "\n\n".join(parts)


def build_correction_message(violation: Violation) -> str:
    """The one corrective request (Req 1.5): names the JSON path and the reason, never the rejected content."""
    return (
        f"Your previous reply did not match the required JSON Schema at {violation.path}: {violation.message}. "
        "Reply again with only a JSON value that matches the schema, and nothing else."
    )


def _add_usage(a: TokenUsage, b: TokenUsage) -> TokenUsage:
    return TokenUsage(
        input_tokens=a.input_tokens + b.input_tokens,
        output_tokens=a.output_tokens + b.output_tokens,
        total_tokens=a.total_tokens + b.total_tokens,
    )


def _llm_failure(provider: object, model: object, reason: object) -> str:
    return f"LLM call failed: provider={provider} model={model} reason={reason}"


class _AttemptFailed(Exception):
    """A structured attempt that fails after spending tokens, so the Job_Result still reports them (Req 1.5, 1.6)."""

    def __init__(self, error: str, usage: TokenUsage, model: str | None = None) -> None:
        super().__init__(error)
        self.error = error
        self.usage = usage
        self.model = model  # the model of the last completion; ``None`` is the provider's own


#: Results whose headers were never asked for (a publish that did not happen) must not pile up.
MAX_REMEMBERED_MODELS = 4096


class JobExecutor:
    """Executes a decoded Job dispatch and builds its Job_Result payload."""

    def __init__(
        self,
        provider: LLMProvider,
        worker_id: str,
        job_timeout: float = 300,
        clock: Clock | None = None,
        capability_models: Mapping[str, str] | None = None,
    ) -> None:
        if not worker_id:
            raise ValueError("worker_id must be non-empty")
        if isinstance(job_timeout, bool) or not job_timeout > 0:
            raise ValueError("job_timeout must be a positive number of seconds")
        self._provider = provider
        self._worker_id = worker_id
        self._job_timeout = float(job_timeout)
        self._clock: Clock = clock or SystemClock()
        self._capability_models: dict[str, str] = dict(capability_models or {})  # Phase 6 Req 2.2
        #: The model that answered each attempt, until its result headers are built (Req 2.5).
        self._models: OrderedDict[tuple[str, str, int], str] = OrderedDict()

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

    def headers_for(self, result: JobResultPayload) -> dict[str, str]:
        """Headers for one Job_Result: the provider and the model that answered it (Phase 6 Req 2.5, D2).

        The model is remembered when the attempt ends and handed out once; a result this executor does not remember
        (it was already asked for, or the attempt never reached a model) gets the provider's own, as in Phase 5.
        """
        headers = self.result_headers()
        model = self._models.pop((result.request_id, result.job_id, result.attempt), None)
        if model is not None:
            headers[MODEL_HEADER] = model
        return headers

    def _remember(self, job: JobDispatchPayload, model: str | None) -> None:
        """Note which model answered ``job``'s attempt; ``None`` is the provider's own, which needs no note."""
        if model is None:
            return
        self._models[(job.request_id, job.job_id, job.attempt)] = model
        while len(self._models) > MAX_REMEMBERED_MODELS:
            self._models.popitem(last=False)

    async def execute(
        self, job: JobDispatchPayload, cancel: asyncio.Event, routing_capability: str | None = None
    ) -> JobResultPayload:
        """Run the completion, racing it against the timeout and the cancel event.

        ``routing_capability`` is the Capability whose queue the dispatch came from; its Capability_Model, when one
        is configured, runs the Job (Phase 6 Req 2.2), otherwise the provider's own model does.

        If the calling task itself is cancelled (shutdown), the provider call is
        cancelled too and ``CancelledError`` propagates.
        """
        start = self._clock.monotonic()
        model = self._capability_models.get(routing_capability) if routing_capability is not None else None

        if cancel.is_set():
            return self._result(job, "Cancelled", start, error="job cancelled")

        if job.output_schema is not None:
            reason = check_schema(job.output_schema)  # a Manager validates plans, but never trust the wire (Req 1.1)
            if reason is not None:
                return self._result(job, "Failed", start, error=f"output_schema_invalid: {reason}")

        messages = [ChatMessage(role="user", content=build_user_message(job))]
        call = asyncio.create_task(self._complete(job, messages, model))
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
            except _AttemptFailed as exc:
                return self._result(job, "Failed", start, error=exc.error, usage=exc.usage, model=exc.model)
            except LLMCallFailed as exc:
                return self._result(
                    job, "Failed", start, error=_llm_failure(exc.provider, exc.model, exc.reason),
                    model=exc.model if exc.model != self._provider.params.model else None,
                )
            except Exception as exc:  # noqa: BLE001 - any other failure fails the attempt
                return self._result(
                    job, "Failed", start,
                    error=_llm_failure(
                        self._provider.provider, self._provider.params.model, f"{type(exc).__name__}: {exc}"
                    ),
                )
            return self._result(
                job, "Succeeded", start, content=completion.content, usage=completion.usage, model=completion.model
            )
        finally:
            for task in (call, cancelled):
                if not task.done():
                    task.cancel()
            # Reap both tasks (retrieves any exception) without masking an
            # outer cancellation of this coroutine.
            await asyncio.gather(call, cancelled, return_exceptions=True)

    async def _complete(
        self, job: JobDispatchPayload, messages: list[ChatMessage], model: str | None = None
    ) -> Completion:
        """One completion, or for a Structured_Job up to two (Req 1.4 to 1.7).

        A reply that does not match gets exactly one corrective completion in the same attempt, on the model that
        answered the first (so a Fallback_Model that rescued it is not asked to fail again). The tokens of both are
        summed into the returned Completion, or into :class:`_AttemptFailed` when the Job fails.
        """
        first = await self._provider.complete(SYSTEM_PROMPT, messages, operation="job", model=model)
        schema = job.output_schema
        if schema is None:
            return first
        verdict = parse_and_validate(first.content, schema)
        if isinstance(verdict, Valid):
            return replace(first, content=verdict.text)
        retry = [
            *messages,
            ChatMessage(role="assistant", content=first.content or "(empty reply)"),
            ChatMessage(role="user", content=build_correction_message(verdict)),
        ]
        try:
            second = await self._provider.complete(SYSTEM_PROMPT, retry, operation="job", model=first.model)
        except LLMCallFailed as exc:
            failed_model = exc.model if exc.model != self._provider.params.model else None
            raise _AttemptFailed(_llm_failure(exc.provider, exc.model, exc.reason), first.usage, failed_model) from exc
        usage = _add_usage(first.usage, second.usage)
        final = parse_and_validate(second.content, schema)
        if isinstance(final, Violation):
            raise _AttemptFailed(f"output_schema_violation: {final.path}: {final.message}", usage, second.model)
        return replace(second, content=final.text, usage=usage)

    def _result(
        self,
        job: JobDispatchPayload,
        status: JobResultStatus,
        start: float,
        *,
        content: str = "",
        usage: TokenUsage | None = None,
        error: str | None = None,
        model: str | None = None,
    ) -> JobResultPayload:
        self._remember(job, model)
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
