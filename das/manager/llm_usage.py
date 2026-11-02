"""UsageRecords of the Manager's own LLM calls (Planner, Summarizer; Req 9.1, 9.3).

Every completion the provider returned yields exactly one UsageRecord, with the
already-normalized usage of the Completion (zeros when the provider reported none,
Req 9.6). A call that failed returned no completion and yields none.
"""
from __future__ import annotations

from typing import Final

from das.llm.base import Completion
from das.models.usage import LLMOperation, UsageRecord

UNKNOWN_LLM: Final = "unknown"


def llm_identity(llm: object) -> tuple[str, str]:
    """``(provider, model)`` of an :class:`~das.llm.retry.LLMProvider`-like object."""
    provider = getattr(llm, "provider", None)
    model = getattr(getattr(llm, "params", None), "model", None)
    return (
        provider if isinstance(provider, str) and provider else UNKNOWN_LLM,
        model if isinstance(model, str) and model else UNKNOWN_LLM,
    )


def usage_record(llm: object, operation: LLMOperation, completion: Completion) -> UsageRecord:
    """The UsageRecord of one completion returned by ``llm``."""
    provider, model = llm_identity(llm)
    return UsageRecord(operation=operation, provider=provider, model=model,
                       usage=completion.usage)


__all__ = ["UNKNOWN_LLM", "llm_identity", "usage_record"]
