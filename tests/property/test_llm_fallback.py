"""Property 5: for all sequences of model failures the LLMProvider calls the Fallback_Model at most once per completion
and never after an authentication error (das-smarter-agents task 2.4; Req 8.5, 2.3, 2.4).

The provider is checked against a small pure reference of the rules: one run on the model asked for (the call and its
transient retries), then, only when that run ended in a permanent error that is not an authentication error, and a
Fallback_Model is configured and differs from the model asked for, exactly one more run on the Fallback_Model."""

from __future__ import annotations

import asyncio
from typing import Literal

from hypothesis import given, settings
from hypothesis import strategies as st

from das.llm.base import ChatMessage, ModelParams
from das.llm.errors import AuthLLMError, LLMCallFailed, PermanentLLMError, TransientLLMError
from das.llm.retry import LLMProvider
from tests.fakes.llm_adapter import FakeAdapter, completion

Kind = Literal["ok", "transient", "permanent", "auth"]
DEFAULT = "big"
PARAMS = ModelParams(model=DEFAULT, temperature=0.0, max_output_tokens=64)
MESSAGES = [ChatMessage(role="user", content="hi")]


def reference(
    script: list[Kind], retries: int, asked: str, fallback: str | None
) -> tuple[list[str], Kind]:
    """The models called, in order, and how the whole completion ended (the end of its last run)."""
    outcomes = iter(script)
    calls: list[str] = []

    def run(model: str) -> Kind:
        attempts = 0
        while True:
            attempts += 1
            kind = next(outcomes)
            calls.append(model)
            if kind in ("ok", "permanent", "auth"):
                return kind
            if attempts > retries:  # a transient error with no retry left
                return "transient"

    first = run(asked)
    if first == "permanent" and fallback is not None and fallback != asked:
        return calls, run(fallback)
    return calls, first


def scripted(kind: Kind):  # noqa: ANN202
    if kind == "ok":
        return completion("ok")
    return {"transient": TransientLLMError("t"), "permanent": PermanentLLMError("p"), "auth": AuthLLMError("a")}[kind]


async def no_sleep(_: float) -> None:
    return None


async def actual(script: list[Kind], retries: int, asked: str | None, fallback: str | None):  # noqa: ANN202
    adapter = FakeAdapter([scripted(k) for k in script])
    provider = LLMProvider(adapter, PARAMS, retry_count=retries, timeout=1, sleep=no_sleep, fallback_model=fallback)
    try:
        result = await provider.complete("s", MESSAGES, operation="job", model=asked)
        return [c.params.model for c in adapter.calls], "ok", result
    except LLMCallFailed as failure:
        return [c.params.model for c in adapter.calls], failure, None


@given(
    script=st.lists(st.sampled_from(["ok", "transient", "permanent", "auth"]), min_size=8, max_size=8),
    retries=st.integers(0, 3),
    asked=st.sampled_from([None, "small"]),
    fallback=st.sampled_from([None, "backup", "big", "small"]),
)
@settings(deadline=None)
def test_the_provider_follows_the_fallback_rules_exactly(
    script: list[Kind], retries: int, asked: str | None, fallback: str | None
) -> None:
    want_calls, want_end = reference(script, retries, asked or DEFAULT, fallback)
    calls, got, result = asyncio.run(actual(script, retries, asked, fallback))
    assert calls == want_calls
    if want_end == "ok":
        assert got == "ok" and result is not None
        assert result.model == (None if calls[-1] == DEFAULT else calls[-1])  # Req 2.5
    else:
        assert isinstance(got, LLMCallFailed)
        assert got.permanent == (want_end in ("permanent", "auth"))
        assert got.auth == (want_end == "auth")
        assert got.model == calls[-1]


@given(
    script=st.lists(st.sampled_from(["ok", "transient", "permanent", "auth"]), min_size=8, max_size=8),
    retries=st.integers(0, 3),
    asked=st.sampled_from([None, "small"]),
)
@settings(deadline=None)
def test_the_fallback_is_called_in_one_run_after_the_model_asked_for_and_never_after_an_auth_error(
    script: list[Kind], retries: int, asked: str | None
) -> None:
    calls, _, _ = asyncio.run(actual(script, retries, asked, "backup"))
    first = asked or DEFAULT
    split = next((i for i, m in enumerate(calls) if m != first), len(calls))
    assert all(m == first for m in calls[:split])  # first the model asked for ...
    assert all(m == "backup" for m in calls[split:])  # ... then, at most, one run on the fallback, never back again
    assert len(calls) - split <= retries + 1  # one completion: a call and its transient retries
    if split < len(calls):  # a fallback happened, so the run before it ended in a permanent, non-auth error
        assert script[split - 1] == "permanent"
