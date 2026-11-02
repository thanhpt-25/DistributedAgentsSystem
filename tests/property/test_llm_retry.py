# Feature: distributed-agent-system, Property 30: LLM retry policy
"""Property 30: LLM retry policy.

For any ``retry_count`` in 0..10 and any sequence of adapter outcomes
(success, transient error, permanent error):

- ``LLMProvider.complete`` stops on the first success or permanent error, or
  after ``retry_count + 1`` calls.
- The waits between calls are ``min(2^(k-1), 30)`` seconds for k = 1, 2, ...
- Permanent errors cause no retry.
- Failures surface as an error carrying provider, model, reason, and attempt
  count, with no content returned.

Timeouts are modelled by the adapter raising ``TimeoutError`` (the same
exception ``asyncio.timeout`` raises), so no example waits on a real clock.

**Validates: Requirements 10.4, 10.5, 10.6**
"""
from __future__ import annotations

import asyncio

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from das.llm.base import ChatMessage, ModelParams
from das.llm.errors import LLMCallFailed, PermanentLLMError, TransientLLMError
from das.llm.retry import LLMProvider
from tests.fakes.llm_adapter import FakeAdapter

MODEL = "fake-model-1"
MESSAGES = (ChatMessage(role="user", content="hello"),)

# Each outcome kind: ("ok", text) | ("transient", reason) | ("timeout", "") | ("permanent", reason)
_reason = st.text(alphabet="abcdefghijklmnopqrstuvwxyz ", min_size=1, max_size=20).map(
    lambda s: "r-" + s
)
_outcome = st.one_of(
    st.tuples(st.just("ok"), st.text(max_size=20)),
    st.tuples(st.just("transient"), _reason),
    st.tuples(st.just("timeout"), st.just("")),
    st.tuples(st.just("permanent"), _reason),
)


def _materialize(kind: str, value: str):
    if kind == "ok":
        return value
    if kind == "transient":
        return TransientLLMError(value)
    if kind == "timeout":
        return TimeoutError()
    return PermanentLLMError(value)


def _expected_delay(k: int) -> float:
    return float(min(2 ** (k - 1), 30))


@settings(max_examples=200, deadline=None)
@given(
    retry_count=st.integers(min_value=0, max_value=10),
    # Script long enough to cover retry_count + 1 calls; a trailing transient
    # default keeps the adapter from running dry.
    script=st.lists(_outcome, min_size=0, max_size=12),
)
def test_llm_retry_policy(retry_count: int, script: list[tuple[str, str]]) -> None:
    delays: list[float] = []

    async def record_sleep(seconds: float) -> None:
        delays.append(seconds)

    adapter = FakeAdapter(
        [_materialize(k, v) for k, v in script],
        default=TransientLLMError("default-transient"),
    )
    provider = LLMProvider(
        adapter,
        ModelParams(model=MODEL, temperature=0.5, max_output_tokens=64),
        retry_count=retry_count,
        timeout=60,
        sleep=record_sleep,
    )

    # Model of the expected behaviour.
    padded = list(script) + [("transient", "default-transient")] * (retry_count + 1)
    max_calls = retry_count + 1
    expected_calls = max_calls
    stop_kind, stop_value = padded[max_calls - 1]
    for i, (kind, value) in enumerate(padded[:max_calls]):
        if kind in ("ok", "permanent"):
            expected_calls = i + 1
            stop_kind, stop_value = kind, value
            break

    async def run():
        return await provider.complete("sys", MESSAGES, operation="job")

    if stop_kind == "ok":
        result = asyncio.run(run())
        assert result.content == stop_value
    else:
        with pytest.raises(LLMCallFailed) as info:
            asyncio.run(run())
        err = info.value
        assert err.provider == adapter.provider
        assert err.model == MODEL
        assert err.attempts == expected_calls
        assert err.reason
        if stop_kind == "permanent":
            assert err.reason == stop_value
        elif stop_kind == "transient":
            assert err.reason == stop_value
        else:
            assert "timeout" in err.reason
        # No partial content is carried on the error.
        assert not hasattr(err, "content")

    # Stops at first success / permanent error, or after retry_count + 1 calls.
    assert len(adapter.calls) == expected_calls
    # Waits between calls follow min(2^(k-1), 30); permanent errors and
    # successes add no further wait.
    assert delays == [_expected_delay(k) for k in range(1, expected_calls)]
