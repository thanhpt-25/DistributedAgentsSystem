"""The retry a Redis client makes through a Failover (das-scale-resilience task 8.3; Phase 7 Req 1.5, 1.8): a connection
error, a timeout or a ``READONLY`` is tried again up to three times after 0.2, 0.5 and 1 second, each retry is counted
by reason, and nothing else is retried."""

from __future__ import annotations

from typing import Any

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from das.state.redisclient import FAILOVER_BACKOFF, FailoverRetry


async def _noop(seconds: float = 0) -> None:
    return None


class Run:
    """One ``call_with_retry`` with scripted outcomes, recording the waits, the failure hooks and the retries."""

    def __init__(self, *outcomes: object, retryable: Any = None, counted: bool = True) -> None:
        self.outcomes = list(outcomes)
        self.sleeps: list[float] = []
        self.failed: list[BaseException] = []
        self.counts: list[str] = []
        self.calls = 0
        self.retryable = retryable
        self.retry = FailoverRetry(on_retry=self.counts.append if counted else None, sleep=self._sleep)

    async def _sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    async def _do(self) -> Any:
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def _fail(self, error: BaseException, *failures: int) -> None:
        self.failed.append(error)

    async def go(self, *, with_failure_count: bool = False) -> Any:
        return await self.retry.call_with_retry(
            self._do, self._fail, is_retryable=self.retryable, with_failure_count=with_failure_count
        )


CONN = RedisConnectionError("reset")
READONLY = ReadOnlyError("You can't write against a read only replica")
SLOW = RedisTimeoutError("timed out")


# --------------------------------------------------------------------------- the schedule


async def test_a_call_that_works_is_not_retried_or_slept_for() -> None:
    run = Run("ok")
    assert await run.go() == "ok" and run.calls == 1 and run.sleeps == [] and run.counts == []


async def test_a_call_that_fails_three_times_then_works_waits_0_2_then_0_5_then_1() -> None:
    run = Run(CONN, CONN, CONN, "ok")
    assert await run.go() == "ok"
    assert run.calls == 4 and run.sleeps == [0.2, 0.5, 1.0] == list(FAILOVER_BACKOFF)


async def test_a_call_that_keeps_failing_is_tried_four_times_and_the_last_error_is_raised() -> None:
    last = RedisConnectionError("still down")
    run = Run(CONN, CONN, CONN, last)
    with pytest.raises(RedisConnectionError) as info:
        await run.go()
    assert info.value is last and run.calls == 4 and run.sleeps == [0.2, 0.5, 1.0]


async def test_the_failure_hook_runs_for_every_failure_including_the_last() -> None:
    """redis-py's hook is what disconnects the broken connection, so the next try opens a new one."""
    run = Run(CONN, CONN, CONN, CONN)
    with pytest.raises(RedisConnectionError):
        await run.go()
    assert len(run.failed) == 4


async def test_the_failure_count_is_passed_when_asked_for() -> None:
    counts: list[int] = []

    async def fail(error: BaseException, failures: int) -> None:
        counts.append(failures)

    outcomes: list[object] = [CONN, CONN, "ok"]

    async def do() -> Any:
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    retry = FailoverRetry(sleep=_noop)
    assert await retry.call_with_retry(do, fail, with_failure_count=True) == "ok"
    assert counts == [1, 2]


# --------------------------------------------------------------------------- what is retried (Req 1.5)


@pytest.mark.parametrize("error", [CONN, READONLY, SLOW])
async def test_a_connection_error_a_readonly_and_a_timeout_are_retried(error: BaseException) -> None:
    run = Run(error, "ok")
    assert await run.go() == "ok" and run.calls == 2


@pytest.mark.parametrize("error", [ResponseError("WRONGTYPE"), ValueError("x"), KeyError("k"), RuntimeError("r")])
async def test_any_other_error_is_raised_at_once_without_a_wait(error: BaseException) -> None:
    run = Run(error, "never")
    with pytest.raises(type(error)):
        await run.go()
    assert run.calls == 1 and run.sleeps == [] and run.counts == [] and run.failed == []


async def test_an_error_the_caller_says_is_not_retryable_is_raised_at_once() -> None:
    run = Run(CONN, "never", retryable=lambda error: False)
    with pytest.raises(RedisConnectionError):
        await run.go()
    assert run.calls == 1 and run.sleeps == []


async def test_a_retryable_filter_that_says_yes_retries() -> None:
    run = Run(CONN, "ok", retryable=lambda error: True)
    assert await run.go() == "ok"


# --------------------------------------------------------------------------- counting by reason (Req 1.8)


async def test_connection_errors_and_timeouts_count_as_connection_and_readonly_as_readonly() -> None:
    run = Run(CONN, SLOW, READONLY, "ok")
    await run.go()
    assert run.counts == ["connection", "connection", "readonly"]


async def test_only_a_retry_that_will_happen_is_counted() -> None:
    run = Run(CONN, CONN, CONN, CONN)  # the fourth failure is final: no retry follows it
    with pytest.raises(RedisConnectionError):
        await run.go()
    assert run.counts == ["connection"] * 3


async def test_a_counter_that_raises_never_breaks_the_retry() -> None:
    def broken(reason: str) -> None:
        raise RuntimeError("boom")

    retry = FailoverRetry(on_retry=broken, sleep=_noop)
    outcomes: list[object] = [CONN, "ok"]

    async def do() -> Any:
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def fail(error: BaseException) -> None:
        return None

    assert await retry.call_with_retry(do, fail) == "ok"


async def test_a_retry_without_a_counter_works() -> None:
    assert await Run(CONN, "ok", counted=False).go() == "ok"


# --------------------------------------------------------------------------- the shape redis-py expects


def test_it_is_a_redis_py_retry_that_supports_exactly_the_three_errors() -> None:
    from redis.asyncio.retry import Retry

    retry = FailoverRetry()
    assert isinstance(retry, Retry)
    assert set(retry._supported_errors) == {RedisConnectionError, RedisTimeoutError, ReadOnlyError}
    assert retry._retries == 3


def test_it_can_be_compared_and_hashed_like_the_retry_it_replaces() -> None:
    """redis-py keeps connection kwargs in sets and compares them; a Retry subclass must not break that."""
    retry = FailoverRetry()
    assert retry == retry and hash(retry) == hash(retry)


def test_a_deep_copy_is_the_same_retry_so_a_copy_cannot_count_into_a_copy_of_the_observer() -> None:
    """redis-py deep-copies the Retry it is given; a copy would hold a copy of the metrics sink and lose every count."""
    import copy

    counts: list[str] = []
    retry = FailoverRetry(on_retry=counts.append)
    assert copy.deepcopy(retry) is retry
    assert copy.deepcopy({"retry": retry})["retry"] is retry
