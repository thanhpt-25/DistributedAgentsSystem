"""An open-loop run against a stack's client API (das-scale-resilience task 12.2; Phase 7 Req 4.1, 4.3).

Open loop: the time at which each Request is sent is fixed in advance by the profile's schedule, and a Request is
sent then whether or not the earlier ones have finished, so a slow stack is offered the same load as a fast one (a
closed loop would slow down with the server and hide exactly what a load test is for). Each Request is then polled to
a terminal state under a deadline. What is recorded per Request: how long the submit took, how long until the
terminal state, how the Request ended, and how late the send was (a generator that falls behind its own schedule says
so)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

import httpx

from das.clock import Clock, SystemClock
from das.loadtest.profile import Arrival, Profile, arrivals

#: How a Request that reached a terminal state ended, by the status the Gateway reports.
TERMINAL: Final = {"Completed": "completed", "Failed": "failed", "Cancelled": "cancelled"}
COMPLETED: Final = "completed"
DEFAULT_POLL_INTERVAL: Final = 0.5
DEFAULT_REQUEST_TIMEOUT: Final = 120.0
DEFAULT_MAX_IN_FLIGHT: Final = 2000


@dataclass(frozen=True, slots=True)
class Sample:
    """One Request: ``outcome`` is ``completed``, ``failed``, ``cancelled`` or an error kind (``submit_429``,
    ``submit_failed``, ``timeout``, ``poll_404``, ``generator_overloaded``, ``error``)."""

    index: int
    step: int
    outcome: str
    submit_ms: float | None
    total_ms: float | None
    late_ms: float


@dataclass(frozen=True, slots=True)
class RunResult:
    profile: Profile
    samples: tuple[Sample, ...]
    started_at: str
    elapsed_s: float


async def run_profile(
    profile: Profile,
    client: httpx.AsyncClient,
    *,
    clock: Clock | None = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    max_in_flight: int = DEFAULT_MAX_IN_FLIGHT,
    on_sample: Callable[[Sample], None] | None = None,
) -> RunResult:
    """Run ``profile`` through ``client`` (its base URL and API key already set) and return every Request's sample."""
    if poll_interval <= 0 or request_timeout <= 0 or max_in_flight < 1:
        raise ValueError("poll_interval, request_timeout and max_in_flight must be positive")
    clock = clock or SystemClock()
    start = clock.monotonic()
    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    in_flight = 0
    tasks: list[asyncio.Task[Sample]] = []

    async def one(arrival: Arrival, late_ms: float) -> Sample:
        nonlocal in_flight
        try:
            sample = await _request(client, clock, arrival, late_ms, poll_interval, request_timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - one Request's failure is a sample, not the end of the run
            sample = Sample(arrival.index, arrival.step, f"error:{type(exc).__name__}", None, None, late_ms)
        finally:
            in_flight -= 1
        if on_sample is not None:
            on_sample(sample)
        return sample

    try:
        for arrival in arrivals(profile):
            wait = start + arrival.at - clock.monotonic()
            if wait > 0:
                await clock.sleep(wait)
            late_ms = max(0.0, clock.monotonic() - start - arrival.at) * 1000
            if in_flight >= max_in_flight:  # the generator cannot keep this many going: say so, do not slow down
                overloaded = Sample(arrival.index, arrival.step, "generator_overloaded", None, None, late_ms)
                tasks.append(asyncio.get_running_loop().create_task(_done(overloaded)))
                continue
            in_flight += 1
            tasks.append(asyncio.get_running_loop().create_task(one(arrival, late_ms)))
        samples = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return RunResult(profile, tuple(sorted(samples, key=lambda s: s.index)), started_at, clock.monotonic() - start)


async def _done(sample: Sample) -> Sample:
    return sample


async def _request(
    client: httpx.AsyncClient, clock: Clock, arrival: Arrival, late_ms: float, poll_interval: float, timeout: float
) -> Sample:
    begun = clock.monotonic()

    def elapsed_ms() -> float:
        return (clock.monotonic() - begun) * 1000

    def sample(outcome: str, submit: float | None, total: float | None) -> Sample:
        return Sample(arrival.index, arrival.step, outcome, submit, total, late_ms)

    try:
        response = await client.post("/v1/requests", json={"prompt": arrival.prompt})
    except httpx.HTTPError:
        return sample("submit_failed", elapsed_ms(), None)
    submit_ms = elapsed_ms()
    if response.status_code != 202:
        return sample(f"submit_{response.status_code}", submit_ms, None)
    try:
        request_id = response.json()["request_id"]
    except (ValueError, KeyError, TypeError):
        return sample("submit_unreadable", submit_ms, None)

    while clock.monotonic() - begun < timeout:
        await clock.sleep(poll_interval)
        try:
            polled = await client.get(f"/v1/requests/{request_id}")
        except httpx.HTTPError:
            continue  # a poll that cannot be answered is tried again until the deadline
        if polled.status_code == 200:
            try:
                ended = TERMINAL.get(polled.json().get("status", ""))
            except (ValueError, AttributeError):
                continue
            if ended is not None:
                return sample(ended, submit_ms, elapsed_ms())
        elif polled.status_code == 429 or polled.status_code >= 500:
            continue
        else:
            return sample(f"poll_{polled.status_code}", submit_ms, None)
    return sample("timeout", submit_ms, None)


__all__ = [
    "COMPLETED", "DEFAULT_MAX_IN_FLIGHT", "DEFAULT_POLL_INTERVAL", "DEFAULT_REQUEST_TIMEOUT", "RunResult", "Sample",
    "run_profile",
]
