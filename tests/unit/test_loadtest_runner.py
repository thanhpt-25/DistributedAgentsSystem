"""The open-loop runner (das-scale-resilience task 12.2; Phase 7 Req 4.1, 4.3): against a small ASGI app that behaves
like the client API, with real (short) time, and against the real Gateway app in the same process."""

from __future__ import annotations

import asyncio
import itertools
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from das.config.schema import ClientLimitsConfig, ClientSettingsConfig
from das.gateway.app import create_app
from das.loadtest.profile import Profile, Step
from das.loadtest.runner import COMPLETED, RunResult, Sample, run_profile
from das.registry import AgentRegistry

FAST = {"poll_interval": 0.01, "request_timeout": 5.0}
KEY = "load-key-0000000001"


class FakeGateway:
    """The two client API calls a load test makes, with a script for how each Request behaves."""

    def __init__(
        self, polls_until_done: int = 2, submit_delay: float = 0.0, submit_status: int = 202, final: str = "Completed"
    ) -> None:
        self.polls_until_done, self.submit_delay = polls_until_done, submit_delay
        self.submit_status, self.final = submit_status, final
        self.polls: dict[str, int] = {}
        self.submitted: list[str] = []
        self.poll_status: int | None = None
        self.poll_failures_left = 0
        self.counter = itertools.count(1)

    async def submit(self, request: Request) -> JSONResponse:
        if self.submit_delay:
            await asyncio.sleep(self.submit_delay)
        if self.submit_status != 202:
            return JSONResponse({"error": "no"}, status_code=self.submit_status)
        self.submitted.append((await request.json())["prompt"])
        rid = f"r{next(self.counter)}"
        self.polls[rid] = 0
        return JSONResponse({"request_id": rid}, status_code=202)

    async def status(self, request: Request) -> JSONResponse:
        if self.poll_failures_left > 0:
            self.poll_failures_left -= 1
            return JSONResponse({}, status_code=503)
        if self.poll_status is not None:
            return JSONResponse({}, status_code=self.poll_status)
        rid = request.path_params["rid"]
        self.polls[rid] += 1
        done = self.polls[rid] >= self.polls_until_done
        return JSONResponse({"status": self.final if done else "Running"})

    def client(self) -> httpx.AsyncClient:
        app = Starlette(routes=[
            Route("/v1/requests", self.submit, methods=["POST"]),
            Route("/v1/requests/{rid}", self.status, methods=["GET"]),
        ])
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


async def run(gateway: FakeGateway, profile: Profile, **kwargs: Any) -> RunResult:
    async with gateway.client() as client:
        return await run_profile(profile, client, **{**FAST, **kwargs})


HALF_SECOND = Profile("p", (Step(20, 0.5),), ("one", "two"))  # 10 Requests, 50 ms apart


# --------------------------------------------------------------------------- what is measured


async def test_every_request_is_submitted_polled_and_measured() -> None:
    gateway = FakeGateway()
    result = await run(gateway, HALF_SECOND)
    assert len(result.samples) == 10 and {s.outcome for s in result.samples} == {COMPLETED}
    assert [s.index for s in result.samples] == list(range(10)) and {s.step for s in result.samples} == {0}
    for s in result.samples:
        assert s.submit_ms is not None and s.total_ms is not None and 0 <= s.submit_ms <= s.total_ms
        assert s.total_ms >= 10, "at least one poll interval of 10 ms passed before the terminal state was seen"
    assert gateway.submitted == ["one", "two"] * 5, "the prompt mix is used in turn"
    assert result.elapsed_s >= 0.45 and result.started_at.endswith("+00:00")


async def test_the_total_time_includes_the_polls_until_the_terminal_state() -> None:
    result = await run(FakeGateway(polls_until_done=6), Profile("p", (Step(2, 1),), ("x",)))
    assert all(s.total_ms >= 6 * 10 for s in result.samples)


async def test_samples_carry_the_step_they_belong_to() -> None:
    result = await run(FakeGateway(), Profile("p", (Step(10, 0.3), Step(20, 0.2)), ("x",)))
    assert [s.step for s in result.samples] == [0, 0, 0, 1, 1, 1, 1]


async def test_each_sample_is_reported_as_it_finishes() -> None:
    seen: list[Sample] = []
    await run(FakeGateway(), HALF_SECOND, on_sample=seen.append)
    assert len(seen) == 10


# --------------------------------------------------------------------------- open loop (Req 4.1)


async def test_a_slow_submit_does_not_slow_the_offered_load() -> None:
    """Each submit takes 300 ms and Requests are due every 50 ms: a closed loop takes 3 s, an open one about 0.8."""
    started = time.monotonic()
    result = await run(FakeGateway(submit_delay=0.3), HALF_SECOND)
    assert time.monotonic() - started < 1.6
    assert max(s.late_ms for s in result.samples) < 150, "every send went out on its own schedule"
    assert all(s.submit_ms is not None and s.submit_ms >= 290 for s in result.samples)


async def test_a_generator_that_falls_behind_says_so_in_the_lateness() -> None:
    class Stall(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            time.sleep(0.12)  # blocks the event loop: the generator itself cannot keep up
            if request.method == "POST":
                return httpx.Response(202, json={"request_id": "r"})
            return httpx.Response(200, json={"status": "Completed"})

    async with httpx.AsyncClient(transport=Stall(), base_url="http://gw") as client:
        result = await run_profile(Profile("p", (Step(50, 0.2),), ("x",)), client, **FAST)
    assert max(s.late_ms for s in result.samples) > 50


# --------------------------------------------------------------------------- how a Request can end


async def test_a_request_the_stack_failed_is_an_error_not_a_success() -> None:
    result = await run(FakeGateway(final="Failed"), HALF_SECOND)
    assert {s.outcome for s in result.samples} == {"failed"} and all(s.total_ms is not None for s in result.samples)


async def test_a_cancelled_request_is_its_own_outcome() -> None:
    assert {s.outcome for s in (await run(FakeGateway(final="Cancelled"), HALF_SECOND)).samples} == {"cancelled"}


@pytest.mark.parametrize("status", [429, 401, 500, 503])
async def test_a_refused_submit_is_an_error_named_by_its_status_with_the_time_it_took(status: int) -> None:
    result = await run(FakeGateway(submit_status=status), HALF_SECOND)
    assert {s.outcome for s in result.samples} == {f"submit_{status}"}
    assert all(s.submit_ms is not None and s.total_ms is None for s in result.samples)


async def test_a_gateway_that_cannot_be_reached_is_submit_failed() -> None:
    class Down(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

    async with httpx.AsyncClient(transport=Down(), base_url="http://gw") as client:
        result = await run_profile(HALF_SECOND, client, **FAST)
    assert {s.outcome for s in result.samples} == {"submit_failed"}


async def test_a_request_that_never_finishes_times_out_at_the_deadline() -> None:
    result = await run(FakeGateway(polls_until_done=10**9), Profile("p", (Step(4, 0.5),), ("x",)), request_timeout=0.3)
    assert {s.outcome for s in result.samples} == {"timeout"}
    assert all(s.submit_ms is not None and s.total_ms is None for s in result.samples)


async def test_polls_that_fail_for_a_moment_are_tried_again() -> None:
    gateway = FakeGateway()
    gateway.poll_failures_left = 3
    assert {s.outcome for s in (await run(gateway, Profile("p", (Step(2, 0.5),), ("x",)))).samples} == {COMPLETED}


async def test_a_request_the_gateway_has_forgotten_is_an_error_at_once() -> None:
    gateway = FakeGateway()
    gateway.poll_status = 404
    result = await run(gateway, Profile("p", (Step(2, 0.5),), ("x",)))
    assert {s.outcome for s in result.samples} == {"poll_404"}


async def test_a_submit_answer_that_is_not_a_request_id_is_unreadable() -> None:
    class Odd(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(202, content=b"not json")

    async with httpx.AsyncClient(transport=Odd(), base_url="http://gw") as client:
        result = await run_profile(Profile("p", (Step(2, 0.5),), ("x",)), client, **FAST)
    assert {s.outcome for s in result.samples} == {"submit_unreadable"}


async def test_more_requests_in_flight_than_the_generator_allows_are_counted_not_slowed() -> None:
    result = await run(FakeGateway(polls_until_done=10**9), HALF_SECOND, max_in_flight=2, request_timeout=0.4)
    outcomes = [s.outcome for s in result.samples]
    assert outcomes.count("generator_overloaded") >= 6 and "timeout" in outcomes


@pytest.mark.parametrize("bad", [{"poll_interval": 0}, {"request_timeout": 0}, {"max_in_flight": 0}])
async def test_nonsense_settings_are_refused(bad: dict[str, float]) -> None:
    async with FakeGateway().client() as client:
        with pytest.raises(ValueError, match="must be positive"):
            await run_profile(HALF_SECOND, client, **bad)


async def test_cancelling_the_run_cancels_every_request_in_flight() -> None:
    gateway = FakeGateway(polls_until_done=10**9)
    async with gateway.client() as client:
        task = asyncio.create_task(run_profile(Profile("p", (Step(20, 2),), ("x",)), client, **FAST))
        await asyncio.sleep(0.3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.05)
        left = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert not [t for t in left if "_request" in repr(t) or "one" in repr(t.get_coro())]


# --------------------------------------------------------------------------- the real Gateway app, in the same process


@pytest.fixture
def gateway_client(state_store, fake_redis, fake_redis_time) -> Callable[[float], httpx.AsyncClient]:
    def make(rate: float) -> httpx.AsyncClient:
        limits = ClientLimitsConfig(default=ClientSettingsConfig(rate=rate, burst=1, max_active_requests=0))
        app = create_app(state_store, AgentRegistry(fake_redis), None, [KEY], 50, client_limits=limits)
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw", headers={"X-API-Key": KEY}
        )

    return make


async def test_against_the_real_gateway_requests_are_accepted_and_wait_for_a_manager(
    gateway_client: Callable[[float], httpx.AsyncClient],
) -> None:
    """Nothing runs the Requests here, so each is accepted (202, with a submit time) and times out: the API contract the
    runner depends on (POST /v1/requests answers 202 with request_id, GET shows a non-terminal status) holds."""
    async with gateway_client(0.0) as client:
        result = await run_profile(
            Profile("p", (Step(10, 0.5),), ("hello",)), client, poll_interval=0.02, request_timeout=0.3
        )
    assert {s.outcome for s in result.samples} == {"timeout"} and len(result.samples) == 5
    assert all(s.submit_ms is not None and s.submit_ms < 500 for s in result.samples)


async def test_against_the_real_gateway_a_rate_limit_shows_up_as_submit_429(
    gateway_client: Callable[[float], httpx.AsyncClient],
) -> None:
    async with gateway_client(1.0) as limited:  # one request a second, burst of one
        result = await run_profile(
            Profile("p", (Step(20, 0.5),), ("hello",)), limited, poll_interval=0.02, request_timeout=0.2
        )
    outcomes = [s.outcome for s in result.samples]
    assert outcomes.count("submit_429") >= 8 and "timeout" in outcomes, outcomes
