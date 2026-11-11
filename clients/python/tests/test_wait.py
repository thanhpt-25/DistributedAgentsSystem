"""``wait()``: stream to the end, then read the final view; poll when streaming fails (Phase 4 Req 9.6)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from conftest import BASE, KEEPALIVE, FrameStream, call, envelope, snapshot, sse_response
from das_client import AuthError, DasError, NotFound, WaitTimeout

EVENTS = f"{BASE}/v1/requests/r-1/events"
GET = f"{BASE}/v1/requests/r-1"
RESULT = {"text": "the answer"}


def view(status: str, **extra: Any) -> httpx.Response:
    return httpx.Response(200, json={"request_id": "r-1", "status": status, **extra})


def done() -> httpx.Response:
    return view("Completed", final_result=RESULT)


def unavailable(request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, json={"error": "service_unavailable"})


def too_many_streams(request: httpx.Request) -> httpx.Response:
    return httpx.Response(429, json={"error": "too_many_streams"})


def refused(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("refused")


@pytest.fixture
def client(make_client, timing):
    return make_client(monotonic=timing.monotonic)


# --------------------------------------------------------------------------- streaming path


async def test_a_finished_request_returns_the_view_with_its_final_result(client, respx_mock) -> None:
    """The stream's snapshot has no final result, so the view is read once more (Req 9.6)."""
    events = respx_mock.get(EVENTS).mock(return_value=sse_response(snapshot("Completed", "3-0")))
    get = respx_mock.get(GET).mock(return_value=done())
    result = await call(client, "wait", "r-1", timeout=30.0)
    assert (result.status, result.final_result) == ("Completed", RESULT)
    assert (events.call_count, get.call_count) == (1, 1)


async def test_it_waits_for_the_terminal_event_then_reads_the_view(client, respx_mock) -> None:
    response = sse_response(
        snapshot("Running"),
        envelope("job.transition", "1-0"),
        envelope("request.completed", "2-0", {"status": "Completed"}),
    )
    events = respx_mock.get(EVENTS).mock(return_value=response)
    get = respx_mock.get(GET).mock(return_value=done())
    result = await call(client, "wait", "r-1", timeout=30.0)
    assert result.is_terminal and result.final_result == RESULT
    assert (events.call_count, get.call_count) == (1, 1)
    assert response.stream.closed


@pytest.mark.parametrize("status", ["Failed", "Cancelled"])
async def test_a_failed_or_cancelled_request_is_returned_not_raised(client, respx_mock, status) -> None:
    respx_mock.get(EVENTS).mock(return_value=sse_response(snapshot(status, "1-0")))
    respx_mock.get(GET).mock(return_value=view(status, error="budget_exceeded"))
    result = await call(client, "wait", "r-1", timeout=30.0)
    assert (result.status, result.error) == (status, "budget_exceeded")


async def test_a_replay_that_reopened_the_request_is_waited_for_again(client, respx_mock) -> None:
    events = respx_mock.get(EVENTS).mock(
        side_effect=[
            sse_response(snapshot("Running"), envelope("request.failed", "1-0")),
            sse_response(snapshot("Running", "2-0"), envelope("request.completed", "3-0")),
        ]
    )
    get = respx_mock.get(GET).mock(side_effect=[view("Running"), done()])  # the first answer is already reopened
    result = await call(client, "wait", "r-1", timeout=30.0)
    assert result.status == "Completed"
    assert (events.call_count, get.call_count) == (2, 2)


# --------------------------------------------------------------------------- fallback to polling


@pytest.mark.parametrize("stream", [unavailable, too_many_streams, refused], ids=["503", "429", "connection-error"])
async def test_when_streaming_fails_it_polls_every_second(client, respx_mock, timing, stream) -> None:
    respx_mock.get(EVENTS).mock(side_effect=stream)
    get = respx_mock.get(GET).mock(side_effect=[view("Running"), view("Running"), done()])
    result = await call(client, "wait", "r-1", timeout=60.0)
    assert result.final_result == RESULT
    assert get.call_count == 3
    assert timing.sleeps == [0.5, 1.0, 2.0, 1.0, 1.0]  # the stream's own retries, then the polling


async def test_polling_runs_out_at_the_deadline(client, respx_mock, timing) -> None:
    respx_mock.get(EVENTS).mock(side_effect=unavailable)

    def running(request: httpx.Request) -> httpx.Response:
        if len(get.calls) > 20:  # a loop that does not wait would spin here for ever; fail instead of hanging
            raise AssertionError("polling without waiting")
        return view("Running")

    get = respx_mock.get(GET).mock(side_effect=running)
    start = timing.now
    with pytest.raises(WaitTimeout) as caught:
        await call(client, "wait", "r-1", timeout=10.0)
    assert isinstance(caught.value, TimeoutError) and isinstance(caught.value, DasError)
    assert timing.now == start + 10.0  # the last wait was shortened to end exactly at the deadline
    assert timing.sleeps[-1] == 0.5
    assert get.call_count == 8


async def test_a_stream_that_keeps_reconnecting_cannot_outlive_the_deadline(client, respx_mock, timing) -> None:
    """Each connection delivers an event and ends, so the failure count never runs out; the deadline must."""
    get = respx_mock.get(GET).mock(return_value=view("Running"))

    def connection(request: httpx.Request) -> httpx.Response:
        timing.now += 5.0  # time passes while the stream is open
        return sse_response(snapshot("Running"), envelope("job.transition", "1-0"))

    respx_mock.get(EVENTS).mock(side_effect=connection)
    with pytest.raises(WaitTimeout):
        await call(client, "wait", "r-1", timeout=12.0)
    assert get.call_count == 0


async def test_a_stream_that_only_sends_keepalives_cannot_outlive_the_deadline(client, respx_mock, timing) -> None:
    """No event, no error and no end: only the deadline check on received data can stop this one."""
    keepalives = httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        stream=FrameStream([KEEPALIVE.encode()] * 100, on_chunk=lambda: setattr(timing, "now", timing.now + 1.0)),
    )
    get = respx_mock.get(GET).mock(return_value=view("Running"))
    respx_mock.get(EVENTS).mock(return_value=keepalives)
    with pytest.raises(WaitTimeout):
        await call(client, "wait", "r-1", timeout=10.0)
    assert keepalives.stream.consumed == 10  # stopped by the clock, not by running out of data
    assert get.call_count == 0


async def test_the_read_timeout_of_a_waiting_stream_never_exceeds_the_time_left(
    make_client, respx_mock, timing
) -> None:
    client = make_client(monotonic=timing.monotonic, stream_timeout=60.0)
    route = respx_mock.get(EVENTS).mock(return_value=sse_response(snapshot("Completed", "1-0")))
    respx_mock.get(GET).mock(return_value=done())
    await call(client, "wait", "r-1", timeout=20.0)
    assert route.calls.last.request.extensions["timeout"]["read"] == 20.0  # not the default 60


# --------------------------------------------------------------------------- errors that are not worth polling for


@pytest.mark.parametrize(("status", "error"), [(404, NotFound), (401, AuthError)])
async def test_an_error_that_polling_would_not_fix_is_raised(client, respx_mock, status, error) -> None:
    respx_mock.get(EVENTS).mock(return_value=httpx.Response(status, json={"error": "x"}))
    get = respx_mock.get(GET).mock(return_value=done())
    with pytest.raises(error):
        await call(client, "wait", "r-1", timeout=30.0)
    assert get.call_count == 0


async def test_a_failing_get_while_polling_is_raised(client, respx_mock) -> None:
    respx_mock.get(EVENTS).mock(side_effect=unavailable)
    respx_mock.get(GET).mock(return_value=httpx.Response(404, json={"error": "not_found"}))
    with pytest.raises(NotFound):
        await call(client, "wait", "r-1", timeout=30.0)
