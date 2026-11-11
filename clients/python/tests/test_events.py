"""``events()``: typed events, reconnect with Last-Event-ID, stop after a terminal state (Phase 4 Req 9.6)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from conftest import API_KEY, BASE, KEEPALIVE, drain, envelope, frame, snapshot, sse_response, take
from das_client import (
    AuthError,
    DasError,
    InvalidRequest,
    NotFound,
    RateLimited,
    ServiceUnavailable,
    TransportFailure,
)

URL = f"{BASE}/v1/requests/r-1/events"


def types(events: list[Any]) -> list[str]:
    return [e.type for e in events]


def sent_ids(route: Any) -> list[str | None]:
    return [call.request.headers.get("last-event-id") for call in route.calls]


def finished(event_id: str = "1-0") -> httpx.Response:
    return sse_response(snapshot("Running"), envelope("request.failed", event_id))


# --------------------------------------------------------------------------- a normal stream


async def test_the_stream_yields_the_snapshot_then_events_and_stops_at_the_terminal_event(
    make_client, respx_mock
) -> None:
    response = sse_response(
        snapshot("Running"),
        envelope("job.transition", "1-0", {"job_id": "a"}),
        envelope("request.completed", "2-0", {"status": "Completed"}),
        envelope("request.cancelled", "3-0"),  # never reached
    )
    route = respx_mock.get(URL).mock(return_value=response)
    events, error = await drain(make_client())
    assert error is None
    assert types(events) == ["snapshot", "job.transition", "request.completed"]
    assert route.call_count == 1
    assert response.stream.consumed == 3  # it did not read past the terminal event
    assert response.stream.closed


async def test_events_are_typed_and_carry_their_envelope_fields(make_client, respx_mock) -> None:
    respx_mock.get(URL).mock(
        return_value=sse_response(
            snapshot("Running", "1-0"),
            envelope("job.transition", "2-0", {"job_id": "a", "to": "Running"}),
            envelope("request.failed", "3-0", {"status": "Failed", "error": "budget_exceeded"}),
        )
    )
    events, _ = await drain(make_client())
    snap, move, last = events
    assert (snap.type, snap.event_id, snap.request_id, snap.data["status"]) == ("snapshot", "1-0", "r-1", "Running")
    assert (move.type, move.event_id, move.data) == ("job.transition", "2-0", {"job_id": "a", "to": "Running"})
    assert last.is_terminal and last.data["error"] == "budget_exceeded"
    assert not snap.is_terminal and not move.is_terminal


async def test_the_request_line_and_headers(make_client, respx_mock) -> None:
    route = respx_mock.get(URL).mock(return_value=finished())
    await drain(make_client())
    request = route.calls.last.request
    assert request.headers["x-api-key"] == API_KEY
    assert request.headers["accept"] == "text/event-stream"
    assert "last-event-id" not in request.headers  # nothing to resume from


async def test_a_given_last_event_id_is_sent(make_client, respx_mock) -> None:
    route = respx_mock.get(URL).mock(return_value=finished("8-0"))
    await drain(make_client(), last_event_id="7-0")
    assert sent_ids(route) == ["7-0"]


async def test_keepalives_unknown_types_and_unusable_frames_are_handled(make_client, respx_mock) -> None:
    respx_mock.get(URL).mock(
        return_value=sse_response(
            snapshot("Running"),
            KEEPALIVE,
            "event: junk\ndata: not json\n\n",
            frame("job.transition", ["not", "an", "envelope"], "1-0"),
            envelope("request.something_new", "2-0", {"x": 1}),
            envelope("request.completed", "3-0"),
        )
    )
    events, error = await drain(make_client())
    assert error is None
    assert types(events) == ["snapshot", "request.something_new", "request.completed"]


@pytest.mark.parametrize("request_id", ["a/b", ".."])
async def test_a_request_id_cannot_change_the_events_path(make_client, respx_mock, request_id) -> None:
    route = respx_mock.route(method="GET", url__startswith=f"{BASE}/v1/requests/").mock(
        return_value=sse_response(snapshot("Failed"))
    )
    await drain(make_client(), request_id)
    raw_path = route.calls.last.request.url.raw_path
    assert raw_path.endswith(b"/events") and raw_path.count(b"/") == 4


async def test_the_read_timeout_is_the_stream_timeout(make_client, respx_mock) -> None:
    route = respx_mock.get(URL).mock(return_value=sse_response(snapshot("Failed")))
    await drain(make_client(stream_timeout=5.0, timeout=11.0))
    timeout = route.calls.last.request.extensions["timeout"]
    assert (timeout["read"], timeout["connect"]) == (5.0, 11.0)


async def test_an_early_stop_by_the_consumer_closes_the_response(make_client, respx_mock) -> None:
    response = sse_response(snapshot("Running"), envelope("job.transition", "1-0"), envelope("request.failed", "2-0"))
    respx_mock.get(URL).mock(return_value=response)
    events = await take(make_client(), 1)
    assert types(events) == ["snapshot"]
    assert response.stream.closed
    assert response.stream.consumed == 1


# --------------------------------------------------------------------------- when a stream is over


async def test_a_terminal_snapshot_followed_by_end_of_stream_ends_the_iteration(
    make_client, respx_mock, timing
) -> None:
    """A Request that was already finished: the server sends the snapshot and closes (Req 7.6)."""
    route = respx_mock.get(URL).mock(return_value=sse_response(snapshot("Completed", "7-0")))
    events, error = await drain(make_client())
    assert (error, types(events), route.call_count, timing.sleeps) == (None, ["snapshot"], 1, [])


async def test_a_terminal_snapshot_still_delivers_the_events_that_follow_it(make_client, respx_mock) -> None:
    route = respx_mock.get(URL).mock(
        return_value=sse_response(
            snapshot("Failed", "7-0"), envelope("job.transition", "6-0"), envelope("request.failed", "7-0")
        )
    )
    events, error = await drain(make_client(), last_event_id="5-0")
    assert (error, types(events), sent_ids(route)) == (None, ["snapshot", "job.transition", "request.failed"], ["5-0"])


@pytest.mark.parametrize("status", ["Completed", "Failed", "Cancelled"])
async def test_every_terminal_status_ends_the_iteration(make_client, respx_mock, status) -> None:
    respx_mock.get(URL).mock(return_value=sse_response(snapshot(status, "1-0")))
    events, error = await drain(make_client())
    assert error is None and types(events) == ["snapshot"]


async def test_end_of_stream_on_a_running_request_reconnects(make_client, respx_mock, timing) -> None:
    responses = [
        sse_response(snapshot("Running", "1-0"), envelope("job.transition", "2-0")),
        sse_response(snapshot("Running", "2-0"), envelope("request.completed", "3-0")),
    ]
    route = respx_mock.get(URL).mock(side_effect=responses)
    events, error = await drain(make_client())
    assert error is None
    assert types(events) == ["snapshot", "job.transition", "request.completed"]
    assert sent_ids(route) == [None, "2-0"]
    assert timing.sleeps == [0.5]


# --------------------------------------------------------------------------- reconnecting


@pytest.mark.parametrize(
    "failure", [httpx.ReadError("reset"), httpx.ReadTimeout("idle"), httpx.RemoteProtocolError("cut off")]
)
async def test_a_broken_stream_resumes_from_the_last_event_id(make_client, respx_mock, timing, failure) -> None:
    route = respx_mock.get(URL).mock(
        side_effect=[
            sse_response(
                snapshot("Running"),
                envelope("job.transition", "1-0"),
                envelope("job.transition", "2-0"),
                error=failure,
            ),
            sse_response(snapshot("Running", "2-0"), envelope("request.completed", "3-0")),
        ]
    )
    events, error = await drain(make_client())
    assert error is None
    assert types(events) == ["snapshot", "job.transition", "job.transition", "request.completed"]  # snapshot once
    assert sent_ids(route) == [None, "2-0"]
    assert timing.sleeps == [0.5]


async def test_a_snapshot_flagged_resync_is_yielded_again_and_moves_the_position(
    make_client, respx_mock, timing
) -> None:
    route = respx_mock.get(URL).mock(
        side_effect=[
            sse_response(snapshot("Running"), envelope("job.transition", "1-0"), error=httpx.ReadError("x")),
            sse_response(snapshot("Running", "9-0", resync=True), error=httpx.ReadError("y")),
            sse_response(snapshot("Running", "9-0"), envelope("request.completed", "10-0")),
        ]
    )
    events, error = await drain(make_client())
    assert error is None
    assert types(events) == ["snapshot", "job.transition", "snapshot", "request.completed"]
    assert events[2].data["resync"] is True
    assert sent_ids(route) == [None, "1-0", "9-0"]  # after a resync the position is the snapshot's
    assert timing.sleeps == [0.5, 1.0]  # the resync snapshot is not progress


async def test_a_plain_reconnect_does_not_move_the_position_to_the_snapshot_id(make_client, respx_mock) -> None:
    """Events between our position and the snapshot id are still on their way; skipping ahead would lose them."""
    route = respx_mock.get(URL).mock(
        side_effect=[
            sse_response(snapshot("Running"), envelope("job.transition", "1-0"), error=httpx.ReadError("x")),
            sse_response(snapshot("Running", "5-0"), error=httpx.ReadError("y")),  # replay cut before 2-0
            sse_response(snapshot("Running", "5-0"), envelope("request.completed", "6-0")),
        ]
    )
    await drain(make_client())
    assert sent_ids(route) == [None, "1-0", "1-0"]


async def test_without_a_position_the_snapshot_of_every_connection_is_yielded(make_client, respx_mock) -> None:
    """No Event_ID yet: the next snapshot is the only record of what happened while we were away."""
    route = respx_mock.get(URL).mock(
        side_effect=[
            sse_response(snapshot("Running"), error=httpx.ReadError("x")),
            sse_response(snapshot("Running", "5-0"), envelope("request.completed", "6-0")),
        ]
    )
    events, error = await drain(make_client())
    assert error is None and types(events) == ["snapshot", "snapshot", "request.completed"]
    assert sent_ids(route) == [None, None]


async def test_a_given_last_event_id_is_kept_across_a_reconnect_with_no_new_events(make_client, respx_mock) -> None:
    route = respx_mock.get(URL).mock(
        side_effect=[
            sse_response(snapshot("Running", "9-0"), error=httpx.ReadError("x")),
            sse_response(snapshot("Running", "9-0"), envelope("request.completed", "10-0")),
        ]
    )
    events, _ = await drain(make_client(), last_event_id="4-0")
    assert sent_ids(route) == ["4-0", "4-0"]
    assert types(events) == ["snapshot", "request.completed"]  # the second snapshot is swallowed


async def test_any_event_resets_the_count_of_failures_in_a_row(make_client, respx_mock, timing) -> None:
    calls = {"n": 0}

    def next_connection(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] > 6:
            return sse_response(snapshot("Running", "6-0"), envelope("request.completed", "7-0"))
        return sse_response(snapshot("Running"), envelope("job.transition", f"{calls['n']}-0"))  # then it just ends

    respx_mock.get(URL).mock(side_effect=next_connection)
    events, error = await drain(make_client())  # 6 reconnects, more than max_retries=3, but each made progress
    assert error is None and events[-1].type == "request.completed"
    assert timing.sleeps == [0.5] * 6


async def test_failures_with_no_event_in_between_run_out_of_retries(make_client, respx_mock, timing) -> None:
    def only_a_snapshot(request: httpx.Request) -> httpx.Response:
        if len(route.calls) > 10:  # a client that never gives up must fail this test, not hang it
            raise AssertionError("the client kept reconnecting")
        return sse_response(snapshot("Running"))

    route = respx_mock.get(URL).mock(side_effect=only_a_snapshot)
    events, error = await drain(make_client())
    assert isinstance(error, TransportFailure) and not isinstance(error, ServiceUnavailable)
    assert route.call_count == 4  # the first connection and 3 retries
    assert timing.sleeps == [0.5, 1.0, 2.0]
    assert set(types(events)) == {"snapshot"}


# --------------------------------------------------------------------------- errors on connect


async def test_503_on_connect_is_retried(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(
        side_effect=[
            httpx.Response(503, json={"error": "service_unavailable"}),
            sse_response(snapshot("Running"), envelope("request.completed", "1-0")),
        ]
    )
    events, error = await drain(make_client())
    assert (error, route.call_count, timing.sleeps) == (None, 2, [0.5])
    assert types(events) == ["snapshot", "request.completed"]


async def test_429_on_connect_waits_at_least_the_retry_after(make_client, respx_mock, timing) -> None:
    respx_mock.get(URL).mock(
        side_effect=[
            httpx.Response(429, json={"error": "too_many_streams"}, headers={"Retry-After": "5"}),
            sse_response(snapshot("Failed")),
        ]
    )
    events, error = await drain(make_client())
    assert error is None and timing.sleeps == [5.0]


async def test_the_503_that_never_clears_is_service_unavailable(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(return_value=httpx.Response(503, json={"error": "too_many_streams"}))
    events, error = await drain(make_client())
    assert isinstance(error, ServiceUnavailable) and events == []
    assert (route.call_count, timing.sleeps) == (4, [0.5, 1.0, 2.0])


async def test_the_429_that_never_clears_is_rate_limited_and_keeps_its_code(make_client, respx_mock) -> None:
    respx_mock.get(URL).mock(return_value=httpx.Response(429, json={"error": "too_many_streams"}))
    _, error = await drain(make_client())
    assert isinstance(error, RateLimited) and error.code == "too_many_streams"


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (401, {"error": "unauthorized"}, AuthError),
        (404, {"error": "not_found"}, NotFound),
        (400, {"error": "invalid_request"}, InvalidRequest),
        (500, {"error": "boom"}, DasError),
    ],
)
async def test_other_statuses_on_connect_raise_at_once(make_client, respx_mock, timing, status, body, expected) -> None:
    route = respx_mock.get(URL).mock(return_value=httpx.Response(status, json=body))
    events, error = await drain(make_client())
    assert type(error) is expected and events == []
    assert (route.call_count, timing.sleeps) == (1, [])


async def test_a_connection_error_on_connect_is_retried_then_typed(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(side_effect=httpx.ConnectError("refused"))
    events, error = await drain(make_client())
    assert isinstance(error, TransportFailure) and isinstance(error.__cause__, httpx.ConnectError)
    assert (route.call_count, len(timing.sleeps)) == (4, 3)


async def test_a_reply_that_is_not_an_event_stream_is_an_error_not_a_retry(make_client, respx_mock, timing) -> None:
    route = respx_mock.get(URL).mock(return_value=sse_response("<html>login</html>", content_type="text/html"))
    _, error = await drain(make_client())
    assert type(error) is DasError and "event stream" in str(error)
    assert (route.call_count, timing.sleeps) == (1, [])


async def test_a_frame_that_is_far_too_large_is_an_error(make_client, respx_mock, timing) -> None:
    respx_mock.get(URL).mock(return_value=sse_response(b"data: " + b"x" * (2 * 1024 * 1024)))
    _, error = await drain(make_client())
    assert type(error) is DasError and "too large" in str(error)
    assert timing.sleeps == []
