"""The ``text/event-stream`` parser and the frame-to-Event conversion (Phase 4 Req 9.6)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from das_client import DasError, Event
from das_client._sse import Frame, SseParser, event_from_frame

SAMPLE = (
    'event: a\nid: 1-0\ndata: {"x":1}\n\n'
    ": keepalive\n\n"
    "event: b\ndata: line one\ndata: line two\n\n"
    "data: no event name\n\n"
)
SAMPLE_FRAMES = [
    Frame("a", '{"x":1}', "1-0"),
    Frame("b", "line one\nline two", None),
    Frame("message", "no event name", None),
]


def parse(*chunks: bytes) -> list[Frame]:
    parser = SseParser()
    frames: list[Frame] = []
    for chunk in chunks:
        frames.extend(parser.feed(chunk))
    return frames


# --------------------------------------------------------------------------- the parser


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_every_line_ending_gives_the_same_frames(newline: str) -> None:
    assert parse(SAMPLE.replace("\n", newline).encode()) == SAMPLE_FRAMES


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_the_result_does_not_depend_on_where_the_chunks_are_cut(newline: str) -> None:
    data = SAMPLE.replace("\n", newline).encode()
    for cut in range(len(data) + 1):  # every single split position
        assert parse(data[:cut], data[cut:]) == SAMPLE_FRAMES, cut
    assert parse(*(data[i : i + 1] for i in range(len(data)))) == SAMPLE_FRAMES  # one byte at a time


def test_a_crlf_cut_between_the_cr_and_the_lf_is_one_line_end() -> None:
    assert parse(b"data: a\r", b"\n\r", b"\n") == [Frame("message", "a", None)]


def test_multibyte_text_survives_a_cut_inside_a_character() -> None:
    data = "data: héllo ✓\n\n".encode()
    assert parse(*(data[i : i + 1] for i in range(len(data)))) == [Frame("message", "héllo ✓", None)]


def test_invalid_utf8_is_replaced_not_fatal() -> None:
    (frame,) = parse(b"data: a\xffb\n\n")
    assert frame.data == "a�b"


def test_a_comment_is_not_a_frame_and_does_not_break_the_one_around_it() -> None:
    assert parse(b"data: a\n: a comment\ndata: b\n\n: keepalive\n\n") == [Frame("message", "a\nb", None)]


@pytest.mark.parametrize(("line", "value"), [("data:x", "x"), ("data: x", "x"), ("data:  x", " x")])
def test_one_leading_space_after_the_colon_is_dropped(line: str, value: str) -> None:
    assert parse(f"{line}\n\n".encode()) == [Frame("message", value, None)]


def test_a_colon_inside_the_value_is_kept() -> None:
    assert parse(b"data: a:b: c\n\n") == [Frame("message", "a:b: c", None)]


def test_a_field_without_a_colon_has_an_empty_value() -> None:
    assert parse(b"data\n\n") == [Frame("message", "", None)]


def test_an_id_containing_nul_is_ignored() -> None:
    assert parse(b"id: a\x00b\ndata: x\n\n") == [Frame("message", "x", None)]


def test_the_id_belongs_to_its_own_frame_only() -> None:
    assert parse(b"id: 1-0\ndata: a\n\ndata: b\n\n") == [Frame("message", "a", "1-0"), Frame("message", "b", None)]


@pytest.mark.parametrize("block", [b"event: x\n\n", b"id: 1-0\n\n", b"retry: 3000\n\n", b"\n\n\n"])
def test_a_block_without_data_is_dropped(block: bytes) -> None:
    assert parse(block) == []


def test_retry_and_unknown_fields_are_ignored() -> None:
    assert parse(b"retry: 3000\nfoo: bar\ndata: x\n\n") == [Frame("message", "x", None)]


def test_a_final_frame_without_its_blank_line_is_dropped() -> None:
    assert parse(b"data: done\n\ndata: cut off\n") == [Frame("message", "done", None)]


def test_a_leading_byte_order_mark_is_removed() -> None:
    assert parse(b"\xef\xbb\xbfdata: x\n\n") == [Frame("message", "x", None)]
    assert parse(b"\xef", b"\xbb", b"\xbfdata: x\n\n") == [Frame("message", "x", None)]


def test_a_frame_that_never_ends_cannot_grow_without_bound() -> None:
    parser = SseParser(max_buffer=100)
    with pytest.raises(DasError, match="too large"):
        parser.feed(b"data: " + b"x" * 200)


def test_many_small_lines_in_one_frame_are_bounded_too() -> None:
    parser = SseParser(max_buffer=100)
    with pytest.raises(DasError, match="too large"):
        for _ in range(100):
            parser.feed(b"data: 0123456789\n")


def test_normal_traffic_does_not_trip_the_bound() -> None:
    parser = SseParser(max_buffer=100)
    for _ in range(1000):
        assert len(parser.feed(b"data: 0123456789\n\n")) == 1


# --------------------------------------------------------------------------- frame to Event


def snapshot_frame(**view: Any) -> Frame:
    return Frame("snapshot", json.dumps({"request_id": "r-1", "status": "Running", **view}), view.get("event_id"))


def envelope_frame(event_type: str = "job.transition", **fields: Any) -> Frame:
    body = {"request_id": "r-1", "event_id": "2-0", "type": event_type, "data": {"job_id": "a"}, **fields}
    return Frame(event_type, json.dumps(body), body.get("event_id"))


def test_a_snapshot_frame_becomes_a_snapshot_event_carrying_the_view() -> None:
    event = event_from_frame(snapshot_frame(event_id="3-0", jobs=[]))
    view = {"request_id": "r-1", "status": "Running", "event_id": "3-0", "jobs": []}
    assert event == Event("snapshot", "3-0", view, "r-1")


def test_a_snapshot_without_events_has_no_event_id() -> None:
    event = event_from_frame(snapshot_frame(event_id=None))
    assert event is not None and event.event_id is None


def test_an_envelope_frame_becomes_an_event() -> None:
    event = event_from_frame(envelope_frame())
    assert event == Event("job.transition", "2-0", {"job_id": "a"}, "r-1")


def test_unknown_event_types_and_extra_fields_pass_through() -> None:
    event = event_from_frame(envelope_frame("request.something_new", extra=1))
    assert event is not None and event.type == "request.something_new" and not event.is_terminal


@pytest.mark.parametrize(
    ("event_type", "terminal"),
    [
        ("request.completed", True),
        ("request.failed", True),
        ("request.cancelled", True),
        ("request.created", False),
        ("request.planned", False),
        ("job.transition", False),
        ("request.reopened", False),
        ("snapshot", False),
    ],
)
def test_terminal_event_types(event_type: str, terminal: bool) -> None:
    assert Event(event_type, "1-0", {}, "r-1").is_terminal is terminal


@pytest.mark.parametrize(
    "data",
    [
        "not json",
        "",
        "[1, 2]",
        '"text"',
        "null",
        "5",
        '{"request_id": "r-1", "event_id": "1-0", "data": {}}',  # no type
        '{"request_id": "r-1", "type": "x", "data": {}}',  # no event id
        '{"request_id": "r-1", "event_id": "1-0", "type": "x", "data": []}',  # data is not an object
        '{"request_id": "r-1", "event_id": 5, "type": "x", "data": {}}',
        '{"request_id": "r-1", "event_id": "1-0", "type": 5, "data": {}}',
    ],
)
def test_a_frame_that_is_not_a_usable_event_is_skipped(data: str) -> None:
    assert event_from_frame(Frame("x", data, None)) is None


def test_a_snapshot_that_is_not_an_object_is_skipped() -> None:
    assert event_from_frame(Frame("snapshot", "[1]", None)) is None
