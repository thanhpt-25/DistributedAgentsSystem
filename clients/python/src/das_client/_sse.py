"""Server-Sent Events: a small parser, and the rules that turn frames into a resumable event stream
(Phase 4 Req 9.6). Pure; the clients only add the network around it.
"""

from __future__ import annotations

import codecs
import json
import re
from dataclasses import dataclass

from .errors import DasError
from .models import TERMINAL_STATUSES, Event

#: Largest unfinished frame (or unterminated line) the parser will hold; a server that never ends one is broken.
MAX_BUFFER = 1024 * 1024
_EOL = re.compile(r"\r\n|\r|\n")


@dataclass(frozen=True, slots=True)
class Frame:
    """One dispatched block of an event stream: its ``event`` name (``message`` by default), data and id."""

    event: str
    data: str
    id: str | None


class SseParser:
    """Incremental ``text/event-stream`` parser: ``feed`` bytes in any chunking, get the frames completed so far.

    Comments (the server's ``: keepalive``) and blocks without ``data`` are dropped, as is a last frame that is
    not followed by its blank line. An ``id`` containing NUL is ignored, as the SSE specification says.
    """

    def __init__(self, max_buffer: int = MAX_BUFFER) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._max = max_buffer
        self._buf = ""
        self._started = False
        self._skip_lf = False  # the previous chunk ended in a CR, so a leading LF belongs to it
        self._event: str | None = None
        self._data: list[str] = []
        self._id: str | None = None
        self._size = 0

    def feed(self, chunk: bytes) -> list[Frame]:
        text = self._decoder.decode(chunk)
        if text and not self._started:
            self._started = True
            text = text.removeprefix("﻿")
        buf = self._buf + text
        start = 0
        if self._skip_lf and buf:
            start = 1 if buf.startswith("\n") else 0
            self._skip_lf = False
        frames: list[Frame] = []
        while (match := _EOL.search(buf, start)) is not None:
            if match.group() == "\r" and match.end() == len(buf):
                self._skip_lf = True
            frame = self._line(buf[start : match.start()])
            if frame is not None:
                frames.append(frame)
            start = match.end()
        self._buf = buf[start:]
        if len(self._buf) + self._size > self._max:
            raise DasError("unexpected response: an event stream frame is too large")
        return frames

    def _line(self, line: str) -> Frame | None:
        if not line:
            return self._dispatch()
        if line.startswith(":"):
            return None
        name, colon, value = line.partition(":")
        if colon and value.startswith(" "):
            value = value[1:]
        if name == "data":
            self._data.append(value)
            self._size += len(value) + 1
        elif name == "event":
            self._event = value
        elif name == "id" and "\0" not in value:
            self._id = value
        return None  # retry and unknown fields are ignored

    def _dispatch(self) -> Frame | None:
        frame = Frame(self._event or "message", "\n".join(self._data), self._id) if self._data else None
        self._event, self._data, self._id, self._size = None, [], None, 0
        return frame


def event_from_frame(frame: Frame) -> Event | None:
    """The :class:`Event` of a frame, or ``None`` for one to skip (not JSON, not an object, not an event).

    One bad frame must not end a client's stream, so unusable frames are dropped, as the server drops a bad entry.
    """
    try:
        obj = json.loads(frame.data)
    except (ValueError, RecursionError):
        return None
    if not isinstance(obj, dict):
        return None
    if frame.event == "snapshot":
        event_id, request_id = obj.get("event_id"), obj.get("request_id")
        return Event(
            "snapshot",
            event_id if isinstance(event_id, str) else None,
            obj,
            request_id if isinstance(request_id, str) else None,
        )
    return Event.from_envelope(obj)


class StreamState:
    """What one ``events()`` call remembers across reconnects (Req 9.6, and the server's Req 7.3 to 7.6).

    The server sends a snapshot first on every connection. After a reconnect with a ``Last-Event-ID`` the events
    that follow start *at that position*, not at the snapshot's id, so the position must not jump to the snapshot's
    id (events in between are still coming). The exceptions are a connection that had no position to send, and a
    snapshot flagged ``resync`` (the server could not honour the position): there the snapshot's id is the new one.
    """

    def __init__(self, last_event_id: str | None) -> None:
        self.last_id = last_event_id or None
        self.finished = False  # a terminal event was delivered
        self.snapshot_terminal = False  # this connection's snapshot says the Request is over
        self._seen_snapshot = False
        self._sent_id: str | None = None

    def begin(self) -> str | None:
        """Start a connection; the position to send as ``Last-Event-ID`` (``None`` for none)."""
        self._sent_id = self.last_id
        self.snapshot_terminal = False
        return self.last_id

    def accept(self, frame: Frame) -> tuple[Event | None, bool]:
        """Process a frame. Returns the event to hand to the caller (or ``None``) and whether it is progress.

        Only a real event counts as progress (it resets the count of failures in a row); a snapshot does not,
        so a server that drops the connection right after every snapshot cannot keep a client looping forever.
        """
        event = event_from_frame(frame)
        if event is None:
            return None, False
        if event.type == "snapshot":
            resync = event.data.get("resync") is True
            if (self._sent_id is None or resync) and event.event_id is not None:
                self.last_id = event.event_id
            first = not self._seen_snapshot
            self._seen_snapshot = True
            self.snapshot_terminal = event.data.get("status") in TERMINAL_STATUSES
            # Swallow the repeat snapshot of a plain resume: the caller already has that state and these events.
            return (event if first or self._sent_id is None or resync else None), False
        self.last_id = event.event_id
        self.finished = event.is_terminal
        return event, True
