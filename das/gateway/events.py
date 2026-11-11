"""Server-Sent Events endpoint ``GET /v1/requests/{id}/events`` (Phase 4 Req 6.2, 7.1-7.9).

A stream is: the ``snapshot`` frame (the Request view without ``final_result``), then every Event
after the client's position, with ``: keepalive`` comments while it is idle. It closes after a
Terminal_Event once the Request is terminal. The frame loop (:func:`event_frames`) talks to its
source through a small protocol and to time through the injected ``Clock``, so it is tested without
Redis or real waiting.

Two limits guard the Gateway (Req 7.7): a per-key cap kept in Redis, so every replica sees it, and
a per-replica cap kept in :class:`StreamGate`. Both are released however a stream ends (Req 7.8).
"""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Any, Final, Protocol

import anyio
from fastapi import APIRouter, Header, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

from das.client.events import TERMINAL_EVENT_TYPES
from das.client.settings import ClientSettings
from das.clock import Clock
from das.config.schema import EventsConfig
from das.gateway.openapi import EVENTS_RESPONSES
from das.models import RequestStatus, is_terminal_request
from das.observability import get_logger
from das.observability.metrics import GatewayMetrics
from das.state.store import StateStore, StoredRequest

log = get_logger(__name__)

MEDIA_TYPE: Final = "text/event-stream"
KEEPALIVE_FRAME: Final = ": keepalive\n\n"
#: Events fetched per read; a full batch means more may be waiting (Req 7.3).
READ_COUNT: Final = 100
#: Longest blocking read in ms; bounds how late a disconnect or a keepalive is noticed (Req 7.9).
BLOCK_MS: Final = 2000
#: Socket timeout of the dedicated events client: longer than a blocking read plus its slack.
EVENTS_SOCKET_TIMEOUT: Final = 10.0
_READ_SLACK: Final = 5.0
_RELEASE_TIMEOUT: Final = 2.0
MIN_SLOT_TTL: Final = 30.0

TOO_MANY_STREAMS: Final = {"error": "too_many_streams"}
_NOT_FOUND: Final = {"error": "not_found"}
_STREAM_ID: Final = re.compile(r"([0-9]{1,20})-([0-9]{1,20})", re.ASCII)
_EVENT_TYPE: Final = re.compile(r"[a-z0-9_.]{1,64}", re.ASCII)
_U64_MAX: Final = 2**64 - 1

Bounded = Callable[..., Awaitable[Any]]


# --------------------------------------------------------------------------- pure helpers


def parse_stream_id(value: str) -> tuple[int, int] | None:
    """``<ms>-<seq>`` as a comparable pair, or ``None`` when ``value`` is not a stream ID (Req 7.4)."""
    match = _STREAM_ID.fullmatch(value)
    if match is None:
        return None
    ms, seq = int(match.group(1)), int(match.group(2))
    return (ms, seq) if ms <= _U64_MAX and seq <= _U64_MAX else None


def _frame(event: str, event_id: str | None, payload: Mapping[str, Any]) -> str:
    lines = [f"event: {event}"]
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append("data: " + json.dumps(payload, separators=(",", ":")))
    return "\n".join(lines) + "\n\n"


def event_frame(request_id: str, event_id: str, fields: Mapping[str, str]) -> str | None:
    """One Event as an SSE frame (``EventEnvelope`` as data), or ``None`` for an entry to skip.

    A stored entry is skipped when it has no usable type or JSON object data: one bad entry
    must not end a client's stream. The type is charset-checked because it becomes a line of the
    frame.
    """
    event_type = fields.get("type")
    raw = fields.get("data")
    if not event_type or raw is None or _EVENT_TYPE.fullmatch(event_type) is None:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    envelope = {"request_id": request_id, "event_id": event_id, "type": event_type, "data": data}
    return _frame(event_type, event_id, envelope)


def snapshot_frame(view: Mapping[str, Any], event_id: str | None, *, resync: bool) -> str:
    """The first frame: the Request view with the Event_ID it was read at (Req 7.2, 7.4).

    ``id`` is left out when the Request has no Event yet. ``resync`` is only present when true.
    """
    payload = {**view, "event_id": event_id}
    if resync:
        payload["resync"] = True
    return _frame("snapshot", event_id, payload)


def slot_ttl(keepalive: float) -> float:
    """Lifetime of a stream's per-key slot; renewed well inside it by :func:`event_frames`."""
    return max(MIN_SLOT_TTL, 3 * keepalive)


# --------------------------------------------------------------------------- the frame loop


class EventSource(Protocol):
    async def read(self, after: str, block_ms: int | None) -> Sequence[tuple[str, Mapping[str, str]]]:
        """Events after ``after``, oldest first, at most :data:`READ_COUNT`; ``None`` does not block."""
        ...

    async def status(self) -> RequestStatus | None:
        """Current Request_Status, ``None`` when the Request no longer exists."""
        ...

    async def renew(self) -> None:
        """Keep this stream's per-key slot alive."""
        ...


async def event_frames(
    *,
    request_id: str,
    snapshot: str,
    cursor: str,
    drain_only: bool,
    source: EventSource,
    clock: Clock,
    keepalive: float,
    renew_every: float,
    is_disconnected: Callable[[], Awaitable[bool]],
) -> AsyncIterator[str]:
    """Frames of one stream: ``snapshot``, then Events after ``cursor``, then keepalives.

    ``drain_only`` (a Request already terminal when the stream opened) sends what is left after
    ``cursor`` without blocking and ends (Req 7.6). Otherwise the stream ends when a Terminal_Event
    has been sent and the Request is still terminal, so a Replay that reopened it keeps the stream
    open (Req 6.4); and at a keepalive tick when the Request is terminal or gone, which also covers
    a purged Request and a Terminal_Event that was trimmed away. A full batch is never taken as
    "drained": events may still be waiting behind it.
    """
    yield snapshot
    block_ms = max(1, min(BLOCK_MS, int(keepalive * 1000)))
    last_sent = last_renew = clock.now()
    while True:
        if await is_disconnected():
            return
        entries = await source.read(cursor, None if drain_only else block_ms)
        sent_terminal = False
        for event_id, fields in entries:
            cursor = event_id
            frame = event_frame(request_id, event_id, fields)
            if frame is None:
                continue
            yield frame
            last_sent = clock.now()
            sent_terminal = sent_terminal or fields.get("type") in TERMINAL_EVENT_TYPES
        drained = len(entries) < READ_COUNT
        if drain_only:
            if drained:
                return
            continue
        now = clock.now()
        idle = not entries and now - last_sent >= keepalive
        if (sent_terminal and drained) or idle:
            current = await source.status()
            if current is None or is_terminal_request(current):
                return
        if idle:
            yield KEEPALIVE_FRAME
            last_sent = now
        if now - last_renew >= renew_every:
            await source.renew()
            last_renew = now


# --------------------------------------------------------------------------- limits


class StreamGate:
    """Per-replica count of open streams, capped at ``events.max_connections`` (Req 7.7)."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.open = 0

    def try_acquire(self) -> bool:
        if self.open >= self.limit:
            return False
        self.open += 1
        return True

    def release(self) -> None:
        self.open = max(0, self.open - 1)


class StoreEventSource:
    """:class:`EventSource` over the State_Store; blocking reads use the dedicated ``client``."""

    def __init__(
        self,
        store: StateStore,
        request_id: str,
        *,
        client: Any,
        fingerprint: str,
        member: str | None,
        ttl: float,
    ) -> None:
        self._store = store
        self._rid = request_id
        self._client = client
        self._fingerprint = fingerprint
        self._member = member
        self._ttl = ttl

    async def read(self, after: str, block_ms: int | None) -> list[tuple[str, dict[str, str]]]:
        async with asyncio.timeout((block_ms or 0) / 1000 + _READ_SLACK):
            return await self._store.read_events(
                self._rid, after, block_ms=block_ms, count=READ_COUNT, client=self._client
            )

    async def status(self) -> RequestStatus | None:
        async with asyncio.timeout(_READ_SLACK):
            return await self._store.get_status(self._rid)

    async def renew(self) -> None:
        if self._member is not None:
            async with asyncio.timeout(_READ_SLACK):
                await self._store.renew_stream(self._fingerprint, self._member, self._ttl)


# --------------------------------------------------------------------------- the route


def register_events_route(
    router: APIRouter,
    *,
    store: StateStore,
    bounded: Bounded,
    metrics: Callable[[], GatewayMetrics],
    clock: Clock,
    config: EventsConfig,
    view: Callable[[StoredRequest], dict[str, Any]],
    events_client: Any = None,
) -> StreamGate:
    """Add ``GET /requests/{request_id}/events`` to the ``/v1`` router; returns the replica gate.

    ``view`` renders a snapshot as the Request view. ``events_client`` serves the blocking reads
    (default: the store's own client); it needs a socket timeout above :data:`BLOCK_MS`.
    """
    gate = StreamGate(config.max_connections)
    ttl = slot_ttl(config.keepalive)

    async def position(
        request_id: str, last_event_id: str | None, snapshot_id: str | None
    ) -> tuple[str, bool]:
        """Where to read from, and whether the client must resync from the snapshot (Req 7.3, 7.4)."""
        cursor = snapshot_id or "0-0"
        if not last_event_id:  # absent or empty
            return cursor, False
        wanted = parse_stream_id(last_event_id)
        if wanted is None:
            return cursor, True
        first = await bounded(store.first_event_id(request_id))
        oldest = parse_stream_id(first) if first else None
        if oldest is not None and wanted < oldest:
            return cursor, True
        return last_event_id, False

    @router.get("/requests/{request_id}/events", responses=EVENTS_RESPONSES)
    async def request_events(
        request: Request,
        request_id: str,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ) -> Any:
        settings: ClientSettings = request.state.client_settings
        snap = await bounded(
            store.load_snapshot(
                request_id, include_transitions=True, include_usage=True, include_client=True
            )
        )
        if snap is None:
            return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=dict(_NOT_FOUND))
        cursor, resync = await position(request_id, last_event_id, snap.last_event_id)
        body = view(snap)
        body.pop("final_result", None)
        snapshot = snapshot_frame(body, snap.last_event_id, resync=resync)

        if not gate.try_acquire():
            metrics().client_rejected("too_many_streams")
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=dict(TOO_MANY_STREAMS)
            )
        member = uuid.uuid4().hex if settings.max_streams > 0 else None
        if member is not None:
            try:
                granted = await bounded(
                    store.acquire_stream(settings.fingerprint, member, settings.max_streams, ttl)
                )
            except BaseException:
                gate.release()
                raise
            if not granted:
                gate.release()
                metrics().client_rejected("too_many_streams")
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS, content=dict(TOO_MANY_STREAMS)
                )

        released = False

        async def release() -> None:
            """Give back both counts; safe to call twice and when the task is being cancelled."""
            nonlocal released
            if released:
                return
            released = True
            gate.release()
            if member is None:
                return
            with anyio.CancelScope(shield=True):
                try:
                    async with asyncio.timeout(_RELEASE_TIMEOUT):
                        await store.release_stream(settings.fingerprint, member)
                except Exception as exc:  # the slot then expires on its own
                    log.warning("event stream slot not released", error=type(exc).__name__)

        source = StoreEventSource(
            store,
            request_id,
            client=events_client or store.client,
            fingerprint=settings.fingerprint,
            member=member,
            ttl=ttl,
        )

        async def frames() -> AsyncIterator[str]:
            try:
                async for frame in event_frames(
                    request_id=request_id,
                    snapshot=snapshot,
                    cursor=cursor,
                    drain_only=is_terminal_request(snap.status),
                    source=source,
                    clock=clock,
                    keepalive=config.keepalive,
                    renew_every=ttl / 3,
                    is_disconnected=request.is_disconnected,
                ):
                    yield frame
            except Exception as exc:
                # Headers are already sent: end the stream and let the client resume from its Last-Event-ID.
                log.warning("event stream ended by a store error", error=type(exc).__name__)
            finally:
                await release()

        return StreamingResponse(
            frames(),
            media_type=MEDIA_TYPE,
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            background=BackgroundTask(release),
        )

    return gate


__all__ = [
    "BLOCK_MS",
    "EVENTS_SOCKET_TIMEOUT",
    "KEEPALIVE_FRAME",
    "READ_COUNT",
    "StoreEventSource",
    "StreamGate",
    "event_frame",
    "event_frames",
    "parse_stream_id",
    "register_events_route",
    "slot_ttl",
    "snapshot_frame",
]
