"""Shared fixtures: both clients behind one interface, with injected sleep and randomness."""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import httpx
import pytest

from das_client import AsyncDasClient, DasClient

BASE = "http://das.test"
API_KEY = "test-key-secret-0001"


class Timing:
    """Records every backoff or polling wait and returns instantly; ``rand`` is a fixed jitter factor.

    A fake monotonic clock (``now``) moves forward by every recorded sleep, so deadlines are exact.
    """

    def __init__(self, rand: float = 1.0) -> None:
        self.sleeps: list[float] = []
        self.rand_value = rand
        self.now = 1000.0

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    async def asleep(self, seconds: float) -> None:
        self.sleep(seconds)

    def rand(self) -> float:
        return self.rand_value

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def timing() -> Timing:
    return Timing()


@pytest.fixture(params=["sync", "async"])
def kind(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
async def make_client(kind: str, timing: Timing):
    """``make_client(**kwargs)`` builds the client under test; the ones it built are closed afterwards."""
    built: list[Any] = []

    def factory(**kwargs: Any) -> Any:
        if kind == "sync":
            kwargs.setdefault("sleep", timing.sleep)
            client: Any = DasClient(BASE, API_KEY, rand=timing.rand, **kwargs)
        else:
            kwargs.setdefault("sleep", timing.asleep)
            client = AsyncDasClient(BASE, API_KEY, rand=timing.rand, **kwargs)
        built.append(client)
        return client

    yield factory
    for client in built:
        await call(client, "aclose" if kind == "async" else "close")


async def call(client: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    """Call a method of either client: awaits the async one, calls the sync one directly."""
    result = getattr(client, name)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


# --------------------------------------------------------------------------- Server-Sent Events helpers


class FrameStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A response body made of chunks that can break with an error, and that records how it was used."""

    def __init__(
        self, chunks: list[bytes], error: Exception | None = None, on_chunk: Callable[[], None] | None = None
    ) -> None:
        self.chunks = chunks
        self.error = error
        self.on_chunk = on_chunk  # called as each chunk is handed out, e.g. to let fake time pass
        self.consumed = 0
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.consumed += 1
            if self.on_chunk is not None:
                self.on_chunk()
            yield chunk
        if self.error is not None:
            raise self.error

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.consumed += 1
            if self.on_chunk is not None:
                self.on_chunk()
            yield chunk
        if self.error is not None:
            raise self.error

    def close(self) -> None:
        self.closed = True

    async def aclose(self) -> None:
        self.closed = True


def sse_response(
    *frames: str | bytes,
    error: Exception | None = None,
    status: int = 200,
    content_type: str = "text/event-stream",
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """A stream response; each frame is one chunk. ``response.stream`` is the :class:`FrameStream`."""
    chunks = [f.encode() if isinstance(f, str) else f for f in frames]
    return httpx.Response(
        status, headers={"content-type": content_type, **(headers or {})}, stream=FrameStream(chunks, error)
    )


def frame(event: str, data: Any, event_id: str | None = None) -> str:
    lines = [f"event: {event}"]
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append("data: " + json.dumps(data, separators=(",", ":")))
    return "\n".join(lines) + "\n\n"


def snapshot(
    status: str = "Running", event_id: str | None = None, *, resync: bool = False, request_id: str = "r-1"
) -> str:
    """The first frame of a stream: the Request view plus the Event_ID it was read at."""
    view: dict[str, Any] = {"request_id": request_id, "status": status, "jobs": [], "event_id": event_id}
    if resync:
        view["resync"] = True
    return frame("snapshot", view, event_id)


def envelope(event_type: str, event_id: str, data: dict[str, Any] | None = None, request_id: str = "r-1") -> str:
    body = {"request_id": request_id, "event_id": event_id, "type": event_type, "data": data or {}}
    return frame(event_type, body, event_id)


KEEPALIVE = ": keepalive\n\n"


async def drain(client: Any, request_id: str = "r-1", **kwargs: Any) -> tuple[list[Any], Exception | None]:
    """All events of ``client.events(...)``, and the error that ended it (``None`` for a normal end)."""
    events: list[Any] = []
    iterator = client.events(request_id, **kwargs)
    try:
        if hasattr(iterator, "__aiter__"):
            async for event in iterator:
                events.append(event)
        else:
            for event in iterator:
                events.append(event)
    except Exception as exc:  # noqa: BLE001  (the tests inspect whatever ended the stream)
        return events, exc
    return events, None


async def take(client: Any, count: int, request_id: str = "r-1", **kwargs: Any) -> list[Any]:
    """The first ``count`` events, then the generator is closed (an early stop by the consumer)."""
    events: list[Any] = []
    iterator = client.events(request_id, **kwargs)
    if hasattr(iterator, "__aiter__"):
        try:
            async for event in iterator:
                events.append(event)
                if len(events) >= count:
                    break
        finally:
            await iterator.aclose()
    else:
        try:
            for event in iterator:
                events.append(event)
                if len(events) >= count:
                    break
        finally:
            iterator.close()
    return events
