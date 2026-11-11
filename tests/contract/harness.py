"""In-process wiring of the Client_Package to the real Gateway_API app (das-client-api task 9.6; Phase 4 Req 9.10).

The async client talks to the app through ``httpx.ASGITransport`` directly. ``ASGITransport`` is async only, so the
blocking client gets a small bridge: each of its requests is run on the test's own event loop (where fakeredis
lives) from the worker thread the test calls the client in, and the whole response is buffered. Buffering is fine
because every stream these tests open ends by itself.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from das_client import AsyncDasClient, DasClient

BASE = "http://gateway.test"


def _is_events(request: httpx.Request) -> bool:
    return request.url.path.endswith("/events")


class AsgiTransport(httpx.ASGITransport):
    """``ASGITransport`` that can be told to fail the next ``fail_events`` stream connections."""

    def __init__(self, app: Any) -> None:
        super().__init__(app=app)
        self.fail_events = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.fail_events and _is_events(request):
            self.fail_events -= 1
            raise httpx.ConnectError("injected stream failure", request=request)
        return await super().handle_async_request(request)


class LoopBridge(httpx.BaseTransport):
    """Blocking transport over :class:`AsgiTransport`, run on ``loop`` (the test's event loop)."""

    def __init__(self, app: Any, loop: asyncio.AbstractEventLoop) -> None:
        self.asgi = AsgiTransport(app)
        self._loop = loop

    @property
    def fail_events(self) -> int:
        return self.asgi.fail_events

    @fail_events.setter
    def fail_events(self, value: int) -> None:
        self.asgi.fail_events = value

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        return asyncio.run_coroutine_threadsafe(self._round_trip(request), self._loop).result(timeout=30)

    async def _round_trip(self, request: httpx.Request) -> httpx.Response:
        response = await self.asgi.handle_async_request(request)
        body = b"".join([chunk async for chunk in response.stream])  # type: ignore[union-attr]
        await response.stream.aclose()  # type: ignore[union-attr]
        return httpx.Response(response.status_code, headers=response.headers, content=body)


async def _no_wait(seconds: float) -> None:
    return None


def _no_wait_blocking(seconds: float) -> None:
    return None


class Contract:
    """One client (``kind`` is ``"async"`` or ``"sync"``) wired to one app, behind a single interface."""

    def __init__(self, kind: str, app: Any, api_key: str, **client_kwargs: Any) -> None:
        self.kind = kind
        self.app = app
        if kind == "async":
            self.transport: Any = AsgiTransport(app)
            self._http: Any = httpx.AsyncClient(transport=self.transport)
            self.client: Any = AsyncDasClient(BASE, api_key, http_client=self._http, sleep=_no_wait, **client_kwargs)
        else:
            self.transport = LoopBridge(app, asyncio.get_running_loop())
            self._http = httpx.Client(transport=self.transport)
            self.client = DasClient(BASE, api_key, http_client=self._http, sleep=_no_wait_blocking, **client_kwargs)

    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """Call a client method; the blocking client runs in a worker thread so the event loop stays free."""
        if self.kind == "async":
            return await getattr(self.client, name)(*args, **kwargs)
        return await asyncio.to_thread(lambda: getattr(self.client, name)(*args, **kwargs))

    async def events(self, request_id: str, **kwargs: Any) -> list[Any]:
        """Every event ``events()`` yields until it ends by itself."""
        if self.kind == "async":
            return [event async for event in self.client.events(request_id, **kwargs)]
        return await asyncio.to_thread(lambda: list(self.client.events(request_id, **kwargs)))

    async def close(self) -> None:
        if self.kind == "async":
            await self._http.aclose()
        else:
            await asyncio.to_thread(self._http.close)
