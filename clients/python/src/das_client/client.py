"""The synchronous client (Phase 4 Req 9.2-9.6)."""

from __future__ import annotations

import datetime as dt
import random
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from typing import Any

import httpx

from ._sse import SseParser, StreamState
from ._transport import (
    POLL_INTERVAL,
    error_from_response,
    events_path,
    is_event_stream,
    is_replay,
    list_params,
    next_delay,
    parse_json,
    parse_retry_after,
    path_segment,
    stream_headers,
    submit_body,
)
from .errors import DasError, RateLimited, ServiceUnavailable, TransportFailure, WaitTimeout
from .models import Budget, CancelResult, Event, Page, RequestView, Submitted


class DasClient:
    """Blocking client for the DAS Gateway. Use as a context manager, or call :meth:`close`.

    ``http_client`` lets the caller supply (and keep ownership of) the ``httpx.Client``. ``sleep``, ``rand`` and
    ``monotonic`` are test seams for the retry wait, its jitter and the ``wait`` deadline. ``stream_timeout`` is
    how long an event stream may stay silent (the server sends a keepalive well inside it) before it is reconnected.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        max_retries: int = 3,
        timeout: float = 30.0,
        stream_timeout: float = 60.0,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rand: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be 0 or more")
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._max_retries = max_retries
        self._timeout = timeout
        self._stream_timeout = stream_timeout
        self._sleep = sleep
        self._rand = rand
        self._monotonic = monotonic
        self._owns_http = http_client is None
        self._http = http_client if http_client is not None else httpx.Client(timeout=timeout)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self._base_url!r})"  # never the API key

    def __enter__(self) -> DasClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the HTTP client, unless the caller supplied it."""
        if self._owns_http:
            self._http.close()

    # ----------------------------------------------------------------------- calls

    def submit(self, prompt: str, *, budget: Budget | None = None, idempotency_key: str | None = None) -> Submitted:
        """Submit a prompt. Without a key a UUID4 is generated *once* and reused by every retry (Req 9.3)."""
        key = idempotency_key if idempotency_key is not None else str(uuid.uuid4())
        response = self._request(
            "POST", "/v1/requests", json=submit_body(prompt, budget), headers={"Idempotency-Key": key}
        )
        return Submitted.from_json(parse_json(response), replayed=is_replay(response))

    def get(self, request_id: str) -> RequestView:
        response = self._request("GET", f"/v1/requests/{path_segment(request_id)}")
        return RequestView.from_json(parse_json(response))

    def cancel(self, request_id: str) -> CancelResult:
        response = self._request("POST", f"/v1/requests/{path_segment(request_id)}/cancel")
        return CancelResult.from_json(parse_json(response))

    def list_requests(
        self,
        *,
        status: str | Iterable[str] | None = None,
        created_after: dt.datetime | str | None = None,
        created_before: dt.datetime | str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Page:
        params = list_params(
            status=status, created_after=created_after, created_before=created_before, limit=limit, cursor=cursor
        )
        return Page.from_json(parse_json(self._request("GET", "/v1/requests", params=params or None)))

    def events(self, request_id: str, *, last_event_id: str | None = None) -> Iterator[Event]:
        """Events of a Request: its snapshot first, then each event, ending after the terminal one.

        Reconnects with ``Last-Event-ID`` when the stream breaks, up to ``max_retries`` times in a row (any event
        resets the count). Close the iterator early (``.close()``, or leave a ``for`` loop through ``break`` and
        let it be collected) to release the connection.
        """
        return self._stream(f"{self._base_url}{events_path(request_id)}", last_event_id, None)

    def wait(self, request_id: str, *, timeout: float) -> RequestView:
        """Block until the Request is Completed, Failed or Cancelled, and return its final view (Req 9.6).

        Streams events, and falls back to polling ``get`` every second if the stream cannot be kept up. Raises
        :class:`WaitTimeout` after ``timeout`` seconds (honoured to within about a second while a stream is open).
        """
        url = f"{self._base_url}{events_path(request_id)}"
        deadline = self._monotonic() + timeout
        streaming = True
        while True:
            if streaming:
                try:
                    for _ in self._stream(url, None, deadline):
                        pass
                except (TransportFailure, ServiceUnavailable, RateLimited):
                    streaming = False
            view = self.get(request_id)  # the stream's snapshot has no final result
            if view.is_terminal:
                return view
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise WaitTimeout("timed out waiting for the request to finish")
            self._sleep(min(POLL_INTERVAL, remaining))

    # ----------------------------------------------------------------------- transport

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        """One call with retries on connection errors, 503 and 429 (Req 9.4); other errors raise at once."""
        url = f"{self._base_url}{path}"
        merged = {"X-API-Key": self._api_key, **(headers or {})}
        attempt = 0
        while True:
            try:
                response = self._http.request(
                    method, url, json=json, headers=merged, params=params, timeout=self._timeout
                )
            except httpx.TransportError as exc:
                delay = next_delay(attempt, self._max_retries, self._rand)
                if delay is None:
                    raise TransportFailure(f"transport failure: {type(exc).__name__}") from exc
            else:
                if response.status_code < 300:
                    return response
                error = error_from_response(response.status_code, response.headers, response.content)
                delay = next_delay(
                    attempt,
                    self._max_retries,
                    self._rand,
                    status=response.status_code,
                    retry_after=parse_retry_after(response.headers.get("retry-after")),
                )
                if delay is None:
                    raise error
            self._sleep(delay)
            attempt += 1

    def _check_deadline(self, deadline: float | None) -> None:
        if deadline is not None and self._monotonic() >= deadline:
            raise WaitTimeout("timed out waiting for the request to finish")

    def _stream_timeouts(self, deadline: float | None) -> httpx.Timeout:
        read = self._stream_timeout
        if deadline is not None:  # a waiting stream never sits silent past the deadline (but not under a second)
            read = min(read, max(deadline - self._monotonic(), 1.0))
        return httpx.Timeout(self._timeout, read=read)

    def _stream(self, url: str, last_event_id: str | None, deadline: float | None) -> Iterator[Event]:
        state = StreamState(last_event_id)
        failures = 0  # in a row: any event resets it
        first = True
        while True:
            if not first:
                self._check_deadline(deadline)
            first = False
            headers = stream_headers(self._api_key, state.begin())
            error: DasError | None = None
            cause: httpx.TransportError | None = None
            status: int | None = None
            retry_after: float | None = None
            try:
                with self._http.stream("GET", url, headers=headers, timeout=self._stream_timeouts(deadline)) as resp:
                    if resp.status_code >= 300:
                        resp.read()
                        error = error_from_response(resp.status_code, resp.headers, resp.content)
                        status = resp.status_code
                        retry_after = parse_retry_after(resp.headers.get("retry-after"))
                    elif not is_event_stream(resp):
                        raise DasError("unexpected response: not an event stream", status_code=resp.status_code)
                    else:
                        parser = SseParser()
                        for chunk in resp.iter_bytes():
                            for frame in parser.feed(chunk):
                                event, progress = state.accept(frame)
                                if progress:
                                    failures = 0
                                if event is not None:
                                    yield event
                                if state.finished:
                                    return
                            self._check_deadline(deadline)
                        if state.snapshot_terminal:
                            return  # the Request was already over: the server sent the snapshot and closed
            except httpx.TransportError as exc:
                cause = exc
            delay = next_delay(failures, self._max_retries, self._rand, status=status, retry_after=retry_after)
            if delay is None:
                if error is not None:
                    raise error
                if cause is not None:
                    raise TransportFailure(f"transport failure: {type(cause).__name__}") from cause
                raise TransportFailure("the event stream ended before the request finished")
            self._check_deadline(deadline)
            self._sleep(delay)
            failures += 1
