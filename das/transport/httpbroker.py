"""The HTTP_Broker: durable queues on Redis streams, served over HTTP behind a bearer token (Phase 7 Req 2.2 to
2.7 and 2.10).

It lets agents use plain HTTPS as their Transport where AMQP is not allowed. The Gateway mounts :func:`broker_routes` on
its Ops_Port when ``http_broker.enabled`` is true; :class:`HttpTransport` (``das.transport.http``) is the client.

A destination is a Redis stream ``das:broker:<queue name>`` with one consumer group ``das``, so a Message survives a
Gateway restart and goes to one consumer of the group. High-priority Messages (``das-priority: high``, consumed here and
never delivered) go to a second stream, ``...:high``, which every pull reads first.

* ``publish`` stores the Message with ``XADD`` and answers only after Redis has it (Req 2.4).
* ``pull`` first reclaims entries pending longer than the visibility timeout (``XAUTOCLAIM``, which counts the
  delivery), then reads new ones (``XREADGROUP``); with ``wait`` it long-polls with ``XREAD BLOCK`` (Req 2.5).
* ``settle`` maps ACK to ``XACK`` plus ``XDEL``; REQUEUE copies the entry to the tail of its stream with a higher count
  and acknowledges the old one in one transaction; REJECT, and a ``jobs/`` Message returned more than the delivery
  limit, are copied to ``dlq`` and ``dlq-events`` with ``x-death-reason``, ``x-original-destination`` and
  ``x-delivery-count``, the same keys every transport writes (Req 2.6). A dead letter is never dead-lettered again.

A requeue puts the Message at the tail, not the front as a RabbitMQ requeue does; Jobs are idempotent by
``(job_id, attempt)``, so the order of a retry does not matter.

Every route is ``POST``, needs ``Authorization: Bearer <token>`` (compared with ``hmac.compare_digest``; a missing or
wrong token is a 401 with an empty body), and answers a short ``{"error": ...}`` for a bad request (400), a Message
over 1 MiB (413) or an unreachable store (503), never a stack trace or an internal detail.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import json
import re
from dataclasses import dataclass
from typing import Any, Final

from redis.exceptions import RedisError, ResponseError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from das.observability import get_logger
from das.transport.base import PRIORITY_HEADER, PRIORITY_HIGH, queue_name_for

log = get_logger("das.transport.httpbroker")

MAX_MESSAGE_BYTES: Final = 1_048_576
MAX_BATCH: Final = 100
MAX_WAIT: Final = 30.0
MAX_HEADERS: Final = 64
MAX_HEADER_KEY: Final = 128
MAX_HEADER_VALUE: Final = 1024
#: A request body can hold one base64 Message (4/3 of 1 MiB), its headers and JSON framing.
MAX_REQUEST_BYTES: Final = MAX_MESSAGE_BYTES * 3 // 2 + 128 * 1024
#: A reply destination is read once by the caller; if the caller is gone, the stream goes too.
REPLY_TTL_SECONDS: Final = 600
KEY_PREFIX: Final = "das:broker:"
GROUP: Final = "das"
#: The longest a pull blocks in Redis before it looks again for entries whose visibility timeout has run out.
BLOCK_SLICE: Final = 1.0
DEFAULT_DEAD_LETTERS: Final = ("dlq", "dlq-events")

_DESTINATION: Final = re.compile(r"[a-z0-9][a-z0-9._/-]{0,127}")
_CONSUMER: Final = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
_DELIVERY_ID: Final = re.compile(r"([nh]):(\d{1,20}-\d{1,20})")
_OUTCOMES: Final = ("ack", "requeue", "reject")


class MessageTooLarge(ValueError):
    """A Message, or a request carrying one, over the size limit (Req 2.10)."""


@dataclass(frozen=True, slots=True)
class BrokerSettings:
    """``visibility_timeout`` seconds before an unacknowledged Message is deliverable again (Req 2.5);
    ``delivery_limit`` is how often a ``jobs/`` Message may be returned (``max_attempts - 1``, as the RabbitMQ
    policy has it)."""

    token: str
    visibility_timeout: float = 30.0
    delivery_limit: int = 4
    dead_letter_destinations: tuple[str, ...] = DEFAULT_DEAD_LETTERS

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("the broker needs a token")
        if not self.visibility_timeout > 0:
            raise ValueError("visibility_timeout must be positive")
        if self.delivery_limit < 0:
            raise ValueError("delivery_limit must not be negative")


@dataclass(frozen=True, slots=True)
class Delivered:
    """A Message handed to a consumer: its settle ``id``, bytes, headers and earlier returns (``delivery_count``)."""

    id: str
    body: bytes
    headers: dict[str, str]
    delivery_count: int


def stream_key(destination: str, high: bool = False) -> str:
    return f"{KEY_PREFIX}{queue_name_for(destination)}{':high' if high else ''}"


def _check_destination(destination: object) -> str:
    if not isinstance(destination, str) or _DESTINATION.fullmatch(destination) is None:
        raise ValueError("destination must be a plain queue name")
    return destination


def _check_headers(headers: object) -> dict[str, str]:
    if not isinstance(headers, dict) or len(headers) > MAX_HEADERS:
        raise ValueError(f"headers must be an object of at most {MAX_HEADERS} entries")
    for key, value in headers.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("headers must map text to text")
        if not key or len(key) > MAX_HEADER_KEY or len(value) > MAX_HEADER_VALUE:
            raise ValueError("a header is too long")
    return dict(headers)


class Broker:
    """The queue logic over a Redis client (the HTTP layer is :func:`broker_routes`)."""

    def __init__(self, redis: Any, settings: BrokerSettings) -> None:
        self._r = redis
        self._settings = settings
        self._groups: set[str] = set()

    # ------------------------------------------------------------------ publish

    async def publish(self, destination: str, body: bytes, headers: dict[str, str]) -> str:
        """Store one Message and return its id, after Redis has it."""
        _check_destination(destination)
        if len(body) > MAX_MESSAGE_BYTES:
            raise MessageTooLarge("message too large")
        headers = _check_headers(headers)
        high = headers.pop(PRIORITY_HEADER, None) == PRIORITY_HIGH
        key = stream_key(destination, high)
        entry = {
            "b": base64.b64encode(body).decode("ascii"),
            "h": json.dumps(headers, separators=(",", ":")),
            "dc": "0",
        }
        entry_id = await self._r.xadd(key, entry)
        if destination.startswith("reply."):
            await self._r.expire(key, REPLY_TTL_SECONDS)
        return str(entry_id)

    # ------------------------------------------------------------------ pull

    async def pull(self, destination: str, consumer: str, max_items: int, wait: float) -> list[Delivered]:
        """Up to ``max_items`` Messages for ``consumer``; with ``wait`` seconds it long-polls until one arrives."""
        _check_destination(destination)
        if _CONSUMER.fullmatch(consumer) is None:
            raise ValueError("consumer must be a plain name")
        if not 1 <= max_items <= MAX_BATCH or not 0 <= wait <= MAX_WAIT:
            raise ValueError("max or wait is out of range")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait
        while True:
            taken = await self._take(destination, consumer, max_items)
            remaining = deadline - loop.time()
            if taken or remaining <= 0:
                return taken
            await self._wait_for_news(destination, min(remaining, BLOCK_SLICE))

    async def _take(self, destination: str, consumer: str, want: int) -> list[Delivered]:
        out: list[Delivered] = []
        for high in (True, False):  # high priority first
            if len(out) >= want:
                break
            key = stream_key(destination, high)
            room = want - len(out)
            out += await self._with_group(key, lambda: self._take_from(destination, key, high, consumer, room))
        return out

    async def _with_group(self, key: str, action: Any) -> Any:
        if key not in self._groups:
            await self._create_group(key)
        try:
            return await action()
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            self._groups.discard(key)  # the stream expired or was deleted: start again
            await self._create_group(key)
            return await action()

    async def _create_group(self, key: str) -> None:
        try:
            await self._r.xgroup_create(key, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._groups.add(key)

    async def _take_from(self, destination: str, key: str, high: bool, consumer: str, want: int) -> list[Delivered]:
        tier = "h" if high else "n"
        out: list[Delivered] = []
        idle_ms = max(1, int(self._settings.visibility_timeout * 1000))
        claimed = await self._r.xautoclaim(key, GROUP, consumer, min_idle_time=idle_ms, start_id="0-0", count=want)
        for entry_id, fields in claimed[1]:
            if not fields:
                continue
            times = await self._times_delivered(key, entry_id)
            returned = max(0, times - 1) + _int(fields.get("dc"))
            if self._exceeds_limit(destination, returned):
                await self._dead_letter(destination, key, entry_id, fields, "delivery_limit", returned)
                continue
            out.append(self._delivered(tier, entry_id, fields, returned))
        room = want - len(out)
        if room > 0:
            read = await self._r.xreadgroup(GROUP, consumer, {key: ">"}, count=room)
            for _, entries in read or []:
                for entry_id, fields in entries:
                    out.append(self._delivered(tier, entry_id, fields, _int(fields.get("dc"))))
        return out

    async def _times_delivered(self, key: str, entry_id: str) -> int:
        rows = await self._r.xpending_range(key, GROUP, min=entry_id, max=entry_id, count=1)
        return int(rows[0]["times_delivered"]) if rows else 1

    @staticmethod
    def _delivered(tier: str, entry_id: str, fields: dict[str, str], returned: int) -> Delivered:
        try:
            parsed = json.loads(fields.get("h") or "{}")
            headers = {str(k): str(v) for k, v in parsed.items()} if isinstance(parsed, dict) else {}
        except ValueError:
            headers = {}
        return Delivered(f"{tier}:{entry_id}", base64.b64decode(fields.get("b", "")), headers, returned)

    async def _wait_for_news(self, destination: str, seconds: float) -> None:
        """Block until something is added to either stream of ``destination``, or ``seconds`` pass."""
        keys = {stream_key(destination, True): "$", stream_key(destination, False): "$"}
        await self._r.xread(keys, block=max(1, int(seconds * 1000)))

    # ------------------------------------------------------------------ settle

    def _exceeds_limit(self, destination: str, returned: int) -> bool:
        return destination.startswith("jobs/") and returned > self._settings.delivery_limit

    async def settle(self, destination: str, delivery_id: str, outcome: str) -> bool:
        """Settle one delivery; ``False`` when it was already settled or lost to another consumer."""
        _check_destination(destination)
        parsed = _DELIVERY_ID.fullmatch(delivery_id) if isinstance(delivery_id, str) else None
        if parsed is None or outcome not in _OUTCOMES:
            raise ValueError("unknown delivery or outcome")
        tier, entry_id = parsed.groups()
        key = stream_key(destination, tier == "h")
        if outcome == "ack":
            async with self._r.pipeline(transaction=True) as pipe:
                pipe.xack(key, GROUP, entry_id)
                pipe.xdel(key, entry_id)
                acked, _ = await pipe.execute()
            return bool(acked)
        rows = await self._r.xrange(key, entry_id, entry_id)
        pending = await self._r.xpending_range(key, GROUP, min=entry_id, max=entry_id, count=1)
        if not rows or not pending:
            return False
        fields = rows[0][1]
        returned = max(0, int(pending[0]["times_delivered"]) - 1) + _int(fields.get("dc"))
        if outcome == "reject":
            await self._dead_letter(destination, key, entry_id, fields, "rejected", returned)
            return True
        returned += 1
        if self._exceeds_limit(destination, returned):
            await self._dead_letter(destination, key, entry_id, fields, "delivery_limit", returned)
            return True
        async with self._r.pipeline(transaction=True) as pipe:  # the copy and the settling of the old one, together
            pipe.xadd(key, {**fields, "dc": str(returned)})
            pipe.xack(key, GROUP, entry_id)
            pipe.xdel(key, entry_id)
            await pipe.execute()
        return True

    async def _dead_letter(
        self, destination: str, key: str, entry_id: str, fields: dict[str, str], reason: str, returned: int
    ) -> None:
        """Copy to every dead-letter destination (never from one) and settle the original, in one transaction."""
        async with self._r.pipeline(transaction=True) as pipe:
            if destination not in self._settings.dead_letter_destinations:
                try:
                    headers = json.loads(fields.get("h") or "{}")
                except ValueError:
                    headers = {}
                headers = {
                    **(headers if isinstance(headers, dict) else {}),
                    "x-death-reason": reason,
                    "x-original-destination": destination,
                    "x-delivery-count": str(returned),
                }
                dead = {"b": fields.get("b", ""), "h": json.dumps(headers, separators=(",", ":")), "dc": "0"}
                for target in self._settings.dead_letter_destinations:
                    pipe.xadd(stream_key(target), dead)
            pipe.xack(key, GROUP, entry_id)
            pipe.xdel(key, entry_id)
            await pipe.execute()

    # ------------------------------------------------------------------ depth

    async def depth(self, destination: str) -> int:
        """Messages waiting in ``destination`` that no consumer holds (``QueueInspector``)."""
        _check_destination(destination)
        total = 0
        for high in (True, False):
            key = stream_key(destination, high)
            length = await self._r.xlen(key)
            try:
                pending = int((await self._r.xpending(key, GROUP))["pending"])
            except ResponseError:  # no group yet: nothing is pending
                pending = 0
            total += max(0, length - pending)
        return total

    async def ping(self) -> None:
        await self._r.ping()


def _int(value: object) -> int:
    try:
        return max(0, int(str(value)))
    except ValueError:
        return 0


# --------------------------------------------------------------------------- the HTTP layer


def _error(status: int, code: str) -> JSONResponse:
    return JSONResponse({"error": code}, status_code=status)


def broker_routes(redis: Any, settings: BrokerSettings) -> list[Route]:
    """The five ``POST`` routes under ``/broker/v1`` (``publish``, ``pull``, ``settle``, ``depth``, ``ping``)."""
    broker = Broker(redis, settings)
    token = settings.token.encode()

    def authorized(request: Request) -> bool:
        header = request.headers.get("authorization", "")
        supplied = header[len("Bearer "):] if header.startswith("Bearer ") else ""
        # Compared even when the header is missing or malformed, so every request does the same work.
        return hmac.compare_digest(supplied.encode(), token) and bool(supplied)

    async def read_json(request: Request) -> dict[str, Any]:
        declared = request.headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > MAX_REQUEST_BYTES:
            raise MessageTooLarge("request too large")
        raw = bytearray()
        async for chunk in request.stream():
            raw += chunk
            if len(raw) > MAX_REQUEST_BYTES:
                raise MessageTooLarge("request too large")
        try:
            body = json.loads(raw)
        except (ValueError, RecursionError):
            raise ValueError("not JSON") from None
        if not isinstance(body, dict):
            raise ValueError("not an object")
        return body

    def handler(action: Any) -> Any:
        async def endpoint(request: Request) -> Response:
            if not authorized(request):
                return Response(status_code=401)
            try:
                return JSONResponse(await action(request))
            except MessageTooLarge:
                return _error(413, "too_large")
            except ValueError:
                return _error(400, "bad_request")
            except (RedisError, OSError):
                log.warning("broker store unavailable", exc_info=True)
                return _error(503, "store_unavailable")

        return endpoint

    async def publish(request: Request) -> dict[str, Any]:
        body = await read_json(request)
        text = body.get("body")
        if not isinstance(text, str):
            raise ValueError("body must be text")
        if len(text) > (MAX_MESSAGE_BYTES + 2) // 3 * 4:
            raise MessageTooLarge("message too large")
        try:
            data = base64.b64decode(text, validate=True)
        except (binascii.Error, ValueError):
            raise ValueError("body is not base64") from None
        return {"id": await broker.publish(body.get("destination"), data, body.get("headers", {}))}  # type: ignore[arg-type]

    async def pull(request: Request) -> dict[str, Any]:
        body = await read_json(request)
        max_items, wait = body.get("max"), body.get("wait", 0)
        if isinstance(max_items, bool) or not isinstance(max_items, int):
            raise ValueError("max must be a number")
        if isinstance(wait, bool) or not isinstance(wait, int | float) or not 0 <= wait <= MAX_WAIT:
            raise ValueError("wait must be 0 to 30")
        consumer = body.get("consumer")
        if not isinstance(consumer, str):
            raise ValueError("consumer must be text")
        items = await broker.pull(body.get("destination"), consumer, max_items, float(wait))  # type: ignore[arg-type]
        return {
            "deliveries": [
                {
                    "id": i.id,
                    "body": base64.b64encode(i.body).decode("ascii"),
                    "headers": i.headers,
                    "delivery_count": i.delivery_count,
                }
                for i in items
            ]
        }

    async def settle(request: Request) -> dict[str, Any]:
        body = await read_json(request)
        done = await broker.settle(body.get("destination"), body.get("id"), body.get("outcome"))  # type: ignore[arg-type]
        return {"ok": done}

    async def depth(request: Request) -> dict[str, Any]:
        body = await read_json(request)
        return {"ready": await broker.depth(body.get("destination"))}  # type: ignore[arg-type]

    async def ping(request: Request) -> dict[str, Any]:
        await broker.ping()
        return {"ok": True}

    actions = (("publish", publish), ("pull", pull), ("settle", settle), ("depth", depth), ("ping", ping))
    return [Route(f"/broker/v1/{name}", handler(action), methods=["POST"]) for name, action in actions]


__all__ = [
    "KEY_PREFIX",
    "MAX_BATCH",
    "MAX_MESSAGE_BYTES",
    "MAX_WAIT",
    "REPLY_TTL_SECONDS",
    "Broker",
    "BrokerSettings",
    "Delivered",
    "MessageTooLarge",
    "broker_routes",
    "stream_key",
]
