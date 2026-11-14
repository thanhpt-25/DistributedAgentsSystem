"""HttpTransport beyond the shared contract (das-scale-resilience task 9.3; Phase 7 Req 2.1, 2.4, 2.6 to 2.9): what it
raises when the broker fails, that it puts the codec's bytes on the wire, that the reserved headers never reach a
handler, that a message a handler could not settle comes back, and how it is built."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from starlette.applications import Starlette

from das.codec import MessageCodec
from das.transport.base import (
    Delivery, Outcome, PublishNotConfirmed, ReconnectPolicy, RequestTimeout, SubscribeOptions, TransportError,
    TransportUnavailable,
)
from das.transport.http import CORRELATION_HEADER, REPLY_TO_HEADER, HttpTransport
from das.transport.httpbroker import BrokerSettings, broker_routes
from tests.transport_contract import Recorder, job, until

TOKEN = "http-token-0123456789abcdef"


class Net(httpx.AsyncBaseTransport):
    """The broker's ASGI app, with a hook to answer a path with a chosen status or to be unreachable."""

    def __init__(self, app: Starlette) -> None:
        self.inner = httpx.ASGITransport(app=app)
        self.fail: dict[str, int | Exception] = {}
        self.seen: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.rsplit("/", 1)[-1]
        self.seen.append(path)
        outcome = self.fail.get(path)
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, int):
            return httpx.Response(outcome, json={"error": "scripted"})
        return await self.inner.handle_async_request(request)


@pytest.fixture
async def redis() -> AsyncIterator[FakeRedis]:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def net(redis: FakeRedis) -> Net:
    settings = BrokerSettings(token=TOKEN, visibility_timeout=0.3, delivery_limit=3)
    return Net(Starlette(routes=broker_routes(redis, settings)))


async def make(net: Net, agent_id: str = "agent", **extra: Any) -> HttpTransport:
    async def quick(seconds: float) -> None:
        await asyncio.sleep(0.05)

    transport = HttpTransport(
        "http://broker", extra.pop("token", TOKEN), agent_id=agent_id, poll_wait=0.2, sleep=quick,
        client=httpx.AsyncClient(transport=net), reconnect_policy=ReconnectPolicy(1.0), **extra,
    )
    await transport.start()
    return transport


@pytest.fixture
async def transport(net: Net) -> AsyncIterator[HttpTransport]:
    t = await make(net)
    yield t
    await t.close()


# --------------------------------------------------------------------------- construction


@pytest.mark.parametrize("kwargs", [
    {"url": "ftp://x", "token": "t"}, {"url": "broker", "token": "t"}, {"url": "http://x", "token": ""},
])
def test_a_bad_url_or_an_empty_token_is_refused(kwargs: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        HttpTransport(**kwargs)


@pytest.mark.parametrize("wait", [0, -1, 30.5, 1000])
def test_the_poll_wait_must_be_above_0_and_at_most_30_seconds(wait: float) -> None:
    with pytest.raises(ValueError, match="poll_wait"):
        HttpTransport("http://x", "t", poll_wait=wait)


def test_a_trailing_slash_on_the_url_is_dropped() -> None:
    assert HttpTransport("https://gateway:8081/", "t").url == "https://gateway:8081"


# --------------------------------------------------------------------------- start and the token


async def test_starting_against_a_broker_that_is_not_there_is_transport_unavailable(net: Net) -> None:
    net.fail["ping"] = httpx.ConnectError("refused")
    t = HttpTransport("http://broker", TOKEN, client=httpx.AsyncClient(transport=net))
    with pytest.raises(TransportUnavailable, match="cannot connect"):
        await t.start()
    assert t.connected is False


async def test_starting_with_the_wrong_token_is_unavailable_and_the_token_is_not_in_the_message(net: Net) -> None:
    t = HttpTransport("http://broker", "wrong-token-0123456789", client=httpx.AsyncClient(transport=net))
    with pytest.raises(TransportUnavailable) as info:
        await t.start()
    assert "wrong-token-0123456789" not in str(info.value)


async def test_operations_before_start_raise_unavailable(net: Net) -> None:
    t = HttpTransport("http://broker", TOKEN, client=httpx.AsyncClient(transport=net))
    with pytest.raises(TransportUnavailable):
        await t.publish("jobs/x", job(1))
    with pytest.raises(TransportUnavailable):
        await t.subscribe("jobs/x", Recorder(t))


async def test_starting_twice_is_harmless(transport: HttpTransport, net: Net) -> None:
    before = len(net.seen)
    await transport.start()
    assert len(net.seen) == before


# --------------------------------------------------------------------------- publish (Req 2.4)


async def test_a_broker_error_status_is_publish_not_confirmed_naming_the_destination(
    net: Net, transport: HttpTransport
) -> None:
    net.fail["publish"] = 503
    with pytest.raises(PublishNotConfirmed) as info:
        await transport.publish("jobs/x", job(1))
    assert info.value.destination == "jobs/x" and "503" in info.value.reason
    assert transport.connected is True  # a refusal is not a lost connection


async def test_a_message_over_the_broker_limit_is_publish_not_confirmed(net: Net, transport: HttpTransport) -> None:
    net.fail["publish"] = 413
    with pytest.raises(PublishNotConfirmed, match="413"):
        await transport.publish("jobs/x", job(1))


async def test_an_unreachable_broker_is_transport_unavailable_and_closes_the_gate(
    net: Net, transport: HttpTransport
) -> None:
    net.fail["publish"] = httpx.ConnectError("refused")
    with pytest.raises(TransportUnavailable):
        await transport.publish("jobs/x", job(1))
    assert transport.connected is False and transport.registry.is_open is False
    net.fail.clear()
    await until(lambda: transport.connected, timeout=5)
    assert transport.registry.is_open


async def test_a_timeout_is_a_lost_connection_too(net: Net, transport: HttpTransport) -> None:
    net.fail["publish"] = httpx.ReadTimeout("slow")
    with pytest.raises(TransportUnavailable):
        await transport.publish("jobs/x", job(1))
    assert transport.connected is False


async def test_a_wrong_token_after_start_is_unavailable_and_says_so(net: Net, transport: HttpTransport) -> None:
    net.fail["publish"] = 401
    with pytest.raises(TransportUnavailable, match="token"):
        await transport.publish("jobs/x", job(1))


async def test_non_text_headers_are_a_type_error_and_nothing_is_sent(net: Net, transport: HttpTransport) -> None:
    before = len(net.seen)
    with pytest.raises(TypeError):
        await transport.publish("jobs/x", job(1), headers={"k": 1})  # type: ignore[dict-item]
    assert len(net.seen) == before


# --------------------------------------------------------------------------- the bytes on the wire (Req 2.7)


async def test_the_stored_bytes_are_exactly_the_codecs_encoding(
    net: Net, transport: HttpTransport, redis: FakeRedis
) -> None:
    message = job(5)
    await transport.publish("jobs/x", message)
    [(_, fields)] = await redis.xrange("das:broker:das.jobs.x")
    assert base64.b64decode(fields["b"]) == MessageCodec().encode(message)


async def test_the_priority_header_is_consumed_by_the_broker_and_never_stored(
    net: Net, transport: HttpTransport, redis: FakeRedis
) -> None:
    await transport.publish("jobs/x", job(1), headers={"das-priority": "high", "k": "v"})
    assert await redis.xlen("das:broker:das.jobs.x:high") == 1 and await redis.xlen("das:broker:das.jobs.x") == 0
    [(_, fields)] = await redis.xrange("das:broker:das.jobs.x:high")
    assert "das-priority" not in fields["h"]


# --------------------------------------------------------------------------- subscribing and settling


async def test_a_handler_that_raises_leaves_the_message_to_be_redelivered_after_the_visibility_timeout(
    net: Net, transport: HttpTransport
) -> None:
    calls: list[Delivery] = []

    async def flaky(delivery: Delivery) -> None:
        calls.append(delivery)
        if len(calls) == 1:
            raise RuntimeError("boom")
        await transport.acknowledge(delivery)

    await transport.subscribe("jobs/x", flaky)
    await transport.publish("jobs/x", job(1))
    await until(lambda: len(calls) == 2)
    assert [c.delivery_count for c in calls] == [0, 1] and len(transport.handler_errors) == 1


async def test_a_handler_that_raises_gives_up_its_slot(net: Net, transport: HttpTransport) -> None:
    seen: list[str] = []

    async def always_fails(delivery: Delivery) -> None:
        seen.append(delivery.message.payload.job_id)  # type: ignore[union-attr]
        raise RuntimeError("boom")

    await transport.subscribe("jobs/x", always_fails, SubscribeOptions(max_in_flight=1))
    await transport.publish("jobs/x", job(1))
    await transport.publish("jobs/x", job(2))
    await until(lambda: "j2" in seen)  # the slot of the first was given up, so the second could be delivered


async def test_settling_twice_sends_one_settle_and_the_second_is_a_no_op(net: Net, transport: HttpTransport) -> None:
    seen = Recorder(transport)
    await transport.subscribe("jobs/x", seen)
    await transport.publish("jobs/x", job(1))
    await until(lambda: len(seen.deliveries) == 1)
    settles = net.seen.count("settle")
    await transport.acknowledge(seen.deliveries[0])
    assert net.seen.count("settle") == settles


async def test_a_delivery_from_another_transport_is_refused(net: Net, transport: HttpTransport) -> None:
    foreign = Delivery(None, "jobs/x", False, 0, {}, token=object())
    with pytest.raises(ValueError, match="not produced"):
        await transport.acknowledge(foreign)


async def test_a_message_that_cannot_be_decoded_is_delivered_with_its_parse_error(
    net: Net, transport: HttpTransport, redis: FakeRedis
) -> None:
    await redis.xadd("das:broker:das.jobs.x", {"b": base64.b64encode(b"not a message").decode(), "h": "{}", "dc": "0"})
    seen = Recorder(transport, Outcome.REJECT)
    await transport.subscribe("jobs/x", seen)
    await until(lambda: len(seen.deliveries) == 1)
    [delivery] = seen.deliveries
    assert delivery.message is None and delivery.parse_error is not None and delivery.body == b"not a message"


async def test_a_shared_limiter_caps_the_slots_across_subscriptions(net: Net, transport: HttpTransport) -> None:
    class Limiter:
        def __init__(self, size: int) -> None:
            self.free = size
            self.changed = asyncio.Event()

        async def acquire(self) -> None:
            while self.free < 1:
                self.changed.clear()
                await self.changed.wait()
            self.free -= 1

        def try_acquire(self) -> bool:
            if self.free < 1:
                return False
            self.free -= 1
            return True

        def release(self) -> None:
            self.free += 1
            self.changed.set()

    limiter = Limiter(2)
    held: list[Delivery] = []

    async def hold(delivery: Delivery) -> None:
        held.append(delivery)

    options = SubscribeOptions(limiter=limiter)
    await transport.subscribe("jobs/a", hold, options)
    await transport.subscribe("jobs/b", hold, options)
    for n in range(4):
        await transport.publish("jobs/a" if n % 2 else "jobs/b", job(n))
    await until(lambda: len(held) == 2)
    await asyncio.sleep(0.5)
    assert len(held) == 2, "two slots in all, whichever subscription takes them"
    await transport.acknowledge(held[0])
    await until(lambda: len(held) == 3)


async def test_cancelling_a_subscription_stops_its_deliveries(net: Net, transport: HttpTransport) -> None:
    seen = Recorder(transport)
    subscription = await transport.subscribe("jobs/x", seen)
    await subscription.cancel()
    await transport.publish("jobs/x", job(1))
    await asyncio.sleep(0.5)
    assert seen.deliveries == [] and subscription.active is False


# --------------------------------------------------------------------------- request-reply (Req 2.8)


async def test_the_reserved_headers_never_reach_a_handler(net: Net, transport: HttpTransport) -> None:
    other = await make(net, "responder")
    headers: list[dict[str, str]] = []

    async def answer(delivery: Delivery) -> Any:
        headers.append(dict(delivery.headers))
        return job(99)

    await other.subscribe("control/ask", answer)
    await transport.request_reply("control/ask", job(1), timeout=5)
    assert headers and REPLY_TO_HEADER not in headers[0] and CORRELATION_HEADER not in headers[0]
    await other.close()


async def test_each_call_gets_the_reply_to_its_own_request(net: Net, transport: HttpTransport) -> None:
    other = await make(net, "responder")

    async def echo(delivery: Delivery) -> Any:
        return job(int(delivery.message.payload.job_id[1:]) + 100)  # type: ignore[union-attr]

    await other.subscribe("control/ask", echo)
    replies = await asyncio.gather(*(transport.request_reply("control/ask", job(n), timeout=5) for n in (1, 2, 3)))
    assert [r.payload.job_id for r in replies] == ["j101", "j102", "j103"]
    await other.close()


async def test_request_reply_timeout_is_a_request_timeout_not_a_hang(net: Net, transport: HttpTransport) -> None:
    started = asyncio.get_running_loop().time()
    with pytest.raises(RequestTimeout):
        await transport.request_reply("control/nobody", job(1), timeout=1.0)
    assert asyncio.get_running_loop().time() - started < 3.0


async def test_an_unreadable_reply_is_a_transport_error(net: Net, transport: HttpTransport, redis: FakeRedis) -> None:
    other = await make(net, "responder")

    async def garble(delivery: Delivery) -> None:
        async for key in redis.scan_iter("das:broker:das.reply.*"):
            await redis.xadd(key, {"b": base64.b64encode(b"junk").decode(), "h": "{}", "dc": "0"})

    await other.subscribe("control/ask", garble)
    with pytest.raises(TransportError, match="unreadable"):
        await transport.request_reply("control/ask", job(1), timeout=5)
    await other.close()


# --------------------------------------------------------------------------- depth, close, config


async def test_ready_count_reports_what_waits_in_the_queue(net: Net, transport: HttpTransport) -> None:
    for n in range(3):
        await transport.publish("jobs/x", job(n))
    assert await transport.ready_count("jobs/x") == 3 and await transport.ready_count("jobs/none") == 0


async def test_closing_stops_every_task_and_further_publishes_raise(net: Net) -> None:
    t = await make(net)
    await t.subscribe("jobs/x", Recorder(t))
    await t.close()
    assert not t._tasks and not t._pollers
    with pytest.raises(TransportUnavailable):
        await t.publish("jobs/x", job(1))


async def test_an_injected_client_is_not_closed_by_the_transport(net: Net) -> None:
    client = httpx.AsyncClient(transport=net)
    t = HttpTransport("http://broker", TOKEN, client=client)
    await t.start()
    await t.close()
    assert client.is_closed is False
    await client.aclose()


def test_from_config_reads_the_http_section_and_the_limits() -> None:
    from pydantic import SecretStr

    class Section:
        url, token, poll_wait = "http://gateway:8081", SecretStr("cfg-token-0123456789"), 12.0

    class Limits:
        max_message_bytes, request_reply_timeout, max_reconnect_interval = 2_097_152, 45.0, 20.0

    class Config:
        http, limits, agent_id = Section(), Limits(), "worker-7"

    t = HttpTransport.from_config(Config())
    assert (t.url, t.agent_id, t.poll_wait, t.request_reply_timeout) == ("http://gateway:8081", "worker-7", 12.0, 45.0)
    assert t.reconnect_policy.max_interval == 20.0


@pytest.mark.parametrize("missing", ["section", "token"])
def test_from_config_without_the_section_or_the_token_is_a_value_error(missing: str) -> None:
    class Section:
        url, token, poll_wait = "http://gateway:8081", None, 12.0

    class Limits:
        max_message_bytes, request_reply_timeout, max_reconnect_interval = 1_048_576, 30.0, 30.0

    class Config:
        http = None if missing == "section" else Section()
        limits, agent_id = Limits(), "a"

    with pytest.raises(ValueError):
        HttpTransport.from_config(Config())


# --------------------------------------------------------------------------- tests added after the fault-injection pass


async def test_a_broker_answer_of_exactly_400_is_a_refusal(net: Net, transport: HttpTransport) -> None:
    net.fail["publish"] = 400
    with pytest.raises(PublishNotConfirmed, match="400"):
        await transport.publish("jobs/x", job(1))


def test_a_poll_wait_of_exactly_30_seconds_is_allowed() -> None:
    assert HttpTransport("http://x", "t", poll_wait=30).poll_wait == 30.0


async def test_a_client_the_transport_made_itself_is_closed_on_close(net: Net, monkeypatch: pytest.MonkeyPatch) -> None:
    client = httpx.AsyncClient(transport=net)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: client)
    t = HttpTransport("http://broker", TOKEN)
    await t.start()
    await t.close()
    assert client.is_closed
    monkeypatch.undo()
    await client.aclose()


async def test_a_stray_reply_for_another_call_is_skipped_and_every_message_is_settled(
    net: Net, transport: HttpTransport, redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from das.transport.httpbroker import stream_key

    ids = iter(["a" * 32, "b" * 32])  # the reply destination, then the correlation id
    monkeypatch.setattr("das.transport.http.uuid", SimpleNamespace(uuid4=lambda: SimpleNamespace(hex=next(ids))))
    reply_key = stream_key("reply." + "a" * 32)
    stray = {"b": base64.b64encode(MessageCodec().encode(job(77))).decode(), "h": '{"das-correlation-id":"other"}',
             "dc": "0"}
    await redis.xadd(reply_key, stray)
    other = await make(net, "responder")

    async def answer(delivery: Delivery) -> Any:
        return job(1)

    await other.subscribe("control/ask", answer)
    reply = await transport.request_reply("control/ask", job(5), timeout=5)
    assert reply.payload.job_id == "j1"  # type: ignore[union-attr]  # not the stray j77
    for _ in range(40):  # the responder settles the request just after replying
        if await redis.xlen(stream_key("control/ask")) == 0:
            break
        await asyncio.sleep(0.1)
    assert await redis.xlen(reply_key) == 0, "the stray and the real reply were both settled"
    assert await redis.xlen(stream_key("control/ask")) == 0, "the responder settled the request"
    await other.close()


async def test_polling_pauses_while_the_connection_is_down(net: Net, transport: HttpTransport) -> None:
    await transport.subscribe("jobs/x", Recorder(transport))
    await asyncio.sleep(0.1)
    net.fail["publish"] = httpx.ConnectError("refused")
    net.fail["ping"] = httpx.ConnectError("refused")
    with pytest.raises(TransportUnavailable):
        await transport.publish("jobs/x", job(1))
    await asyncio.sleep(0.5)  # a poll that was already in flight finishes
    pulls = net.seen.count("pull")
    await asyncio.sleep(0.7)
    assert net.seen.count("pull") == pulls


async def test_empty_polls_do_not_use_up_a_slot(net: Net, transport: HttpTransport) -> None:
    seen = Recorder(transport)
    await transport.subscribe("jobs/x", seen, SubscribeOptions(max_in_flight=1))
    await asyncio.sleep(0.7)  # several polls that find nothing
    await transport.publish("jobs/x", job(1))
    await until(lambda: len(seen.deliveries) == 1)


async def test_a_failed_poll_gives_its_slot_back(net: Net, transport: HttpTransport) -> None:
    seen = Recorder(transport)
    await transport.subscribe("jobs/x", seen, SubscribeOptions(max_in_flight=1))
    await asyncio.sleep(0.1)
    net.fail["pull"] = httpx.ConnectError("refused")
    await asyncio.sleep(0.4)
    net.fail.clear()
    await until(lambda: transport.connected, timeout=5)
    await transport.publish("jobs/x", job(1))
    await until(lambda: len(seen.deliveries) == 1)
