"""The HTTP_Broker: durable queues on Redis streams, behind a bearer token (das-scale-resilience task 9.2; Phase 7 Req
2.2, 2.3, 2.5, 2.6, 2.7, 2.10). The queue logic (:class:`Broker`) is tested against fakeredis; the HTTP layer (auth,
limits, answers) through Starlette's ASGI interface with a real httpx client, no sockets."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from starlette.applications import Starlette

import das.transport.httpbroker as httpbroker
from das.transport.base import PRIORITY_HEADER, PRIORITY_HIGH
from das.transport.httpbroker import (
    MAX_BATCH, MAX_MESSAGE_BYTES, MAX_WAIT, Broker, BrokerSettings, broker_routes,
)

TOKEN = "broker-token-0123456789abcdef"


def settings(**extra: Any) -> BrokerSettings:
    return BrokerSettings(token=TOKEN, **{"visibility_timeout": 30.0, "delivery_limit": 2, **extra})


@pytest.fixture
async def redis() -> AsyncIterator[FakeRedis]:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def broker(redis: FakeRedis) -> Broker:
    return Broker(redis, settings())


async def take(broker: Broker, destination: str = "jobs/x", consumer: str = "c1", count: int = 10) -> list[Any]:
    return await broker.pull(destination, consumer, count, 0)


# --------------------------------------------------------------------------- publish and pull (Req 2.3)


async def test_a_published_message_is_pulled_with_its_body_and_headers(broker: Broker) -> None:
    await broker.publish("jobs/x", b"\x00\x01binary\xff", {"k": "v", "k2": "v2"})
    [item] = await take(broker)
    assert item.body == b"\x00\x01binary\xff" and item.headers == {"k": "v", "k2": "v2"}
    assert item.delivery_count == 0 and item.id


async def test_a_message_goes_to_one_of_two_competing_consumers_only(broker: Broker) -> None:
    for n in range(10):
        await broker.publish("jobs/x", f"m{n}".encode(), {})
    first = await broker.pull("jobs/x", "c1", 4, 0)
    second = await broker.pull("jobs/x", "c2", 10, 0)
    bodies = [i.body for i in first] + [i.body for i in second]
    assert sorted(bodies) == sorted(f"m{n}".encode() for n in range(10)) and len(set(bodies)) == 10


async def test_messages_keep_their_order_for_one_consumer(broker: Broker) -> None:
    for n in range(5):
        await broker.publish("jobs/x", str(n).encode(), {})
    assert [i.body for i in await take(broker)] == [b"0", b"1", b"2", b"3", b"4"]


async def test_pull_returns_at_most_max_items(broker: Broker) -> None:
    for n in range(5):
        await broker.publish("jobs/x", str(n).encode(), {})
    assert len(await broker.pull("jobs/x", "c1", 2, 0)) == 2
    assert len(await broker.pull("jobs/x", "c1", 10, 0)) == 3


async def test_an_empty_destination_pulls_nothing_and_a_wait_returns_after_the_wait(broker: Broker) -> None:
    assert await take(broker) == []
    started = asyncio.get_running_loop().time()
    assert await broker.pull("jobs/x", "c1", 5, 0.3) == []
    assert asyncio.get_running_loop().time() - started >= 0.25


async def test_a_pull_that_waits_returns_as_soon_as_a_message_arrives(broker: Broker) -> None:
    async def later() -> None:
        await asyncio.sleep(0.1)
        await broker.publish("jobs/x", b"late", {})

    task = asyncio.create_task(later())
    started = asyncio.get_running_loop().time()
    items = await broker.pull("jobs/x", "c1", 5, 5.0)
    await task
    assert [i.body for i in items] == [b"late"] and asyncio.get_running_loop().time() - started < 2.0


async def test_destinations_are_separate_queues(broker: Broker) -> None:
    await broker.publish("jobs/x", b"x", {})
    await broker.publish("jobs/y", b"y", {})
    await broker.publish("results", b"r", {})
    assert [i.body for i in await take(broker, "jobs/y")] == [b"y"]
    assert [i.body for i in await take(broker, "results")] == [b"r"]
    assert [i.body for i in await take(broker, "jobs/x")] == [b"x"]


async def test_messages_published_before_anyone_subscribed_are_still_delivered(broker: Broker) -> None:
    await broker.publish("jobs/late", b"early bird", {})
    assert [i.body for i in await take(broker, "jobs/late")] == [b"early bird"]


# --------------------------------------------------------------------------- settling (Req 2.6)


async def test_an_acknowledged_message_is_gone_for_good(broker: Broker, redis: FakeRedis) -> None:
    await broker.publish("jobs/x", b"once", {})
    [item] = await take(broker)
    assert await broker.settle("jobs/x", item.id, "ack") is True
    assert await take(broker) == [] and await redis.xlen("das:broker:das.jobs.x") == 0


async def test_settling_twice_is_not_an_error_and_says_so(broker: Broker) -> None:
    await broker.publish("jobs/x", b"once", {})
    [item] = await take(broker)
    assert await broker.settle("jobs/x", item.id, "ack") is True
    assert await broker.settle("jobs/x", item.id, "ack") is False


async def test_a_requeued_message_comes_back_with_a_higher_delivery_count(broker: Broker) -> None:
    await broker.publish("jobs/x", b"again", {})
    [first] = await take(broker)
    assert await broker.settle("jobs/x", first.id, "requeue") is True
    [second] = await take(broker)
    assert second.body == b"again" and second.delivery_count == 1 and second.id != first.id
    await broker.settle("jobs/x", second.id, "requeue")
    [third] = await take(broker)
    assert third.delivery_count == 2


async def test_a_rejected_message_is_dead_lettered_with_its_reason_and_origin(broker: Broker) -> None:
    await broker.publish("jobs/x", b"bad", {"k": "v"})
    [item] = await take(broker)
    assert await broker.settle("jobs/x", item.id, "reject") is True
    assert await take(broker) == []
    for destination in ("dlq", "dlq-events"):
        [dead] = await take(broker, destination)
        assert dead.body == b"bad"
        assert dead.headers["x-death-reason"] == "rejected" and dead.headers["x-original-destination"] == "jobs/x"
        assert dead.headers["k"] == "v" and dead.headers["x-delivery-count"] == "0"


async def test_a_message_returned_more_than_the_limit_is_dead_lettered_as_delivery_limit(broker: Broker) -> None:
    await broker.publish("jobs/x", b"loop", {})  # delivery_limit is 2: it may be returned twice and delivered again
    for _ in range(2):
        [item] = await take(broker)
        await broker.settle("jobs/x", item.id, "requeue")
    [item] = await take(broker)
    assert item.delivery_count == 2
    await broker.settle("jobs/x", item.id, "requeue")  # the third return passes the limit
    assert await take(broker) == []
    [dead] = await take(broker, "dlq-events")
    assert dead.headers["x-death-reason"] == "delivery_limit" and dead.headers["x-original-destination"] == "jobs/x"
    assert dead.headers["x-delivery-count"] == "3"


async def test_a_dead_letter_is_never_dead_lettered_again(broker: Broker) -> None:
    await broker.publish("dlq", b"x", {})
    [item] = await take(broker, "dlq")
    await broker.settle("dlq", item.id, "reject")
    assert await take(broker, "dlq") == [] and await take(broker, "dlq-events") == []


async def test_the_delivery_limit_applies_to_jobs_only(broker: Broker) -> None:
    await broker.publish("results", b"r", {})
    for _ in range(6):
        [item] = await take(broker, "results")
        await broker.settle("results", item.id, "requeue")
    assert [i.delivery_count for i in await take(broker, "results")] == [6]


# --------------------------------------------------------------------------- not acknowledged in time (Req 2.5)


async def test_an_unacknowledged_message_becomes_deliverable_again_after_the_visibility_timeout(
    redis: FakeRedis,
) -> None:
    broker = Broker(redis, settings(visibility_timeout=0.1))
    await broker.publish("jobs/x", b"slow", {})
    [first] = await broker.pull("jobs/x", "c1", 5, 0)
    assert await broker.pull("jobs/x", "c2", 5, 0) == []  # still with c1
    await asyncio.sleep(0.15)
    [again] = await broker.pull("jobs/x", "c2", 5, 0)
    assert again.id == first.id and again.body == b"slow" and again.delivery_count == 1


async def test_a_message_that_keeps_timing_out_is_dead_lettered_after_the_limit(redis: FakeRedis) -> None:
    broker = Broker(redis, settings(visibility_timeout=0.05, delivery_limit=1))
    await broker.publish("jobs/x", b"stuck", {})
    seen = []
    for _ in range(4):
        seen += await broker.pull("jobs/x", "c1", 5, 0)
        await asyncio.sleep(0.08)
    assert [i.delivery_count for i in seen] == [0, 1]  # delivered, delivered once more, then dead-lettered
    [dead] = await take(broker, "dlq-events")
    assert dead.headers["x-death-reason"] == "delivery_limit" and dead.body == b"stuck"


# --------------------------------------------------------------------------- priority (Req 2.7)


async def test_a_high_priority_message_is_delivered_before_normal_ones_queued_earlier(broker: Broker) -> None:
    await broker.publish("jobs/x", b"normal-1", {})
    await broker.publish("jobs/x", b"normal-2", {})
    await broker.publish("jobs/x", b"urgent", {PRIORITY_HEADER: PRIORITY_HIGH})
    assert [i.body for i in await take(broker)] == [b"urgent", b"normal-1", b"normal-2"]


async def test_the_priority_header_is_consumed_not_delivered(broker: Broker) -> None:
    await broker.publish("jobs/x", b"urgent", {PRIORITY_HEADER: PRIORITY_HIGH, "k": "v"})
    [item] = await take(broker)
    assert item.headers == {"k": "v"}


async def test_any_other_priority_value_is_normal(broker: Broker) -> None:
    await broker.publish("jobs/x", b"n1", {})
    await broker.publish("jobs/x", b"n2", {PRIORITY_HEADER: "low"})
    assert [i.body for i in await take(broker)] == [b"n1", b"n2"]


async def test_a_high_priority_message_can_be_settled_like_any_other(broker: Broker) -> None:
    await broker.publish("jobs/x", b"urgent", {PRIORITY_HEADER: PRIORITY_HIGH})
    [item] = await take(broker)
    await broker.settle("jobs/x", item.id, "requeue")
    [again] = await take(broker)
    assert again.body == b"urgent" and again.delivery_count == 1
    assert await broker.settle("jobs/x", again.id, "ack") is True


# --------------------------------------------------------------------------- depth and reply destinations


async def test_depth_counts_messages_waiting_not_delivered_ones(broker: Broker) -> None:
    for n in range(4):
        await broker.publish("jobs/x", str(n).encode(), {})
    await broker.publish("jobs/x", b"hi", {PRIORITY_HEADER: PRIORITY_HIGH})
    assert await broker.depth("jobs/x") == 5
    await broker.pull("jobs/x", "c1", 2, 0)
    assert await broker.depth("jobs/x") == 3
    assert await broker.depth("jobs/never-used") == 0


async def test_a_reply_destination_expires_so_an_abandoned_call_leaves_nothing_behind(
    broker: Broker, redis: FakeRedis
) -> None:
    await broker.publish("reply.abc123", b"late reply", {})
    ttl = await redis.ttl("das:broker:das.reply.abc123")
    assert 0 < ttl <= httpbroker.REPLY_TTL_SECONDS
    await broker.publish("jobs/x", b"job", {})
    assert await redis.ttl("das:broker:das.jobs.x") == -1  # an ordinary queue never expires


async def test_destination_names_that_could_reach_other_keys_are_refused(broker: Broker) -> None:
    for bad in ("", "a b", "../x", "x:y", "x\ny", "A" * 200, "jobs/x*", "{x}"):
        with pytest.raises(ValueError):
            await broker.publish(bad, b"x", {})


# --------------------------------------------------------------------------- the HTTP layer (Req 2.2, 2.10)


@pytest.fixture
async def http(redis: FakeRedis) -> AsyncIterator[httpx.AsyncClient]:
    app = Starlette(routes=broker_routes(redis, settings()))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://broker") as client:
        yield client


def auth(token: str = TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


async def publish(
    http: httpx.AsyncClient, destination: str, body: bytes, headers: dict[str, str] | None = None
) -> Any:
    return await http.post(
        "/broker/v1/publish", headers=auth(),
        json={"destination": destination, "body": b64(body), "headers": headers or {}},
    )


async def test_the_three_routes_publish_pull_and_settle_work_end_to_end(http: httpx.AsyncClient) -> None:
    sent = await publish(http, "jobs/x", b"hello", {"k": "v"})
    assert sent.status_code == 200 and sent.json()["id"]
    pulled = await http.post("/broker/v1/pull", headers=auth(),
                             json={"destination": "jobs/x", "consumer": "c1", "max": 5, "wait": 0})
    [item] = pulled.json()["deliveries"]
    assert base64.b64decode(item["body"]) == b"hello" and item["headers"] == {"k": "v"}
    assert item["delivery_count"] == 0
    settled = await http.post("/broker/v1/settle", headers=auth(),
                              json={"destination": "jobs/x", "id": item["id"], "outcome": "ack"})
    assert settled.status_code == 200 and settled.json() == {"ok": True}
    depth = await http.post("/broker/v1/depth", headers=auth(), json={"destination": "jobs/x"})
    assert depth.json() == {"ready": 0}


async def test_ping_answers_for_a_client_that_only_wants_to_know_the_broker_is_there(
    http: httpx.AsyncClient,
) -> None:
    assert (await http.post("/broker/v1/ping", headers=auth())).json() == {"ok": True}


@pytest.mark.parametrize("path", ["publish", "pull", "settle", "depth", "ping"])
async def test_every_route_refuses_a_missing_or_wrong_token_with_401_and_no_detail(
    http: httpx.AsyncClient, path: str
) -> None:
    for headers in ({}, auth("wrong-token"), {"Authorization": TOKEN}, {"Authorization": "Basic abc"},
                    {"Authorization": "Bearer"}, {"Authorization": "Bearer "}, auth(TOKEN + "x"), auth(TOKEN[:-1])):
        response = await http.post(f"/broker/v1/{path}", headers=headers, json={})
        assert response.status_code == 401 and response.content == b"", (path, headers)


async def test_the_token_is_compared_in_constant_time(redis: FakeRedis, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[bytes, bytes]] = []
    real = httpbroker.hmac.compare_digest

    def spy(a: Any, b: Any) -> bool:
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(httpbroker.hmac, "compare_digest", spy)
    app = Starlette(routes=broker_routes(redis, settings()))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://b") as client:
        await client.post("/broker/v1/ping", headers=auth("wrong"))
        await client.post("/broker/v1/ping", headers=auth())
    assert [pair[1] for pair in calls] == [TOKEN.encode(), TOKEN.encode()]


async def test_a_request_without_a_token_does_nothing_even_when_it_is_otherwise_valid(
    http: httpx.AsyncClient, redis: FakeRedis
) -> None:
    response = await http.post("/broker/v1/publish", json={"destination": "jobs/x", "body": b64(b"x"), "headers": {}})
    assert response.status_code == 401 and await redis.exists("das:broker:das.jobs.x") == 0


async def test_only_post_is_accepted(http: httpx.AsyncClient) -> None:
    for method in ("GET", "PUT", "DELETE"):
        assert (await http.request(method, "/broker/v1/ping", headers=auth())).status_code == 405


async def test_a_message_over_one_mebibyte_is_413(http: httpx.AsyncClient) -> None:
    ok = await publish(http, "jobs/x", b"x" * MAX_MESSAGE_BYTES)
    too_big = await publish(http, "jobs/x", b"x" * (MAX_MESSAGE_BYTES + 1))
    assert ok.status_code == 200 and too_big.status_code == 413


async def test_a_request_body_far_over_the_limit_is_413_without_being_read_as_a_message(
    http: httpx.AsyncClient,
) -> None:
    response = await http.post("/broker/v1/publish", headers=auth(), content=b"{" + b" " * (3 * MAX_MESSAGE_BYTES))
    assert response.status_code == 413


@pytest.mark.parametrize("batch", [0, MAX_BATCH + 1, -1, 10**6])
async def test_a_batch_outside_1_to_100_is_400(http: httpx.AsyncClient, batch: int) -> None:
    response = await http.post("/broker/v1/pull", headers=auth(),
                               json={"destination": "jobs/x", "consumer": "c", "max": batch, "wait": 0})
    assert response.status_code == 400


async def test_a_batch_of_exactly_100_is_accepted(http: httpx.AsyncClient) -> None:
    response = await http.post("/broker/v1/pull", headers=auth(),
                               json={"destination": "jobs/x", "consumer": "c", "max": MAX_BATCH, "wait": 0})
    assert response.status_code == 200 and response.json() == {"deliveries": []}


@pytest.mark.parametrize("wait", [-1, MAX_WAIT + 0.1, 3600])
async def test_a_wait_over_30_seconds_or_negative_is_400(http: httpx.AsyncClient, wait: float) -> None:
    response = await http.post("/broker/v1/pull", headers=auth(),
                               json={"destination": "jobs/x", "consumer": "c", "max": 1, "wait": wait})
    assert response.status_code == 400


@pytest.mark.parametrize("body", [
    {}, {"destination": "jobs/x"}, {"destination": 5, "body": "AA==", "headers": {}},
    {"destination": "jobs/x", "body": 5, "headers": {}},
    {"destination": "jobs/x", "body": "not base64!!", "headers": {}},
    {"destination": "jobs/x", "body": "AA==", "headers": []},
    {"destination": "jobs/x", "body": "AA==", "headers": {"a": 1}},
    {"destination": "jobs/x", "body": "AA==", "headers": {"a": "b" * 5000}},
    {"destination": "jobs/x", "body": "AA==", "headers": {f"k{i}": "v" for i in range(100)}},
    {"destination": "../x", "body": "AA==", "headers": {}}, {"destination": "x y", "body": "AA==", "headers": {}},
])
async def test_a_malformed_publish_is_400_and_stores_nothing(
    http: httpx.AsyncClient, redis: FakeRedis, body: Any
) -> None:
    response = await http.post("/broker/v1/publish", headers=auth(), json=body)
    assert response.status_code == 400 and not await redis.keys("das:broker:*")


async def test_a_body_that_is_not_json_is_400(http: httpx.AsyncClient) -> None:
    for content in (b"not json", b"[1, 2]", b'"text"', b"null", b""):
        response = await http.post("/broker/v1/publish", headers=auth(), content=content)
        assert response.status_code == 400, content


@pytest.mark.parametrize("outcome", ["", "nack", "ACK", 5, None])
async def test_an_unknown_settle_outcome_is_400(http: httpx.AsyncClient, outcome: Any) -> None:
    response = await http.post("/broker/v1/settle", headers=auth(),
                               json={"destination": "jobs/x", "id": "n:1-0", "outcome": outcome})
    assert response.status_code == 400


@pytest.mark.parametrize("delivery_id", ["", "1-0", "x:1-0", "n:", "n:not-an-id", "n:1-0\n", 5])
async def test_a_settle_id_that_is_not_one_the_broker_gave_is_400(http: httpx.AsyncClient, delivery_id: Any) -> None:
    response = await http.post("/broker/v1/settle", headers=auth(),
                               json={"destination": "jobs/x", "id": delivery_id, "outcome": "ack"})
    assert response.status_code == 400


async def test_a_redis_failure_is_503_so_the_client_does_not_think_the_message_was_stored() -> None:
    class Down(FakeRedis):
        async def xadd(self, *args: Any, **kwargs: Any) -> Any:
            raise ConnectionError("redis is down")

    broken = Down(server=FakeServer(), decode_responses=True)
    app = Starlette(routes=broker_routes(broken, settings()))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://b") as client:
        response = await client.post("/broker/v1/publish", headers=auth(),
                                     json={"destination": "jobs/x", "body": b64(b"x"), "headers": {}})
    await broken.aclose()
    assert response.status_code == 503 and "redis" not in response.text.lower()


async def test_responses_carry_no_stack_trace_or_internal_detail(http: httpx.AsyncClient) -> None:
    response = await http.post("/broker/v1/publish", headers=auth(), content=b"not json")
    assert "Traceback" not in response.text and 'File "' not in response.text
    assert json.loads(response.text).keys() == {"error"}


# --------------------------------------------------------------------------- tests added after the fault-injection pass


@pytest.mark.parametrize("separator", ["-", ":", "_", ""])
async def test_only_the_bearer_scheme_with_a_space_is_accepted(http: httpx.AsyncClient, separator: str) -> None:
    response = await http.post("/broker/v1/ping", headers={"Authorization": f"Bearer{separator}{TOKEN}"})
    assert response.status_code == 401


@pytest.mark.parametrize("consumer", ["", "has space", "x" * 129, "a/b", "ünï"])
async def test_a_consumer_name_that_is_not_plain_is_refused(broker: Broker, consumer: str) -> None:
    with pytest.raises(ValueError):
        await broker.pull("jobs/x", consumer, 1, 0)


async def test_the_queue_logic_checks_its_own_wait_and_batch_limits(broker: Broker) -> None:
    with pytest.raises(ValueError):
        await broker.pull("jobs/x", "c", 1, MAX_WAIT + 1)
    with pytest.raises(ValueError):
        await broker.pull("jobs/x", "c", MAX_BATCH + 1, 0)
    with pytest.raises(ValueError):
        await broker.pull("jobs/x", "c", 1, -0.1)
