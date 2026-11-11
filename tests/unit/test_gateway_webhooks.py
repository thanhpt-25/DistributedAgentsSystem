"""Webhook_Sender (das-client-api tasks 7.6, 7.7; Phase 4 Req 8.3-8.8, 8.10, 10.1).

HTTP is mocked with respx; Redis is fakeredis with a controllable ``TIME``. The sender gets a FakeClock
and a fixed jitter, so retry times are exact."""

from __future__ import annotations

import asyncio
import io
import json
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families
from pydantic import SecretStr

from das.client.webhook import next_due, sign, verify, webhook_body
from das.clock import FakeClock
from das.config.schema import ClientLimitsConfig, ClientSettingsConfig, WebhookConfig
from das.gateway.app import create_app
from das.gateway.webhooks import WebhookSender
from das.observability.logging import configure_logging
from das.observability.metrics import GatewayMetrics, MetricsRecorder
from das.registry import AgentRegistry
from das.state import StateStore
from tests.unit.test_state_phase4_events import SECRET_NAME, URL, events, submitted

SECRET = "webhook-secret-0123456789abcdef"
SECRETS = {SECRET_NAME: SecretStr(SECRET)}
BASE = 10.0
MAX_BACKOFF = 100.0
RAND = 0.5  # jitter factor 0.8 + 0.4 * 0.5 = 1.0: a retry is exactly ``backoff`` after the claim


def config(**fields: Any) -> WebhookConfig:
    return WebhookConfig(
        **{"workers": 1, "timeout": 5.0, "base_backoff": BASE, "max_backoff": MAX_BACKOFF, "max_attempts": 8, **fields}
    )


class Harness:
    def __init__(self, store: StateStore, client: httpx.AsyncClient, clock: FakeClock, **fields: Any) -> None:
        self.store = store
        self.clock = clock
        self.recorder = MetricsRecorder(clock=FakeClock())
        self.metrics = GatewayMetrics(self.recorder)
        self.sender = WebhookSender(
            store=store,
            client=client,
            secrets=fields.pop("secrets", SECRETS),
            config=config(**fields),
            clock=clock,
            metrics=lambda: self.metrics,
            rand=lambda: RAND,
        )

    def counted(self, outcome: str) -> float:
        body, _ = self.recorder.render()
        for fam in text_string_to_metric_families(body.decode()):
            for s in fam.samples:
                if s.name == "das_webhook_deliveries_total" and s.labels == {"outcome": outcome}:
                    return s.value
        return 0.0


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(follow_redirects=False, timeout=5.0) as c:
        yield c


@pytest.fixture
def harness(state_store, client, fake_redis_time) -> Harness:
    return Harness(state_store, client, FakeClock())


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    out = io.StringIO()
    configure_logging("webhook-test", stream=out)  # no redaction list: absence must come from the sender
    yield out
    configure_logging("test", stream=io.StringIO())


async def pending_delivery(store: StateStore) -> tuple[str, str, str, dict[str, Any]]:
    """A Request cancelled with a webhook target: its pending delivery and Terminal_Event."""
    rid = await submitted(store, webhook=True)
    assert (await store.cancel_request(rid)).ok
    event_id, event_type, data = (await events(store, rid))[-1]
    return rid, event_id, event_type, data


# --------------------------------------------------------------------------- delivered


async def test_a_2xx_response_delivers_with_a_signed_canonical_body(harness, respx_mock, fake_redis_time):
    rid, event_id, event_type, data = await pending_delivery(harness.store)
    route = respx_mock.post(URL).mock(return_value=httpx.Response(204))
    assert await harness.sender.deliver_one() is True

    request = route.calls.last.request
    body = webhook_body(rid, event_id, event_type, data)
    assert request.content == body
    assert request.headers["content-type"] == "application/json"
    assert request.headers["das-webhook-id"] == f"{rid}:{event_id}"
    timestamp = request.headers["das-webhook-timestamp"]
    assert timestamp == str(int(harness.clock.now()))
    signature = request.headers["das-webhook-signature"]
    assert signature == sign(SECRET.encode(), int(timestamp), body)
    assert verify(SECRET.encode(), timestamp, body, signature, harness.clock.now(), 300)

    state = await harness.store.get_webhook(rid)
    assert (state.state, state.attempts, state.last_status) == ("delivered", 1, "204")
    assert await harness.store.next_webhook_due() is None  # removed from the outbox (Req 8.5)
    assert harness.counted("delivered") == 1


async def test_nothing_due_sends_nothing(harness, respx_mock):
    route = respx_mock.post(URL).mock(return_value=httpx.Response(200))
    assert await harness.sender.deliver_one() is False
    assert not route.called


async def test_a_delivery_is_held_by_one_sender_at_a_time(state_store, client, respx_mock, fake_redis_time):
    await pending_delivery(state_store)
    route = respx_mock.post(URL).mock(return_value=httpx.Response(200))
    a, b = Harness(state_store, client, FakeClock()), Harness(state_store, client, FakeClock())
    results = await asyncio.gather(a.sender.deliver_one(), b.sender.deliver_one())
    assert sorted(results) == [False, True]
    assert route.call_count == 1  # Req 8.3


# --------------------------------------------------------------------------- retries


@pytest.mark.parametrize(
    ("mock", "last_status"),
    [
        (lambda: httpx.Response(500), "500"),
        (lambda: httpx.Response(404), "404"),
        (lambda: httpx.Response(302, headers={"location": "https://elsewhere.example.test/x"}), "302"),
        (lambda: httpx.ReadTimeout("slow"), "timeout"),
        (lambda: httpx.ConnectTimeout("slow"), "timeout"),
        (lambda: httpx.ConnectError("refused"), "connect_error"),
        (lambda: httpx.RemoteProtocolError("garbage"), "connect_error"),
    ],
)
async def test_failures_are_retried_at_the_backoff_after_the_claim(
    harness, respx_mock, fake_redis_time, mock, last_status
):
    rid, *_ = await pending_delivery(harness.store)
    outcome = mock()
    if isinstance(outcome, Exception):
        respx_mock.post(URL).mock(side_effect=outcome)
    else:
        respx_mock.post(URL).mock(return_value=outcome)
    claimed_at = fake_redis_time.now()
    assert await harness.sender.deliver_one() is True

    state = await harness.store.get_webhook(rid)
    assert (state.state, state.attempts, state.last_status) == ("pending", 1, last_status)
    assert await harness.store.next_webhook_due() == pytest.approx(next_due(claimed_at, 1, BASE, MAX_BACKOFF, RAND))
    assert harness.counted("retry") == 1


async def test_a_redirect_is_not_followed(harness, respx_mock, fake_redis_time):
    await pending_delivery(harness.store)
    respx_mock.post(URL).mock(return_value=httpx.Response(307, headers={"location": "https://evil.example.test/"}))
    target = respx_mock.post("https://evil.example.test/").mock(return_value=httpx.Response(200))
    await harness.sender.deliver_one()
    assert not target.called


async def test_retry_delays_double_and_are_capped(harness, respx_mock, fake_redis_time):
    rid, *_ = await pending_delivery(harness.store)
    respx_mock.post(URL).mock(return_value=httpx.Response(503))
    for attempt, expected in [(1, 10.0), (2, 20.0), (3, 40.0), (4, 80.0), (5, 100.0)]:
        claimed_at = fake_redis_time.now()
        assert await harness.sender.deliver_one() is True
        assert await harness.store.next_webhook_due() == pytest.approx(claimed_at + expected)
        fake_redis_time.advance(expected + 1)
    assert (await harness.store.get_webhook(rid)).attempts == 5


async def test_the_retry_time_comes_from_redis_not_the_host_clock(harness, respx_mock, fake_redis_time):
    await pending_delivery(harness.store)
    respx_mock.post(URL).mock(return_value=httpx.Response(500))
    harness.clock.advance(10_000)  # the sender's clock is far from Redis time
    claimed_at = fake_redis_time.now()
    await harness.sender.deliver_one()
    assert await harness.store.next_webhook_due() == pytest.approx(claimed_at + BASE)


# --------------------------------------------------------------------------- failed


async def test_the_last_attempt_marks_the_delivery_failed(state_store, client, respx_mock, fake_redis_time):
    rid, *_ = await pending_delivery(state_store)
    h = Harness(state_store, client, FakeClock(), max_attempts=2)
    respx_mock.post(URL).mock(return_value=httpx.Response(500))

    await h.sender.deliver_one()
    assert (await state_store.get_webhook(rid)).state == "pending"
    fake_redis_time.advance(BASE + 1)
    await h.sender.deliver_one()

    state = await state_store.get_webhook(rid)
    assert (state.state, state.attempts, state.last_status) == ("failed", 2, "500")
    assert await state_store.next_webhook_due() is None
    assert (h.counted("retry"), h.counted("failed")) == (1, 1)


async def test_failed_exactly_when_attempts_reach_the_maximum(state_store, client, respx_mock, fake_redis_time):
    rid, *_ = await pending_delivery(state_store)
    h = Harness(state_store, client, FakeClock(), max_attempts=1)
    respx_mock.post(URL).mock(return_value=httpx.Response(500))
    await h.sender.deliver_one()
    assert (await state_store.get_webhook(rid)).state == "failed"


async def test_a_2xx_on_the_last_attempt_is_still_delivered(state_store, client, respx_mock, fake_redis_time):
    rid, *_ = await pending_delivery(state_store)
    h = Harness(state_store, client, FakeClock(), max_attempts=2)
    route = respx_mock.post(URL)
    route.mock(return_value=httpx.Response(500))
    await h.sender.deliver_one()
    fake_redis_time.advance(BASE + 1)
    route.mock(return_value=httpx.Response(200))
    await h.sender.deliver_one()
    assert (await state_store.get_webhook(rid)).state == "delivered"


async def test_a_missing_secret_fails_the_delivery_without_a_request(
    state_store, client, respx_mock, fake_redis_time, stream
):
    rid, *_ = await pending_delivery(state_store)
    route = respx_mock.post(URL).mock(return_value=httpx.Response(200))
    h = Harness(state_store, client, FakeClock(), secrets={})
    assert await h.sender.deliver_one() is True

    state = await state_store.get_webhook(rid)
    assert (state.state, state.last_status) == ("failed", "secret_missing")
    assert not route.called
    assert h.counted("failed") == 1
    warnings = [json.loads(x) for x in stream.getvalue().splitlines() if '"warning"' in x]
    assert len(warnings) == 1  # logged once per delivery


# --------------------------------------------------------------------------- response handling


async def test_a_huge_response_body_is_read_only_up_to_64_kib(harness, respx_mock, fake_redis_time):
    rid, *_ = await pending_delivery(harness.store)
    chunks = {"read": 0}

    async def body() -> AsyncIterator[bytes]:
        for _ in range(4000):
            chunks["read"] += 1
            yield b"x" * 1024

    respx_mock.post(URL).mock(return_value=httpx.Response(200, content=body()))
    await harness.sender.deliver_one()
    assert (await harness.store.get_webhook(rid)).state == "delivered"
    assert chunks["read"] <= 66  # 64 KiB in 1 KiB chunks, never the full 4000


# --------------------------------------------------------------------------- stale claims and logs


async def test_a_stale_completion_is_not_counted(harness, respx_mock, fake_redis_time, monkeypatch):
    await pending_delivery(harness.store)
    respx_mock.post(URL).mock(return_value=httpx.Response(200))

    async def stale(*_: Any, **__: Any) -> bool:
        return False

    monkeypatch.setattr(harness.store, "complete_webhook", stale)
    assert await harness.sender.deliver_one() is True
    assert harness.counted("delivered") == 0


async def test_secrets_and_signatures_never_reach_the_logs(
    state_store, client, respx_mock, fake_redis_time, stream
):
    await pending_delivery(state_store)
    route = respx_mock.post(URL)
    h = Harness(state_store, client, FakeClock())
    route.mock(return_value=httpx.Response(500))
    await h.sender.deliver_one()
    signature = route.calls.last.request.headers["das-webhook-signature"]
    fake_redis_time.advance(BASE + 1)
    route.mock(side_effect=httpx.ConnectError(f"boom {SECRET}"))  # even an error that quotes the secret
    await h.sender.deliver_one()
    logs = stream.getvalue()
    assert SECRET not in logs and signature not in logs and signature.removeprefix("v1=") not in logs


# --------------------------------------------------------------------------- the worker loop


async def wait_for(predicate, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.005)


async def _async(value: bool) -> bool:
    return value


async def test_the_worker_delivers_then_stops_on_request(harness, respx_mock, fake_redis_time):
    rid, *_ = await pending_delivery(harness.store)
    respx_mock.post(URL).mock(return_value=httpx.Response(200))
    stop = asyncio.Event()
    task = asyncio.create_task(harness.sender.run(stop))

    async def delivered() -> bool:
        state = await harness.store.get_webhook(rid)
        return state is not None and state.state == "delivered"

    await wait_for(delivered)
    stop.set()
    await asyncio.wait_for(task, 3)


async def test_an_idle_worker_waits_at_most_one_second(harness, respx_mock, fake_redis_time):
    stop = asyncio.Event()
    task = asyncio.create_task(harness.sender.run(stop))
    await wait_for(lambda: _async(len(harness.clock.sleeps) >= 3))
    stop.set()
    await asyncio.wait_for(task, 3)
    assert set(harness.clock.sleeps) == {1.0}  # empty outbox: the 1 s cap


async def test_an_idle_worker_wakes_for_a_retry_due_sooner_than_one_second(
    state_store, client, respx_mock, fake_redis_time
):
    await pending_delivery(state_store)
    h = Harness(state_store, client, FakeClock(), base_backoff=0.4)
    respx_mock.post(URL).mock(return_value=httpx.Response(500))
    await h.sender.deliver_one()  # retry due in 0.4 s of Redis time
    stop = asyncio.Event()
    task = asyncio.create_task(h.sender.run(stop))
    await wait_for(lambda: _async(len(h.clock.sleeps) >= 1))
    stop.set()
    await asyncio.wait_for(task, 3)
    assert 0 < h.clock.sleeps[0] <= 0.4 + 1e-6


async def test_a_send_in_flight_finishes_before_the_worker_stops(harness, respx_mock, fake_redis_time):
    rid, *_ = await pending_delivery(harness.store)
    started, release = asyncio.Event(), asyncio.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        return httpx.Response(200)

    respx_mock.post(URL).mock(side_effect=slow)
    stop = asyncio.Event()
    task = asyncio.create_task(harness.sender.run(stop))
    await asyncio.wait_for(started.wait(), 3)
    stop.set()
    await asyncio.sleep(0.05)
    assert not task.done()  # still inside its send
    release.set()
    await asyncio.wait_for(task, 3)
    assert (await harness.store.get_webhook(rid)).state == "delivered"


async def test_the_worker_survives_a_store_error(harness, respx_mock, fake_redis_time, monkeypatch):
    rid, *_ = await pending_delivery(harness.store)
    respx_mock.post(URL).mock(return_value=httpx.Response(200))
    real = harness.store.claim_webhook
    calls = {"n": 0}

    async def flaky(*args: Any, **kwargs: Any):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("redis down")
        return await real(*args, **kwargs)

    monkeypatch.setattr(harness.store, "claim_webhook", flaky)
    stop = asyncio.Event()
    task = asyncio.create_task(harness.sender.run(stop))

    async def delivered() -> bool:
        state = await harness.store.get_webhook(rid)
        return state is not None and state.state == "delivered"

    await wait_for(delivered)
    stop.set()
    await asyncio.wait_for(task, 3)
    assert calls["n"] >= 2


# --------------------------------------------------------------------------- lifespan


def sender_tasks() -> list[asyncio.Task[Any]]:
    return [t for t in asyncio.all_tasks() if t.get_name().startswith("das-webhook-sender")]


def app_with(state_store, fake_redis, **kwargs: Any):
    limits = ClientLimitsConfig(default=ClientSettingsConfig(rate=0.0))
    return create_app(
        state_store, AgentRegistry(fake_redis), None, ["client-key-0001"], 50, client_limits=limits, **kwargs
    )


async def test_the_lifespan_runs_the_configured_workers_and_stops_them(
    state_store, fake_redis, respx_mock, fake_redis_time
):
    rid, *_ = await pending_delivery(state_store)
    respx_mock.post(URL).mock(return_value=httpx.Response(200))
    app = app_with(state_store, fake_redis, webhook=config(workers=2), webhook_secrets=SECRETS)
    async with app.router.lifespan_context(app):
        assert len(sender_tasks()) == 2

        async def delivered() -> bool:
            state = await state_store.get_webhook(rid)
            return state is not None and state.state == "delivered"

        await wait_for(delivered)
    assert sender_tasks() == []


async def test_zero_workers_start_no_sender(state_store, fake_redis, fake_redis_time):
    app = app_with(state_store, fake_redis, webhook=config(workers=0), webhook_secrets=SECRETS)
    async with app.router.lifespan_context(app):
        assert sender_tasks() == []
