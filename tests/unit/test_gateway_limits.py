"""Gateway per-key rate limit (Phase 4 Req 1.2, 1.3, 2.1, 2.2, 2.5, 2.6, 10.1)."""

from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from das.clock import FakeClock
from das.config.schema import ClientLimitsConfig, ClientOverride, ClientSettingsConfig
from das.gateway.app import create_app
from das.observability.logging import configure_logging
from das.observability.metrics import MetricsRecorder
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore, keys

KEY = "client-key-0001"
OTHER_KEY = "client-key-0002"
ADMIN_KEY = "operator-admin-key-0001"
AUTH = {"X-API-Key": KEY}
OTHER_AUTH = {"X-API-Key": OTHER_KEY}
ADMIN_AUTH = {"X-API-Key": ADMIN_KEY}


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def limits(rate: float = 1.0, burst: int = 2, *overrides: ClientOverride) -> ClientLimitsConfig:
    return ClientLimitsConfig(
        default=ClientSettingsConfig(rate=rate, burst=burst), overrides=list(overrides)
    )


class Recorders:
    """Captures the recorder built by ``create_app``'s metrics factory."""

    def __init__(self) -> None:
        self.recorder: MetricsRecorder | None = None

    def __call__(self, templates: Sequence[str]) -> MetricsRecorder:
        self.recorder = MetricsRecorder(clock=FakeClock(), route_templates=templates)
        return self.recorder

    def value(self, sample_name: str, **labels: str) -> float:
        assert self.recorder is not None
        body, _ = self.recorder.render()
        for fam in text_string_to_metric_families(body.decode()):
            for s in fam.samples:
                if s.name == sample_name and s.labels == labels:
                    return s.value
        return 0.0


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    out = io.StringIO()
    configure_logging("gw-test", secrets=[KEY, OTHER_KEY, ADMIN_KEY], stream=out)
    yield out
    configure_logging("test", stream=io.StringIO())


def audits(stream: io.StringIO) -> list[dict[str, Any]]:
    lines = [json.loads(line) for line in stream.getvalue().splitlines() if line]
    return [e for e in lines if e.get("message") == "audit"]


async def make_client(
    state_store: StateStore, fake_redis, recorders: Recorders, client_limits: ClientLimitsConfig
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        state_store,
        AgentRegistry(fake_redis),
        None,
        [KEY, OTHER_KEY],
        10,
        admin_api_keys=[ADMIN_KEY],
        client_limits=client_limits,
        metrics_factory=recorders,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        yield c


@pytest.fixture
async def client(state_store, fake_redis, fake_redis_time, recorders):
    async for c in make_client(state_store, fake_redis, recorders, limits(rate=1.0, burst=2)):
        yield c


# --------------------------------------------------------------------------- bucket


async def test_burst_is_allowed_then_429_with_retry_after(client, fake_redis_time):
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 200
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 200
    limited = await client.get("/v1/agents", headers=AUTH)
    assert limited.status_code == 429
    assert limited.json() == {"error": "rate_limited"}
    assert limited.headers["Retry-After"] == "1"

    fake_redis_time.advance(1.0)
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 200


async def test_retry_after_is_whole_seconds_rounded_up(client, fake_redis_time):
    await client.get("/v1/agents", headers=AUTH)
    await client.get("/v1/agents", headers=AUTH)
    fake_redis_time.advance(0.5)  # half a token back: 0.5 s to go
    limited = await client.get("/v1/agents", headers=AUTH)
    assert limited.status_code == 429
    assert limited.headers["Retry-After"] == "1"


async def test_each_key_has_its_own_bucket(client):
    await client.get("/v1/agents", headers=AUTH)
    await client.get("/v1/agents", headers=AUTH)
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 429
    assert (await client.get("/v1/agents", headers=OTHER_AUTH)).status_code == 200


async def test_override_by_fingerprint_replaces_the_default(
    state_store, fake_redis, fake_redis_time, recorders
):
    override = ClientOverride(fingerprint=fp(OTHER_KEY), rate=0.0)  # unlimited
    async for c in make_client(state_store, fake_redis, recorders, limits(1.0, 1, override)):
        assert (await c.get("/v1/agents", headers=AUTH)).status_code == 200
        assert (await c.get("/v1/agents", headers=AUTH)).status_code == 429
        for _ in range(5):
            assert (await c.get("/v1/agents", headers=OTHER_AUTH)).status_code == 200


async def test_rate_zero_never_touches_the_bucket_key(
    state_store, fake_redis, fake_redis_time, recorders
):
    async for c in make_client(state_store, fake_redis, recorders, limits(rate=0.0, burst=1)):
        for _ in range(3):
            assert (await c.get("/v1/agents", headers=AUTH)).status_code == 200
    assert await fake_redis.exists(keys.client_bucket(fp(KEY))) == 0


async def test_rejected_calls_do_not_consume_tokens(client):
    for _ in range(5):
        bad = await client.get("/v1/agents", headers={"X-API-Key": "wrong-key-000000"})
        assert bad.status_code == 401
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 200
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 200


async def test_healthz_is_exempt(client):
    await client.get("/v1/agents", headers=AUTH)
    await client.get("/v1/agents", headers=AUTH)
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 429
    assert (await client.get("/healthz")).status_code in (200, 503)  # never 429


async def test_default_config_still_serves_normal_traffic(state_store, fake_redis, fake_redis_time):
    """Without ``client_limits`` the Phase 4 defaults apply and a handful of calls pass."""
    app = create_app(state_store, AgentRegistry(fake_redis), None, [KEY], 10)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        for _ in range(20):
            assert (await c.get("/v1/agents", headers=AUTH)).status_code == 200


# --------------------------------------------------------------------------- failure


async def test_store_failure_is_503_and_never_fails_open(client, state_store, monkeypatch):
    async def boom(*_: Any, **__: Any) -> float | None:
        raise ConnectionError("redis down")

    monkeypatch.setattr(state_store, "rate_check", boom)
    response = await client.get("/v1/agents", headers=AUTH)
    assert response.status_code == 503
    assert response.json() == {"error": "service_unavailable"}


# --------------------------------------------------------------------------- metrics


async def test_rejection_is_counted_by_reason(client, recorders):
    await client.get("/v1/agents", headers=AUTH)
    await client.get("/v1/agents", headers=AUTH)
    await client.get("/v1/agents", headers=AUTH)
    await client.get("/v1/agents", headers=AUTH)
    assert recorders.value("das_client_rejections_total", reason="rate_limited") == 2


# --------------------------------------------------------------------------- admin


async def test_admin_routes_are_limited_and_the_429_is_audited(client, stream):
    assert (await client.get("/v1/admin/dlq", headers=ADMIN_AUTH)).status_code == 200
    assert (await client.get("/v1/admin/dlq", headers=ADMIN_AUTH)).status_code == 200
    limited = await client.get("/v1/admin/dlq", headers=ADMIN_AUTH)
    assert limited.status_code == 429
    assert limited.headers["Retry-After"] == "1"

    *_, last = audits(stream)
    assert last["http_status"] == 429
    assert last["error_code"] == "rate_limited"
    assert last["key_fingerprint"] == fp(ADMIN_KEY)


async def test_non_admin_key_on_admin_route_is_403_without_consuming_a_token(client):
    for _ in range(4):
        assert (await client.get("/v1/admin/dlq", headers=AUTH)).status_code == 403
    assert (await client.get("/v1/agents", headers=AUTH)).status_code == 200
