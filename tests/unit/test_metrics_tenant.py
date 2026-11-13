"""The ``tenant`` label (das-multi-tenant task 7.1; Phase 5 Req 8.1, 8.4).

Exactly two metrics carry it, ``das_requests_submitted_total`` and ``das_requests_finished_total``, limited by
``metrics.label_limits.tenant`` like the other dynamic labels. The Gateway reads the tenant from the Principal, the
Manager from the Request it finished. Nothing else (the duration histogram, the HTTP metrics) gains the label."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from das.config.schema import LabelLimitsConfig, TenantConfig
from das.gateway.app import create_app
from das.models.request import RequestStatus
from das.observability.metrics import (
    ALLOWED_LABEL_NAMES,
    CATALOG,
    CATALOG_BY_NAME,
    DYNAMIC_LABELS,
    GatewayMetrics,
    ManagerMetrics,
)
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore
from tests.unit.test_gateway_v1_extensions import Recorders
from tests.unit.test_metrics_recorder import make, samples, value

SUBMITTED, FINISHED = "das_requests_submitted_total", "das_requests_finished_total"
A1, B1, DEFAULT = "team-a-key-000000001", "team-b-key-000000001", "default-key-00000001"


# --------------------------------------------------------------------------- the catalog


def test_the_label_is_allowed_and_dynamic() -> None:
    assert "tenant" in ALLOWED_LABEL_NAMES and "tenant" in DYNAMIC_LABELS


def test_exactly_the_two_request_counters_carry_the_label() -> None:
    carrying = {spec.name for spec in CATALOG if "tenant" in spec.labels}
    assert carrying == {SUBMITTED, FINISHED}
    assert CATALOG_BY_NAME[SUBMITTED].labels == ("tenant",)
    assert CATALOG_BY_NAME[FINISHED].labels == ("status", "tenant")


# --------------------------------------------------------------------------- the facades


def test_submitted_counts_per_tenant() -> None:
    rec, _, _ = make()
    gateway = GatewayMetrics(rec)
    gateway.request_submitted("team-a")
    gateway.request_submitted("team-a")
    gateway.request_submitted("team-b")
    assert value(rec, SUBMITTED, tenant="team-a") == 2
    assert value(rec, SUBMITTED, tenant="team-b") == 1
    assert value(rec, SUBMITTED, tenant="team-c") is None


@pytest.mark.parametrize("facade", [GatewayMetrics, ManagerMetrics])
def test_finished_counts_per_tenant_and_status_and_the_duration_has_no_tenant(facade: Any) -> None:
    rec, _, _ = make()
    metrics = facade(rec)
    metrics.request_finished(RequestStatus.COMPLETED, 1.5, "team-a")
    metrics.request_finished(RequestStatus.COMPLETED, 1.5, "team-b")
    metrics.request_finished(RequestStatus.FAILED, 2, "team-a")
    assert value(rec, FINISHED, status="Completed", tenant="team-a") == 1
    assert value(rec, FINISHED, status="Completed", tenant="team-b") == 1
    assert value(rec, FINISHED, status="Failed", tenant="team-a") == 1
    duration = samples(rec)["das_request_duration_seconds"]
    assert duration and all("tenant" not in s.labels for s in duration)  # Req 8.1: only the two counters
    assert value(rec, "das_request_duration_seconds_count", status="Completed") == 2  # both tenants, one series


def test_tenants_beyond_the_limit_share_the_overflow_value_and_one_warning_is_logged() -> None:
    rec, log, _ = make(label_limits=LabelLimitsConfig(tenant=2))
    gateway = GatewayMetrics(rec)
    for tenant in ["a", "b", "c", "d", "a"]:
        gateway.request_submitted(tenant)
    assert value(rec, SUBMITTED, tenant="a") == 2
    assert value(rec, SUBMITTED, tenant="b") == 1
    assert value(rec, SUBMITTED, tenant="_other") == 2  # the third and fourth tenant
    assert [(w["metric"], w["label"], w["limit"]) for w in log.events("metric_label_overflow")] == [
        (SUBMITTED, "tenant", 2)
    ]


@pytest.mark.parametrize(("metric", "labels"), [(SUBMITTED, {}), (FINISHED, {"status": "Failed"})])
def test_an_observation_without_a_tenant_is_not_recorded_and_is_logged(metric: str, labels: dict[str, str]) -> None:
    rec, log, _ = make()
    rec.inc(metric, labels)
    assert samples(rec).get(metric.removesuffix("_total"), []) == [] or value(rec, metric, **labels) is None
    assert [e["metric"] for e in log.events("metric_record_failed")] == [metric]


def test_the_limit_is_per_metric() -> None:
    rec, _, _ = make(label_limits=LabelLimitsConfig(tenant=1))
    GatewayMetrics(rec).request_submitted("a")
    ManagerMetrics(rec).request_finished(RequestStatus.COMPLETED, 0, "b")  # the finished counter has its own budget
    assert value(rec, FINISHED, status="Completed", tenant="b") == 1


# --------------------------------------------------------------------------- the Gateway


def tenant_config(tid: str, key: str) -> TenantConfig:
    keys = [{"fingerprint": key_fingerprint(key.encode()), "scopes": ["submit", "read"]}]
    return TenantConfig.model_validate({"id": tid, "keys": keys})


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, recorders: Recorders) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [A1, B1, DEFAULT], 1000,
        tenants=[tenant_config("team-a", A1), tenant_config("team-b", B1)], metrics_factory=recorders,
    )  # fmt: skip
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        yield client


async def submit(gw: httpx.AsyncClient, key: str, **headers: str) -> httpx.Response:
    return await gw.post("/v1/requests", json={"prompt": "p"}, headers={"X-API-Key": key, **headers})


async def test_a_submit_is_counted_for_the_tenant_of_the_key(gw: httpx.AsyncClient, recorders: Recorders) -> None:
    await submit(gw, A1)
    await submit(gw, A1)
    await submit(gw, B1)
    await submit(gw, DEFAULT)
    assert recorders.value(SUBMITTED, tenant="team-a") == 2
    assert recorders.value(SUBMITTED, tenant="team-b") == 1
    assert recorders.value(SUBMITTED, tenant="default") == 1


async def test_a_replay_and_a_refused_submit_are_not_counted(gw: httpx.AsyncClient, recorders: Recorders) -> None:
    first = await submit(gw, A1, **{"Idempotency-Key": "k1"})
    again = await submit(gw, A1, **{"Idempotency-Key": "k1"})
    assert (first.status_code, again.status_code) == (202, 200)
    refused = await gw.post("/v1/requests", json={"prompt": ""}, headers={"X-API-Key": A1})
    assert refused.status_code == 400
    assert recorders.value(SUBMITTED, tenant="team-a") == 1


async def test_a_cancel_is_counted_for_the_tenant_that_owns_the_request(
    gw: httpx.AsyncClient, recorders: Recorders
) -> None:
    rid = (await submit(gw, A1)).json()["request_id"]
    foreign = await gw.post(f"/v1/requests/{rid}/cancel", headers={"X-API-Key": B1})
    assert foreign.status_code == 404
    assert recorders.value(FINISHED, status="Cancelled", tenant="team-b") == 0  # a refused cancel finished nothing
    assert (await gw.post(f"/v1/requests/{rid}/cancel", headers={"X-API-Key": A1})).status_code == 202
    assert recorders.value(FINISHED, status="Cancelled", tenant="team-a") == 1
    assert recorders.value(FINISHED, status="Cancelled", tenant="team-b") == 0
