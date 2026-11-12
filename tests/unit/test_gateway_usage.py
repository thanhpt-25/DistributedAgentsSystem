"""The usage endpoints (das-multi-tenant task 5.5; Phase 5 Req 6.3, 6.4, 6.5, 6.7).

``GET /v1/usage`` is the Usage_Report of the caller's own tenant (Scope ``read``); ``GET /v1/admin/usage`` is the same
report for any tenant, or for every tenant, for an Operator. The counters are read from the rollups the state scripts
keep; the cost is computed when the report is read."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest

from das.config.schema import TenantConfig
from das.gateway.app import create_app
from das.ops.cost import PriceEntry, PriceTable
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import ClientRequestSpec, StateStore
from tests.unit.test_state_phase2_writes import FINAL, usage
from tests.unit.test_state_store import PLAN

A1, A_NOREAD, B1 = "team-a-key-000000001", "team-a-norea-000001", "team-b-key-000000001"
DEFAULT, OP = "default-key-00000001", "root-key-0000000001"
# Not the real current day: the endpoint must take "today" from Redis TIME (here a fake clock), not from the machine.
NOON = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
TODAY = NOON.date()
PRICES = PriceTable(
    currency="USD", models=[PriceEntry(provider="openai", model="gpt", input_per_million="2", output_per_million="4")]
)


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def h(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


TENANTS = [
    TenantConfig.model_validate({"id": "team-a", "keys": [{"fingerprint": fp(A1), "scopes": ["submit", "read"]},
                                                          {"fingerprint": fp(A_NOREAD), "scopes": ["submit"]}]}),
    TenantConfig.model_validate({"id": "team-b", "keys": [{"fingerprint": fp(B1), "scopes": ["read"]}]}),
]  # fmt: skip


async def finished_request(store: StateStore, tenant: str | None, tokens_in: int, tokens_out: int) -> None:
    """A Request of ``tenant`` that planned, summarized and completed, with this many tokens in all."""
    rid = (await store.submit_request("p", ClientRequestSpec(fingerprint="cbabecbd", tenant=tenant))).request_id
    assert rid is not None
    lease = (await store.acquire_lease(rid, "m1", 30)).lease
    assert lease is not None
    assert (await store.commit_plan(lease, PLAN, [usage("plan", i=tokens_in, o=tokens_out)])).ok
    assert (await store.finalize(lease, FINAL, usage=[usage("summarize", i=1, o=1)])).ok


async def build(state_store: StateStore, fake_redis: Any, prices: PriceTable | None = None) -> httpx.AsyncClient:
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [A1, A_NOREAD, B1, DEFAULT], 100,
        admin_api_keys=[OP], tenants=TENANTS, price_table=prices,
    )  # fmt: skip
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    fake_redis_time.set_ms(int(NOON.timestamp() * 1000))
    await finished_request(state_store, "team-a", 100, 50)
    await finished_request(state_store, "team-a", 10, 5)
    await finished_request(state_store, "team-b", 7, 3)
    fake_redis_time.set_ms(int((NOON - timedelta(days=1)).timestamp() * 1000))  # yesterday, for a second day
    await finished_request(state_store, "team-a", 1, 1)
    fake_redis_time.set_ms(int(NOON.timestamp() * 1000))
    async with await build(state_store, fake_redis) as client:
        yield client


def day(report: dict[str, Any], when: date) -> dict[str, Any]:
    return next(d for d in report["days"] if d["date"] == when.isoformat())


# =========================================================================== GET /v1/usage


async def test_a_tenant_gets_the_report_of_its_own_tenant_for_the_last_seven_days(gw: httpx.AsyncClient) -> None:
    reply = await gw.get("/v1/usage", headers=h(A1))
    assert reply.status_code == 200
    report = reply.json()
    assert report["tenant"] == "team-a" and (report["from"], report["to"]) == ("2026-02-27", "2026-03-05")
    assert [d["date"] for d in report["days"]] == [
        "2026-02-27", "2026-02-28", "2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05",
    ]  # fmt: skip  # one entry per day
    today = day(report, TODAY)
    assert today["requests"] == {"submitted": 2, "finished": {"Completed": 2, "Failed": 0, "Cancelled": 0}}
    (model,) = today["models"]
    # Per request: the plan's tokens plus the summary's one in and one out.
    assert (model["provider"], model["model"], model["input_tokens"], model["output_tokens"]) == (
        "openai",
        "gpt",
        112,
        57,
    )
    assert day(report, TODAY - timedelta(days=3))["requests"]["submitted"] == 0  # a quiet day is zeros, not missing


async def test_the_totals_add_up_the_range(gw: httpx.AsyncClient) -> None:
    totals = (await gw.get("/v1/usage", headers=h(A1))).json()["totals"]
    assert totals["requests"] == {"submitted": 3, "finished": {"Completed": 3, "Failed": 0, "Cancelled": 0}}
    # (100+1, 50+1) and (10+1, 5+1) today, (1+1, 1+1) yesterday.
    assert (totals["input_tokens"], totals["output_tokens"], totals["total_tokens"]) == (114, 59, 173)


async def test_each_tenant_sees_only_its_own_numbers(gw: httpx.AsyncClient) -> None:
    b = (await gw.get("/v1/usage", headers=h(B1))).json()
    assert b["tenant"] == "team-b" and b["totals"]["requests"]["submitted"] == 1
    assert b["totals"]["input_tokens"] == 8
    default = (await gw.get("/v1/usage", headers=h(DEFAULT))).json()
    assert default["tenant"] == "default" and default["totals"]["requests"]["submitted"] == 0


async def test_a_key_without_the_read_scope_is_refused(gw: httpx.AsyncClient) -> None:
    reply = await gw.get("/v1/usage", headers=h(A_NOREAD))
    assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": "read"})


async def test_an_operator_calling_the_tenant_endpoint_gets_the_default_tenants_report(gw: httpx.AsyncClient) -> None:
    assert (await gw.get("/v1/usage", headers=h(OP))).json()["tenant"] == "default"


async def test_from_and_to_select_the_days_inclusive(gw: httpx.AsyncClient) -> None:
    report = (await gw.get("/v1/usage?from=2026-03-04&to=2026-03-05", headers=h(A1))).json()
    assert [d["date"] for d in report["days"]] == ["2026-03-04", "2026-03-05"]
    assert report["totals"]["requests"]["submitted"] == 3
    one = (await gw.get("/v1/usage?from=2026-03-04&to=2026-03-04", headers=h(A1))).json()
    assert [d["date"] for d in one["days"]] == ["2026-03-04"] and one["totals"]["requests"]["submitted"] == 1
    default_to = (await gw.get("/v1/usage?from=2026-03-03", headers=h(A1))).json()
    assert (default_to["from"], default_to["to"]) == ("2026-03-03", "2026-03-05")  # a missing `to` is today


@pytest.mark.parametrize(
    ("query", "parameter"),
    [
        ("from=2026-13-01", "from"),
        ("from=yesterday", "from"),
        ("from=2026-03-05&to=2026-03-01", "from"),  # inverted: names `from`
        ("from=2026-03-01&from=2026-03-02", "from"),  # a repeated parameter is not a range
        ("to=2026-02-30", "to"),
        ("to=12/11/2026", "to"),
        ("from=2025-10-01&to=2026-03-05", "to"),  # more than 92 days: names `to`
    ],
)
async def test_an_invalid_range_is_a_400_naming_the_parameter(
    gw: httpx.AsyncClient, query: str, parameter: str
) -> None:
    reply = await gw.get(f"/v1/usage?{query}", headers=h(A1))
    assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": parameter})


async def test_exactly_ninety_two_days_is_allowed(gw: httpx.AsyncClient) -> None:
    reply = await gw.get("/v1/usage?from=2025-12-04&to=2026-03-05", headers=h(A1))
    assert reply.status_code == 200 and len(reply.json()["days"]) == 92


# =========================================================================== cost


async def test_without_a_price_table_the_cost_is_null(gw: httpx.AsyncClient) -> None:
    report = (await gw.get("/v1/usage", headers=h(A1))).json()
    assert report["totals"]["estimated_cost"] is None
    assert all(m["estimated_cost"] is None for d in report["days"] for m in d["models"])


async def test_with_a_price_table_the_cost_is_computed_when_the_report_is_read(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    fake_redis_time.set_ms(int(NOON.timestamp() * 1000))
    await finished_request(state_store, "team-a", 1_000_000, 500_000)
    async with await build(state_store, fake_redis, PRICES) as gw:
        report = (await gw.get("/v1/usage", headers=h(A1))).json()
    # 1,000,001 in at $2 per million and 500,001 out at $4 per million.
    total = report["totals"]["estimated_cost"]
    assert (total["currency"], total["complete"]) == ("USD", True)
    assert float(total["amount"]) == pytest.approx(2.000002 + 2.000004, abs=1e-6)
    (model,) = day(report, TODAY)["models"]
    assert model["estimated_cost"] is not None


# =========================================================================== GET /v1/admin/usage


async def test_an_operator_gets_one_tenants_report(gw: httpx.AsyncClient) -> None:
    report = (await gw.get("/v1/admin/usage?tenant=team-b", headers=h(OP))).json()
    assert report["tenant"] == "team-b" and report["totals"]["requests"]["submitted"] == 1
    assert (await gw.get("/v1/admin/usage?tenant=team-a&from=2026-03-04&to=2026-03-04", headers=h(OP))).json()[
        "totals"
    ]["requests"]["submitted"] == 1


async def test_without_a_tenant_an_operator_gets_every_tenant(gw: httpx.AsyncClient) -> None:
    body = (await gw.get("/v1/admin/usage", headers=h(OP))).json()
    by_tenant = {r["tenant"]: r["totals"]["requests"]["submitted"] for r in body["tenants"]}
    assert by_tenant == {"default": 0, "team-a": 3, "team-b": 1}


async def test_the_admin_report_is_for_operators_only(gw: httpx.AsyncClient) -> None:
    for key in (A1, B1, DEFAULT):
        reply = await gw.get("/v1/admin/usage?tenant=team-a", headers=h(key))
        assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": "operator"}), key


async def test_the_admin_report_validates_its_query(gw: httpx.AsyncClient) -> None:
    for query, parameter in (("tenant=Bad_ID", "tenant"), ("from=nope", "from"), ("to=2026-02-30", "to")):
        reply = await gw.get(f"/v1/admin/usage?{query}", headers=h(OP))
        assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": parameter})


async def test_usage_from_before_the_upgrade_is_not_in_any_report(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    """A Request created by Phase 4 scripts never wrote a rollup, so it is simply absent (Req 6.7)."""
    fake_redis_time.set_ms(int(NOON.timestamp() * 1000))
    rid = await state_store.create_request("old")
    await state_store.client.hdel(f"das:tenant:default:usage:{TODAY:%Y%m%d}", "submitted")
    async with await build(state_store, fake_redis) as gw:
        report = (await gw.get("/v1/usage", headers=h(DEFAULT))).json()
    assert rid and report["totals"]["requests"]["submitted"] == 0
