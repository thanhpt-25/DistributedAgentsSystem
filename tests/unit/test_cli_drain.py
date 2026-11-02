"""Unit tests for ``das drain`` / ``das undrain`` (Requirements 17.14, 17.17)."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

import das.cli as cli
from das.clock import FakeClock

GW = "http://gateway.test:8080"
KEY = "das-admin-key-0123456789abcdef"
ENV = {"DAS_ADMIN_API_KEY": KEY}
DRAIN_URL = f"{GW}/v1/admin/agents/worker-1/drain"
UNDRAIN_URL = f"{GW}/v1/admin/agents/worker-1/undrain"
AGENTS_URL = f"{GW}/v1/agents"


def agent(drained: bool, in_flight: int = 0) -> dict:
    return {
        "agent_id": "worker-1",
        "role": "worker",
        "status": "draining",
        "in_flight": in_flight,
        "drained": drained,
    }


def run(argv: list[str], clock: FakeClock | None = None, env: dict | None = None) -> int:
    return cli.main(argv, ENV if env is None else env, clock=clock or FakeClock())


def assert_no_key(capsys) -> str:
    captured = capsys.readouterr()
    text = captured.out + captured.err
    assert KEY not in text
    assert KEY[:8] not in text
    return text


@respx.mock
def test_drain_202_exits_0_and_sends_key_and_body(capsys) -> None:
    route = respx.post(DRAIN_URL).mock(
        return_value=httpx.Response(202, json={"agent_id": "worker-1", "status": "draining"})
    )
    assert run(["drain", GW, "worker-1", "--release-leases"]) == 0
    request = route.calls.last.request
    assert request.headers["X-API-Key"] == KEY
    assert json.loads(request.content) == {"release_leases": True}
    assert "draining" in assert_no_key(capsys)


@respx.mock
def test_drain_default_release_leases_false() -> None:
    route = respx.post(DRAIN_URL).mock(return_value=httpx.Response(202, json={}))
    assert run(["drain", GW, "worker-1"]) == 0
    assert json.loads(route.calls.last.request.content) == {"release_leases": False}


@respx.mock
def test_undrain_202_exits_0() -> None:
    route = respx.post(UNDRAIN_URL).mock(
        return_value=httpx.Response(202, json={"agent_id": "worker-1", "status": "available"})
    )
    assert run(["undrain", f"{GW}/", "worker-1"]) == 0
    assert route.called


def test_agent_id_is_path_quoted() -> None:
    with respx.mock:
        route = respx.post(f"{GW}/v1/admin/agents/a%2Fb/drain").mock(
            return_value=httpx.Response(202, json={})
        )
        assert run(["drain", GW, "a/b"]) == 0
        assert route.called


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (404, {"error": "not_found"}, ["404", "not_found"]),
        (409, {"error": "agent_state", "current_state": "unavailable"}, ["409", "agent_state"]),
        (504, {"error": "agent_timeout"}, ["504", "agent_timeout"]),
        (403, {"error": "forbidden"}, ["403", "forbidden"]),
    ],
)
@respx.mock
def test_non_202_exits_1_with_status_and_code(capsys, status, body, expected) -> None:
    respx.post(DRAIN_URL).mock(return_value=httpx.Response(status, json=body))
    assert run(["drain", GW, "worker-1"]) == 1
    text = assert_no_key(capsys)
    for part in expected:
        assert part in text


@respx.mock
def test_non_json_error_still_names_status(capsys) -> None:
    respx.post(UNDRAIN_URL).mock(return_value=httpx.Response(502, text="bad gateway"))
    assert run(["undrain", GW, "worker-1"]) == 1
    assert "HTTP 502" in assert_no_key(capsys)


@respx.mock
def test_connection_error_exits_1_without_key(capsys) -> None:
    respx.post(DRAIN_URL).mock(side_effect=httpx.ConnectError(f"refused {KEY}"))
    assert run(["drain", GW, "worker-1"]) == 1
    assert "[REDACTED]" in assert_no_key(capsys)


@respx.mock
def test_server_echoing_key_is_redacted(capsys) -> None:
    respx.post(DRAIN_URL).mock(return_value=httpx.Response(400, json={"error": KEY}))
    assert run(["drain", GW, "worker-1"]) == 1
    assert_no_key(capsys)


def test_missing_key_exits_2_naming_variable(capsys) -> None:
    with respx.mock(assert_all_called=False) as mock:
        route = mock.post(DRAIN_URL)
        assert run(["drain", GW, "worker-1"], env={}) == 2
        assert not route.called
    assert "DAS_ADMIN_API_KEY" in capsys.readouterr().err


@pytest.mark.parametrize("timeout", ["0", "0.5", "86401", "-1", "nan", "inf", "abc"])
def test_timeout_out_of_range_exits_2(capsys, timeout) -> None:
    assert run(["drain", GW, "worker-1", "--wait", "--timeout", timeout]) == 2
    assert "--timeout" in capsys.readouterr().err


@pytest.mark.parametrize("timeout", ["1", "86400"])
def test_timeout_bounds_accepted(timeout) -> None:
    args = cli.build_parser().parse_args(["drain", GW, "a", "--timeout", timeout])
    assert args.timeout == float(timeout)


def test_timeout_default_600() -> None:
    args = cli.build_parser().parse_args(["drain", GW, "a"])
    assert args.timeout == 600
    assert args.wait is False and args.release_leases is False


def test_invalid_gateway_url_exits_2() -> None:
    assert run(["drain", "gateway:8080", "worker-1"]) == 2


@respx.mock
def test_wait_polls_every_2s_until_drained(capsys) -> None:
    respx.post(DRAIN_URL).mock(return_value=httpx.Response(202, json={"status": "draining"}))
    agents = respx.get(AGENTS_URL).mock(
        side_effect=[
            httpx.Response(200, json=[agent(False, in_flight=2)]),
            httpx.Response(503, json={"error": "service_unavailable"}),
            httpx.Response(200, json=[agent(False, in_flight=1)]),
            httpx.Response(200, json=[agent(True)]),
        ]
    )
    clock = FakeClock()
    assert run(["drain", GW, "worker-1", "--wait", "--timeout", "60"], clock=clock) == 0
    assert agents.call_count == 4
    assert clock.sleeps == [2.0, 2.0, 2.0]
    assert agents.calls.last.request.headers["X-API-Key"] == KEY
    assert "drained" in assert_no_key(capsys)


@respx.mock
def test_wait_times_out_with_exit_1(capsys) -> None:
    respx.post(DRAIN_URL).mock(return_value=httpx.Response(202, json={}))
    agents = respx.get(AGENTS_URL).mock(
        return_value=httpx.Response(200, json=[agent(False, in_flight=1)])
    )
    clock = FakeClock()
    assert run(["drain", GW, "worker-1", "--wait", "--timeout", "5"], clock=clock) == 1
    assert sum(clock.sleeps) == pytest.approx(5.0)
    assert clock.sleeps == [2.0, 2.0, 1.0]
    assert agents.call_count == 4
    assert "timeout" in assert_no_key(capsys)


@respx.mock
def test_wait_not_run_when_drain_rejected() -> None:
    respx.post(DRAIN_URL).mock(return_value=httpx.Response(404, json={"error": "not_found"}))
    agents = respx.get(AGENTS_URL)
    assert run(["drain", GW, "worker-1", "--wait"]) == 1
    assert not agents.called


@respx.mock
def test_wait_ignores_other_agents_and_tolerates_items_envelope() -> None:
    respx.post(DRAIN_URL).mock(return_value=httpx.Response(202, json={}))
    other = {**agent(True), "agent_id": "worker-2"}
    respx.get(AGENTS_URL).mock(
        side_effect=[
            httpx.Response(200, json=[other]),
            httpx.Response(200, json={"items": [other, agent(True)]}),
        ]
    )
    clock = FakeClock()
    assert run(["drain", GW, "worker-1", "--wait"], clock=clock) == 0
    assert clock.sleeps == [2.0]


@respx.mock
def test_wait_auth_failure_exits_1_immediately(capsys) -> None:
    respx.post(DRAIN_URL).mock(return_value=httpx.Response(202, json={}))
    respx.get(AGENTS_URL).mock(return_value=httpx.Response(401, json={"error": "unauthorized"}))
    clock = FakeClock()
    assert run(["drain", GW, "worker-1", "--wait"], clock=clock) == 1
    assert clock.sleeps == []
    assert "401" in assert_no_key(capsys)
