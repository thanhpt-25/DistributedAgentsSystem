"""``das loadtest`` and ``das loadtest-compare`` through ``das.cli.main`` (das-scale-resilience task 12.1; Phase 7
Req 4.1, 4.7): the arguments, the report that is written, the exit codes, and that the API key appears nowhere."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from das.cli import build_parser, main
from das.errors import EXIT_CONFIG_ERROR, EXIT_DEPENDENCY_UNREACHABLE, EXIT_OK
from das.loadtest.command import KEY_ENV
from tests.unit.test_loadtest_runner import FakeGateway

KEY = "load-test-key-0123456789"
PROFILE = {"name": "tiny", "rate": 20, "duration": 0.25, "prompts": ["hello"]}


def stack(gateway: FakeGateway | None = None, agents_status: int = 200) -> httpx.AsyncBaseTransport:
    gateway = gateway or FakeGateway()

    async def agents(request: Any) -> JSONResponse:
        if request.headers.get("x-api-key") != KEY:
            return JSONResponse({}, status_code=401)
        return JSONResponse([], status_code=agents_status)

    app = Starlette(routes=[
        Route("/v1/agents", agents, methods=["GET"]),
        Route("/v1/requests", gateway.submit, methods=["POST"]),
        Route("/v1/requests/{rid}", gateway.status, methods=["GET"]),
    ])
    return httpx.ASGITransport(app=app)


def profile_file(tmp_path: Path, data: dict[str, Any] | None = None) -> str:
    path = tmp_path / "tiny.yaml"
    path.write_text(yaml.safe_dump(data or PROFILE), encoding="utf-8")
    return str(path)


def loadtest(
    tmp_path: Path, *extra: str, env: dict[str, str] | None = None, transport: Any = None, key: bool = True,
    profile: str | None = None,
) -> tuple[int, Path]:
    out = tmp_path / "reports" / "run.json"
    argv = [
        "loadtest", "--gateway", "http://stack:8080", "--profile", profile or profile_file(tmp_path),
        "--out", str(out), "--transport", "rabbitmq", "--poll-interval", "0.01", "--request-timeout", "5", *extra,
    ]
    if key:
        argv += ["--key", KEY]
    return main(argv, env or {}, http_transport=transport or stack()), out


# --------------------------------------------------------------------------- the arguments


def test_the_two_commands_are_registered_beside_the_others() -> None:
    parser = build_parser()
    args = parser.parse_args(["loadtest", "--gateway", "http://g:8080/", "--out", "r.json", "--transport", "http"])
    assert args.command == "loadtest" and args.gateway == "http://g:8080" and args.profile == "smoke"
    assert (args.poll_interval, args.request_timeout) == (0.5, 120.0)
    assert (args.max_error_rate, args.max_p99_ms) == (0.01, 10000.0)
    assert build_parser().parse_args(["loadtest-compare", "a.json", "b.json"]).max_regression == 0.1


@pytest.mark.parametrize("argv", [
    ["loadtest", "--out", "r.json", "--transport", "x"],                                        # no gateway
    ["loadtest", "--gateway", "g", "--out", "r.json", "--transport", "x"],                       # not a URL
    ["loadtest", "--gateway", "ftp://g", "--out", "r.json", "--transport", "x"],
    ["loadtest", "--gateway", "http://g", "--transport", "x"],                                   # no --out
    ["loadtest", "--gateway", "http://g", "--out", "r.json"],                                    # no --transport
    ["loadtest", "--gateway", "http://g", "--out", "r", "--transport", "x", "--poll-interval", "0"],
    ["loadtest", "--gateway", "http://g", "--out", "r", "--transport", "x", "--max-error-rate", "1.5"],
    ["loadtest", "--gateway", "http://g", "--out", "r", "--transport", "x", "--max-p99-ms", "nan"],
    ["loadtest-compare", "a.json"],
    ["loadtest-compare", "a.json", "b.json", "--max-regression", "-1"],
    ["loadtest-compare", "a.json", "b.json", "--max-regression", "11"],
    ["loadtest-compare", "a.json", "b.json", "--max-regression", "much"],
])
def test_bad_arguments_exit_2_before_anything_is_contacted(argv: list[str]) -> None:
    assert main(argv, {}) == 2


# --------------------------------------------------------------------------- a run


def test_a_run_writes_the_report_and_prints_a_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code, out = loadtest(tmp_path, "--setting", "managers=2", "--setting", "workers=2")
    assert code == EXIT_OK
    report = json.loads(out.read_text())
    assert report["schema"] == 1 and report["transport"] == "rabbitmq"
    assert report["settings"] == {"managers": "2", "workers": "2"}
    assert report["profile"]["name"] == "tiny" and report["results"]["requests"] == 5
    assert report["results"]["completed"] == 5 and report["results"]["errors"] == 0
    assert report["machine"]["cpu_count"] >= 1 and report["das_version"]
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["requests"] == 5 and summary["completed"] == 5 and "saturation_rps" in summary
    assert f"report written to {out}" in captured.err


def test_the_report_goes_into_a_new_directory_and_replaces_an_old_report(tmp_path: Path) -> None:
    code, out = loadtest(tmp_path)
    assert code == 0 and out.parent.name == "reports"
    out.write_text("old")
    assert loadtest(tmp_path)[0] == 0 and json.loads(out.read_text())["schema"] == 1
    assert not list(out.parent.glob("*.tmp")), "the temporary file was renamed away"


def test_an_unknown_profile_name_exits_2() -> None:
    assert main(["loadtest", "--gateway", "http://g", "--out", "r", "--transport", "x", "--profile", "nope",
                 "--key", KEY], {}) == EXIT_CONFIG_ERROR  # the built-ins are smoke, steady and ramp


def test_the_key_can_come_from_the_environment(tmp_path: Path) -> None:
    code, _ = loadtest(tmp_path, key=False, env={KEY_ENV: KEY})
    assert code == EXIT_OK


def test_the_key_appears_in_no_output_and_no_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code, out = loadtest(tmp_path, "--setting", "note=plain")
    captured = capsys.readouterr()
    assert code == 0 and KEY not in captured.out + captured.err + out.read_text()


def test_the_run_reports_errors_by_kind_and_still_exits_0(tmp_path: Path) -> None:
    code, out = loadtest(tmp_path, transport=stack(FakeGateway(submit_status=429)))
    report = json.loads(out.read_text())
    assert code == 0 and report["results"]["errors_by_kind"] == {"submit_429": 5} and report["saturation_rps"] is None


# --------------------------------------------------------------------------- what stops a run before it starts


def test_without_a_key_it_says_so_and_exits_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code, out = loadtest(tmp_path, key=False)
    assert code == EXIT_CONFIG_ERROR and not out.exists() and KEY_ENV in capsys.readouterr().err


def test_a_key_the_gateway_refuses_exits_2_and_does_not_print_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "r.json"
    code = main(["loadtest", "--gateway", "http://stack", "--profile", profile_file(tmp_path), "--out", str(out),
                 "--transport", "http", "--key", "some-other-key-0123456789"], {}, http_transport=stack())
    err = capsys.readouterr().err
    assert code == EXIT_CONFIG_ERROR and "refused the API key" in err and "some-other-key" not in err
    assert not out.exists()


def test_a_gateway_that_cannot_be_reached_exits_4(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    class Down(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

    code, out = loadtest(tmp_path, transport=Down())
    assert code == EXIT_DEPENDENCY_UNREACHABLE and not out.exists() and "not answering" in capsys.readouterr().err


def test_a_gateway_that_answers_an_error_to_the_probe_exits_4(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _ = loadtest(tmp_path, transport=stack(agents_status=503))
    assert code == EXIT_DEPENDENCY_UNREACHABLE and "answered 503" in capsys.readouterr().err


@pytest.mark.parametrize("extra", [["--setting", "no-equals"], ["--setting", "a=1", "--setting", "a=2"]])
def test_a_bad_setting_exits_2(tmp_path: Path, extra: list[str]) -> None:
    code, out = loadtest(tmp_path, *extra)
    assert code == EXIT_CONFIG_ERROR and not out.exists()


def test_a_bad_transport_name_exits_2(tmp_path: Path) -> None:
    out = tmp_path / "r.json"
    argv = ["loadtest", "--gateway", "http://stack", "--profile", profile_file(tmp_path), "--out", str(out),
            "--transport", "has space", "--key", KEY]
    assert main(argv, {}, http_transport=stack()) == EXIT_CONFIG_ERROR


def test_a_profile_that_cannot_be_used_exits_2_naming_the_field(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _ = loadtest(tmp_path, profile=profile_file(tmp_path, {"rate": 0, "duration": 5}))
    assert code == EXIT_CONFIG_ERROR and "rate must be above 0" in capsys.readouterr().err


# --------------------------------------------------------------------------- loadtest-compare


def report_file(tmp_path: Path, name: str, throughput: float, p99: float, profile: str = "steady") -> str:
    path = tmp_path / name
    path.write_text(json.dumps({
        "schema": 1, "profile": {"name": profile}, "transport": "rabbitmq",
        "results": {"throughput_rps": throughput, "total_ms": {"p99": p99}},
    }))
    return str(path)


def test_compare_exits_0_and_prints_the_verdict_when_nothing_regressed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["loadtest-compare", report_file(tmp_path, "a", 10, 1000), report_file(tmp_path, "b", 10.5, 990)], {})
    assert code == 0 and json.loads(capsys.readouterr().out)["regressed"] is False


def test_compare_exits_1_when_throughput_fell_or_p99_rose_beyond_the_ratio(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base = report_file(tmp_path, "a", 10, 1000)
    assert main(["loadtest-compare", base, report_file(tmp_path, "b", 7, 1000), "--max-regression", "0.2"], {}) == 1
    assert "regression: throughput fell" in capsys.readouterr().err
    assert main(["loadtest-compare", base, report_file(tmp_path, "c", 10, 1500), "--max-regression", "0.2"], {}) == 1
    assert "regression: p99 rose" in capsys.readouterr().err
    assert main(["loadtest-compare", base, report_file(tmp_path, "d", 9, 1100), "--max-regression", "0.2"], {}) == 0


def test_compare_exits_2_for_reports_it_cannot_compare(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    a = report_file(tmp_path, "a", 10, 1000)
    assert main(["loadtest-compare", a, report_file(tmp_path, "b", 10, 1000, profile="ramp")], {}) == 2
    assert "not comparable" in capsys.readouterr().err
    assert main(["loadtest-compare", a, str(tmp_path / "missing.json")], {}) == 2
    assert "cannot be read" in capsys.readouterr().err
    (tmp_path / "junk.json").write_text("not json")
    assert main(["loadtest-compare", a, str(tmp_path / "junk.json")], {}) == 2
    assert "is not JSON" in capsys.readouterr().err


def test_compare_prints_a_note_when_the_context_differs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    other = tmp_path / "other.json"
    other.write_text(json.dumps({
        "schema": 1, "profile": {"name": "steady"}, "transport": "http",
        "results": {"throughput_rps": 10, "total_ms": {"p99": 1000}},
    }))
    assert main(["loadtest-compare", report_file(tmp_path, "a", 10, 1000), str(other)], {}) == 0
    assert "note: transport differs" in capsys.readouterr().err
