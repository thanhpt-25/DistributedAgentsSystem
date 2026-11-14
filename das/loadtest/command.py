"""``das loadtest`` and ``das loadtest-compare`` (das-scale-resilience task 12.1; Phase 7 Req 4.1, 4.7).

``das loadtest --gateway URL --key KEY --profile NAME|FILE --out REPORT.json --transport NAME [--setting K=V ...]``
runs a profile against a running stack through its client API and writes the Capacity_Report. The API key comes from
``--key`` or ``DAS_LOADTEST_KEY`` and is never printed or written. Exit codes: 0 the run is written, 2 for a profile,
key or option that cannot be used, 4 when the Gateway cannot be reached.

``das loadtest-compare BASE.json NEW.json --max-regression RATIO`` exits 1 when throughput fell or p99 rose by more
than the ratio, 0 when it did not, 2 for reports that cannot be compared. The verdict is JSON on stdout."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import urlsplit

import httpx

from das.errors import EXIT_CONFIG_ERROR, EXIT_DEPENDENCY_UNREACHABLE, EXIT_OK
from das.loadtest.compare import CompareError, compare
from das.loadtest.profile import ProfileError, load_profile
from das.loadtest.report import (
    DEFAULT_MAX_ERROR_RATE, DEFAULT_MAX_P99_MS, Limits, ReportError, build_report, check_transport, parse_settings,
)
from das.loadtest.runner import DEFAULT_POLL_INTERVAL, DEFAULT_REQUEST_TIMEOUT, run_profile

KEY_ENV = "DAS_LOADTEST_KEY"
#: The comparison found a regression.
EXIT_REGRESSION = 1
DEFAULT_MAX_REGRESSION = 0.1


def _gateway_url(value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        parts = None
    if parts is None or parts.scheme not in ("http", "https") or not parts.netloc:
        raise argparse.ArgumentTypeError("must be an http:// or https:// URL")
    return value.rstrip("/")


def _positive(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number above 0") from None
    if not number > 0:  # also false for NaN
        raise argparse.ArgumentTypeError("must be a number above 0")
    return number


def _ratio(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number from 0 to 1") from None
    if not 0 <= number <= 1:
        raise argparse.ArgumentTypeError("must be a number from 0 to 1")
    return number


def _regression(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a ratio, 0.1 for ten percent") from None
    if not 0 <= number <= 10:
        raise argparse.ArgumentTypeError("must be a ratio from 0 to 10")
    return number


def add_parsers(sub: Any) -> None:
    """Register the two subcommands on the CLI's subparsers."""
    load = sub.add_parser(
        "loadtest",
        help="drive a running stack through the client API and write a Capacity_Report",
        description=f"The API key is read from --key or ${KEY_ENV}. Built-in profiles: smoke, steady, ramp.",
    )
    load.add_argument("--gateway", required=True, type=_gateway_url, help="Gateway_API base URL")
    load.add_argument("--key", help=f"API key (default: ${KEY_ENV})")
    load.add_argument("--profile", default="smoke", help="a built-in profile name or the path of a YAML file")
    load.add_argument("--out", required=True, help="where to write the Capacity_Report (JSON)")
    load.add_argument("--transport", required=True, help="the transport the stack runs on (rabbitmq, http, kafka)")
    load.add_argument(
        "--setting", action="append", default=[], metavar="KEY=VALUE",
        help="a setting of the stack, recorded in the report (managers=2, workers=2, stub_latency_ms=50)",
    )
    load.add_argument("--poll-interval", type=_positive, default=DEFAULT_POLL_INTERVAL, help="seconds between polls")
    load.add_argument(
        "--request-timeout", type=_positive, default=DEFAULT_REQUEST_TIMEOUT,
        help="seconds a Request may take to reach a terminal state",
    )
    load.add_argument(
        "--max-error-rate", type=_ratio, default=DEFAULT_MAX_ERROR_RATE,
        help="error rate a step may have and still count as coping (0 to 1)",
    )
    load.add_argument(
        "--max-p99-ms", type=_positive, default=DEFAULT_MAX_P99_MS,
        help="p99 of the total time a step may have and still count as coping",
    )

    cmp = sub.add_parser(
        "loadtest-compare",
        help="compare two Capacity_Reports; exit 1 when the new one regressed",
        description="Throughput fell or p99 rose by more than --max-regression (a ratio) is a regression.",
    )
    cmp.add_argument("base", help="the Capacity_Report to compare against")
    cmp.add_argument("new", help="the Capacity_Report of the change under test")
    cmp.add_argument(
        "--max-regression", type=_regression, default=DEFAULT_MAX_REGRESSION,
        help=f"allowed ratio, 0.1 for ten percent (default {DEFAULT_MAX_REGRESSION})",
    )


def _say(err: TextIO, message: str) -> None:
    err.write(message + "\n")


# --------------------------------------------------------------------------- das loadtest


async def _probe(client: httpx.AsyncClient) -> str | None:
    """None when the Gateway answers and accepts the key; else why not."""
    try:
        response = await client.get("/v1/agents", timeout=10)
    except httpx.HTTPError as exc:
        return f"the Gateway is not answering ({type(exc).__name__})"
    if response.status_code in (401, 403):
        return "the Gateway refused the API key"
    if response.status_code != 200:
        return f"the Gateway answered {response.status_code} to GET /v1/agents"
    return None


def _write(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def loadtest_main(
    args: argparse.Namespace,
    environ: Mapping[str, str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    out, err = out or sys.stdout, err or sys.stderr
    key = args.key or environ.get(KEY_ENV)
    if not key:
        _say(err, f"no API key: pass --key or set {KEY_ENV}")
        return EXIT_CONFIG_ERROR
    try:
        profile = load_profile(args.profile)
        check_transport(args.transport)
        settings = parse_settings(args.setting)
        limits = Limits(args.max_error_rate, args.max_p99_ms)
    except (ProfileError, ReportError) as exc:
        _say(err, str(exc))
        return EXIT_CONFIG_ERROR

    async def run() -> int:
        async with httpx.AsyncClient(
            base_url=args.gateway, headers={"X-API-Key": key}, timeout=30, transport=transport
        ) as client:
            problem = await _probe(client)
            if problem is not None:
                _say(err, problem.replace(key, "***"))
                return EXIT_CONFIG_ERROR if "refused the API key" in problem else EXIT_DEPENDENCY_UNREACHABLE
            _say(err, f"running profile {profile.name}: {profile.total_seconds:g} s of load against {args.gateway}")
            result = await run_profile(
                profile, client, poll_interval=args.poll_interval, request_timeout=args.request_timeout
            )
        report = build_report(result, transport=args.transport, settings=settings, limits=limits)
        _write(Path(args.out), report)
        summary = {k: report["results"][k] for k in ("requests", "completed", "errors", "error_rate", "throughput_rps")}
        summary["total_p99_ms"] = report["results"]["total_ms"]["p99"]
        summary["saturation_rps"] = report["saturation_rps"]
        out.write(json.dumps(summary, indent=2) + "\n")
        _say(err, f"report written to {args.out}")
        return EXIT_OK

    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        _say(err, "interrupted; no report written")
        return 130


# --------------------------------------------------------------------------- das loadtest-compare


def _read(path: str, which: str) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as exc:
        raise CompareError(f"the {which} report {path} cannot be read ({type(exc).__name__})") from None
    except ValueError:
        raise CompareError(f"the {which} report {path} is not JSON") from None


def compare_main(args: argparse.Namespace, *, out: TextIO | None = None, err: TextIO | None = None) -> int:
    out, err = out or sys.stdout, err or sys.stderr
    try:
        verdict = compare(_read(args.base, "base"), _read(args.new, "new"), args.max_regression)
    except CompareError as exc:
        _say(err, str(exc))
        return EXIT_CONFIG_ERROR
    out.write(json.dumps(verdict.to_dict(), indent=2) + "\n")
    for note in verdict.notes:
        _say(err, f"note: {note}")
    for regression in verdict.regressions:
        _say(err, f"regression: {regression}")
    return EXIT_REGRESSION if verdict.regressed else EXIT_OK


__all__ = ["EXIT_REGRESSION", "KEY_ENV", "add_parsers", "compare_main", "loadtest_main"]
