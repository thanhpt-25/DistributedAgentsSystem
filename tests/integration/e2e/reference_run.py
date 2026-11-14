"""Make the reference run of the load test suite (das-scale-resilience task 12.4; Phase 7 Req 4.4, 4.8).

Builds the Compose stack with the stub LLM and the load test Gateway config, runs a profile through ``das loadtest`` and
writes the Capacity_Report, so the numbers published in the README and in ``docs/capacity/`` can be reproduced::

    uv run python -m tests.integration.e2e.reference_run --out docs/capacity/reference-rabbitmq.json
    uv run python -m tests.integration.e2e.reference_run --transport http --out docs/capacity/reference-http.json

The stack is two Managers and two Workers (``research,summarize`` and ``code-review``, four concurrent Jobs each) on one
machine, with the stub LLM answering after ``--stub-latency-ms``. It is a measurement of that stack on that machine, not
a guarantee. The stack is removed afterwards."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from das.cli import main as das_main
from tests.integration.e2e.stackkit import API_KEY, DEPLOY, docker_available, running_stack

LOAD_OVERRIDE = Path(__file__).with_name("docker-compose.loadtest.yml")
HTTP_OVERRIDE = DEPLOY / "docker-compose.http-transport.yml"
HTTP_TOKEN = "reference-run-http-token-0123456789"
MANAGERS, WORKERS = 2, 2  # the replicas deploy/docker-compose.yml starts


def parse(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="reference_run", description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--out", required=True, help="where to write the Capacity_Report")
    parser.add_argument("--profile", default="ramp", help="built-in profile or YAML file (default: ramp)")
    parser.add_argument("--transport", choices=("rabbitmq", "http"), default="rabbitmq")
    parser.add_argument("--stub-latency-ms", type=float, default=50.0, help="every LLM answer takes this long")
    parser.add_argument("--max-error-rate", default="0.01")
    parser.add_argument("--max-p99-ms", default="10000")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse(argv)
    if not docker_available():
        print("Docker and docker compose are required", file=sys.stderr)
        return 4
    files = [LOAD_OVERRIDE] + ([HTTP_OVERRIDE] if args.transport == "http" else [])
    env = {"STUB_LATENCY_MS": f"{args.stub_latency_ms:g}"}
    if args.transport == "http":
        env["DAS_HTTP_BROKER_TOKEN"] = HTTP_TOKEN
    project = f"das-e2e-ref-{args.transport}"
    with tempfile.TemporaryDirectory() as tmp, running_stack(project, files, (), Path(tmp), extra_env=env) as stack:
        return das_main(
            ["loadtest", "--gateway", stack.gateway_url, "--key", API_KEY, "--profile", args.profile,
             "--out", args.out, "--transport", args.transport, "--setting", f"managers={MANAGERS}",
             "--setting", f"workers={WORKERS}", "--setting", f"stub_latency_ms={args.stub_latency_ms:g}",
             "--setting", "worker_concurrency=4", "--setting", "stack=docker compose on one machine",
             "--max-error-rate", args.max_error_rate, "--max-p99-ms", args.max_p99_ms, "--request-timeout", "120"],
            {},
        )


if __name__ == "__main__":
    raise SystemExit(main())
