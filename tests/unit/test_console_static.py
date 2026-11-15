"""The admin console (``console/``, React + CoreUI) is built by npm, so Python never imports it. These checks catch
what would otherwise only show up in a browser or a Docker build: a call to a Gateway route that does not exist, an
unpinned dependency, a Dockerfile that copies a missing file, a proxy that would buffer the SSE stream.

The TypeScript itself is covered by ``npm run typecheck`` and ``npm test`` inside ``console/``."""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from das.tenancy.scopes import ROUTE_SCOPES

ROOT = Path(__file__).resolve().parents[2]
CONSOLE = ROOT / "console"
SOURCES = sorted(p for p in (CONSOLE / "src").rglob("*") if p.suffix in {".ts", ".tsx"} and ".test." not in p.name)
HOLE = "\0"  # a ``${...}`` in the TypeScript: any one path segment (an id, or a verb held in a variable)


def test_console_has_sources() -> None:
    assert SOURCES, "console/src is empty"


def _segments_match(called: list[str], template: list[str]) -> bool:
    return len(called) == len(template) and all(
        HOLE in c or t.startswith("{") or c == t for c, t in zip(called, template, strict=True)
    )


def test_every_api_path_the_console_calls_is_a_gateway_route() -> None:
    templates = [path.strip("/").split("/") for _, path in ROUTE_SCOPES]
    called = set()
    for src in SOURCES:
        for raw in re.findall(r"[\"'`](/(?:v1/|healthz)[^\"'`?]*)", src.read_text()):
            called.add(re.sub(r"\$\{[^}]*\}", HOLE, raw).strip("/"))
    assert called, "found no API paths in the console"
    unknown = sorted(c for c in called if not any(_segments_match(c.split("/"), t) for t in templates))
    assert not unknown, f"not in ROUTE_SCOPES: {unknown}"


def test_console_never_assigns_html() -> None:
    """Gateway strings (prompts, errors, hostnames) must stay text: React escapes them unless these are used."""
    unsafe = re.compile(r"dangerouslySetInnerHTML|\.innerHTML\b|insertAdjacentHTML\s*\(")
    for src in SOURCES:
        assert not unsafe.search(src.read_text()), src.name


def test_dependencies_are_exact_pins_and_use_coreui() -> None:
    package = json.loads((CONSOLE / "package.json").read_text())
    deps = {**package["dependencies"], **package["devDependencies"]}
    assert {"@coreui/react", "@coreui/coreui", "react"} <= deps.keys()
    loose = {name: v for name, v in deps.items() if not re.fullmatch(r"\d+\.\d+\.\d+", v)}
    assert not loose, f"pin exactly, as the Python side does: {loose}"
    assert (CONSOLE / "package-lock.json").is_file(), "the Dockerfile runs `npm ci`, which needs the lockfile"


def test_dockerfile_copies_files_that_exist_and_dockerignore_lets_them_through() -> None:
    dockerfile = (ROOT / "deploy/docker/console.Dockerfile").read_text()
    sources = [tok for line in dockerfile.splitlines() if line.startswith("COPY ") and "--from" not in line
               for tok in line.split()[1:-1]]
    assert sources
    for source in sources:
        assert (ROOT / source).exists(), f"console.Dockerfile copies missing {source}"
    ignore = (ROOT / ".dockerignore").read_text().splitlines()
    assert "!deploy/config/console.nginx.conf" in ignore, "`deploy` is ignored wholesale: re-include the nginx conf"
    assert "console/node_modules" in ignore


def test_proxy_keeps_sse_unbuffered_and_sets_a_csp() -> None:
    conf = (ROOT / "deploy/config/console.nginx.conf").read_text()
    assert "location /v1/" in conf and "proxy_buffering off" in conf and "proxy_read_timeout" in conf
    assert "Content-Security-Policy" in conf and "script-src" not in conf.replace("default-src 'self'", ""), (
        "scripts fall back to default-src 'self': no inline script, no third-party script"
    )


def test_compose_override_builds_the_console_image() -> None:
    compose = (ROOT / "deploy/docker-compose.console.yml").read_text()
    assert "deploy/docker/console.Dockerfile" in compose and "depends_on" in compose
    assert "networks: [das]" in compose, "without the stack's network nginx cannot resolve `gateway`"


def test_console_port_is_loopback_only_like_every_other_operator_ui() -> None:
    """The console carries the operator's API key over plain HTTP: it must not listen on every interface."""
    compose = yaml.safe_load((ROOT / "deploy/docker-compose.console.yml").read_text())
    ports = compose["services"]["console"]["ports"]
    assert ports and all(str(p).startswith("127.0.0.1:") for p in ports), ports


def test_ci_runs_the_console_checks_with_pinned_actions() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"]["console"]
    assert "needs" not in job and "if" not in job, "the console job runs on every push, like `fast`"
    assert job["defaults"]["run"]["working-directory"] == "console"
    runs = [s["run"] for s in job["steps"] if "run" in s]
    assert runs == [
        "npm ci --no-audit --no-fund", "npm run typecheck", "npm run lint", "npm run test:coverage", "npm run build",
        "npm run e2e:install", "npm run e2e",
    ]
    for step in job["steps"]:
        if "uses" in step:
            assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", step["uses"]), f"pin {step['uses']} to a commit SHA"


def test_dockerfile_pins_images_by_digest_and_runs_unprivileged() -> None:
    dockerfile = (ROOT / "deploy/docker/console.Dockerfile").read_text()
    stages = [line for line in dockerfile.splitlines() if line.startswith("FROM ")]
    assert len(stages) == 2
    for stage in stages:
        assert re.search(r"@sha256:[0-9a-f]{64}", stage), f"pin the base image by digest: {stage}"
    assert "nginx-unprivileged" in stages[-1], "the runtime stage must not run as root"
    assert "/etc/nginx/templates/default.conf.template" in dockerfile, "the entrypoint renders the config from here"
    assert "DAS_GATEWAY_URL=" in dockerfile and "DAS_RESOLVER=" in dockerfile, "the template's variables need defaults"
    assert "HEALTHCHECK" in dockerfile


def test_nginx_config_is_a_template_that_listens_on_an_unprivileged_port() -> None:
    conf = (ROOT / "deploy/config/console.nginx.conf").read_text()
    assert "${DAS_GATEWAY_URL}" in conf and "${DAS_RESOLVER}" in conf, "no hardcoded Compose-only hostnames"
    assert re.search(r"listen\s+8080;", conf), "a non-root user cannot bind port 80"
    compose = yaml.safe_load((ROOT / "deploy/docker-compose.console.yml").read_text())
    assert all(str(p).endswith(":8080") for p in compose["services"]["console"]["ports"])


MAX_FUNCTION_LINES = 50
TOP_LEVEL_FUNCTION = re.compile(r"^(?:export )?(?:default )?(?:async )?function \w+")


def test_no_top_level_function_exceeds_the_size_limit() -> None:
    """The code-review rule: functions of 50 lines at most. Each card of a page is its own component."""
    too_long = []
    for src in SOURCES:
        lines = src.read_text().splitlines()
        start = None
        for number, line in enumerate(lines, 1):
            if start is None and TOP_LEVEL_FUNCTION.match(line):
                start = number
            elif start is not None and line.startswith("}"):
                if number - start > MAX_FUNCTION_LINES:
                    too_long.append(f"{src.relative_to(CONSOLE)}:{start} is {number - start} lines")
                start = None
    assert not too_long, too_long
