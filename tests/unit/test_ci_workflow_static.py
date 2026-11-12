"""Static checks for the CI workflow and the README Development section.

Parses `.github/workflows/ci.yml` with `yaml.safe_load` (plus the raw text for
the `# vX.Y.Z` comments that YAML drops) and checks it against README.md, so a
CI command change without a README change fails the fast suite.

Each ``check_*`` function is pure: it takes the workflow text (and README text
where needed) and returns a list of problems, so it can be run on a mutated copy.

Validates (das-hardening-ci): Requirements 7.1, 7.2, 7.3, 7.7, 7.8, 8.1, 8.2,
8.3, 8.7, 8.9, 9.1, 9.2, 9.5, 9.6, 12.1, 12.7
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
README = ROOT / "README.md"
INTEGRATION = ROOT / "tests" / "integration"

PINNED_REF = re.compile(r"^[\w.-]+/[\w.-]+@[0-9a-f]{40}$")
USES_LINE = re.compile(r"^\s*(?:-\s+)?uses:\s*(?P<ref>\S+)(?P<rest>.*)$")
VERSION_COMMENT = re.compile(r"^\s+#\s*v\d+\.\d+\.\d+\s*$")
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
CHECK_CMD = re.compile(r"^uv run (ruff|mypy|lint-imports|pytest)\b")
FENCE = re.compile(r"^```[^\n]*\n(.*?)^```", re.MULTILINE | re.DOTALL)

EXPECTED_TIMEOUTS = {"fast": 15, "client": 10, "integration": 30, "e2e": 30, "k8s": 30}
EXPECTED_FAST_CHECKS = [
    "uv run ruff check",
    "uv run mypy",
    "uv run lint-imports",
    "uv run pytest tests/unit tests/property tests/contract",
]
# Chart checks of the fast job (das-multi-tenant task 11.2; Phase 5 Req 11.1): each step name is the command it runs.
EXPECTED_CHART_CHECKS = ["helm lint", "helm template", "kubeconform -strict"]
CHART_VALUES_FILES = ("deploy/helm/das/ci/values-min.yaml", "deploy/helm/das/ci/values-all.yaml")
TOOLS_STEP = "Install helm and kubeconform"
KIND_TOOLS_STEP = "Install helm, kind and kubectl"
# Requirement 11.5: every tool the CI installs for the chart has an exact version and a pinned SHA-256.
CHART_TOOLS = ("HELM", "KUBECONFORM", "KIND", "KUBECTL")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
KIND_JOB_NAME = "Kind_Job"
KIND_COMMAND = "uv run pytest tests/integration/k8s"
KIND_CLUSTER = "das-e2e-k8s"  # the cluster name the test harness creates (tests/integration/k8s/cluster.py)

# Client_Job (das-client-api task 10.1; Phase 4 Req 9.1, 9.10, 11.5). The Client_Package supports more Pythons than
# the server and is not a uv workspace member, so its own tests run in an isolated environment, once per Python.
ROOT_PYPROJECT = ROOT / "pyproject.toml"
CLIENT_PYPROJECT = ROOT / "clients" / "python" / "pyproject.toml"
CLIENT_TEST_TOOLS = ("pytest", "pytest-asyncio", "respx")
CLIENT_COMMAND_PREFIX = "uv run --isolated --no-project --with-editable clients/python "
CLIENT_COMMAND_SUFFIX = " pytest clients/python/tests -c clients/python/pyproject.toml --rootdir clients/python"
PYTHON_RANGE = re.compile(r">=3\.(?P<low>\d+),<3\.(?P<high>\d+)")
WITH_PIN = re.compile(r"--with (?P<name>[\w.-]+)==(?P<version>\S+)")
MAIN_OR_MANUAL = (
    "github.event_name == 'workflow_dispatch' || (github.event_name == 'push' && github.ref == 'refs/heads/main')"
)
UV_VERSION_EXPR = "${{ env.UV_VERSION }}"


# --------------------------------------------------------------------- helpers


def _parse(text: str) -> dict[Any, Any]:
    data = yaml.safe_load(text)
    assert isinstance(data, dict), "workflow file is not a YAML mapping"
    return data


def _triggers(data: dict[Any, Any]) -> Any:
    # PyYAML (YAML 1.1) reads the bare key `on` as boolean True.
    return data.get("on", data.get(True))


def _norm_expr(value: Any) -> str:
    text = " ".join(str(value).split())
    if text.startswith("${{") and text.endswith("}}"):
        text = text[3:-2].strip()
    return text


def _jobs(data: dict[Any, Any]) -> dict[str, Any]:
    jobs = data.get("jobs")
    return jobs if isinstance(jobs, dict) else {}


def _uv_commands(run: str) -> list[str]:
    """`uv run ...` / `uv sync ...` lines of a `run:` script (echo, exports skipped)."""
    lines = (line.strip() for line in run.splitlines())
    return [line for line in lines if line.startswith(("uv run ", "uv sync"))]


def _workflow_commands(data: dict[Any, Any]) -> list[tuple[str, str, str]]:
    """(job, step name, command) for every uv command run by any step."""
    found = []
    for job_id, job in _jobs(data).items():
        for step in job.get("steps", []):
            run = step.get("run")
            if isinstance(run, str):
                for cmd in _uv_commands(run):
                    found.append((job_id, str(step.get("name")), cmd))
    return found


def development_section(readme: str) -> str:
    """README text from `## Development` up to the next level-2 heading."""
    match = re.search(r"^## Development[^\n]*\n(.*?)(?=^## |\Z)", readme, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


def code_blocks(section: str) -> list[str]:
    return [block.strip() for block in FENCE.findall(section)]


# ---------------------------------------------------------------------- checks


def check_pinned_actions(text: str) -> list[str]:
    problems = []
    raw_count = 0
    for lineno, line in enumerate(text.splitlines(), 1):
        match = USES_LINE.match(line)
        if not match:
            continue
        raw_count += 1
        ref, rest = match.group("ref").strip("'\""), match.group("rest")
        if not PINNED_REF.match(ref):
            problems.append(f"line {lineno}: `uses: {ref}` is not owner/repo@<40-hex SHA>")
        if not VERSION_COMMENT.match(rest):
            problems.append(f"line {lineno}: `uses: {ref}` lacks a trailing `# vX.Y.Z` comment")
    parsed_count = sum(1 for job in _jobs(_parse(text)).values() for step in job.get("steps", []) if "uses" in step)
    if raw_count != parsed_count:
        problems.append(
            f"found {raw_count} `uses:` lines but {parsed_count} parsed `uses` steps; "
            "write each `uses:` on its own line"
        )
    return problems


def check_uv_version(text: str) -> list[str]:
    data = _parse(text)
    problems = []
    version = str((data.get("env") or {}).get("UV_VERSION", ""))
    if not SEMVER.match(version):
        problems.append(f"workflow env UV_VERSION is {version!r}, expected exact X.Y.Z")
    for job_id, job in _jobs(data).items():
        if "UV_VERSION" in (job.get("env") or {}):
            problems.append(f"job {job_id} overrides UV_VERSION")
        setups = [s for s in job.get("steps", []) if str(s.get("uses", "")).startswith("astral-sh/setup-uv@")]
        if not setups:
            problems.append(f"job {job_id} has no astral-sh/setup-uv step")
        for step in setups:
            got = (step.get("with") or {}).get("version")
            if got != UV_VERSION_EXPR:
                problems.append(f"job {job_id}: setup-uv version is {got!r}, expected {UV_VERSION_EXPR!r}")
    return problems


def check_permissions_and_secrets(text: str) -> list[str]:
    data = _parse(text)
    problems = []
    if data.get("permissions") != {"contents": "read"}:
        problems.append(f"workflow permissions are {data.get('permissions')!r}, expected {{'contents': 'read'}}")
    for job_id, job in _jobs(data).items():
        if "permissions" in job and job["permissions"] != {"contents": "read"}:
            problems.append(f"job {job_id} declares permissions {job['permissions']!r}")
    if "secrets." in text:
        problems.append("workflow references `secrets.`")
    return problems


def check_jobs_and_triggers(text: str) -> list[str]:
    data = _parse(text)
    problems = []
    jobs = _jobs(data)
    if set(jobs) != set(EXPECTED_TIMEOUTS):
        problems.append(f"jobs are {sorted(jobs)}, expected {sorted(EXPECTED_TIMEOUTS)}")
    for job_id, minutes in EXPECTED_TIMEOUTS.items():
        got = (jobs.get(job_id) or {}).get("timeout-minutes")
        if got != minutes:
            problems.append(f"job {job_id}: timeout-minutes is {got!r}, expected {minutes}")

    fast = jobs.get("fast") or {}
    if "needs" in fast or "if" in fast:
        problems.append("job fast must have no `needs`/`if` (it runs on every trigger)")
    for job_id in ("integration", "e2e", "k8s"):
        job = jobs.get(job_id) or {}
        needs = job.get("needs")
        if needs not in ("fast", ["fast"]):
            problems.append(f"job {job_id}: needs is {needs!r}, expected 'fast' only")
        if _norm_expr(job.get("if", "")) != MAIN_OR_MANUAL:
            problems.append(f"job {job_id}: if is {job.get('if')!r}, expected {MAIN_OR_MANUAL!r}")

    concurrency = data.get("concurrency") or {}
    if concurrency.get("cancel-in-progress") is not True:
        problems.append("concurrency.cancel-in-progress is not true")
    group = str(concurrency.get("group", ""))
    for part in ("github.event_name", "github.head_ref || github.ref"):
        if part not in group:
            problems.append(f"concurrency.group {group!r} does not contain {part!r}")

    on = _triggers(data)
    if not isinstance(on, dict) or set(on) != {"push", "pull_request", "workflow_dispatch"}:
        problems.append(f"triggers are {on!r}, expected push, pull_request, workflow_dispatch")
        return problems
    push = on["push"] or {}
    if push.get("branches") != ["**"] or set(push) - {"branches"}:
        problems.append(f"push trigger is {push!r}, expected branches ['**'] only")
    pr_types = set((on["pull_request"] or {}).get("types", []))
    if pr_types != {"opened", "synchronize", "reopened"}:
        problems.append(f"pull_request types are {sorted(pr_types)}, expected opened/synchronize/reopened")
    return problems


def check_fast_steps(text: str) -> list[str]:
    steps = (_jobs(_parse(text)).get("fast") or {}).get("steps", [])
    problems = []
    ids = [s.get("id") for s in steps]
    if "install" not in ids:
        return ["job fast has no step with id `install`"]
    checks = steps[ids.index("install") + 1 :]
    names = [s.get("name") for s in checks]
    expected = EXPECTED_FAST_CHECKS + EXPECTED_CHART_CHECKS
    if names != expected:
        problems.append(f"fast check steps after install are {names}, expected {expected}")
    for step in checks:
        name = step.get("name")
        run = str(step.get("run", ""))
        if name in EXPECTED_CHART_CHECKS:
            if name not in run:
                problems.append(f"fast step {name!r} does not run `{name}`")
            for values in CHART_VALUES_FILES:  # the default values and the file that enables every feature
                if values not in run:
                    problems.append(f"fast step {name!r} does not use {values}")
        else:
            cmds = _uv_commands(run)
            if cmds != [name]:
                problems.append(f"fast step {name!r} runs {cmds}, expected exactly [{name!r}]")
        cond = _norm_expr(step.get("if", ""))
        for part in ("!cancelled()", "steps.install.outcome == 'success'"):
            if part not in cond:
                problems.append(f"fast step {name!r}: if {cond!r} does not contain {part!r}")
    # The chart's unit tests run inside the pytest step, so the real helm has to be installed before it (Req 11.1).
    before = [s.get("name") for s in steps[: ids.index("install")]]
    if TOOLS_STEP not in before:
        problems.append(f"job fast must run {TOOLS_STEP!r} before the install step, so the chart tests find helm")
    return problems


def _run_text(job: dict[Any, Any]) -> str:
    return "\n".join(str(s.get("run", "")) for s in job.get("steps", []))


def check_chart_tool_pins(text: str) -> list[str]:
    """Requirement 11.5: exact versions, pinned SHA-256s, a verification after every download, nothing 'latest'."""
    data = _parse(text)
    env = data.get("env") or {}
    problems = []
    for tool in CHART_TOOLS:
        version, digest = str(env.get(f"{tool}_VERSION", "")), str(env.get(f"{tool}_SHA256", ""))
        if not SEMVER.match(version):
            problems.append(f"workflow env {tool}_VERSION is {version!r}, expected exact X.Y.Z")
        if not SHA256.match(digest):
            problems.append(f"workflow env {tool}_SHA256 is {digest!r}, expected 64 lowercase hex characters")
    if not SEMVER.match(str(env.get("KUBERNETES_SCHEMA_VERSION", ""))):
        problems.append("workflow env KUBERNETES_SCHEMA_VERSION is not an exact X.Y.Z")
    if re.search(r"\blatest\b|stable\.txt", text):
        problems.append("workflow mentions `latest` or `stable.txt`: every tool version must be pinned")
    for job_id, job in _jobs(data).items():
        for step in job.get("steps", []):
            run = str(step.get("run", ""))
            downloads, checks = len(re.findall(r"\bcurl\b", run)), len(re.findall(r"sha256sum -c", run))
            if downloads != checks:
                problems.append(
                    f"job {job_id} step {step.get('name')!r}: {downloads} download(s) but {checks} `sha256sum -c`"
                )
    fast = _run_text(_jobs(data).get("fast") or {})
    if "${KUBERNETES_SCHEMA_VERSION}" not in fast or "-kubernetes-version" not in fast:
        problems.append("job fast does not pass the pinned Kubernetes schema version to kubeconform")
    return problems


def check_kind_job(text: str) -> list[str]:
    """Requirement 11.2: the Kind_Job builds the images, installs pinned tools, runs the cluster tests, keeps logs."""
    job = _jobs(_parse(text)).get("k8s")
    if job is None:
        return ["workflow has no job `k8s`"]
    problems = []
    if job.get("name") != KIND_JOB_NAME:
        problems.append(f"job k8s is named {job.get('name')!r}, expected {KIND_JOB_NAME!r} (branch protection uses it)")
    env = job.get("env") or {}
    for key in ("DAS_REQUIRE_DOCKER", "DAS_K8S_E2E"):
        if env.get(key) != "1":
            problems.append(f"job k8s: env {key} is {env.get(key)!r}, expected '1'")
    steps = job.get("steps", [])
    by_name = {str(s.get("name")): s for s in steps}
    runs = _run_text(job)
    if KIND_COMMAND not in [cmd for cmd in _uv_commands(runs)]:
        problems.append(f"job k8s does not run `{KIND_COMMAND}`")
    tools = by_name.get(KIND_TOOLS_STEP)
    if tools is None:
        problems.append(f"job k8s has no step {KIND_TOOLS_STEP!r}")
    else:
        for tool in ("helm", "kind", "kubectl"):
            if f"{tool.upper()}_SHA256" not in str(tools.get("run", "")):
                problems.append(f"{KIND_TOOLS_STEP!r} does not verify {tool} against its pinned SHA-256")
    build = " ".join(str(s.get("run", "")) for s in steps if "docker build" in str(s.get("run", "")))
    for role in ("gateway", "manager", "worker"):
        if role not in build:
            problems.append(f"job k8s does not build the {role} image")
    if "deploy/docker/" not in build or "das-" not in build:
        problems.append("job k8s builds images that are not the das-<role>:local the chart tests load")
    uploads = [s for s in steps if str(s.get("uses", "")).startswith("actions/upload-artifact@")]
    if not uploads or "failure()" not in _norm_expr(uploads[0].get("if", "")):
        problems.append("job k8s does not upload the pod logs when it fails")
    if not any("DAS_K8S_LOG_DIR" in str(s.get("env", "")) for s in steps):
        problems.append("job k8s never sets DAS_K8S_LOG_DIR, so the harness writes no logs to upload")
    cleanup = [s for s in steps if f"kind delete cluster --name {KIND_CLUSTER}" in str(s.get("run", ""))]
    if not cleanup or "always()" not in _norm_expr(cleanup[0].get("if", "")):
        problems.append("job k8s does not always delete its kind cluster")
    for key in ("if", "needs"):
        if key not in job:
            problems.append(f"job k8s has no `{key}`")
    return problems


def check_integration_ignores_the_cluster_tests(text: str) -> list[str]:
    """The cluster tests run in the Kind_Job only; the Integration_Job must not collect them a second time."""
    commands = [cmd for job, _, cmd in _workflow_commands(_parse(text)) if job == "integration"]
    suite = [c for c in commands if c.startswith("uv run pytest tests/integration ") and "--count" not in c]
    if len(suite) != 1:
        return [f"job integration runs {suite} for its suite, expected exactly one command"]
    return (
        []
        if "--ignore=tests/integration/k8s" in suite[0]
        else ["job integration does not ignore tests/integration/k8s"]
    )


def client_python_versions() -> list[str]:
    """Every minor version of the Python range the Client_Package claims, as the strings a matrix needs."""
    spec = tomllib.loads(CLIENT_PYPROJECT.read_text(encoding="utf-8"))["project"]["requires-python"]
    match = PYTHON_RANGE.fullmatch(spec.replace(" ", ""))
    assert match, f"clients/python requires-python {spec!r} is not '>=3.A,<3.B'; teach this test the new form"
    return [f"3.{minor}" for minor in range(int(match["low"]), int(match["high"]))]


def root_dev_pins() -> dict[str, str]:
    """``name -> version`` for the exact pins (``name==version``) of the root project's dev group."""
    dev = tomllib.loads(ROOT_PYPROJECT.read_text(encoding="utf-8"))["dependency-groups"]["dev"]
    return {name: version for name, sep, version in (entry.partition("==") for entry in dev) if sep}


def check_client_job(text: str) -> list[str]:
    """The `client` job: one run of the Client_Package's tests per supported Python, with the dev group's pins."""
    job = _jobs(_parse(text)).get("client")
    if job is None:
        return ["workflow has no job `client`"]
    problems = []
    for key in ("needs", "if"):
        if key in job:
            problems.append(f"job client must have no `{key}`: it runs on every trigger and is not gated by fast")

    strategy = job.get("strategy") or {}
    if strategy.get("fail-fast") is not False:
        problems.append("job client: strategy.fail-fast must be false, so one Python's failure does not hide another's")
    expected = client_python_versions()
    got = (strategy.get("matrix") or {}).get("python")
    if got != expected:
        problems.append(f"job client: matrix.python is {got!r}, expected the quoted strings {expected!r}")

    steps = job.get("steps", [])
    setups = [s for s in steps if str(s.get("uses", "")).startswith("astral-sh/setup-uv@")]
    if not setups:
        problems.append("job client has no astral-sh/setup-uv step")
    for step in setups:
        value = _norm_expr((step.get("with") or {}).get("python-version", ""))
        if value != "matrix.python":
            problems.append(f"job client: setup-uv python-version is {value!r}, expected '${{{{ matrix.python }}}}'")

    commands = [cmd for step in steps if isinstance(step.get("run"), str) for cmd in _uv_commands(step["run"])]
    if len(commands) != 1:
        return [*problems, f"job client runs {commands}, expected exactly one uv command (no `uv sync`)"]
    command = commands[0]
    if not (command.startswith(CLIENT_COMMAND_PREFIX) and command.endswith(CLIENT_COMMAND_SUFFIX)):
        shape = f"{CLIENT_COMMAND_PREFIX}...{CLIENT_COMMAND_SUFFIX}"
        problems.append(f"job client command {command!r} does not have the shape `{shape}`")
    pins = {m["name"]: m["version"] for m in WITH_PIN.finditer(command)}
    dev = root_dev_pins()
    wanted = {name: dev.get(name) for name in CLIENT_TEST_TOOLS}
    if pins != wanted:
        problems.append(f"job client pins {pins} differ from the root dev group's {wanted}")
    return problems


def check_readme_matches_workflow(text: str, readme: str) -> list[str]:
    section = development_section(readme)
    if not section:
        return ["README.md has no `## Development` section"]
    blocks = set(code_blocks(section))
    problems = []
    commands = _workflow_commands(_parse(text))
    for job_id, name, cmd in commands:
        if cmd not in blocks:
            problems.append(f"job {job_id} step {name!r}: `{cmd}` is not its own code block in README Development")
    workflow_cmds = {cmd for _, _, cmd in commands}
    for block in blocks:
        for line in block.splitlines():
            line = line.strip()
            if CHECK_CMD.match(line) and line not in workflow_cmds:
                problems.append(f"README Development command `{line}` is not run by any workflow step")
    return problems


def check_no_module_level_skip(root: Path) -> list[str]:
    return [
        f"{path.relative_to(ROOT)} uses allow_module_level=True"
        for path in sorted(root.rglob("*.py"))
        if "allow_module_level=True" in path.read_text(encoding="utf-8")
    ]


# ----------------------------------------------------------------------- tests


@pytest.fixture(scope="module")
def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def readme_text() -> str:
    return README.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "check",
    [
        check_pinned_actions,
        check_uv_version,
        check_permissions_and_secrets,
        check_jobs_and_triggers,
        check_fast_steps,
        check_client_job,
        check_chart_tool_pins,
        check_kind_job,
        check_integration_ignores_the_cluster_tests,
    ],
    ids=lambda f: f.__name__,
)
def test_workflow(check: Any, workflow_text: str) -> None:
    problems = check(workflow_text)
    assert not problems, "\n".join(problems)


#: Edits to a correct workflow that the chart and Kind_Job checks must each notice: name -> (check, old, new).
CHART_FAULTS = {
    "an unpinned helm version": (check_chart_tool_pins, 'HELM_VERSION: "4.3.1"', 'HELM_VERSION: "4"'),
    "a malformed kind checksum": (check_chart_tool_pins, "KIND_SHA256: ", "KIND_SHA256: 0x"),
    "a download without its checksum": (
        check_chart_tool_pins,
        'echo "${KUBECONFORM_SHA256}  kubeconform.tgz" | sha256sum -c -\n',
        "",
    ),
    "a floating tool version": (check_chart_tool_pins, "v${KUBECTL_VERSION}", "stable.txt"),
    "kubeconform without the schema version": (check_chart_tool_pins, "-kubernetes-version", "-strict-version"),
    "the fast job loses its helm install": (
        check_fast_steps,
        "      - name: Install helm and kubeconform\n",
        "      - name: Get tools\n",
    ),
    "helm lint skips the all-features values": (
        check_fast_steps,
        "ci/values-all.yaml; do\n            helm lint",
        "ci/values-min.yaml; do\n            helm lint",
    ),
    "a Kind_Job that is not Kind_Job": (check_kind_job, "name: Kind_Job", "name: Cluster"),
    "a Kind_Job without the opt-in": (check_kind_job, '      DAS_K8S_E2E: "1"', '      DAS_K8S_E2E: "0"'),
    "a Kind_Job without DAS_REQUIRE_DOCKER": (
        check_kind_job,
        '      DAS_REQUIRE_DOCKER: "1"',
        '      DAS_REQUIRE_DOCKER: "0"',
    ),
    "a Kind_Job that never builds the worker image": (check_kind_job, "gateway manager worker", "gateway manager"),
    "a Kind_Job that keeps its cluster": (
        check_kind_job,
        "kind delete cluster --name das-e2e-k8s",
        "kind get clusters",
    ),
    "a Kind_Job without pod logs": (check_kind_job, "DAS_K8S_LOG_DIR:", "DAS_OTHER_DIR:"),
    "a Kind_Job that skips the cluster tests": (
        check_kind_job,
        "run: uv run pytest tests/integration/k8s",
        "run: echo skipped",
    ),
    "an Integration_Job that collects the cluster tests": (
        check_integration_ignores_the_cluster_tests,
        "run: uv run pytest tests/integration --ignore=tests/integration/e2e --ignore=tests/integration/k8s",
        "run: uv run pytest tests/integration --ignore=tests/integration/e2e",
    ),
}


@pytest.mark.parametrize("fault", CHART_FAULTS)
def test_chart_checks_catch(fault: str, workflow_text: str) -> None:
    check, old, new = CHART_FAULTS[fault]
    # A Kind_Job fault edits the k8s job only: its env lines look like the other jobs'.
    head, marker, tail = workflow_text.partition("\n  k8s:\n") if check is check_kind_job else (workflow_text, "", "")
    target = marker + tail if marker else workflow_text
    assert old in target, f"the workflow no longer contains {old!r}; update this fault"
    mutated = head + target.replace(old, new, 1) if marker else workflow_text.replace(old, new, 1)
    assert check(mutated), f"{check.__name__} missed: {fault}"


def test_kind_job_check_fails_when_the_job_is_missing(workflow_text: str) -> None:
    assert check_kind_job(workflow_text.replace("  k8s:\n", "  k8s_gone:\n", 1)) == ["workflow has no job `k8s`"]


def test_readme_ci_section_names_every_job(readme_text: str) -> None:
    """Requirement 11.6: the job names are what branch protection refers to."""
    section = re.search(r"^## Continuous integration\n(.*?)(?=^## |\Z)", readme_text, re.MULTILINE | re.DOTALL)
    assert section, "README.md has no `## Continuous integration` section"
    for job in ("Fast_Job", "Client_Job", "Integration_Job", "E2E_Job", KIND_JOB_NAME):
        assert job in section.group(1), f"the README CI section does not name {job}"
    assert "`k8s`" in section.group(1) and "DAS_K8S_E2E" in section.group(1)


#: Edits to a correct workflow that `check_client_job` must each notice: name -> (text to replace, replacement).
CLIENT_JOB_FAULTS = {
    "a matrix without 3.12": ('["3.10", "3.11", "3.12", "3.13"]', '["3.10", "3.11", "3.13"]'),
    "an unquoted 3.10 (YAML reads it as the float 3.1)": ('["3.10", "3.11"', '[3.10, "3.11"'),
    "a drifted pytest pin": ("--with pytest==", "--with pytest==0."),
    "a drifted respx pin": ("--with respx==", "--with respx==0."),
    "no python-version on setup-uv": ("          python-version: ${{ matrix.python }}\n", ""),
    "a client job gated on fast": ("  client:\n", "  client:\n    needs: fast\n"),
    "fail-fast left on": ("      fail-fast: false\n", ""),
    "a uv sync step": (
        "      - name: uv run pytest clients/python/tests\n",
        "      - name: uv sync --locked\n"
        "        run: uv sync --locked\n"
        "      - name: uv run pytest clients/python/tests\n",
    ),
    "a command that is not isolated": ("uv run --isolated --no-project ", "uv run "),
}


@pytest.mark.parametrize("fault", CLIENT_JOB_FAULTS)
def test_client_job_check_catches(fault: str, workflow_text: str) -> None:
    old, new = CLIENT_JOB_FAULTS[fault]
    assert old in workflow_text, f"the workflow no longer contains {old!r}; update this fault"
    assert check_client_job(workflow_text.replace(old, new, 1)), f"check_client_job missed: {fault}"


def test_client_job_check_fails_when_the_job_is_missing(workflow_text: str) -> None:
    without = workflow_text.replace("  client:\n", "  client_gone:\n", 1)
    assert check_client_job(without) == ["workflow has no job `client`"]


def test_client_matrix_follows_the_claimed_python_range() -> None:
    """The test reads the range from the package, so widening ``requires-python`` makes CI lag visibly."""
    assert client_python_versions() == ["3.10", "3.11", "3.12", "3.13"]


def test_readme_development_matches_workflow(workflow_text: str, readme_text: str) -> None:
    problems = check_readme_matches_workflow(workflow_text, readme_text)
    assert not problems, "\n".join(problems)


def test_repeat_run_check_is_in_workflow_and_readme(workflow_text: str, readme_text: str) -> None:
    cmds = {cmd for job, _, cmd in _workflow_commands(_parse(workflow_text)) if job == "integration"}
    repeat = [c for c in cmds if "--count 20" in c]
    assert repeat, "integration job has no Repeat_Run_Check step (`--count 20`)"
    assert repeat[0] in code_blocks(development_section(readme_text)), "Repeat_Run_Check missing from README"


def test_integration_tests_have_no_module_level_skip() -> None:
    problems = check_no_module_level_skip(INTEGRATION)
    assert not problems, "\n".join(problems)
