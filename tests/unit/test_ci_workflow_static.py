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

EXPECTED_TIMEOUTS = {"fast": 15, "integration": 30, "e2e": 30}
EXPECTED_FAST_CHECKS = [
    "uv run ruff check",
    "uv run mypy",
    "uv run lint-imports",
    "uv run pytest tests/unit tests/property",
]
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
    for job_id in ("integration", "e2e"):
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
    if names != EXPECTED_FAST_CHECKS:
        problems.append(f"fast check steps after install are {names}, expected {EXPECTED_FAST_CHECKS}")
    for step in checks:
        name = step.get("name")
        cmds = _uv_commands(str(step.get("run", "")))
        if cmds != [name]:
            problems.append(f"fast step {name!r} runs {cmds}, expected exactly [{name!r}]")
        cond = _norm_expr(step.get("if", ""))
        for part in ("!cancelled()", "steps.install.outcome == 'success'"):
            if part not in cond:
                problems.append(f"fast step {name!r}: if {cond!r} does not contain {part!r}")
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
    ],
    ids=lambda f: f.__name__,
)
def test_workflow(check: Any, workflow_text: str) -> None:
    problems = check(workflow_text)
    assert not problems, "\n".join(problems)


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
