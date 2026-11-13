"""``docs/security/code-execution.md`` states what Phase 6 Req 6.8 requires and stays true to the code
(das-smarter-agents task 6.4): the threat model, every limit and every protective setting, what the sandbox does not
protect against, and how to run a Code_Worker on a dedicated host."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from das.sandbox.command import NOBODY, SCRATCH, build_command
from das.sandbox.limits import CEILINGS, FLOORS, SCRATCH_MEGABYTES, Limits

DOC = Path(__file__).resolve().parents[2] / "docs" / "security" / "code-execution.md"


@pytest.fixture(scope="module")
def doc() -> str:
    return DOC.read_text(encoding="utf-8")


def section(doc: str, title: str) -> str:
    """The text under the ``## <title>`` heading, up to the next ``## `` heading."""
    match = re.search(rf"^## {re.escape(title)}\n(.*?)(?=^## |\Z)", doc, re.MULTILINE | re.DOTALL)
    assert match, f"the document has no '## {title}' section"
    return match.group(1)


def test_the_document_has_the_sections_the_requirement_names(doc: str) -> None:
    for title in ("Threat model", "What every run gets", "Limits", "What the sandbox does not protect against",
                  "Running a Code_Worker on a dedicated host", "Enabling it"):
        assert section(doc, title).strip(), title


def test_the_document_opens_with_a_plain_warning(doc: str) -> None:
    assert "warning" in doc.split("\n## ", 1)[0].lower()


@pytest.mark.parametrize(
    ("name", "shown"),
    [("timeout", "60"), ("memory_mb", "1024"), ("cpus", "2"), ("pids", "256"), ("output_bytes", "65536")],
)
def test_every_ceiling_is_stated_as_the_code_has_it(doc: str, name: str, shown: str) -> None:
    assert CEILINGS[name] == float(shown)
    assert re.search(rf"\b{name}\b[^\n]*\b{shown}\b", section(doc, "Limits")), name


@pytest.mark.parametrize("name", sorted(FLOORS))
def test_every_floor_is_stated_too(doc: str, name: str) -> None:
    shown = f"{FLOORS[name]:g}"
    assert re.search(rf"\b{name}\b[^\n]*\b{re.escape(shown)}\b", section(doc, "Limits")), name


def test_every_default_is_stated(doc: str) -> None:
    limits = Limits()
    for name in CEILINGS:
        shown = f"{getattr(limits, name):g}"
        assert re.search(rf"\b{name}\b[^\n]*\b{re.escape(shown)}\b", section(doc, "Limits")), name


def test_every_protective_setting_the_command_uses_is_in_the_document(doc: str) -> None:
    protections = section(doc, "What every run gets")
    argv = build_command("docker", "das-sbx-0123456789abcdef", "python:3.12-slim", Limits())
    for flag in ("--network", "--read-only", "--cap-drop", "--security-opt", "--user", "--pids-limit", "--memory",
                 "--memory-swap", "--cpus", "--tmpfs", "--rm"):
        assert flag in argv and flag in protections, flag
    for value in ("none", "ALL", "no-new-privileges", NOBODY, SCRATCH, f"{SCRATCH_MEGABYTES}m", "noexec", "nosuid"):
        assert value in protections, value


def test_the_document_says_that_nothing_in_the_configuration_can_weaken_these(doc: str) -> None:
    assert "cannot be loosened" in section(doc, "What every run gets").lower()


def test_the_document_says_the_code_travels_on_stdin_and_is_never_logged(doc: str) -> None:
    text = doc.lower()
    assert "stdin" in text and "never logged" in text and "no host mount" in text


def test_the_gaps_are_named_honestly(doc: str) -> None:
    gaps = section(doc, "What the sandbox does not protect against").lower()
    for gap in ("shared kernel", "denial of service", "image", "output", "side channel"):
        assert gap in gaps, gap


def test_the_dedicated_host_section_gives_concrete_steps_and_never_the_socket(doc: str) -> None:
    host = section(doc, "Running a Code_Worker on a dedicated host").lower()
    for word in ("dedicated", "runsc", "no other workload", "firewall", "egress", "rootless"):
        assert word in host, word
    assert "never mount" in doc.lower()


def test_the_enabling_section_shows_the_configuration_keys(doc: str) -> None:
    enabling = section(doc, "Enabling it")
    for key in ("sandbox:", "enabled: true", "container_cli", "image", "oci_runtime", "code-execution",
                "das_sandbox_runs_total"):
        assert key in enabling, key


def test_every_internal_link_points_at_a_file_that_exists(doc: str) -> None:
    for target in re.findall(r"\]\(([^)#]+)\)", doc):
        if "://" not in target:
            assert (DOC.parent / target).resolve().exists(), target
