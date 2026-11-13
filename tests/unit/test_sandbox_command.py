"""The sandbox's limits and the container command (das-smarter-agents task 6.1; Phase 6 Req 6.4, 6.5): every run uses
every protective flag, the configurable values are clamped to their ceilings, and nothing a configuration can set
produces a weaker command."""

from __future__ import annotations

import pytest

from das.sandbox.command import build_command
from das.sandbox.limits import CEILINGS, FLOORS, SCRATCH_MEGABYTES, Limits

NAME = "das-sbx-0123456789abcdef"
IMAGE = "python:3.12-slim"


def command(limits: Limits | None = None, *, cli: str = "docker", oci_runtime: str | None = None) -> list[str]:
    return build_command(cli, NAME, IMAGE, limits or Limits(), oci_runtime=oci_runtime)


def value_of(argv: list[str], flag: str) -> str:
    """The value following ``flag`` (each flag appears once, in the ``--flag value`` form)."""
    assert argv.count(flag) == 1, f"{flag} must appear exactly once"
    return argv[argv.index(flag) + 1]


# --------------------------------------------------------------------------- the ceilings (Req 6.4)


def test_the_ceilings_are_the_ones_the_requirement_names() -> None:
    assert CEILINGS == {"timeout": 60.0, "memory_mb": 1024, "cpus": 2.0, "pids": 256, "output_bytes": 65_536}


def test_every_default_is_within_its_floor_and_ceiling() -> None:
    limits = Limits()
    for name, ceiling in CEILINGS.items():
        assert FLOORS[name] <= getattr(limits, name) <= ceiling


@pytest.mark.parametrize("name", sorted(CEILINGS))
def test_a_value_above_its_ceiling_is_clamped_to_it(name: str) -> None:
    assert getattr(Limits(**{name: CEILINGS[name] * 10}), name) == CEILINGS[name]


@pytest.mark.parametrize("name", sorted(CEILINGS))
def test_a_value_below_its_floor_is_raised_to_it(name: str) -> None:
    assert getattr(Limits(**{name: 0}), name) == FLOORS[name]


@pytest.mark.parametrize("name", sorted(CEILINGS))
def test_a_value_at_its_ceiling_is_kept(name: str) -> None:
    assert getattr(Limits(**{name: CEILINGS[name]}), name) == CEILINGS[name]


@pytest.mark.parametrize("name", sorted(CEILINGS))
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), True, "1", None])
def test_a_value_that_is_not_a_finite_number_is_refused(name: str, bad: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        Limits(**{name: bad})  # type: ignore[arg-type]


def test_limits_cannot_be_changed_after_they_are_built() -> None:
    limits = Limits()
    with pytest.raises(AttributeError):
        limits.memory_mb = 99_999  # type: ignore[misc]


def test_there_is_no_field_for_what_the_requirement_forbids() -> None:
    names = set(Limits.__dataclass_fields__)
    assert names == set(CEILINGS)
    for forbidden in ("privileged", "network", "volumes", "mounts", "devices", "socket", "user", "capabilities"):
        assert forbidden not in names
        with pytest.raises(TypeError):
            Limits(**{forbidden: True})  # type: ignore[arg-type]


# --------------------------------------------------------------------------- the command (Req 6.4)


def test_the_command_starts_with_the_runtime_and_run() -> None:
    assert command()[:2] == ["docker", "run"]
    assert command(cli="podman")[:2] == ["podman", "run"]


def test_the_container_is_removed_when_it_ends_and_has_a_known_name() -> None:
    argv = command()
    assert "--rm" in argv and value_of(argv, "--name") == NAME


def test_there_is_no_network() -> None:
    assert value_of(command(), "--network") == "none"


def test_the_root_file_system_is_read_only_with_one_bounded_scratch_directory() -> None:
    argv = command()
    assert "--read-only" in argv
    tmpfs = value_of(argv, "--tmpfs")
    assert tmpfs.startswith("/scratch:") and f"size={SCRATCH_MEGABYTES}m" in tmpfs
    options = tmpfs.split(":", 1)[1].split(",")
    assert {"rw", "noexec", "nosuid"} <= set(options)


def test_every_capability_is_dropped_and_new_privileges_are_refused() -> None:
    argv = command()
    assert value_of(argv, "--cap-drop") == "ALL"
    assert value_of(argv, "--security-opt") == "no-new-privileges"
    assert "--cap-add" not in argv


def test_the_process_runs_as_a_non_root_user_and_group() -> None:
    assert value_of(command(), "--user") == "65534:65534"


def test_the_resource_flags_carry_the_limits() -> None:
    argv = command(Limits(memory_mb=512, cpus=1.5, pids=100))
    assert value_of(argv, "--memory") == "512m"
    assert value_of(argv, "--memory-swap") == "512m"  # no swap on top of the memory limit
    assert value_of(argv, "--cpus") == "1.5"
    assert value_of(argv, "--pids-limit") == "100"


def test_the_working_directory_is_the_scratch_directory_and_python_is_isolated() -> None:
    argv = command()
    assert value_of(argv, "--workdir") == "/scratch"
    assert argv[-3:] == ["python", "-I", "-"]  # the code arrives on stdin; the image is just before the interpreter
    assert argv[-4] == IMAGE


def test_stdin_is_attached_so_the_code_can_be_sent_on_it() -> None:
    assert "-i" in command()


def test_the_environment_is_fixed_and_carries_nothing_from_the_host() -> None:
    argv = command()
    envs = [argv[i + 1] for i, a in enumerate(argv) if a == "--env"]
    assert sorted(envs) == ["HOME=/scratch", "PYTHONDONTWRITEBYTECODE=1"]


def test_an_alternative_oci_runtime_is_selected_by_name_only() -> None:
    assert value_of(command(oci_runtime="runsc"), "--runtime") == "runsc"
    assert "--runtime" not in command()


@pytest.mark.parametrize("bad", ["", "--privileged", "runsc --privileged", "a b", "../x", "run;sc", "-x", "x" * 100])
def test_an_oci_runtime_name_that_is_not_a_plain_name_is_refused(bad: str) -> None:
    with pytest.raises(ValueError, match="runtime"):
        command(oci_runtime=bad)


# --------------------------------------------------------------------------- nothing weaker can be built (Req 6.4, 6.5)

FORBIDDEN = {
    "--privileged", "--cap-add", "--device", "--volume", "-v", "--mount", "--pid", "--ipc", "--uts", "--userns",
    "--security-opt=seccomp=unconfined", "--add-host", "--dns", "--publish", "-p", "--volumes-from", "--env-file",
    "--cgroupns", "--group-add", "--privileged=true",
}
EXTREME = Limits(timeout=1e9, memory_mb=10**9, cpus=1e9, pids=10**9, output_bytes=10**9)


@pytest.mark.parametrize("cli", ["docker", "podman"])
@pytest.mark.parametrize("runtime", [None, "runsc", "kata-runtime"])
@pytest.mark.parametrize("limits", [Limits(), EXTREME])
def test_no_forbidden_flag_and_no_docker_socket_is_ever_present(cli: str, runtime: str | None, limits: Limits) -> None:
    argv = command(limits, cli=cli, oci_runtime=runtime)
    assert FORBIDDEN.isdisjoint(argv)
    assert not any("docker.sock" in a or a.startswith("/var/run") or a.startswith("unix://") for a in argv)
    assert "host" not in argv  # no host network, pid or ipc namespace


def test_an_extreme_configuration_still_gets_the_ceilings_in_the_command() -> None:
    argv = command(EXTREME)
    assert value_of(argv, "--memory") == "1024m" and value_of(argv, "--cpus") == "2.0"
    assert value_of(argv, "--pids-limit") == "256"


def test_the_image_comes_after_every_flag_and_is_followed_only_by_the_interpreter_and_its_arguments() -> None:
    argv = command()
    image_at = argv.index(IMAGE)
    assert not argv[image_at].startswith("-")
    assert argv[image_at + 1:] == ["python", "-I", "-"]


@pytest.mark.parametrize(
    "bad", ["", "-v", "--privileged", "img --privileged", "a b", "img\n--privileged", "x" * 300, "UPPER case"]
)
def test_an_image_name_that_is_not_a_plain_reference_is_refused(bad: str) -> None:
    with pytest.raises(ValueError, match="image"):
        build_command("docker", NAME, bad, Limits())


@pytest.mark.parametrize(
    "image", ["python:3.12-slim", "registry.example.com:5000/team/python:3.12", "python@sha256:" + "a" * 64]
)
def test_ordinary_image_references_are_accepted(image: str) -> None:
    assert image in build_command("docker", NAME, image, Limits())


@pytest.mark.parametrize("bad", ["", "x y", "--rm", "a/b", "A" * 100, "name;rm -rf /"])
def test_a_container_name_that_is_not_a_plain_name_is_refused(bad: str) -> None:
    with pytest.raises(ValueError, match="name"):
        build_command("docker", bad, IMAGE, Limits())


@pytest.mark.parametrize("bad", ["", "sh", "docker run", "/usr/bin/docker", "dock er", "kubectl"])
def test_only_docker_and_podman_are_accepted_as_the_container_cli(bad: str) -> None:
    with pytest.raises(ValueError, match="container"):
        build_command(bad, NAME, IMAGE, Limits())


def test_integer_limits_stay_integers_and_float_limits_stay_floats_whatever_was_given() -> None:
    limits = Limits(timeout=5, memory_mb=512.7, cpus=1, pids=100.9, output_bytes=1000.5)  # type: ignore[arg-type]
    assert [type(getattr(limits, n)) for n in ("timeout", "memory_mb", "cpus", "pids", "output_bytes")] == [
        float, int, float, int, int,
    ]
    argv = command(limits)
    assert value_of(argv, "--memory") == "512m" and value_of(argv, "--cpus") == "1.0"
    assert value_of(argv, "--pids-limit") == "100"
