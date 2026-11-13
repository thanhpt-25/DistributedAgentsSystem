"""The container command for one sandboxed run. Pure: it builds a list and runs nothing (Phase 6 Req 6.4, 6.5).

Every run gets every protective setting and no weaker one, none of them taken from configuration: no network, a
read-only root file system with one bounded scratch directory, all capabilities dropped, ``no-new-privileges``, a
non-root user, no mount, no device, a process limit, a memory limit with no swap, and a CPU limit. The only inputs are
the limits (already clamped), the image, an optional alternative OCI runtime by name (for example ``runsc``), and the
container's name. The code is not an argument at all: it goes to the interpreter's standard input (``python -I -``), so
it is never in a command line, never in a file and never on the host.
"""

from __future__ import annotations

import re
from typing import Final

from das.sandbox.limits import SCRATCH_MEGABYTES, Limits

CONTAINER_CLIS: Final = frozenset({"docker", "podman"})
NOBODY: Final = "65534:65534"
SCRATCH: Final = "/scratch"
_NAME: Final = re.compile(r"^das-sbx-[0-9a-f]{8,32}$")  # ours alone, so a cleanup never touches another container
_IMAGE: Final = re.compile(r"^[a-z0-9][a-z0-9._/:@-]{0,199}$")
_OCI_RUNTIME: Final = re.compile(r"^[a-z][a-z0-9_.-]{0,30}$")


def build_command(
    cli: str, name: str, image: str, limits: Limits, *, oci_runtime: str | None = None
) -> list[str]:
    """``docker run`` (or ``podman run``) for one run named ``name``, reading the code from standard input."""
    if cli not in CONTAINER_CLIS:
        raise ValueError("the container runtime must be docker or podman")
    if _NAME.fullmatch(name) is None:
        raise ValueError("the container name must be das-sbx- and hexadecimal digits")
    if _IMAGE.fullmatch(image) is None:
        raise ValueError("the sandbox image is not a plain image reference")
    if oci_runtime is not None and _OCI_RUNTIME.fullmatch(oci_runtime) is None:
        raise ValueError("the OCI runtime name is not a plain name")
    memory = f"{limits.memory_mb}m"
    argv = [
        cli, "run", "--rm", "-i", "--name", name,
        "--network", "none",
        "--read-only",
        "--tmpfs", f"{SCRATCH}:rw,noexec,nosuid,size={SCRATCH_MEGABYTES}m",
        "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--user", NOBODY,
        "--pids-limit", str(limits.pids),
        "--memory", memory,
        "--memory-swap", memory,
        "--cpus", str(limits.cpus),
        "--workdir", SCRATCH,
        "--env", f"HOME={SCRATCH}",
        "--env", "PYTHONDONTWRITEBYTECODE=1",
    ]
    if oci_runtime is not None:
        argv += ["--runtime", oci_runtime]
    return [*argv, image, "python", "-I", "-"]


__all__ = ["CONTAINER_CLIS", "NOBODY", "SCRATCH", "build_command"]
