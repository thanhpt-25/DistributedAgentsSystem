# Code execution (the Code_Worker)

> **Warning.** A Code_Worker runs code that a language model wrote, on behalf of whoever can submit a Request. That
> code can be hostile: a prompt can steer the model into writing it. The sandbox below makes this much safer, not
> safe. It is **off by default**. Turn it on only on a host you can afford to lose, as described in
> [Running a Code_Worker on a dedicated host](#running-a-code_worker-on-a-dedicated-host).

A Code_Worker is an ordinary Worker with the Capability `code-execution`. A Job that requires that Capability is offered
one tool, `run_python`. The model sends a complete Python program; the Worker runs it in a throwaway container and gives
the model back the exit code and what the program printed, so it can fix the program or answer.

## Threat model

**What we protect:** the host the Code_Worker runs on, the other containers and services on it, the platform's secrets
(API keys, the LLM key, Redis and RabbitMQ credentials), and the other tenants' data.

**Who we defend against:** an author of code we do not trust. That is any Request submitter who can phrase a prompt, and
any text the model reads (a web page fetched by `http_fetch`, a dependency result) that tries to steer it. We assume the
code is written to escape, to read secrets, to reach the network, to exhaust the host, or to hold a resource forever.

**What the attacker controls:** the program text, its behaviour for up to the timeout, and what it prints.

**What the attacker does not control:** the container command line, the image, the limits, the environment, and the
configuration. Those come from the Worker's configuration and from code, never from the Request or the model.

**Trust boundary:** the container. Everything inside is untrusted. The Worker process outside it is trusted and holds
the platform's credentials, which is why the code is never given to the Worker's own interpreter and the Worker's
environment is never passed in.

## What every run gets

Every run uses all of these settings, and no weaker ones. There is no setting in the configuration for any of them, so
they cannot be loosened: a key such as `privileged`, `network`, `volumes` or `user` in the `sandbox` section is a
configuration error, and the command is built in one place that has no parameter for them.

| Setting | Command line | Effect |
| --- | --- | --- |
| No network | `--network none` | The program has no network interface but loopback. |
| Read-only root | `--read-only` | The image cannot be modified. |
| Bounded scratch | `--tmpfs /scratch:rw,noexec,nosuid,size=16m` | The only writable place; 16m, gone with the container, nothing in it can be executed, no set-uid files. |
| No capabilities | `--cap-drop ALL` | None of the kernel's root powers, even for a process that thinks it is root. |
| No privilege gain | `--security-opt no-new-privileges` | `setuid` binaries and file capabilities do not raise privilege. |
| Not root | `--user 65534:65534` | Runs as `nobody:nogroup`. |
| Process limit | `--pids-limit` | A fork bomb stops at the limit. |
| Memory limit | `--memory` and `--memory-swap` | The same value for both, so there is no swap; over it the container is killed. |
| CPU limit | `--cpus` | A fraction of a core up to the ceiling. |
| Removed afterwards | `--rm`, then `rm -f` by name | The container is removed also after a timeout or an error. |

Also: no host mounts and no devices (nothing is bind-mounted, not even the code), no host network, PID or IPC
namespace, no environment from the host (only `HOME=/scratch` and `PYTHONDONTWRITEBYTECODE=1`), and the program runs as
`python -I -` (isolated mode, no user site directory, no environment-driven start-up files).

The code arrives on **stdin**, so it is never in a command line (where `ps` could show it), never in a file, and never on
the host's disk. A wall-clock timeout kills the container by name, and then the container CLI process; output beyond the
cap is read and discarded so the program is never blocked on a full pipe.

The command is built by `build_command` in `das/sandbox/command.py` and tested for every flag, with a property test over
arbitrary limits; the image, container name, container CLI and OCI runtime name are checked against fixed patterns so
none of them can carry an extra flag.

## Limits

You can set these five values; each is clamped between its floor and its ceiling, and a configuration outside the range
does not load.

| Setting | Default | Floor | Ceiling | Meaning |
| --- | --- | --- | --- | --- |
| `timeout` | 10 | 1 | 60 | Wall-clock seconds before the container is killed. |
| `memory_mb` | 256 | 64 | 1024 | Megabytes of memory, with no swap. |
| `cpus` | 1 | 0.1 | 2 | CPUs. |
| `pids` | 64 | 16 | 256 | Processes and threads. |
| `output_bytes` | 16384 | 256 | 65536 | Bytes of output kept (stdout and stderr together); the rest is discarded. |

The code itself may be at most 65536 bytes. The Job timeout (`limits.job_timeout`) still applies around the whole Job,
including every model round and every run.

## What the sandbox does not protect against

Be honest about the gaps; they decide where to run a Code_Worker.

- **A shared kernel.** A container is not a virtual machine. A bug in the host kernel, in the container runtime, or in a
  syscall the profile allows can let the program out. Running the runtime `runsc` (gVisor) or a Kata runtime narrows
  this a great deal; plain `runc` does not.
- **Denial of service on the host.** The limits bound one run. Many runs at once, each at its ceiling, can still starve
  the host. Keep the Worker's `worker.concurrency_limit` low, and give the host nothing else to starve.
- **The image.** The program can use everything in the image. Use a small image with nothing in it you would mind it
  reading, and pin it by digest. If the image is not on the host, the first run pulls it, from the host's own network.
- **Side channels.** Timing, cache and CPU-contention channels between containers on one host are not closed.
- **Output.** What the program prints goes to the model and may reach the client. The sandbox does not judge it: it can be
  false, offensive, or an attempt to steer the model further. Treat it as untrusted text.
- **Abuse of what is allowed.** A program may burn its whole timeout, many times, at your cost. Rate limits, quotas and
  budgets (the client API) are the control for that, not the sandbox.
- **Secrets you put in the image or the Request.** Nothing from the Worker's environment is passed on, but a Request's
  prompt, and any dependency result, reach the model and can reach the code the model writes.

What the Worker logs: the exit code, the duration and the sizes of each run, and a container's name if it could not be
removed. The code, its output and its errors are never logged.

## Running a Code_Worker on a dedicated host

Treat the host as disposable and the Code_Worker as the only thing on it.

1. **A dedicated host or virtual machine** with no other workload: nothing else to read, reach, or starve. Rebuild it from
   an image on a schedule.
2. **Never mount the runtime's socket into any container**, in Compose or in Kubernetes. The repository's own Compose
   files and Helm chart do not, and a test fails if they ever do. The Code_Worker should run as a plain process on the
   dedicated host (a service, or `das run` under a supervisor), talking to the local runtime as a user allowed to run
   containers.
3. **Prefer a rootless runtime** (rootless Podman, or a rootless Docker daemon), so that a break-out lands in an
   unprivileged user, not in root.
4. **Prefer `runsc`.** Install gVisor, register it with the runtime, and set `sandbox.oci_runtime: runsc`. Only a plain
   name is accepted there; nothing else about the runtime is configurable.
5. **Firewall the host.** Allow inbound only what the Worker needs (the control port from the Managers), and **egress**
   only to Redis, RabbitMQ, the LLM provider and the image registry. Sandboxed programs have no network, but the host
   does, and a break-out would inherit its egress.
6. **Pre-pull and pin the image**, by digest, so a run never waits on a pull and the image cannot change under you.
7. **Keep the host's secrets minimal:** the Worker's own keys only, supplied from the environment or a secrets file,
   never in the image.
8. **Low concurrency.** Start with `worker.concurrency_limit: 2`, and size from the host's memory divided by `memory_mb`.
9. **Watch it.** `das_sandbox_runs_total{outcome}` counts runs (`ok`, `error`, `timeout`, `killed`); a rise in `killed` or
   `timeout` is a program fighting its limits. `das_tool_calls_total{tool="run_python"}` counts the tool calls.

## Enabling it

The sandbox is off. To make a Code_Worker, set the `sandbox` section on a Worker (and only on a Worker; the Worker's
LLM provider must support tools):

```yaml
role: worker
worker:
  capabilities: [llm]            # code-execution is added by the sandbox below; listing it is also fine
sandbox:
  enabled: true                  # without this, a Worker that lists code-execution does not start
  container_cli: docker          # docker or podman
  image: python:3.12-slim        # a plain image reference, ideally pinned by digest and already pulled
  oci_runtime: runsc             # optional: an alternative OCI runtime, by name
  timeout: 10                    # seconds, 1 to 60
  memory_mb: 256                 # 64 to 1024
  cpus: 1                        # 0.1 to 2
  pids: 64                       # 16 to 256
  output_bytes: 16384            # 256 to 65536
```

At start-up the Worker asks the container runtime (`<container_cli> info`); if it does not answer, the Worker exits with
code 4 naming it. With `sandbox.enabled` false, a Worker that lists `code-execution` exits with code 2, and a Worker never
advertises it. With it true the Worker advertises `code-execution`, and a Job requiring it is offered `run_python`.
Each run is counted in `das_sandbox_runs_total{outcome}`.

See also the [README](../../README.md).
