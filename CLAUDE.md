# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

All tasks in `.kiro/specs/distributed-agent-system/tasks.md` are implemented. The spec (`requirements.md`, `design.md`, `tasks.md`) is the reference for behavior: `design.md` defines interfaces and the Lua script contracts, and code comments, docstrings, and tests cite requirement numbers (e.g. "Req 6.6") and task numbers (e.g. "task 19.4"). Check the cited requirement before changing behavior, and keep the citations when editing.

## Commands

Python 3.12 only (`>=3.12,<3.13`), managed with `uv`; dependencies are exact-pinned.

```bash
uv sync                                   # install runtime + dev deps into .venv
uv run pytest                             # all tests (integration tests need Docker)
uv run pytest -m "not integration"        # skip testcontainers/Compose tests
uv run pytest tests/unit/test_clock.py::test_name   # single test
HYPOTHESIS_PROFILE=ci uv run pytest tests/property  # 200 examples, derandomized
uv run lint-imports                       # enforce the import-linter contract

# Local multi-node stack (RabbitMQ, Redis, gateway, manager, worker)
cp deploy/.env.example deploy/.env        # then set RABBITMQ_USER/PASS and DAS__LLM__* settings
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml --profile ollama up -d    # local LLM
docker compose -f deploy/docker-compose.yml up -d --scale manager=3
```

The agent entry point is `das run --config <yaml>`; the role comes from the config (`deploy/config/{gateway,manager,worker}.yaml`). Every network address reaches agents through `DAS__*` env vars; nothing in `das/` hardcodes Compose service names.

End-to-end tests (`tests/integration/e2e/test_compose_e2e.py`) bring up the Compose stack plus `docker-compose.e2e.yml` under a separate project (`das-e2e`), with `llm_stub.py` as an Ollama-compatible fake LLM. They are skipped when Docker is unavailable; run with `-s` to see measured timings.

gRPC stubs in `das/transport/proto/` (`das_pb2*.py`) are generated from `das.proto` and checked in. If you regenerate them with `grpc_tools.protoc`, keep the relative import (`from . import das_pb2`) in `das_pb2_grpc.py`.

`asyncio_mode = "auto"`: async tests need no decorator. Hypothesis profiles live in `tests/conftest.py` (`default` = 100 examples, `ci` = 200). The `fake_clock` fixture provides a `FakeClock` whose `sleep` advances time instantly.

## Architecture

DAS is an asyncio platform with three process roles: **Gateway_API** (FastAPI), **Manager_Agent** (stateless planner/dispatcher), **Worker_Agent** (runs one LLM completion per Job). Redis holds all durable state (State_Store + Agent_Registry); RabbitMQ carries Job dispatch and Job_Result messages; gRPC carries control commands (cancel) and request-reply. Package layout under `das/` mirrors the roles plus shared layers (`config`, `models`, `plan`, `codec`, `transport`, `llm`, `state`, `registry`, `observability`, `health`).

Key design rules that span many files:

- **Pure core, thin I/O shell.** Plan validation, DAG scheduling, the Job state machine (`manager/machine.py`, a pure `step(snapshot, event, ctx) -> Decision` reducer emitting effects), backoff, final-result assembly, config resolution, redaction, and message codec are pure and property-tested with Hypothesis. Adapters (Redis, RabbitMQ, gRPC, LLM SDKs) wrap them.
- **Redis is the source of truth; queues are only delivery.** Every state change goes through an atomic Lua script (`das/state/lua/`) that checks lease ownership with a fencing epoch (`LEASE_LOST` / `REQUEST_TERMINAL` returns). Any Manager can ingest a Job_Result into a per-request Redis stream inbox, but only the lease owner applies it. Lease expiry and heartbeat staleness use Redis `TIME`, not host clocks.
- **At-least-once delivery, idempotent handling.** Results are keyed by `(job_id, attempt)`; the reducer discards duplicates and stale attempts.
- **Transport abstraction.** Manager/Worker code talks to the network only through the `Transport` protocol and `MessageCodec` (canonical JSON, byte-identical across transports). Adapters are loaded via the `das.transports` entry-point group. This is enforced by the import-linter contract in `pyproject.toml`: `das.manager` and `das.worker` may not import `aio_pika`, `aiormq`, `pamqp`, `grpc`, or `redis` directly (indirect imports via `das.state`/`das.transport` are allowed).
- **Capability routing.** One RabbitMQ quorum queue per Capability (`das.jobs.<cap>`); multi-capability Jobs go to the deterministic min of the "routing capabilities" that every capable worker fully satisfies, plus a worker-side requeue guard. Workers use pull-mode (`basic.get`) gated by a semaphore because quorum queues lack global QoS.
- **LLM access** goes only through `LLMProvider` (retry/timeout wrapper) around a `ProviderAdapter` (openai, anthropic, bedrock, ollama). A `FakeAdapter` is injected in tests in code, never selectable by config.
- **Errors and CLI exit codes** are typed in `das/errors.py` (config=2, transport=3, dependency unreachable=4, registration=5, generic=1). Error messages must never contain credential values; secrets are `SecretStr`, accepted only from env or `secrets_file`, never YAML.
- Config precedence: env (`DAS__` prefix, `__` nesting) > file > default.

Inject `Clock` (`das/clock.py`) instead of calling `time`/`asyncio.sleep` directly, so backoff and timeout logic is testable instantly.
