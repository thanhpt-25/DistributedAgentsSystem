# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Phases 1–4 are implemented (core platform, observability & operations, hardening & CI, client API: idempotency keys, rate limits/quotas, budgets, SSE events, webhooks); Phase 5 (multi-tenant and deployable: tenants and scopes, request priority, usage reports, a Helm chart with Kubernetes CI) is implemented too, with `das-multi-tenant`'s final checkpoint (task 14) the only open task. Each phase has a spec folder under `.kiro/specs/` (`distributed-agent-system`, `das-observability-operations`, `das-hardening-ci`, `das-client-api`, `das-multi-tenant`), and `.kiro/specs/roadmap.md` / `open-decisions.md` hold the phase plan and decisions. The specs (`requirements.md`, `design.md`, `tasks.md`) are the reference for behavior: `design.md` defines interfaces and the Lua script contracts, and code comments, docstrings, and tests cite requirement numbers (e.g. "Req 6.6") and task numbers (e.g. "task 19.4"). Check the cited requirement before changing behavior, and keep the citations when editing.

`.superpowers/sdd/` and `docs/superpowers/plans/` are agent-workflow artifacts (task briefs, reports, implementation plans), not product code.

## Commands

Python 3.12 only (`>=3.12,<3.13`), managed with `uv`; dependencies are exact-pinned.

```bash
uv sync                                   # install runtime + dev deps into .venv
uv run pytest                             # all tests (integration tests need Docker)
uv run pytest -m "not integration"        # skip testcontainers/Compose tests
uv run pytest tests/contract              # the Python client (clients/python) against the real Gateway app, in-process
uv run pytest tests/unit/test_clock.py::test_name   # single test
HYPOTHESIS_PROFILE=ci uv run pytest tests/property  # 200 examples, derandomized
uv run lint-imports                       # enforce the import-linter contract
uv run ruff check                         # lint (E, F only; line length 120; das/transport/proto excluded)
uv run mypy                               # type check the das package (not strict; pydantic plugin)

# Local multi-node stack (RabbitMQ, Redis, gateway, manager, worker)
cp deploy/.env.example deploy/.env        # then set RABBITMQ_USER/PASS and DAS__LLM__* settings
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml --profile ollama up -d    # local LLM
docker compose -f deploy/docker-compose.yml up -d --scale manager=3
```

The agent entry point is `das run --config <yaml>`; the role comes from the config (`deploy/config/{gateway,manager,worker}.yaml`). Every network address reaches agents through `DAS__*` env vars; nothing in `das/` hardcodes Compose service names.

End-to-end tests (`tests/integration/e2e/test_compose_e2e.py`) bring up the Compose stack plus `docker-compose.e2e.yml` under a separate project (`das-e2e`), with `llm_stub.py` as an Ollama-compatible fake LLM. `test_client_api_e2e.py` does the same for the Phase 4 client API under project `das-e2e-client` (override `docker-compose.client-api.e2e.yml` and the Gateway config `gateway.client-api.yaml`: two Gateways, a webhook sink, `webhook_sink.py`; its constants are shared in `client_api_constants.py`), and `test_observability_e2e.py` has its own project too (`das-e2e-obs`). `DAS_E2E_CLIENT_OVERRIDE_EXTRA` layers one more Compose file on the client API stack for broken-variant experiments. `test_tenancy_e2e.py` is the Phase 5 stack under project `das-e2e-tenant` (override `docker-compose.tenancy.e2e.yml`, Gateway config `gateway.tenancy.yaml`: two tenants, an Operator and two Gateways; keys and fingerprints in `tenancy_constants.py`, which must avoid the words `tenant` and `operator` because the log redactor masks any 8-character window of a key). Its broken variants are built by `tenancy_variants.py` (config edits, or a patched source file bind-mounted over the installed one) and layered with `DAS_E2E_TENANT_OVERRIDE_EXTRA`; `uv run python -m tests.integration.e2e.tenancy_variants --list` names them and the tests each must break, and `tests/unit/test_e2e_tenancy_static.py` checks every patch still applies exactly once. The observability stack publishes fixed host ports (9090, 3000, 16686): its tests skip, not fail, when one is in use. They are skipped when Docker is unavailable; run with `-s` to see measured timings.

The Helm chart (`deploy/helm/das/`) has static render tests (`tests/unit/test_chart_*.py`, real `helm`, skipped when it is missing) and a kind cluster suite, `tests/integration/k8s/`, that installs it with `tests/k8s/values-kind.yaml` next to the Redis, RabbitMQ and stub-LLM manifests in `tests/k8s/`. It is opt-in: `DAS_K8S_E2E=1` plus `kind`, `kubectl`, `helm` and the three images built as `das-<role>:local` (`docker build -f deploy/docker/<role>.Dockerfile -t das-<role>:local .`). The cluster gets its own kubeconfig (yours is never touched), is deleted afterwards (`DAS_K8S_KEEP=1` keeps it), and writes pod logs to `DAS_K8S_LOG_DIR` after a failure; `DAS_KIND_NODE_IMAGE` picks the node image. A skip that names Docker fails under `DAS_REQUIRE_DOCKER=1`, as for the other Docker tests.

gRPC stubs in `das/transport/proto/` (`das_pb2*.py`) are generated from `das.proto` and checked in. If you regenerate them with `grpc_tools.protoc`, keep the relative import (`from . import das_pb2`) in `das_pb2_grpc.py`.

CI (`.github/workflows/ci.yml`): the fast job runs `ruff check`, `mypy`, `lint-imports`, then `pytest tests/unit tests/property tests/contract` with a random `--hypothesis-seed` (the `default` profile, since `ci` derandomizes and would ignore the seed). A separate `client` job runs the Python client package's own tests (`clients/python`) once per Python 3.10 to 3.13, in an isolated `uv run --isolated --no-project` environment because the client is not a uv workspace member (the server's lock is Python 3.12 only); its exact command is in the README Development section, and it takes the interpreter from `UV_PYTHON`. Integration and e2e jobs run only on `main` pushes or manual dispatch with `DAS_REQUIRE_DOCKER=1`, which turns a Docker-related skip into a failure; set it locally to confirm Docker tests actually ran. `ci.yml`, the lint/mypy config in `pyproject.toml`, and `deploy/` have static unit tests (`test_ci_workflow_static.py`, `test_lint_config.py`, `test_deploy_static.py`, `test_deploy_observability.py`), so edits there can fail tests. Wire-format baselines (`tests/fixtures/phase3_baseline`, `v1_codec.py`) pin codec bytes; changing the codec output breaks them on purpose.

`asyncio_mode = "auto"`: async tests need no decorator. Hypothesis profiles live in `tests/conftest.py` (`default` = 100 examples, `ci` = 200). The `fake_clock` fixture provides a `FakeClock` whose `sleep` advances time instantly.

## Architecture

DAS is an asyncio platform with three process roles: **Gateway_API** (FastAPI), **Manager_Agent** (stateless planner/dispatcher), **Worker_Agent** (runs one LLM completion per Job). Redis holds all durable state (State_Store + Agent_Registry); RabbitMQ carries Job dispatch and Job_Result messages; gRPC carries control commands (cancel) and request-reply. Package layout under `das/` mirrors the roles plus shared layers (`config`, `models`, `plan`, `codec`, `transport`, `llm`, `state`, `registry`, `observability`, `health`). Two further packages hold pure logic that the Gateway and admin endpoints wrap: `das/ops/` (DLQ replay/discard, retention, usage and cost, cursors, Redis key helpers) and `das/client/` (Phase 4 client API: idempotency keys, token-bucket rate limit, budget resolution, event payloads, webhook signing).

Key design rules that span many files:

- **Pure core, thin I/O shell.** Plan validation, DAG scheduling, the Job state machine (`manager/machine.py`, a pure `step(snapshot, event, ctx) -> Decision` reducer emitting effects), backoff, final-result assembly, config resolution, redaction, and message codec are pure and property-tested with Hypothesis. Adapters (Redis, RabbitMQ, gRPC, LLM SDKs) wrap them.
- **Redis is the source of truth; queues are only delivery.** Every state change goes through an atomic Lua script (`das/state/lua/`) that checks lease ownership with a fencing epoch (`LEASE_LOST` / `REQUEST_TERMINAL` returns). Any Manager can ingest a Job_Result into a per-request Redis stream inbox, but only the lease owner applies it. Lease expiry and heartbeat staleness use Redis `TIME`, not host clocks.
- **At-least-once delivery, idempotent handling.** Results are keyed by `(job_id, attempt)`; the reducer discards duplicates and stale attempts.
- **Transport abstraction.** Manager/Worker code talks to the network only through the `Transport` protocol and `MessageCodec` (canonical JSON, byte-identical across transports). Adapters are loaded via the `das.transports` entry-point group. This is enforced by the import-linter contract in `pyproject.toml`: `das.manager` and `das.worker` may not import `aio_pika`, `aiormq`, `pamqp`, `grpc`, `redis`, `prometheus_client`, or `opentelemetry` directly (indirect imports via `das.state`/`das.transport` are allowed).
- **Tenancy is a field plus twin keys, not a partition.** A Request carries `tenant` (absent = `default`) and priority (stored only when `high`); the same Lua scripts that keep the global indexes also keep per-tenant twins (`das:tenant:{t}:requests`, `:requests:by_status:{s}`, `:active`, the DLQ twins, and a per-day Usage_Rollup), so the global keys stay what Phase 4 wrote and a rollback loses nothing. `das/tenancy/` is the pure core (Principal, Scope, the route table `ROUTE_SCOPES`, key directory, settings layering, usage report); `das/gateway/auth.py` resolves a key to a Principal, `authz.py` checks the route before anything is read, and `can_see(principal, tenant)` is the one visibility rule (another tenant's Request answers exactly like a missing one). Until the Manager's Tenant_Backfill sets `das:tenant:index:ready`, only the default tenant can list. Priority travels as the transport header `das-priority: high`, which only the RabbitMQ adapter turns into AMQP priority 9; the payload and codec output never change. A key's fingerprint, never its value, names it in config, logs and indexes; metrics and spans carry the tenant id, never a fingerprint.
- **Capability routing.** One RabbitMQ quorum queue per Capability (`das.jobs.<cap>`); multi-capability Jobs go to the deterministic min of the "routing capabilities" that every capable worker fully satisfies, plus a worker-side requeue guard. Workers use pull-mode (`basic.get`) gated by a semaphore because quorum queues lack global QoS.
- **LLM access** goes only through `LLMProvider` (retry/timeout wrapper) around a `ProviderAdapter` (openai, anthropic, bedrock, ollama). A `FakeAdapter` is injected in tests in code, never selectable by config.
- **Errors and CLI exit codes** are typed in `das/errors.py` (config=2, transport=3, dependency unreachable=4, registration=5, generic=1). Error messages must never contain credential values; secrets are `SecretStr`, accepted only from env or `secrets_file`, never YAML.
- Config precedence: env (`DAS__` prefix, `__` nesting) > file > default.

Inject `Clock` (`das/clock.py`) instead of calling `time`/`asyncio.sleep` directly, so backoff and timeout logic is testable instantly.
