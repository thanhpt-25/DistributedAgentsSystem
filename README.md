# Distributed Agent System (DAS)

DAS turns a natural-language prompt into a plan of Jobs, runs those Jobs on Worker agents spread across machines, and returns a summarized Final_Result.

- **Gateway**: HTTP API for submitting, querying, and cancelling Requests.
- **Manager**: claims a Request (via a Redis lease), asks an LLM for a Job plan (a DAG), dispatches Jobs, retries failures, and summarizes results. Run several for failover.
- **Worker**: declares Capabilities (e.g. `research`, `code-review`), consumes matching Jobs, and calls an LLM to do the work.

Infrastructure: RabbitMQ (job queues), Redis (state and agent registry), gRPC (control messages such as cancel). LLM providers: OpenAI, Anthropic, AWS Bedrock, or Ollama.

## Quick start (Docker Compose)

Requires Docker with Compose v2.30+ (the stack uses `post_start` hooks).

```bash
cp deploy/.env.example deploy/.env
# edit deploy/.env: RabbitMQ password, DAS_GATEWAY_API_KEYS, LLM provider/model/key
docker compose -f deploy/docker-compose.yml up -d --build
```

This starts RabbitMQ, Redis, one Gateway on port 8080, two Managers, and two Workers (`research,summarize` and `code-review`).

To use a local model instead of a hosted provider, set the Ollama lines in `deploy/.env`, then:

```bash
docker compose -f deploy/docker-compose.yml --profile ollama up -d
docker compose -f deploy/docker-compose.yml exec ollama ollama pull llama3.2
```

Stop everything with `docker compose -f deploy/docker-compose.yml down` (add `-v` to delete data).

## Observability stack (Prometheus, Grafana, Jaeger)

The `observability` profile adds Prometheus, Grafana, and Jaeger. It is off by default, so the stack above is unchanged without it.

Set these in `deploy/.env` first:

| Variable | Purpose |
|---|---|
| `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | Grafana admin login. Both are required: if either is unset or empty, Grafana exits with a message naming the missing variable instead of starting with `admin/admin`. Anonymous access and sign-up are disabled. |
| `DAS__TRACING__ENABLED` | `true` makes every agent send traces to Jaeger (OTLP gRPC, `http://jaeger:4317`, set by Compose). Default `false`. If the profile is not running, agents only log a warning. |

Start and stop the profile:

```bash
# Start (or add to) the stack with the observability services
docker compose -f deploy/docker-compose.yml --profile observability up -d --build

# Stop only the observability services, leaving the agents running
docker compose -f deploy/docker-compose.yml --profile observability stop prometheus grafana jaeger

# Stop and remove everything, observability services included
docker compose -f deploy/docker-compose.yml --profile observability down
```

Every published port binds to `127.0.0.1` only:

| Service | Address | Notes |
|---|---|---|
| Prometheus | http://127.0.0.1:9090 | Scrapes `/metrics` on the Ops_Port of the Gateway and of every Manager and Worker replica (found by DNS, so `--scale` needs no config change), every 15 s |
| Grafana | http://127.0.0.1:3000 | Log in with `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`. The Prometheus data source and the `DAS Overview` dashboard are provisioned |
| Jaeger (trace backend) | http://127.0.0.1:16686 | UI and query API. OTLP ports 4317/4318 are reachable only on the `das` Compose network |

## Using the API

Every `/v1` call needs an `X-API-Key` header with one of the keys from `DAS_GATEWAY_API_KEYS`.

```bash
KEY=change-me-api-key
GW=http://localhost:8080

# Submit a prompt: 202 {"request_id": "..."}
curl -s -X POST $GW/v1/requests -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Research the CAP theorem and summarize the trade-offs"}'

# Check progress
curl -s $GW/v1/requests/REQUEST_ID -H "X-API-Key: $KEY"

# Cancel: 202 {"status": "Cancelled"}
curl -s -X POST $GW/v1/requests/REQUEST_ID/cancel -H "X-API-Key: $KEY"

# List registered agents and their Capabilities
curl -s $GW/v1/agents -H "X-API-Key: $KEY"

# Health (no key needed): 200 healthy, 503 unhealthy
curl -s $GW/healthz
```

The query response contains:

- `status`: `Pending`, `Running`, `Completed`, `Failed`, or `Cancelled`
- `jobs`: `[{job_id, state}]` in plan order (`Waiting`, `Ready`, `Dispatched`, `Succeeded`, `Failed`, `Skipped`, `Cancelled`)
- `transitions`: every recorded state change, oldest first
- `final_result` (only when `Completed`): `summary`, per-Job reports (worker, token usage), and `failures`
- `error` when the Request failed

Errors are flat JSON objects:

| Status | Body | Meaning |
|---|---|---|
| 400 | `empty_prompt`, `prompt_too_long` (with `max_length`), `invalid_body` | Bad submission |
| 401 | `unauthorized` | Missing or unknown API key |
| 404 | `not_found` | Unknown Request ID |
| 409 | `request_terminal` (with current `status`) | Cancel on a finished Request |
| 503 | `service_unavailable` | Redis unreachable |

## Scaling

Add capacity without restarting other agents:

```bash
docker compose -f deploy/docker-compose.yml up -d --no-recreate --scale manager=3 --scale worker-research=2
```

New agents register within one heartbeat. Scaling down sends SIGTERM: agents finish in-flight work, deregister, and exit. If a Manager dies, another takes over its Requests after the lease expires (`limits.lease_duration`, default 30 s).

To add a new Capability, add a worker service with a different `DAS__WORKER__CAPABILITIES`, and add the matching `das.jobs.<capability>` queue and binding to `deploy/rabbitmq/definitions.json`.

## Ops_Port endpoints (metrics, liveness, readiness)

Every agent (Gateway, Manager, Worker) serves an internal HTTP Ops_Port on `health_port` (default 8081). None of these endpoints needs an API key. The Gateway keeps its public API and `/healthz` on `http_port` (8080), which answers 404 for the paths below.

| Endpoint | Answer |
|---|---|
| `GET /metrics` | Prometheus text format 0.0.4. Returns 404 when `metrics.enabled` is `false` |
| `GET /livez` | 200 or 503, JSON `{"status": "pass"\|"fail", "checks": {...}}`. Fails only when a critical loop has ended or the event loop is stalled, never because a dependency is down. Use it as the restart probe |
| `GET /readyz` | 200 or 503, JSON with `status`, per-condition `checks`, and the `failed` names. Checks `startup` and each dependency connection; Managers and Workers also check `registered_available`, `not_draining`, and `not_shutting_down`. Use it to route traffic |
| `GET /healthz` | v1 behavior, unchanged (on `http_port` for the Gateway) |

The Ops_Port is not published to the host in Compose; Prometheus reaches it on the `das` network. To look at it by hand (the images have no curl):

```bash
docker compose -f deploy/docker-compose.yml exec manager \
  python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8081/readyz').read().decode())"
```

## Admin API

The Admin_API lives under `/v1/admin` on the Gateway. It is disabled until you set `DAS__ADMIN_API_KEYS` in `deploy/.env` (comma-separated; each key 16 to 256 printable ASCII characters, at most 32 keys, none equal to a `DAS_GATEWAY_API_KEYS` entry). Send an admin key in the same `X-API-Key` header. An admin key also works on every normal `/v1` endpoint.

| Status | Body | Meaning |
|---|---|---|
| 401 | `unauthorized` | Missing or unknown key |
| 403 | `forbidden` | A normal API key was used on `/v1/admin` |
| 403 | `admin_disabled` | No admin key is configured |

Every admin call is written to an audit log entry with a key fingerprint, never the key itself.

```bash
ADMIN_KEY=change-me-admin-key-0123456789

# Dead letters: filter by status (Pending, Replayed, Discarded), reason
# (retries_exhausted, delivery_limit, malformed), request_id, capability;
# limit 1-100 (default 20); follow next_cursor with cursor=...
curl -s "$GW/v1/admin/dlq?status=Pending&limit=20" -H "X-API-Key: $ADMIN_KEY"
curl -s $GW/v1/admin/dlq/DEAD_LETTER_ID -H "X-API-Key: $ADMIN_KEY"

# Replay a dead-lettered Job: 202 {dead_letter_id, request_id, job_id, attempt}
curl -s -X POST $GW/v1/admin/dlq/DEAD_LETTER_ID/replay -H "X-API-Key: $ADMIN_KEY"

# Discard it: 200 {"status": "Discarded", ...} (repeating it is also 200)
curl -s -X POST $GW/v1/admin/dlq/DEAD_LETTER_ID/discard -H "X-API-Key: $ADMIN_KEY"

# Drain / undrain a Manager or Worker: 202 {agent_id, status}
curl -s -X POST $GW/v1/admin/agents/AGENT_ID/drain -H "X-API-Key: $ADMIN_KEY" \
  -H 'Content-Type: application/json' -d '{"release_leases": false}'
curl -s -X POST $GW/v1/admin/agents/AGENT_ID/undrain -H "X-API-Key: $ADMIN_KEY"
```

Admin errors:

| Status | Body | Meaning |
|---|---|---|
| 400 | `invalid_query` (with `parameter`) | Bad DLQ filter, limit, or cursor |
| 400 | `invalid_request` (with `parameter`) | Drain body is not a JSON object, or `release_leases` is not a boolean |
| 404 | `not_found` | Unknown dead letter or agent ID |
| 409 | replay rule code (with `current_state`) | Replay refused: `record_not_pending`, `not_replayable`, `request_gone`, `request_state`, `job_state`, `stale_attempt`, or `lease_held` |
| 409 | `record_not_pending` | Discard on a replayed record |
| 409 | `agent_state` (with `status`) | Drain/undrain on an Unavailable agent or on a Gateway |
| 504 | `agent_timeout` | The agent did not confirm within 5 s. Nothing was changed or resent |
| 503 | `service_unavailable` | Redis or the control transport is unavailable |

## Draining agents (CLI)

Drain a Manager or Worker before maintenance. A draining Worker stops taking new Jobs, returns unstarted ones to the queue, and finishes the Jobs it is running. A draining Manager stops taking new Requests and keeps driving the ones it holds; with `--release-leases` it releases them so another Manager takes over. `/readyz` reports `not_draining` failing, and `GET /v1/agents` shows the agent as `Draining` with its in-flight count and a `drained` flag. Draining does not survive a restart: a restarted agent registers as Available.

The CLI reads the admin key from `DAS_ADMIN_API_KEY`; it never prints it. Get agent IDs from `GET /v1/agents`.

```bash
export DAS_ADMIN_API_KEY=change-me-admin-key-0123456789

uv run das drain http://localhost:8080 AGENT_ID                      # returns once the agent confirms
uv run das drain http://localhost:8080 AGENT_ID --release-leases     # Manager only
uv run das drain http://localhost:8080 AGENT_ID --wait --timeout 600 # poll every 2 s until drained
uv run das undrain http://localhost:8080 AGENT_ID                    # back to Available
```

`--timeout` is 1 to 86400 seconds (default 600) and only applies with `--wait`. The command exits 0 when the Gateway answers 202 (and, with `--wait`, once the agent reports drained). It exits 1 on any other HTTP status (the message names the status and error code), on a network error, or when `--wait` times out, and exits 2 when `DAS_ADMIN_API_KEY` is not set.

## Upgrading from v1

With `tracing.enabled` set to `false` everywhere, upgraded agents and v1 agents can run side by side during a rolling upgrade: messages stay byte-identical to v1. Admin actions are the exception. Replay writes request transitions and a `replay` inbox kind, and drain introduces the `Draining` status, which v1 agents do not understand. **Only use Replay and drain after every Gateway, Manager, and Worker has been upgraded.**

## Configuration

Each agent runs `das run --config <file>`. The YAML file sets the role and tuning; samples live in `deploy/config/`.

Any field can be overridden by an environment variable: `DAS__` plus the field path in upper case, joined by `__`. Precedence is environment, then file, then default.

```bash
DAS__LIMITS__MAX_ATTEMPTS=5                     # limits.max_attempts
DAS__WORKER__CAPABILITIES=research,summarize    # lists are comma-separated
DAS__REDIS__URL=redis://redis-host:6379/0
```

Credentials are never accepted in the YAML file. Provide API keys and LLM credentials (`DAS__API_KEYS`, `DAS__LLM__API_KEY`, `DAS__LLM__AWS_*`) through the environment, or through a secrets file of `NAME=VALUE` lines referenced by `secrets_file:` or `DAS__SECRETS_FILE`. Credentials are redacted from logs.

Key settings:

| Field | Role | Purpose |
|---|---|---|
| `role` | all | `gateway`, `manager`, or `worker` |
| `http_port` | gateway | HTTP API port (default 8080) |
| `health_port` | all | Ops_Port: `/metrics`, `/livez`, `/readyz` (default 8081; on the Gateway it must differ from `http_port`). Managers and Workers also serve `/healthz` here |
| `transports.jobs`, `transports.control` | all | Transport adapters by name (`rabbitmq`, `grpc`) |
| `rabbitmq.url`, `redis.url` | all | Infrastructure addresses |
| `grpc.bind`, `grpc.advertise_host`, `grpc.advertise_port` | all | gRPC listener and the address other agents use |
| `llm.provider`, `llm.model`, `llm.base_url` | manager, worker | `openai`, `anthropic`, `bedrock`, or `ollama` |
| `worker.capabilities`, `worker.concurrency_limit` | worker | What the Worker does, and how many Jobs at once |
| `limits.max_attempts`, `base_backoff`, `max_backoff` | manager | Job retry policy |
| `limits.lease_duration` | manager | Failover speed |
| `limits.job_timeout`, `max_jobs`, `max_prompt_length` | various | Bounds on work |
| `limits.startup_timeout`, `startup_retry_interval` | all | How long to wait for dependencies at startup |
| `metrics.enabled`, `metrics.queue_sample_interval` | all | Turn `/metrics` on or off (default on); how often queue depth is sampled (default 15 s) |
| `tracing.enabled`, `otlp_endpoint`, `otlp_protocol`, `sample_ratio` | all | OpenTelemetry export (default off; `grpc` or `http/protobuf`; ratio 0 to 1). `tracing.otlp_headers` is a credential: environment or secrets file only |
| `admin_api_keys` | gateway | Admin_API keys (`DAS__ADMIN_API_KEYS`, credential: environment or secrets file only) |
| `retention.*` | manager | Retention_Policy, see below |
| `pricing.*` | gateway, manager, worker | Price_Table, see below |

If you change `limits.max_attempts`, set the `das-jobs` policy `delivery-limit` in `deploy/rabbitmq/definitions.json` to `max_attempts - 1`.

### Retention_Policy

By default nothing is deleted. With `retention.enabled: true`, each Manager runs a sweeper that deletes expired data from Redis. Several Managers can sweep at once; each item is deleted and counted (`das_retention_deleted_total{kind}`) once. Requests that are still Pending or Running, and dead letters still `Pending`, are never deleted.

```yaml
# deploy/config/manager.yaml
retention:
  enabled: true
  terminal_request_ttl: 604800   # seconds after a Request finishes before it and all its keys are deleted (1 h to 365 d, default 7 d)
  inbox_ttl: 3600                # seconds after finishing before its inbox stream is deleted (1 min to 7 d, default 1 h)
  dlq_record_ttl: 2592000        # seconds after a dead letter is Replayed or Discarded before it is deleted (1 h to 365 d, default 30 d)
  sweep_interval: 60             # seconds between sweeps (10 to 3600)
  batch_size: 500                # items per kind per sweep (1 to 10000)
```

A deleted Request answers 404 on `GET /v1/requests/{id}`. If Redis fails during a sweep, the sweep stops, keeps the data, and retries at the next interval.

### Price_Table

An optional Price_Table turns token usage into an estimated cost. With it, the Gateway adds `estimated_cost` (total, currency, `complete`, `unpriced_models`, and per-model amounts) to every Request query; without it, `estimated_cost` is `null`. Managers and Workers use it for the `das_llm_estimated_cost_total` metric, so put the same table in every agent's config.

```yaml
pricing:
  currency: USD                  # ISO 4217 code, default USD
  models:                        # 0 to 200 entries; provider/model pairs must be unique
    - provider: openai
      model: gpt-4o-mini
      input_per_million: "0.15"  # price per 1,000,000 tokens, 0 to 1000000, at most 6 decimal places
      output_per_million: "0.60"
    - provider: anthropic
      model: claude-3-5-haiku-latest
      input_per_million: "0.80"
      output_per_million: "4.00"
```

Write prices as quoted strings. They are read as exact decimals; an unquoted YAML number goes through a float first. Provider and model must match the Usage_Record exactly (case-sensitive). Models with no entry are listed in `unpriced_models` and set `complete` to `false`; an empty `models` list makes every model unpriced. An invalid table stops the agent with exit code 2 before it connects to anything.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Clean shutdown |
| 1 | Unexpected error |
| 2 | Invalid configuration (every bad field is logged) |
| 3 | Transport adapter not installed or incomplete |
| 4 | Dependency (Redis, RabbitMQ, ...) unreachable after `startup_timeout` |
| 5 | Agent registration failed |
| 130 | Interrupted during startup |

## Running across multiple hosts

Nothing in the code assumes Compose service names. To run a role on another machine, point `DAS__RABBITMQ__URL` and `DAS__REDIS__URL` at the shared infrastructure and set `DAS__GRPC__ADVERTISE_HOST` to an address the other agents can reach.

## Development

Requirements:

- Python 3.12 and [uv](https://docs.astral.sh/uv/) 0.13.6, the same version CI uses. Regenerate `uv.lock` with this version when you add a dependency, otherwise `uv sync --locked` fails on CI.
- Docker, running, for the integration suite, the e2e suite, and the repeat-run check.
- Docker Compose 2.30 or later for the e2e suite.

The lint, type, and import checks and the fast test suite do not need Docker.

The commands below are the exact commands CI runs, in the same order. Run them from the repository root.

Install dependencies from the lock file (fails if `uv.lock` is missing or out of date with `pyproject.toml`, and never rewrites it):

```bash
uv sync --locked
```

Lint (ruff):

```bash
uv run ruff check
```

Type check (mypy, package `das/`):

```bash
uv run mypy
```

Import contracts (Manager and Worker must not import broker, RPC, or Redis clients):

```bash
uv run lint-imports
```

Fast suite (unit and property tests, no Docker):

```bash
uv run pytest tests/unit tests/property
```

Integration suite (RabbitMQ and gRPC through testcontainers, needs Docker):

```bash
uv run pytest tests/integration --ignore=tests/integration/e2e
```

E2E suite (full Compose stack with a fake LLM, needs Docker and Compose 2.30+, about 1.5 min):

```bash
uv run pytest tests/integration/e2e
```

Repeat-run check (runs `test_reconnect_restores_subscriptions` 20 times in one pytest session against one RabbitMQ container, needs Docker; exits non-zero if any of the 20 runs fails):

```bash
uv run pytest "tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions" --count 20 -v
```

Integration and e2e tests skip when Docker is unavailable. On a machine with Docker, a plain `pytest` runs them too. Set `DAS_REQUIRE_DOCKER=1` to turn those Docker-related skips into failures, so a missing Docker cannot pass silently; CI sets it for the integration and e2e jobs. Without the variable, tests skip as before.

To reproduce a fast-suite failure from CI with the same Hypothesis seed, see [Reproducing a Hypothesis failure](#reproducing-a-hypothesis-failure).

Run an agent locally:

```bash
uv run das run --config path/to/config.yaml
```

### Generated gRPC code

`das/transport/proto/` holds code generated from `das.proto` (`das_pb2.py`, `das_pb2.pyi`, `das_pb2_grpc.py`). It is excluded from both ruff and mypy; do not edit it by hand. After changing `das.proto`, regenerate it with `grpcio-tools`:

```bash
uv run python -m grpc_tools.protoc -I das/transport/proto --python_out=das/transport/proto --pyi_out=das/transport/proto --grpc_python_out=das/transport/proto das/transport/proto/das.proto
```

Then fix the import in `das/transport/proto/das_pb2_grpc.py`: `protoc` writes `import das_pb2 as das__pb2`, which must become the relative import `from . import das_pb2 as das__pb2`.

Project layout:

```
das/
  gateway/    HTTP API            manager/   planner, state machine, driver
  worker/     Job executor        llm/       provider adapters and retry
  transport/  rabbitmq, grpc      state/     Redis store and Lua scripts
  registry/   agent registry      config/    schema, loader, secrets
  codec/      message envelope    plan/      Job_Plan parsing and validation
  health/     Ops_Port server     cli.py     das entry point (run, drain, undrain)
  ops/        cost, usage, replay, retention, cursor, keys (pure core)
  observability/  metrics and tracing
deploy/       Dockerfiles, Compose stack, RabbitMQ definitions, sample configs, Prometheus and Grafana config
tests/        unit, property (Hypothesis), integration, e2e
```

Transport adapters are discovered through the `das.transports` entry-point group, so a new transport can ship as a separate package without changing Manager or Worker code.

## Continuous integration

The workflow `.github/workflows/ci.yml` (named **CI**) has three jobs. Each job installs Python 3.12 and uv 0.13.6, runs `uv sync --locked`, then runs the commands listed below, which are the same commands as in [Development](#development). Each job reports its own status check on the commit and pull request.

| Job | Commands | Runs on | Timeout |
|---|---|---|---|
| `fast` (Fast_Job) | `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property` | every push to any branch (not tags); pull requests when opened, synchronized, or reopened; manual run | 15 min |
| `integration` (Integration_Job) | `uv run pytest tests/integration --ignore=tests/integration/e2e`, then the repeat-run check (`... --count 20 -v`) | push to `main`; manual run | 30 min |
| `e2e` (E2E_Job) | checks Docker Compose is 2.30 or later, then `uv run pytest tests/integration/e2e` | push to `main`; manual run | 30 min |

`integration` and `e2e` start only after `fast` in the same workflow run finishes with status success. If `fast` fails or is cancelled, they do not run. On pull requests and on pushes to other branches they are reported as skipped. Both set `DAS_REQUIRE_DOCKER=1`, so a Docker-related skip fails the job. In `fast`, all four checks run even if one of them fails, so one run shows every failure. When `e2e` fails, the container logs are uploaded as the `e2e-logs-<run id>-<attempt>` artifact.

A new commit on the same branch, for the same event type, cancels the previous run that is still in progress.

### Running CI manually

To run all three jobs on any branch:

1. Open the repository on GitHub and go to the **Actions** tab.
2. Select the **CI** workflow in the list on the left.
3. Click **Run workflow**.
4. In **Use workflow from**, choose the branch to run.
5. Click the green **Run workflow** button.

### Reproducing a Hypothesis failure

Each `fast` run picks a random Hypothesis seed. To rerun a failed run locally with the same seed:

1. Open the failed run, then the `fast` job, then the step `uv run pytest tests/unit tests/property`.
2. Find the line `Hypothesis seed: <n>` near the top of the step log. The next line prints the full local command.
3. Run the fast suite with that seed, replacing `<n>` with the number: `uv run pytest tests/unit tests/property --hypothesis-seed=<n>`.

CI uses the `default` Hypothesis profile. Do not set `HYPOTHESIS_PROFILE=ci` when reproducing: that profile is derandomized and ignores `--hypothesis-seed`.
