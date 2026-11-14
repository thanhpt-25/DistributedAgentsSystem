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
- `budget`: `null`, or for a Request with a [budget](#budgets) `{limits, used, exceeded}`: `limits` holds the limits that are set (`max_total_tokens`, `max_cost` as canonical decimal text, so `"0.50"` reads back as `"0.5"`), `used` holds `total_tokens` and `cost` (`null` when the cost cannot be priced), and `exceeded` is `true` once a limit was reached or cannot be enforced
- `webhook`: `null` until the Request has a [webhook](#webhooks) delivery, then `{state, attempts, last_status}`

Errors are flat JSON objects:

| Status | Body | Meaning |
|---|---|---|
| 400 | `empty_prompt`, `prompt_too_long` (with `max_length`), `invalid_body`, `invalid_request` (with `parameter`), `budget_above_limit` (with `parameter` and `limit`) | Bad submission; see [Idempotent submits](#idempotent-submits) and [Budgets](#budgets) |
| 401 | `unauthorized` | Missing or unknown API key |
| 404 | `not_found` | Unknown Request ID |
| 409 | `request_terminal` (with current `status`) | Cancel on a finished Request |
| 422 | `idempotency_key_reused` | The `Idempotency-Key` was used before with a different prompt or budget |
| 429 | `rate_limited` (with a `Retry-After` header), `quota_exceeded` (with `limit`), `too_many_streams` | A per-key limit was reached; see [Rate limits and quotas](#rate-limits-and-quotas) |
| 503 | `service_unavailable`, `too_many_streams` | Redis unreachable, or this Gateway replica already serves `events.max_connections` event streams |

### Idempotent submits

Send an `Idempotency-Key` header with a submit, so that retrying it cannot create a second Request. The key is 1 to 128 printable ASCII characters (no spaces).

```bash
curl -s -X POST $GW/v1/requests -H "X-API-Key: $KEY" \
  -H 'Idempotency-Key: order-1042' -H 'Content-Type: application/json' \
  -d '{"prompt": "Summarize the CAP theorem"}'
```

- The first call answers `202` with the new `request_id`.
- The same key with the same prompt and budget answers `200` with the same `request_id` and the header `Idempotent-Replayed: true`. No Request is created, and a replay is answered even when the key's quota is full.
- The same key with a different prompt or budget answers `422 {"error": "idempotency_key_reused"}`. `max_cost` is compared by value, so `"1.50"` and `"1.5"` are the same budget.
- A key that is not 1 to 128 printable ASCII characters answers `400 {"error": "invalid_request", "parameter": "Idempotency-Key"}`.
- Keys belong to the API key that sent them and are remembered for `client_limits.idempotency_ttl` seconds (default 86400). They stay **per API key, not per tenant**: two keys of one tenant never share an `Idempotency-Key`, and a **rotated key starts with no idempotency records** (a new key value has a new fingerprint), so a retry sent with the new key after a rotation creates a new Request. See [Rotating a key](#rotating-a-key).
- Without the header, every submit creates a new Request, as before.

### Budgets

Add a `budget` to a submit to cap what one Request may spend:

```bash
curl -s -X POST $GW/v1/requests -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"prompt": "Research the CAP theorem", "budget": {"max_total_tokens": 20000, "max_cost": "0.50"}}'
```

- `max_total_tokens` is a JSON integer of at least 1. `max_cost` is a number greater than 0 with at most 6 decimal places, in the currency of the [Price_Table](#price_table); send it as a string such as `"0.50"` so no float rounding applies. Set either or both.
- **`max_cost` needs a Price_Table** (`pricing` in the config). Without one, a Request with a `max_cost` fails before planning with `error: "budget_unenforceable"`, and its `budget.exceeded` already reads `true`. The same error ends a Request whose Job used a provider or model that is not in the Price_Table.
- A key can have a `default_budget` (see [Client API settings](#client-api-settings)). It applies to submits that send no budget, and it is a cap: a submit may ask for less, never more.
- Bad input answers `400 {"error": "invalid_request", "parameter": "budget.max_cost"}`; the parameter is `budget`, `budget.max_total_tokens`, `budget.max_cost`, or `budget.<name>` for an unknown field. A value above the cap answers `400 {"error": "budget_above_limit", "parameter": "budget.max_total_tokens", "limit": 1000}`.
- **What happens at the limit:** once usage reaches a limit, the Manager dispatches nothing more. Jobs that are already running finish, so usage can end slightly above the limit. Jobs that have not started are `Skipped` with reason `budget_exceeded`, no summary is written, and the Request ends `Failed` with `error: "budget_exceeded"`. A budget-stopped Request cannot be replayed (see the 409 under [Admin API](#admin-api)).
- `final_result` is only returned for a `Completed` Request, so a budget-stopped Request has none. `jobs` and `transitions` show which Jobs succeeded and which were skipped.

### Rate limits and quotas

Every `/v1` call, including opening an event stream and reading the OpenAPI document, takes one token from a per-API-key bucket. The bucket holds `burst` tokens and refills at `rate` tokens per second. It is kept in Redis, so the limit holds across every Gateway replica. `/healthz` and the Ops_Port are never limited.

- An empty bucket answers `429 {"error": "rate_limited"}` with a `Retry-After` header in whole seconds (at least 1). This check comes before everything else, so a limited call is not validated or counted.
- A key may have at most `max_active_requests` Requests that are not yet finished. One more answers `429 {"error": "quota_exceeded", "limit": 100}` (the number is the key's limit). A Request leaves the count when it becomes `Completed`, `Failed`, or `Cancelled`.
- A key may hold at most `max_streams` event streams open. One more answers `429 {"error": "too_many_streams"}`.
- Set `rate`, `max_active_requests`, or `max_streams` to `0` to turn that limit off. Different keys can have different settings: see [Client API settings](#client-api-settings).
- Every rejection is counted in `das_client_rejections_total{reason}`, with `reason` one of `rate_limited`, `quota_exceeded`, `too_many_streams`, `budget_above_limit`, `idempotency_key_reused`, `forbidden` (a key without the Scope or role its route needs), `tenant_quota_exceeded` (the tenant's own limit on unfinished Requests, answered `429` with `"limit_scope": "tenant"`), or `priority_above_limit` (a `priority` above the tenant's `max_priority`, answered `400` with the limit).

### Event streams

`GET /v1/requests/{id}/events` streams a Request's progress as Server-Sent Events (`text/event-stream`). It takes the same `X-API-Key` and counts against the same rate limit.

```bash
curl -N -H "X-API-Key: $KEY" $GW/v1/requests/REQUEST_ID/events
```

A stream of a Request that was then cancelled looks like this (the `snapshot` data is shortened):

```text
event: snapshot
id: 1794402379676-0
data: {"request_id":"REQUEST_ID","status":"Pending","jobs":[],"transitions":[],...,"budget":null,"webhook":null,"event_id":"1794402379676-0"}

event: request.cancelled
id: 1794402380695-0
data: {"request_id":"REQUEST_ID","event_id":"1794402380695-0","type":"request.cancelled","data":{"status":"Cancelled","error":null,"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}
```

- The first frame is always a `snapshot`: the same body as `GET /v1/requests/{id}`, without `final_result`, plus `event_id`, the id of the newest event it reflects (`null` if there is none yet).
- Every later frame is one event, in order: `event:` is its type, `id:` its event id, and `data:` the object `{request_id, event_id, type, data}`.
- The types are `request.created`, `request.planned` (Job ids, Capabilities, and dependencies; never instructions), `job.transition`, `request.reopened` (a Replay reopened a failed Request), `request.replanned` (only with `limits.max_replans` above 0: the Manager replaced the Jobs that failed with a new plan instead of failing the Request; it carries `replans`, `failed_jobs`, and `new_jobs`), and the terminal `request.completed`, `request.failed`, and `request.cancelled`. A terminal event carries `status`, `error`, the token `usage`, and `budget` when the Request has one. Events never contain the prompt, Job instructions, or results.
- **Resuming:** without `Last-Event-ID` the stream sends the snapshot and then only newer events. With `Last-Event-ID: <event id>` (a browser `EventSource` sends it on reconnect) it sends the snapshot and then every event after that id. If the id is not valid, or is older than the oldest event kept (the newest `events.max_len` are kept), the snapshot has `"resync": true` and the stream continues after the snapshot's own id: rebuild your state from the snapshot.
- A `: keepalive` comment is sent after `events.keepalive` seconds of silence.
- The stream ends after the terminal event. A Request that is already finished gets its snapshot, and any events after `Last-Event-ID`, and then the stream closes.
- A key may hold `max_streams` streams open (`429 too_many_streams` beyond it), and one Gateway replica serves at most `events.max_connections` (`503 too_many_streams`). A closed stream frees its slot.

### Webhooks

A webhook posts a Request's terminal event to a URL when the Request ends. Webhooks are configured **per API key by the operator**, not per request: see `webhook_url` and `webhook_secret_name` in [Client API settings](#client-api-settings). Requests created with a key that has no webhook have none.

A delivery is a `POST` of the event, with these headers:

```text
Content-Type: application/json
DAS-Webhook-Id: REQUEST_ID:1794402431264-0
DAS-Webhook-Timestamp: 1794402432
DAS-Webhook-Signature: v1=4885fcdc346945f01951464f5031295785efb9f62ae3d2a0cfc0ad6f4e6d35d8
```

and a body in canonical JSON (keys sorted, no spaces):

```json
{"data":{"error":null,"status":"Cancelled","usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}},"event_id":"1794402431264-0","request_id":"REQUEST_ID","type":"request.cancelled"}
```

**Verify every delivery.** `DAS-Webhook-Signature` is `v1=` followed by the hex HMAC-SHA256, keyed with your webhook secret, of the text `{timestamp}.{raw body}`, where `timestamp` is the `DAS-Webhook-Timestamp` header. Compare it in constant time, and reject a timestamp more than 5 minutes from your clock. With the Python [client package](#python-client):

```python
from das_client import InvalidSignature, verify_webhook

def handle(body: bytes, headers: dict[str, str]) -> int:
    try:
        event = verify_webhook(body, headers, SECRET)  # raises InvalidSignature
    except InvalidSignature:
        return 401
    if already_processed(headers["DAS-Webhook-Id"]):  # delivery is at-least-once: dedupe on this id
        return 200
    process(event)
    return 200
```

- **Delivery is at-least-once.** The same delivery can arrive more than once, for example when a Gateway replica stops after sending but before recording the result. Receivers must dedupe on `DAS-Webhook-Id` (`<request_id>:<event_id>`).
- Any `2xx` answer is a success. Any other answer (a redirect is not followed), a timeout (`webhook.timeout`), or a connection error is retried. The wait before retry `n` is `base_backoff * 2^(n-1)`, at most `max_backoff`, with up to 20% jitter. After `max_attempts` attempts in all, the delivery is `failed`. The answer's body is read up to 64 KiB and ignored.
- `webhook` in `GET /v1/requests/{id}` shows `state` (`pending`, `delivered`, or `failed`), `attempts`, and `last_status`: the HTTP status of the last attempt, `timeout`, `connect_error`, or `secret_missing`. If the secret is removed from the Gateway's config after the Request was created, the delivery fails with `secret_missing` and nothing is sent.
- A `webhook_url` must be `https`, unless `client_limits.allow_insecure_webhooks` is `true`.
- Each attempt is counted in `das_webhook_deliveries_total{outcome}`, with `outcome` one of `delivered`, `retry`, or `failed`.

### OpenAPI

`GET /v1/openapi.json` returns the OpenAPI 3.1 document of the client API, error responses included. It needs an API key like any other `/v1` call. The admin routes are not in it, and `/docs`, `/redoc`, and `/openapi.json` are not served.

## Python client

`clients/python` holds `das-client`, a small library for this API: Python 3.10 to 3.13, with `httpx` as its only dependency. It is not published to PyPI; install it from the repository:

```bash
pip install ./clients/python
```

```python
from das_client import Budget, DasClient

with DasClient("http://localhost:8080", "change-me-api-key") as client:
    submitted = client.submit("Research the CAP theorem", budget=Budget(max_total_tokens=20000))
    print(submitted.request_id, submitted.replayed)
    view = client.wait(submitted.request_id, timeout=300)  # streams events; polls if the stream fails
    print(view.status, view.final_result)
```

`AsyncDasClient` has the same methods as coroutines, and `events` as an async iterator:

```python
import asyncio

from das_client import AsyncDasClient

async def main() -> None:
    async with AsyncDasClient("http://localhost:8080", "change-me-api-key") as client:
        submitted = await client.submit("Research the CAP theorem")
        async for event in client.events(submitted.request_id):
            print(event.type, event.data)  # the snapshot first, then each event, ending at the terminal one

asyncio.run(main())
```

- The methods are `submit`, `get`, `cancel`, `list_requests`, `events`, and `wait`, plus `verify_webhook` for receivers.
- `submit` without an `idempotency_key` makes a random one and reuses it for every retry of that call, so a retried submit cannot create two Requests.
- Connection errors, `503`, and `429` are retried up to `max_retries` times (default 3), waiting 0.5 s doubling to 8 s with jitter, and at least the `Retry-After` of a `429`.
- `events` resumes with `Last-Event-ID` after a broken stream and stops after the terminal event. `wait` returns the final view, polling every second if streaming keeps failing.
- Errors derive from `DasError`: `AuthError` (401, 403), `NotFound` (404), `Conflict` (409), `IdempotencyKeyReused` (422), `InvalidRequest` and `BudgetAboveLimit` (400, with `.parameter`), `RateLimited` (with `.retry_after`) and `QuotaExceeded` (with `.limit`) after the retries, `ServiceUnavailable` (503) and `TransportFailure` (cannot connect), `WaitTimeout`, and `InvalidSignature` from `verify_webhook`.
- Responses are read leniently: a field the server adds later does not break an older client.

## Tenants and scopes

Without a `tenants` section, every key in `DAS__API_KEYS` belongs to the **default tenant** with the Scopes `submit` and `read`, and every key in `DAS__ADMIN_API_KEYS` is an **Operator**: the single-team behaviour of earlier phases. A `tenants` section gives a team its own keys, Scopes, quota and priority. Its Requests, dead letters and usage are then invisible to every other tenant.

```yaml
# deploy/config/gateway.yaml
tenants:
  - id: team-a                     # lowercase letters, digits and hyphens, up to 32; not "default"
    keys:
      - fingerprint: "484857e0"    # the placeholder key `change-me-api-key`
        scopes: [submit, read]
      - fingerprint: "ee978a75"    # a second key of the same tenant that may also administer its dead letters
        scopes: [read, admin]
    max_active_requests: 200       # unfinished Requests the whole tenant may have
    default_priority: normal
    max_priority: high
    settings:
      rate: 120.0
      burst: 240
      default_budget:
        max_total_tokens: 100000
  - id: team-b
    keys:
      - fingerprint: "0d41cb3f"
        scopes: [submit, read]
```

A key is named by its fingerprint, never by its value (`printf %s "$KEY" | shasum -a 256 | cut -c1-8`), and must also be one of `DAS__API_KEYS`. A fingerprint that matches no key, an Admin_API_Key, a fingerprint named twice or by two tenants, or a `default` tenant is refused when the Gateway starts, and the message names the field, never a key. A key that no tenant names stays in the default tenant. Configure `tenants` only after the upgrade has finished: see [Upgrading to Phase 5](#upgrading-to-phase-5).

### Scopes

| Scope | What it allows |
|---|---|
| `submit` | `POST /v1/requests` and `POST /v1/requests/{id}/cancel` |
| `read` | `GET /v1/requests` (the listing), `GET /v1/requests/{id}`, `GET /v1/requests/{id}/events`, `GET /v1/agents`, `GET /v1/usage`, `GET /v1/openapi.json` |
| `admin` | the dead-letter routes of the key's own tenant: `GET /v1/admin/dlq`, `GET /v1/admin/dlq/{id}`, `POST /v1/admin/dlq/{id}/replay`, `POST /v1/admin/dlq/{id}/discard` |
| Operator | an Admin_API_Key: every Scope on every tenant, and the platform routes `POST /v1/admin/agents/{id}/drain`, `POST /v1/admin/agents/{id}/undrain` and `GET /v1/admin/usage` |

A call without the Scope its route needs answers `403 {"error": "forbidden", "scope": "submit"}`, naming the Scope (or `operator`). The check comes before anything is read or written and before the rate limit, so a refused call costs no token and creates nothing. `GET /healthz` needs no key. When nothing in the configuration can administer (no Admin_API_Key and no key with `admin`), the admin routes keep answering `403 {"error": "admin_disabled"}` as before.

### What a tenant sees

- **Requests.** Another tenant's Request, by `GET`, cancel or events, answers `404 {"error": "not_found"}`, exactly as a Request that does not exist, so a Request_ID reveals nothing. A cancel of another tenant's Request writes nothing.
- **The listing.** `GET /v1/requests` returns only the caller's tenant. A `next_cursor` belongs to the caller's scope: used by another tenant, or by an Operator, it answers `400 {"error": "invalid_query", "parameter": "cursor"}`. The `tenant` parameter is for Operators (`GET /v1/requests?tenant=team-a`); from anyone else it answers `400 {"error": "invalid_query", "parameter": "tenant"}`.
- **Dead letters.** A key with `admin` lists, reads, replays and discards the dead letters of its own tenant. A record of another tenant, or a record whose Request no longer exists, answers `404`. An Operator sees every record and may filter with `tenant`.
- **Agents.** `GET /v1/agents` shows a tenant key the Capabilities, status and load of the agents, without their `agent_id` and `host`. The default tenant and Operators see them.
- **Quota.** `max_active_requests` directly under a tenant is a limit for the whole tenant, on top of the per-key one. At the limit a submit answers `429 {"error": "quota_exceeded", "limit": 200, "limit_scope": "tenant"}` and creates nothing; the per-key body is unchanged and is reported first when both limits are reached. The tenant's `settings` sit between `client_limits.default` and a per-key override: a field a layer leaves out is inherited from the layer below.
- **The Operator** sees every tenant. Only the Operator may use `tenant` and the platform routes.

Every Audit_Entry of an admin call carries the caller's `tenant` and whether it was an Operator, next to its key fingerprint; key values are never logged.

### Tenant settings

| Setting | Default | Meaning |
|---|---|---|
| `tenants[].id` | none | The tenant's name: lowercase letters, digits and hyphens, up to 32, not `default` |
| `tenants[].keys[].fingerprint` | none | An API key of this tenant, by fingerprint (8 hex characters) |
| `tenants[].keys[].scopes` | none | Any of `submit`, `read`, `admin` |
| `tenants[].max_active_requests` | 0 | Unfinished Requests the whole tenant may have; `0` means no tenant limit |
| `tenants[].default_priority` | `normal` | The priority of a submit that names none; not above `max_priority` |
| `tenants[].max_priority` | `normal` | The highest priority a submit may ask for (`normal` or `high`) |
| `tenants[].settings` | none | The per-key settings of [Client API settings](#client-api-settings) for every key of the tenant |
| `usage.retention_days` | 400 | Days a tenant's daily usage rollup is kept (30 to 3650) |
| `metrics.label_limits.tenant` | 100 | Distinct tenant values of the `tenant` metric label (1 to 500) |

### Rotating a key

Add the new key to `DAS__API_KEYS` and its fingerprint to the tenant, redeploy the Gateways, move the clients to the new key, then remove the old key and its fingerprint. Both keys work during the overlap. Idempotency records are per key, so the new key starts with none: a retry sent with the new key after the move is a new submit.

## Priority

A submit may carry `"priority": "high"` (or `"normal"`). Without it the tenant's `default_priority` applies. A value above the tenant's `max_priority` answers `400 {"error": "priority_above_limit", "limit": "normal"}` and creates nothing; any other value answers `400 {"error": "invalid_request", "parameter": "priority"}`. The default tenant may use `high`. The Request view shows its `priority`, `normal` for a Request that has none.

Priority is a **weighting, not a strict order**. With both classes queued, `high` Jobs are delivered about two for every `normal` one, and `normal` Jobs are never starved. Every dispatch of a `high` Request, a retry or a re-dispatch after a takeover or a Replay included, is published with the header `das-priority: high`; the RabbitMQ transport turns it into AMQP priority 9 on the quorum queues, which RabbitMQ serves as a second class. Queue declarations, the `das-jobs` policy and the message payload are unchanged.

## Usage reports

`GET /v1/usage` is the report of the caller's own tenant (Scope `read`); `GET /v1/admin/usage` is the Operator's, for one tenant (`?tenant=team-a`) or for every tenant (`{"tenants": [...]}`).

```bash
curl -s "$GW/v1/usage?from=2026-11-01&to=2026-11-07" -H "X-API-Key: $KEY"
```

- The range is UTC days, `from` and `to` inclusive, `YYYY-MM-DD`. Without them the report covers the last seven days up to today by the State_Store clock. A malformed, inverted or longer than 92 days range answers `400 {"error": "invalid_query", "parameter": "from"}` (or `"to"`).
- The body has one entry per day with the Requests `submitted` and `finished` (`Completed`, `Failed`, `Cancelled`), the tokens in and out per `{provider}/{model}`, and `totals` for the range.
- `estimated_cost` is computed when the report is read, from the [Price_Table](#price_table): `null` without one.
- **Usage recorded before the upgrade is not in any report.** Requests written by Phase 4 never wrote the daily rollup, and no report is back-filled.
- Tokens and cost per tenant are served by these reports, not by Prometheus. The only per-tenant metrics are the `tenant` label of `das_requests_submitted_total` and `das_requests_finished_total` (and the Grafana panel of them); `das_client_rejections_total` gains the reasons `forbidden`, `tenant_quota_exceeded` and `priority_above_limit`.

## Kubernetes (Helm chart)

Docker Compose stays a supported way to run DAS. The chart in `deploy/helm/das/` runs the same three roles on Kubernetes: Gateway, Manager and Worker Deployments, one Worker Deployment per Capability set, optional KEDA autoscaling of Workers, a `preStop` drain so a pod finishes its Jobs before it stops, and a `values.schema.json` that rejects unknown or mistyped values. Redis and RabbitMQ are prerequisites, not part of the chart, and every secret comes from a Secret that already exists.

```bash
helm install das deploy/helm/das -f my-values.yaml
```

The chart's own [README](deploy/helm/das/README.md) covers the prerequisites, the Secret keys, the values, tenants, the grace period rule, KEDA, draining and how to test it. CI renders the chart for two values files with `helm lint`, `helm template` and `kubeconform -strict`, and the `k8s` job installs it on a throwaway kind cluster. If you protect a branch, the required status checks are the job names of the workflow: `Fast_Job`, `Client_Job (Python 3.10)`, `Client_Job (Python 3.11)`, `Client_Job (Python 3.12)`, `Client_Job (Python 3.13)`, `Integration_Job`, `E2E_Job` and `Kind_Job`.

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

Client API metrics (on the Gateway, except the last):

| Metric | Type | Labels and meaning |
|---|---|---|
| `das_client_rejections_total` | counter | `reason`: `rate_limited`, `quota_exceeded`, `too_many_streams`, `budget_above_limit`, `idempotency_key_reused`, `forbidden`, `tenant_quota_exceeded`, `priority_above_limit` |
| `das_idempotent_replays_total` | counter | Submits answered from an idempotency record (not counted as new Requests) |
| `das_sse_streams` | gauge | Event streams open on this Gateway replica |
| `das_webhook_deliveries_total` | counter | `outcome`: `delivered`, `retry`, `failed` |
| `das_budget_exceeded_total` | counter | On the Manager. `kind`: `tokens`, `cost`, `unenforceable` |

## Admin API

The Admin_API lives under `/v1/admin` on the Gateway. It is disabled until you set `DAS__ADMIN_API_KEYS` in `deploy/.env` (comma-separated; each key 16 to 256 printable ASCII characters, at most 32 keys, none equal to a `DAS_GATEWAY_API_KEYS` entry). Send an admin key in the same `X-API-Key` header. An admin key also works on every normal `/v1` endpoint.

| Status | Body | Meaning |
|---|---|---|
| 401 | `unauthorized` | Missing or unknown key |
| 403 | `forbidden` | The key lacks what the route needs; the body names it: `"scope": "admin"` on the dead-letter routes, `"scope": "operator"` on drain, undrain and `/v1/admin/usage` (see [Scopes](#scopes)) |
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
| 409 | `budget_exceeded` | Replay refused: the Request failed by its budget (`budget_exceeded` or `budget_unenforceable`), so a replay would spend it again. Nothing was changed |
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

## Upgrading to Phase 4

Phase 4 adds idempotency keys, budgets, per-key limits, event streams, and webhooks. A client that sends no `Idempotency-Key` and no `budget`, and stays within the default limits, sees the same responses as before, except that `GET /v1/requests/{id}` has two more fields, `budget` and `webhook` (both `null` for such a Request).

- **Upgrade order.** Gateways and Managers can be upgraded in any order. **Events are complete only after every Manager runs Phase 4:** a Manager that has not been upgraded writes no events and does not enforce budgets, so a stream of a Request it handles shows a gap. Upgrade every Manager before you rely on event streams or budgets.
- **Requests created before the upgrade** have no budget, no webhook, and do not count towards any key's quota. A stream of one sends its snapshot followed by whatever events exist.
- **The limits are on by default.** After the upgrade each API key is limited to 60 requests per second (burst 120), 100 unfinished Requests, and 10 open event streams. A client that ran faster or wider before will see `429` responses until the limits are raised or set to `0`; see [Client API settings](#client-api-settings). Clients that retry should wait for `Retry-After`.
- The new metrics are listed under [Ops_Port endpoints](#ops_port-endpoints-metrics-liveness-readiness).

## Upgrading to Phase 5

Phase 5 adds [tenants and scopes](#tenants-and-scopes), [priority](#priority), [usage reports](#usage-reports) and a [Helm chart](#kubernetes-helm-chart). A client of the default tenant that sends no `priority` and uses no new endpoint sees the same responses as in Phase 4, except that `GET /v1/requests/{id}` has one more field, `priority` (`normal` for a Request that has none), a 403 on an admin route names its Scope, and the Audit_Entry has the fields `tenant` and `operator`.

1. **Upgrade every Gateway, Manager and Worker, in any order.** The keys Phase 5 adds are additive and ignored by Phase 4 instances, so a rollback loses no Phase 4 data.
2. **Wait for the Tenant_Backfill.** The first Manager that starts files every existing Request and dead letter under the default tenant, then sets `das:tenant:index:ready` in Redis and logs `tenant_index_backfilled`. Until then a listing for the default tenant reads the global index and a listing for any other tenant answers `503 {"error": "index_not_ready"}`.
3. **Then add `tenants`** to the Gateway configuration and redeploy the Gateways. Configure tenants only after the flag is set.

What does not carry over: **usage from before the upgrade is not in any report**, and **idempotency records stay per key**, so a rotated key starts new ones. Requests created before the upgrade belong to the default tenant. Replay, drain and the other admin actions still need every instance upgraded, as in [Upgrading from v1](#upgrading-from-v1).

### Not in Phase 5

Redis replication, Sentinel or Cluster; a key-management API, or keys stored in Redis (keys and tenants come from the configuration); per-tenant worker pools or queues; SSO or OAuth; billing integration; tool use or code execution; HTTP or Kafka transports; and per-tenant token or cost metrics in Prometheus (use the usage reports).

## Smarter agents (Phase 6)

Phase 6 lets a Job return JSON that matches a schema, lets operators pick a model per Capability and a fallback, lets a Request try a new plan after a failure, measures the Planner, and lets a Worker use tools and (carefully) run code. Every feature is **off unless you configure it**; see [Upgrading to Phase 6](#upgrading-to-phase-6) and the [settings table](#phase-6-settings).

### Structured output

A planned Job may carry `output_schema`, a JSON Schema object (at most 16 KiB serialized, nested at most 16 levels). The Planner is told it may add one; a plan with an invalid schema is rejected with the check `invalid_output_schema`. A Job without `output_schema` is dispatched and run exactly as before.

```json
{"jobs": [
  {"job_id": "find", "instruction": "List three facts about the Moon.", "capabilities": ["research"],
   "output_schema": {"type": "object", "properties": {"facts": {"type": "array", "items": {"type": "string"}, "minItems": 3}},
                     "required": ["facts"], "additionalProperties": false}},
  {"job_id": "write", "instruction": "Write one paragraph from the facts.", "capabilities": ["summarize"], "depends_on": ["find"]}
]}
```

The Worker parses the completion as JSON (one Markdown code fence around it is accepted) and validates it against the schema. If it does not match, the Worker makes **one** corrective completion in the same attempt, naming the first violation's JSON path and message (never the rejected content); the tokens of both completions are in the Job_Result usage. If that is still wrong the Job fails with `output_schema_violation: <path>: <message>` and follows the normal retry and dead-letter rules. A valid result is stored as canonical JSON (sorted keys, no extra whitespace). A completion over 256 KiB is a violation without being parsed.

### Models by Capability and the fallback

`llm.capability_models` maps a Capability to a model name: a Worker runs a Job with the model of the Capability it was routed by, else `llm.model`. `llm.fallback_model` is a safety net: when a completion fails with a **permanent** provider error, the LLMProvider makes exactly one further completion with the fallback. An authentication or authorization error (HTTP 401 or 403) does **not** trigger it, because no other model will be let in either. The Planner and Summarizer use `llm.model` and the fallback, never a Capability model. The Job_Result header `x-llm-model` and the usage records name the model that really answered, so cost estimates use its price. `das_llm_fallbacks_total{provider, model}` counts each use, `model` being the one that failed. An empty model name is a configuration error naming the field.

```yaml
llm:
  provider: anthropic
  model: claude-sonnet-5-5
  capability_models:
    summarize: claude-haiku-5-5
  fallback_model: claude-opus-5-5
```

### Replanning

`limits.max_replans` (0 to 3) is **off by default**. Above 0, when every Job of a Request is terminal and at least one Failed or was Skipped, the Manager asks the Planner for a new plan instead of finishing the Request. The new plan keeps every Succeeded Job unchanged and replaces the rest (fresh Job ids); it is written atomically under the lease and the Request stays Running. A `request.replanned` event carries `replans`, `failed_jobs` and `new_jobs`, and the Request view has `replans` (0 when never replanned). A budget still bounds the whole Request, replanning tokens included, and a budget stop is never replanned. If replanning itself fails, the Request fails with its original error. Replaying the dead letter of a replaced Job is refused.

```yaml
limits:
  max_replans: 1
```

### Plan evaluation

`das eval-plans` runs the real Planner, with the configured provider, over a JSON Lines dataset (`--dataset`) and prints a JSON report (per-line results and the valid ratio, expectation ratio, mean attempts and total tokens).

```bash
das eval-plans --config deploy/config/manager.yaml --dataset plans.jsonl --min-pass 0.9
```

A dataset line is a JSON object with `prompt`, `capabilities` (what the Planner is offered) and an optional `expect` (`min_jobs`, `max_jobs`, `require_capabilities`, `forbid_capabilities`, `require_output_schema`):

```jsonl
{"prompt": "Research the Moon and summarize it", "capabilities": ["research", "summarize"], "expect": {"min_jobs": 2, "require_capabilities": ["summarize"]}}
{"prompt": "Say hello", "capabilities": ["chat"], "expect": {"max_jobs": 1}}
```

Exit code **0** when both ratios reach `--min-pass` (default 0, so a run without a threshold never fails on quality), **1** when either falls short, and **2** for a dataset line or configuration that cannot be used (the line number is named; no LLM call has been made). Logs go to stderr, the report alone to stdout.

### Tools

A Worker can let the model call tools. Nothing is enabled by default. Each enabled tool is advertised as a Capability, `tool-calculator`, `tool-http-fetch` or `tool-current-time`; a Job that requires such Capabilities is offered **exactly** those tools, and a Job that requires none runs as before. The Worker loops: complete, run each requested tool, append the results, complete again, until the model answers in text. A Job may use `tools.max_rounds` rounds of tools and is then asked once more; a model that still asks for tools fails the Job with `tool_rounds_exceeded`. The tokens of every round are in the usage; the Job timeout and a cancel stop the loop, also during a tool call. A tool's error, refusal or timeout goes back to the model as the result text, never as a failure of the Job, cut to `tools.max_result_bytes`. Tool names and result sizes are logged, never arguments or results. `das_tool_calls_total{tool, outcome}` counts calls (`ok`, `error`, `timeout`). The LLM provider must support tools (all four do); a Worker that enables a tool with one that does not exits with code 2.

- `calculator`: arithmetic through a safe parser (numbers, `+ - * / // % **`, parentheses, a few math functions), never `eval`, with limits on length, nesting, exponent and result size.
- `http_fetch`: `GET` only, `https` only (`http` only with `allow_http`), only the hosts in `allowed_hosts` (at least one is required to enable it), the default port, no credentials in the URL, **no redirects**, a timeout and a size cap. It refuses a host that resolves to a private, loopback, link-local or reserved address, however it is written, and it connects to the address it checked, so a changing DNS answer cannot redirect it.
- `current_time`: the current UTC time from the Worker's clock.

```yaml
tools:
  enabled: [calculator, current_time, http_fetch]
  max_rounds: 4
  http_fetch:
    allowed_hosts: [docs.example.com]
```

### Code execution

> **Warning.** A Code_Worker runs code written by a language model, which a prompt can steer. The sandbox makes this safer, not safe. It is **off by default**; enable it only on a **dedicated host** you can afford to lose, and read [docs/security/code-execution.md](docs/security/code-execution.md) first (threat model, every limit, what the sandbox does not protect against, and how to run one).

A Worker with `sandbox.enabled: true` advertises `code-execution`; a Job that requires it is offered one tool, `run_python`, through the tool loop above. Each run is a throwaway container with no network, a read-only root, a small scratch directory, all capabilities dropped, no privilege gain, a non-root user, and memory, CPU, process, time and output limits that have ceilings no configuration can raise (timeout 60 s, 1 GiB, 2 CPUs, 256 processes, 64 KiB of output). The code goes in on stdin: no mount, nothing on the host disk, never logged. The container is always removed. At start-up the Worker asks the container runtime and exits with code 4 naming it if it does not answer. **Never mount the container runtime's socket** into any container: the repository's Compose files and Helm chart do not, and a test fails if they ever do. `das_sandbox_runs_total{outcome}` counts runs (`ok`, `error`, `timeout`, `killed`).

```yaml
worker:
  capabilities: [llm]
sandbox:
  enabled: true
  container_cli: docker
  image: python:3.12-slim
  oci_runtime: runsc
  timeout: 10
  memory_mb: 256
```

### Phase 6 settings

| Field | Default | Range | Purpose |
|---|---|---|---|
| `llm.capability_models` | none | Capability to non-empty model name | Model per Capability |
| `llm.fallback_model` | none | a non-empty model name | One more try after a permanent error |
| `limits.max_replans` | 0 | 0 to 3 | Replans per Request; 0 is off |
| `tools.enabled` | none | calculator, http_fetch, current_time | Tools the Worker offers |
| `tools.max_rounds` | 4 | 1 to 10 | Rounds of tool use per Job |
| `tools.max_result_bytes` | 16384 | 256 to 262144 | Bytes of a tool result the model sees |
| `tools.http_fetch.allowed_hosts` | none | up to 50 host names; at least 1 to enable `http_fetch` | Hosts it may fetch from |
| `tools.http_fetch.allow_http` | false | true or false | Also allow plain `http` |
| `tools.http_fetch.timeout` | 10 | 1 to 60 | Seconds per fetch |
| `tools.http_fetch.max_bytes` | 1048576 | 1024 to 10485760 | Bytes of a response read |
| `sandbox.enabled` | false | true or false | Make this Worker a Code_Worker |
| `sandbox.container_cli` | docker | docker or podman | Container runtime CLI |
| `sandbox.image` | python:3.12-slim | a plain image reference | Image a run starts from |
| `sandbox.oci_runtime` | none | a plain runtime name such as runsc | Alternative OCI runtime |
| `sandbox.timeout` | 10 | 1 to 60 | Seconds before a run is killed |
| `sandbox.memory_mb` | 256 | 64 to 1024 | Memory of a run, no swap |
| `sandbox.cpus` | 1 | 0.1 to 2 | CPUs of a run |
| `sandbox.pids` | 64 | 16 to 256 | Processes of a run |
| `sandbox.output_bytes` | 16384 | 256 to 65536 | Bytes of output kept |

### Upgrading to Phase 6

With none of the new configuration set, nothing changes: every response, message and metric of Phase 5 is the same, apart from one additive field, `replans` (0), in the Request view, and the additions to the OpenAPI document and the metrics catalog. A message without `output_schema` is encoded byte for byte as before, and the contract baseline changed only by additions. Upgrade Gateways, Managers and Workers in any order; turn features on one at a time. A Worker that lists `code-execution` without `sandbox.enabled` does not start.

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
| `client_limits`, `events`, `webhook` | gateway | Per-key rate limits, quotas, budgets, and webhook targets; event streams; the webhook sender. See [Client API settings](#client-api-settings) |
| `webhook_secrets` | gateway | Webhook signing secrets by name (`DAS__WEBHOOK_SECRETS__<NAME>`, credential: environment or secrets file only) |
| `retention.*` | manager | Retention_Policy, see below |
| `pricing.*` | gateway, manager, worker | Price_Table, see below |

If you change `limits.max_attempts`, set the `das-jobs` policy `delivery-limit` in `deploy/rabbitmq/definitions.json` to `max_attempts - 1`.

### Client API settings

The Gateway reads these from its config. `deploy/config/gateway.yaml` lists every one with its default (a test keeps that file equal to the defaults), so it is the place to start. They can also be set as environment variables in the usual form, for example `DAS__CLIENT_LIMITS__DEFAULT__RATE=0`. Limits are per API key and shared by every Gateway replica.

| Field | Default | Meaning |
|---|---|---|
| `client_limits.default.rate` | 60 | Requests per second per API key; `0` turns the rate limit off |
| `client_limits.default.burst` | 120 | How many requests may arrive at once before the rate applies |
| `client_limits.default.max_active_requests` | 100 | Unfinished Requests per key; `0` turns the quota off |
| `client_limits.default.max_streams` | 10 | Open event streams per key; `0` turns the cap off |
| `client_limits.default.default_budget` | none | A budget (`max_total_tokens`, `max_cost`) applied to submits that send none, and a cap on what a submit may ask for |
| `client_limits.default.webhook_url` | none | Where terminal events are posted (`https`, unless `allow_insecure_webhooks`) |
| `client_limits.default.webhook_secret_name` | none | The name of the webhook secret that signs them |
| `client_limits.overrides` | none | Per-key settings, matched by Key_Fingerprint; see below |
| `client_limits.idempotency_ttl` | 86400 | Seconds an `Idempotency-Key` is remembered (60 to 604800) |
| `client_limits.allow_insecure_webhooks` | false | Allow `http://` webhook URLs, for testing only |
| `events.max_len` | 1000 | Events kept per Request (100 to 100000); the oldest are dropped beyond it |
| `events.max_connections` | 500 | Event streams one Gateway replica serves at once (`503` beyond it) |
| `events.keepalive` | 15 | Seconds of silence before a `: keepalive` comment is sent (up to 300) |
| `webhook.workers` | 2 | Delivery workers per Gateway replica; `0` turns the sender off on that replica |
| `webhook.timeout` | 10 | Seconds to wait for the receiver's answer |
| `webhook.base_backoff` | 30 | Seconds before the first retry; it doubles each attempt |
| `webhook.max_backoff` | 3600 | The longest wait between attempts (at least `base_backoff`) |
| `webhook.max_attempts` | 8 | Attempts in all before a delivery is `failed` |

To give one API key its own settings, add an entry to `client_limits.overrides` with the key's fingerprint: the first 8 hex characters of `sha256(api_key)`. Fields an entry leaves out are inherited from `client_limits.default`. A fingerprint that matches no configured key, or that is listed twice, stops the Gateway from starting.

```bash
printf %s "$KEY" | shasum -a 256 | cut -c1-8    # Linux: sha256sum; for the placeholder key change-me-api-key this is 484857e0
```

```yaml
client_limits:
  default:
    rate: 30.0
    max_active_requests: 20
    default_budget:
      max_total_tokens: 200000
  overrides:
    - fingerprint: "484857e0"  # the key change-me-api-key
      rate: 600.0
      burst: 1200
      webhook_url: https://hooks.example.com/das
      webhook_secret_name: partner
```

```yaml
events:
  max_connections: 200
webhook:
  workers: 4
  max_attempts: 5
```

A webhook secret is set as `DAS__WEBHOOK_SECRETS__PARTNER` for `webhook_secret_name: partner` (the name is lower-cased). It must be at least 16 characters and comes only from the environment or the secrets file, never from YAML. **Never leave it set but empty:** an empty or short value stops the Gateway from starting. In Docker Compose, `deploy/.env.example` keeps the line commented out and the `gateway` service passes the name through.

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
- Python 3.10 to 3.13 for the client package's own tests (uv installs the interpreter on demand).
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

Fast suite (unit, property, and contract tests, no Docker). The contract tests run the Python client package against the real Gateway app in-process:

```bash
uv run pytest tests/unit tests/property tests/contract
```

Client package suite (`clients/python`, Python 3.10 to 3.13, no Docker). The client is not a uv workspace member: the server's lock is Python 3.12 only, and a member would pin the client to it. So its own tests run in an isolated environment, on the interpreter named by `UV_PYTHON` (CI sets it from its Python matrix; locally, run `UV_PYTHON=3.10 uv run ...`, or add `--python 3.10`):

```bash
uv run --isolated --no-project --with-editable clients/python --with pytest==9.1.1 --with pytest-asyncio==1.4.0 --with respx==0.23.1 pytest clients/python/tests -c clients/python/pyproject.toml --rootdir clients/python
```

Integration suite (RabbitMQ and gRPC through testcontainers, needs Docker):

```bash
uv run pytest tests/integration --ignore=tests/integration/e2e --ignore=tests/integration/k8s
```

Kubernetes suite (the Helm chart on a throwaway kind cluster; opt-in with `DAS_K8S_E2E=1`, needs Docker, `kind`, `kubectl`, `helm` and the three images built as `das-<role>:local`, about 4 min once they exist):

```bash
uv run pytest tests/integration/k8s
```

E2E suite (full Compose stacks with a fake LLM, needs Docker and Compose 2.30+, about 4 min once the images are built; the first run also builds the three `das-*` images, about 2.5 min):

```bash
uv run pytest tests/integration/e2e
```

The suite brings up three Compose projects, one after the other, each torn down with `down -v` before the next: `das-e2e` (the platform), `das-e2e-obs` (the observability stack) and `das-e2e-client` (the client API: two Gateways, two Managers, two Workers and a webhook sink, about 70 s). The ports they publish are chosen free for each run, so nothing in them is fixed. The client API stack checks the shared Rate_Limit bucket, event latency, webhook retries with signatures, a budget stop, `das-client` against the real stack, and a stream that survives killing the Manager that owns the Request. `DAS_E2E_CLIENT_OVERRIDE_EXTRA` names one more Compose file to layer on top of that stack, for trying a deliberately broken variant.

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

The workflow `.github/workflows/ci.yml` (named **CI**) has five jobs. Every job installs uv 0.13.6 and runs the commands listed below, which are the same commands as in [Development](#development). `fast`, `integration`, `e2e`, and `k8s` use Python 3.12 and run `uv sync --locked` first. `client` sets up each supported Python itself and does not sync, because it tests the client package in an isolated environment. Each job reports its own status check on the commit and pull request; `client` reports one per Python (`Client_Job (Python 3.10)` and so on).

| Job | Commands | Runs on | Timeout |
|---|---|---|---|
| `fast` (Fast_Job) | `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property tests/contract` (which includes the Helm chart's rendered-output tests), then `helm lint`, `helm template` and `kubeconform -strict` on the chart for `deploy/helm/das/ci/values-min.yaml` and for `deploy/helm/das/ci/values-all.yaml` (every optional feature on) | every push to any branch (not tags); pull requests when opened, synchronized, or reopened; manual run | 15 min |
| `client` (Client_Job) | the client package suite from [Development](#development), once for each of Python 3.10, 3.11, 3.12, and 3.13 | same as `fast`; does not wait for it | 10 min per Python |
| `integration` (Integration_Job) | `uv run pytest tests/integration --ignore=tests/integration/e2e --ignore=tests/integration/k8s`, then the repeat-run check (`... --count 20 -v`) | push to `main`; manual run | 30 min |
| `e2e` (E2E_Job) | checks Docker Compose is 2.30 or later, then `uv run pytest tests/integration/e2e` | push to `main`; manual run | 30 min |
| `k8s` (Kind_Job) | installs helm, kind and kubectl, builds the three images, then `uv run pytest tests/integration/k8s` with `DAS_K8S_E2E=1` | push to `main`; manual run | 30 min |

`integration`, `e2e` and `k8s` start only after `fast` in the same workflow run finishes with status success. If `fast` fails or is cancelled, they do not run. On pull requests and on pushes to other branches they are reported as skipped. All three set `DAS_REQUIRE_DOCKER=1`, so a Docker-related skip fails the job. In `fast`, every check runs even if one of them fails, so one run shows every failure. When `e2e` fails, the container logs are uploaded as the `e2e-logs-<run id>-<attempt>` artifact; when `k8s` fails, every pod's logs are uploaded as `k8s-logs-<run id>-<attempt>`.

The Helm, kubeconform, kind and kubectl versions are pinned in the workflow's `env`, each with the SHA-256 of its download, and a downloaded binary is verified before it is installed. To upgrade one, change its version and its checksum together, taking the checksum from the publisher's own checksum file. `KUBERNETES_SCHEMA_VERSION` is the Kubernetes version `kubeconform` validates the rendered chart against.

A new commit on the same branch, for the same event type, cancels the previous run that is still in progress.

### Running CI manually

To run every job on any branch:

1. Open the repository on GitHub and go to the **Actions** tab.
2. Select the **CI** workflow in the list on the left.
3. Click **Run workflow**.
4. In **Use workflow from**, choose the branch to run.
5. Click the green **Run workflow** button.

### Reproducing a Hypothesis failure

Each `fast` run picks a random Hypothesis seed. To rerun a failed run locally with the same seed:

1. Open the failed run, then the `fast` job, then the step `uv run pytest tests/unit tests/property tests/contract`.
2. Find the line `Hypothesis seed: <n>` near the top of the step log. The next line prints the full local command.
3. Run the fast suite with that seed, replacing `<n>` with the number: `uv run pytest tests/unit tests/property tests/contract --hypothesis-seed=<n>`.

CI uses the `default` Hypothesis profile. Do not set `HYPOTHESIS_PROFILE=ci` when reproducing: that profile is derandomized and ignores `--hypothesis-seed`.
