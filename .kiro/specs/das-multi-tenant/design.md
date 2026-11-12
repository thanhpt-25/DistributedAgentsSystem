# Design Document: DAS Multi-Tenant and Deployable (Phase 5)

## Overview

Phase 5 lets one DAS installation serve several teams and run on Kubernetes. Tenancy is a **field plus additive keys**: the Request hash gains `tenant`, and every tenant-visible index gets a per-tenant twin that the same Lua scripts keep in step. No existing key is renamed, so there is no data migration, only a backfill. Keys belong to tenants through their Key_Fingerprint in configuration (the `client_limits.overrides` technique), so no new secret channel appears. A small pure core (`das/tenancy/`) resolves a Principal and decides authorization from one route table. Priority is a reserved Message header that the RabbitMQ adapter turns into the AMQP `priority` property. The Helm chart is verified statically on every push and on a `kind` cluster on `main`.

Facts this design rests on, each checked in the code or by the G0 spikes (`docs/superpowers/plans/2026-11-12-das-phase5-multi-tenant-deployable.md`, section 9):

- `_common.lua` already derives keys inside scripts from the request key (`key_prefix(req_key)` builds `das:client:{fp}:active`), and says it is only safe on a single Redis primary. Tenant keys use the same technique.
- `list_index.lua` takes its source zsets as `KEYS`, so tenant listing needs different key names from Python, not a new script.
- `append_usage` in `_common.lua` is the single place every UsageRecord is persisted exactly once (from `commit_plan`, `apply_transition` and `finalize`).
- The Manager's Job dispatch publish (`das/manager/driver.py`, the call without `headers`) is the one place a header can be added for all retries, takeovers and replays.
- RabbitMQ 4.1.8 quorum queues reject `x-max-priority`, accept a plain `priority` property, have two levels (0 to 4 normal, 5 to 9 high) with a measured 2:1 weighting and no starvation, and keep priority through requeue and dead-lettering.
- On Kubernetes the unmodified images run; the gRPC advertise host must be the pod IP; a pod killed mid-Job loses nothing; the grace period must exceed `shutdown_timeout`, not `job_timeout`.

### Design rules kept from earlier phases

- Pure core, thin I/O shell. New pure logic lives in `das/tenancy/` (scopes, Principal, key resolution, settings layering, usage report assembly) and is property-tested.
- Redis is the source of truth. Every new key is written by a Lua script in the same call as the change it mirrors. Time comes from Redis `TIME` inside scripts.
- Credentials only from env or `secrets_file`. Tenant configuration carries Key_Fingerprints (8 hex characters, not secrets), never key values.
- Manager_Agent and Worker_Agent keep the import-linter contract. The Manager sets a header through the `Transport` protocol; only the RabbitMQ adapter knows what it means.
- Single Redis primary stays (v2 Req 20.7). Tenant keys follow the existing `key_prefix` pattern and therefore, like the code today, would need a re-layout for Redis Cluster; that is Phase 7.

### Key decisions

| ID | Decision | Reason |
|---|---|---|
| D1 | Tenants and keys come from static configuration, resolved by fingerprint; one `KeyDirectory` interface hides the source | No key-management API or new secret path; stored keys can replace it later without touching handlers. Pending product-owner confirmation |
| D2 | The tenant is a field on the Request hash plus additive per-tenant keys (`das:tenant:{t}:…`); no key is renamed | No data migration, only a backfill; independent of Redis HA |
| D3 | Tenant keys are derived inside the Lua scripts from the request key and the stored `tenant` field | Same pattern as `das:client:{fp}:*`; no change to the `KEYS` lists built in Python |
| D4 | Cross-tenant access is 404 `not_found`, never 403 | No existence leak |
| D5 | One Route_Scope_Table drives authorization, and a test walks every registered route | A new route cannot ship unclassified |
| D6 | Usage is rolled up per tenant, UTC day and `{provider}/{model}` in `append_usage`; cost is computed when the report is read | Every UsageRecord is counted exactly once without a price in Lua; a price change re-prices history |
| D7 | Priority is the header `das-priority: high`, translated to AMQP priority 9; no payload or codec change; `x-max-priority` is never declared | G0a: quorum queues reject `x-max-priority` and give two weighted levels; baselines stay byte-identical |
| D8 | `tenant` is a label on two metrics only, and a Span attribute; no token or cost metric per tenant | Workers do not know the tenant; the Usage_Report is the source for chargeback |
| D9 | Migration is expand then contract with a `das:tenant:index:ready` flag and an idempotent backfill | Rolling upgrades with mixed versions stay correct |
| D10 | The Chart deploys no Redis or RabbitMQ and no definitions job; advertise host is the pod IP; grace period above `shutdown_timeout` | G0b findings |
| D11 | Kubernetes is verified by static checks on every push and a `kind` job on `main` | Probes, drain and gRPC addressing are only proven on a cluster. Pending confirmation |
| D12 | Redis HA is not part of Phase 5 | Sentinel keeps the layout but needs a durability review of leases and fencing; Cluster needs a state-layer rewrite. Pending confirmation of the uptime target |

### Deviations from the Phase 5 plan (what reading the code changed)

1. **Idempotency stays per key, not per tenant.** Scoping it to the tenant would make two legacy keys that share an `Idempotency-Key` collide, a Phase 4 behaviour change. Key rotation therefore starts new records (documented).
2. **The tenant-wide quota is additional to the per-key quota** and defaults to 0 (off), so a legacy installation is unaffected.
3. **Cost is computed at read time** from per-model token rollups, not precomputed in `finalize`. This removes the plan's open point about the Price_Table in Lua.
4. **Two labelled metrics, not four**, and no per-tenant token or cost metric.
5. **Priority needs no Job payload field**: a header does it, so the codec output cannot change.
6. **About 12 scripts change, not 19**: leases, `requests:pending`, `webhooks:due` and the list scripts are infrastructure or already key-parameterised.

## Architecture

### Auth path

```
client ──X-API-Key──▶ Gateway
  1. api_key_auth: KeyDirectory.resolve(key) → Principal(tenant, scopes, operator, fp)   → 401
  2. authorize_route: ROUTE_SCOPES[(method, route template)] vs Principal                  → 403 forbidden {scope}
  3. rate_limit (per key fingerprint, v4)                                                  → 429
  4. handler (tenant routes read or write only through the Principal's tenant)
```

### Submit path

```
POST /v1/requests (scope submit)
  resolve Client_Settings: key override > tenant settings > client_limits.default       (pure)
  validate prompt, Idempotency-Key, budget, priority (≤ tenant max_priority)            → 400
  create_request.lua (existing checks first: idempotency replay, per-key quota)
     tenant quota:  SCARD das:tenant:{t}:active ≥ max_active_requests  → TENANT_QUOTA   → 429 quota_exceeded {limit_scope: tenant}
     HSET tenant, [priority=high], …; tenant index (Pending); SADD tenant active; usage submitted
```

### Read paths

```
GET/cancel/events {id}: load the Request; tenant ≠ Principal.tenant and not operator → 404 not_found
GET /v1/requests:       list_index.lua over das:tenant:{t}:requests[:by_status:{s}]  (operator: global keys or ?tenant=)
```

### State-change path

`index_move` gains the request key, so every call site that moves a Request between index zsets also moves it between the tenant's twins. `client_active_add` and `client_active_remove` also maintain the Tenant_Active_Set, at exactly the call sites Phase 4 already has. `append_usage` bumps the Usage_Rollup. The Manager's work is unchanged except the priority header.

### Priority path

```
POST priority=high → Request hash priority=high → RequestSnapshot.priority
  → driver dispatch: transport.publish(dest, message, headers={"das-priority": "high"})   (every dispatch, retry, takeover, replay)
  → RabbitMQ adapter: pop the header, Message(priority=9); normal sends priority 0 (aio-pika's default)
```

### Package layout (new and changed)

```
das/
  tenancy/                    NEW (pure)
    scopes.py                 Scope, Principal, Requirement, ROUTE_SCOPES, authorize
    directory.py              KeyDirectory (Protocol), StaticKeyDirectory
    settings.py               layered resolve (key > tenant > default)
    usage.py                  day range, rollup merge, report assembly, cost at read time
  config/schema.py            TenantConfig, TenantKeyConfig, TenantSettings, UsageConfig; AgentConfig.tenants/.usage;
                              LabelLimitsConfig.tenant; cross-field tenant_errors
  client/settings.py          resolve_settings takes the tenant layer
  gateway/auth.py             Principal dependency (replaces fingerprint-only identity)
  gateway/authz.py            NEW  authorize_route dependency (reads ROUTE_SCOPES)
  gateway/usage.py            NEW  GET /v1/usage, GET /v1/admin/usage
  gateway/listing.py          tenant keys, tenant in Page_Cursor filters, operator `tenant` param
  gateway/admin.py            DLQ scoping, operator-only drain/undrain
  gateway/audit.py            tenant, operator fields
  gateway/app.py, submit.py   priority, tenant quota, tenant stored on create, 404 rule
  state/keys.py               tenant key functions
  state/lua/_common.lua       tenant helpers; index_move(req_key); active helpers; usage bump in append_usage
  state/lua/                  changed: create_request, commit_plan, finalize, cancel_request, reopen_request,
                              replay_dead_letter, purge_request, create_dead_letter, discard_dead_letter,
                              purge_dead_letter, apply_transition; NEW tenant_backfill.lua
  state/store.py              tenant and priority in ClientRequestSpec and the snapshot; tenant listing, usage read
  manager/retention.py        TenantIndexBackfill next to IndexBackfill
  manager/machine.py, driver.py   snapshot priority; dispatch header
  transport/base.py           PRIORITY_HEADER
  transport/rabbitmq.py       header → AMQP priority 9
  observability/metrics.py    tenant label on two metrics; new rejection reasons
  observability/tracing.py    das.tenant in the attribute allow-list
deploy/helm/das/              NEW  chart (templates, values, values.schema.json, ci/ values, README)
deploy/grafana/               dashboard panel: Requests per tenant
tests/k8s/                    NEW  Redis, RabbitMQ and stub-LLM manifests for the kind job
tests/integration/k8s/        NEW  kind tests
.github/workflows/ci.yml      chart steps in the fast job; Kind_Job
```

## Components and Interfaces

### 1. Configuration (Req 1, 4, 6, 8)

```yaml
tenants:
  - id: team-a                      # Tenant_ID, unique, not "default"
    keys:
      - {fingerprint: "1a2b3c4d", scopes: [submit, read]}
      - {fingerprint: "5e6f7a8b", scopes: [submit, read, admin]}
    settings:                       # layer between client_limits.default and a key override
      rate: 120
      burst: 240
      default_budget: {max_total_tokens: 200000}
    max_active_requests: 200        # Tenant_Quota, 0 = none
    default_priority: normal        # normal | high
    max_priority: high              # default normal (high for the Default_Tenant)
usage:
  retention_days: 400               # 30 to 3650
metrics:
  label_limits:
    tenant: 100                     # 1 to 500 (next to capability, queue, provider, model)
```

`TenantSettings` has the optional fields of `ClientOverride` without `fingerprint` (`rate`, `burst`, `max_active_requests`, `max_streams`, `default_budget`, `webhook_url`, `webhook_secret_name`) with the same ranges. A new cross-field validator `tenant_errors(config)` in the module of `client_api_errors` returns `FieldError`s (path only, never a key or secret value) for every rule of Requirement 1.4. A fingerprint is checked against the fingerprints of `api_keys` and `admin_api_keys` computed with `das.ops.keys.key_fingerprint`, as `client_api_errors` does for overrides.

### 2. Pure core `das/tenancy` (Req 1, 2, 4)

```python
# scopes.py  (Req 2)
class Scope(StrEnum): SUBMIT="submit"; READ="read"; ADMIN="admin"
OPERATOR: Final = "operator"
Requirement = Scope | Literal["operator"] | Literal["public"]

@dataclass(frozen=True)
class Principal:
    tenant: str
    scopes: frozenset[Scope]
    operator: bool
    fingerprint: str

ROUTE_SCOPES: Mapping[tuple[str, str], Requirement]   # (METHOD, route template) -> requirement
def authorize(principal: Principal, requirement: Requirement) -> bool
#   public -> True; operator -> principal.operator; Scope -> operator or scope in principal.scopes

# directory.py  (Req 1, 12.3)
class KeyDirectory(Protocol):
    def resolve(self, key: str | None) -> Principal | None: ...

class StaticKeyDirectory:
    def __init__(self, api_keys, admin_keys, tenants: Sequence[TenantConfig]) -> None
    # uses das.ops.keys.classify_key (constant time over every configured key), then maps the matched
    # key's fingerprint to its Tenant_Config; admin keys -> Operator in "default" with all Scopes;
    # unclaimed api keys -> "default" with {submit, read}

# settings.py  (Req 4)
def resolve_settings(fingerprint, limits, tenant: TenantConfig | None) -> ClientSettings
#   field by field: key override > tenant.settings > limits.default
```

`ROUTE_SCOPES` is built by listing the routes explicitly (not by decorators), so the table is the one place to read what each route requires. `/healthz`, `/livez`, `/readyz` and `/metrics` are `public` (they are not `/v1` routes).

| Route | Requirement |
|---|---|
| `POST /v1/requests`, `POST /v1/requests/{request_id}/cancel` | `submit` |
| `GET /v1/requests`, `GET /v1/requests/{request_id}`, `GET /v1/requests/{request_id}/events`, `GET /v1/agents`, `GET /v1/usage`, `GET /v1/openapi.json` | `read` |
| `GET /v1/admin/dlq`, `GET /v1/admin/dlq/{dead_letter_id}`, `POST …/{dead_letter_id}/replay`, `POST …/{dead_letter_id}/discard` | `admin` (own tenant; records of no tenant and other tenants are not visible) |
| `POST /v1/admin/agents/{agent_id}/drain`, `…/undrain`, `GET /v1/admin/usage` | `operator` |

The templates are the application's `path_format` strings, as registered (`{request_id}`, `{dead_letter_id}`, `{agent_id}`); `das/tenancy/scopes.py` holds the table and `tests/unit/test_tenancy_scopes.py` pins it.

### 3. State_Store keys and scripts (Req 3, 5, 6, 9)

New keys (all additive; `t` is a Tenant_ID, which cannot contain `:`):

| Key | Type | Content |
|---|---|---|
| `das:req:{rid}` field `tenant` | hash field | Tenant_ID; absent = `default` |
| `das:req:{rid}` field `priority` | hash field | `high` only; absent = `normal` |
| `das:tenant:{t}:requests` | zset | rid scored by created_at ms (twin of `das:requests:index`) |
| `das:tenant:{t}:requests:by_status:{s}` | zset | twin of `das:requests:by_status:{s}` |
| `das:tenant:{t}:active` | set | non-terminal rid of the tenant (Tenant_Quota) |
| `das:tenant:{t}:dlq:index`, `:dlq:by_status:{s}`, `:dlq:by_reason:{r}` | zsets | twins of the global DLQ indexes |
| `das:dlq:{dlid}` field `tenant` | hash field | set at creation when the Request exists; absent = no tenant |
| `das:tenant:{t}:usage:{yyyymmdd}` | hash | `submitted`, `finished/{Completed,Failed,Cancelled}`, `tokens/{in,out,total}/{provider}/{model}`; TTL `usage.retention_days` |
| `das:tenant:index:ready` | string | `1` once the Tenant_Backfill completed |
| `das:tenant:index:backfill_lock` | string | `SET NX PX` lock of the Tenant_Backfill |

Helpers added to `_common.lua` (names indicative):

```lua
local function tenant_of(req_key)                       -- HGET tenant, default 'default'
local function tenant_twin(global_key, t)               -- the twin of a global index key, nil for keys without one
                                                        --   das:requests:index -> das:tenant:{t}:requests
                                                        --   das:requests:by_status:S -> das:tenant:{t}:requests:by_status:S
                                                        --   das:dlq:index|by_status:S|by_reason:R -> das:tenant:{t}:dlq:…
local function index_move(rid, from_key, to_key, score, req_key)   -- existing moves, plus the same moves on the twins when req_key is given
local function client_active_add(req_key, rid)          -- existing, plus SADD das:tenant:{t}:active (for every Request)
local function client_active_remove(req_key, rid)       -- existing, plus SREM
local function utc_day(ms)                              -- 'yyyymmdd' from epoch ms (reuses iso_ms)
local function usage_bump(req_key, field, n, retention_days)  -- HINCRBY das:tenant:{t}:usage:{utc_day}; EXPIRE retention_days (400 if absent)
-- append_usage(usage_key, req_key, records_json): for each record also usage_bump 'tokens/in|out|total/{provider}/{model}'
-- usage.retention_days reaches the scripts through the key das:tenant:usage_retention_days, which the Gateway and Manager
-- write at startup (StateStore.set_usage_retention) and usage_bump reads (400 if absent); an ARGV tail clashes with the
-- variable-length tails of commit_plan and cancel_request.
```

Where each script changes:

| Script | Change |
|---|---|
| `create_request` | store `tenant`, `priority`; tenant quota check (before any write); twin index adds; `SADD` tenant active; `usage_bump submitted` |
| `commit_plan`, `reopen_request` | `index_move` with `req_key` (twins follow) |
| `finalize`, `cancel_request` | `index_move` with `req_key`; tenant active remove (through `client_active_remove`); `usage_bump finished/{status}` |
| `replay_dead_letter` | `index_move`, `client_active_add`; DLQ twin moves |
| `purge_request` | ZREM from the twins (index and the status zset, `tenant_of` read before the hash is deleted); tenant active remove |
| `create_dead_letter` | when the Request hash exists, store `tenant` on the record and add to the DLQ twins |
| `apply_transition`, `discard_dead_letter`, `purge_dead_letter` | DLQ twin status moves and removal, using the record's `tenant` field |
| `tenant_backfill` (new) | see below |

Unchanged: lease scripts, `requests:pending`, `webhooks:due`, `list_index.lua` and `list_dead_letters.lua` (their source zsets arrive as `KEYS`; the store passes the tenant twins), webhook scripts, the stream scripts.

**Tenant_Backfill (`tenant_backfill.lua`, batched).** Input: a cursor over `das:requests:index` and a batch size. For each rid with no tenant twin entry: `ZADD das:tenant:default:requests` and the status zset (from the Request's current status), `SADD` the tenant active set when non-terminal. For DLQ records with an existing Request and no `tenant` field: set `tenant=default` and add to the default twins. Idempotent (`ZADD`, `SADD`, `HSETNX`), so concurrent writers and repeated runs converge; for the same reason it also `ZREM`s the Request from the other four status twins and `SREM`s a terminal Request from the active set, which removes what a Phase 4 writer left stale during a mixed-version upgrade. It scans the global indexes with `ZSCAN` and starts only after the Phase 2 Request_Index backfill has set its own ready flag. The `TenantIndexBackfill` Manager task (next to `IndexBackfill` in `das/manager/retention.py`) holds `das:tenant:index:backfill_lock` with `SET NX PX`, loops batches, then `SET das:tenant:index:ready 1`.

Until the flag is set, `list_requests` for the Default_Tenant reads the global index and for any other tenant raises `IndexNotReady` (503 `index_not_ready`, the same error as v2 Req 15).

### 4. Gateway (Req 2, 3, 5, 6, 7)

- `api_key_auth` builds the Principal with the `KeyDirectory`, stores it on `request.state.principal`, and still stores `request.state.client_settings` (now resolved with the tenant layer) for the rate limit. A new router-level dependency `authorize_route` runs next: it looks up `(method, request.scope["route"].path_format)` in `ROUTE_SCOPES`; a missing entry **fails closed** (403) and a unit test makes a missing entry a test failure.
- Admin router: the DLQ routes accept `admin` or Operator; drain, undrain and admin usage require Operator. A caller that lacks the Scope gets 403 `forbidden` with `scope`. For a Phase 4 configuration (no `admin_api_keys` and no tenant granting `admin`) the answer stays 403 `admin_disabled`, as today.
- Tenant routes compare the Request's `tenant` (loaded with the snapshot) to the Principal's, except for Operators, and answer 404 `not_found` otherwise, before starting a stream or writing. The tenant field never changes after creation, so there is no check-then-act race.
- `listing.py`: `parse_list_query` accepts `tenant` only for Operators. The effective tenant (or `*` for an Operator listing everything) is part of the Page_Cursor `filters`, so a cursor cannot cross tenants.
- `GET /v1/usage` and `GET /v1/admin/usage` (`das/gateway/usage.py`): read the Usage_Rollup hashes for the days in range (`HGETALL` per day, at most 92), merge them with the pure `das.tenancy.usage.build_report`, and price them with the Gateway's Price_Table.

```json
{ "tenant": "team-a", "from": "2026-11-01", "to": "2026-11-07",
  "days": [ { "date": "2026-11-01",
              "requests": { "submitted": 12, "finished": { "Completed": 10, "Failed": 1, "Cancelled": 1 } },
              "models": [ { "provider": "openai", "model": "gpt-4o", "input_tokens": 1200, "output_tokens": 800,
                            "total_tokens": 2000, "estimated_cost": "0.0123" } ] } ],
  "totals": { "requests": { "submitted": 12, "finished": { "Completed": 10, "Failed": 1, "Cancelled": 1 } },
              "input_tokens": 1200, "output_tokens": 800, "total_tokens": 2000,
              "estimated_cost": { "currency": "USD", "amount": "0.0123", "complete": true } } }
```

Every day of the range has an entry, with zero counters and no models when nothing happened. A model's `estimated_cost` is `null` when there is no Price_Table or the model is unpriced (v2 Req 10.7). The total is `null` without a Price_Table; with one it is `{currency, amount, complete}`, where `amount` covers the priced models and `complete` is false when any model is unpriced. Cost is computed from the per-model token aggregates with `das.ops.cost` (linear in tokens, so an aggregate prices exactly as its records would). The admin report without `tenant` returns `{"tenants": [report, …]}`.

- `submit.py`: `priority` validation (`invalid_request` naming `priority`, `priority_above_limit` with the limit), tenant and priority passed in `ClientRequestSpec`, tenant quota result mapped to 429 `quota_exceeded` with `limit_scope: "tenant"` (the per-key rejection body is unchanged from Phase 4 and has no `limit_scope`).
- `GET /v1/agents`: for a non-Operator Principal outside the Default_Tenant, entries drop `agent_id` and `host`.

### 5. Manager (Req 7)

`RequestSnapshot` gains `priority` (read by `load_snapshot` with the other Phase 4 fields). In `das/manager/driver.py` the Job dispatch call becomes `transport.publish(effect.destination, message, headers)` where `headers` is `{PRIORITY_HEADER: "high"}` for a `high` Request and empty otherwise. The reducer is not touched: priority never changes a decision.

### 6. Transport (Req 7)

`das/transport/base.py` defines `PRIORITY_HEADER = "das-priority"`. In `RabbitMQTransport.publish`, a header value `high` is popped from the AMQP headers and becomes `aio_pika.Message(priority=9)`; any other case sends no `priority` property. Queue declarations, the `das-jobs` policy and the delivery limit are untouched, and `x-max-priority` is never declared (a quorum queue rejects it).

### 7. Observability (Req 8)

`DYNAMIC_LABELS` gains `tenant` (limit from `LabelLimitsConfig.tenant`). `das_requests_submitted_total` and `das_requests_finished_total` get the label; the Gateway records it at submit from the Principal and the Manager at finish from the snapshot's tenant. `das_client_rejections_total` gains `forbidden`, `tenant_quota_exceeded` and `priority_above_limit`. The Tracer's allow-list gains `das.tenant`, set on `gateway.submit` and `manager.request`. The audit entry gains `tenant` and `operator`. The Grafana dashboard JSON gains one panel, and its static test is extended.

### 8. Helm chart (Req 10)

```
deploy/helm/das/
  Chart.yaml  values.yaml  values.schema.json  README.md
  ci/values-min.yaml  ci/values-all.yaml
  templates/_helpers.tpl  serviceaccount.yaml  configmap.yaml  NOTES.txt
            gateway.yaml (Deployment + Service)  manager.yaml  workers.yaml (range .Values.workers)
            pdb.yaml  keda.yaml
```

Values (shape):

```yaml
image: {repository: das, tag: ""}           # one image per role: <repository>-gateway|-manager|-worker
existingSecret: das-secrets                 # required
secretEnv:                                  # env name -> key in the Secret; the chart never sees a value
  gateway: {DAS__API_KEYS: API_KEYS, DAS__ADMIN_API_KEYS: ADMIN_API_KEYS, DAS__REDIS__URL: REDIS_URL}
  manager: {DAS__REDIS__URL: REDIS_URL, DAS__RABBITMQ__URL: RABBITMQ_URL, DAS__LLM__API_KEY: LLM_API_KEY}
  worker:  {DAS__REDIS__URL: REDIS_URL, DAS__RABBITMQ__URL: RABBITMQ_URL, DAS__LLM__API_KEY: LLM_API_KEY}
rabbitmq: {managementUrl: "http://rabbitmq:15672"}
config: {common: {...}, gateway: {...}, manager: {...}, worker: {...}}   # rendered into ConfigMaps (no secrets)
gateway: {replicas: 2, resources: {}}
manager: {replicas: 2, resources: {}}
workers: [{name: research, capabilities: [research, summarize], replicas: 2, resources: {}}]
terminationGracePeriodSeconds: 45
lifecycle: {preStopDrain: {enabled: false}}
keda: {enabled: false, queueLength: 10, pollingInterval: 15}
```

Every pod sets `DAS__GRPC__ADVERTISE_HOST` from `status.podIP`, `DAS__AGENT_ID` from `metadata.name`, probes on the health port, `terminationGracePeriodSeconds`, and a pod anti-affinity preference. A `fail` in `_helpers.tpl` aborts rendering when the grace period is below `limits.shutdown_timeout + 10`. `values.schema.json` rejects unknown keys. Tenant configuration lives in `config.gateway.tenants`; its key fingerprints are not secrets.

### 9. CI (Req 11)

- Fast job: pinned `helm` and `kubeconform` (SHA-256 checked) installed, then `helm lint` and `helm template` for `ci/values-min.yaml` and `ci/values-all.yaml`, `kubeconform -strict` with a pinned Kubernetes version, and the chart unit tests (`tests/unit/test_chart_*.py`, which render with the `helm` binary, skip when it is missing locally, and run in CI).
- `Kind_Job` (`k8s`, `main` pushes and manual dispatch, `DAS_REQUIRE_DOCKER=1`, timeout 30 min): pinned `kind` and `kubectl`, build the three images, `kind create cluster`, load images (with the `docker save --platform` fallback documented in the plan), apply `tests/k8s/`, `helm install`, `pytest tests/integration/k8s`, then collect pod logs on failure as an artifact.
- `EXPECTED_TIMEOUTS` and the expected fast steps in `tests/unit/test_ci_workflow_static.py` are updated with the new job and steps.

## Data Models

Principal, Scope and the Usage_Report are above. The Request view gains `priority`. Errors:

| Status | Body | When |
|---|---|---|
| 403 | `{"error":"forbidden","scope":"submit"}` | Principal lacks the Scope or `operator` |
| 403 | `{"error":"admin_disabled"}` | Phase 4 configuration with no admin capability anywhere (unchanged) |
| 404 | `{"error":"not_found"}` | Request, record or stream of another tenant (same body as a missing one) |
| 429 | `{"error":"quota_exceeded","limit":N,"limit_scope":"tenant"}` | Tenant_Quota reached |
| 400 | `{"error":"priority_above_limit","limit":"normal"}` | `priority` above `max_priority` |
| 400 | `{"error":"invalid_request","parameter":"priority"}` | bad `priority` |
| 400 | `{"error":"invalid_query","parameter":"tenant"}` | `tenant` parameter from a non-Operator; bad usage range |
| 503 | `{"error":"index_not_ready"}` | non-default tenant listing before the backfill finished |

## Correctness Properties

Each property maps to exactly one property-test sub-task. Tests carry the tag `# Feature: das-multi-tenant, Property {n}: {title}` and run at least 100 examples. Properties 2, 3 and 7 run the real Lua scripts on `fakeredis[lua]` with the existing `FakeRedisTime` fixture.

### Property 1: Tenant isolation
*For any* sequence of submit, get, cancel, list (with paging), events, Dead_Letter replay and discard, and usage calls by Principals of two or three tenants and an Operator, no Principal of tenant `t` observes a Request_ID, field, count, cursor position or Dead_Letter_Record of another tenant. A cursor issued to one tenant is rejected for another. Every cross-tenant access answers 404 with the body of a missing resource. This is a stateful Hypothesis test over the in-process Gateway on fakeredis.
**Validates: Requirements 3.2, 3.3, 3.4, 3.6, 5.3**

### Property 2: Tenant_Index mirrors the global index
*For any* sequence of create, plan commit, finalize, cancel, reopen, replay and purge over several tenants, after every operation, for each tenant `t` and status `s`, the tenant's `by_status:{s}` zset equals the members of the global `by_status:{s}` whose Request belongs to `t`, `das:tenant:{t}:requests` equals the members of `das:requests:index` belonging to `t`, and `das:tenant:{t}:active` equals the tenant's non-terminal Requests. Requests without `tenant` are in the `default` tenant's sets. Operations rejected with `LEASE_LOST` or `REQUEST_TERMINAL` change no tenant key.
**Validates: Requirements 3.1, 3.5, 3.9**

### Property 3: Usage rollup is exact
*For any* sequence of operations that append UsageRecords (plan, Job results, summarize, Replay), per tenant, UTC day and `{provider}/{model}`, the rollup's `tokens/in`, `tokens/out` and `tokens/total` equal the sums over the records appended that day, `submitted` equals the Requests created that day, and `finished/{status}` equals the terminal writes that day. A duplicate result discarded by the reducer adds nothing.
**Validates: Requirements 6.1, 6.2**

### Property 4: Every route is classified, and authorization is exact
The set of `(method, route template)` pairs registered on the Gateway application (with the admin router) equals the keys of `ROUTE_SCOPES` plus the public health routes. *For any* Principal (tenant, any subset of Scopes, operator flag) and any entry, `authorize` is true if and only if the Principal holds the required Scope (an Operator holds all and `operator`), and the Gateway answers 403 before any State_Store access beyond authentication exactly when it is false.
**Validates: Requirements 2.2, 2.3, 2.4, 2.5**

### Property 5: Principal resolution
*For any* valid configuration and presented key, `StaticKeyDirectory.resolve` returns exactly one Principal or none: an `admin_api_keys` key is an Operator in `default` with all Scopes; an `api_keys` key listed by a tenant gets that tenant and those Scopes; any other `api_keys` key is `default` with `{submit, read}`; an unknown key gives none. With no `tenants`, every `api_keys` key is a `default` key. Configurations with a fingerprint matching no key, listed twice, listed for an admin key, or shared by two keys are rejected by `tenant_errors`, and the error never contains a key value.
**Validates: Requirements 1.2, 1.3, 1.4, 1.5, 1.7**

### Property 6: Settings layering
*For any* platform default, tenant settings and key override, each resolved field equals the value of the most specific layer that sets it. `resolve_budget` applied to the resolved default keeps v4 Property 4 (it never returns a limit above the default).
**Validates: Requirements 4.1, 4.4, 4.5**

### Property 7: Tenant_Backfill is idempotent and complete
*For any* Phase 4 data set (Requests with and without `client_fp`, in any status, and Dead_Letter_Records with and without an existing Request) and any interleaving of the backfill batches with writers, once the backfill reports ready the default tenant's twins equal what the Phase 5 scripts would have written, running the backfill again changes nothing, and records without a Request stay out of every tenant's DLQ index.
**Validates: Requirements 5.1, 5.2, 5.5, 9.1, 9.2**

### Property 8: Priority header
*For any* Request priority and any sequence of dispatches, retries, takeovers and Replays, every Job dispatch carries the Priority_Header if and only if the Request's priority is `high`, and the dispatch payload is identical to the one produced without priority. The RabbitMQ adapter maps `high` to AMQP priority 9 and everything else to no priority (adapter test with a fake channel).
**Validates: Requirements 7.3, 7.4, 7.5**

## Error Handling

| Condition | Response | Notes |
|---|---|---|
| Unknown or missing key | 401 `unauthorized` | as today |
| Scope or role missing | 403 `forbidden` `{scope}` | before any State_Store access and before the Rate_Limit |
| Route missing from `ROUTE_SCOPES` | 403 `forbidden` | fails closed; the completeness test fails the build |
| Request, record or stream of another tenant | 404 `not_found` | identical to a missing one |
| `tenant` parameter from a non-Operator | 400 `invalid_query` `tenant` | |
| Cursor from another tenant | 400 `invalid_query` `cursor` | the tenant is part of the cursor filters |
| Tenant_Quota reached | 429 `quota_exceeded` `limit_scope: tenant` | after the per-key checks, before any write |
| `priority` invalid / above the tenant limit | 400 `invalid_request` / `priority_above_limit` | |
| Usage range malformed, inverted or longer than 92 days | 400 `invalid_query` | names `from` (malformed or inverted) or `to` (malformed or too long) |
| Non-default tenant listing before the backfill | 503 `index_not_ready` | |
| Redis unreachable | 503 as today | fail closed |
| Chart grace period below `shutdown_timeout + 10` | `helm template` fails with a message | |
| Chart `values.schema.json` violation | `helm lint` fails | |

## Testing Strategy

- **Property tests** (`tests/property/`): Properties 1 to 8, in the sub-tasks named in `tasks.md`.
- **Unit tests**: tenant config validation (every rule of Requirement 1.4, no key value in any message); the route table completeness walk; scope enforcement per route; every error row above; layered settings; usage report assembly and cost at read time with and without a Price_Table; Lua helper behaviour per script; the audit entry; metric labels and the tenant limit; the Tracer allow-list; the Grafana dashboard JSON; `PRIORITY_HEADER` mapping in the adapter; chart rendering (probes, advertise host, grace period and its refusal, no secret in any ConfigMap with sentinel secret values, optional features on and off); the rewritten Compose-only test.
- **Contract tests** (`tests/contract/`): the existing client against a Gateway configured with tenants, covering a Default_Tenant key unchanged and a tenant key; `das-client` itself is unchanged in Phase 5.
- **Integration**: the priority ordering and 2:1 weighting against a real RabbitMQ container (testcontainers), asserting delivery order from `basic.get`, requeue and dead-lettering with priority preserved.
- **Compose e2e** (`tests/integration/e2e/test_tenancy_e2e.py`, own project `das-e2e-tenant`, own override and Gateway config as in Phase 4): two tenants and an Operator against two Gateways: isolation across every route, scopes, the tenant quota, usage report totals equal to the stub LLM's known token counts, a `high` Request overtaking a queued `normal` backlog, and a mixed-version style upgrade (data written without tenant fields, backfilled, then read).
- **Kind e2e** (`tests/integration/k8s/`): Requirement 11.3.
- **Regression**: Phase 1 to 4 suites, `lint-imports`, ruff, mypy, with the deliberate changes of Requirement 9.6 only.
- **Fault injection and negative controls**, as in Phase 4: each new e2e and integration test is run against a deliberately broken variant (a tenant filter removed, a script that skips the twin update, a wrong scope, priority dropped) and must fail.
