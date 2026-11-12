# Requirements Document

## Introduction

This is Phase 5 "Multi-tenant and deployable" of the Distributed Agent System (DAS). Phase 1 (`.kiro/specs/distributed-agent-system/`), Phase 2 (`.kiro/specs/das-observability-operations/`), Phase 3 (`.kiro/specs/das-hardening-ci/`) and Phase 4 (`.kiro/specs/das-client-api/`) are implemented. Phase 5 lets one DAS installation serve several teams, and run on Kubernetes as well as Docker Compose. It adds:

1. Tenants: API keys belong to a tenant; Requests, listings, Dead_Letter_Records and usage are scoped to it.
2. Scopes (`submit`, `read`, `admin`) and an `operator` role, replacing the flat user/admin split.
3. Per-tenant settings, a tenant-wide active-Request quota, and durable per-tenant usage reports.
4. A Request priority, `normal` or `high`.
5. A Helm chart and its verification, including a `kind` end-to-end job.

Reference convention: "v1 Req X.Y" is Requirement X, criterion Y in the Phase 1 spec; "v2 Req X.Y" in Phase 2; "v3 Req X.Y" in Phase 3; "v4 Req X.Y" in Phase 4. Every earlier term keeps its glossary meaning.

Design rules kept from earlier phases: pure core + thin I/O shell; Manager_Agent and Worker_Agent reach the network only through Transport and Message_Codec; inject Clock; credentials never in YAML or logs; config precedence env (`DAS__`) > file > default; every new piece of state is written by a Lua script, in the same script as the state change it belongs to.

Intended semantic changes from earlier phases:

- A tenant sees only its own Requests (Requirement 3). This replaces v2 Req 20.1 for installations that configure tenants. An installation that configures none behaves as before: every key belongs to the Default_Tenant.
- A tenant name may appear in two metric labels, a span attribute, the audit log and per-tenant indexes (Requirement 8). This amends v2 Req 20.3. Key values and Key_Fingerprints stay out of metrics, spans and indexes.
- The repository may contain a Helm chart and Kubernetes manifests (Requirements 10 and 11). This replaces v2 Req 20.6.
- v2 Req 20.7 is **unchanged**: one Redis instance, no replication, Sentinel or Cluster. Redis HA is a later phase.

Assumptions pending product-owner confirmation (they follow the recommendations of the Phase 5 plan, `docs/superpowers/plans/2026-11-12-das-phase5-multi-tenant-deployable.md`, Decisions 1, 3 and 8): tenants and keys come from static configuration (no key-management API); Redis HA is out of scope; Kubernetes is verified by static checks on every push plus a `kind` end-to-end job on `main`. If any of these changes, Requirements 1, 12 and 11 respectively are the ones to revisit.

Places where property-based testing applies are marked by criteria of the form "FOR ALL".

## Glossary

- **Tenant**: A named group of API keys whose Requests, Dead_Letter_Records and usage are isolated from other tenants.
- **Tenant_ID**: A string matching `^[a-z0-9][a-z0-9-]{0,31}$`. `default` is reserved for the Default_Tenant.
- **Default_Tenant**: The implicit tenant `default`. It owns every Request that has no `tenant` field and every API key that no tenant entry claims.
- **Tenant_Config**: One entry of the `tenants` list of Agent_Config.
- **Scope**: One of `submit`, `read`, `admin`. A key holds a set of Scopes within its tenant.
- **Operator**: A Principal that is not bound to one tenant. The keys in `admin_api_keys` are Operators.
- **Principal**: The result of authenticating a call: Tenant_ID, Scopes, `operator` flag and Key_Fingerprint.
- **Route_Scope_Table**: The table that names, for every Gateway_API route, the Scope or role it requires.
- **Tenant_Index**: The per-tenant Redis zsets `das:tenant:{t}:requests` and `das:tenant:{t}:requests:by_status:{s}`, mirroring the global Request_Index for the Requests of tenant `t`.
- **Tenant_DLQ_Index**: The per-tenant zsets `das:tenant:{t}:dlq:index`, `…:dlq:by_status:{s}` and `…:dlq:by_reason:{r}`, mirroring the global DLQ indexes.
- **Tenant_Active_Set**: Redis set `das:tenant:{t}:active` of the non-terminal Request_IDs of tenant `t`.
- **Tenant_Quota**: Maximum size of the Tenant_Active_Set (`max_active_requests` of the Tenant_Config; 0 = no limit).
- **Usage_Rollup**: Redis hash `das:tenant:{t}:usage:{yyyymmdd}` (UTC day from Redis `TIME`) holding request counters and token totals per `{provider}/{model}`.
- **Usage_Report**: The response of `GET /v1/usage` (own tenant) or `GET /v1/admin/usage` (Operator).
- **Priority_Class**: `normal` or `high`. The Request field is `priority`.
- **Priority_Header**: The reserved Message header `das-priority` with value `high`, set by the Manager_Agent on Job dispatch and translated by the RabbitMQ adapter.
- **Tenant_Backfill**: The Manager_Agent task that fills the Tenant_Index and Tenant_DLQ_Index from the global indexes, then sets `das:tenant:index:ready`.
- **Chart**: The Helm chart in `deploy/helm/das/`.
- **Kind_Job**: The CI job that installs the Chart on a `kind` cluster and runs `tests/integration/k8s/`.

## Requirements

### Requirement 1: Tenants and Key Mapping

**User Story:** As an operator, I want to group API keys into tenants in configuration, so that teams are isolated without a new secret channel.

#### Acceptance Criteria

1. THE Agent_Config SHALL accept a `tenants` list (at most 100 entries). Each Tenant_Config SHALL have `id` (Tenant_ID, unique, not `default`), `keys` (1 to 50 entries, each with a required `fingerprint` of 8 lowercase hex characters and `scopes`, a non-empty subset of `submit`, `read`, `admin`), and optional `settings`, `max_active_requests`, `default_priority` and `max_priority` (Requirements 4 and 7).
2. WHEN a presented key matches an entry of `api_keys`, THE Gateway_API SHALL derive its Key_Fingerprint and resolve its Principal: the Tenant_Config whose `keys` list that fingerprint, with that entry's Scopes; or, when no Tenant_Config lists it, the Default_Tenant with Scopes `submit` and `read`.
3. WHEN a presented key matches an entry of `admin_api_keys`, THE Gateway_API SHALL resolve an Operator Principal in the Default_Tenant with Scopes `submit`, `read` and `admin`.
4. THE Agent_Config validation SHALL reject, with the config exit code 2 and an error naming the field path but never a key or secret value: a tenant fingerprint that matches no configured key in `api_keys`; a fingerprint listed twice in one tenant or in two tenants; a tenant fingerprint that matches a key in `admin_api_keys`; two configured keys (in `api_keys` or `admin_api_keys`) with the same Key_Fingerprint; a duplicate or invalid `id`; an invalid Scope; and a `webhook_secret_name` that is not a configured webhook secret.
5. WHEN the `tenants` list is absent or empty, THE Gateway_API SHALL behave as in Phase 4: every key in `api_keys` is a Default_Tenant key with Scopes `submit` and `read`.
6. THE `client_limits.overrides` entries of Phase 4 SHALL keep working: an override applies to the key with that fingerprint whatever its tenant.
7. THE Principal resolution SHALL be a pure function of the configuration and the presented key. FOR ALL valid configurations and presented keys, it SHALL return exactly one Principal or none, and SHALL give the same result for the same inputs.

### Requirement 2: Scopes and Authorization

**User Story:** As an operator, I want keys with least privilege, so that a key that only reads cannot submit, and no tenant key can administer the platform.

#### Acceptance Criteria

1. THE Gateway_API SHALL authenticate every call to a `/v1` route once, producing a Principal; an unknown or missing key SHALL be rejected with 401 `unauthorized` as today (v1 Req 1.3).
2. THE Route_Scope_Table SHALL require: `submit` for `POST /v1/requests` and `POST /v1/requests/{id}/cancel`; `read` for `GET /v1/requests`, `GET /v1/requests/{id}`, `GET /v1/requests/{id}/events`, `GET /v1/agents`, `GET /v1/usage` and `GET /v1/openapi.json`; `admin` for the DLQ routes of the caller's own tenant (Requirement 5); and the Operator role for agent drain and undrain, the DLQ routes outside any tenant, and `GET /v1/admin/usage`.
3. WHEN a Principal lacks the Scope or role a route requires, THE Gateway_API SHALL reject the call with 403 `forbidden` and a `scope` field naming the required Scope or `operator`, before any State_Store access other than authentication and before the Rate_Limit is applied.
4. FOR ALL routes registered on the Gateway_API application, THE Route_Scope_Table SHALL contain an entry. A route without an entry SHALL fail the test suite.
5. FOR ALL Principals and routes, a call SHALL pass the authorization check if and only if the Principal holds the required Scope (an Operator holds every Scope and the `operator` role).
6. THE `GET /v1/agents` response for a Principal of a tenant other than the Default_Tenant who is not an Operator SHALL omit `agent_id` and `host`. For the Default_Tenant and Operators it SHALL be unchanged.
7. THE Audit_Entry (v2 Req 11.8) SHALL gain `tenant` and `operator` fields. Key values SHALL never be logged; the Key_Fingerprint stays in the Audit_Entry as in Phase 2.

### Requirement 3: Tenant-Scoped Requests

**User Story:** As a team, I want to see and cancel only our own Requests, so that other teams' prompts and results are never exposed to us.

#### Acceptance Criteria

1. WHEN the Gateway_API creates a Request, THE State_Store SHALL store the Principal's Tenant_ID in the field `tenant` of the Request hash. A Request without a `tenant` field SHALL belong to the Default_Tenant.
2. WHEN a Principal that is not an Operator calls `GET /v1/requests/{id}`, `POST /v1/requests/{id}/cancel` or `GET /v1/requests/{id}/events` for a Request of another tenant, THE Gateway_API SHALL answer 404 `not_found` with the same body as for a Request that does not exist, and SHALL NOT start a stream or write any state.
3. THE `GET /v1/requests` endpoint SHALL list only the Requests of the Principal's tenant, read from the Tenant_Index, with the same order, filters, limits and errors as v2 Req 15, and a Page_Cursor bound to the tenant (a cursor issued to one tenant SHALL be rejected for another with 400 `invalid_query` naming `cursor`).
4. THE `GET /v1/requests` endpoint SHALL accept the query parameter `tenant` only from an Operator, who then lists that tenant's Requests, or all Requests when the parameter is absent. Any other Principal sending it SHALL get 400 `invalid_query` naming `tenant`.
5. THE scripts that create, plan, finalize, cancel, reopen, replay and purge Requests SHALL maintain the Tenant_Index and the Tenant_Active_Set in the same call that maintains the global indexes. FOR ALL sequences of these operations over several tenants, after every operation, for each tenant `t` and status `s`, `das:tenant:{t}:requests:by_status:{s}` SHALL equal the members of `das:requests:by_status:{s}` whose Request belongs to `t`, and `das:tenant:{t}:requests` SHALL equal the members of `das:requests:index` whose Request belongs to `t`.
6. FOR ALL sequences of operations by several tenants and an Operator (submit, get, cancel, list with paging, Dead_Letter replay and discard, usage), no Principal of tenant `t` SHALL observe a Request_ID, a Request field, a cursor position, a count or a Dead_Letter_Record of another tenant.
7. WHEN the Tenant_Quota of a tenant is reached, THE Gateway_API SHALL reject a submit with 429 `quota_exceeded` and `limit_scope: "tenant"`, creating nothing. The per-key quota of v4 Req 3 SHALL stay in force, and both SHALL apply.
8. THE Idempotency_Record of v4 Req 4 SHALL stay per Key_Fingerprint. A key rotation therefore starts new idempotency records; the README SHALL say so.
9. THE retention task SHALL remove a purged Request from the Tenant_Index and the Tenant_Active_Set.

### Requirement 4: Settings Layering and Tenant Defaults

**User Story:** As an operator, I want to give a team defaults for its keys, with one key still able to differ, so that I configure a team once.

#### Acceptance Criteria

1. THE Gateway_API SHALL resolve the Client_Settings of a key from three layers, each field taken from the most specific layer that sets it: the key's `client_limits.overrides` entry, then the Tenant_Config's `settings`, then `client_limits.default`.
2. THE Tenant_Config `settings` SHALL accept the Client_Settings fields of v4 Req 1.2 (`rate`, `burst`, `max_active_requests`, `max_streams`, `default_budget`, `webhook_url`, `webhook_secret_name`) with the same ranges and validation.
3. THE `max_active_requests` of the Tenant_Config itself (range 0 to 1000000, default 0) SHALL be the Tenant_Quota of Requirement 3.7, and SHALL be distinct from the per-key `max_active_requests` inside `settings`.
4. WHEN the Effective_Budget is resolved (v4 Req 5.2 to 5.4), THE `default_budget` used SHALL be the one of the resolved Client_Settings, so that a tenant default acts as the cap for a client budget exactly as a key default does in Phase 4.
5. FOR ALL layer combinations, the resolved value of each field SHALL equal the value of the most specific layer that sets it, and `resolve_budget` SHALL keep its v4 Property 4 on the resolved default.
6. THE Request SHALL store the webhook target resolved at creation (v4 Req 8.1), so that a later change of the tenant's webhook configuration does not redirect Requests already created.

### Requirement 5: Dead-Letter and Admin Scoping

**User Story:** As a team lead, I want to replay or discard our own dead-lettered Jobs without seeing anyone else's, so that we can recover without a platform operator.

#### Acceptance Criteria

1. WHEN a Dead_Letter_Record is created for a Job message whose `request_id` belongs to an existing Request, THE State_Store SHALL add it to the Tenant_DLQ_Index of that Request's tenant, in the same call that adds it to the global DLQ indexes, and SHALL keep the Tenant_DLQ_Index in step on replay, discard and purge.
2. A Dead_Letter_Record without a `request_id`, or whose Request does not exist, SHALL belong to no tenant: it SHALL be visible only to Operators.
3. A Principal with Scope `admin` who is not an Operator SHALL, on the DLQ routes, list only the records of its own tenant, and SHALL get 404 `not_found` for the record of another tenant or of no tenant, on get, replay and discard alike.
4. An Operator SHALL see every record, and `GET /v1/admin/dlq` SHALL accept `tenant` to filter. Replay and discard semantics, error codes and the budget guard (v4 Req 5.10) SHALL be unchanged.
5. FOR ALL sequences of Dead_Letter operations over several tenants, the Tenant_DLQ_Index of each tenant SHALL equal the records of the global DLQ indexes whose Request belongs to it, per status and per reason.
6. THE agent drain and undrain routes SHALL require the Operator role.

### Requirement 6: Usage and Cost Reports

**User Story:** As a finance owner, I want durable per-tenant usage and cost per day, so that I can charge teams back.

#### Acceptance Criteria

1. THE State_Store SHALL add to the Usage_Rollup of the Request's tenant and the current UTC day (Redis `TIME`): `submitted` when a Request is created, `finished/{Completed|Failed|Cancelled}` on each terminal write, and, for every UsageRecord at the moment it is appended to a Request, `tokens/in/{provider}/{model}`, `tokens/out/{provider}/{model}` and `tokens/total/{provider}/{model}`. Each write SHALL happen in the script that makes the state change.
2. FOR ALL sequences of operations, the token fields of each tenant's Usage_Rollup for a day SHALL equal, per `{provider}/{model}`, the sum of the UsageRecords appended to that tenant's Requests that day. A Replay SHALL add the usage of the extra attempts and one more terminal count, and nothing else.
3. THE `GET /v1/usage` endpoint (Scope `read`) SHALL return the Usage_Report of the Principal's tenant for the UTC dates `from` and `to` (inclusive; default the last 7 days; range at most 92 days), one entry per day with request counters and per-model token totals, and SHALL reject an invalid range with 400 `invalid_query` naming `from` (malformed date or inverted range) or `to` (malformed date or a range longer than 92 days).
4. THE `GET /v1/admin/usage` endpoint (Operator) SHALL return the same report for one tenant given by `tenant`, or for every tenant when it is absent.
5. THE Usage_Report SHALL carry `estimated_cost` computed when the report is read, from the Gateway's Price_Table (v2 Req 10), per model and in total. It SHALL be `null` without a Price_Table, and `null` for a model the Price_Table does not price, as in v2 Req 10.7; with a Price_Table the total is `{currency, amount, complete}`, where `amount` covers the priced models and `complete` is false when any model is unpriced. The report totals SHALL also carry the request counters of the range. No price SHALL be needed in a Lua script.
6. THE `usage.retention_days` setting (default 400, range 30 to 3650) SHALL set the expiry of each Usage_Rollup hash, refreshed on every write.
7. Usage recorded before the upgrade SHALL NOT appear in a Usage_Report; the README SHALL say so.

### Requirement 7: Request Priority

**User Story:** As a product owner, I want interactive Requests to be served ahead of batch ones, so that a batch flood does not delay a person waiting.

#### Acceptance Criteria

1. THE `POST /v1/requests` body SHALL accept an optional `priority`, `normal` or `high`. An invalid value SHALL be rejected with 400 `invalid_request` naming `priority`. WHEN absent, THE Gateway_API SHALL use the tenant's `default_priority` (default `normal`).
2. WHEN `priority` exceeds the tenant's `max_priority` (default `normal`; `high` for the Default_Tenant), THE Gateway_API SHALL reject the submit with 400 `priority_above_limit` and the limit, creating nothing.
3. THE State_Store SHALL store `priority` on the Request only when it is `high`, and the Request view SHALL return `priority` (`normal` when not stored).
4. THE Manager_Agent SHALL set the Priority_Header `das-priority: high` on every dispatch of a Job of a `high` Request, including retries and Jobs re-dispatched after a Replay or a takeover, and SHALL set no header for `normal`. FOR ALL dispatch sequences, the header SHALL be present if and only if the Request's priority is `high`.
5. THE RabbitMQ adapter SHALL translate the Priority_Header into the AMQP `priority` message property 9, SHALL leave the `priority` property at the client default 0 otherwise, and SHALL NOT declare `x-max-priority`. Other adapters SHALL ignore the header. The Job message payload and the Message_Codec output SHALL NOT change.
6. THE README SHALL state that priority is a weighting and not strict: with both classes queued, `high` Jobs are delivered about two for every `normal` one, and `normal` Jobs are never starved.
7. THE existing queue declarations, the `das-jobs` policy and the delivery limit SHALL be unchanged.

### Requirement 8: Observability

**User Story:** As an operator, I want to see load per tenant and trace a tenant's problem, without opening a new leak.

#### Acceptance Criteria

1. THE Gateway_API and Manager_Agent SHALL add a `tenant` label, limited by the new `metrics.label_limits.tenant` (default 100, range 1 to 500, handled as the other dynamic labels of v2 Req 5.3 and 5.8), to `das_requests_submitted_total` and `das_requests_finished_total` only.
2. THE `das_client_rejections_total{reason}` SHALL gain the reasons `forbidden`, `tenant_quota_exceeded` and `priority_above_limit`.
3. THE Spans `gateway.submit` and `manager.request` SHALL carry the attribute `das.tenant`. The attribute allow-list of the Tracer SHALL be extended by exactly that name.
4. No metric label, Span attribute or index SHALL carry a Key_Fingerprint or a key value, and the global Request_Index SHALL gain no tenant data.
5. THE Grafana dashboard SHALL gain a panel of Requests submitted and finished per tenant. Token and cost per tenant are served by the Usage_Report, not by Prometheus.

### Requirement 9: Migration and Compatibility

**User Story:** As an operator, I want to upgrade a running Phase 4 fleet to Phase 5 without downtime or data loss.

#### Acceptance Criteria

1. THE new release SHALL write the Tenant_Index, Tenant_DLQ_Index, Tenant_Active_Set and Usage_Rollup for every Request and Dead_Letter_Record it handles, and SHALL treat a Request without `tenant` as `default`.
2. THE Manager_Agent SHALL run the Tenant_Backfill (a lock with `SET NX PX`, as the v2 Index_Backfill) that adds every existing Request and Dead_Letter_Record to the Default_Tenant's indexes, and the Tenant_Active_Set of non-terminal Requests, then sets `das:tenant:index:ready`. The backfill SHALL be idempotent: running it twice, or concurrently with writers, SHALL leave the indexes as one run would.
3. UNTIL `das:tenant:index:ready` is set, a Default_Tenant listing SHALL read the global index, and a listing for any other tenant SHALL be answered 503 `index_not_ready`. The README SHALL tell operators to configure `tenants` only after the flag is set.
4. THE Gateway_API and Manager_Agent instances SHALL upgrade in any order. Keys added by Phase 5 are additive and SHALL be ignored by Phase 4 instances, so a rollback loses no Phase 4 data.
5. A client of the Default_Tenant that sends no `priority` and uses no new endpoint SHALL observe the same responses as in Phase 4, apart from the added `priority` field of the Request view and the added fields of the Audit_Entry.
6. THE existing Phase 1 to 4 test suites SHALL keep passing except the tests that pin v2 Req 20.1, 20.3 and 20.6, which SHALL be changed deliberately in this phase; the wire-format baselines (`tests/fixtures/phase3_baseline`) SHALL stay byte-identical; and the import-linter contract SHALL stay kept.

### Requirement 10: Helm Chart

**User Story:** As a platform engineer, I want a Helm chart, so that I can run DAS on Kubernetes.

#### Acceptance Criteria

1. THE Chart in `deploy/helm/das/` SHALL render: a Deployment and Service for the Gateway_API (default 2 replicas), a Deployment for the Manager_Agent (default 2), one Deployment per entry of the `workers` value (each with its `capabilities` and `replicas`), a PodDisruptionBudget for each, a ServiceAccount, and, when enabled, a KEDA `ScaledObject` scaling Workers on the depth of their Capability queues.
2. THE Chart SHALL render the agent configuration into ConfigMaps from structured values, and SHALL take every secret (API keys, Admin_API_Keys, webhook secrets, LLM credentials, Redis and RabbitMQ URLs with credentials) from an existing Secret named in `existingSecret`, through `secretKeyRef`. FOR ALL values, no rendered ConfigMap or manifest other than a Secret reference SHALL contain a secret value.
3. EVERY pod SHALL set `DAS__GRPC__ADVERTISE_HOST` from `status.podIP` and `DAS__AGENT_ID` from `metadata.name`, and SHALL declare `startupProbe` and `livenessProbe` on `/livez` and `readinessProbe` on `/readyz` of the health port.
4. THE `terminationGracePeriodSeconds` SHALL default to 45, and the Chart SHALL refuse to render when it is less than `limits.shutdown_timeout` plus 10 seconds.
5. THE Chart SHALL NOT deploy Redis or RabbitMQ and SHALL NOT include a RabbitMQ definitions job: it SHALL take `redis.url`, `rabbitmq.url` (credential-free) and `rabbitmq.managementUrl` as values, or, for a URL that carries credentials, through `secretEnv` from the Secret.
6. THE `lifecycle.preStopDrain.enabled` value (default false) SHALL add a `preStop` hook that runs `das drain` for the pod's agent id, with the Admin_API_Key taken from the Secret.
7. THE Chart SHALL ship a `values.schema.json` that rejects unknown or mistyped values, and a README that states the Redis and RabbitMQ prerequisites and that a Job whose Worker is killed repeatedly is dead-lettered after the delivery limit.
8. Docker Compose SHALL remain a supported deployment, and its files and e2e stacks SHALL be unchanged except where a requirement above names them.

### Requirement 11: Kubernetes Verification and CI

**User Story:** As a maintainer, I want the chart checked on every change and proven on a real cluster, so that a broken chart is caught before a release.

#### Acceptance Criteria

1. THE fast job SHALL run `helm lint` and `helm template` for the default values and for a values file that enables every optional feature, `kubeconform` (pinned version and pinned Kubernetes schema version) on the output, and the rendered-output unit tests of the Chart.
2. THE Kind_Job SHALL run only on `main` pushes and manual dispatch, with `DAS_REQUIRE_DOCKER=1`; SHALL create a `kind` cluster, build and load the DAS images, deploy Redis, RabbitMQ and the stub LLM from `tests/k8s/`, install the Chart, and run `tests/integration/k8s/`.
3. THE Kind_Job tests SHALL check: every pod becomes Ready; a Request completes; an admin drain reaches an agent over gRPC (the advertise host); deleting a Worker pod and a Manager pod while Requests run loses no Request; and a rolling restart of the Managers and of the Workers with Requests in flight completes every Request.
4. THE test of v2 Req 20.6 (`tests/unit/test_deploy_observability.py`) SHALL be rewritten to assert that Docker Compose files still exist and are valid, and that Kubernetes manifests exist only under `deploy/helm/` and `tests/k8s/`.
5. EVERY tool the CI installs for the Chart (Helm, kind, kubectl, kubeconform) SHALL be pinned to an exact version, and a downloaded binary SHALL be verified against a pinned SHA-256.
6. THE CI static test SHALL list the new job and its timeout, and the README CI section SHALL describe the job names (they are what branch protection refers to).

### Requirement 12: Superseded Boundaries and Out of Scope

**User Story:** As a product owner, I want the changes to earlier boundaries written down, so that later phases and reviewers see what moved.

#### Acceptance Criteria

1. THIS spec SHALL supersede v2 Req 20.1 (visibility across keys), v2 Req 20.3 (tenant identity in labels, Spans and indexes) and v2 Req 20.6 (Compose as the only artifact), each as stated in the Introduction. v2 Req 20.7 SHALL remain in force.
2. THE following SHALL be out of scope for Phase 5: Redis replication, Sentinel or Cluster; a key-management API or keys stored in Redis; per-tenant worker pools or queues; SSO or OAuth; billing integration; tool use or code execution; HTTP or Kafka transports; and per-tenant token or cost metrics in Prometheus.
3. THE `KeyDirectory` interface (design) SHALL be the only place that knows where tenants and keys come from, so that stored keys can replace static configuration later without changing a route handler.
