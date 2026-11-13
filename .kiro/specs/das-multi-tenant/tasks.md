# Implementation Plan: DAS Multi-Tenant and Deployable (Phase 5)

## Overview

Implement Phase 5 in Python 3.12 (asyncio) on top of Phases 1 to 4, keeping the pure core + thin I/O shell model. Order: pure core (`das/tenancy`); config schema; Lua scripts and State_Store; Gateway authentication and scopes; tenant-scoped routes; priority; observability; compatibility and regression; the Helm chart; Kubernetes CI; a Compose e2e project for tenancy; documentation. The chart work (group 10) does not depend on the tenancy code and can run beside it. Every group follows the Phase 4 discipline: tests first (red observed), fault injection on each new test with an unmutated control, drift tests for README, deploy and CI files.

Assumptions pending product-owner confirmation (requirements Introduction): static tenant configuration, no Redis HA, `kind` job on `main`.

## Tasks

- [ ] 1. Pure core `das/tenancy`
  - [x] 1.1 Implement scopes, Principal and the route table
    - `das/tenancy/__init__.py`, `das/tenancy/scopes.py`: `Scope`, `OPERATOR`, `Requirement`, `Principal`, `authorize(principal, requirement)`, `ROUTE_SCOPES` listing every route explicitly (design §2 table, health routes `public`)
    - _Requirements: 2.2, 2.5_
  - [x] 1.2 Implement the key directory
    - `das/tenancy/directory.py`: `KeyDirectory` protocol; `StaticKeyDirectory` using `das.ops.keys.classify_key` (constant time over every configured key), then mapping the matched fingerprint to its `TenantConfig`; admin keys → Operator in `default`; unclaimed api keys → `default` with `submit`, `read`
    - _Requirements: 1.2, 1.3, 1.5, 1.7, 12.3_
  - [x] 1.3 Implement settings layering
    - `das/tenancy/settings.py` and `das/client/settings.py` `resolve_settings(fingerprint, limits, tenant)`: key override > `tenant.settings` > `client_limits.default`, field by field, an override applying to its key whatever the tenant; the Effective_Budget default comes from the resolved settings
    - _Requirements: 1.6, 4.1, 4.2, 4.4_
  - [x] 1.4 Implement usage report assembly
    - `das/tenancy/usage.py`: UTC date range validation (default last 7 days, at most 92, inverted or malformed rejected), merge of rollup hashes into per-day entries and totals, cost at read time through `das.ops.cost` (`null` without a Price_Table or for an unpriced model)
    - _Requirements: 6.3, 6.5_
  - [x] 1.5 Write property test for Principal resolution
    - **Property 5: Principal resolution**
    - **Validates: Requirements 1.2, 1.3, 1.4, 1.5, 1.7**
  - [x] 1.6 Write property test for settings layering
    - **Property 6: Settings layering**
    - **Validates: Requirements 4.1, 4.4, 4.5**

- [ ] 2. Configuration
  - [x] 2.1 Add the tenant schema
    - `das/config/schema.py`: `TenantKeyConfig`, `TenantSettings` (the optional fields of `ClientOverride` without `fingerprint`), `TenantConfig`, `UsageConfig` (`retention_days`), `LabelLimitsConfig.tenant`, `AgentConfig.tenants` and `.usage`; Tenant_ID pattern, scope set, `max_priority` default `normal`
    - _Requirements: 1.1, 4.2, 4.3, 6.6, 7.1, 7.2, 8.1_
  - [x] 2.2 Add cross-field validation
    - `tenant_errors(config)` next to `client_api_errors`: fingerprint matching no key, listed twice, listed for an admin key, two keys with one fingerprint, duplicate or invalid `id`, `webhook_secret_name` not configured; paths only, never a key or secret value; wired into config loading (exit code 2)
    - _Requirements: 1.4_
  - [x] 2.3 Write unit tests for the tenant config
    - Every rule of 1.4 with a sentinel key value that must not appear in any message; absent `tenants` equals Phase 4; ranges and defaults; `deploy/config/gateway.yaml` sample gains a commented tenants example that a drift test loads
    - _Requirements: 1.1, 1.4, 1.5_

- [ ] 3. State_Store keys and scripts
  - [x] 3.1 Add the tenant keys and Lua helpers
    - `das/state/keys.py`: tenant key functions; `_common.lua`: `tenant_of`, `tenant_twin`, `index_move(..., req_key)`, tenant maintenance inside `client_active_add` and `client_active_remove`, `usage_bump` (UTC day from `TIME`, `EXPIRE` from `usage.retention_days` passed in ARGV)
    - _Requirements: 3.5, 5.1, 6.1_
  - [x] 3.2 Maintain the Tenant_Index in the Request scripts
    - `create_request` (store `tenant` and `priority`, tenant quota check before any write, twin adds, tenant active, `submitted`), `commit_plan`, `reopen_request`, `finalize`, `cancel_request`, `replay_dead_letter`, `purge_request` (twins removed using the status read before the hash goes), `finalize` and `cancel_request` bump `finished/{status}`
    - _Requirements: 3.1, 3.5, 3.7, 3.9, 6.1, 7.3_
  - [x] 3.3 Maintain the Tenant_DLQ_Index in the DLQ scripts
    - `create_dead_letter` (store `tenant` on the record when the Request exists; add to the twins), `apply_transition` (`dlq_revert`), `discard_dead_letter`, `replay_dead_letter`, `purge_dead_letter` (twins follow the record's `tenant`)
    - _Requirements: 5.1, 5.2_
  - [x] 3.4 Roll up usage in `append_usage`
    - For every appended UsageRecord, `usage_bump` of `tokens/in`, `tokens/out`, `tokens/total` per `{provider}/{model}`; a record is counted exactly once because `append_usage` is the only place it is persisted
    - _Requirements: 6.1, 6.6_
  - [x] 3.5 Extend the StateStore API
    - `ClientRequestSpec` gains `tenant`, `tenant_max_active`, `priority`; the snapshot and `StoredRequest` gain `tenant` and `priority`; `list_requests` and `list_dead_letters` take a tenant (twin keys, or the global keys for an Operator); `IndexNotReady` for a non-default tenant before `das:tenant:index:ready`; `read_usage(tenant, days)`; submit result `TENANT_QUOTA_EXCEEDED`
    - _Requirements: 3.3, 3.4, 5.3, 6.3, 9.3_
  - [x] 3.6 Write property test for the Tenant_Index mirror
    - **Property 2: Tenant_Index mirrors the global index**
    - **Validates: Requirements 3.1, 3.5, 3.9**
  - [x] 3.7 Write property test for the usage rollup
    - **Property 3: Usage rollup is exact**
    - **Validates: Requirements 6.1, 6.2**
  - [x] 3.8 Add the Tenant_Backfill
    - `tenant_backfill.lua` (batched, idempotent: `ZADD`, `SADD`, `HSETNX`; DLQ records with an existing Request get `tenant=default`), `TenantIndexBackfill` in `das/manager/retention.py` with `das:tenant:index:backfill_lock` (`SET NX PX`) and `das:tenant:index:ready`, started with the other Manager retention tasks
    - _Requirements: 9.2_
  - [x] 3.9 Write property test for the backfill
    - **Property 7: Tenant_Backfill is idempotent and complete**
    - **Validates: Requirements 5.1, 5.2, 5.5, 9.1, 9.2**

- [ ] 4. Gateway authentication and scopes
  - [x] 4.1 Authenticate to a Principal
    - `das/gateway/auth.py`: `api_key_auth` uses the `KeyDirectory`, stores `request.state.principal` and the resolved `client_settings`; `admin_key_auth` keeps `admin_disabled` for a Phase 4 configuration with no admin capability anywhere
    - _Requirements: 1.2, 1.3, 2.1_
  - [x] 4.2 Enforce scopes on every route
    - `das/gateway/authz.py` `authorize_route` (router-level, after authentication and before the rate limit): looks up `(method, route template)` in `ROUTE_SCOPES`, fails closed on a missing entry, answers 403 `forbidden` with `scope`; applied to the `/v1` and admin routers
    - _Requirements: 2.2, 2.3, 2.4, 2.5_
  - [x] 4.3 Write property test for route classification and authorization
    - **Property 4: Every route is classified, and authorization is exact**
    - **Validates: Requirements 2.2, 2.3, 2.4, 2.5**
  - [x] 4.4 Extend the Audit_Entry
    - `tenant` and `operator` fields; key values never logged
    - _Requirements: 2.7_
  - [x] 4.5 Restrict the agents view
    - `GET /v1/agents` drops `agent_id` and `host` for a non-Operator outside the Default_Tenant
    - _Requirements: 2.6_

- [ ] 5. Tenant-scoped routes
  - [x] 5.1 Isolate get, cancel and events
    - Compare the Request's `tenant` with the Principal's (Operators excepted) before starting a stream or writing; 404 `not_found` with the body of a missing Request
    - _Requirements: 3.2_
  - [x] 5.2 Scope the listing
    - `das/gateway/listing.py`: tenant twin keys, tenant (or `*`) in the Page_Cursor filters, `tenant` accepted only from an Operator (400 `invalid_query` otherwise), 503 `index_not_ready` for a non-default tenant before the flag
    - _Requirements: 3.3, 3.4, 9.3_
  - [x] 5.3 Scope submit
    - Tenant stored on create, layered Client_Settings, tenant quota to 429 `quota_exceeded` with `limit_scope: "tenant"` (the per-key body stays as in Phase 4), webhook target resolved at creation, Effective_Budget default from the resolved settings
    - _Requirements: 3.1, 3.7, 4.1, 4.4, 4.6_
  - [x] 5.4 Scope the DLQ and admin routes
    - Tenant `admin` lists, gets, replays and discards only its tenant's records (404 for others and for records of no tenant); Operators see all and may filter with `tenant`; drain and undrain require Operator
    - _Requirements: 5.3, 5.4, 5.6_
  - [x] 5.5 Add the usage endpoints
    - `das/gateway/usage.py`: `GET /v1/usage` (read) and `GET /v1/admin/usage` (Operator, `tenant` optional), reading rollup hashes and building the report with `das.tenancy.usage`
    - _Requirements: 6.3, 6.4, 6.5, 6.7_
  - [ ] 5.6 Update the OpenAPI schema
    - New routes, `tenant` and `priority` parameters, the new error bodies; the OpenAPI test lists every route
    - _Requirements: 2.2, 6.3, 7.1_
  - [x] 5.7 Write stateful property test for tenant isolation
    - **Property 1: Tenant isolation**
    - **Validates: Requirements 3.2, 3.3, 3.4, 3.6, 5.3**
  - [ ] 5.8 Write unit tests for the error responses
    - Every row of the design Error Handling table, including cursor reuse across tenants, the `admin_disabled` compatibility case and 503 when Redis is unreachable
    - _Requirements: 2.3, 3.2, 3.3, 3.4, 3.7, 5.3, 6.3, 7.1, 7.2_

- [ ] 6. Priority
  - [x] 6.1 Validate and store priority
    - `priority` in the submit body (`invalid_request` naming `priority`), tenant `default_priority` and `max_priority` (`priority_above_limit`), stored only when `high`, `priority` in the Request view
    - _Requirements: 7.1, 7.2, 7.3_
  - [ ] 6.2 Carry priority to dispatch
    - `RequestSnapshot.priority` read by `load_snapshot`; `das/manager/driver.py` dispatch publishes with `headers={PRIORITY_HEADER: "high"}` for a `high` Request and none otherwise (retries, takeovers and replays use the same call)
    - _Requirements: 7.4_
  - [ ] 6.3 Write property test for the Priority header
    - **Property 8: Priority header**
    - **Validates: Requirements 7.3, 7.4, 7.5**
  - [x] 6.4 Translate the header in the RabbitMQ adapter
    - `das/transport/base.py` `PRIORITY_HEADER = "das-priority"`; `das/transport/rabbitmq.py` pops it and sets `Message(priority=9)` for `high`, nothing otherwise; `x-max-priority` is never declared; other adapters ignore it; a unit test with a fake channel; codec output and queue declarations unchanged
    - _Requirements: 7.5, 7.7_
  - [x] 6.5 Write the real-broker priority test
    - `tests/integration/test_rabbitmq_priority.py` (testcontainers, RabbitMQ 4.1): delivery order from `basic.get`, the 2:1 weighting with no starvation, requeue keeps the class, a dead-lettered message keeps its priority, no `x-max-priority`
    - _Requirements: 7.5, 7.6_

- [ ] 7. Observability
  - [x] 7.1 Add the tenant label to two metrics
    - `DYNAMIC_LABELS` gains `tenant`, limited by `label_limits.tenant`; `das_requests_submitted_total` (Gateway, from the Principal) and `das_requests_finished_total` (Manager, from the snapshot)
    - _Requirements: 8.1_
  - [ ] 7.2 Add rejection reasons
    - `das_client_rejections_total` reasons `forbidden`, `tenant_quota_exceeded`, `priority_above_limit`
    - _Requirements: 8.2_
  - [x] 7.3 Add the Span attribute
    - `das.tenant` in the Tracer allow-list, set on `gateway.submit` and `manager.request`; a test that no label, attribute or index carries a Key_Fingerprint or key value
    - _Requirements: 8.3, 8.4_
  - [ ] 7.4 Add the Grafana panel
    - Requests submitted and finished per tenant in the dashboard JSON; the existing dashboard static test is extended
    - _Requirements: 8.5_

- [ ] 8. Compatibility and regression
  - [ ] 8.1 Change the tests that pin superseded boundaries
    - Find and update the tests that assert v2 Req 20.1 (listing not filtered by key) and v2 Req 20.3 (no tenant identity in labels, Spans, indexes), each with a comment citing this spec; list them in the task report
    - _Requirements: 9.6, 12.1_
  - [ ] 8.2 Write the mixed-version test
    - Data written by the Phase 4 scripts (no `tenant`, no twins), then the Phase 5 scripts: Default_Tenant listing correct before the flag, non-default 503 before it, correct after the backfill; a Phase 4 reader ignores the new keys
    - _Requirements: 9.1, 9.2, 9.3, 9.4_
  - [ ] 8.3 Write the Default_Tenant compatibility test
    - With no `tenants` configured, the Phase 4 contract tests and Gateway tests pass unchanged and responses match Phase 4 apart from the added fields; the wire-format baselines stay byte-identical
    - _Requirements: 9.5, 9.6_

- [ ] 9. Checkpoint - Ensure all tests pass

- [ ] 10. Helm chart
  - [x] 10.1 Create the chart skeleton
    - `deploy/helm/das/`: `Chart.yaml`, `values.yaml`, `values.schema.json` (unknown keys rejected), `_helpers.tpl` with the grace-period guard (`fail` below `limits.shutdown_timeout + 10`), `ci/values-min.yaml`, `ci/values-all.yaml`
    - _Requirements: 10.4, 10.7_
  - [x] 10.2 Write the workload templates
    - Gateway (Deployment and Service), Manager, one Worker Deployment per `workers` entry, ConfigMaps from structured values, ServiceAccount, PodDisruptionBudgets; secrets only through `secretKeyRef` from `existingSecret`; `DAS__GRPC__ADVERTISE_HOST` from `status.podIP`, `DAS__AGENT_ID` from `metadata.name`; startup and liveness `/livez`, readiness `/readyz`; no Redis, RabbitMQ or definitions job
    - _Requirements: 10.1, 10.2, 10.3, 10.5_
  - [x] 10.3 Add the optional features
    - KEDA `ScaledObject` per Worker Deployment on its Capability queues (`das.jobs.<cap>`), and `lifecycle.preStopDrain` running `das drain` for the pod's agent id with the Admin_API_Key from the Secret
    - _Requirements: 10.1, 10.6_
  - [x] 10.4 Write rendered-output unit tests
    - `tests/unit/test_chart_*.py` render with the `helm` binary (skipped locally without it, required in CI): probes, advertise host, agent id, grace period and its refusal, no sentinel secret in any ConfigMap or manifest, features on and off, `values.schema.json` rejections
    - _Requirements: 10.2, 10.3, 10.4, 10.6, 10.7, 11.1_
  - [x] 10.5 Write the chart README
    - Prerequisites (Redis, RabbitMQ with the management plugin), the secret keys, tenant configuration, the grace period rule, the KEDA and preStop options, and the delivery-limit caveat for a Worker killed repeatedly
    - _Requirements: 10.7_

- [ ] 11. Kubernetes verification and CI
  - [x] 11.1 Write the cluster tests
    - `tests/k8s/` manifests for Redis, RabbitMQ and the stub LLM; `tests/integration/k8s/` tests: pods Ready, a Request completes, an admin drain reaches an agent (advertise host), Worker and Manager pod deletion and rolling restarts with Requests in flight lose none
    - _Requirements: 11.3_
  - [x] 11.2 Update CI
    - Fast job: pinned `helm` and `kubeconform` (SHA-256 verified), `helm lint`, `helm template` for both values files, `kubeconform -strict`; the `Kind_Job` on `main` and manual dispatch (`DAS_REQUIRE_DOCKER=1`, pinned `kind` and `kubectl`, image build and load, test manifests, `helm install`, `pytest tests/integration/k8s`, pod logs as an artifact); `EXPECTED_TIMEOUTS` and the expected fast steps in `test_ci_workflow_static.py`; `actionlint` clean
    - _Requirements: 11.1, 11.2, 11.5, 11.6_
  - [x] 11.3 Rewrite the Compose-only test
    - `tests/unit/test_deploy_observability.py` (the v2 Req 20.6 test): Compose files still exist and are valid; Kubernetes manifests exist only under `deploy/helm/` and `tests/k8s/`
    - Done together with 10.1 (wave 0), because the chart skeleton alone makes the old pin fail; `tests/integration/k8s/` (Python tests) is allowed as a directory name
    - _Requirements: 10.8, 11.4_

- [ ] 12. Compose end-to-end tests
  - [ ] 12.1 Write the tenancy e2e
    - `tests/integration/e2e/test_tenancy_e2e.py`, own project `das-e2e-tenant` with its override and Gateway config: two tenants and an Operator on two Gateways; isolation across every route, scopes, the tenant quota, usage report totals equal to the stub LLM's token counts, a `high` Request overtaking a `normal` backlog, backfill after data written without tenant fields; each test run against a deliberately broken variant and a control
    - _Requirements: 3.2, 3.3, 3.7, 5.3, 6.2, 6.3, 7.4, 7.6, 9.2, 9.3_
  - [ ] 12.2 Run the full regression
    - `DAS_REQUIRE_DOCKER=1 uv run pytest`, `ruff`, `mypy`, `lint-imports`, the client suite on Python 3.10 to 3.13, `helm lint` and `kubeconform`; only the tests named in 8.1 and 11.3 changed, and the Compose files and Phase 3 and 4 e2e stacks are untouched
    - _Requirements: 9.6, 10.8_

- [ ] 13. Documentation
  - [ ] 13.1 Update README and CLAUDE.md
    - README: tenants and scopes (with the config example), the upgrade steps (upgrade every replica, wait for `das:tenant:index:ready`, then add `tenants`), usage reports and the "before the upgrade not included" note, priority as a weighting, idempotency per key and key rotation, the Kubernetes section and Kind_Job names for branch protection, and a "not in Phase 5" list (Redis HA, key-management API, per-tenant worker pools, per-tenant token metrics); `CLAUDE.md` e2e and chart lines; drift tests extended
    - _Requirements: 3.8, 6.7, 7.6, 9.3, 11.6, 12.2_

- [ ] 14. Final checkpoint - Ensure all tests pass, ask the user if questions arise.

## Notes

- Each task references specific requirement criteria. Each of the 8 design properties maps to exactly one property-test sub-task (1.5, 1.6, 3.6, 3.7, 3.9, 4.3, 5.7, 6.3).
- Property tests live in `tests/property/`, carry `# Feature: das-multi-tenant, Property {n}: {title}`, and run at least 100 examples through the Hypothesis profiles.
- No new runtime dependency for the `das` package; the chart tests need the `helm` binary and skip without it locally, while CI installs a pinned one.
- Every task that changes the config schema or the environment variables regenerates `tests/fixtures/phase3_baseline/contract.json` deliberately (`uv run python -m tests.fixtures.phase3_baseline.capture`) and checks the diff is additive (no removed line) and that `codec_corpus.jsonl` stays byte-identical. Wave 0 did this for task 2.1.
- The Phase 5 plan (`docs/superpowers/plans/2026-11-12-das-phase5-multi-tenant-deployable.md`) holds the G0 spike results this spec depends on.
- Timing and multi-Gateway behaviour, priority weighting and Kubernetes behaviour are covered by e2e and integration tests, not property tests.
- If implementation finds a missing or contradictory requirement, update `requirements.md` before changing the design.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.4", "2.1", "3.1", "10.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "3.2", "3.3", "3.4", "6.4", "10.2", "11.3"] },
    { "id": 2, "tasks": ["1.5", "1.6", "2.2", "3.5", "3.6", "3.7", "3.8", "6.5", "10.3", "11.1"] },
    { "id": 3, "tasks": ["2.3", "3.9", "4.1", "10.4", "10.5"] },
    { "id": 4, "tasks": ["4.2", "4.4", "4.5", "11.2"] },
    { "id": 5, "tasks": ["4.3", "5.1", "5.2", "5.3", "5.4", "5.5"] },
    { "id": 6, "tasks": ["5.7", "6.1", "7.1", "7.3"] },
    { "id": 7, "tasks": ["5.6", "5.8", "6.2", "7.2", "7.4", "8.1", "8.2"] },
    { "id": 8, "tasks": ["6.3", "8.3"] },
    { "id": 9, "tasks": ["9"] },
    { "id": 10, "tasks": ["12.1"] },
    { "id": 11, "tasks": ["12.2", "13.1"] },
    { "id": 12, "tasks": ["14"] }
  ]
}
```
