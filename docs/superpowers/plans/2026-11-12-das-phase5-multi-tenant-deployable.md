# Phase 5 plan: multi-tenant and deployable

Status: **plan only, nothing implemented.** Written 2026-11-12 after Phase 4 shipped (`039d0bc`, CI green).
Source: `.kiro/specs/roadmap.md` (Phase 5), checked against the code. Next step is a spec folder
`.kiro/specs/das-multi-tenant/` (requirements, design, tasks), as for Phases 1 to 4; this plan feeds it.

**Goal.** One DAS installation serves several teams with isolation, least-privilege keys, per-tenant usage and
priorities, and it runs on Kubernetes as well as Docker Compose.

**Not in Phase 5:** Redis HA (stays Phase 7, see Decision 3), per-tenant worker pools, a key-management API,
SSO/OAuth, billing integration, tool use or code execution (Phase 6), HTTP/Kafka transports.

## 1. What the code says today (verified, not assumed)

| Area | Fact | Where |
|---|---|---|
| Identity | Two static key lists (`DAS__API_KEYS`, `DAS__ADMIN_API_KEYS`; env or secrets file only). The only identity is the 8-hex Key_Fingerprint (32 bits: fine for a bucket name, too weak to be a tenant boundary). An admin key also passes `/v1` auth. | `das/gateway/auth.py`, `das/ops/keys.py` |
| Per-key settings | `client_limits.overrides` match by fingerprint; Phase 4 already stores `client_fp` on every Request hash and keeps `das:client:{fp}:{bucket,active,idem,streams}`. Phase 4 design D3 says "Phase 5 tenancy replaces it". | `das/config/schema.py`, `das/state/keys.py` |
| Visibility | Any valid key can query, cancel and **list every Request** (`GET /v1/requests` is unfiltered). DLQ and admin routes are global. | Phase 2 Req 20.1 |
| Redis layout | Per-Request keys are `das:req:{rid}…` (no literal hash tag). Global indexes (`requests:index`, `by_status:*`, `finished`, `pending`, `leases:expiry`, `dlq:*`, `webhooks:due`) are written by **19 of the 23 Lua scripts** (and the shared `_common.lua`), in the same call as the per-Request keys. | `das/state/keys.py`, `das/state/lua/` |
| DLQ | Records can have `request_id = None` (malformed messages), so no tenant can own them. | `das/models/dlq.py` |
| Metrics | Label policy is explicit (`DYNAMIC_LABELS` capped per label); `tenant` is not a label. `das_queue_depth{queue}` already exists. | `das/observability/metrics.py` |
| Queues | RabbitMQ 4.1 quorum queue per Capability (`das.jobs.<cap>`); workers pull with `basic.get`; no priority anywhere. | `das/transport/rabbitmq.py` |
| Ops hooks | `/livez`, `/readyz`, SIGTERM graceful shutdown, `das drain` CLI and admin drain; `job_timeout` 300 s, `lease_duration` 30 s. | `das/health/`, `das/cli.py` |
| Tooling here | `helm`, `kubectl`, `kind`, `minikube` installed; `kubeconform` is not. A real cluster test is possible locally. | |

## 2. Shipped boundaries that Phase 5 deliberately supersedes

Phase 2 Req 20 is "out of scope for this phase", written so later phases can add these. Phase 5's spec must name each
one it replaces, and the tests that pin it must change on purpose, not fail by surprise.

| Phase 2 Req | Says | Phase 5 |
|---|---|---|
| 20.1 | Any key sees every Request; listing not filtered by key | Replaced: a tenant sees only its own. Legacy single-tenant installs behave as before (all keys map to `default`). |
| 20.3 | No tenant identity in Metric_Label, Span attribute or Request_Index; no Key_Fingerprint or key value | Amended: a tenant **name** (config, not a secret) may appear in a bounded metric label, a span attribute and a per-tenant index. Key values and fingerprints stay banned. |
| 20.6 | Compose is the only deployment artifact; no K8s manifests or Helm chart | Replaced. Pinned by `tests/unit/test_deploy_observability.py:297` (scans the repo for manifests): rewrite it to allow `deploy/helm/` and keep checking that Compose still works. |
| 20.7 | One Redis, no replication, Sentinel or Cluster | **Unchanged** in Phase 5 (Decision 3). |

(20.2, per-key limits, was already superseded by Phase 4.)

## 3. Decisions (recommendation first; say if you want a different one)

Three need your answer because they change scope or cost: **1, 3 and 8**.

1. **Where tenants and keys come from.** *Recommended: static config.* The YAML names each tenant and maps key
   **fingerprints** to scopes (the same trick as `client_limits.overrides`); the key values stay in the existing
   env/secrets channel, so no new secret path. Startup rejects two keys with the same fingerprint. Key rotation is a
   Secret change plus a rollout. Alternative: tenants and hashed keys stored in Redis with an admin API (no restart,
   self-service), roughly a second L-size item (hashing, show-once secrets, revocation, an auth lookup on every call).
   The plan puts a `KeyDirectory` interface in between so that can follow later without touching handlers.
2. **How the tenant enters Redis.** *Recommended: a `tenant` field on the Request hash (like `client_fp`) plus additive
   per-tenant keys* (`das:tenant:{t}:requests`, `…:by_status:{s}`, `…:active`, `…:dlq:*`, `…:usage:{yyyymmdd}`), written
   by the Lua scripts that already write the global indexes. Per-Request keys do not change, so there is **no data
   migration, only a backfill** (Requests without the field are `default`), using the Phase 2 `index:ready` pattern.
   The roadmap's alternative, the tenant in every key name, renames every key and couples tenancy to Cluster.
3. **Redis HA: not in Phase 5; answer the uptime question.** Cluster needs every key of one script in one slot, and
   today's scripts mix per-Request keys with global indexes, so Cluster means rewriting the state layer. **Sentinel
   (one primary, failover) works with the current layout**, but failover can lose the last acknowledged writes, and
   the lease and fencing design assumes they are durable: that needs its own review. So HA is a small, separate phase,
   pulled ahead only if you have an uptime target. *Do you?*
4. **Scopes.** `submit` (create, cancel), `read` (get, list, events, own usage), `admin` (own tenant's DLQ replay and
   discard, own usage), `operator` (platform: agents, drain, malformed DLQ, every tenant). Compatibility is a hard
   constraint, as in Phase 4: today's `API_KEYS` become `default`-tenant `{submit, read}` and `ADMIN_API_KEYS` become
   `default`-tenant `{submit, read, admin}` plus `operator`; Phase 1 to 4 suites pass unchanged.
5. **Usage reports.** *Recommended: durable daily rollups in Redis, written by `finalize.lua`* (a Request finalizes
   exactly once, so no double count), plus a bounded `tenant` label on four metrics for Grafana. Prometheus alone
   is not chargeback-grade (resets, scrape gaps, retention). The roadmap's "S" is optimistic: cost needs the Price_Table
   (the Manager has it, the script does not) and the cancel path computes it in the Gateway.
6. **Priority.** Request `priority` in `low|normal|high`, tenant default and ceiling, carried in the Job message as an
   optional field (omitted when `normal`, so the codec baselines stay byte-identical). The transport mechanism
   (quorum-queue priority, which in 4.x has two levels, versus a queue per class) is **decided by a spike**, G0 below,
   because workers pull with `basic.get`.
7. **Autoscaling.** The chart ships an optional KEDA `ScaledObject` on `das_queue_depth` (or the RabbitMQ scaler), and
   a CPU HPA as the default-off baseline. No Prometheus adapter dependency.
8. **How deep to verify Kubernetes.** *Recommended: `helm lint`, `helm template` plus `kubeconform` and rendered-output
   unit tests on every push, and a `kind` end-to-end job on `main` (cluster, Redis and RabbitMQ, the chart, a Request
   completing, a rolling restart that loses no Request).* Costs a fifth CI job (my guess: 6 to 10 minutes). Cheaper
   alternative: static checks only, which would leave probes, drain and gRPC addressing unproven.
9. **Fairness.** Workers stay one shared pool. Noisy neighbours are handled by per-tenant quotas (Phase 4 mechanism,
   keyed by tenant) and priority, not by worker isolation.

## 4. Design sketch

- **Principal.** Auth produces `Principal(tenant, scopes, key_fp)` instead of only `client_settings`. The rate limit
  stays per key (fingerprint); quotas, default budgets, webhook target and secret names become per tenant. Idempotency
  keys are scoped to the tenant, so rotating a key does not break replays.
- **Isolation rule.** Every tenant route reads through the tenant's own index or checks the Request's `tenant`; a
  mismatch is **404 `not_found`**, never 403 (no existence leak). Operator routes keep the global keys.
- **Atomicity.** Tenant index writes live inside the existing Lua scripts (`create_request`, `index_request`,
  `finalize`, `cancel_request`, `purge_request`, `reopen_request`, `create_dead_letter`, replay and discard), so the
  tenant view cannot drift from the global one. Purge and retention clean the tenant keys with the Request.
- **Migration (expand, then contract).** Release N writes both indexes and still reads the global one; a backfill
  fills `default`'s tenant index; release N+1 reads tenant indexes once `das:tenant:index:ready` is set. Mixed-version
  rolling upgrades are a test case, not a hope.
- **DLQ.** A record with a `request_id` is scoped through that Request's tenant; a record without one (or whose
  Request was purged) is operator-only.
- **Chart (`deploy/helm/das/`).** Gateway, Manager and one Worker Deployment per capability set (values list); config
  in a ConfigMap, secrets by `existingSecret`; Redis and RabbitMQ are external URLs (test-only manifests under
  `tests/k8s/` for the kind job); probes `/livez` and `/readyz`; `terminationGracePeriodSeconds` above `job_timeout`
  for Workers; optional `preStop` running `das drain --wait`; PDB; `DAS__GRPC__ADVERTISE_HOST` from the pod IP
  (downward API), to be proven in G0.

## 5. Work groups, order and dependencies

Sizes are the roadmap's scale (S/M/L) corrected where the code says otherwise. Task count is a guess: 55 to 70 boxes
(Phase 4 had 51).

| # | Group | Size | Depends on |
|---|---|---|---|
| G0 | **Spikes.** (a) RabbitMQ 4.1 priority with quorum queues and `basic.get`: order, starvation, requeue and retry behaviour. (b) The existing images on a `kind` cluster: config, probes, gRPC advertise address, drain on `preStop`. | S | none |
| G1 | **Spec** `.kiro/specs/das-multi-tenant/` (requirements, design, tasks), naming the superseded Req 20 criteria | M | decisions 1, 3, 8; G0 |
| G2 | **Identity and scopes:** tenant config and validation (fingerprint collisions, reserved `default`), `KeyDirectory`, `Principal`, scope checks per route, legacy mapping, tenant in the audit entry, OpenAPI and error table | M | G1 |
| G3 | **Tenant on Requests:** `tenant` field, tenant indexes inside the 19 scripts, scoped get, cancel, list and events, per-tenant quota and defaults, backfill and `ready` flag, retention | L | G2 |
| G4 | **DLQ and admin scoping:** tenant-scoped DLQ, tenant `admin` versus `operator`, malformed records operator-only | M | G3 |
| G5 | **Usage and cost:** rollups in `finalize` and the cancel path, `GET /v1/usage` and `/v1/admin/usage`, bounded `tenant` label (amending Req 20.3), Grafana panel | M | G3 |
| G6 | **Priority:** submit field, tenant default and ceiling, Job message field, transport mechanism from G0, retries and DLQ replay keep it | M | G0, G3 |
| G7 | **Compatibility:** Phase 1 to 4 suites unchanged, codec baselines byte-identical, mixed-version upgrade test, updated pins of Req 20.1, 20.3, 20.6 | M | G3 to G6 (grows with each) |
| G8 | **Helm chart:** templates, values, probes, drain, PDB, optional KEDA, README; `kubeconform` added to CI | M | G0b (can run beside G2 to G6) |
| G9 | **CI, docs, final checkpoint:** kind job (Decision 8), README and CLAUDE.md, branch-protection note | S | all |

## 6. Verification strategy

- **Isolation is a property, not a list of cases:** a Hypothesis stateful test drives two or three tenants (submit,
  cancel, list, page, DLQ, replay) and asserts nobody ever observes another tenant's Request, ids, cursors or counts.
  Cursors must not work across tenants (the Page_Cursor already binds its filter; the tenant joins it).
- **Same discipline as Phase 4:** tests first (red observed), real-stack e2e on Compose with two tenants (isolation,
  scopes, usage, priority order), fault injection on each new test with an unmutated control, drift tests for README,
  deploy and CI files.
- **Chart:** rendered-output unit tests (probes, grace period, env), `helm lint`, `kubeconform`, then the kind job.
- **Gates unchanged:** `ruff`, `mypy`, `lint-imports` (tenancy code stays out of `das.manager` and `das.worker`
  transport imports), `DAS_REQUIRE_DOCKER=1 uv run pytest`.

## 7. Risks and open points

- **Tenant isolation bug = data leak.** Mitigated by the stateful property test and the 404 rule, but every new route
  must be forced through the scope check (a test that walks the OpenAPI routes and fails on an unclassified one).
- **Touching 19 Lua scripts.** Largest regression surface; the existing atomicity and fault-injection tests are the net.
- **Mixed-version rollout** is the real migration risk (see section 4).
- **Priority behaviour is unproven** until G0a; if quorum-queue priority is too coarse, the queue-per-class design costs
  more queues and a worker polling rule.
- **Cost attribution** in `finalize` needs the Price_Table where the script runs; may force passing precomputed totals.
- **Unverified by me:** how RabbitMQ 4.1 orders `basic.get` with priorities (from memory, hence the spike); the pod-IP
  gRPC advertise address on Kubernetes; GitHub runner time for a kind job.
- **Spec conflict still open from Phase 4** (Req 5.9 vs 11.1, partial results of a budget-stopped Request) is
  independent of this phase; fold it into the spec only if you decide to expose results.

## 8. Next step

Confirm or change decisions **1, 3 and 8** (the rest follow the recommendations unless you object), then run G0 (two
small spikes, no product code) and write the G1 spec for your review before any implementation.
