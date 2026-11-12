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
6. **Priority.** *Resolved by spike G0a (section 9): two classes, `normal` and `high`*, not three. Request `priority`
   (tenant default and ceiling) is carried in the Job message as an optional field (omitted when `normal`, so the
   codec baselines stay byte-identical) and the transport sets the AMQP `priority` property (5 or more for `high`) on the
   existing quorum queues. No queue change and no migration. It is a **2:1 weighting, not strict priority**: under
   contention `high` gets about two thirds of the deliveries and `normal` never starves. `low` is dropped (it would
   be `normal`).
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
  `tests/k8s/` for the kind job); startup and liveness `/livez`, readiness `/readyz`, all on port 8081 for every role;
  `DAS__GRPC__ADVERTISE_HOST` from the **pod IP** (downward API; the pod name does not work) and `DAS__AGENT_ID`
  from the pod name; `terminationGracePeriodSeconds` just above `limits.shutdown_timeout` (45 s as in Compose; it
  does **not** need to cover `job_timeout`); no RabbitMQ definitions-import job (the Manager applies the policy and
  the transport declares the queues); PDB and at least two Gateway replicas; `preStop` drain is optional (section 9).

## 5. Work groups, order and dependencies

Sizes are the roadmap's scale (S/M/L) corrected where the code says otherwise. Task count is a guess: 55 to 70 boxes
(Phase 4 had 51).

| # | Group | Size | Depends on |
|---|---|---|---|
| G0 | **Spikes. DONE, results in section 9.** (a) RabbitMQ 4.1 priority with quorum queues and `basic.get`. (b) The existing images on a `kind` cluster: config, probes, gRPC advertise address, pod deletion, rolling restarts. | S | none |
| G1 | **Spec** `.kiro/specs/das-multi-tenant/` (requirements, design, tasks), naming the superseded Req 20 criteria | M | decisions 1, 3, 8; G0 |
| G2 | **Identity and scopes:** tenant config and validation (fingerprint collisions, reserved `default`), `KeyDirectory`, `Principal`, scope checks per route, legacy mapping, tenant in the audit entry, OpenAPI and error table | M | G1 |
| G3 | **Tenant on Requests:** `tenant` field, tenant indexes inside the 19 scripts, scoped get, cancel, list and events, per-tenant quota and defaults, backfill and `ready` flag, retention | L | G2 |
| G4 | **DLQ and admin scoping:** tenant-scoped DLQ, tenant `admin` versus `operator`, malformed records operator-only | M | G3 |
| G5 | **Usage and cost:** rollups in `finalize` and the cancel path, `GET /v1/usage` and `/v1/admin/usage`, bounded `tenant` label (amending Req 20.3), Grafana panel | M | G3 |
| G6 | **Priority:** `normal`/`high` submit field, tenant default and ceiling, optional Job message field, AMQP priority property (no queue change), retries and DLQ replay keep it | S to M | G3 |
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
- **Priority is coarse by nature:** two classes with a 2:1 weighting (measured, section 9). If strict priority or more
  than two classes are ever needed, that is a queue-per-class design (more queues, a worker polling rule), not a tweak.
- **Cost attribution** in `finalize` needs the Price_Table where the script runs; may force passing precomputed totals.
- **Still unverified:** GitHub runner time for a kind job; `kind load` behaviour on a CI runner (section 9, point 10);
  autoscaling (HPA or KEDA), PDB behaviour, a Gateway rollout with two replicas behind a Service, a `preStop` drain
  hook, and Redis or RabbitMQ restarts on Kubernetes were not exercised by the spike.
- **A Job killed repeatedly is dead-lettered:** the broker requeues a Job whose Worker died, but only
  `delivery-limit` (2) times, so a Job whose pods keep being evicted ends in the DLQ. Worth a chart note.
- **Spec conflict still open from Phase 4** (Req 5.9 vs 11.1, partial results of a budget-stopped Request) is
  independent of this phase; fold it into the spec only if you decide to expose results.

## 8. Next step

Confirm or change decisions **1, 3 and 8** (the rest follow the recommendations unless you object); G0 is done
(section 9), so the G1 spec for your review comes next, before any implementation.

## 9. G0 results (2026-11-12; scripts and logs in the session scratchpad, no product code or repo file changed)

**G0a: RabbitMQ 4.1.8, a quorum queue declared as the transport declares it (`x-queue-type: quorum` plus the dead-letter
exchange, and the `das-jobs` delivery-limit policy), messages with an AMQP `priority`, consumed with `basic.get`.**

1. `x-max-priority` is **rejected** on a quorum queue (`PRECONDITION_FAILED`). Never declare it. A plain `priority`
   message property is accepted with no declaration.
2. **Two levels only:** 0 to 4 behave as `normal`, 5 to 9 as `high`. Inside a level, delivery is FIFO: priority 9 does not
   overtake 5, and single messages of priority 1 to 4 queue behind a flood of 0 in publish order.
3. **Weighted, not strict:** with 200 `normal` ahead of 60 `high` in the queue, the first 90 deliveries alternated
   `high, high, normal` until the `high` messages ran out at delivery 89. So `high` gets about two thirds of the
   throughput under contention and `normal` never starves.
4. Messages with no priority are plain FIFO: omitting the property leaves today's behaviour unchanged.
5. `nack(requeue=True)` of a `high` message: redelivered first, priority kept, `x-delivery-count` 1. With
   `delivery-limit` 2 the message is delivered three times and then dead-lettered through the DLX **with its priority
   preserved** (this is `max_attempts - 1`, as the transport computes it).

**G0b: the unmodified `das-gateway`, `das-manager` and `das-worker` images on `kind` v0.33 (Kubernetes 1.37), with the real
`deploy/config/*.yaml`, in-cluster Redis, RabbitMQ 4.1 and the stub LLM (throwaway manifests, not the chart).**

6. **Up in 20 s, zero restarts**; a Request completes end to end. Every role serves `/livez`, `/readyz` and `/healthz`
   on port 8081 (Gateway included), and `/livez` plus `/readyz` probes work as startup, liveness and readiness.
7. **RabbitMQ needs no definitions import.** With no `definitions.json`, the Manager applied the `das-jobs` policy and the
   transport created every queue, DLQs included. Compose's import is a convenience, not a requirement.
8. **gRPC advertise host must be the pod IP.** With `status.podIP` (downward API), admin drain, which goes Gateway to
   agent over gRPC at the advertised address, answered `202` in 0.06 s and the agent became `Draining`. With the
   pod name (the Compose default, `$HOSTNAME`) it answered `504 agent_timeout` and the agent stayed `Available`.
   `DAS__AGENT_ID` from the pod name works, so agent ids equal pod names.
9. **Pod deletion and restarts lose nothing:**
   - graceful delete of a Worker mid-Job: it finished the Job (15 s) and exited after 13 s; the Request completed;
   - force delete (grace 0) of a Worker mid-Job: the broker requeued the unacknowledged Job, the replacement redid it, and the
     Request completed in 33 s (no wait for `job_timeout`);
   - a Job longer than `shutdown_timeout` (45 s against 30 s): the pod exited at 26 s, inside the 45 s grace period; the
     Job was requeued and redone from scratch by the replacement, and the Request completed;
   - rolling restart with 6 Requests in flight: Managers 6 of 6 Completed (31 s), Workers 6 of 6 Completed (19 s).
10. **Tooling note:** `kind load docker-image` failed for the multi-platform base images (`content digest … not found`) under
    Docker Desktop's containerd image store; `docker save --platform linux/arm64` plus `kind load image-archive` worked.
    Not expected on a GitHub runner (classic store), but unverified. Cost here: cluster 45 s, image load 26 s, a 1.3 GB
    node image left in the local Docker cache.

**What changed in this plan because of G0:** Decision 6 is resolved (two classes, no queue change); G6 shrinks; the chart
sketch now says pod IP for the advertise host, a grace period just above `shutdown_timeout` rather than `job_timeout`, no
RabbitMQ definitions job, and an optional `preStop` (a plain SIGTERM already drains safely); the "unverified" list in
section 7 is shorter.

## 10. Implementation progress

The spec is `.kiro/specs/das-multi-tenant/` (committed `991e6df`); `tasks.md` there is the tracker.

**Wave 0 (tasks 1.1, 1.4, 2.1, 3.1, 10.1, and 11.3 pulled forward): done, uncommitted at the time of writing.**
- **New:** `das/tenancy/scopes.py` (Scope, Principal, `authorize`, the read-only `ROUTE_SCOPES`, 16 entries) and `das/tenancy/usage.py` (date range, rollup parsing, report with cost at read time); the tenant schema in `das/config/schema.py` (`TenantConfig`, `TenantKeyConfig`, `TenantSettings`, `UsageConfig`, `metrics.label_limits.tenant`, `AgentConfig.tenants` and `.usage`); the tenant key builders and a Python `tenant_twin` in `das/state/keys.py`; the Lua helpers `tenant_of`, `tenant_twin`, `utc_day`, `usage_bump` and tenant maintenance inside `index_move`, `client_active_add` and `client_active_remove` in `_common.lua`; the chart skeleton in `deploy/helm/das/` (values, strict schema, helpers with the grace-period guard, `ci/values-min.yaml` and `values-all.yaml`).
- **Tests first, red then green:** 236 new tests (`test_tenancy_scopes`, `test_tenancy_usage`, `test_config_tenants`, `test_state_tenant_keys`, `test_chart_skeleton`, the last against the real `helm` 4.3.1). **Fault injection: 17 of 17 faults caught**, with an unmutated control that passes and every file restored byte-for-byte. Fast-job set 2950 passed; ruff, mypy and import-linter clean; the Phase 3 platform and Phase 4 client API Compose stacks (14 tests) pass on real Redis with the changed `_common.lua`.
- **Found while building, and fixed in the spec:** `label_limits` lives under `metrics` (`metrics.label_limits.tenant`), not at the top level; the real route templates use `{request_id}`, `{dead_letter_id}` and `{agent_id}`; the report total gains `complete` and the request counters; usage range errors name `from` (malformed or inverted) or `to` (malformed or too long); `redis.url` and `rabbitmq.url` are credential-free values while credentialed URLs come from `secretEnv`.
- **Two consequences handled deliberately:** the v2 Req 20.6 test cannot coexist with a chart, so task 11.3 (its rewrite) was done with 10.1, and a stray manifest outside `deploy/helm/` and `tests/k8s/` still fails it (checked); and the config contract baseline `tests/fixtures/phase3_baseline/contract.json` was regenerated for the new fields, with 224 lines added, none removed, and the codec corpus byte-identical.
- **Open for task 3.4:** how `usage.retention_days` reaches the scripts (an extra ARGV); `usage_bump` already takes it and defaults to 400.
