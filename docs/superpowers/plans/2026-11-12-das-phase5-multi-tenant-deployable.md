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
- **Decided in task 3.4:** `usage.retention_days` reaches the scripts through the Redis key `das:tenant:usage_retention_days`, written at startup by the Gateway and Manager (an ARGV tail clashes with `commit_plan` and `cancel_request`); `usage_bump` reads it, default 400.

**Wave 1 (tasks 1.2, 1.3, 3.2, 3.3, 3.4, 6.4, 10.2): done, uncommitted at the time of writing.**
- **New:** `das/tenancy/directory.py` (`KeyDirectory`, `StaticKeyDirectory`), `das/tenancy/settings.py` (`layered`), the `das-priority` header and AMQP priority 9 in the RabbitMQ adapter, the chart workload templates, and tenant/DLQ twin and usage-rollup maintenance in 12 Lua scripts plus `StateStore.set_usage_retention`.
- **Tests first:** `test_tenancy_directory`, `test_tenancy_settings`, `test_chart_templates` (real `helm`), `test_state_tenant_scripts` (46), and transport additions. Fault injection: 27 faults, 1 survived at first (a static `checksum/config` on the Gateway; the test only changed manager config), the test now covers every role and all three templates are caught.
- **Existing tests adjusted on purpose:** the retention test now allows the tenant usage rollup to outlive its Request, and the Dead_Letter order property excludes the tenant DLQ twins from its "untouched" snapshot.
- **Verified:** fast job set 3070 passed, Compose e2e (Phase 3 platform and Phase 4 client API, 14 tests) on real Redis, ruff, mypy, lint-imports.

**Wave 2 (tasks 1.5, 1.6, 2.2, 3.5 to 3.8, 6.5, 10.3 and 11.1): done, uncommitted at the time of writing.**
- **New:** `tenant_errors` (config exit code 2, paths only), the tenant-aware `StateStore` reads (`tenant=` on both listings, `IndexNotReady`, `read_usage`, snapshot `tenant` and `priority`), `tenant_backfill.lua` with `TenantIndexBackfill` (a subclass of the Phase 2 `IndexBackfill`, started once the Request_Index is ready), the KEDA `ScaledObject` and `preStop` drain options of the chart, and the kind cluster harness (`tests/k8s/`, `tests/integration/k8s/`, opt-in with `DAS_K8S_E2E=1`).
- **Properties 2, 3, 5 and 6** are Hypothesis tests over a model of the platform (`tests/property/tenant_ops.py`): random sequences of create, plan, job result, finalize, fail, cancel, replay, discard, purge, rejected writes and clock ticks, checked after every step, plus one explicit sequence that runs every kind.
- **Fault injection:** 37 faults on the wave's code, 31 caught at first. The survivors were a generator that rarely reached dead letters and `cancel` (fixed: every operation now builds the state it needs), a backfill test with nothing stale to remove (a test added), and one equivalent mutant (`HSETNX` against `HSET` when the record has no tenant yet). 14 of 14 chart faults and 2 of 3 transport faults were caught by the real-broker tests (the third, declaring `x-max-priority`, makes the broker refuse the queue and the test hang, which fails it by timeout).
- **Decided:** the KEDA minimum defaults to 1, not 0. The Manager fails a Job as `no capable worker` after `limits.capability_wait` when no registered Worker has its Capability, so a pool scaled to zero never lets its queue grow. The `preStop` hook uses the first key of the Secret value (it may hold the whole admin list), always exits 0, and is not added to the Gateway (it is not an agent in the registry).
- **Caveat for the README (task 10.5):** `preStop` puts one Admin_API_Key into the environment of every Manager and Worker pod; use a dedicated admin key in its own Secret key (`lifecycle.preStopDrain.adminKeySecretKey`) rather than the whole `ADMIN_API_KEYS` list.

**Wave 3 (tasks 2.3, 3.9, 4.1, 10.4 and 10.5): done, uncommitted at the time of writing.**
- **4.1, the first change to the Gateway's running code:** `api_key_auth` and `admin_key_auth` resolve a Principal through the `KeyDirectory` (injectable), store it on `request.state.principal`, and resolve the Client_Settings with the tenant layer; `create_app`, `create_admin_router` and the CLI pass `tenants`. A Phase 4 call shape still works. `admin_disabled` now means that nothing can administer: no admin key and no tenant key holding the `admin` Scope.
- **3.9, Property 7:** two Worlds fed the same operations, one stripped back to Phase 4 data and backfilled a batch at a time beside later writers, must end with the same keys. The single designed difference is a dead letter whose Request was purged before the backfill reached it: it gets no tenant. The orphan set is derived from the state before the backfill, not from its result, because the first version derived it from the result and so hid the fault "files records without a Request".
- **2.3:** the sample `gateway.yaml` has a commented `tenants` example, loaded by a drift test (it resolves the sample key to the tenant it names and shows the layers in order); every rule of Req 1.4 is shown not to echo a key, including a key pasted where a fingerprint goes.
- **10.4, a finding:** a credentialed URL in `redis.url` or `rabbitmq.url` was accepted and rendered as a plain env value, and `config.*` accepted secret fields. The chart now refuses both (`das.refuseSecrets`, with `das.secretFields` kept equal to the agent's `SECRET_FIELDS` by a test). A schema pattern was tried first and rejected: Helm repeats the offending value in its error, which would have printed a password into CI logs. The chart tests also fail, not skip, when `CI=true` and `helm` is missing.
- **10.5:** the chart README, with drift tests that compare it with `values.yaml`, the secret keys, the CI values files and the files it names.
- **Fault injection:** 4 of 4 on the sample, 7 of 7 on the secret guard, 14 of 14 on the README (after tightening two weak assertions), 10 of 10 on the backfill property (after fixing the orphan derivation and adding a dead-letter stale-status test).

**Wave 4 (tasks 4.2, 4.4, 4.5 and 11.2): done, uncommitted at the time of writing.**
- **4.2:** `das/gateway/authz.py` `authorize_route` runs after authentication and before the rate limit on `/v1` and on the admin router. It looks up `(method, route template)` in `ROUTE_SCOPES`, fails closed on a missing entry (an Operator included), and answers 403 `forbidden` with a `scope` field. A refusal costs no rate-limit token and touches no store. **Interim state, on purpose:** `admin_key_auth` still lets only an Operator through the admin router, because the DLQ routes are not tenant-scoped until task 5.4; letting a tenant's `admin` key in now would show it every tenant's dead letters. Task 5.4 removes that gate when the routes are scoped.
- **4.4:** the Audit_Entry gains `tenant` and `operator` (null when no key matched). A test key named `operator-...` once hid the field, because the log redactor masks every 8-character window of a registered key anywhere in a log line, field names included; the tests now avoid such key names.
- **4.5:** `GET /v1/agents` drops `agent_id` and `host` for a non-Operator outside the default tenant.
- **11.2:** the fast job installs pinned helm and kubeconform (versions and SHA-256s in the workflow `env`, each verified with `sha256sum -c` before install, the checksums cross-checked against the publishers' own files), then runs `helm lint --strict`, `helm template` and `kubeconform -strict` (Kubernetes schema 1.37.0, KEDA from the datree CRD catalog) for both values files. The Kind_Job (`k8s`) runs on `main` and manual dispatch with pinned helm, kind and kubectl, builds the three images, runs `tests/integration/k8s`, uploads pod logs on failure and always deletes its cluster. The Integration_Job now ignores `tests/integration/k8s`. The static CI test and the README CI section name the new job.
- **Not run here:** `kubeconform`, `actionlint` and the GitHub job itself. The sandbox refused to execute a freshly downloaded binary, so the kubeconform flags and the `-strict` result on the rendered chart are verified only by reading the CRD schema (every rendered ScaledObject field is allowed; metadata values are strings) and by the chart tests. The first CI run is the real test of that step.
- **Fault injection:** 17 of 17 on authorization, the agents view and the audit fields (after adding a test that the admin router fails closed on an unlisted route); 15 fault edits of the workflow, each caught by its check.

**Wave 5 (tasks 4.3, 5.1 to 5.5): done, uncommitted at the time of writing.**
- **Isolation (5.1 to 5.3):** `can_see(principal, tenant)` is the one rule (an Operator sees every tenant, anyone else their own). `GET`, `cancel` and `events` compare tenants and answer the body of a missing Request; `cancel` reads the tenant first (`StateStore.request_tenant`, one `HMGET`) so a foreign cancel writes nothing, and `events` decides before a stream slot is taken. The listing reads the tenant twins, binds the tenant (or `*`) into the Page_Cursor filters, accepts `tenant` only from an Operator, and maps `IndexNotReady` to 503 `index_not_ready` (`_bounded` lets it through instead of calling it a store failure). Submit stores the tenant, applies the tenant quota (429 `quota_exceeded` with `limit_scope: "tenant"`; the per-key body is unchanged) and the tenant's default budget and webhook target. A cursor issued before the upgrade has no tenant in its filters and is refused once, with 400 `invalid_query` naming `cursor`.
- **DLQ and admin (5.4):** `admin_key_auth` now only authenticates, so a tenant key holding Scope `admin` reaches the dead-letter routes (the interim operator gate of wave 4 is gone); each handler shows it only its own tenant's records and answers 404 for another's or for a record of no tenant, on get, replay and discard alike. An Operator sees everything and may filter with `tenant`. Drain, undrain and the admin usage report stay Operator-only. `admin_disabled` means nothing can administer: no admin key and no tenant key with `admin` (`can_administer`).
- **Usage (5.5):** `GET /v1/usage` (the caller's tenant) and `GET /v1/admin/usage` (one tenant or all, Operator only), with "today" taken from Redis `TIME`, the clock the rollups are keyed by.
- **Property 4 (4.3)** and **a lesson:** recent FastAPI wraps an included router in one object, so a walk over `app.routes` found no routes and passed vacuously, in the wave 4 test as well as the first draft of this one. `tests/fixtures/routes.py` now flattens the routers and refuses to return fewer than ten routes. Both walks also check that no table entry names a route that does not exist.
- **Fault injection:** 35 faults on isolation, the listing and cursors, submit and the quota, DLQ scoping, `admin_disabled` and usage; 32 were caught at first. The survivors were a fake clock that equalled the real date (moved), a repeated query parameter (a test added) and a store read that HTTP cannot tell apart (direct store tests added); all 35 are now caught.

**Wave 6 (tasks 5.7, 6.1, 7.1, 7.3): done, uncommitted at the time of writing.**
- **Priority at the Gateway (6.1):** the submit body takes `priority` (`normal` or `high`; `null` counts as absent). An absent one is the tenant's `default_priority`; one above its `max_priority` is 400 `priority_above_limit` with the limit and nothing created; anything else is 400 `invalid_request` naming `priority`. The Default_Tenant has no `TenantConfig` and may use `high` (`ClientSettings.default_priority` / `max_priority`). The store already kept it only when `high`; the Request view now always shows it. The type is tested before membership, because a list or object body is unhashable and would otherwise have been a 500. The Idempotency Body_Hash is unchanged (it covers prompt and budget, as in Phase 4), so a retry that changes only `priority` is a replay of the first call. The `priority_above_limit` rejection counter is task 7.2 and the dispatch header task 6.2, both wave 7.
- **Tenant label (7.1):** `tenant` joins `DYNAMIC_LABELS` and is on exactly `das_requests_submitted_total` (Gateway, from the Principal) and `das_requests_finished_total` (Gateway cancel from the pre-read owner; Manager from the Request it finished, remembered in `_adopt` so the pure reducer's snapshot is untouched). The duration histogram keeps `status` alone. `tenant` is a required argument of both facades: a default would have been a silent wrong label. Existing selectors such as `{status="Completed"}` still match the series.
- **Span attribute (7.3):** `das.tenant` is in the allow-list and set on `gateway.submit` (when the span opens, so a refused submit is attributable too) and `manager.request`, on no other span. A test scans spans, the rendered metrics and every `das:tenant:*` key for the key value and its fingerprint, and checks that the global Request index holds no tenant data.
- **Property 1 (5.7):** a Hypothesis sequence mixing the `World` operations of Properties 2 and 3 (create, plan, finalize, fail, cancel, replay, discard, purge, clock ticks) with Gateway calls by two tenants, the default tenant and an Operator: submit, get, events, cancel, the paged listing, a cursor taken to another scope (Requests and Dead Letters), the Dead Letter list, get, replay and discard, and usage. Each response is checked against the ownership model and scanned for the ids the caller may not see; a refused mutation must leave the whole store unchanged. `World.adopt` lets a Request created through the Gateway join the model.
- **Fault injection:** 35 faults over the isolation checks, priority, the metrics label and the span attribute. Two survived at first and were gaps in the property, not in the product (no Dead Letter cursor probe, and no submit through the Gateway); both probes were added and all 35 are now caught.
- **Contract baseline:** regenerated; the diff is the new label on the two counters only.

**Wave 7 (tasks 5.6, 5.8, 6.2, 7.2, 7.4, 8.1, 8.2): done, uncommitted at the time of writing.**
- **Dispatch header (6.2):** `_publish_dispatch` is the single place a Job is dispatched, so first dispatches, retries, takeover re-dispatches and Replays all go through it; it passes `{das-priority: high}` for a `high` Request and no headers otherwise. *Deviation from the task text:* the priority is remembered by the driver in `_adopt` (re-read on every reload) instead of being added to the pure reducer's `RequestSnapshot`, as the tenant already is for the metric label: the reducer has no use for it, and a header is not a reducer concern. Tests: every dispatch of a `high` Request, retries, a takeover re-dispatch, none for `normal` or a Request written before the upgrade, and an identical decoded payload with and without priority. The broker-ordering property is task 6.3 (wave 8).
- **Rejection reasons (7.2):** `forbidden` (counted in `authorize_route`, for both routers; `admin_disabled` is a different answer and is not counted), `tenant_quota_exceeded` (the tenant 429 no longer uses `quota_exceeded`, which stays the per-key reason) and `priority_above_limit`. The README lists them; the README drift test and the catalog test were updated.
- **Grafana (7.4):** a 14th panel, submitted per tenant and finished per tenant and status; the static test now requires fourteen panels with unique ids and places, and that the tenant panel has no token or cost query (the Usage_Report serves those). Added as a text insertion: the rest of the dashboard file is byte-for-byte unchanged.
- **OpenAPI (5.6):** the document now lists exactly the authenticated routes the application serves (a route added without a place in the document fails the test), documents `GET /v1/usage`, the list route's query parameters (including Operator-only `tenant`; the Phase 4 ones were undocumented before), the `priority` enum, `403 forbidden` on every route, `priority_above_limit`, `limit_scope`, `index_not_ready`, and two new optional `ErrorResponse` fields (`scope`, `limit_scope`).
- **Error table (5.8):** one test per row of the design table through the real Gateway, including cursor reuse across tenants, `admin_disabled`, 403 before any store access and before the rate limit (with Redis unreachable and with a one-token bucket), the per-key quota being reported before the tenant's, and 503 for eight routes with Redis unreachable. The two chart rows are `tests/unit/test_chart_skeleton.py`.
- **Superseded boundaries (8.1), the list:**
  - v2 Req 20.1 (listing not filtered by key): `tests/unit/test_gateway_list_requests.py` (module docstring and `test_admin_key_lists_too_and_bad_key_is_401`: still true for the default tenant and the Operator, now said so); superseded in behaviour by `tests/unit/test_gateway_isolation.py` and `tests/property/test_tenant_isolation.py`.
  - v2 Req 20.3 (no tenant identity in labels, Spans, indexes): `tests/property/test_span_attribute_allowlist.py` and `tests/property/test_metrics_cardinality.py` (docstrings: `das.tenant` and the `tenant` label are allowed, key values and fingerprints still are not); `tests/unit/test_metrics_recorder.py`, `test_client_metrics.py`, `test_gateway_v1_extensions.py`, `test_gateway_submit_client.py`, `test_manager_agent.py` and `test_manager_driver_observability.py` pinned the old label set of the two counters and now name the tenant; `tests/unit/test_tracing_tenant.py` and `test_metrics_tenant.py` assert the new boundary, including that no key or fingerprint appears.
  - Other pre-existing tests changed by this phase, each with a comment citing the requirement: `test_page_cursor_rejection.py` (a cursor is bound to a tenant, Req 3.3), `test_admin_api_authorization.py` and `test_gateway_admin_auth.py` (the 403 names its Scope, Req 2.3), `test_replay_discard_check_order.py` (the DLQ twins, Req 5.1), `test_state_phase2_retention.py` (usage rollups outlive their Request, Req 6), `test_gateway_contract.py` (the added `priority` field), `test_deploy_observability.py` (the 14th panel), `test_ci_workflow_static.py` and `test_rabbitmq_transport.py` (the kind job and the priority header). v2 Req 20.6 (no chart in the repository) was superseded by the chart itself, in wave 2.
  - The wire-format baselines stay byte-identical (`codec_corpus.jsonl` unchanged); `contract.json` gained the two usage routes, the `tenant` label of two counters and the three rejection reasons, all additions.
- **Mixed-version (8.2):** Phase 4-shaped data (no `tenant`, no `das:tenant:*`) under the real Gateway and the real `TenantIndexBackfill`: before the flag the default tenant lists correctly from the global index and a configured tenant gets 503; after it the old Requests belong to the default tenant and a configured tenant starts empty; new Requests are filed under their tenant; writes between the upgrade and the backfill are reconciled; a second backfill changes nothing; dead letters follow the same rule (one with no Request belongs to no tenant); the backfill changes nothing a Phase 4 reader reads, and with every `das:tenant:*` key removed (a rollback) a Phase 4 read gets the same answers. The Phase 4 data is the Phase 5 scripts' output with everything Phase 5 added removed, as in the backfill tests, not the Phase 4 scripts themselves: running those needs the Phase 4 commit's Lua, which a shallow CI checkout does not have.
- **Fault injection:** 29 faults over the header, the rejection reasons, the schema, the error table and the upgrade; 28 were caught at first. The survivor was a gap in the new upgrade test (no dead-letter check, so a backfill that skipped them passed); a test was added and all 29 are caught.

**Wave 8 (tasks 6.3, 8.3): done, uncommitted at the time of writing.** No `das/` code changed: the wave is two tests and a fixture.
- **Property 8 (6.3):** `tests/property/test_priority_header.py`. Manager half: a Request of any priority (absent, `normal`, `high`) is run through failures and retries (attempt limit 1 to 3, first 0 to 3 attempts failing), an optional takeover (the first dispatch is never answered; another Manager re-dispatches) and an optional Replay from a dead letter filed with the Job's real last attempt. Every Job dispatch on the wire carries the header if and only if the priority is `high`, never any other value, never in the payload or its wire body, and the decoded payload has exactly the fields of `JobDispatchPayload`. Adapter half: for any header mapping, `high` gives AMQP priority 9 and everything else 0, the reserved header never reaches the wire, other headers are kept, and the body bytes do not depend on the header. The AMQP value is stated as `9` in the test and not read from the adapter's constant (a first version compared the adapter with itself and a mutant that changed the constant survived). The scenarios use a short retry backoff: at the machine's default of 2 s and 4 s the property took 167 s, with the backoff 19 s.
- **Default_Tenant compatibility (8.3):** `tests/unit/test_default_tenant_compat.py`. (1) The codec corpus is pinned by SHA-256 to the Phase 4 file (`a1e51e5d...`), and `test_codec_baseline_bytes.py` already replays every message through the current codec; together, the wire format did not change. (2) `tests/fixtures/phase4_contract.json` is a verbatim copy of the Phase 4 contract baseline (`git show 039d0bc:...`; copied with the shell, not retyped, because it is 55 KB), and the current baseline must contain all of it: no route, metric, label, reason, environment variable, exit code, configuration field or default removed or changed, and no new `required` field in an existing section; the checker has its own test that it notices each kind of loss. (3) With no tenants and the Tenant_Backfill flag unset, on Phase 4-shaped data, the Phase 4 flows answer what Phase 4 answered: submit, idempotent replay and conflict, the Request view (the Phase 4 keys plus `priority`), the listing and its item shape, cancel and its 409, the 404s, the 400s, 401, the per-key quota body (no `limit_scope`), the Admin_API_Key on every route, and the 403 of a normal key on an admin route (Phase 4's body plus `scope`). `tests/contract` (the Python client against the real Gateway) is unchanged since Phase 4 and passes.
- **Known difference from Phase 4 (Req 9.5):** a normal key calling an admin route now gets `{"error": "forbidden", "scope": "admin"}`, where Phase 4 sent `{"error": "forbidden"}`. It is an added field, by design (Req 2.3), and the only difference a Default_Tenant client can observe on the routes it already used, besides `priority` in the Request view; the test pins it as exactly that.
- **Fault injection:** Property 8, 10 faults (header never, always, first attempt only, wrong value, not adopted; adapter inverted, any value high, header left on the wire, wrong AMQP value, normal given a priority): 9 caught at first, the survivor above fixed, 10 of 10. Compatibility, 9 faults: 8 caught; the survivor is an equivalent mutant (with no tenants an Admin key and every request are in the default tenant, so the Operator exemption changes nothing; Property 1 covers it with tenants).

**Wave 9 (task 9, checkpoint): done, uncommitted at the time of writing.** Parent tasks 1 to 8, 10 and 11 are ticked: every child of each is done.
- **Full regression** on the tree of commit `74d3de0`, with `DAS_REQUIRE_DOCKER=1`: 3490 passed, 18 skipped, none failed. The kind suite, opt-in, run separately on rebuilt images: 10 passed. The Python client package (isolated environment, Python 3.12, the README command): 505 passed. `ruff`, `mypy` (119 files) and the import-linter contract are clean.
- **The 18 skips:** 9 are the kind tests, which are the opt-in run above; 1 checks only when `CI=true`; **6 are the observability e2e tests, skipped because a `node` process that is not part of this repository holds 127.0.0.1:3000, a port the observability profile publishes and cannot move (Req 18).** They passed in wave 7 (21 e2e tests, none skipped); `das/` has not changed since, only tests, a fixture and this plan. They should be run again once the port is free: `DAS_REQUIRE_DOCKER=1 uv run pytest tests/integration/e2e/test_observability_e2e.py`.
- **Not exercised anywhere yet:** the `kubeconform -strict` step of the CI fast job (the tool is not installed here and running a downloaded binary was refused), `actionlint`, and the GitHub Actions jobs themselves. The chart is covered by `helm lint` and `helm template` in the unit tests and by the kind suite.

