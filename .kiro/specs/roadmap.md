# DAS Product Roadmap

Status as of 2026-10-29. Each phase below should get its own spec folder under `.kiro/specs/` (requirements, design, tasks), like Phases 1–3.

## Where we are

| Phase | Spec | Status |
|---|---|---|
| 1. Core platform | `distributed-agent-system` | Done (105 tasks) |
| 2. Observability & operations | `das-observability-operations` | Done (104 tasks) |
| 3. Hardening & CI | `das-hardening-ci` | Done (47 tasks) |

Verified 2026-10-29: 1,978 fast tests pass, 24 integration/e2e tests pass (twice in a row), import-linter contract kept.

What exists today: prompt → Job DAG → capability-routed workers → summarized Final_Result; lease takeover between Managers; retries and DLQ with admin replay/discard; Prometheus metrics, OpenTelemetry tracing, Grafana dashboards; token usage and cost estimation; retention and request listing; liveness/readiness and drain; ruff, mypy, GitHub Actions workflow.

Gaps found in the code (none of these exist today):

- The workspace is not a git repository, so `.github/workflows/ci.yml` has never run.
- Clients can only poll `GET /v1/requests/{id}`: no streaming, no webhooks.
- No idempotency key on `POST /v1/requests`; a client retry after a timeout creates a duplicate Request.
- No rate limit or quota per API key (`das/observability/ratelimit.py` only throttles log lines).
- Cost is measured after the fact but there is no per-Request token or cost cap.
- API keys are flat: no tenants, projects, or scoped permissions.
- Workers are LLM-only: no tool use, no code execution.
- Docker Compose is the only deployment target.
- Single Redis primary; HTTP and Kafka transports deferred (v1 Req 15).

## Phase 0: Put the baseline under version control (days)

Prerequisite for everything else.

- `git init`, first commit of the green baseline, push to GitHub.
- Confirm `ci.yml` passes on GitHub runners (fast job on every push; integration and e2e on `main`).
- Branch protection on `main` requiring the fast job.

## Phase 4: Production-ready client API (Now)

Goal: an application team can integrate DAS without polling loops or duplicate work, and one client cannot starve the others.

| Item | Why | Size |
|---|---|---|
| `Idempotency-Key` header on submit | Safe client retries; maps to existing at-least-once design | S |
| Server-Sent Events: `GET /v1/requests/{id}/events` (job transitions, final result) | Replaces polling; transitions are already logged per Request | M |
| Webhooks on terminal status, signed with HMAC, retried with backoff | Async integrations without an open connection | M |
| Per-key rate limits and concurrent-Request quotas at the Gateway (Redis-backed, `429` + `Retry-After`) | Protects the shared worker pool | M |
| Per-Request budget: `max_total_tokens` / `max_cost`; Manager stops dispatching and fails or cancels when exceeded | Cost control; builds on Phase 2 usage accounting | M |
| Python client SDK (sync + async) and an OpenAPI-published contract | Lowers integration effort | S |

Risks: SSE across Gateway replicas needs a fan-out source (Redis stream per Request already exists as the inbox; a separate events stream is cleaner). Budget enforcement must go through the reducer and Lua fencing like every other state change.

## Phase 5: Multi-tenant and deployable (Next)

Goal: one DAS installation serves several teams, and it runs outside Docker Compose.

| Item | Why | Size |
|---|---|---|
| Tenants/projects: API keys belong to a tenant; Requests, listings, DLQ and usage are scoped to it | Isolation between teams | L |
| Scoped keys (submit, read, admin) replacing the flat user/admin split | Least privilege | M |
| Usage and cost reports per tenant (API + Grafana panel) | Chargeback; reuses Phase 2 metrics | S |
| Helm chart / Kubernetes manifests with readiness, drain on `preStop`, HPA on queue depth | Real deployments | M |
| Request priority (per tenant or per Request) via RabbitMQ priority or separate queues | Interactive vs batch traffic | M |

Risks: tenant ID must enter Redis keys, which overlaps with the key-layout change Redis HA needs. Design both key layouts together to migrate once.

## Phase 6: Smarter agents (Later)

Goal: workers do more than one LLM completion.

| Item | Why | Size |
|---|---|---|
| Tool use in workers (function calling through `ProviderAdapter`, tool registry advertised as Capabilities) | Retrieval, APIs, calculations | L |
| Sandboxed code execution as a separate worker type (container/gVisor, no network by default) | Data analysis tasks; deferred from v1 | L |
| Structured output: JSON schema per Job, validated before the Job succeeds | Reliable downstream consumption | M |
| Per-capability model selection and fallback model on permanent provider errors | Cost/quality trade-off | M |
| Planner quality loop: replan on Job failure, plan evaluation set | Better results, measurable | M |

Risks: tool use and code execution need a new security spec (egress, secrets, resource limits) before implementation.

## Phase 7: Scale and resilience (Later, when load demands it)

**Status: implemented** as `das-scale-resilience`. Redis HA took the Sentinel route: the key layout did not change, so there is nothing to migrate, and Redis Cluster is refused with the reason (the State_Store scripts update keys in different slots at once). The HTTP and Kafka transports and the load test suite are in, and `docs/capacity/` holds one reference run (a measurement on one machine, not a guarantee). The table below is the original plan.

| Item | Why | Size |
|---|---|---|
| Redis HA (Sentinel or Cluster): hash-tag keys by Request ID, migrate existing data | Removes the single point of failure (OD 4.1) | L |
| HTTP transport adapter (v1 Req 15.1) | Environments without RabbitMQ | M |
| Kafka transport adapter (v1 Req 15.2), with its own retry/DLQ design | High-throughput deployments | L |
| Load test suite and published capacity numbers | Sizing guidance | M |

## Suggested order

1. Phase 0 now.
2. Phase 4 items in this order: idempotency key → rate limits/quotas → budgets → SSE → webhooks → SDK.
3. Decide the Redis key layout for tenants and HA together before starting Phase 5.
4. Phase 6 and 7 are driven by user demand: tool use if users ask for richer agents, Redis HA if uptime requirements appear.

## Open questions for the product owner

- Who are the first real users: internal teams (favours Phase 5 tenancy early) or a single application (favours Phase 4 first)?
- Is there an uptime target? That decides whether Redis HA moves ahead of Phase 6.
- Is code execution wanted at all, given its security cost?
