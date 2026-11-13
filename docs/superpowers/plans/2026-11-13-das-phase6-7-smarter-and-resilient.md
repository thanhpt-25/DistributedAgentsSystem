# DAS Phase 6 and 7: smarter agents, then scale and resilience

Date: 2026-11-13. Inputs: `.kiro/specs/roadmap.md` (Phases 6 and 7), the Phase 5 code (commit `b70a294`), and the instruction
"finish until phase 7". Output: two specs, `.kiro/specs/das-smarter-agents/` (Phase 6) and
`.kiro/specs/das-scale-resilience/` (Phase 7), and the code and tests they describe, built in waves like Phase 5.

## What the roadmap asks for

| Phase | Item | Size |
|---|---|---|
| 6 | Tool use in workers (function calling through `ProviderAdapter`, tools advertised as Capabilities) | L |
| 6 | Sandboxed code execution as a separate worker type | L |
| 6 | Structured output: a JSON schema per Job, validated before the Job succeeds | M |
| 6 | Per-capability model selection, and a fallback model on permanent provider errors | M |
| 6 | Planner quality loop: replan on Job failure, and a plan evaluation set | M |
| 7 | Redis HA (Sentinel or Cluster), hash-tag keys, migration | L |
| 7 | HTTP transport adapter (v1 Req 15.1) | M |
| 7 | Kafka transport adapter (v1 Req 15.2), with its own retry/DLQ design | L |
| 7 | Load test suite and published capacity numbers | M |

The roadmap's three questions to the product owner (first users, an uptime target, whether code execution is wanted at
all) were never answered. The decisions below are the safe defaults that let the work go on; each is easy to reverse.

## Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Code execution ships, off by default, behind an explicit opt-in.** `sandbox.enabled` is false; the runner always uses `--network none`, a read-only root, no capabilities, a non-root user and hard CPU, memory, process, time and output limits, none of which is configurable upwards past a ceiling. The Compose files do not mount the Docker socket anywhere. A security document (`docs/security/code-execution.md`) is part of the spec. | The roadmap says this needs a security spec before code; a worker that can run arbitrary code is the riskiest thing in the product, so it is the one feature that is inert until an operator turns it on. |
| D2 | **Redis HA is Sentinel, not Cluster.** `redis.mode: sentinel` with sentinel addresses and a master name; the key layout does not change, so there is no migration. Cluster is refused at startup with a message that says why. | A state script touches a Request's keys and the global indexes (`das:requests:index`, the by-status sets, the tenant twins) in one atomic call. In Cluster those keys live in different slots, so the scripts cannot run. Hash-tagging by Request ID cannot put a global index in the same slot as every Request. Sentinel gives automatic failover with no change to any script. |
| D3 | **The HTTP transport is a small durable broker served on the Gateway's internal Ops_Port, backed by Redis Streams, with a token.** Agents are its clients (long-poll pull, ack, requeue, reject). | A transport needs durable queues with competing consumers, which plain request/response HTTP does not give. Redis is already a hard dependency, so the broker adds no new infrastructure; the Ops_Port is already internal and unpublished. |
| D4 | **Replan is a Request-level swap of the remaining work, in the driver, not in the reducer.** When a Request would fail because Jobs failed, and `limits.max_replans` allows, the Manager asks the Planner for a plan that keeps every Succeeded Job and replaces the rest, and a new atomic script `replan.lua` swaps it in under the Lease. The Request stays Running. `max_replans` defaults to 0, so nothing changes until it is turned on. | The reducer and its property tests do not change; the new behaviour has one seam (`_finalize`) and one script. |
| D5 | **Structured output is validated by the Worker, with one corrective re-ask, and the schema travels as an optional field of the dispatch.** The field is absent when unset, so every existing message stays byte-identical. | Same pattern as the Planner's correction loop; the codec baseline must not move. |
| D6 | **Tools are built in and opt-in.** A Worker advertises `tool-<name>` Capabilities for the tools it enables; the Planner assigns them like any Capability; the executor offers the model only the tools its Job requires. `http_fetch` cannot be enabled without an allowlist. | Reuses Capability routing; no new registry concept; network egress is an explicit, reviewable list. |
| D7 | **New runtime dependencies, pinned:** `jsonschema` (structured output) and `aiokafka` (Kafka transport). | Phase 5's "no new runtime dependency" rule was for that phase. |

## Waves

Same working rules as Phase 5: a test first and seen failing, a fault-injection pass on the logic that matters, `/verify`
before each commit, one commit per wave, nothing pushed to `main`, the contract baselines regenerated deliberately and
checked to be additive, and `codec_corpus.jsonl` byte-identical.

| Wave | Tasks |
|---|---|
| 0 | The two specs and this plan |
| 1 | Phase 6 foundations: `jsonschema` dependency, the schema field on a planned Job and the dispatch, plan validation, the Worker's validation and corrective re-ask |
| 2 | Per-capability models, the fallback model, the auth error class, its metric |
| 3 | `replan.lua`, the driver seam, the Planner's failure context, the `request.replanned` event, `limits.max_replans` |
| 4 | Plan evaluation: dataset, `das eval-plans`, metrics |
| 5 | Tool types, adapter mappings (OpenAI, Anthropic, Ollama, Bedrock), the tool registry and its built-in tools, the executor's tool loop |
| 6 | The sandbox runner, the code worker, the security document |
| 7 | Phase 6 README, e2e on the Compose stack, full regression |
| 8 | Phase 7: Redis Sentinel client and configuration, cluster refusal |
| 9 | Phase 7: the HTTP transport broker and adapter |
| 10 | Phase 7: the Kafka transport |
| 11 | Phase 7: Sentinel failover e2e, Helm values |
| 12 | Load test suite, measured capacity numbers |
| 13 | Phase 7 README, full regression, final checkpoint, a CI run on a branch |

## Risks

- **Tool use and code execution widen the attack surface**; every limit is on by default and the dangerous parts are off.
- **Kafka and Sentinel need real infrastructure to test.** Unit tests use fakes; integration and e2e tests run the real
  thing under Docker and skip without it, as the existing integration tests do.
- **Capacity numbers are only as good as the machine that produced them.** The published numbers say what they ran on.
