# Requirements Document: DAS Phase 6, Smarter Agents

## Introduction

Phases 1 to 5 made DAS a multi-tenant platform in which a Worker does exactly one LLM completion per Job. Phase 6 lets a
Worker do more, safely: a Job can demand structured output, a Worker can use tools, a code worker can run code in a
sandbox, models can differ by Capability and fall back on a permanent provider error, and the Manager can re-plan when
Jobs fail. The plan is `docs/superpowers/plans/2026-11-13-das-phase6-7-smarter-and-resilient.md`; its decisions D1 and
D4 to D7 are assumed here.

Everything is additive and off unless configured. A Request that uses none of it behaves as in Phase 5, and the wire
format baselines (`tests/fixtures/phase3_baseline/codec_corpus.jsonl`) stay byte-identical: every new message field is
absent when unset.

Phase 6 supersedes two statements of earlier specs: v2 Req 20.5 ("a Worker makes exactly one completion per attempt and
sends no tool definition and executes no tool call") for Jobs that require a tool Capability or `code-execution`, and the
v1 "no code execution" rule for the Code_Worker alone. Nothing else is superseded.

## Glossary

- **Output_Schema**: a JSON Schema (draft 2020-12) object attached to a planned Job.
- **Structured_Job**: a Job with an Output_Schema.
- **Capability_Model**: a model name configured for one Capability (`llm.capability_models`).
- **Fallback_Model**: the model tried once after a permanent provider error (`llm.fallback_model`).
- **Replan**: replacing the unfinished part of a Request's plan with a new plan, keeping every Succeeded Job.
- **Tool**: a function the model may call during a Job: `calculator`, `http_fetch`, `current_time`.
- **Tool_Capability**: `tool-<name>` with the tool's name in hyphen case, advertised by a Worker that enables the tool.
- **Sandbox**: the container in which the Code_Worker runs one code snippet.
- **Code_Worker**: a Worker whose Capabilities include `code-execution`.
- **Plan_Evaluation**: running the Planner over a dataset of prompts and scoring the plans.

## Requirements

### Requirement 1: Structured Output

**User Story:** As a client, I want a Job's result to be JSON that matches a schema, so that I can consume it without parsing prose.

#### Acceptance Criteria

1. A planned Job MAY carry `output_schema`, a JSON Schema object. Job_Plan validation SHALL reject a plan with an `output_schema` that is not a valid JSON Schema, is larger than 16 KiB serialized, or is nested deeper than 16 levels, with a new check `invalid_output_schema` naming the Job.
2. The Planner prompt SHALL tell the model it may add `output_schema` to a Job and SHALL accept a plan without it.
3. The Job dispatch SHALL carry `output_schema` only when the planned Job has one. A dispatch without one SHALL encode byte for byte as in Phase 5.
4. WHEN a Worker executes a Structured_Job it SHALL parse the completion as JSON (accepting one Markdown code fence around it) and validate it against the Output_Schema.
5. WHEN the completion is not valid JSON or does not match, THE Worker SHALL make exactly one corrective completion in the same attempt, naming the first violation (its JSON path and message, never the rejected content), and validate that. Tokens of both completions SHALL be in the Job_Result usage.
6. WHEN the corrected completion is still invalid, THE Job_Result SHALL be `Failed` with the error `output_schema_violation: <path>: <message>`, and the Job SHALL follow the normal retry and dead-letter rules.
7. WHEN the completion is valid, THE Job_Result `content` SHALL be the canonical JSON text of the parsed value (sorted keys, no extra whitespace).
8. A completion longer than 256 KiB SHALL be a violation without being parsed.

### Requirement 2: Models by Capability, and a Fallback Model

**User Story:** As an operator, I want cheap models for simple Capabilities and a safety net when a model is unavailable.

#### Acceptance Criteria

1. THE `llm` section SHALL accept `capability_models` (a map from Capability to model name) and `fallback_model` (a model name), both absent by default.
2. A Worker SHALL execute a Job with the Capability_Model of the Job's routing Capability when there is one, else with `llm.model`.
3. WHEN a completion fails with a permanent provider error that is not an authentication or authorization error, and a Fallback_Model is configured and differs from the model used, THE LLMProvider SHALL make exactly one further completion with the Fallback_Model.
4. An authentication or authorization error (HTTP 401 or 403) SHALL NOT trigger the fallback: it is a new `AuthLLMError`, a kind of `PermanentLLMError`.
5. THE Job_Result header `x-llm-model` and the UsageRecord SHALL name the model that produced the completion, the Fallback_Model when it was used, so cost estimates are priced by the model really used.
6. A new counter `das_llm_fallbacks_total{provider, model}` SHALL count each use of the fallback, `model` being the model that failed, bounded like the other `model` labels.
7. THE Planner and Summarizer SHALL use `llm.model` and the Fallback_Model, never a Capability_Model.
8. An empty model name in `capability_models` or `fallback_model` SHALL be a configuration error naming the field.

### Requirement 3: Replan

**User Story:** As a client, I want a Request that lost a Job to try a different plan before it fails.

#### Acceptance Criteria

1. THE `limits` section SHALL accept `max_replans`, 0 to 3, default 0. At 0 nothing in this requirement applies.
2. WHEN the Manager would finish a Request as Failed because Jobs failed (not because of a budget stop, a cancellation or a summarization failure), and the Request has been replanned fewer than `max_replans` times, THE Manager SHALL replan instead.
3. THE Planner SHALL be given the prompt, the advertised Capabilities, the Succeeded Jobs with their results, and the failed Jobs with their errors, and SHALL return a Job_Plan that contains every Succeeded Job unchanged and replaces all other Jobs. A plan that drops or changes a Succeeded Job SHALL be rejected like any invalid plan, and the Planner's correction loop applies.
4. A new script `replan` SHALL, under the Lease and in one atomic write, store the new plan, keep the Succeeded Job records, write the new Jobs as Waiting with their transitions, append the planning Usage_Records, add 1 to the Request's `replans`, and append a `request.replanned` Event. The Request SHALL stay Running.
5. WHEN replanning itself fails (the LLM, a rejected plan, no capable Worker), THE Request SHALL fail as it would have without replanning, with its original error.
6. THE Request view SHALL carry `replans` (0 when never replanned). The new Event type `request.replanned` SHALL carry `replans`, `failed_jobs` and `new_jobs`. The Phase 4 terminal-event rules are unchanged.
7. Dead_Letter_Records of replaced Jobs SHALL stay; replaying one SHALL be refused as for any record whose Job is no longer Failed, and SHALL NOT change the new plan.
8. A budget SHALL still bound the whole Request: replanning tokens count towards it and a budget stop is never replanned.

### Requirement 4: Plan Evaluation

**User Story:** As a developer, I want to measure the Planner, so that a prompt or model change can be judged.

#### Acceptance Criteria

1. THE CLI SHALL offer `das eval-plans --config <yaml> --dataset <jsonl> [--min-pass <ratio>]`.
2. A dataset line SHALL be a JSON object with `prompt`, `capabilities` (the Capabilities the Planner is offered) and optional `expect`: `min_jobs`, `max_jobs`, `require_capabilities`, `forbid_capabilities`, `require_output_schema`.
3. FOR each line THE command SHALL run the real Planner with the configured provider and record whether a valid plan was produced, how many attempts it took, the tokens used, and whether every expectation held.
4. THE command SHALL print a JSON report (per-line results and the totals: valid ratio, expectation ratio, mean attempts, total tokens) and exit 0 when both ratios reach `--min-pass` (default 0), else 1. An invalid dataset line SHALL be a usage error (exit 2) naming its line number.
5. THE scoring SHALL be a pure function of the Planner's outcome and the expectations, tested without an LLM.

### Requirement 5: Tool Use

**User Story:** As a client, I want a Job to use a calculator or fetch a page, so that its answer rests on more than the model's memory.

#### Acceptance Criteria

1. THE `ProviderAdapter` protocol SHALL gain an optional operation for a completion with tools. A Worker that enables a tool with an adapter lacking it SHALL refuse to start with exit code 2 naming the provider.
2. THE four adapters (OpenAI, Anthropic, Ollama, Bedrock) SHALL implement it by mapping tool specifications, tool calls and tool results to the provider's own format, and SHALL return tool calls as data (`ToolCall`: id, name, arguments as a JSON object).
3. A Worker SHALL accept `tools`: `enabled` (tool names), `max_rounds` (1 to 10, default 4), `max_result_bytes` (default 16 KiB) and per-tool settings. An unknown tool name SHALL be a configuration error.
4. A Worker SHALL advertise `tool-<name>` for each enabled tool, besides its own Capabilities, in the Agent_Registry.
5. WHEN a Job requires `tool-<name>` Capabilities, THE executor SHALL offer the model exactly those tools. A Job that requires none SHALL run exactly as in Phase 5.
6. THE executor SHALL loop: complete, execute each requested tool, append the results, complete again, until the model returns text or `max_rounds` rounds have been used (then the Job fails `tool_rounds_exceeded`). Tokens of every round SHALL be in the Job_Result usage. The Job timeout and a cancel SHALL stop the loop between and during rounds.
7. A tool error (a bad argument, a failure, a timeout) SHALL be returned to the model as the tool result text, never raised, and results SHALL be capped at `max_result_bytes`.
8. THE `calculator` tool SHALL evaluate arithmetic with a safe parser (numbers, `+ - * / // % **`, parentheses, unary minus, a few math functions), never `eval`, with limits on length, exponent size and result size.
9. THE `http_fetch` tool SHALL use GET only, https only (http only when `allow_http` is set), only hosts of the configured `allowed_hosts` (at least one is required to enable the tool), no redirects, a timeout and a response size cap, and SHALL refuse a host that resolves to a private, loopback, link-local or reserved address.
10. THE `current_time` tool SHALL return the current UTC time from the injected Clock.
11. Tool names and a result's size MAY be logged; tool arguments and results SHALL NOT be logged, and a secret SHALL never be in a tool result.
12. THE metrics SHALL gain `das_tool_calls_total{tool, outcome}` (`ok`, `error`, `timeout`) with `tool` bounded by the tool names.

### Requirement 6: Sandboxed Code Execution

**User Story:** As a client, I want a Job that can run code, without the platform being at risk.

#### Acceptance Criteria

1. THE Code_Worker SHALL be an ordinary Worker with the Capability `code-execution` and a `sandbox` section. `sandbox.enabled` SHALL default to false; with it false a Worker SHALL NOT advertise `code-execution` and SHALL refuse to start with that Capability configured.
2. WHEN enabled, THE Worker SHALL check at startup that the container runtime answers, and fail with exit code 4 naming it otherwise.
3. THE model returns code through the one tool `run_python`; the Sandbox runs it; the output goes back to the model, using the tool loop of Requirement 5.
4. EVERY run SHALL use these and no weaker settings, none of which a configuration can loosen: no network (`--network none`), a read-only root file system, a writable scratch directory of bounded size only, all capabilities dropped, `no-new-privileges`, a non-root user, no host mounts, no devices, a process limit, a memory limit, a CPU limit, a wall-clock timeout that kills the container, and a cap on the bytes of output kept. Configurable values SHALL have ceilings (timeout 60 s, memory 1 GiB, CPUs 2, processes 256, output 64 KiB).
5. THE runtime MAY be an alternative OCI runtime (for example `runsc`); it SHALL NOT be configurable to run privileged, with the host network, or with the Docker socket.
6. A run SHALL always remove its container, also on a timeout or an error; a failure to do so SHALL be logged with the container name.
7. A run's code, stdout and stderr SHALL NOT be logged; its exit code, duration and output sizes SHALL be.
8. THE Compose and Helm files of the repository SHALL NOT mount the Docker socket or run a privileged container. THE document `docs/security/code-execution.md` SHALL state the threat model, every limit above, what the sandbox does not protect against, and how to run a Code_Worker on a dedicated host.
9. THE metrics SHALL gain `das_sandbox_runs_total{outcome}` (`ok`, `error`, `timeout`, `killed`).

### Requirement 7: Compatibility and Documentation

#### Acceptance Criteria

1. WITH none of the new configuration set, every response, message and metric of Phase 5 SHALL be unchanged, apart from the Request view field `replans` (0) and the additions to the OpenAPI document.
2. THE contract baseline SHALL change only by additions; `codec_corpus.jsonl` SHALL stay byte-identical.
3. THE README SHALL document structured output, models and the fallback, replanning, plan evaluation, tools and the code worker (with the security warning), each with its configuration; the README drift tests SHALL be extended.
4. EVERY new configuration field SHALL have a documented default and range, checked by a test against the schema.

### Requirement 8: Correctness Properties

#### Acceptance Criteria

1. FOR ALL schemas and values, THE Worker's validation SHALL accept a value if and only if it matches the schema (Property 1, against the `jsonschema` reference).
2. FOR ALL plans and replacement plans, a replan SHALL keep every Succeeded Job unchanged and the resulting plan SHALL be a valid Job_Plan (Property 2).
3. FOR ALL expressions, THE calculator SHALL return the value of a reference evaluation for the supported grammar and SHALL refuse everything else without raising (Property 3).
4. FOR ALL host and address inputs, `http_fetch` SHALL refuse every private, loopback, link-local and reserved address however it is written (Property 4).
5. FOR ALL sequences of model failures, the LLMProvider SHALL call the Fallback_Model at most once per completion and never after an authentication error (Property 5).
