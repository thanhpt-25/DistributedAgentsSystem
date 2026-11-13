# Implementation Plan: DAS Phase 6, Smarter Agents

Each task cites the requirements it implements. A test is written first and seen failing. Logic that matters gets a
fault-injection pass. One commit per wave.

## Tasks

- [x] 1. Structured output (Wave 1)
  - [x] 1.1 Add `jsonschema` (pinned) and `das/structured/` (`check_schema`, `parse_and_validate`, canonical JSON) with unit tests (Req 1.1, 1.4, 1.7, 1.8)
  - [x] 1.2 `output_schema` on `PlannedJob` and `JobDispatchPayload`; the codec omits it when unset; the check `invalid_output_schema` in plan validation; the Planner prompt (Req 1.1 to 1.3)
  - [x] 1.3 The executor's validation and single corrective completion, usage summed, `output_schema_violation` (Req 1.5, 1.6)
  - [x] 1.4 Property 1: validation agrees with `jsonschema` (Req 8.1)
- [x] 2. Models and fallback (Wave 2)
  - [x] 2.1 `AuthLLMError`; `error_for_status` returns it for 401 and 403 (Req 2.4)
  - [x] 2.2 `capability_models`, `fallback_model` in `LLMConfig`, with range checks (Req 2.1, 2.8)
  - [x] 2.3 `LLMProvider.complete(model=...)`, the fallback, `Completion.model`, the executor's model choice and `x-llm-model` (Req 2.2, 2.3, 2.5, 2.7)
  - [x] 2.4 `das_llm_fallbacks_total` (Req 2.6); Property 5 (Req 8.5)
- [x] 3. Replan (Wave 3)
  - [x] 3.1 `limits.max_replans`; the `replans` field of the Request view; the `request.replanned` Event type (Req 3.1, 3.6)
  - [x] 3.2 `replan.lua` and `StateStore.replan`, with script tests and a fault-injection pass (Req 3.4)
  - [x] 3.3 `Planner.replan` and the `keeps_succeeded` check; Property 2 (Req 3.3, 8.2)
  - [x] 3.4 The driver seam in `_finalize`: conditions, fallback to the original failure, budget (Req 3.2, 3.5, 3.7, 3.8)
- [x] 4. Plan evaluation (Wave 4)
  - [x] 4.1 `das/eval/plans.py`: dataset loading, pure scoring, report (Req 4.2 to 4.5)
  - [x] 4.2 `das eval-plans` command and exit codes (Req 4.1, 4.4)
- [x] 5. Tool use (Wave 5)
  - [x] 5.1 Tool types; `complete_with_tools_once` in the four adapters and `LLMProvider.complete_with_tools` (Req 5.1, 5.2)
  - [x] 5.2 Tool registry, `tools` configuration, Capability advertisement, startup refusal (Req 5.3, 5.4, 5.1)
  - [x] 5.3 `calculator` with Property 3 and fault injection (Req 5.8, 8.3)
  - [x] 5.4 `http_fetch` with Property 4 and fault injection (Req 5.9, 8.4); `current_time` (Req 5.10)
  - [x] 5.5 The tool loop in the executor; `das_tool_calls_total`; logging rules (Req 5.5 to 5.7, 5.11, 5.12)
- [x] 6. Sandboxed code execution (Wave 6)
  - [x] 6.1 `Limits` with ceilings; `build_command` (pure) tested for every flag (Req 6.4, 6.5)
  - [x] 6.2 `SandboxRunner`: timeout kill, cleanup in every path, output cap, no code in logs (Req 6.6, 6.7)
  - [x] 6.3 `sandbox` configuration, `code-execution` Capability, startup check, `run_python` tool, `das_sandbox_runs_total` (Req 6.1 to 6.3, 6.9)
  - [x] 6.4 `docs/security/code-execution.md`; a static test that no Compose or Helm file mounts the Docker socket or is privileged (Req 6.8)
- [ ] 7. Documentation, compatibility and end-to-end (Wave 7)
  - [ ] 7.1 Regenerate the contract baseline (additive only, corpus byte-identical); a compatibility test with no new configuration (Req 7.1, 7.2)
  - [ ] 7.2 README sections and drift tests; a schema test that every new field has a documented default and range (Req 7.3, 7.4)
  - [ ] 7.3 E2E on the Compose stack: structured output, fallback, replan, a tool call (Req 1 to 3, 5)
  - [ ] 7.4 Final checkpoint: the whole suite, ruff, mypy, lint-imports
