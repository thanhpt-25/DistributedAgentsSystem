# Design Document: DAS Phase 6, Smarter Agents

Requirement numbers refer to `requirements.md` in this folder. Everything here is additive: with no new configuration a
Phase 5 deployment behaves as before (Req 7.1).

## Overview

```text
Gateway ─ Request ─▶ Manager ── plan (+ output_schema per Job) ──▶ Redis
                       │  dispatch (+ output_schema when set; headers as before)
                       ▼
                     Worker: JobExecutor
                       ├─ model = capability_models[routing capability] or llm.model        (Req 2.2)
                       ├─ no tool Capability required → one completion, as Phase 5
                       ├─ tool Capabilities required  → ToolLoop (complete ⇄ tools)         (Req 5)
                       └─ output_schema set           → StructuredValidator (+1 re-ask)      (Req 1)
                     LLMProvider: retry → permanent error → Fallback_Model once              (Req 2.3)
Manager driver: Jobs failed and replans < max_replans → Planner.replan → replan.lua          (Req 3)
CLI: das eval-plans                                                                          (Req 4)
```

New modules: `das/structured/` (schema check and validator, pure), `das/tools/` (types, registry, calculator, http_fetch,
current_time, loop), `das/sandbox/` (runner, limits), `das/manager/replan.py`, `das/eval/plans.py`. Existing ones that
change: `das/models/plan.py`, `das/models/results.py`, `das/plan/validate.py`, `das/plan/codec.py`, `das/llm/*`,
`das/worker/executor.py`, `das/worker/worker.py`, `das/manager/{planner,driver}.py`, `das/config/schema.py`,
`das/state/{store.py,scripts.py,lua/replan.lua}`, `das/client/events.py`, `das/observability/metrics.py`, `das/cli.py`.

The import-linter contract is kept: `das.manager` and `das.worker` import no SDK. `das.sandbox` is the only package that
starts a subprocess; the worker reaches it through `das.tools`.

## Structured output (Req 1)

- `PlannedJob.output_schema: dict[str, Any] | None = None` and `JobDispatchPayload.output_schema` likewise. The codec
  omits a `None` field, so a dispatch without one is byte-identical (the codec corpus pins this).
- `das.structured.schema.check_schema(schema) -> str | None` returns an error text or `None`: size (16 KiB), depth
  (16), and `jsonschema.Draft202012Validator.check_schema`. `validate_plan` adds the check `invalid_output_schema`.
- `das.structured.validate.parse_and_validate(text, schema) -> Valid(value) | Violation(path, message)`: strips one code
  fence, parses, validates, reports the first violation by `absolute_path` order. A value is canonicalised with
  `json.dumps(sort_keys=True, separators=(",", ":"))`.
- `JobExecutor` wraps the completion: on `Violation` it appends the rejected reply and a correction message built only from
  the path and message, completes once more, sums the usage, and fails with `output_schema_violation: <path>: <message>`.

## Models and fallback (Req 2)

- `LLMConfig.capability_models: dict[Capability, str]`, `fallback_model: str | None`.
- `LLMProvider.complete(..., model: str | None = None)` selects the model for one call; `params` stays the default.
  The retry wrapper is unchanged; after it raises `PermanentLLMError` (not `AuthLLMError`) and a fallback exists and
  differs, `complete` runs the whole wrapper once more with the fallback and records it. `Completion` gains
  `model: str | None` so callers know which model answered.
- `error_for_status` returns `AuthLLMError` for 401 and 403. The four adapters already route through it.
- The Worker knows the routing Capability from the queue it consumed (`routing_capability`); the dispatch is unchanged.

## Replan (Req 3)

The reducer is unchanged. In `Driver._finalize`, a `FinalizeFailed` whose cause is "jobs failed" (not a budget stop, not a
cancel) and `replans < max_replans` calls `_replan()`:

1. Build the failure context: Succeeded Jobs (id, instruction, result excerpt capped at 2 KiB) and failed Jobs (id, error).
2. `Planner.replan(prompt, capabilities, context, config)` asks for a full plan, reusing the correction loop. A new check
   `keeps_succeeded` rejects a plan that drops or changes a Succeeded Job (compared with `normalize_plan`).
3. `store.replan(lease, plan, usages)` runs `replan.lua`; on `OK` the driver reloads the snapshot and re-enters the loop;
   on `LEASE_LOST` / `REQUEST_TERMINAL` it stops as `_plan` does.
4. Any failure in 2 or 3 falls back to the original `FinalizeFailed` with its original error (Req 3.5).

### `replan.lua` contract

```text
KEYS: request hash, lease hash, jobs hash, transitions list, usage list (as commit_plan)
ARGV: request_id, manager_id, epoch, plan JSON, planning UsageRecords JSON, request.replanned event JSON,
      then alternating job_id, JobRecord JSON for every NEW job
Returns {'OK', at} | {'LEASE_LOST'} | {'REQUEST_TERMINAL'} | {'INVALID_STATE', status}
```

It requires status `Running`. Before the first write it decodes every input. It deletes the records of Jobs not Succeeded
and not in the new plan, writes the new Jobs `Waiting` through `put_job`, sets `plan` and `job_count`, `HINCRBY replans 1`,
appends usage and the event, all in the one call. Succeeded records are untouched (Req 3.4).

## Plan evaluation (Req 4)

`das.eval.plans` has `load_dataset(path) -> list[Case]` (raises `DatasetError(line, reason)`), `score(case, outcome) ->
CaseScore` (pure), `summarize(scores) -> Report`, and `run(config, cases)` which calls the real Planner. The CLI prints
`Report` as JSON and maps `DatasetError` to exit 2.

## Tools (Req 5)

- `das.tools.types`: `ToolSpec(name, description, parameters)`, `ToolCall(id, name, arguments)`,
  `ToolResult(call_id, content, is_error)`, `ToolTurn(text, calls, usage)`.
- `ProviderAdapter` gets an optional `complete_with_tools_once(system, turns, tools, params, timeout) -> ToolTurn`;
  `LLMProvider.complete_with_tools` wraps it with the same retry and fallback. An adapter without it is detected by
  `hasattr`; the Worker refuses to start (exit 2).
- The registry maps a name to `Tool(spec, run)`; `run(arguments, ctx) -> str` may raise `ToolError`, which the loop turns
  into the result text. `ToolContext` carries the Clock and the per-tool settings.
- `ToolLoop.run(job, tools, cancel)` implements Req 5.6.
- `calculator`: parse with `ast`, walk only `Expression, BinOp, UnaryOp, Constant, Call` over a whitelist; reject names,
  attributes, comprehensions; caps: 200 characters, exponent at most 1000, result below 10^1000.
- `http_fetch`: a `Resolver` is injected; the host is checked against `allowed_hosts`, then every resolved address with
  `ipaddress` (private, loopback, link-local, reserved, multicast, unspecified, IPv4-mapped IPv6 unwrapped); the request
  goes to the checked address with the original `Host` header so a rebind cannot change it.

## Sandbox (Req 6)

`das.sandbox.runner.SandboxRunner(runtime, limits, spawn=asyncio.create_subprocess_exec)`. `build_command(code_path, limits)
-> list[str]` is pure and tested for every flag of Req 6.4. A run: create a scratch directory, write the code, start
`<runtime> run --rm --name das-sbx-<uuid> --network none --read-only --cap-drop ALL --security-opt no-new-privileges
--user 65534:65534 --pids-limit N --memory M --cpus C --tmpfs /scratch:rw,noexec,size=16m -v <code>:/code.py:ro <image>
python /code.py`, wait with `asyncio.timeout`, on timeout `<runtime> kill` then `rm -f`, read at most `output_limit` bytes,
always `rm -f` in `finally`. `Limits` clamps to the ceilings in its constructor, so a configuration cannot exceed them.
Forbidden values (`privileged`, `host` network, a socket path) have no field to set.

## Compatibility

New fields are absent when unset (the codec omits `None`; the Request view adds `replans`).
`tests/fixtures/phase3_baseline/contract.json` is regenerated and checked to differ only by additions.

## Test strategy

Properties 1 to 5 of Req 8 in `tests/property/`; fakes for the adapters' tool turns, the runtime subprocess and the
resolver; fault injection on the validator, the calculator, the address check and `replan.lua`; an e2e on the Compose stack
with the stub LLM extended to answer tool calls and schema prompts; the sandbox itself is tested end to end only when the
Docker daemon is up and skips otherwise.
