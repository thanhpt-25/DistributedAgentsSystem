# DAS with a local Gemma model: one request, then three at once

**A measurement on one machine, not a guarantee.** We ran the Docker Compose stack (Gateway, 2 Managers, 2 Workers, Redis,
RabbitMQ) against a real local model, `gemma4:e4b`, first with one request and then with three submitted at the same
moment. Everything below comes from those runs; the raw data is in [`gemma-local/`](gemma-local/).

## Headline

- A multi-part prompt (research comparison, code review of a buggy snippet, summary memo) was planned by the model into
  3 Jobs and finished correctly: **1 request in 248 s, 0 failures**.
- **3 concurrent requests finished in 805 s, 0 failures, 0 retries.** That is about the same as running them one after
  another (3 x 248 = 744 s). The single local model, not DAS, was the bottleneck: the Gateway took each submit in
  25 to 28 ms.
- The model did the work well, with one visible flaw: it invented a date (see [What the output looked like](#what-the-output-looked-like)).

## The context of the runs

| | |
|---|---|
| Machine | 10 CPUs (arm64), 16 GiB memory, macOS (Apple silicon), Docker Desktop with an 8 GB VM |
| DAS | version 0.1.0, commit `1c0d9e0` |
| Stack | `deploy/docker-compose.yml`: 1 Gateway, 2 Managers, 2 Workers (`research,summarize` and `code-review`), Redis, RabbitMQ |
| LLM | `gemma4:e4b` in **host** Ollama 0.40.16 (8.0B parameters, Q4_K_M, 9.6 GB on disk, reported as 3.6 GB loaded, 65,536 context), reached from the containers at `host.docker.internal:11434`. Every Manager and Worker used it. |
| Changed settings | LLM call timeout 300 s (default 60 s), job timeout 600 s (default 300 s) |
| Prompt | [`gemma-local/prompt.txt`](gemma-local/prompt.txt), the same one in every run |

Why the model is on the host and not in a container: we assumed a 9.6 GB model would not fit in the 8 GB Docker VM, and
host Ollama also uses the GPU (Metal). Ollama later reported the loaded model at about 3.6 GB, so a container may have
fitted it, CPU-only and slower. We did not try.

## Run 0: plumbing check with a stub LLM

The repo's own end-to-end test (`tests/integration/e2e/test_compose_e2e.py`, tests `stack_ready` and `prompt_to_final`)
uses a fake Ollama-compatible server, so it measures DAS and not a model.

| Metric | Measured | Test limit |
|---|---|---|
| Stack ready (all agents registered) | 9.85 s | 120 s |
| Submit | 1.8 ms | 500 ms |
| Query, median (max) | 4.7 ms (6.5 ms) | 500 ms |

Both tests passed in 22 s. The prompt was planned into 3 Jobs (`j1`, `j2`, and `j3` which depends on both) and all
succeeded.

## Run 1: one request

| Time | What happened |
|---|---|
| 0 s | Request accepted (`Pending`) |
| 36 s | Manager's plan arrived: 3 Jobs, two dispatched in parallel, the memo Job waiting on both |
| 94 s | `compare_consensus` Succeeded |
| 157 s | `review_python_concurrency` Succeeded, memo Job dispatched |
| 203 s | memo Job Succeeded |
| **248 s** | **Request `Completed`** |

| Job | Tokens in / out |
|---|---|
| `compare_consensus` | 102 / 1,915 |
| `review_python_concurrency` | 186 / 2,171 |
| `generate_recommendation_memo` | 2,430 / 1,315 |

Times come from polling every 3 s, so read them as plus or minus 3 s. Data: [`single-request.json`](gemma-local/single-request.json).

## Run 2: three concurrent requests

The same prompt, submitted 3 times in parallel. All three: `Completed`, no failed Jobs, no replans.

| | req0 | req1 | req2 |
|---|---|---|---|
| Submit latency | 26 ms | 25 ms | 28 ms |
| Plan arrived, Jobs dispatched | ~30 s | ~64 s | ~100 s |
| **Completed** | **654 s** | **802 s** | **763 s** |
| Slowdown vs. the single run (248 s) | 2.6x | 3.2x | 3.1x |
| Tokens in / out, whole request | 6,925 / 7,768 | 7,978 / 8,677 | 6,707 / 7,307 |

Totals: 21,610 tokens in, 23,752 tokens out, 45,362 in all, on one model (`gemma4:e4b`). Data:
[`concurrent-3.json`](gemma-local/concurrent-3.json).

Where the tokens go, for req0: planning 1,772, the three Jobs 8,246, and a final `summarize` call 4,675. That
`summarize` call is separate from the memo Job, so each request makes five LLM calls, not three. In the single run the
Request stayed `Running` for about 45 s after the last Job finished; this summarize call is the likely reason (about 1,385
output tokens at roughly 30 tokens per second), though we did not time it separately.

### What it shows

1. **No speedup from concurrency.** Output rate for the Jobs alone was about 21 tokens/s with 3 requests
   (16,672 tokens in 802 s) and about 22 with one (5,401 in 248 s). Three requests took about 8% longer than three runs
   in a row. Adding Managers or Workers will not help while one local model serves all of them.
2. **Planning queues up.** The three plans arrived 30, 64 and 100 s in, one after another, which fits one model
   serving the planning calls in sequence. We did not confirm how Ollama schedules them.
3. **Nothing broke under 3x contention.** No call hit the 300 s timeout. The captured logs (479 lines) hold no
   warnings, errors, retries or attempt numbers above 1.
4. **Plans are not deterministic.** All three requests got the same shape (two parallel Jobs, then a memo) but different
   Job names (`compare_consensus`, `research_consensus`, `review_python_code`, ...) and different summary lengths
   (3,705, 3,727 and 2,829 characters).

## What the output looked like

The run-1 summary is a memo. Its content was correct: it recommended Raft over Paxos for a small team (citing etcd and
Consul), found both bugs in the snippet (the lost update on `counter += 1` and the check-then-act race on `cache`) and fixed
them with a `threading.Lock`, then ended with three action items.

Its header was wrong. The model wrote `DATE: October 26, 2023` and `FROM: Worker Agent`. The run was on 2026-11-15.
Nothing in `das/worker`, `das/llm`, `das/manager` or `das/plan` passes the current date to the model (we searched), so
a model that writes a date will guess one. That is a platform gap, not a model bug.

## Reproduce

Needs Docker with Compose v2.30+, Ollama on the host with the model pulled (`ollama pull gemma4:e4b`), and the three DAS
images (`docker compose -f deploy/docker-compose.yml build`).

```bash
# Warm the model once (the first load took about 7 s here)
curl -s localhost:11434/api/chat -d '{"model":"gemma4:e4b","stream":false,"keep_alive":"40m","messages":[{"role":"user","content":"ready?"}]}' >/dev/null

# Start the stack against it (separate Compose project, nothing else is touched)
G=docs/benchmarks/gemma-local
docker compose --project-directory deploy -p das-gemma --env-file $G/gemma.env.example \
  -f deploy/docker-compose.yml -f $G/docker-compose.gemma.yml up -d --no-build

# N concurrent requests: python script OUT.json N PROMPT_FILE (N=1 is the single-request case)
uv run python $G/run_concurrent.py out.json 3 $G/prompt.txt

# Tear down and unload the model
docker compose --project-directory deploy -p das-gemma --env-file $G/gemma.env.example \
  -f deploy/docker-compose.yml -f $G/docker-compose.gemma.yml down -v
curl -s localhost:11434/api/generate -d '{"model":"gemma4:e4b","keep_alive":0}' >/dev/null
```

`single-request.json` was produced by an earlier one-request variant of `run_concurrent.py`; the flow is the same.

## Limits of this data

- One machine, one model, one run per configuration. Timings are from 3 s polling.
- Three requests is the only concurrency level tried.
- `estimated_cost` was `None` because no price was configured for the model.
- We checked token usage for all three requests, but did not compare the three outputs for quality.

## Where you can help

These are open questions this run raised, not committed plans. Each one is small enough to try in an afternoon.

- **Find out why concurrency gave no speedup.** Try `OLLAMA_NUM_PARALLEL` on the host, or point the same stack at a hosted
  provider, and see where the curve bends. A hosted model would show whether DAS itself scales when the model is not
  the bottleneck.
- **Bound the LLM calls in flight.** Managers and Workers can issue many calls at once, and we did not find a cap on
  concurrent calls per provider in `das/`. A limit or a queue-aware planner might keep a small local model from being
  flooded.
- **Give the agents the current date** so memos stop inventing one. Small, testable, and the fix lives in the prompt
  assembly.
- **Investigate the HTTP transport's ceiling.** The load-test reference run in [`../capacity/`](../capacity/README.md)
  kept inside its limits up to 10 requests per second and timed out above that; the README says why has not been
  investigated.
- **Measure plan variance.** Run the same prompt 20 times and see how often the plan shape or Job count changes, with and
  without the Phase 6 plan evaluation.
- **Try the model inside a container** with Docker's memory raised, and report the speed.
- **Turn this into an opt-in test** next to the stub-LLM e2e tests, behind an environment flag, so a change that slows
  local-model runs is visible.

Found a number here you cannot reproduce? Open an issue with your machine, model and the `--out` JSON. That is useful
data too.
