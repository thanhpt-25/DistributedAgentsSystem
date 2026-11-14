# Capacity: what one reference run measured

These numbers are **a measurement on one machine, not a guarantee.** They say how the Compose stack behaved on the machine
below, with a stub LLM, under one load profile. Your hardware, your LLM provider, your Worker count and your network will
give other numbers; run `das loadtest` against your own stack and keep its report.

The raw reports are in this directory: [`reference-rabbitmq.json`](reference-rabbitmq.json) and
[`reference-http.json`](reference-http.json). Each carries the context below inside it, so a number never travels without
it (Phase 7 Req 4.4).

## The context of the runs

| | |
|---|---|
| Machine | 10 CPUs (arm64), 16 GiB memory, macOS 27.2 (Apple silicon), Docker Desktop |
| DAS version | 0.1.0 |
| Stack | `deploy/docker-compose.yml` on that one machine: 2 Managers, 2 Workers (`research,summarize` and `code-review`, 4 concurrent Jobs each), Redis, and RabbitMQ (first run) or the HTTP transport (second run) |
| LLM | the stub (`tests/integration/e2e/llm_stub.py`): every answer takes 50 ms (`STUB_LATENCY_MS=50`), none fails |
| Load generator | `das loadtest` on the same machine, so it competes with the stack for CPU |
| Gateway limits | none: `gateway.loadtest.yaml` turns off the per-key rate limit and quotas, which would otherwise answer 429 above 60 requests per second |
| Profile | `ramp`: 2, 5, 10, 20 and 40 Requests per second, 30 s each (2310 Requests), a prompt mix of three, every Request planned into three Jobs |
| Limits for "coping" | error rate at most 1 % and p99 of the total time at most 10 s, per step |

## Reproduce

```bash
uv run python -m tests.integration.e2e.reference_run --out docs/capacity/reference-rabbitmq.json
uv run python -m tests.integration.e2e.reference_run --transport http --out docs/capacity/reference-http.json
uv run das loadtest-compare docs/capacity/reference-rabbitmq.json docs/capacity/reference-http.json --max-regression 0.1
```

The script builds the stack, runs `das loadtest --profile ramp`, writes the report and removes the stack. It needs Docker
and about five minutes. Against a stack of your own:
`das loadtest --gateway URL --key KEY --profile ramp --transport rabbitmq --out report.json --setting managers=2`.

## Results

### RabbitMQ transport

2310 Requests, 2310 completed, no errors. Throughput over the whole run (including the time to finish the last Requests):
12.8 completed Requests per second. **Saturation point: 20 Requests per second.**

| offered rate | requests | completed | errors | p99 total time |
|---:|---:|---:|---:|---:|
| 2 rps for 30 s | 60 | 60 | 0 | 2.1 s |
| 5 rps for 30 s | 150 | 150 | 0 | 2.5 s |
| 10 rps for 30 s | 300 | 300 | 0 | 2.5 s |
| 20 rps for 30 s | 600 | 600 | 0 | 2.5 s |
| 40 rps for 30 s | 1200 | 1200 | 0 | 30.4 s |

Over the run the total time (submit to terminal state) had p50 3.0 s, p90 27.3 s, p99 29.9 s. The Gateway's submit time
had p50 5 ms; its p90 and p99 were 14.6 s and 15.7 s, all of it in the last step: **past saturation the platform queues,
and the Gateway also becomes slow to accept**. Below it, every step stayed under about 2.5 s. The load generator sent every
Request within 1.1 s of its schedule.

### HTTP transport

2310 Requests, 1523 completed, 787 errors (all `submit_failed`: the Gateway did not answer a submit in time). Throughput
8.1 completed Requests per second. **Saturation point: 10 Requests per second.**

| offered rate | requests | completed | errors | p99 total time |
|---:|---:|---:|---:|---:|
| 2 rps for 30 s | 60 | 60 | 0 | 2.0 s |
| 5 rps for 30 s | 150 | 150 | 0 | 2.0 s |
| 10 rps for 30 s | 300 | 300 | 0 | 2.5 s |
| 20 rps for 30 s | 600 | 600 | 0 | 70.9 s |
| 40 rps for 30 s | 1200 | 413 | 787 | 65.2 s |

Total time p50 9.2 s, p90 61.7 s, p99 70.3 s; submit time p50 55 ms, p99 52.8 s. The load generator fell up to 3.6 s behind
its schedule in this run, so the top steps are also limited by the generator sharing the machine.

## What to take from it, and what not to

- On this machine and this stack, the RabbitMQ transport kept every step inside its limits up to 20 Requests per second,
  and the HTTP transport up to 10. `das loadtest-compare` reports the second run as a regression of the first (throughput
  down 37 %, p99 up 135 %) and notes that the transports differ.
- The HTTP transport serves its broker from the Gateway process (on the Ops_Port), next to the client API; both share one
  process here. That is the setup that was measured. **Why the HTTP run degraded earlier has not been investigated**: it is
  an observation about this setup, not a finding about HTTP as such. If you need the HTTP transport at higher rates, measure
  it on your stack, and consider a Gateway replica that serves the broker and is not the one clients use.
- A stub LLM answers in 50 ms. A real model takes seconds and varies, so a real stack saturates at a much lower rate; the
  point of the stub is to measure DAS and not a provider.
- One run is one sample. The runs were not repeated, so there is no measure of run-to-run variation here.
