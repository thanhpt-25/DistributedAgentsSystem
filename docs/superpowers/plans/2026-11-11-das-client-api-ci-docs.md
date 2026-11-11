# DAS Client API (Phase 4) CI, Deployment and Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spec task group 10 in `.kiro/specs/das-client-api/tasks.md`: CI runs the client package's own tests on every supported Python and the contract test in the fast job (10.1), and an operator can find, configure and use every Phase 4 feature from the repo's own files: sample config, `.env.example`, Compose and the README (10.2).

**Architecture:** Nothing in `das/` or `clients/python/` changes. The work is the workflow file, the static test that guards it, three deploy files, and documentation. The repo already treats CI and deploy files as tested artifacts (`test_ci_workflow_static.py`, `test_deploy_static.py`), so each change here starts with the test that pins it, and each README claim is traced to code or a test that proves it.

Spec references:
- `requirements.md` Req 9.10 and 11.5 (CI), Req 11.3 (upgrade order stated in the README), Req 1 (the config being documented), Req 2, 4, 5, 7, 8, 11.4 (the behaviour being documented).
- `design.md` §8 (CI), and the Error Handling table.

## Global Constraints

- **Static tests are part of the product.** `ci.yml`, `EXPECTED_*` in `tests/unit/test_ci_workflow_static.py`, and the README `## Development` code blocks must change in the same step, or the fast suite fails.
- **No behaviour change.** Defaults stay as the code defines them; the sample config shows them, and a test proves the sample equals the schema defaults.
- **No unverified documentation.** Every statement in the README about status codes, headers, fields, defaults and limits is read from the code or a test first (the table in Task 3 lists the sources), and every runnable example is run against a real Gateway before it is committed.
- **No secrets in the repo.** Examples use placeholders of the existing `change-me-...` style; webhook secrets are at least 16 characters (the loader rejects shorter ones) and only ever come from the environment or a secrets file.
- **One source for pinned test tools:** `pytest==9.1.1`, `pytest-asyncio==1.4.0` and `respx==0.23.1` appear in the root dev group and in the client CI command; a test keeps them equal.
- **Honest limits:** a GitHub workflow cannot be fully run locally. Parse tests, the exact job command run on every Python, and (if installable) a workflow linter are the local proof; the first real run on GitHub is the final check and is reported as such.
- Run everything through `uv run`. Gates after each task: `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, and the touched test files; the full `pytest tests/unit tests/property tests/contract` at the end.

## Progress

**Task 1 (spec 10.1) is done.** `ci.yml` has the `client` job (Python 3.10, 3.11, 3.12 and 3.13, `fail-fast: false`, 10 minutes, no `needs`, `enable-cache: false`) and the fast job's pytest step now runs `tests/unit tests/property tests/contract` (name, `echo` line and command changed together). `test_ci_workflow_static.py` gained `check_client_job` (matrix equals the strings for every minor version in `clients/python`'s `requires-python`, `setup-uv` takes `python-version: ${{ matrix.python }}`, exactly one `uv` command of the planned shape, its `pytest`, `pytest-asyncio` and `respx` pins equal the root dev group's), nine fault cases that each edit a correct workflow and must be noticed (a matrix without 3.12, an unquoted `3.10`, a drifted pytest or respx pin, no `python-version`, a job gated on fast, `fail-fast` left on, a `uv sync` step, a non-isolated command), a missing-job case, and a test that the matrix follows the claimed range. The README `## Development` and `## Continuous integration` sections, `CLAUDE.md`, design §8 and spec 10.1 are updated. The tests were written first: 12 failed before the workflow changed, and after it only the README match failed, which is what forced the README blocks.

Proof run here: the `run:` line was extracted from `ci.yml` itself (not retyped) and run with `UV_PYTHON` set the way `setup-uv` sets it, once per matrix entry: Python 3.10.22, 3.11.18, 3.12.12 and 3.13.11, 505 passed each (3.11 had not been run before). The pytest header confirms the interpreter, so `UV_PYTHON` is honoured under `--isolated --no-project`, which is the mechanism the job relies on. `actionlint` (via `uvx --from actionlint-py`) exits 0 on the workflow, and exits 1 on a copy with a deliberate typo (`matrix.pythn`), so it did check the file. `ruff`, `mypy`, `lint-imports` and the static tests that read `ci.yml`, the README and the deploy files pass (93).

Not verified, and cannot be locally: how GitHub expands the matrix and names the jobs (`Client_Job (Python 3.10)`), and `setup-uv` setting `UV_PYTHON` on a runner. The first push is the real test. If the repository has required status checks, the owner must add the four `Client_Job (Python ...)` checks; the `Fast_Job` name is unchanged.

Deviations from the plan as first written: none in design. Decision 1 (four Pythons) and decision 2 (contract tests in the existing step) were applied as recommended; design §8 and spec 10.1 now say so.

**Task 2 (the deploy-file half of spec 10.2) is done; 10.2 itself stays open until the README (Task 3).** `deploy/config/gateway.yaml` now writes out `client_limits`, `events` and `webhook` with the schema defaults (each line commented with what it does and what `0` means), plus two commented example blocks (a `default_budget` cap, and a per-key override by Key_Fingerprint with a webhook target) between `# BEGIN EXAMPLE` and `# END EXAMPLE` markers. `deploy/.env.example` has a commented `DAS__WEBHOOK_SECRETS__PARTNER` block, and the `gateway` service in `docker-compose.yml` passes that one name through. The new `tests/unit/test_deploy_client_api.py` (10 tests) proves: the sample loads through `load_config`; its sections equal the schema defaults; every setting that has a value is shown (a field added to the schema without being written out fails); the example blocks load when switched on, with the expected effective settings for the real fingerprint of `change-me-api-key` (`484857e0`); the example webhook is refused without its secret; an empty secret stops the Gateway; `.env.example` keeps the secret commented out and explains why; Compose passes it by name only and only to the gateway. The tests were written first (5 failed against the old files). The 148 tests that read config, deploy files and CI pass; `ruff`, `mypy`, `lint-imports` are clean.

Probe result (Step 1), and the real-world check behind it: an **empty** `DAS__WEBHOOK_SECRETS__PARTNER` makes the config loader refuse to start (`webhook_secrets.partner: must be at least 16 characters`), an unset one loads, and the name is lower-cased. With a throwaway one-service Compose project (an image already on the machine, torn down afterwards) the three `.env` cases were checked end to end: line absent, the variable is **absent** inside the container; line set, it arrives intact; line `NAME=`, it arrives as an **empty string**. So the example stays commented out, and a README sentence must say "never leave it empty". `docker compose config` validates the edited file and its merge with the e2e override.

Fault injection: eleven deliberate mistakes in copies of the deploy files, each caught by the intended test, with an unmutated control that passes: a changed default (rate, `max_attempts`), `events.keepalive` no longer shown, a wrong example fingerprint, prose inside an example block, an invalid example budget, removed markers, an uncommented empty secret, a missing `.env.example` line, a Compose default value on the secret, and a missing Compose pass-through.

Found while testing: the first version of the example block had explanatory prose inside the markers, so switching it on produced invalid YAML. The test caught it; the prose now sits above the `BEGIN` marker. Deviations from the plan as first written: the example is two blocks (budget and override) instead of one, because they sit at different indentation levels; and `overrides` is not written out as an empty list, because the commented override block would then define the same key twice.

**Task 3 (the README half of spec 10.2) is done, and with it spec 10.2.** The README has new subsections under `## Using the API` (Idempotent submits, Budgets, Rate limits and quotas, Event streams, Webhooks, OpenAPI), a new `## Python client`, a new `## Upgrading to Phase 4`, a `### Client API settings` table with a YAML example and the fingerprint recipe, config-table rows, new rows in the `/v1` error table, `budget` and `webhook` in the view's field list, and a Client API metrics table. `tests/unit/test_readme_client_api.py` (48 tests, one per setting group, rejection reason, outcome, event type, metric, protocol term and YAML example; written first, and 41 of them failed against the old README) checks mechanically what can be: every setting, rejection reason, webhook outcome, event type, metric, header and error code is mentioned; every default in the settings table equals the schema's real default; every YAML example loads through `load_config`; every Python example parses and imports only names `das_client` exports; the fingerprint recipe and the placeholder key's fingerprint are right; internal links resolve. A fault-injection run (13 deliberate mistakes in copies of the README, with a control) is caught on every one, after one fix: the first mention check could be fooled by prose between two code spans (the word "retry" passed for a documented webhook outcome), so it now parses code spans properly.

**Every statement was read from the code first, and every example was run against a real Gateway.** The real CLI (`das run`) was started from the shipped `deploy/config/gateway.yaml` against a real Redis container (no RabbitMQ needed), and these were observed and match the text: idempotency (`202`, replay `200` with `Idempotent-Replayed: true`, `422`, the two `400` shapes); every budget input rule; the view's `budget` and `webhook` fields; the `429` for the rate limit (with `Retry-After: 1`), the quota (with `limit`) and the stream cap, and that `/healthz` is never limited and that streams and the OpenAPI document draw from the same bucket; real SSE frames (snapshot first, the terminal frame, the stream closing after it, a finished Request getting only its snapshot, `resync` for a bad `Last-Event-ID`) with blocking reads on a real Redis; a real signed webhook delivery to a local receiver that answered 500 twice then 200 (attempts at about 0.8 s, 1.8 s and 3.5 s with `base_backoff: 1.0`, every delivery verified by `verify_webhook`, the view going from `null` to `{state: delivered, attempts: 3, last_status: "200"}`). The README's own `curl` blocks, its sync and async Python examples (with a helper that cancels the Request so `wait` and `events` can finish without a Manager) and its webhook `handle` snippet were extracted from the file and executed. The extra real-Redis SSE and webhook checks cover part of what group 11's e2e tests were meant to prove; they ran by hand here, not as tests.

Corrections to this plan: there are **five** Phase 4 metrics, not four (the Gateway's four, plus the Manager's `das_budget_exceeded_total`). `max_cost` is stored and shown in canonical form (`"0.50"` reads back as `"0.5"`), and `max_cost` needs a Price_Table: on the shipped config (which has no `pricing`) a Request with a `max_cost` already shows `budget.exceeded: true` and would fail `budget_unenforceable` before planning; the README says so.

**Spec gap found, not fixed here (needs your decision before group 11):** Req 5.9 says "Job results of Succeeded Jobs SHALL stay queryable after `budget_exceeded`", and design §Testing says the e2e test checks a budget-stopped Request "ends Failed with `budget_exceeded` and partial results visible". But `GET /v1/requests/{id}` returns `final_result` only for a `Completed` Request (`das/gateway/app.py`, `request_view`), and `jobs` carries only `job_id` and `state`, so no Job result is reachable through the API for a budget-stopped Request. Showing `final_result` on a Failed Request would also change the Phase 3 response shape, which Req 11.1 says must not change. The README therefore says what the API does (a budget-stopped Request has no `final_result`; `jobs` and `transitions` show which Jobs succeeded or were skipped). Group 11's e2e test (11.1) should either assert on `jobs` and `transitions`, or the requirement and the view should be changed on purpose.

Cleanup: the Gateway process group and the Redis container started for the live runs were stopped and removed; no files outside the scratchpad were created by them.

**Task 4 (checkpoint) is done, and with it group 10.** `uv run ruff check`, `uv run mypy`, `uv run lint-imports` and `actionlint` on the workflow are clean; `uv run pytest tests/unit tests/property tests/contract` passes with **2645 tests** (the 2575 at the end of group 9, plus 12 CI-static, 10 deploy and 48 README tests), with the same 3 known pytest-asyncio warnings; the client suite passes (505) on Python 3.10, 3.11, 3.12 and 3.13 with `-W error`, using the exact command from the workflow and `UV_PYTHON`. The reviewer pass over the diff of `README.md`, `deploy/`, `.github/` and `CLAUDE.md` found no stale references (no old fast command, no "three jobs", no 3.10/3.13-only wording in the user-facing files); the only stale text was "four" metrics in two places in this plan's own body, now corrected to five. The numeric ranges the README states (`idempotency_ttl` 60 to 604800, `events.max_len` 100 to 100000, `keepalive` up to 300, and so on) were checked against the schema bounds. No stray files or processes were left by the live runs.

Still not verified, because it cannot be locally: how GitHub expands the `client` matrix and names its jobs, and `setup-uv` setting `UV_PYTHON` on a real runner; the first push is the real test. If the repository has required status checks, the four `Client_Job (Python ...)` checks need adding by the owner. Open decision carried to group 11: the budget-stop e2e test cannot assert "partial results visible" through the API as the spec is written (see the Task 3 note).

## Findings that shape the plan (read from the code)

- **The CI static test pins a lot.** `check_jobs_and_triggers` requires the job set to equal `EXPECTED_TIMEOUTS` (`fast`, `integration`, `e2e`), so a new `client` job needs an entry there. `check_fast_steps` requires each fast step's `name` to equal the single `uv` command it runs, and the list to equal `EXPECTED_FAST_CHECKS`. `check_uv_version` requires **every** job to have a `setup-uv` step with `version: ${{ env.UV_VERSION }}`. `check_pinned_actions` requires `owner/repo@<40-hex>` plus a `# vX.Y.Z` comment on every `uses:`. `check_readme_matches_workflow` requires **every** `uv run`/`uv sync` line of every job to be its own fenced block in README `## Development` (the whole block equals the command, on one line).
- **The matrix cannot be in the command.** A command containing `${{ matrix.python }}` could never equal a README block. `astral-sh/setup-uv` has a `python-version` input (already used by the fast job) that sets `UV_PYTHON`, which `uv run --isolated --no-project` honours, so the command stays constant and the Python comes from the matrix through that input. `3.10` must be quoted in YAML or it parses as the float `3.1`.
- **uv is fine with the lock.** Checked while planning, read-only: `uv lock --check` with uv 0.13.6 (CI) passes on the current `uv.lock`, and `uv sync --frozen --no-dev --no-install-project --no-editable` with uv 0.8.4 (the Dockerfiles) works without `clients/` in the image. The hand-restored `revision = 5` header from Task 1 of group 9 is accepted.
- **All four Pythons exist locally** (3.10.22, 3.11.18, 3.12.12, 3.13.x), so the exact CI client command can be run for the whole matrix here.
- **The README has almost no Phase 4 text.** The only Phase 4 line is the admin-replay `409 budget_exceeded` row. The `/v1` error table has no 422 or 429 rows, "Upgrading from v1" says nothing about Phase 4, and the CI section says "three jobs ... Python 3.12 and uv 0.13.6 ... `uv sync --locked`" (the client job runs neither `uv sync` nor only 3.12).
- **Config shape** (`das/config/schema.py`): `client_limits.default` and `.overrides[]` (by 8-hex Key_Fingerprint) carry `rate` (60.0), `burst` (120), `max_active_requests` (100), `max_streams` (10), `default_budget{max_total_tokens, max_cost}`, `webhook_url`, `webhook_secret_name`; `client_limits.idempotency_ttl` (86400 s, 60 to 604800) and `allow_insecure_webhooks` (false); `events{max_len 1000, max_connections 500, keepalive 15.0}`; `webhook{workers 2, timeout 10.0, base_backoff 30.0, max_backoff 3600.0, max_attempts 8}`; `webhook_secrets` (name pattern `^[A-Za-z0-9]+(_[A-Za-z0-9]+)*$`, lower-cased; value at least 16 characters; env `DAS__WEBHOOK_SECRETS__<NAME>` or the secrets file only). `0` disables `rate`, `max_active_requests` and `max_streams`; `webhook.workers: 0` turns the sender off on that replica. Cross-field rules: Key_Fingerprint collisions, unknown or duplicate override fingerprints, `base_backoff <= max_backoff`.
- **Key_Fingerprint** is the first 8 hex characters of `sha256(api_key)` (`das/ops/keys.py`). There is no CLI for it; the docs give `printf %s "$KEY" | shasum -a 256 | cut -c1-8` (and the Python equivalent), which a test must prove equals `key_fingerprint`.
- **Compose** passes secrets "by name" (`DAS__ADMIN_API_KEYS:` with no value; `test_gateway_passes_admin_api_keys_by_name` pins that shape). A name set empty in `.env` would arrive as an empty string, and a webhook secret of length 0 fails the 16-character rule, so the Gateway might not start. Task 2 starts with a probe of exactly that, and `.env.example` keeps the webhook secret line commented out unless the probe shows an empty value is tolerated.
- **Group 11 reuses these names.** The e2e override (group 11) needs a webhook secret name and a second Gateway replica; it should use the secret name and Compose pass-through chosen here.
- **`CLAUDE.md` line 38** describes the fast job's command and will be wrong after Task 1.

## Decisions (recommendation first; say if you want a different one)

1. **Client job matrix: Python 3.10, 3.11, 3.12 and 3.13** (the range `clients/python/pyproject.toml` claims, Req 9.1), not only 3.10 and 3.13 as the spec says. The fast job never runs the client's own suite, so without 3.12 nothing in CI would test the client on the server's own interpreter. Cost: two more small jobs. If agreed, design §8 and spec 10.1 are corrected in Task 1.
2. **Contract tests join the existing fast step** (`uv run pytest tests/unit tests/property tests/contract`, one run, one seed) instead of a new step. The step name, command, `EXPECTED_FAST_CHECKS`, the README block and the "Reproduce locally" lines all change together.
3. **`deploy/config/gateway.yaml` gets the Phase 4 sections active, with the defaults written out** (a test proves they equal the schema defaults, so it changes no behaviour and cannot drift), plus a commented example block (an override, a default budget, a webhook target) that a test proves loads when uncommented.
4. **Compose passes one example webhook secret by name** (`DAS__WEBHOOK_SECRETS__PARTNER`), unset by default, as the pattern to copy for more names.
5. **README placement:** new `###` subsections under `## Using the API` for idempotency, budgets, limits, events and webhooks; a new `## Python client` section; a Phase 4 paragraph under "Upgrading"; config-table rows; the new error codes in the error table.

## Order and dependencies

```
Task 1 (10.1 CI)            --\
                               +--> Task 3 (README) --> Task 4 (checkpoint, ticks)
Task 2 (10.2 deploy files)  --/
```
Tasks 1 and 2 are independent. Task 3 needs both (it links the `## Development` blocks and uses the sample config and variable names). The spec runs 10.1 and 10.2 in the same wave.

### Task 1: CI (spec 10.1)

**Files:** `.github/workflows/ci.yml`, `tests/unit/test_ci_workflow_static.py`, `README.md` (`## Development`, `## Continuous integration`), `CLAUDE.md` (CI line), `.kiro/specs/das-client-api/design.md` §8 and `tasks.md` 10.1 (matrix wording).

- [ ] **Step 1: Tests first** in `test_ci_workflow_static.py`:
  - `EXPECTED_TIMEOUTS` gains `"client": 10`; the last `EXPECTED_FAST_CHECKS` entry becomes `uv run pytest tests/unit tests/property tests/contract`.
  - New pure `check_client_job(text)`, in the same style as the other checks: the job exists; it has no `needs` or `if` (it runs on every trigger, like `fast`, and is not gated by it); `strategy.fail-fast` is false; `strategy.matrix.python` is exactly the **strings** for every minor version of `clients/python/pyproject.toml`'s `requires-python` (derived with `tomllib`, so widening the claim fails the test until CI follows); its `setup-uv` step has `python-version: ${{ matrix.python }}`; it has no `uv sync`; exactly one `uv run` command, which starts `uv run --isolated --no-project --with-editable clients/python`, ends with `pytest clients/python/tests -c clients/python/pyproject.toml --rootdir clients/python`, and whose `--with name==version` pins for `pytest`, `pytest-asyncio` and `respx` equal the pins in the root `pyproject.toml` dev group.
  - Mutation cases that run `check_client_job` on edited copies of the workflow and expect a problem: a matrix missing 3.12; an unquoted `3.10`; a drifted pin; a missing `python-version`; a `needs: fast`.
  - The existing `check_readme_matches_workflow` then requires the client command to be its own README block once the workflow has the job.
- [ ] **Step 2: RED.** Run the file; the new checks and the changed constants fail because the workflow and README are unchanged.
- [ ] **Step 3: `ci.yml`.** In `fast`: rename the pytest step to `uv run pytest tests/unit tests/property tests/contract`, and change its `run:` block (the two `echo` lines and the final command) to match. Add the job (same pinned `actions/checkout` and `astral-sh/setup-uv` SHAs and version comments as the other jobs):
  ```yaml
  client:
    name: Client_Job (Python ${{ matrix.python }})
    runs-on: ubuntu-24.04
    timeout-minutes: 10
    strategy:
      fail-fast: false
      matrix:
        python: ["3.10", "3.11", "3.12", "3.13"]
    steps:
      - name: Checkout
        uses: actions/checkout@<same sha> # v7.0.1
      - name: Set up uv
        uses: astral-sh/setup-uv@<same sha> # v10.3.3
        with:
          version: ${{ env.UV_VERSION }}
          python-version: ${{ matrix.python }}
          enable-cache: false
      - name: uv run pytest clients/python/tests
        run: uv run --isolated --no-project --with-editable clients/python --with pytest==9.1.1 --with pytest-asyncio==1.4.0 --with respx==0.23.1 pytest clients/python/tests -c clients/python/pyproject.toml --rootdir clients/python
  ```
  The exact step name and command are fixed by the test written in Step 1. The client suite reads `tests/fixtures/webhook_vectors.json`, which is in the checkout.
- [ ] **Step 4: README.** `## Development`: replace the fast-suite block with the new command; add one block for the client command (with a sentence on `UV_PYTHON=3.10` or `--python 3.10` to try another interpreter, and why the client is not a uv workspace member). `## Continuous integration`: "four jobs"; add the `client` row (every push, pull request and manual run; 10 min; Python 3.10 to 3.13); fix the sentence that says every job installs Python 3.12 and runs `uv sync --locked`; update the "Reproducing a Hypothesis failure" step name and command. `CLAUDE.md`: the CI line (fast job now includes `tests/contract`; the `client` job and its command).
- [ ] **Step 5: GREEN and proof.** `uv run pytest tests/unit/test_ci_workflow_static.py`. Then run the **exact** `run:` command once for each of 3.10, 3.11, 3.12 and 3.13 with `UV_PYTHON` set the way `setup-uv` sets it, and show the four passing counts. Try `uvx --from actionlint-py actionlint .github/workflows/ci.yml` (a workflow linter); if it cannot be installed, say so instead of claiming the workflow was linted.
- [ ] **Step 6: Spec text.** Correct design §8 and the 10.1 line for the four-version matrix and the contract step. Tick 10.1.

### Task 2: Sample config, `.env.example` and Compose (spec 10.2, deploy files)

**Files:** `deploy/config/gateway.yaml`, `deploy/.env.example`, `deploy/docker-compose.yml`, `tests/unit/test_deploy_static.py` (or a new `tests/unit/test_deploy_client_api.py`).

- [ ] **Step 1: Probe, before writing anything.** What happens to the config loader when `DAS__WEBHOOK_SECRETS__PARTNER` is set to the empty string (what Compose passes if `.env` holds `NAME=`), and when it is unset? Use the same entry point `das run` uses to resolve a gateway config. Record the result in this plan's Progress section; it decides whether the `.env.example` line may be uncommented.
- [ ] **Step 2: Tests first:**
  - The shipped `gateway.yaml` loads through the real config entry point, and its `client_limits`, `events` and `webhook` sections **equal the schema defaults** (compare against an `AgentConfig` built without them), so the sample is behaviour-neutral and cannot drift.
  - Between marker comments (`# BEGIN EXAMPLE` and `# END EXAMPLE`) the file holds a commented example (one override by fingerprint, a `default_budget`, a `webhook_url` with `webhook_secret_name`); a test uncomments it, supplies the secret through the environment, and requires it to load with the expected effective settings for that fingerprint.
  - The example's fingerprint is the real one for a documented example key, computed with `key_fingerprint`.
  - `deploy/.env.example` declares the webhook secret line commented out (or as the probe allows), with a comment on the 16-character minimum and the name rules, and no literal secret beyond a placeholder; Compose passes `DAS__WEBHOOK_SECRETS__PARTNER` to the gateway **by name** (reusing the existing `_passed_by_name` helper), never with a value or default.
- [ ] **Step 3: RED**, then edit the three files. `gateway.yaml`: the three sections with defaults and one-line comments (what each does, what `0` means); the commented example block; a comment on how to compute a fingerprint. `.env.example`: a "Webhook secrets" block. `docker-compose.yml`: the pass-through line in the `gateway` environment.
- [ ] **Step 4: GREEN**, plus the gates. All three sample configs are loaded through the loader by the unit tests, so a broken sample is caught there and not by `docker compose up`.

### Task 3: README (spec 10.2, docs)

**Files:** `README.md`, plus a new drift test `tests/unit/test_readme_client_api.py`.

Content, each point with its source to read before writing:

| Section | Must state | Source of truth |
|---|---|---|
| Idempotent submits | `Idempotency-Key` header, 1 to 128 printable ASCII; `202` first, `200` plus `Idempotent-Replayed: true` on a replay; `422 idempotency_key_reused` for another body; a replay is answered even when the quota is full; key lifetime `idempotency_ttl`; the key is scoped to the API key | `das/gateway/submit.py`, `test_gateway_submit_client.py`, `test_gateway_client_errors.py` |
| Budgets | `budget{max_total_tokens, max_cost}` on submit (positive integer, decimal string); key `default_budget` as a cap (`400 budget_above_limit` with `parameter` and `limit`); `400 invalid_request` with `parameter`; `max_cost` needs a Price_Table (otherwise `budget_unenforceable`); a stop ends the Request `Failed` with `budget_exceeded` and keeps partial results; the `budget` object in the query view; replaying a budget-failed dead letter is `409` | `das/client/budget.py`, Req 5, `test_gateway_request_view_client.py`, the contract test |
| Rate limits and quotas | per-key token bucket (`rate`, `burst`), `429 rate_limited` + `Retry-After`; `429 quota_exceeded` + `limit`; `0` disables; overrides by Key_Fingerprint and how to compute one; limits are per key across all Gateway replicas (Redis `TIME`); `/healthz` is never limited | `das/gateway/limits.py`, `test_gateway_limits.py` |
| Events (SSE) | `GET /v1/requests/{id}/events`; first frame `snapshot` (the view without `final_result`), then events; frame format; `Last-Event-ID`; `resync`; `: keepalive`; closes after the terminal event; a finished Request gets its snapshot and closes; caps (`429` or `503 too_many_streams`); event types; events carry no prompt, instruction or result; a `curl -N` example; `events.max_len` trimming | `das/gateway/events.py`, `test_gateway_events.py`, the contract test |
| Webhooks | per-key `webhook_url` + `webhook_secret_name`, secret from `DAS__WEBHOOK_SECRETS__<NAME>`; `https` required unless `allow_insecure_webhooks`; one delivery per terminal event; headers `DAS-Webhook-Id`, `DAS-Webhook-Timestamp`, `DAS-Webhook-Signature`; body envelope; the signature recipe in plain terms and with `das_client.verify_webhook`; 300 s tolerance; **at-least-once, so dedupe on `DAS-Webhook-Id`** (the note the spec asks for); retry schedule from `webhook.*`; no redirects; the response body is read up to 64 KiB; a removed secret fails the delivery (`secret_missing`) | `das/gateway/webhooks.py`, `das/client/webhook.py`, `test_gateway_webhooks.py` |
| OpenAPI | `GET /v1/openapi.json` behind the API key; admin routes not in it; `/docs` and `/redoc` are off | `das/gateway/openapi.py`, `test_gateway_openapi.py` |
| Python client | install from the repo (`pip install ./clients/python`; not published), supported Pythons, a sync and an async example (submit with a budget, `wait`, `events`), the error classes and what raises them, retry defaults, `verify_webhook` | `clients/python`, the contract test; both examples are run for real |
| Upgrading | Gateway and Manager upgrade in any order; **events are complete only after every Manager runs Phase 4** (Req 11.3); old Requests have no budget, webhook or Active_Set and stream as a snapshot plus whatever events exist; the new limits are **on by default** (60/s, burst 120, 100 active, 10 streams), so clients that ran unthrottled before see `429`s until `rate` and `max_active_requests` are raised or set to `0`; the five new metrics | Req 11, `das/observability/metrics.py` |
| Reference | new rows in the error table (422, the 429 codes, the new 400 codes, 503 `too_many_streams`) and the config table (`client_limits`, `events`, `webhook`, `webhook_secrets`) | the schema above |

- [ ] **Step 1: Drift test first** (`test_readme_client_api.py`): the README mentions every field of `ClientSettingsConfig`, `ClientLimitsConfig`, `EventsConfig` and `WebhookConfig` (iterated from the models, so a field added later without docs fails), every `ClientRejectionReason`, every `WebhookOutcome`, the five Phase 4 metric names from the catalog, the three webhook header names, `Idempotency-Key`, `Idempotent-Replayed`, `Last-Event-ID` and `/v1/openapi.json`; the fingerprint recipe in the README equals `key_fingerprint` for a sample key (computed with `hashlib`); every fenced `yaml` block marked for validation loads. RED.
- [ ] **Step 2: Verify the facts** in the Source column against the code and tests listed, before writing a line. Anything the code does not do is left out, not described.
- [ ] **Step 3: Write the sections.** Keep each short: the behaviour in a few sentences, one example, the error codes in a table. Aim for about 150 lines in total, written for someone who has not read the specs.
- [ ] **Step 4: Run the examples for real.** Start a real Gateway (uvicorn on fakeredis, as the earlier live smoke checks did) and run every `curl` example and both Python client examples; compare the output with what the README says. A webhook receiver example verifies a signature produced by the real sender. Fix the README, not the output, when they differ, unless the difference is a code bug (then stop and report it).
- [ ] **Step 5: GREEN**, then the gates.

### Task 4: Checkpoint and bookkeeping

- [ ] `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property tests/contract` (in the background, log to the scratchpad), and the client suite on the four Pythons.
- [ ] Read `git diff` of `README.md`, `deploy/` and `.github/` once as a reviewer: stale numbers, broken anchors, claims without a source.
- [ ] Tick 10.1, 10.2 and the group-10 parent in `tasks.md`; add a Progress section to this plan with deviations; report what could not be verified locally.

## Risks and open points

- **The workflow's real behaviour is only proven on GitHub.** Matrix expansion, `setup-uv`'s `python-version` setting `UV_PYTHON`, and the `Client_Job (Python ...)` job names cannot be fully reproduced locally. The mitigations above cover the parts that can; the first push to a branch is the real test, and a failure there is fixed in the workflow, not worked around in the tests. If the repository has branch protection with required status checks, the owner must add the new `client` checks; this plan cannot see that setting.
- **Required checks:** the `fast` job's name stays `Fast_Job`, so an existing required check keeps working.
- **README drift.** The drift test catches a missing mention, not a wrong one; wrong statements are caught only by the verification in Step 2 and the live run in Step 4, so those steps are not optional.
- **Behaviour change to flag in the docs, not hide:** limits are on by default after the upgrade (recorded in the Gateway plan as a deviation). The README says so plainly.
- **Not planned:** publishing `das-client` to PyPI or a release workflow, Grafana dashboards or alerts for the new metrics, a Key_Fingerprint CLI, Windows instructions, and anything in group 11 (the e2e override, a second Gateway replica, the webhook sink).
