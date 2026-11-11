# DAS Client API (Phase 4) Client Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spec task group 9 in `.kiro/specs/das-client-api/tasks.md`: a small, separately installable Python library, `das-client` (`clients/python/`, import name `das_client`), with `DasClient` and `AsyncDasClient`, retries, typed errors, an SSE events iterator, `wait()`, and `verify_webhook`. A contract test in the main repo runs it against the real Gateway app.

**Architecture:** The Gateway side is done (group 7). The client is a thin typed wrapper over the wire shapes that exist today. All decisions that can be pure (retry decision and delay, error mapping, SSE parsing, model parsing, webhook verification) are plain functions written once and shared by the sync and async classes. The two client classes only differ in how they send and sleep. `das_client` never imports `das` (Req 9.1); the only link back is the shared vector file.

Spec references:
- `requirements.md` Req 9 (all ten criteria), Req 8.4 and 9.7/9.8 (webhook signature), Req 6.2 and 7 (events as the client sees them), Req 11.5 (CI).
- `design.md` Components §7 (Client Package) and §8 (CI), Property 8, Testing Strategy ("Contract test", "Client package tests").

**Tech Stack:** Python >=3.10,<3.14 for the package (the repo itself is 3.12), `httpx==0.28.1` (the only runtime dependency). Tests: pytest 9.1.1, pytest-asyncio 1.4.0, respx 0.23.1, Hypothesis 6.170.8, all already pinned in the root dev group.

## Global Constraints

- **Python 3.10 floor.** No `typing.Self`, `StrEnum`, `asyncio.timeout`, `asyncio.TaskGroup`, `tomllib` (in the package), `ExceptionGroup`/`except*`, `datetime.UTC`. Every module starts with `from __future__ import annotations`. CI proves it on 3.10 and 3.13 (group 10), so check any new stdlib call against 3.10.
- **One runtime dependency:** `httpx==0.28.1`, exact pin, same as the server. Nothing from `das`, no `pydantic`, no direct `anyio` import.
- **Responses are parsed leniently** (Req 9.9): models take the fields they know and ignore the rest, and a missing optional field is `None`. A field the client needs but the server did not send is a `DasError`, not a `KeyError`.
- **Secrets:** the API key and webhook secret never appear in `repr()`, error messages or logs. `DasClient.__repr__` shows the base URL only.
- **Time and randomness are injectable** (the project rule, as `Clock` is on the server): each client takes `sleep`, `rand` and `monotonic` keyword-only seams, so retry and polling tests run instantly and exactly. Defaults are `time.sleep`/`asyncio.sleep`, `random.random` and `time.monotonic`.
- **Tests first:** write each task's tests first and watch them fail before the code (the project and the user's rules both ask for it).
- Run everything through `uv run`. Quality gates stay: `uv run ruff check` (covers `clients/python` too), `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property tests/contract`, plus the client's own suite (Task 1 defines the command).
- Docstrings cite requirements as `Phase 4 Req X.Y`. Tick the boxes in `tasks.md` at the end of each task.

## Progress

**Task 1 (spec 9.1) is done.** Spike result: a uv **workspace member does not work**. The lock keeps `requires-python = "==3.12.*"` and uv refuses `--python 3.10` and `--python 3.13` for the member. The **path-source fallback works**: the root dev group has `das-client` plus `[tool.uv.sources] das-client = { path = "clients/python", editable = true }`; `uv lock --check` and `uv sync --locked` pass; the Docker-style `uv sync --frozen --no-dev` still works without `clients/` in the image; and the client suite runs on 3.10, 3.12 and 3.13 through `uv run --isolated --no-project --python <v> --with-editable clients/python --with pytest==9.1.1 --with pytest-asyncio==1.4.0 --with respx==0.23.1 pytest clients/python/tests -c clients/python/pyproject.toml --rootdir clients/python`. All three interpreters exist locally, so 3.10 and 3.13 were really run. `design.md` §7 and §8 and spec 9.1 now say this. Deviations from the plan as first written:

- `uv lock` with the local uv 0.9.28 rewrote the lock header `revision = 5` to `3`; it was restored by hand so the lock diff is 13 insertions and nothing else. A later `uv lock` run will do it again, so check `git diff uv.lock` after any lock change.
- Only `__init__.py` and `py.typed` were created for the source tree; the other modules (`errors`, `models`, `_transport`, `_sse`, `webhook`) are created in the task that fills them, not as empty files. `clients/python/tests/` has no `__init__.py`, so its modules cannot collide with the root `tests` package.
- The guard test for `__all__` is deferred to Tasks 2 and 3, when there are names to check. A `README.md` for the client was added (the `readme` field of its pyproject names it).
- Group 10.1's `client` job must use the isolated command above (not `uv run --project clients/python`), and the CI static test pins the job set, so update it together.

**Task 2 (spec 9.2) is done.** `errors.py`, `models.py`, `_transport.py`, `client.py` and `aclient.py` exist; the client suite is 298 tests (294 in the four new modules, 4 in `test_package.py`, 2 of which are new), passing on Python 3.10, 3.12 and 3.13 with `-W error`; `ruff`, `mypy` and `lint-imports` are clean. Tests were written first and failed (the shared `conftest.py` could not import the clients). A mutation check in a scratch copy confirmed six deliberate breakages are caught (Retry-After floor on 503, no retry of 429, dot ids not encoded, empty budget sent, empty explicit key replaced, no jitter). Deviations from the plan as first written:

- **Security fix found while writing the tests:** httpx folds a literal `..` path segment, so `get("..")` would have requested `/v1/` instead of `/v1/requests/..`. `path_segment` now percent-encodes dot-only ids (`%2E`), and an empty id is a `ValueError` before anything is sent.
- **Naive datetimes are rejected client-side** (`ValueError`), because the server requires a time zone on `created_after`/`created_before`; aware ones are converted to UTC. `datetime.UTC` is avoided (3.11+), as are other 3.11-only names.
- **Retry loop:** the decision "wait how long, or fail now" is one pure function, `next_delay` (connection errors have no status; only 429 gets the `Retry-After` floor). The sync and async loops stay as two short copies calling it. A `Retry-After` of more than 10 digits is ignored, so a hostile header cannot overflow a float.
- **`stream_timeout` and `monotonic` are not constructor parameters yet**; Task 3 adds them with the code that uses them (no dead parameters). The async client closes with `aclose()` (the `httpx` name), the sync one with `close()`.
- **`3xx` responses are errors** (the client does not follow redirects), so a proxy login page is a `DasError`, not a parse failure further on.
- `__all__` has 22 names; Task 3 extends the guard in `test_package.py` with `verify_webhook`, `InvalidSignature`, `WaitTimeout` and `Event`.

**Task 3 (spec 9.3 and 9.5) is done.** `_sse.py` (parser, `event_from_frame`, `StreamState`), `webhook.py` (`sign`, `verify`, `verify_webhook`), `Event`, `WaitTimeout`, `InvalidSignature`, and `events()`/`wait()` on both clients. The client suite is 505 tests, passing on Python 3.10, 3.12 and 3.13 with `-W error`; `ruff`, `mypy` and `lint-imports` are clean. Tests were written first and failed to collect. A mutation check (15 deliberate breakages in a scratch copy, with an unmutated control that had to pass) is caught on every one. Deviations from the plan as first written:

- **The server's resume semantics changed the reconnect rule.** After a reconnect with `Last-Event-ID` X the server sends events after X, but the snapshot it sends first carries a *later* id. Moving the position to that id would lose the events in between if the connection broke again mid-replay, so the position moves to a snapshot's id only when the connection had no position to send, or the snapshot is flagged `resync`. A snapshot is also yielded again in exactly those two cases, and swallowed on a plain resume. Both are pinned by tests.
- **A snapshot is not progress.** Only a real event resets the failures-in-a-row count; otherwise a server that closes right after every snapshot would keep the client reconnecting for ever (a mutation that made it so hung the first version of its test, which now fails fast instead).
- **A terminal stream with no terminal event is normal.** A Request that is already finished gets its snapshot and then the stream closes, so EOF after a terminal snapshot ends the iteration.
- **`wait()` always reads the view with `get` after the stream ends**, because the snapshot has no `final_result`, and sleeps one poll interval whenever the view is not yet terminal, so a stream that ends while `get` still says Running (a replay reopened it) cannot make it spin.
- **Deadline:** checked after every received chunk and before every reconnect and sleep, and the read timeout of a waiting stream is capped at the time left (never under one second, so the deadline can be overshot by about that much). A stream that sends only keepalives is stopped by the clock (own test).
- **`verify_webhook` is strict about the envelope** (`request_id`, `event_id`, `type` all text, `data` an object) and refuses a genuinely signed body that is not one; one of the shared signing vectors signs an *empty* body, which `verify` accepts and `verify_webhook` therefore refuses (own test).
- **The SSE parser is bounded** (1 MiB of unfinished frame or accumulated data raises `DasError`), reads CR, LF and CRLF in any chunking (every split position is tested), strips a BOM and ignores an `id` containing NUL.
- **Test infrastructure lesson:** a mutation harness that runs `uv run ... pytest` through `subprocess.run(timeout=...)` kills `uv` but leaves its pytest child running; hung mutants had to be killed by PID. Use a process group if this is repeated.
- `__all__` now has 26 names, as in "Public API" (`Event`, `InvalidSignature`, `WaitTimeout`, `verify_webhook` added).

**Task 4 (spec 9.4) is done.** `tests/property/test_webhook_signature_roundtrip.py` has 9 properties (Property 8): both copies sign identically; both copies agree on hostile header text (signs, spaces, non-ASCII digits, NaN clocks, odd tolerances); a signature verifies exactly within the tolerance, checked against an exact `Fraction` oracle; the boundary is inclusive; a non-finite clock never verifies; a changed body, secret, any part of the signature, or timestamp (even one still inside the window) is rejected; and `verify_webhook` accepts exactly what the server's `verify` accepts, returning the event that was signed. It passes on the default and `ci` profiles and under 30 different seeds with fresh Hypothesis storage; `ruff`, `mypy` and `lint-imports` are clean. A fault-injection check (8 deliberate faults in the server's and the client's copies, applied in memory by a scratch pytest plugin, plus an unmutated control) is caught on every one, each by the property meant to catch it. Deviations from the plan as first written:

- **A generator bug the first fault run exposed.** HMAC zero-pads a short key, so secrets that differ only by trailing NUL bytes (`b"k"` and `b"k\x00"`) give identical signatures, and "any other secret is rejected" is false for them. The control passed only because Hypothesis had not drawn such a pair; later runs replayed the saved failing example from its database. The generator now requires secrets that differ after stripping trailing NULs. It is not a product problem: webhook secrets come from environment variables (which cannot hold a NUL) or a secrets file and only have a minimum length.
- **A coverage gap the same run exposed.** A NaN clock was caught only by accident (the header-text property rarely draws NaN together with a valid signature), so it has its own property.
- **Timestamps in the oracle are exact:** the clock offsets are multiples of 1/8, so float sums and differences are exact and the oracle (`abs(Fraction(now) - ts) <= tol`) cannot disagree with the code for rounding reasons.
- **Method:** each fault runs in its own process group and its own `HYPOTHESIS_STORAGE_DIRECTORY`, so a hang is killed cleanly and a saved example from one run cannot affect the next.

**Task 5 (spec 9.6) is done, and with it group 9.** `tests/contract/` (`harness.py`, `conftest.py`, `test_client_contract.py`) runs the client against the real `create_app(...)` on fakeredis: 25 tests once per client (async and blocking) plus one test of a delivery signed by the real `WebhookSender`, 51 in all. The full checkpoint passes: `ruff`, `mypy`, `lint-imports`, `pytest tests/unit tests/property tests/contract` (2575 passed, the same 3 known pytest-asyncio warnings), and the client suite (505) on Python 3.10, 3.12 and 3.13. Deviations from the plan as first written:

- **The loop bridge works; the live-uvicorn fallback was not needed.** `LoopBridge` (a blocking `httpx.BaseTransport`) runs each request of the blocking client on the test's own event loop with `run_coroutine_threadsafe`, from a worker thread the test calls the client in, and buffers the response. No deadlock, and fakeredis is happy because it only ever runs on its own loop. The spike was one `submit` and `get` for both clients, before the rest was written.
- **Streams over this transport end by themselves.** Every stream the tests open is over (terminal Request, or cancelled), because the response is buffered. A stream that stays open is group 11's. A `fail_events` counter on the transport injects connection failures for the stream, which is how the `wait()` polling fallback is exercised against the real server.
- **The contract is sensitive to the server.** A scratch pytest plugin changed what the Gateway puts on the wire, one change at a time (16 changes: renamed `request_id`, `status`, `budget`, `next_cursor`, the cancel `status`; a dropped `Idempotent-Replayed` or `Retry-After` header; a dropped quota `limit`; renamed error codes; 404, 401 and 409 turned into other statuses; a renamed SSE `snapshot` event, a wrong content type, a renamed envelope `type`), with an unmutated control that had to pass. Every change was caught, each by a test that makes sense for it.
- **A server behaviour worth knowing, found while writing the tests:** on a Gateway without a Price_Table, a fresh Request with a `max_cost` budget reports `budget.exceeded: true` (unenforceable, Phase 4 Req 5.6; the Manager would fail it before planning). That is the specified behaviour, so the test asserts only the limits for such a budget, and `exceeded: false` for a tokens-only one.
- **Not per client:** the real-sender webhook test (`pending_delivery` + `WebhookSender` + `verify_webhook`, with a one-byte tamper and a wrong secret) ties the server's signing to the client's verification; it runs once.
- **For group 10.1:** the fast job's command becomes `pytest tests/unit tests/property tests/contract` (a plain `uv run pytest` already includes `tests/contract`, because `testpaths` is `tests`), and the CI static test pins the job set, so the new `client` job and its `EXPECTED_TIMEOUTS` entry change together.

## Findings that shape the plan (read from the code)

- **Wire shapes the client must parse** (all verified in `das/gateway`):
  - Submit: `202 {"request_id"}`; a replay is `200` with header `Idempotent-Replayed: true`.
  - Cancel: `POST /v1/requests/{id}/cancel` answers `202 {"status": "Cancelled"}`, `409` if already terminal, `404`, `401`, `503`.
  - View (`GET /v1/requests/{id}`): `request_id`, `status`, `jobs[{job_id,state}]`, `transitions[...]`, optional `final_result` (Completed only) and `error`, `token_usage`, `estimated_cost` (null without a price table), `replay_history`, `budget` (null or `{limits, used:{total_tokens,cost}, exceeded}`), `webhook` (null or `{state, attempts, last_status}`).
  - List (`GET /v1/requests`): `{"items": [{request_id,status,created_at,finished_at,job_count,token_usage:{input,output,total}}], "next_cursor"?}`; filters `status` (comma list), `created_after`, `created_before`, `limit`, `cursor`.
  - Errors are JSON `{"error": <code>, ...}`: `invalid_request` (+`parameter`), `budget_above_limit` (+`parameter`, `limit`), `idempotency_key_reused` (422), `quota_exceeded` (+`limit`), `rate_limited` (+`Retry-After`), `too_many_streams` (429 per key, 503 per replica), `service_unavailable` (503), `unauthorized`, `not_found`.
  - SSE frames are `event: <type>\nid: <event_id>\ndata: <json>\n\n`, with `: keepalive` comment lines. The first frame is `event: snapshot` (the view without `final_result`, plus `event_id`, and `resync: true` when the client's `Last-Event-ID` was too old). Other frames carry the envelope `{request_id, event_id, type, data}`. Terminal types: `request.completed`, `request.failed`, `request.cancelled`.
  - A Request that is already terminal when a stream opens gets the snapshot, then whatever Events remain after `Last-Event-ID`, then the server closes (`drain_only` in `das/gateway/events.py`). **A reconnect after the Terminal_Event can therefore end with a snapshot and EOF and no Event.** The client must treat "EOF after a snapshot whose status is terminal" as the end, or `events()` would reconnect forever.
- **Webhook verification already has a drift guard:** `tests/fixtures/webhook_vectors.json` has 3 `vectors` (sign), 2 `accept` and 8 `reject` cases (header text and `now`/`tolerance`), already checked against the server by `tests/unit/test_client_webhook.py`. `verify_webhook` is a header-reading wrapper over a copy of `das.client.webhook.verify`, and must keep its strict timestamp rule (1 to 19 ASCII digits, NaN `now` rejected, non-ASCII signature rejected, constant-time compare). Header names: `DAS-Webhook-Id`, `DAS-Webhook-Timestamp`, `DAS-Webhook-Signature`; the body is the canonical JSON envelope.
- **`uv.lock` is `requires-python = "==3.12.*"`.** A uv workspace resolves with one `requires-python` for all members, so adding a member that claims 3.10 to 3.13 may narrow or conflict, and `uv run --project clients/python` on Python 3.10 may refuse the lock. This is the biggest unknown; Task 1 starts with a spike (see Risks).
- **`httpx.ASGITransport` is async only.** The contract test can drive `AsyncDasClient` in-process directly, but not `DasClient`. Task 5 bridges the sync client onto the test's event loop (see Risks).
- **Property 8 needs both packages**, so it cannot live in `clients/python/tests`. It lives in `tests/property/test_webhook_signature_roundtrip.py` in the main repo, where the client is installed in the dev group (as the design says for the contract test).
- **Root tool config:** `[tool.ruff]` is `py312`, E/F only, 120 columns, and applies to `clients/python` unless the client has its own `[tool.ruff]`. mypy is `files = ["das"]` only. `testpaths = ["tests"]`, so the client's own tests run only when pointed at.
- **CI static tests will notice group 10, not group 9:** `tests/unit/test_ci_workflow_static.py` pins the job set to `{fast, integration, e2e}`. Adding the `client` job (10.1) must update `EXPECTED_TIMEOUTS` in the same change. Nothing in group 9 edits `ci.yml`.

## Public API (decided here, so the tasks agree)

```python
# das_client/__init__.py exports exactly:
DasClient, AsyncDasClient, verify_webhook,
DasError, AuthError, NotFound, Conflict, IdempotencyKeyReused, BudgetAboveLimit,
InvalidRequest, RateLimited, QuotaExceeded, ServiceUnavailable, InvalidSignature,
TransportFailure, WaitTimeout,
Budget, Submitted, CancelResult, RequestView, JobView, BudgetView, WebhookState,
RequestSummary, Page, Event

class DasClient:
    def __init__(self, base_url: str, api_key: str, *, max_retries: int = 3, timeout: float = 30.0,
                 stream_timeout: float = 60.0, http_client: httpx.Client | None = None,
                 sleep=time.sleep, rand=random.random, monotonic=time.monotonic) -> None
    def submit(self, prompt, *, budget: Budget | None = None, idempotency_key: str | None = None) -> Submitted
    def get(self, request_id) -> RequestView
    def cancel(self, request_id) -> CancelResult          # 202 {"status": "Cancelled"}
    def events(self, request_id, *, last_event_id: str | None = None) -> Iterator[Event]
    def wait(self, request_id, *, timeout: float) -> RequestView
    def list_requests(self, *, status=None, created_after=None, created_before=None, limit=None,
                      cursor=None) -> Page
    def close(self) -> None;  __enter__/__exit__
# AsyncDasClient: the same methods as coroutines / async generators, httpx.AsyncClient, asyncio.sleep.
```

- `Submitted(request_id: str, replayed: bool)`. `Budget(max_total_tokens: int | None = None, max_cost: Decimal | str | None = None)`, serialized with `max_cost` as a decimal **string** (the server rejects bare floats and bools).
- `Event(type, event_id, data, request_id)`. The first event of a stream is `type == "snapshot"` with `data` = the snapshot view. `Event.is_terminal` is true for the three terminal types.
- `RequestView.is_terminal` is true for `Completed`, `Failed`, `Cancelled`.
- Error classes carry `status_code`, `code` (the body's `error`), `body` (the parsed dict, or `{}`), and class-specific fields: `RateLimited.retry_after: float | None`, `QuotaExceeded.limit: int | None`, `BudgetAboveLimit.parameter/.limit`, `InvalidRequest.parameter`.

## Order and dependencies

Spec waves: 9.1 first; 9.2 and 9.3 together; then 9.4, 9.5, 9.6. This plan keeps that order but writes each task's unit tests **with** its code (tests first), so 9.5 is ticked at Task 3 once all of its listed cases exist.

```
Task 1 (9.1 scaffold + spike) -> Task 2 (9.2 clients) -> Task 3 (9.3 events/wait/webhook, finishes 9.5)
Task 3 -> Task 4 (9.4 Property 8; needs only verify_webhook)
Task 3 + Task 4 -> Task 5 (9.6 contract test, then the group's checkpoint)
```

### Task 1: Scaffold and workspace spike (spec 9.1)

**Files:** create `clients/python/pyproject.toml`, `clients/python/src/das_client/{__init__,errors,models,webhook,_sse,_transport}.py` (docstring-only to start), `clients/python/src/das_client/py.typed`, `clients/python/tests/__init__.py`; modify root `pyproject.toml` and `uv.lock`.

- [ ] **Step 1: Spike the workspace** before writing anything else. Try, in order: (a) a uv workspace member (`[tool.uv.workspace] members = ["clients/python"]`, `[tool.uv.sources] das-client = { workspace = true }`, `das-client` in the dev group); run `uv lock` and `uv sync --locked`, then `uv run --project clients/python --python 3.10 pytest --co`. If it locks and 3.10 works, use it. (b) If the lock narrows to 3.12 or `--python 3.10` is refused, fall back to a **path source without a workspace**: `[tool.uv.sources] das-client = { path = "clients/python", editable = true }` in the root dev group, and give the client job (group 10) its own command: `uv run --isolated --python 3.10 --with-editable clients/python --with pytest==9.1.1 --with pytest-asyncio==1.4.0 --with respx==0.23.1 pytest clients/python/tests`. Record which one worked and why in this plan's Progress section, and add a one-line note to `design.md` §7 (the design says "workspace entry"; the spike decides whether that holds).
- [ ] **Step 2: `pyproject.toml`** for `das-client`: `requires-python = ">=3.10,<3.14"`, `dependencies = ["httpx==0.28.1"]`, hatchling build (same exact pin as the root: `hatchling==1.27.0`), `[tool.hatch.build.targets.wheel] packages = ["src/das_client"]`, `[tool.pytest.ini_options] asyncio_mode = "auto"` and `asyncio_default_fixture_loop_scope = "function"`, and `[tool.ruff] target-version = "py310"`, `line-length = 120`, `[tool.ruff.lint] select = ["E", "F"]`, so ruff flags nothing that 3.10 cannot run and the root config is not weakened.
- [ ] **Step 3: Guard tests first** (`clients/python/tests/test_package.py`): (1) no module under `src/das_client` imports `das` or `das.*` (parse each file with `ast`, Req 9.1); (2) `das_client.__all__` equals the names in "Public API" (start with what exists and extend per task).
- [ ] **Step 4: Root test** `tests/unit/test_client_package_static.py`: reads `clients/python/pyproject.toml` with `tomllib` and checks the name, `requires-python`, that the single dependency is exact-pinned and equal to the server's `httpx` pin in the root `pyproject.toml`. This keeps the two pins from drifting.
- [ ] **Step 5:** `uv lock`, `uv sync --locked`, then the four root gates, plus the client's suite under 3.12. Confirm `uv run lint-imports` is unchanged.
- [ ] **Step 6:** Tick 9.1.

### Task 2: Models, errors, retry and the two clients (spec 9.2)

**Files:** `errors.py`, `models.py`, `_transport.py`, `__init__.py`, and the two classes (`client.py`, `aclient.py`; each under ~250 lines).

Tests first, in `clients/python/tests/test_errors.py`, `test_models.py`, `test_retry.py`, `test_client_http.py` (respx):

- [ ] **Error mapping** (Req 9.5), one parametrized case per row: 401 and 403 `AuthError`; 404 `NotFound`; 409 `Conflict`; 422 `idempotency_key_reused` `IdempotencyKeyReused`; 400 `budget_above_limit` `BudgetAboveLimit` (`parameter`, `limit` from the body); any other 400 `InvalidRequest` (`parameter` if present); 429 `quota_exceeded` `QuotaExceeded(limit)`; any other 429 `RateLimited(retry_after)`; 503 `ServiceUnavailable`. A non-JSON or empty body still maps by status and gives `body == {}`. An unmapped status (for example 418 or 500) is a plain `DasError` carrying the status. All derive from `DasError`; `repr` and `str` contain neither the API key nor a request body.
- [ ] **Retry policy** (Req 9.4), with injected `sleep` and `rand`: connection errors (`httpx.TransportError`: connect errors, read timeouts, protocol errors), 503 and 429 are retried up to `max_retries` times (default 3, so at most 4 attempts); other statuses are not. Delay is `min(8.0, 0.5 * 2**attempt) * rand()` (full jitter, base 0.5 s, max 8 s); on 429 the delay is `max(delay, Retry-After)`, with a malformed or absent header ignored and a huge one honored as given. After the last attempt the mapped error is raised, and a transport failure raises `TransportFailure` (a new `DasError` subclass, the `httpx` error as `__cause__`). `max_retries=0` means one attempt.
- [ ] **Idempotency** (Req 9.3): `submit` without a key generates a UUID4 string **before the first attempt** and sends the same value on every retry of that call (assert from each recorded request's header); a second `submit` call generates a different key; an explicit key is sent unchanged. `submit` with `budget=None` sends no `budget` field; `Budget(max_cost=Decimal("1.5"))` sends `"1.5"` as a string; an empty `Budget()` is omitted.
- [ ] **Each method's request line:** `submit` is `POST /v1/requests` JSON `{prompt, budget?}` plus `Idempotency-Key`; `get` is `GET /v1/requests/{id}`; `cancel` is `POST /v1/requests/{id}/cancel`; `list_requests` is `GET /v1/requests` with only the given filters as query parameters (a `datetime` as ISO 8601 with `Z`, statuses as a comma list, `limit` as int). `X-API-Key` on every call. A request id is URL-quoted, so an id with `/` or `?` cannot change the path.
- [ ] **Models** (Req 9.9): `from_json` picks known fields and ignores unknown ones at every level (view, job, budget, webhook, list item, event), accepts `null` for every optional field, and a payload missing `request_id` or `status` raises `DasError`. Frozen dataclasses with `slots=True`. `max_cost` and `used.cost` parse to `Decimal`. An unknown `status` string stays a plain `str`.
- [ ] **Shared core:** `_transport.py` holds the pure parts: `build_headers`, `retry_delay`, `should_retry`, `error_from_response`, `parse_retry_after`. Both clients call them; only `send` and `sleep` differ.
- [ ] **Implement** to green. A caller-supplied `http_client` is **not** closed by `close()`. A trailing slash on `base_url` is tolerated. Both classes are context managers.
- [ ] **Gates and tick 9.2.** (9.5 stays open until Task 3.)

### Task 3: Events, wait and webhook verification (spec 9.3, finishes 9.5)

**Files:** `_sse.py`, `webhook.py`, the `events`/`wait` methods on both clients, `__init__.py` exports.

Tests first (`test_sse.py`, `test_events.py`, `test_wait.py`, `test_webhook_vectors.py`):

- [ ] **SSE parser** (pure): events split across arbitrary chunk boundaries (feed the same sample in every split position); `\n`, `\r\n` and `\r` line ends; multi-line `data:` joined with `\n`; `:` comment lines (the server's `: keepalive`) skipped; a missing space after the colon; an `id` containing NUL ignored (SSE spec); a frame with no `data` dropped; a frame whose `data` is not a JSON object skipped, not fatal (as the server skips a bad stored entry); an unterminated final frame dropped.
- [ ] **`events()` behaviour** (Req 9.6), respx with a byte stream that can break mid-frame:
  - Sends `Last-Event-ID` only when given, then updates it from each frame's `id`.
  - Yields the `snapshot` first as `Event(type="snapshot")`. On a **reconnect** the snapshot is swallowed unless it carries `resync: true`, in which case it is yielded again (events were lost).
  - Stops after yielding a terminal event, without reconnecting.
  - **EOF handling:** EOF after a terminal event: stop. EOF after a snapshot whose `status` is terminal with no further events: stop. EOF otherwise: reconnect.
  - **Reconnect budget:** reconnect on `httpx.TransportError`, a mid-stream break, a clean EOF without a terminal state, and on 503 and 429 from the reconnect itself (honoring `Retry-After`), up to `max_retries` times **in a row**; any received event resets the count. Past the budget raise the mapped error or `TransportFailure`. Non-retryable statuses (401, 404, 400) raise at once.
  - Read timeout is `stream_timeout` (default 60 s); a read timeout counts as a transport error and reconnects.
  - The generator closes its response if the consumer stops early.
- [ ] **`wait(request_id, *, timeout)`** (Req 9.6): returns the final `RequestView` from a `get` **after** the terminal event (the stream snapshot has no `final_result`, so `wait` calls `get` once more, even when the snapshot is already terminal). If `events()` raises `TransportFailure`, `ServiceUnavailable` or `RateLimited`, it polls `get` every 1.0 s (injected `sleep`) until terminal. If the deadline passes it raises `WaitTimeout` (a `DasError` and a `TimeoutError`). The deadline (injected `monotonic`) bounds streaming too, so a stream that never ends cannot hang `wait`; `NotFound` and `AuthError` propagate. Cover: terminal right away, terminal after events, stream failure then polling success, stream failure then timeout.
- [ ] **`verify_webhook(body, headers, secret, *, tolerance=300, now=None)`** (Req 9.7): `body` is `bytes` (a `str` is encoded as UTF-8), `headers` any mapping with **case-insensitive** lookup (`httpx.Headers`, a plain dict in any casing, Starlette headers), `secret` is `bytes | str`, `now` defaults to `time.time()`. A missing timestamp or signature header raises `InvalidSignature` (never `KeyError`). The signature is checked first; only a valid one lets the body be parsed, and a valid signature on a body that is not a JSON envelope (`request_id`, `event_id`, `type`, `data` of the right types) also raises `InvalidSignature`. Returns the parsed `Event`. `InvalidSignature` messages carry no secret, body or signature. The compare is `hmac.compare_digest`; a non-ASCII signature (`"v1=é"`) is "invalid", not a `TypeError`.
- [ ] **Shared vectors** (Req 9.8, spec 9.5): load `tests/fixtures/webhook_vectors.json` by a path relative to the test file, and run **every** entry of `vectors` (sign output equals the stored signature), `accept` and `reject` through the client's own `sign`/`verify` copy and through `verify_webhook` (building the three headers from the entry). A guard asserts the counts are 3, 2 and 8, so a vector file that silently loses entries fails.
- [ ] **Implement** to green; then gates, tick 9.3 and 9.5.

### Task 4: Property 8, webhook signature round trip (spec 9.4)

**Files:** `tests/property/test_webhook_signature_roundtrip.py` (new, main repo).

- [ ] **Write** the property with the header `# Feature: das-client-api, Property 8: Webhook signature round trip`. For any secret (bytes, 1 to 64), timestamp (0 to 2**40), body (bytes up to 2 KiB), `now` and `tolerance`:
  - `verify(secret, str(ts), body, sign(secret, ts, body), now, tol)` equals `abs(now - ts) <= tol`, for **both** `das.client.webhook.verify` and `das_client.webhook.verify`, and the two always agree (Req 9.8).
  - Changing any one of body, timestamp, signature or secret to a different value makes both return false (a changed timestamp is a different one within tolerance, since the signature covers it).
  - `verify_webhook` on headers built from the same values accepts exactly when the server's `verify` does, and returns an event equal to the envelope fields of a body made by `das.client.webhook.webhook_body`.
- [ ] **Check it can fail:** confirm a deliberately broken scratch copy of `verify` (for example skipping the tolerance check) makes the property fail before trusting the green run. Run with `HYPOTHESIS_PROFILE=ci` too.
- [ ] Tick 9.4.

### Task 5: Contract test (spec 9.6)

**Files:** `tests/contract/__init__.py`, `tests/contract/test_client_contract.py` (and a `conftest.py` only if a fixture must be shared).

- [ ] **Harness:** the real app from `create_app(state_store, AgentRegistry(fake_redis), ..., [KEY], ..., client_limits=..., events=...)` with the existing fakeredis fixtures. `AsyncDasClient(http_client=httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw"), ...)`. For the sync `DasClient`, add a small **loop bridge transport** in `tests/contract/` (an `httpx.BaseTransport` whose `handle_request` runs `ASGITransport.handle_async_request` on the test's own loop with `asyncio.run_coroutine_threadsafe`, reads the whole body, and returns a buffered `httpx.Response`); the test calls the sync client with `await asyncio.to_thread(...)`. Buffering is fine because every stream in the test ends (terminal Requests). Spike this first with a `get` call; if it deadlocks or fakeredis objects to the loop, fall back to running the sync client against a real uvicorn on an ephemeral port in a thread, and keep the in-process run for `AsyncDasClient`.
- [ ] **Cover every client method and error mapping against the real server, for both clients** (a parametrized `client_kind`):
  - `submit` with and without a key (202; the same key replays: `replayed=True`, one Request only); key reused with another prompt (`IdempotencyKeyReused`); invalid budget (`InvalidRequest`, `parameter == "budget.max_cost"`); budget above the key's default (`BudgetAboveLimit` with `limit`); quota full (`QuotaExceeded(limit)`, with `max_retries=0` so it is instant); empty bucket (`RateLimited`, `retry_after >= 1`); bad key (`AuthError`); unknown id (`NotFound`).
  - `get`: the view parses, `budget` and `webhook` are `None`; a Request with a budget shows its `BudgetView`.
  - `cancel`: `CancelResult("Cancelled")`, and a second cancel raises `Conflict`.
  - `list_requests`: filter by status; page through `next_cursor` until exhausted with no duplicates.
  - `events` to terminal: drive store writes directly for a finished Request (helpers from `tests/unit/test_state_phase4_events.py`), assert the snapshot then the ordered events and that the iterator ends by itself; with `last_event_id` earlier events are skipped.
  - `wait`: on a terminal Request it returns the view **with** `final_result`; the polling fallback is covered by making `events` fail once. (A stream that stays open cannot run in-process over `ASGITransport`; that is group 11's.)
- [ ] **Mismatch detection is the point:** one guard test patches the app's view to drop `status` and asserts the client raises `DasError`, so a server change that breaks a field the client needs cannot pass; one adds an unknown field and asserts the client still works (Req 9.9 against the real server).
- [ ] **Run it explicitly,** since `tests/contract` is not in the fast job yet; note for 10.1 that the fast job's command becomes `pytest tests/unit tests/property tests/contract`.
- [ ] **Gates, tick 9.6, then the group's checkpoint:** `uv run ruff check && uv run mypy && uv run lint-imports && uv run pytest tests/unit tests/property tests/contract`, plus the client's own suite on 3.12, and on 3.10 and 3.13 if `uv python list` shows them installed. If not, say so in the report: CI (group 10) is the first place 3.10 is really tested.

## Risks and open decisions

- **Workspace vs `requires-python` (highest risk).** See Task 1's spike and its fallback. If the workspace fails, the design's "workspace entry" becomes a path source plus an isolated client test command, and `design.md` §7 must say so (the spec is fixed before the design is changed, per the notes in `tasks.md`).
- **Sync client in the contract test.** `ASGITransport` is async only. The loop bridge is the plan; the live-uvicorn fallback is defined above. Either way the async client is covered fully in-process.
- **Additions beyond Req 9.5's class list, to be confirmed:** `TransportFailure` (network errors after retries; `wait()` needs something typed to fall back on), `WaitTimeout` (the deadline) and `CancelResult`. All are small and have no server counterpart. The per-key `too_many_streams` 429 on `events()` raises `RateLimited` (`.code == "too_many_streams"` tells callers apart); the spec has no dedicated class.
- **Retrying 429 `quota_exceeded`.** Req 9.4 retries every 429 and then raises `QuotaExceeded`. That wastes a few seconds on a quota that will not clear by itself, but the key makes it harmless for `submit`, and the spec says so. Kept as specified; `max_retries=0` avoids it.
- **Not planned (YAGNI):** a CLI, logging hooks, a webhook receiver helper, an `unasync`-generated sync client, mypy enforcement on the client in CI (the `py.typed` marker ships), publishing to PyPI.
- **What the contract test cannot prove:** a stream that stays open in-process, real network timing, and Python 3.10/3.13 behaviour. Real-Redis streaming and `das-client` `wait()` against the Compose stack are group 11's (11.1).
