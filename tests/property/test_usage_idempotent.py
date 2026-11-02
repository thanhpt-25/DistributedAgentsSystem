# Feature: das-observability-operations, Property 13: Kế toán idempotent
"""Property 13: idempotent token accounting.

For any Job_Plan and any delivery sequence of Job_Results (duplicate deliveries,
different payloads for the same Job/attempt, stale attempts, future attempts, results
for a Job that does not exist and results for a Request that does not exist) pushed
through ``ResultIngest.handle`` -> Request inbox -> ``RequestMachine.step`` ->
``apply_transition.lua`` on fakeredis[lua], the Token_Usage_Summary read back from the
State_Store equals the Token_Usage_Summary of that sequence after dropping duplicates
and attempts other than the Job's current attempt. The number of ``job`` Usage_Records
equals the number of Job_Results written to the results hash.

Inbox entries are turned into events exactly as the driver does (``InboxEntry`` ->
``JobResultReceived(entry.job_result(), _llm_meta(entry))``) and the inbox cursor is
committed with each transition. Failed/TimedOut results schedule retries; firing the
retry timer moves the Job to the next attempt, so earlier attempts become stale.

**Validates: Requirements 9.2, 9.5, 9.11**
"""

from __future__ import annotations

import asyncio
import dataclasses

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import HealthCheck, event, given, settings
from hypothesis import strategies as st

from das.codec import new_message
from das.manager.driver import _llm_meta
from das.manager.ingest import LLM_MODEL_HEADER, LLM_PROVIDER_HEADER, ResultIngest
from das.manager.machine import (
    TIMER_RETRY,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    RequestSnapshot,
    StepContext,
    TimerFired,
    step,
)
from das.models import (
    AgentRecord,
    JobPlan,
    JobResultPayload,
    JobState,
    PlannedJob,
    RequestStatus,
    TokenUsage,
)
from das.models.usage import UsageRecord
from das.ops.usage import summarize_usage
from das.state import StateStore, keys
from das.state.store import INBOX_JOB_RESULT
from das.transport.base import Delivery, Outcome

CAP = "research"
AGENTS = (
    AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=frozenset({CAP}),
        status="Available",
        last_heartbeat=0.0,
    ),
)
MISSING_JOB = "zz-missing"
MISSING_RID = "no-such-request"
STATUSES = ["Succeeded", "Failed", "TimedOut", "Cancelled"]
# Header values include case variants and absent/empty headers (-> "unknown").
PROVIDERS = st.sampled_from(["openai", "OpenAI", "anthropic", "", None])
MODELS = st.sampled_from(["gpt-x", "GPT-X", "claude", "", None])
USAGES = st.builds(
    TokenUsage,
    input_tokens=st.integers(0, 10_000),
    output_tokens=st.integers(0, 10_000),
    total_tokens=st.integers(0, 20_000),
)
OPS = st.sampled_from(
    ["result"] * 5 + ["duplicate"] * 3 + ["drain"] * 3 + ["retry"] * 2 + ["confirm"]
)


class AckTransport:
    """Records the settlement of each Delivery handed to ``ResultIngest``."""

    def __init__(self) -> None:
        self.outcomes: list[Outcome] = []

    async def acknowledge(self, delivery: Delivery, outcome: Outcome) -> None:
        self.outcomes.append(outcome)


def _headers(provider: str | None, model: str | None) -> dict[str, str]:
    h: dict[str, str] = {}
    if provider is not None:
        h[LLM_PROVIDER_HEADER] = provider
    if model is not None:
        h[LLM_MODEL_HEADER] = model
    return h


def _delivery(payload: JobResultPayload, headers: dict[str, str]) -> Delivery:
    message = new_message(payload, correlation_id=payload.request_id)
    return Delivery(
        message=message,
        destination="results",
        redelivered=False,
        delivery_count=0,
        headers=headers,
        token=object(),
    )


def _expected_record(payload: JobResultPayload, headers: dict[str, str]) -> UsageRecord:
    """Independent oracle of the Usage_Record of an accepted Job_Result (Req 9.2)."""
    return UsageRecord(
        operation="job",
        provider=headers.get(LLM_PROVIDER_HEADER) or "unknown",
        model=headers.get(LLM_MODEL_HEADER) or "unknown",
        usage=payload.usage,
        job_id=payload.job_id,
        attempt=payload.attempt,
    )


def _key(record: UsageRecord) -> str:
    return record.model_dump_json()


async def _scenario(data: st.DataObject) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    store = StateStore(redis)
    transport = AckTransport()
    ingest = ResultIngest(transport, store)  # type: ignore[arg-type]

    n_jobs = data.draw(st.integers(1, 3), label="jobs")
    job_ids = [f"j{i}" for i in range(n_jobs)]
    plan = JobPlan(
        jobs=[PlannedJob(job_id=j, instruction=f"do {j}", capabilities=[CAP]) for j in job_ids]
    )
    plan_usage = [
        UsageRecord(operation="plan", provider=p, model=m, usage=u)
        for p, m, u in data.draw(
            st.lists(
                st.tuples(st.sampled_from(["openai", "anthropic"]), st.just("m"), USAGES),
                max_size=2,
            ),
            label="plan usage",
        )
    ]
    cfg = MachineConfig(max_attempts=data.draw(st.integers(1, 3), label="max_attempts"))

    rid = await store.create_request("the prompt")
    lease = (await store.acquire_lease(rid, "m1", 3600.0)).lease
    assert lease is not None
    assert (await store.commit_plan(lease, plan, usage=plan_usage)).ok

    async def load() -> RequestSnapshot:
        stored = await store.load_snapshot(rid)
        assert stored is not None
        return RequestSnapshot.from_stored(stored)

    async def commit(ev: object, cursor: str | None = None) -> None:
        snap = await load()
        ctx = StepContext(now=await store.server_time(), config=cfg, agents=AGENTS)
        batch = step(snap, ev, ctx).updates
        if cursor is not None:
            batch = dataclasses.replace(batch, inbox_cursor=cursor)
        assert (await store.apply_transition(lease, batch)).ok

    await commit(PlanCommitted())

    # Every Delivery ever ingested, so "duplicate" can redeliver any of them.
    sent: list[tuple[JobResultPayload, dict[str, str]]] = []
    # Ingested entries of this Request: entry_id -> (payload, headers).
    by_entry: dict[str, tuple[JobResultPayload, dict[str, str]]] = {}
    expected: list[UsageRecord] = []
    accepted_keys: set[tuple[str, int]] = set()
    cursor = "0-0"
    stats = {"stale": 0, "future": 0, "dup": 0, "missing": 0}

    async def drain() -> bool:
        """Apply pending inbox entries like the driver; False once not Running."""
        nonlocal cursor
        for entry in await store.read_inbox(rid, after=cursor):
            snap = await load()
            if snap.status is not RequestStatus.RUNNING:
                return False
            assert entry.kind == INBOX_JOB_RESULT
            payload, headers = by_entry[entry.entry_id]
            # Oracle: drop duplicates and attempts other than the current attempt.
            key = (payload.job_id, payload.attempt)
            record = snap.jobs.get(payload.job_id)
            if record is None:
                stats["missing"] += 1
            elif payload.attempt < record.attempt:
                stats["stale"] += 1
            elif payload.attempt > record.attempt:
                stats["future"] += 1
            elif key in accepted_keys:
                stats["dup"] += 1
            else:
                assert record.state in (JobState.READY, JobState.DISPATCHED)
                accepted_keys.add(key)
                expected.append(_expected_record(payload, headers))
            await commit(JobResultReceived(entry.job_result(), _llm_meta(entry)), entry.entry_id)
            cursor = entry.entry_id
        return True

    running = True
    for n in range(data.draw(st.integers(3, 25), label="steps")):
        op = data.draw(OPS)
        if op in ("result", "duplicate"):
            if op == "duplicate" and sent:
                payload, headers = data.draw(st.sampled_from(sent))
            else:
                snap = await load()
                target = data.draw(
                    st.sampled_from(job_ids + [MISSING_JOB]), label="job"
                )
                cur = snap.jobs[target].attempt if target in snap.jobs else 1
                attempt = data.draw(st.integers(1, cur + 1), label="attempt")
                payload = JobResultPayload(
                    request_id=data.draw(st.sampled_from([rid] * 6 + [MISSING_RID])),
                    job_id=target,
                    attempt=attempt,
                    worker_id="w1",
                    status=data.draw(st.sampled_from(STATUSES)),  # type: ignore[arg-type]
                    content=f"out {n}",
                    error=None,
                    usage=data.draw(USAGES, label="usage"),
                    duration_ms=1,
                )
                headers = _headers(data.draw(PROVIDERS), data.draw(MODELS))
                sent.append((payload, headers))
            before = await redis.xlen(keys.inbox(rid))
            await ingest.handle(_delivery(payload, headers))
            assert transport.outcomes[-1] is Outcome.ACK
            entries = await store.read_inbox(rid, after="0-0", count=10_000)
            if payload.request_id == rid:
                assert len(entries) == before + 1
                by_entry[entries[-1].entry_id] = (payload, headers)
            else:
                # Unknown Request: nothing written anywhere (Req 9.5).
                assert len(entries) == before
                assert not await redis.exists(keys.inbox(MISSING_RID))
        elif op == "drain":
            running = await drain()
        elif op == "retry":
            snap = await load()
            if snap.status is not RequestStatus.RUNNING:
                running = False
            else:
                for t in list(snap.timers):
                    if t.kind == TIMER_RETRY:
                        await commit(TimerFired(TIMER_RETRY, t.job_id, t.attempt))
        else:
            snap = await load()
            if snap.status is not RequestStatus.RUNNING:
                running = False
            else:
                for job_id, rec in snap.jobs.items():
                    if rec.state is JobState.READY:
                        await commit(PublishConfirmed(job_id, rec.attempt))
        if not running:
            break
    if running:
        await drain()

    stored_usage = await store.load_usage(rid)
    job_usage = [r for r in stored_usage if r.operation == "job"]
    plan_stored = [r for r in stored_usage if r.operation == "plan"]
    results_count = await redis.hlen(keys.results(rid))

    # Number of ``job`` Usage_Records == number of Job_Results written (Req 9.2, 9.5).
    assert len(job_usage) == results_count == len(accepted_keys)
    # Exactly the oracle's records (provider/model from headers, usage from the result).
    assert sorted(map(_key, job_usage)) == sorted(map(_key, expected))
    assert sorted(map(_key, plan_stored)) == sorted(map(_key, plan_usage))
    # Token_Usage_Summary equals the summary of the deduplicated sequence (Req 9.11).
    assert summarize_usage(stored_usage, job_ids) == summarize_usage(
        [*plan_usage, *expected], job_ids
    )

    for name, count in stats.items():
        if count:
            event(f"{name} results discarded")
    await redis.aclose()


@settings(
    max_examples=150,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(data=st.data())
def test_usage_accounting_is_idempotent(data: st.DataObject) -> None:
    """**Validates: Requirements 9.2, 9.5, 9.11**"""
    asyncio.run(_scenario(data))
