"""Property test for the summarization trigger (task 12.13).

A consistent Request snapshot is drawn directly: a random DAG whose Jobs are assigned
states in topological order so that the snapshot is a fixed point of failure
propagation and readiness (a Job is Skipped iff a dependency ended Failed/Skipped/
Cancelled, Waiting iff some dependency is still unsettled, otherwise any of Succeeded,
Failed (final), Cancelled, Ready, Dispatched, or Failed/TimedOut awaiting a retry timer).
Each Job also carries Job_Results for its earlier failed attempts, so the Summarizer
inputs must come from the *current* Succeeded attempt only.

One ``Tick`` is stepped and the decision must contain:

- exactly one ``RunSummarizer`` and no ``FinalizeFailed`` when every Job is settled and
  at least one Succeeded, with inputs = prompt + exactly the Succeeded contents (7.1);
- exactly one ``FinalizeFailed`` and no ``RunSummarizer`` when every Job is settled and
  none Succeeded, its Final_Result listing every Job's ID, state, and reason (7.3);
- neither otherwise.
"""

# Feature: distributed-agent-system, Property 23: Summarization trigger and inputs

from __future__ import annotations

import hypothesis
from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.final_result import skip_reason
from das.manager.machine import (
    TIMER_RETRY,
    Decision,
    FinalizeFailed,
    MachineConfig,
    RequestSnapshot,
    RunSummarizer,
    StepContext,
    Tick,
    apply_decision,
    step,
)
from das.models import (
    AgentRecord,
    JobRecord,
    JobResultPayload,
    JobState,
    RequestStatus,
    TokenUsage,
)
from das.plan.dag import topo_order
from das.state import Timer
from tests.strategies import job_plans

S = JobState
RID = "r1"
CAP = "research"
PROMPT = "the prompt"
NOW = 100.0
CFG = MachineConfig(max_attempts=3, base_backoff=2, max_backoff=60, capability_wait=60)
WORKERS = (
    AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=frozenset({CAP}),
        status="Available",
        last_heartbeat=0.0,
    ),
)

SETTLED_KINDS = ("succeeded", "failed_final", "cancelled")
UNSETTLED_KINDS = ("ready", "dispatched", "failed_retry", "timed_out_retry")


def ctx() -> StepContext:
    return StepContext(now=NOW, config=CFG, agents=WORKERS)


def result(job_id: str, attempt: int, status: str, content: str, error: str | None = None):
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}",
        status=status,  # type: ignore[arg-type]
        content=content,
        error=error,
        usage=TokenUsage(input_tokens=attempt, output_tokens=attempt),
        duration_ms=1,
    )


def of(d: Decision, kind: type) -> list:
    return [e for e in d.effects if isinstance(e, kind)]


@st.composite
def snapshots(draw):
    plan = draw(job_plans(max_jobs=8, capabilities=frozenset({CAP})))
    by_id = {j.job_id: j for j in plan.jobs}
    # Bias toward fully settled plans so both trigger branches are exercised often.
    settled_only = draw(st.booleans())
    kinds: dict[str, str] = {}
    jobs: dict[str, JobRecord] = {}
    results: dict[tuple[str, int], JobResultPayload] = {}
    timers: list[Timer] = []

    def prior_failures(job_id: str, attempt: int) -> None:
        for k in range(1, attempt):
            results[(job_id, k)] = result(job_id, k, "Failed", f"bad {job_id}@{k}", "boom")

    for job_id in topo_order(plan):
        job = by_id[job_id]
        dead = [d for d in job.depends_on if kinds[d] in ("failed_final", "cancelled", "skipped")]
        pending = [d for d in job.depends_on if kinds[d] not in ("succeeded", "failed_final", "cancelled", "skipped")]
        if dead:
            kind = "skipped"
        elif pending:
            kind = "waiting"
        else:
            pool = SETTLED_KINDS if settled_only else SETTLED_KINDS + UNSETTLED_KINDS
            kind = draw(st.sampled_from(pool))
        kinds[job_id] = kind

        if kind == "skipped":
            jobs[job_id] = JobRecord(job=job, state=S.SKIPPED, reason=skip_reason(dead[0]))
        elif kind == "waiting":
            jobs[job_id] = JobRecord(job=job, state=S.WAITING)
        elif kind == "succeeded":
            a = draw(st.integers(1, CFG.max_attempts))
            prior_failures(job_id, a)
            results[(job_id, a)] = result(job_id, a, "Succeeded", f"ok {job_id}@{a}")
            jobs[job_id] = JobRecord(job=job, state=S.SUCCEEDED, attempt=a, worker_id=f"w-{job_id}")
        elif kind == "failed_final":
            a = draw(st.integers(1, CFG.max_attempts))
            prior_failures(job_id, a)
            if draw(st.booleans()):  # failed by a final Failed result
                results[(job_id, a)] = result(job_id, a, "Failed", f"bad {job_id}@{a}", "boom")
                jobs[job_id] = JobRecord(
                    job=job, state=S.FAILED, attempt=a, worker_id=f"w-{job_id}", reason="boom"
                )
            else:  # failed without a result (e.g. dispatch failure / no capable worker)
                jobs[job_id] = JobRecord(job=job, state=S.FAILED, attempt=a, reason="no capable worker")
        elif kind == "cancelled":
            a = draw(st.integers(1, CFG.max_attempts))
            prior_failures(job_id, a)
            if draw(st.booleans()):
                results[(job_id, a)] = result(job_id, a, "Cancelled", f"partial {job_id}@{a}", "stop")
            jobs[job_id] = JobRecord(job=job, state=S.CANCELLED, attempt=a, reason="stop")
        elif kind in ("ready", "dispatched"):
            a = draw(st.integers(1, CFG.max_attempts))
            prior_failures(job_id, a)
            state = S.READY if kind == "ready" else S.DISPATCHED
            jobs[job_id] = JobRecord(
                job=job, state=state, attempt=a, ready_since=NOW if state is S.READY else None
            )
        else:  # failed_retry / timed_out_retry: attempt < max with a pending retry timer
            a = draw(st.integers(1, CFG.max_attempts - 1))
            prior_failures(job_id, a)
            status = "Failed" if kind == "failed_retry" else "TimedOut"
            results[(job_id, a)] = result(job_id, a, status, f"bad {job_id}@{a}", "boom")
            jobs[job_id] = JobRecord(
                job=job, state=S(status), attempt=a, worker_id=f"w-{job_id}", reason="boom"
            )
            timers.append(Timer(kind=TIMER_RETRY, job_id=job_id, attempt=a, due=NOW + 10))

    snap = RequestSnapshot(
        request_id=RID,
        status=RequestStatus.RUNNING,
        prompt=PROMPT,
        plan=plan,
        jobs=jobs,
        results=results,
        timers=tuple(timers),
    )
    return snap, kinds


@settings(max_examples=300, deadline=None)
@given(snapshots())
def test_summarization_trigger_and_inputs(case):
    """**Validates: Requirements 7.1, 7.3**"""
    snap, kinds = case
    plan = snap.plan
    assert plan is not None

    decision = step(snap, Tick(), ctx())
    summarize = of(decision, RunSummarizer)
    finalize = of(decision, FinalizeFailed)

    # The drawn snapshot is a fixed point: a Tick changes no Job state.
    assert decision.changes == ()

    all_settled = all(k in ("succeeded", "failed_final", "cancelled", "skipped") for k in kinds.values())
    succeeded_ids = {j for j, k in kinds.items() if k == "succeeded"}
    branch = "summarize" if all_settled and succeeded_ids else "finalize" if all_settled else "none"
    hypothesis.event(f"branch={branch}")

    if branch == "summarize":
        assert len(summarize) == 1 and finalize == []
        eff = summarize[0]
        assert eff.prompt == PROMPT
        expected = {
            j: snap.results[(j, snap.jobs[j].attempt)].content for j in succeeded_ids
        }
        assert dict(eff.succeeded) == expected
        # No content from Failed/Skipped/Cancelled Jobs or from earlier failed attempts.
        excluded = {
            r.content for r in snap.results.values() if r.status != "Succeeded"
        }
        assert not excluded & set(eff.succeeded.values())
        assert set(eff.succeeded) <= succeeded_ids
    elif branch == "finalize":
        assert len(finalize) == 1 and summarize == []
        final = finalize[0].final_result
        assert final.summary is None
        assert final.error == "no job succeeded"
        assert [r.job_id for r in final.jobs] == plan.job_ids()
        assert {r.job_id: r.state for r in final.jobs} == {
            j: r.state for j, r in snap.jobs.items()
        }
        # 0 Succeeded: every Job is a failure entry with its terminal state and a reason.
        assert {f.job_id: f.state for f in final.failures} == {
            j: r.state for j, r in snap.jobs.items()
        }
        assert all(f.reason for f in final.failures)
    else:
        assert summarize == [] and finalize == []

    # The trigger is re-emitted on every step while the condition holds.
    again = step(apply_decision(snap, decision), Tick(), ctx())
    assert len(of(again, RunSummarizer)) == len(summarize)
    assert len(of(again, FinalizeFailed)) == len(finalize)
