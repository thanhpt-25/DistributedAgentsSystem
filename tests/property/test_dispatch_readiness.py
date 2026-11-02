"""Property test for dispatch readiness and dependency inputs (task 12.7).

A random Job_Plan is driven to completion through ``step`` / ``apply_decision`` with a
random interleaving of ``Tick``, ``PublishConfirmed`` (current and stale attempts) and
Succeeded ``JobResultReceived`` events. After every step the readiness invariants of
design Property 15 are checked against the snapshot before (``pre``) and after
(``post``) the step:

- A Job is published only if it was Waiting and all its dependencies are Succeeded;
  each such Job gets exactly one ``PublishDispatch`` and moves to Ready (attempt + 1,
  ``ready_since == now``), not Dispatched.
- Every Waiting Job with a non-Succeeded dependency stays Waiting with no dispatch.
- ``dependency_results`` has exactly the dependency Job_IDs as keys, mapped to their
  stored Succeeded contents (count == ``len(depends_on)``).
- The dispatch is routed via ``choose_routing_capability``.
- A Job moves Ready -> Dispatched only on ``PublishConfirmed`` for its current attempt.
"""

# Feature: distributed-agent-system, Property 15: Dispatch readiness and dependency inputs

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishDispatch,
    RequestSnapshot,
    RunSummarizer,
    StepContext,
    Tick,
    apply_decision,
    step,
)
from das.manager.routing import choose_routing_capability
from das.models import (
    AgentRecord,
    JobPlan,
    JobRecord,
    JobResultPayload,
    JobState,
    RequestStatus,
    TokenUsage,
)
from tests.strategies import job_plans

S = JobState
RID = "r1"
CFG = MachineConfig(max_attempts=3, base_backoff=2, max_backoff=60, capability_wait=60)


def _worker(caps: frozenset[str]) -> AgentRecord:
    return AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=caps,
        status="Available",
        last_heartbeat=0.0,
    )


def _initial(plan: JobPlan) -> RequestSnapshot:
    return RequestSnapshot(
        request_id=RID,
        status=RequestStatus.RUNNING,
        prompt="the prompt",
        plan=plan,
        jobs={j.job_id: JobRecord(job=j, state=S.WAITING, attempt=0) for j in plan.jobs},
    )


def _result(job_id: str, attempt: int, content: str) -> JobResultPayload:
    return JobResultPayload(
        request_id=RID,
        job_id=job_id,
        attempt=attempt,
        worker_id="w1",
        status="Succeeded",
        content=content,
        usage=TokenUsage(),
        duration_ms=1,
    )


def _check_step(pre: RequestSnapshot, event, decision, post: RequestSnapshot, ctx: StepContext):
    dispatches = [e for e in decision.effects if isinstance(e, PublishDispatch)]
    ids = [d.job_id for d in dispatches]
    assert len(ids) == len(set(ids)), f"more than one dispatch per Job: {ids}"

    def deps_succeeded(job_id: str) -> bool:
        return all(post.jobs[d].state is S.SUCCEEDED for d in post.jobs[job_id].job.depends_on)

    expected = {
        j for j, rec in pre.jobs.items() if rec.state is S.WAITING and deps_succeeded(j)
    }
    assert set(ids) == expected

    for d in dispatches:
        before, after = pre.jobs[d.job_id], post.jobs[d.job_id]
        assert before.state is S.WAITING
        assert after.state is S.READY
        assert after.attempt == before.attempt + 1 == d.attempt
        assert after.ready_since == ctx.now
        assert d.routing_cap == choose_routing_capability(after.job.capabilities, ctx.agents)
        assert d.instruction == after.job.instruction
        deps = after.job.depends_on
        assert set(d.dependency_results) == set(deps)
        assert len(d.dependency_results) == len(deps)
        for dep in deps:
            dep_rec = post.jobs[dep]
            assert d.dependency_results[dep] == post.results[(dep, dep_rec.attempt)].content

    for j, rec in pre.jobs.items():
        if rec.state is S.WAITING and not deps_succeeded(j):
            assert post.jobs[j].state is S.WAITING
            assert j not in ids

    for j, rec in pre.jobs.items():
        if rec.state is not S.DISPATCHED and post.jobs[j].state is S.DISPATCHED:
            assert rec.state is S.READY
            assert isinstance(event, PublishConfirmed)
            assert (event.job_id, event.attempt) == (j, rec.attempt)


@settings(max_examples=200, deadline=None)
@given(plan=job_plans(max_jobs=8), data=st.data())
def test_dispatch_readiness_and_dependency_inputs(plan: JobPlan, data: st.DataObject):
    """**Validates: Requirements 4.1, 4.2, 4.3**"""
    caps = frozenset(c for j in plan.jobs for c in j.capabilities)
    now = 100.0
    snapshot = _initial(plan)
    contents = {j.job_id: data.draw(st.text(max_size=20), label=f"content {j.job_id}")
                for j in plan.jobs}
    dispatch_count = {j.job_id: 0 for j in plan.jobs}
    summarized = False

    event: object = PlanCommitted()
    random_steps = 6 * len(plan.jobs)
    for index in range(random_steps + 2 * len(plan.jobs) + 2):
        ctx = StepContext(now=now, config=CFG, agents=(_worker(caps),))
        decision = step(snapshot, event, ctx)
        post = apply_decision(snapshot, decision)
        _check_step(snapshot, event, decision, post, ctx)
        for e in decision.effects:
            if isinstance(e, PublishDispatch):
                dispatch_count[e.job_id] += 1
            elif isinstance(e, RunSummarizer):
                summarized = True
        snapshot = post
        if all(r.state is S.SUCCEEDED for r in snapshot.jobs.values()):
            break

        now += data.draw(st.floats(min_value=0.0, max_value=5.0), label="dt")
        ready = sorted(j for j, r in snapshot.jobs.items() if r.state is S.READY)
        running = sorted(
            j for j, r in snapshot.jobs.items() if r.state in (S.READY, S.DISPATCHED)
        )
        choices: list[str] = ["tick"]
        if ready:
            choices += ["confirm", "stale_confirm"]
        if running:
            choices += ["result", "result"]
        # After the random phase, finish the plan deterministically with results.
        if index >= random_steps:
            assert running, "an unfinished plan must have a Ready or Dispatched Job"
            kind = "result"
        else:
            kind = data.draw(st.sampled_from(choices), label="event")
        if kind == "tick":
            event = Tick()
        elif kind == "confirm":
            j = data.draw(st.sampled_from(ready))
            event = PublishConfirmed(job_id=j, attempt=snapshot.jobs[j].attempt)
        elif kind == "stale_confirm":
            j = data.draw(st.sampled_from(ready))
            event = PublishConfirmed(job_id=j, attempt=snapshot.jobs[j].attempt + 1)
        else:
            j = data.draw(st.sampled_from(running))
            event = JobResultReceived(result=_result(j, snapshot.jobs[j].attempt, contents[j]))
    else:
        raise AssertionError("plan did not complete within the step budget")

    # Every Job was dispatched exactly once and the plan completed.
    assert all(n == 1 for n in dispatch_count.values()), dispatch_count
    assert summarized
