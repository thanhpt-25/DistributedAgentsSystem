"""Property test for the capability wait timeout (task 12.9).

A root Job enters Ready on ``PlanCommitted`` at ``t0`` (``ready_since = t0``). It is then
driven by a sequence of ``Tick`` / ``TimerFired(capwait)`` events at non-decreasing times,
each with its own registry snapshot. "Routing capability exists" means
``capable_routing_capabilities(R, agents)`` is non-empty for that step's snapshot (not
"a fully capable worker exists": the design's definition of ``C`` is conservative).

Model: the Job is dispatched (to ``jobs/min(C)``) at the first step whose snapshot has a
non-empty ``C``, provided no earlier step already failed it; it fails with
``no capable worker`` and is never published at the first step with ``now - t0 >=
capability_wait`` when every step so far (inclusive) had an empty ``C``.
"""

# Feature: distributed-agent-system, Property 18: Capability wait timeout

from __future__ import annotations

from hypothesis import event, given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    REASON_NO_CAPABLE_WORKER,
    TIMER_CAPABILITY_WAIT,
    MachineConfig,
    PlanCommitted,
    PublishDispatch,
    RequestSnapshot,
    StepContext,
    Tick,
    TimerFired,
    apply_decision,
    step,
)
from das.models import JobPlan, JobRecord, JobState, PlannedJob, RequestStatus
from das.registry.availability import capable_routing_capabilities
from das.state import TimerKey
from tests.strategies import agent_records, capability_names

JOB = "j1"
WAIT = 60.0
CFG = MachineConfig(capability_wait=WAIT)
T0 = 1_000.0


def _snapshot(required: list[str]) -> RequestSnapshot:
    plan = JobPlan(
        jobs=[PlannedJob(job_id=JOB, instruction="do it", capabilities=required, depends_on=[])]
    )
    return RequestSnapshot(
        request_id="r1",
        status=RequestStatus.RUNNING,
        prompt="the prompt",
        plan=plan,
        jobs={JOB: JobRecord(job=plan.jobs[0], state=JobState.WAITING, attempt=0)},
    )


@st.composite
def scenarios(draw):
    pool = draw(st.lists(capability_names, min_size=1, max_size=3, unique=True))
    required = sorted(draw(st.lists(st.sampled_from(pool), min_size=1, max_size=3, unique=True)))
    agents_st = st.one_of(
        st.just(()),
        st.lists(agent_records(capability_pool=pool), max_size=4).map(tuple),
    )
    initial = draw(agents_st)
    # Integer deltas hit the `>= capability_wait` boundary exactly in some examples.
    steps = draw(
        st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=int(WAIT // 2)),
                agents_st,
                st.booleans(),  # True: Tick, False: TimerFired(capwait)
            ),
            min_size=1,
            max_size=12,
        )
    )
    return required, initial, steps


@settings(max_examples=300, deadline=None)
@given(scenarios())
def test_capability_wait_timeout(case):
    """**Validates: Requirements 4.6**"""
    required, initial, steps = case
    req = frozenset(required)

    snap = _snapshot(required)
    now = T0
    events = [(now, initial, PlanCommitted())]
    for delta, agents, is_tick in steps:
        now += delta
        events.append((now, agents, Tick() if is_tick else TimerFired(TIMER_CAPABILITY_WAIT, JOB, 1)))

    # Model outcome: index of the first dispatch or of the failure, else still waiting.
    expected: tuple[str, int] | None = None
    for i, (t, agents, _) in enumerate(events):
        if capable_routing_capabilities(req, agents):
            expected = ("dispatch", i)
            break
        if t - T0 >= WAIT:
            expected = ("fail", i)
            break

    event(f"outcome: {expected[0] if expected else 'waiting'}")

    dispatched_at: list[int] = []
    failed_at: list[int] = []
    for i, (t, agents, ev) in enumerate(events):
        decision = step(snap, ev, StepContext(now=t, config=CFG, agents=tuple(agents)))
        pubs = [e for e in decision.effects if isinstance(e, PublishDispatch)]
        if pubs:
            assert len(pubs) == 1
            assert pubs[0].routing_cap == min(capable_routing_capabilities(req, agents))
            dispatched_at.append(i)
        snap = apply_decision(snap, decision)
        record = snap.jobs[JOB]
        if i == 0:
            assert record.state is JobState.READY and record.ready_since == T0
        if record.state is JobState.FAILED and not failed_at:
            failed_at.append(i)

    record = snap.jobs[JOB]
    capwait = TimerKey(TIMER_CAPABILITY_WAIT, JOB, 1)
    if expected is None:
        # Never capable, wait not yet elapsed: still Ready and waiting.
        assert not dispatched_at and not failed_at
        assert record.state is JobState.READY and record.ready_since == T0
        assert capwait in snap.timer_keys()
    elif expected[0] == "dispatch":
        assert dispatched_at == [expected[1]]
        assert not failed_at
        assert record.state is JobState.READY  # publish in flight
        assert capwait not in snap.timer_keys()
    else:
        assert failed_at == [expected[1]]
        assert not dispatched_at
        assert record.state is JobState.FAILED
        assert record.reason == REASON_NO_CAPABLE_WORKER
        assert capwait not in snap.timer_keys()
