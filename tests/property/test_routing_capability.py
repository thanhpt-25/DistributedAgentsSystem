"""Property test for routing-capability selection (task 10.3).

``capable_routing_capabilities(R, agents)`` returns
``C = { c in R | every Available worker advertising c advertises all of R, and at least one does }``
(design: "Capability routing decision"). The Manager publishes to ``jobs/min(C)``.

Note on emptiness: ``C`` empty does NOT imply that no Available worker advertises all
of ``R``. With ``R = {a, b}`` and workers ``{a, b}``, ``{a}``, ``{b}`` a fully capable
worker exists, yet every queue in ``R`` also has a partial consumer, so ``C`` is empty.
The design implies only the one-way implication ``C non-empty => a fully capable
Available worker exists`` (equivalently: no capable worker => ``C`` empty).
"""

# Feature: distributed-agent-system, Property 17: Routing-capability selection is sound

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.models.agent import AgentRecord
from das.registry.availability import capable_routing_capabilities
from tests.strategies import capability_names, registry_snapshots


def _available_workers(agents):
    return [a for a in agents if a.role == "worker" and a.status == "Available"]


@st.composite
def snapshot_and_required(draw):
    snap = draw(registry_snapshots())
    pool = sorted({c for a in snap.agents for c in a.capabilities})
    # Mostly draw R from capabilities in the snapshot; occasionally add unknown ones.
    inner = draw(st.lists(st.sampled_from(pool), max_size=4)) if pool else []
    outer = draw(st.lists(capability_names, max_size=1))
    required = frozenset(inner) | frozenset(outer)
    if not required:
        required = frozenset(draw(st.lists(capability_names, min_size=1, max_size=2)))
    return snap, required


@settings(max_examples=300)
@given(snapshot_and_required())
def test_routing_capability_selection_is_sound(case):
    """**Validates: Requirements 4.5**"""
    snap, required = case
    agents = snap.agents
    workers = _available_workers(agents)
    capable = [w for w in workers if required <= w.capabilities]

    chosen = capable_routing_capabilities(required, agents)
    assert snap.capable_routing_capabilities(required) == chosen

    # Soundness: each c is in R, has a holder, and all holders are fully capable.
    for c in chosen:
        assert c in required
        holders = [w for w in workers if c in w.capabilities]
        assert holders
        assert all(required <= w.capabilities for w in holders)

    # Exactness against the design definition (no qualifying c is omitted).
    expected = frozenset(
        c
        for c in required
        if (h := [w for w in workers if c in w.capabilities])
        and all(required <= w.capabilities for w in h)
    )
    assert chosen == expected

    if chosen:
        # Publishing to jobs/min(C) reaches only fully capable Available workers, and some.
        dest = min(chosen)
        reached = [w for w in workers if dest in w.capabilities]
        assert reached
        assert all(required <= w.capabilities for w in reached)
        # Hence C non-empty implies a fully capable Available worker exists.
        assert capable
    else:
        # C empty iff every c in R lacks holders or has a partially capable holder.
        for c in required:
            holders = [w for w in workers if c in w.capabilities]
            assert not holders or any(not required <= w.capabilities for w in holders)

    # Contrapositive: no fully capable Available worker => C is empty.
    if not capable:
        assert chosen == frozenset()


def _worker(agent_id: str, caps: set[str], status: str = "Available") -> AgentRecord:
    return AgentRecord(
        agent_id=agent_id,
        role="worker",
        host="h",
        grpc_endpoint=None,
        capabilities=frozenset(caps),
        concurrency_limit=1,
        status=status,
        last_heartbeat=0.0,
    )


def test_empty_selection_despite_capable_worker():
    """C can be empty while a fully capable worker exists (definition is conservative)."""
    agents = [_worker("w1", {"a", "b"}), _worker("w2", {"a"}), _worker("w3", {"b"})]
    assert capable_routing_capabilities(frozenset({"a", "b"}), agents) == frozenset()


def test_partial_worker_excludes_only_its_queue():
    agents = [_worker("w1", {"a", "b"}), _worker("w2", {"a"})]
    assert capable_routing_capabilities(frozenset({"a", "b"}), agents) == frozenset({"b"})


def test_unavailable_partial_worker_is_ignored():
    agents = [_worker("w1", {"a", "b"}), _worker("w2", {"a"}, status="Unavailable")]
    assert capable_routing_capabilities(frozenset({"a", "b"}), agents) == frozenset({"a", "b"})
