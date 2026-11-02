"""Unit tests for das.manager.routing (Req 4.1, 4.5, 4.6)."""
from __future__ import annotations

from das.manager.routing import choose_routing_capability, jobs_destination
from das.models import AgentRecord


def _worker(agent_id: str, *caps: str, status: str = "Available") -> AgentRecord:
    return AgentRecord(
        agent_id=agent_id,
        role="worker",
        host="h",
        capabilities=frozenset(caps),
        status=status,  # type: ignore[arg-type]
        last_heartbeat=0.0,
    )


def test_single_capability_routes_to_its_queue() -> None:
    assert choose_routing_capability(["research"], [_worker("w1", "research")]) == "research"


def test_min_of_candidates_is_chosen() -> None:
    agents = [_worker("w1", "a", "b", "c")]
    assert choose_routing_capability(["c", "b"], agents) == "b"


def test_partial_consumer_disqualifies_queue() -> None:
    agents = [_worker("w1", "a", "b"), _worker("w2", "a")]
    assert choose_routing_capability(["a", "b"], agents) == "b"


def test_none_when_no_worker() -> None:
    assert choose_routing_capability(["a"], []) is None
    assert choose_routing_capability(["a"], [_worker("w1", "a", status="Unavailable")]) is None


def test_known_gap_fully_capable_worker_but_no_safe_queue() -> None:
    agents = [_worker("w1", "a", "b"), _worker("w2", "a"), _worker("w3", "b")]
    assert choose_routing_capability(["a", "b"], agents) is None


def test_destination() -> None:
    assert jobs_destination("research") == "jobs/research"
