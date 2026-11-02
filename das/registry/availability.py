"""Pure availability and capability logic for the Agent_Registry (Req 3.3, 4.5, 11.2, 11.7).

These functions take plain values so they can be property-tested without Redis.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

from das.models import AgentRecord, AgentStatus

# Each constant keeps its own Literal so it fits both AgentStatus and the
# narrower SettableStatus of Agent_Registry.heartbeat / set_status.
AVAILABLE: Final = "Available"
UNAVAILABLE: Final = "Unavailable"
DRAINING: Final = "Draining"
WORKER_ROLE = "worker"

# An agent is stale after this many missed heartbeat intervals (Req 11.2).
MISSED_INTERVALS = 3


def compute_status(last_heartbeat: float, interval: float, now: float) -> AgentStatus:
    """Unavailable iff ``now - last_heartbeat >= 3 * interval``, else Available."""
    if interval <= 0:
        raise ValueError("heartbeat interval must be positive")
    return UNAVAILABLE if now - last_heartbeat >= MISSED_INTERVALS * interval else AVAILABLE


def _available_workers(agents: Iterable[AgentRecord]) -> list[AgentRecord]:
    return [a for a in agents if a.role == WORKER_ROLE and a.status == AVAILABLE]


def advertised_capabilities(agents: Iterable[AgentRecord]) -> frozenset[str]:
    """Union of the Capabilities of every Available Worker_Agent (Req 3.3, 11.2, 11.7).

    Draining and Unavailable Workers advertise nothing (Req 17.6).
    """
    caps: set[str] = set()
    for agent in _available_workers(agents):
        caps |= agent.capabilities
    return frozenset(caps)


def capable_routing_capabilities(
    required: frozenset[str], agents: Iterable[AgentRecord]
) -> frozenset[str]:
    """Capabilities ``c`` in ``required`` that are safe routing queues for a Job (Req 4.5).

    ``c`` qualifies when at least one Available Worker advertises ``c`` and every
    Available Worker advertising ``c`` advertises all of ``required``. Publishing to
    ``jobs/min(result)`` therefore only reaches fully capable Workers.
    """
    required = frozenset(required)
    workers = _available_workers(agents)
    result: set[str] = set()
    for cap in required:
        holders = [w for w in workers if cap in w.capabilities]
        if holders and all(required <= w.capabilities for w in holders):
            result.add(cap)
    return frozenset(result)


__all__ = [
    "AVAILABLE",
    "DRAINING",
    "MISSED_INTERVALS",
    "UNAVAILABLE",
    "WORKER_ROLE",
    "advertised_capabilities",
    "capable_routing_capabilities",
    "compute_status",
]
