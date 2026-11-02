"""Agent_Registry and availability logic (Requirement 11)."""

from das.registry.availability import (
    AVAILABLE,
    DRAINING,
    MISSED_INTERVALS,
    UNAVAILABLE,
    WORKER_ROLE,
    advertised_capabilities,
    capable_routing_capabilities,
    compute_status,
)
from das.registry.registry import (
    AGENTS_KEY,
    AgentInfo,
    AgentRegistry,
    RegistrySnapshot,
    agent_key,
)

__all__ = [
    "AGENTS_KEY",
    "AVAILABLE",
    "DRAINING",
    "MISSED_INTERVALS",
    "UNAVAILABLE",
    "WORKER_ROLE",
    "AgentInfo",
    "AgentRegistry",
    "RegistrySnapshot",
    "advertised_capabilities",
    "agent_key",
    "capable_routing_capabilities",
    "compute_status",
]
