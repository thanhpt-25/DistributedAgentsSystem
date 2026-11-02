"""Routing-capability selection for Job dispatch (Requirements 4.1, 4.5, 4.6). Pure.

A Job requiring the Capability set ``R`` is published to the per-Capability queue
``jobs/min(C)``, where ``C = capable_routing_capabilities(R, agents)`` is the set of
Capabilities ``c ∈ R`` such that at least one Available Worker advertises ``c`` and every
Available Worker advertising ``c`` advertises all of ``R``. When ``C`` is empty no queue
is safe and the Job waits (and eventually fails with ``no capable worker``, Req 4.6).

Known design gap: ``C`` can be empty even though a fully capable Worker exists. With
Workers ``{a, b}``, ``{a}``, ``{b}`` and ``R = {a, b}``, both queues ``a`` and ``b`` have
a partially capable consumer, so neither qualifies. DAS follows the design here: the Job
fails with ``no capable worker`` once the capability wait elapses.
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Final

from das.models import AgentRecord
from das.registry.availability import capable_routing_capabilities

JOBS_DESTINATION_PREFIX: Final = "jobs/"

__all__ = ["JOBS_DESTINATION_PREFIX", "choose_routing_capability", "jobs_destination"]


def choose_routing_capability(
    required: Iterable[str], agents: Iterable[AgentRecord]
) -> str | None:
    """Deterministic routing Capability ``min(C)`` for a Job, or None when ``C`` is empty."""
    candidates = capable_routing_capabilities(frozenset(required), agents)
    return min(candidates) if candidates else None


def jobs_destination(capability: str) -> str:
    """Transport destination of the dispatch queue for one Capability."""
    return f"{JOBS_DESTINATION_PREFIX}{capability}"
