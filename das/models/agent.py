"""Agent_Registry record (Req 11)."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from das.models.base import DasModel

# ``Draining`` (Req 17): alive and finishing in-flight work, but takes no new work.
AgentStatus = Literal["Available", "Unavailable", "Draining"]


class AgentRecord(DasModel):
    agent_id: str = Field(min_length=1)
    role: str = Field(min_length=1)
    host: str
    grpc_endpoint: str | None = None
    capabilities: frozenset[str] = frozenset()
    concurrency_limit: int | None = Field(default=None, ge=1, le=100)
    status: AgentStatus
    last_heartbeat: float
    # Jobs executing (Worker) or Leases held (Manager) per the latest heartbeat (Req 17.7).
    # v1 records lack it, so it defaults to 0.
    in_flight: int = Field(default=0, ge=0)
