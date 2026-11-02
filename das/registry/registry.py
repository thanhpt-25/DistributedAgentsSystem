"""Redis-backed Agent_Registry (Req 5.1, 11.1, 11.2, 11.6, 11.7).

Layout (matches the design's Redis key table):

``das:agent:{id}``  hash  AgentRecord fields plus ``heartbeat_interval``
``das:agents``      zset  agent_id scored by last_heartbeat

Every mutation is one Lua script, so a heartbeat and a sweep can't interleave.
Time comes from Redis ``TIME`` so heartbeat staleness is immune to host clock
skew. Tests may inject a ``time_source`` instead; its value is passed to the
scripts in place of ``TIME``.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Literal

from pydantic import Field

from das.models import AgentRecord, DasModel
from das.registry.availability import (
    AVAILABLE,
    DRAINING,
    MISSED_INTERVALS,
    UNAVAILABLE,
    advertised_capabilities,
    capable_routing_capabilities,
)

# Key names are local to the registry; they mirror the design's key layout.
KEY_PREFIX = "das"
AGENTS_KEY = f"{KEY_PREFIX}:agents"
AGENT_KEY_PREFIX = f"{KEY_PREFIX}:agent:"


def agent_key(agent_id: str) -> str:
    return f"{AGENT_KEY_PREFIX}{agent_id}"


TimeSource = Callable[[], Awaitable[float]]


class AgentInfo(DasModel):
    """What an agent declares when it registers (Req 11.6)."""

    agent_id: str = Field(min_length=1)
    role: Literal["manager", "worker", "gateway"]
    host: str
    grpc_endpoint: str | None = None
    capabilities: frozenset[str] = frozenset()
    concurrency_limit: int | None = Field(default=None, ge=1, le=100)
    heartbeat_interval: float = Field(default=5, ge=1, le=60)


class RegistrySnapshot(DasModel):
    """All registered agents (including Unavailable ones) plus Redis server time."""

    agents: tuple[AgentRecord, ...]
    server_time: float

    def get(self, agent_id: str) -> AgentRecord | None:
        return next((a for a in self.agents if a.agent_id == agent_id), None)

    def advertised_capabilities(self) -> frozenset[str]:
        return advertised_capabilities(self.agents)

    def capable_routing_capabilities(self, required: Iterable[str]) -> frozenset[str]:
        return capable_routing_capabilities(frozenset(required), self.agents)


# --------------------------------------------------------------------------- Lua

_NOW = """
local function now_from(arg)
  if arg ~= nil and arg ~= '' then return arg end
  local t = redis.call('TIME')
  return string.format('%d.%06d', tonumber(t[1]), tonumber(t[2]))
end
"""

# KEYS: agent hash, agents zset
# ARGV: agent_id, role, host, grpc_endpoint, capabilities_json, concurrency_limit,
#       heartbeat_interval, now ('' = server TIME)
_REGISTER = (
    _NOW
    + """
local now = now_from(ARGV[8])
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1],
  'agent_id', ARGV[1], 'role', ARGV[2], 'host', ARGV[3], 'grpc_endpoint', ARGV[4],
  'capabilities', ARGV[5], 'concurrency_limit', ARGV[6], 'heartbeat_interval', ARGV[7],
  'status', 'Available', 'last_heartbeat', now, 'in_flight', '0')
redis.call('ZADD', KEYS[2], now, ARGV[1])
return now
"""
)

# KEYS: agent hash, agents zset.
# ARGV: agent_id, now, desired_status ('Available'|'Draining', '' = Available), in_flight.
# Returns false for an unknown agent, else {now, previous_status}.
_HEARTBEAT = (
    _NOW
    + """
if redis.call('EXISTS', KEYS[1]) == 0 then return false end
local now = now_from(ARGV[2])
local desired = ARGV[3]
if desired == nil or desired == '' then desired = 'Available' end
local in_flight = ARGV[4]
if in_flight == nil or in_flight == '' then in_flight = '0' end
local prev = redis.call('HGET', KEYS[1], 'status')
redis.call('HSET', KEYS[1], 'status', desired, 'last_heartbeat', now, 'in_flight', in_flight)
redis.call('ZADD', KEYS[2], now, ARGV[1])
return {now, prev}
"""
)

# KEYS: agent hash.  ARGV: status ('Available'|'Draining'), in_flight ('' = keep).
# An agent's own drain/undrain transition (Req 17.2, 17.4, 17.8); doesn't touch
# last_heartbeat. Returns false for an unknown agent, else 1.
_SET_STATUS = """
if redis.call('EXISTS', KEYS[1]) == 0 then return false end
redis.call('HSET', KEYS[1], 'status', ARGV[1])
if ARGV[2] ~= nil and ARGV[2] ~= '' then
  redis.call('HSET', KEYS[1], 'in_flight', ARGV[2])
end
return 1
"""

# KEYS: agent hash, agents zset.  ARGV: agent_id.
_DEREGISTER = """
local n = redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return n
"""

# KEYS: agents zset.  ARGV: agent key prefix, now, missed intervals.
# Marks stale Available or Draining agents Unavailable (Req 17.6); returns their ids.
_SWEEP = (
    _NOW
    + """
local now = tonumber(now_from(ARGV[2]))
local missed = tonumber(ARGV[3])
local marked = {}
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local key = ARGV[1] .. id
  local f = redis.call('HMGET', key, 'status', 'last_heartbeat', 'heartbeat_interval')
  if not f[2] then
    redis.call('ZREM', KEYS[1], id)
  elseif (f[1] == 'Available' or f[1] == 'Draining') and now - tonumber(f[2]) >= missed * tonumber(f[3]) then
    redis.call('HSET', key, 'status', 'Unavailable')
    table.insert(marked, id)
  end
end
return marked
"""
)

# KEYS: agents zset.  ARGV: agent key prefix.
# Returns {sec, usec, {hgetall...}, ...}.
_SNAPSHOT = """
local t = redis.call('TIME')
local out = {t[1], t[2]}
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  local h = redis.call('HGETALL', ARGV[1] .. id)
  if #h > 0 then table.insert(out, h) end
end
return out
"""


def _s(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _record_from_flat(flat: list[Any]) -> AgentRecord:
    fields = {_s(flat[i]): _s(flat[i + 1]) for i in range(0, len(flat), 2)}
    return AgentRecord(
        agent_id=fields["agent_id"],
        role=fields["role"],
        host=fields["host"],
        grpc_endpoint=fields.get("grpc_endpoint") or None,
        capabilities=frozenset(json.loads(fields.get("capabilities") or "[]")),
        concurrency_limit=int(fields["concurrency_limit"])
        if fields.get("concurrency_limit")
        else None,
        status=fields["status"],
        last_heartbeat=float(fields["last_heartbeat"]),
        # v1 hashes have no in_flight field.
        in_flight=int(fields.get("in_flight") or 0),
    )


SettableStatus = Literal["Available", "Draining"]
_SETTABLE: frozenset[str] = frozenset({AVAILABLE, DRAINING})


def _check_settable(status: str) -> None:
    if status not in _SETTABLE:
        raise ValueError(f"status must be Available or Draining, got {status!r}")


def _check_in_flight(in_flight: int) -> None:
    if isinstance(in_flight, bool) or not isinstance(in_flight, int) or in_flight < 0:
        raise ValueError(f"in_flight must be a non-negative int, got {in_flight!r}")


class AgentRegistry:
    """Async Agent_Registry over a ``redis.asyncio`` client (or fakeredis)."""

    def __init__(self, redis: Any, time_source: TimeSource | None = None) -> None:
        self._redis = redis
        self._time_source = time_source
        self._register = redis.register_script(_REGISTER)
        self._heartbeat = redis.register_script(_HEARTBEAT)
        self._set_status = redis.register_script(_SET_STATUS)
        self._deregister = redis.register_script(_DEREGISTER)
        self._sweep = redis.register_script(_SWEEP)
        self._snapshot = redis.register_script(_SNAPSHOT)

    async def _now_arg(self, now: float | None = None) -> str:
        if now is None and self._time_source is not None:
            now = await self._time_source()
        return "" if now is None else repr(float(now))

    async def register(self, info: AgentInfo) -> float:
        """Register (or re-register) ``info`` with status Available (Req 11.6).

        Returns the stored ``last_heartbeat``.
        """
        args = [
            info.agent_id,
            info.role,
            info.host,
            info.grpc_endpoint or "",
            json.dumps(sorted(info.capabilities)),
            "" if info.concurrency_limit is None else str(info.concurrency_limit),
            repr(float(info.heartbeat_interval)),
            await self._now_arg(),
        ]
        now = await self._register(keys=[agent_key(info.agent_id), AGENTS_KEY], args=args)
        return float(_s(now))

    async def heartbeat(
        self,
        agent_id: str,
        desired_status: SettableStatus = AVAILABLE,
        in_flight: int = 0,
    ) -> bool:
        """Refresh ``agent_id``, set its status and in-flight count (Req 11.1, 11.7, 17.3).

        ``desired_status`` is the agent's own view (Available, or Draining while
        draining); a heartbeat also revives an agent the sweeper marked Unavailable.
        Returns False if the agent isn't registered, so the caller can re-register.
        """
        _check_settable(desired_status)
        _check_in_flight(in_flight)
        res = await self._heartbeat(
            keys=[agent_key(agent_id), AGENTS_KEY],
            args=[agent_id, await self._now_arg(), desired_status, str(in_flight)],
        )
        return bool(res)

    async def set_status(
        self,
        agent_id: str,
        status: SettableStatus,
        in_flight: int | None = None,
    ) -> bool:
        """Set ``agent_id``'s own status to Available or Draining (Req 17.2, 17.4, 17.8).

        Called by the agent when it handles drain/undrain, so the change is visible
        before the next heartbeat. ``in_flight`` is updated when given. Doesn't
        refresh ``last_heartbeat``. Returns False if the agent isn't registered.
        """
        _check_settable(status)
        if in_flight is not None:
            _check_in_flight(in_flight)
        res = await self._set_status(
            keys=[agent_key(agent_id)],
            args=[status, "" if in_flight is None else str(in_flight)],
        )
        return bool(res)

    async def deregister(self, agent_id: str) -> bool:
        """Remove ``agent_id``. Returns True if it was registered."""
        n = await self._deregister(keys=[agent_key(agent_id), AGENTS_KEY], args=[agent_id])
        return int(n) > 0

    async def snapshot(self) -> RegistrySnapshot:
        """Every registered agent plus server time, read atomically."""
        raw = await self._snapshot(keys=[AGENTS_KEY], args=[AGENT_KEY_PREFIX])
        server_time = int(_s(raw[0])) + int(_s(raw[1])) / 1_000_000
        if self._time_source is not None:
            server_time = await self._time_source()
        agents = tuple(_record_from_flat(flat) for flat in raw[2:])
        return RegistrySnapshot(agents=agents, server_time=server_time)

    async def sweep(self, now: float | None = None) -> list[str]:
        """Mark Available/Draining agents with ``now - last_heartbeat >= 3 * interval``
        Unavailable (Req 11.2, 17.6).

        ``now`` defaults to the injected time source, then Redis server time.
        Returns the ids newly marked Unavailable.
        """
        marked = await self._sweep(
            keys=[AGENTS_KEY],
            args=[AGENT_KEY_PREFIX, await self._now_arg(now), str(MISSED_INTERVALS)],
        )
        return [_s(m) for m in marked]


__all__ = [
    "AGENTS_KEY",
    "AVAILABLE",
    "DRAINING",
    "AgentInfo",
    "AgentRegistry",
    "RegistrySnapshot",
    "SettableStatus",
    "UNAVAILABLE",
    "agent_key",
]
