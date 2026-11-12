"""Redis State_Store: typed async API over atomic Lua scripts.

Every multi-key change goes through one Lua script (see ``das/state/lua``), so it is
atomic on a single Redis primary. Scripts that a Manager calls take the Lease's
``(owner, epoch)`` fencing pair and return ``LEASE_LOST`` when it does not match the
current unexpired Lease (Req 2.7, 3.12), and ``REQUEST_TERMINAL`` when the Request is
already Completed, Failed, or Cancelled (Req 1.7). Lease expiry and transition
timestamps use Redis server time.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Final, Literal

from das.client.budget import Budget, budget_status, budget_view
from das.client.events import planned_event_data
from das.models import (
    TERMINAL_JOB,
    TERMINAL_REQUEST,
    FinalResult,
    JobPlan,
    JobRecord,
    JobResultPayload,
    JobState,
    Lease,
    RequestStatus,
    TokenUsage,
    TransitionEntry,
)
from das.models.dlq import (
    DEAD_LETTER_REASONS,
    RECORD_STATUSES,
    DeadLetterFields,
    DeadLetterReason,
    DeadLetterRecord,
    RecordStatus,
    ReplayHistoryEntry,
    RequestTransitionEntry,
)
from das.models.usage import UsageRecord
from das.observability import get_logger
from das.observability.trace_context import TraceContext
from das.ops.cost import PriceTable
from das.ops.replay import DiscardOutcome, ReplayOutcome
from das.state import keys
from das.state.scripts import register_all

log = get_logger(__name__)

INBOX_JOB_RESULT: Final = "job_result"
INBOX_UNACKED_EXHAUSTED: Final = "unacked_delivery_exhausted"
#: Inbox kind of an accepted Replay; data = {dead_letter_id, job_id, attempt} (design D7).
INBOX_REPLAY: Final = "replay"

CANCEL_REASON: Final = "cancelled: request cancelled"

# Event_Stream length cap of a Request without a stored events_max_len (Phase 4 Req 6.5).
DEFAULT_EVENTS_MAX_LEN: Final = 1000

# Tenant_ID (Phase 5 Req 1.1); the config schema and create_request.lua check the same shape.
_TENANT_ID: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}")
#: Range of ``usage.retention_days`` (Phase 5 Req 6.6).
USAGE_RETENTION_DAYS: Final = (30, 3650)

# Owner-checked PEXPIRE / DEL of a single-key lock (IndexBackfill, Req 15.10).
_RENEW_LOCK_LUA: Final = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then "
    "return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end"
)
_RELEASE_LOCK_LUA: Final = (
    "if redis.call('GET', KEYS[1]) == ARGV[1] then "
    "return redis.call('DEL', KEYS[1]) else return 0 end"
)


@dataclass(frozen=True, slots=True)
class ClientRequestSpec:
    """Phase 4 fields of a new Request (Phase 4 Req 3.1, 4.2, 5.4, 6.5, 8.1).

    ``idempotency_key_hash`` is ``das.client.idempotency.idem_key_hash`` of the
    Idempotency-Key and needs ``body_hash``. ``max_active_requests == 0`` means no quota.
    The webhook target is stored only when both webhook fields are set.
    """

    fingerprint: str
    budget: Budget | None = None
    webhook_url: str | None = None
    webhook_secret_name: str | None = None
    idempotency_key_hash: str | None = None
    body_hash: str | None = None
    idempotency_ttl: int = 86_400
    max_active_requests: int = 0
    events_max_len: int = DEFAULT_EVENTS_MAX_LEN
    # Phase 5: the tenant the Request belongs to, its Tenant_Quota (0 = none) and its priority (Req 3.1, 3.7, 7.3).
    # ``tenant`` None leaves the field off (the Default_Tenant); priority is stored only when ``high``.
    tenant: str | None = None
    tenant_max_active: int = 0
    priority: str | None = None

    def __post_init__(self) -> None:
        if self.tenant is not None and not _TENANT_ID.fullmatch(self.tenant):
            raise ValueError(f"tenant is not a valid Tenant_ID: {self.tenant!r}")
        if self.priority not in (None, "normal", "high"):
            raise ValueError(f"priority must be normal or high, not {self.priority!r}")
        if self.tenant_max_active < 0:
            raise ValueError("tenant_max_active must be >= 0")


CreateOutcome = Literal[
    "created", "replayed", "idempotency_conflict", "quota_exceeded", "tenant_quota_exceeded"
]


@dataclass(frozen=True, slots=True)
class CreateResult:
    """Outcome of :meth:`StateStore.submit_request` (Phase 4 Req 3.2, 4.2-4.5)."""

    outcome: CreateOutcome
    request_id: str | None = None  # created / replayed
    limit: int | None = None  # quota_exceeded


@dataclass(frozen=True, slots=True)
class WebhookState:
    """Delivery state of a Request's Webhook_Delivery (Phase 4 Req 8.2, 9.7).

    ``last_status`` is ``None`` until a delivery attempt recorded one.
    """

    state: str
    attempts: int
    last_status: str | None


@dataclass(frozen=True, slots=True)
class WebhookClaim:
    """A claimed Webhook_Delivery (Phase 4 Req 8.3).

    ``claim`` and ``event_id`` together fence :meth:`StateStore.complete_webhook`
    (a later terminal write resets the delivery with a new ``event_id``);
    ``attempts`` counts the attempts made before this one. ``claimed_at`` is the Redis
    time of the claim in seconds: the ``now`` to pass to ``das.client.webhook.next_due``,
    so the sender schedules a retry without its host clock.
    """

    request_id: str
    claim: int  # with event_id, fences complete_webhook
    attempts: int  # attempts made before this one
    url: str
    secret_name: str
    event_id: str
    event_type: str
    data: dict[str, Any]
    claimed_at: float  # Redis seconds of the claim; the ``now`` for next_due



WebhookOutcome = Literal["delivered", "retry", "failed"]
_WEBHOOK_OUTCOMES: Final = frozenset({"delivered", "retry", "failed"})


class StoreStatus(StrEnum):
    OK = "OK"
    LEASE_LOST = "LEASE_LOST"
    REQUEST_TERMINAL = "REQUEST_TERMINAL"  # Manager write on a Completed/Failed/Cancelled Request
    TERMINAL = "TERMINAL"  # Gateway cancel on a terminal Request (Req 1.8)
    NOT_FOUND = "NOT_FOUND"
    HELD = "HELD"  # acquire: another Manager holds an unexpired Lease
    INVALID_STATE = "INVALID_STATE"  # e.g. commit_plan on a Running Request
    # finalize: a Replay was accepted but not yet applied by the owner (Req 13.15).
    REPLAY_PENDING = "REPLAY_PENDING"


class StoreError(Exception):
    """Unexpected State_Store condition (not a normal script outcome)."""


LEASE_KIND_INITIAL: Final = "initial"
LEASE_KIND_TAKEOVER: Final = "takeover"


@dataclass(frozen=True, slots=True)
class AcquireResult:
    status: StoreStatus
    lease: Lease | None = None
    # On OK: "takeover" when the previous acquire was by another agent, else "initial" (Req 3.3).
    kind: str | None = None

    @property
    def ok(self) -> bool:
        return self.status is StoreStatus.OK


@dataclass(frozen=True, slots=True)
class RenewResult:
    status: StoreStatus
    expires_at: float | None = None

    @property
    def ok(self) -> bool:
        return self.status is StoreStatus.OK


@dataclass(frozen=True, slots=True)
class WriteResult:
    status: StoreStatus
    at: datetime | None = None  # Redis-time timestamp stamped on transitions
    transitions_written: int = 0
    detail: str | None = None  # e.g. current status for INVALID_STATE
    # finalize on OK (Phase 2): Redis-time finished_at and the Request's created_at,
    # both epoch ms, for das_request_duration_seconds (Req 2.2, 14.2).
    finished_at_ms: int | None = None
    created_at_ms: int | None = None

    @property
    def ok(self) -> bool:
        return self.status is StoreStatus.OK


@dataclass(frozen=True, slots=True)
class CancelResult:
    status: StoreStatus  # OK | TERMINAL | NOT_FOUND
    request_status: RequestStatus | None = None  # current status when TERMINAL
    # On OK (Phase 2): Redis-time cancel timestamp, finished_at and created_at in
    # epoch ms (Req 2.3, 14.2).
    at: datetime | None = None
    finished_at_ms: int | None = None
    created_at_ms: int | None = None

    @property
    def ok(self) -> bool:
        return self.status is StoreStatus.OK


@dataclass(frozen=True, slots=True)
class TimerKey:
    kind: str
    job_id: str
    attempt: int

    @property
    def member(self) -> str:
        return keys.timer_member(self.kind, self.job_id, self.attempt)


@dataclass(frozen=True, slots=True)
class Timer:
    kind: str
    job_id: str
    attempt: int
    due: float

    @property
    def key(self) -> TimerKey:
        return TimerKey(self.kind, self.job_id, self.attempt)


@dataclass(frozen=True, slots=True)
class TransitionBatch:
    """Changes applied atomically by ``apply_transition``.

    Transition-log entries are derived by the script: one entry for every Job whose
    stored state differs from the new record's state (Req 13.7).
    """

    jobs: Mapping[str, JobRecord] = field(default_factory=dict)
    results: Sequence[JobResultPayload] = ()
    timers_add: Sequence[Timer] = ()
    timers_remove: Sequence[TimerKey] = ()
    inbox_cursor: str | None = None
    request_fields: Mapping[str, str] = field(default_factory=dict)
    # Phase 2. ``usage``: UsageRecords of Job_Results in ``results`` (operation ``job``);
    # each is stored only when its result is new, i.e. not a duplicate (Req 9.2, 9.5).
    usage: Sequence[UsageRecord] = ()
    # Dead_Letter_ID written on every transition entry of the batch (Req 13.4).
    tag: str | None = None
    # Dead_Letter_IDs removed from ``pending_replays`` (replay applied or rejected).
    replays_applied: Sequence[str] = ()
    # Dead_Letter_IDs whose record goes back Replayed -> Pending (Req 13.15).
    dead_letter_reverts: Sequence[str] = ()
    job_count: int | None = None


@dataclass(frozen=True, slots=True)
class CreateDeadLetterResult:
    """``created`` is False when the record already existed (redelivery, Req 12.2)."""

    created: bool
    dead_lettered_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Outcome of ``replay_dead_letter`` (codes of :data:`das.ops.replay.ReplayOutcome`).

    ``accepted`` carries ``request_status_before`` and ``new_attempt``; a rejection
    carries the current state of the violating object (or ``None``) in ``current_state``.
    """

    outcome: ReplayOutcome
    current_state: str | None = None
    request_id: str | None = None
    job_id: str | None = None
    new_attempt: int | None = None
    request_status_before: RequestStatus | None = None

    @property
    def accepted(self) -> bool:
        return self.outcome == "accepted"


@dataclass(frozen=True, slots=True)
class DiscardResult:
    """Outcome of ``discard_dead_letter`` (:data:`das.ops.replay.DiscardOutcome`).

    ``accepted`` and ``already`` carry the discard time (Req 13.11, 13.12);
    ``record_not_pending`` carries the current Record_Status.
    """

    outcome: DiscardOutcome
    status: RecordStatus | None = None
    discarded_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DeadLetterPage:
    """One page of ``list_dead_letters``: records in (dead_lettered_at desc, dlid desc)
    order; ``next_after`` is the ``(score_ms, dlid)`` of the last record when more
    matching records follow, else ``None`` (Req 12.5)."""

    records: list[DeadLetterRecord]
    next_after: tuple[int, str] | None = None


#: Reply of ``purge_request`` (``STAMPED``: finished_at was missing and is now set, Req 14.9).
PURGE_DELETED: Final = "DELETED"
PURGE_KEEP: Final = "KEEP"
PURGE_STAMPED: Final = "STAMPED"

#: Request_Status order of the five ``by_status`` KEYS of ``purge_request`` / ``index_request``.
_STATUS_ORDER: Final = (
    RequestStatus.PENDING,
    RequestStatus.RUNNING,
    RequestStatus.COMPLETED,
    RequestStatus.FAILED,
    RequestStatus.CANCELLED,
)


@dataclass(frozen=True, slots=True)
class RequestListItem:
    """One entry of ``GET /v1/requests`` (Req 15.3): no prompt, plan or results.

    ``finished_at_ms`` is ``None`` unless the Request is terminal; ``job_count`` is 0
    without a Job_Plan and ``usage`` is zero without Usage_Records.
    """

    request_id: str
    status: RequestStatus
    created_at_ms: int
    finished_at_ms: int | None = None
    job_count: int = 0
    usage: TokenUsage = field(default_factory=TokenUsage)


@dataclass(frozen=True, slots=True)
class RequestPage:
    """One page of ``list_requests`` in (created_at desc, Request_ID desc) order.

    ``next_after`` is the ``(created_at_ms, request_id)`` position to resume after when
    at least one more matching Request follows, else ``None`` (Req 15.4, 15.7)."""

    items: list[RequestListItem]
    next_after: tuple[int, str] | None = None


@dataclass(frozen=True, slots=True)
class InboxEntry:
    entry_id: str
    kind: str
    data: str  # JSON
    # Phase 2: provider/model JSON of a Job_Result taken from the Message headers (Req 9.2).
    meta: str | None = None

    def meta_payload(self) -> dict[str, Any] | None:
        """Decoded ``meta``; ``None`` when absent or not a JSON object."""
        if not self.meta:
            return None
        try:
            data = json.loads(self.meta)
        except ValueError:
            return None
        return data if isinstance(data, dict) else None

    def job_result(self) -> JobResultPayload:
        if self.kind != INBOX_JOB_RESULT:
            raise StoreError(f"inbox entry {self.entry_id} is {self.kind}, not a job result")
        return JobResultPayload.model_validate_json(self.data)

    def payload(self) -> Any:
        return json.loads(self.data)


@dataclass(frozen=True, slots=True)
class StoredRequest:
    """Everything persisted for one Request, read in one MULTI."""

    request_id: str
    status: RequestStatus
    prompt: str
    created_at: float
    error: str | None
    final_result: FinalResult | None
    plan: JobPlan | None
    jobs: dict[str, JobRecord]  # in plan order when a plan exists
    results: dict[tuple[str, int], JobResultPayload]
    timers: list[Timer]  # ordered by due time
    inbox_cursor: str
    lease: Lease | None  # None when nobody holds it; may be expired (compare with server time)
    transitions: list[TransitionEntry] | None = None  # only when requested (Job entries)
    # Phase 2 fields (absent on Requests written by v1 scripts).
    trace_context: TraceContext | None = None  # persisted by the Gateway (Req 6.1)
    finished_at: int | None = None  # epoch ms, set when the Request became terminal (Req 14.2)
    job_count: int | None = None
    usage_totals: TokenUsage = field(default_factory=TokenUsage)
    replay_history: list[ReplayHistoryEntry] = field(default_factory=list)
    # Request-level entries of the transitions list; only when transitions are requested.
    request_transitions: list[RequestTransitionEntry] | None = None
    usage: list[UsageRecord] | None = None  # UsageRecords, only when requested
    # Phase 4 fields (absent on Requests created without a client, Phase 4 Req 5.4).
    budget: Budget | None = None  # Effective_Budget
    client_fp: str | None = None
    # Only with ``load_snapshot(include_client=True)`` (Phase 4 Req 8.9, 7.1).
    webhook: WebhookState | None = None  # None: no Webhook_Delivery exists
    last_event_id: str | None = None  # newest Event_ID; None when the Event_Stream is empty


def _s(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


#: Fields of the Webhook_Delivery hash a Request view shows (Phase 4 Req 8.9).
_WEBHOOK_FIELDS: Final = ("state", "attempts", "last_status")


def _webhook_state(raw: Sequence[Any]) -> WebhookState | None:
    """``HMGET`` reply over :data:`_WEBHOOK_FIELDS` as a state, or ``None`` without a delivery."""
    if raw[0] is None:
        return None
    return WebhookState(
        state=_s(raw[0]), attempts=_opt_int(_opt(raw[1])) or 0, last_status=_opt(raw[2]) or None
    )


def _opt(value: Any) -> str | None:
    return None if value is None else _s(value)


def _status(reply: Sequence[Any]) -> StoreStatus:
    return StoreStatus(_s(reply[0]))


def _parse_at(value: Any) -> datetime:
    return datetime.fromisoformat(_s(value).replace("Z", "+00:00"))


def _new_request_id() -> str:
    return str(uuid.uuid4())


def _encode_trace_context(ctx: TraceContext | None) -> str:
    if ctx is None:
        return ""
    data = {"traceparent": ctx.traceparent}
    if ctx.tracestate is not None:
        data["tracestate"] = ctx.tracestate
    return json.dumps(data, separators=(",", ":"), sort_keys=True)


def _decode_trace_context(raw: str | None) -> TraceContext | None:
    """Lenient: a missing or malformed stored context reads as ``None``."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    return TraceContext.parse(data.get("traceparent"), data.get("tracestate"))


def _opt_int(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _split_transitions(
    raw: Sequence[Any],
) -> tuple[list[TransitionEntry], list[RequestTransitionEntry]]:
    """Separate Job entries from Phase 2 request-level entries of one transitions list."""
    job_entries: list[TransitionEntry] = []
    request_entries: list[RequestTransitionEntry] = []
    for item in raw:
        text = _s(item)
        if json.loads(text).get("scope") == "request":
            request_entries.append(RequestTransitionEntry.model_validate_json(text))
        else:
            job_entries.append(TransitionEntry.model_validate_json(text))
    return job_entries, request_entries


class StateStore:
    """Typed access to Request, Job, Job_Result, Lease, inbox, and timer state."""

    def __init__(
        self,
        client: Any,
        *,
        id_factory: Callable[[], str] = _new_request_id,
        max_id_attempts: int = 5,
    ) -> None:
        self._r = client
        self._id_factory = id_factory
        self._max_id_attempts = max_id_attempts
        self._scripts = register_all(client)

    @property
    def client(self) -> Any:
        return self._r

    async def _run(self, name: str, keys_: Sequence[str], args: Sequence[Any]) -> list[Any]:
        reply = await self._scripts[name](keys=list(keys_), args=list(args))
        return list(reply)

    # ------------------------------------------------------------------ time / scans

    async def set_usage_retention(self, days: int) -> None:
        """Set how many days each Usage_Rollup hash lives (Phase 5 Req 6.6).

        One cluster-wide setting: every Gateway and Manager writes its configured ``usage.retention_days`` at
        startup, and the scripts that write usage read it, defaulting to 400 days while it is absent. Keep the
        value identical across the fleet; the last writer wins.
        """
        low, high = USAGE_RETENTION_DAYS
        if not low <= int(days) <= high:
            raise ValueError(f"usage retention must be between {low} and {high} days")
        await self._r.set(keys.TENANT_USAGE_RETENTION, int(days))

    async def server_time(self) -> float:
        sec, usec = await self._r.time()
        return int(sec) + int(usec) / 1_000_000

    async def server_time_ms(self) -> int:
        """Redis ``TIME`` in integer epoch ms, computed as the Lua scripts do."""
        sec, usec = await self._r.time()
        return int(sec) * 1000 + int(usec) // 1000

    async def rate_check(self, fingerprint: str, rate: float, burst: int) -> float | None:
        """Take one token from the client's rate-limit bucket by Redis time (Phase 4 Req 2.1-2.4).

        Returns ``None`` when the call is allowed, otherwise ``retry_after`` in seconds.
        ``rate == 0`` disables the limit and never touches the bucket key.
        """
        reply = await self._run(
            "rate_check", [keys.client_bucket(fingerprint)], [f"{rate:.17g}", int(burst)]
        )
        if _s(reply[0]) == "OK":
            return None
        return float(_s(reply[1]))

    async def acquire_stream(
        self, fingerprint: str, member: str, max_streams: int, ttl: float
    ) -> bool:
        """Register an open Event_Stream connection unless the client already holds ``max_streams``.

        ``member`` is unique per connection; it expires ``ttl`` seconds after its last
        :meth:`renew_stream`, so a crashed Gateway's connections stop counting by themselves
        (Phase 4 Req 7.7, 7.8). Expired members are pruned first.
        """
        reply = await self._run(
            "stream_acquire",
            [keys.client_streams(fingerprint)],
            [member, int(max_streams), f"{ttl:.17g}"],
        )
        return _s(reply[0]) == "OK"

    async def renew_stream(self, fingerprint: str, member: str, ttl: float) -> None:
        """Push a live connection's expiry ``ttl`` seconds out; re-adds it when it was pruned."""
        key = keys.client_streams(fingerprint)
        now = await self.server_time()
        async with self._r.pipeline(transaction=True) as pipe:
            pipe.zadd(key, {member: now + ttl})
            pipe.pexpire(key, int(ttl * 1000) + 60_000)
            await pipe.execute()

    async def release_stream(self, fingerprint: str, member: str) -> None:
        """Forget a connection that ended; unknown members are ignored (Phase 4 Req 7.8)."""
        await self._r.zrem(keys.client_streams(fingerprint), member)

    async def read_events(
        self,
        rid: str,
        after: str,
        *,
        block_ms: int | None = None,
        count: int = 100,
        client: Any = None,
    ) -> list[tuple[str, dict[str, str]]]:
        """Events with an Event_ID greater than ``after``, oldest first (Phase 4 Req 7.3).

        ``block_ms`` waits up to that long for a new Event (``None``: return at once). A blocking
        read holds its connection, so the SSE endpoint passes ``client``, a dedicated pool with a
        socket timeout longer than the block, instead of using the shared one.
        """
        reply = await (client or self._r).xread({keys.events(rid): after}, count=count, block=block_ms)
        if not reply:
            return []
        return [
            (_s(event_id), {_s(k): _s(v) for k, v in fields.items()})
            for event_id, fields in reply[0][1]
        ]

    async def first_event_id(self, rid: str) -> str | None:
        """Event_ID of the oldest retained Event, or ``None`` when the Event_Stream is empty."""
        entries = await self._r.xrange(keys.events(rid), "-", "+", count=1)
        return _s(entries[0][0]) if entries else None

    async def claimable_request_ids(self, limit: int = 100) -> list[str]:
        """Pending Requests plus Requests whose lease expiry has passed (for ClaimLoop)."""
        now = await self.server_time()
        pending = await self._r.zrange(keys.REQUESTS_PENDING, 0, limit - 1)
        expired = await self._r.zrangebyscore(keys.LEASES_EXPIRY, "-inf", now, start=0, num=limit)
        seen: dict[str, None] = {}
        for rid in [*pending, *expired]:
            seen.setdefault(_s(rid), None)
        return list(seen)[:limit]

    # ------------------------------------------------------------------ requests

    async def create_request(
        self, prompt: str, trace_context: TraceContext | None = None
    ) -> str:
        """Persist a Pending Request and return its unique Request_ID (Req 1.1).

        ``trace_context`` (the Gateway's ``gateway.submit`` Span) is stored on the
        Request so the owning Manager can parent ``manager.request`` on it (Req 6.1).
        The Request is added to the listing index and ``by_status:Pending`` (Req 15.11),
        and its Event_Stream starts with ``request.created`` (Phase 4 Req 6.1).
        """
        result = await self._create(prompt, trace_context, [])
        assert result.request_id is not None  # no client fields: always created
        return result.request_id

    async def submit_request(
        self,
        prompt: str,
        client: ClientRequestSpec,
        trace_context: TraceContext | None = None,
    ) -> CreateResult:
        """Create a Request for a Phase 4 client, idempotent and quota-checked in one script.

        An Idempotency_Record of the same client and key replays its Request when the
        Body_Hash matches and conflicts otherwise; neither writes anything (Phase 4
        Req 4.2-4.4, 4.7). A replay is answered even when the Active_Set is full (Req 4.5);
        otherwise a full Active_Set returns ``quota_exceeded`` (Req 3.2). A created Request
        stores ``client_fp``, the Budget (Req 5.4), the webhook target (Req 8.1) and
        ``events_max_len`` (Req 6.5), and joins the client's Active_Set (Req 3.1).

        Raises ``ValueError`` when ``idempotency_key_hash`` is set without ``body_hash``.
        """
        if client.idempotency_key_hash is not None and client.body_hash is None:
            raise ValueError("body_hash is required with idempotency_key_hash")
        budget = client.budget
        budget_json = (
            "" if budget is None
            else json.dumps(budget.to_json_obj(), sort_keys=True, separators=(",", ":"))
        )
        extra = [
            client.fingerprint,
            budget_json,
            client.webhook_url or "",
            client.webhook_secret_name or "",
            client.idempotency_key_hash or "",
            client.body_hash or "",
            int(client.idempotency_ttl),
            int(client.max_active_requests),
            int(client.events_max_len),
            client.tenant or "",  # ARGV[13]: stored on the Request; absent = the Default_Tenant
            int(client.tenant_max_active),  # ARGV[14]: Tenant_Quota, 0 = none
            "high" if client.priority == "high" else "",  # ARGV[15]: stored only when high
        ]
        return await self._create(prompt, trace_context, extra)

    async def _create(
        self, prompt: str, trace_context: TraceContext | None, extra: Sequence[Any]
    ) -> CreateResult:
        """Run ``create_request.lua``, drawing a new Request_ID on each collision."""
        ctx = _encode_trace_context(trace_context)
        for _ in range(self._max_id_attempts):
            rid = self._id_factory()
            reply = await self._run(
                "create_request",
                [
                    keys.request(rid),
                    keys.REQUESTS_PENDING,
                    keys.REQUESTS_INDEX,
                    keys.requests_by_status(RequestStatus.PENDING.value),
                ],
                [rid, prompt, ctx, *extra],
            )
            status = _s(reply[0])
            if status == "OK":
                return CreateResult("created", request_id=rid)
            if status == "REPLAYED":
                return CreateResult("replayed", request_id=_s(reply[1]))
            if status == "IDEM_CONFLICT":
                return CreateResult("idempotency_conflict")
            if status == "QUOTA_EXCEEDED":
                return CreateResult("quota_exceeded", limit=int(_s(reply[1])))
            if status == "TENANT_QUOTA_EXCEEDED":
                return CreateResult("tenant_quota_exceeded", limit=int(_s(reply[1])))
            if status != "EXISTS":
                raise StoreError(f"create_request returned an unexpected reply: {status}")
            # EXISTS: Request_ID collision; draw another.
        raise StoreError("could not allocate a unique request_id")

    async def get_status(self, rid: str) -> RequestStatus | None:
        value = await self._r.hget(keys.request(rid), keys.F_STATUS)
        return None if value is None else RequestStatus(_s(value))

    async def get_request_job(
        self, rid: str, job_id: str | None
    ) -> tuple[RequestStatus | None, JobRecord | None]:
        """Request_Status and one Job record, read consistently (one MULTI/EXEC).

        Either value is ``None`` when it does not exist; the Job is only read when the
        Request exists (Req 12.7, 14.14)."""
        async with self._r.pipeline(transaction=True) as pipe:
            pipe.hget(keys.request(rid), keys.F_STATUS)
            if job_id is not None:
                pipe.hget(keys.jobs(rid), job_id)
            replies = await pipe.execute()
        if replies[0] is None:
            return None, None
        status = RequestStatus(_s(replies[0]))
        raw_job = replies[1] if job_id is not None else None
        job = None if raw_job is None else JobRecord.model_validate_json(raw_job)
        return status, job

    async def cancel_request(
        self, rid: str, max_retries: int = 16, prices: PriceTable | None = None
    ) -> CancelResult:
        """Cancel a Pending/Running Request and its non-terminal Jobs (Req 1.6, 1.8).

        Sets ``finished_at``, moves the Request to ``by_status:Cancelled`` and returns
        unapplied Replays to Pending (Req 2.3, 13.15, 14.2, 15.11). Appends one
        ``job.transition`` per cancelled Job and ``request.cancelled``, and enqueues the
        Webhook_Delivery when the Request has a webhook target (Phase 4 Req 6.1, 6.4, 8.2).
        A budgeted Request's ``request.cancelled`` carries its Budget_Used, computed with
        ``prices`` from the stored Budget and UsageRecords read in the same pipeline as
        the Job records on every attempt, so a retry after CONFLICT never sends a stale
        view next to fresh usage totals. That read is fenced by the script's Job-record
        compare-and-set: every UsageRecord append comes with a Job change, so usage that
        changed after the read makes the script answer CONFLICT.

        Corrupt stored budget or usage data never blocks a cancel (Phase 1 Req 1.6): the
        Request is cancelled, ``request.cancelled`` is written without ``budget``, and one
        warning names the Request (never the stored value).
        """
        budget_warned = False
        for _ in range(max_retries):
            async with self._r.pipeline(transaction=True) as pipe:
                pipe.hgetall(keys.jobs(rid))
                # Replays accepted but not yet applied go back to Pending (Req 13.15); the
                # script re-checks the set's content (compare-and-set).
                pipe.smembers(keys.pending_replays(rid))
                pipe.hget(keys.request(rid), keys.F_BUDGET)
                pipe.lrange(keys.usage(rid), 0, -1)
                raw, members, raw_budget, raw_usage = await pipe.execute()
            dlids = sorted(_s(m) for m in members)
            budget_json = ""
            if raw_budget:
                try:
                    budget = Budget.from_json_obj(json.loads(_s(raw_budget)))
                    usage = [UsageRecord.model_validate_json(_s(item)) for item in raw_usage]
                    budget_json = _budget_view_json(
                        budget_view(budget, budget_status(usage, budget, prices))
                    )
                except (ValueError, TypeError, AttributeError) as exc:
                    if not budget_warned:
                        log.warning(
                            "cancel_request: budget view skipped, stored budget or usage is corrupt",
                            request_id=rid,
                            error=type(exc).__name__,
                        )
                        budget_warned = True
            args: list[str] = [rid, json.dumps(dlids), budget_json]
            for job_id, current in raw.items():
                record = JobRecord.model_validate_json(current)
                if record.state in TERMINAL_JOB:
                    continue
                cancelled = record.model_copy(
                    update={"state": JobState.CANCELLED, "reason": CANCEL_REASON}
                )
                args += [_s(job_id), _s(current), cancelled.model_dump_json()]
            reply = await self._run("cancel_request", cancel_request_keys(rid, dlids), args)
            tag = _s(reply[0])
            if tag == "OK":
                return CancelResult(
                    StoreStatus.OK,
                    at=_parse_at(reply[1]) if len(reply) > 1 else None,
                    finished_at_ms=int(_s(reply[2])) if len(reply) > 2 else None,
                    created_at_ms=int(_s(reply[3])) if len(reply) > 3 else None,
                )
            if tag == "TERMINAL":
                return CancelResult(StoreStatus.TERMINAL, RequestStatus(_s(reply[1])))
            if tag == "NOT_FOUND":
                return CancelResult(StoreStatus.NOT_FOUND)
            # CONFLICT: Job records changed between read and script; re-read.
        raise StoreError(f"cancel_request for {rid} kept conflicting with concurrent writes")

    # ------------------------------------------------------------------ leases

    async def acquire_lease(self, rid: str, manager_id: str, duration: float) -> AcquireResult:
        reply = await self._run(
            "acquire_lease",
            [keys.request(rid), keys.lease(rid), keys.LEASES_EXPIRY, keys.REQUESTS_PENDING],
            [rid, manager_id, duration],
        )
        status = _status(reply)
        if status is not StoreStatus.OK:
            return AcquireResult(status)
        lease = Lease(
            request_id=rid, owner=manager_id, epoch=int(_s(reply[1])), expires_at=float(_s(reply[2]))
        )
        kind = _s(reply[3]) if len(reply) > 3 else LEASE_KIND_INITIAL
        return AcquireResult(status, lease, kind)

    async def renew_lease(self, lease: Lease, duration: float) -> RenewResult:
        rid = lease.request_id
        reply = await self._run(
            "renew_lease",
            [keys.request(rid), keys.lease(rid), keys.LEASES_EXPIRY],
            [rid, lease.owner, lease.epoch, duration],
        )
        status = _status(reply)
        expires = float(_s(reply[1])) if status is StoreStatus.OK else None
        return RenewResult(status, expires)

    async def release_lease(self, lease: Lease) -> WriteResult:
        rid = lease.request_id
        reply = await self._run(
            "release_lease",
            [keys.request(rid), keys.lease(rid), keys.LEASES_EXPIRY, keys.REQUESTS_PENDING],
            [rid, lease.owner, lease.epoch],
        )
        return WriteResult(_status(reply))

    async def get_lease(self, rid: str) -> Lease | None:
        raw = await self._r.hgetall(keys.lease(rid))
        return _lease_from_hash(rid, raw)

    # ------------------------------------------------------------------ guarded writes

    async def commit_plan(
        self, lease: Lease, plan: JobPlan, usage: Sequence[UsageRecord] = ()
    ) -> WriteResult:
        """Persist the plan with every Job Waiting and status Running atomically (Req 3.9).

        ``usage`` (the planning attempts' UsageRecords, Req 9.1) and ``job_count`` are
        written in the same script; the Request moves to ``by_status:Running``. The
        Event_Stream gets ``request.planned`` and one ``job.transition`` per Job
        (Phase 4 Req 6.1).
        """
        rid = lease.request_id
        args: list[Any] = [
            rid,
            lease.owner,
            lease.epoch,
            plan.model_dump_json(),
            _usage_array(usage),
            json.dumps(planned_event_data(plan), ensure_ascii=False),
        ]
        for job in plan.jobs:
            args += [job.job_id, JobRecord(job=job, state=JobState.WAITING).model_dump_json()]
        reply = await self._run("commit_plan", commit_plan_keys(rid), args)
        return _write_result(reply)

    async def apply_transition(self, lease: Lease, batch: TransitionBatch) -> WriteResult:
        """Apply Job updates, results, timers, inbox cursor, and transitions atomically."""
        rid = lease.request_id
        reserved = set(batch.request_fields) & keys.RESERVED_REQUEST_FIELDS
        if reserved:
            raise ValueError(f"request fields {sorted(reserved)} cannot be set by apply_transition")
        for job_id, record in batch.jobs.items():
            if record.job.job_id != job_id:
                raise ValueError(f"record for {job_id} carries job_id {record.job.job_id}")
        for result in batch.results:
            if result.request_id != rid:
                raise ValueError(f"result for request {result.request_id} applied to {rid}")
        result_fields = {keys.result_field(r.job_id, r.attempt) for r in batch.results}
        usage_pairs: list[list[str]] = []
        for rec in batch.usage:
            if rec.operation != "job" or rec.job_id is None or rec.attempt is None:
                raise ValueError("apply_transition usage must be job UsageRecords with job_id/attempt")
            f = keys.result_field(rec.job_id, rec.attempt)
            if f not in result_fields:
                raise ValueError(f"usage for {f} has no Job_Result in the batch")
            usage_pairs.append([f, _usage_json(rec)])
        if batch.job_count is not None and batch.job_count < 0:
            raise ValueError("job_count must be non-negative")
        payload = {
            "jobs": [[jid, rec.model_dump_json()] for jid, rec in batch.jobs.items()],
            "results": [
                [keys.result_field(r.job_id, r.attempt), r.model_dump_json()] for r in batch.results
            ],
            "timers_add": [[t.key.member, f"{t.due:.6f}"] for t in batch.timers_add],
            "timers_remove": [t.member for t in batch.timers_remove],
            "inbox_cursor": batch.inbox_cursor,
            "request_fields": [[k, v] for k, v in batch.request_fields.items()],
            "usage": usage_pairs,
            "tag": batch.tag,
            "replays_applied": list(batch.replays_applied),
            "dead_letter_reverts": list(batch.dead_letter_reverts),
            "job_count": batch.job_count,
        }
        reply = await self._run(
            "apply_transition",
            apply_transition_keys(rid, batch.dead_letter_reverts),
            [rid, lease.owner, lease.epoch, json.dumps(payload, ensure_ascii=False)],
        )
        return _write_result(reply)

    async def finalize(
        self,
        lease: Lease,
        final_result: FinalResult | None,
        status: RequestStatus = RequestStatus.COMPLETED,
        error: str | None = None,
        usage: Sequence[UsageRecord] = (),
        budget_view: Mapping[str, Any] | None = None,
    ) -> WriteResult:
        """Write the Final_Result, then the terminal status, then release the lease (Req 7.4).

        ``Completed`` is only accepted for a Running Request with a Final_Result.
        ``usage`` holds the Summarizer (and, on a planning failure, Planner) UsageRecords
        written in the same script (Req 9.1, 9.3). ``REPLAY_PENDING`` is returned while a
        Replay waits to be applied. On OK the result carries ``finished_at_ms`` and
        ``created_at_ms`` (Req 2.2, 14.2). The same script appends the Terminal_Event
        (with ``budget_view`` when given) and enqueues the Webhook_Delivery when the
        Request has a webhook target (Phase 4 Req 6.1, 6.4, 8.2).
        """
        if status not in (RequestStatus.COMPLETED, RequestStatus.FAILED):
            raise ValueError("finalize status must be Completed or Failed")
        rid = lease.request_id
        reply = await self._run(
            "finalize",
            finalize_keys(rid, status),
            [
                rid,
                lease.owner,
                lease.epoch,
                status.value,
                final_result.model_dump_json() if final_result is not None else "",
                error or "",
                _usage_array(usage),
                _budget_view_json(budget_view),
            ],
        )
        status_ = _status(reply)
        if status_ is StoreStatus.OK:
            return WriteResult(
                status_,
                finished_at_ms=int(_s(reply[1])) if len(reply) > 1 else None,
                created_at_ms=int(_s(reply[2])) if len(reply) > 2 else None,
            )
        return _write_result(reply)

    async def fail_request(
        self,
        lease: Lease,
        error: str,
        final_result: FinalResult | None = None,
        usage: Sequence[UsageRecord] = (),
        budget_view: Mapping[str, Any] | None = None,
    ) -> WriteResult:
        """Set the Request Failed with ``error`` (and optional Final_Result); release the lease."""
        if not error:
            raise ValueError("fail_request requires an error message")
        return await self.finalize(
            lease, final_result, RequestStatus.FAILED, error, usage, budget_view=budget_view
        )

    # ------------------------------------------------------------------ inbox

    async def record_result_inbox(
        self,
        rid: str,
        result: JobResultPayload,
        meta: str | Mapping[str, Any] | None = None,
    ) -> str | None:
        """Append a Job_Result to the Request inbox. Returns the entry ID, or None if the
        Request does not exist. ``meta`` carries the provider/model of the Job_Result
        taken from the Message headers (Req 9.2)."""
        return await self.record_inbox_event(
            rid, INBOX_JOB_RESULT, result.model_dump_json(), meta=meta
        )

    async def record_inbox_event(
        self,
        rid: str,
        kind: str,
        data: str | Mapping[str, Any],
        meta: str | Mapping[str, Any] | None = None,
    ) -> str | None:
        text = _json_text(data)
        meta_text = "" if meta is None else _json_text(meta)
        reply = await self._run(
            "record_inbox", [keys.request(rid), keys.inbox(rid)], [kind, text, meta_text]
        )
        return _s(reply[1]) if _s(reply[0]) == "OK" else None

    async def read_inbox(
        self, rid: str, after: str | None = None, count: int = 100
    ) -> list[InboxEntry]:
        """Inbox entries strictly after ``after`` (default: the stored inbox cursor)."""
        if after is None:
            after = _opt(await self._r.hget(keys.request(rid), keys.F_INBOX_CURSOR)) or "0-0"
        reply = await self._r.xread({keys.inbox(rid): after}, count=count)
        entries: list[InboxEntry] = []
        for _stream, items in reply or []:
            for entry_id, fields in items:
                f = {_s(k): _s(v) for k, v in fields.items()}
                entries.append(
                    InboxEntry(
                        _s(entry_id), f.get("kind", ""), f.get("data", ""), f.get("meta") or None
                    )
                )
        return entries

    # ------------------------------------------------------------------ dead letters

    async def create_dead_letter(self, fields: DeadLetterFields) -> CreateDeadLetterResult:
        """Create the Dead_Letter_Record once; a redelivery returns ``created=False``
        and writes nothing (Req 12.1, 12.2). ``dead_lettered_at`` is Redis time."""
        args = [fields.dead_letter_id, *_dead_letter_args(fields)]
        reply = await self._run(
            "create_dead_letter",
            [
                keys.dlq(fields.dead_letter_id),
                keys.DLQ_INDEX,
                keys.dlq_by_status("Pending"),
                keys.dlq_by_reason(fields.reason),
            ],
            args,
        )
        if _s(reply[0]) == "CREATED":
            return CreateDeadLetterResult(True, int(_s(reply[1])))
        return CreateDeadLetterResult(False)

    async def get_dead_letter(self, dlid: str) -> DeadLetterRecord | None:
        """The Dead_Letter_Record, or ``None`` when it does not exist (Req 12.7, 12.8).

        A stored record that does not parse raises, so the Gateway (``_bounded()``)
        answers 503 instead of a partial record (Req 12.12)."""
        raw = await self._r.hgetall(keys.dlq(dlid))
        if not raw:
            return None
        return _dead_letter_from_hash({_s(k): _s(v) for k, v in raw.items()})

    async def list_dead_letters(
        self,
        *,
        status: RecordStatus | None = None,
        reason: DeadLetterReason | None = None,
        request_id: str | None = None,
        capability: str | None = None,
        limit: int = 20,
        after: tuple[int, str] | None = None,
    ) -> DeadLetterPage:
        """One page of Dead_Letter_Records matching every given filter, ordered by
        dead_lettered_at then Dead_Letter_ID, both descending (Req 12.4, 12.5).

        ``after`` is the ``(score_ms, dlid)`` of the last record of the previous page
        (decoded from the Page_Cursor by the Gateway).
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if status is not None and status not in RECORD_STATUSES:
            raise ValueError(f"unknown Record_Status {status!r}")
        if reason is not None and reason not in DEAD_LETTER_REASONS:
            raise ValueError(f"unknown Dead_Letter_Reason {reason!r}")
        if status is not None:
            source = keys.dlq_by_status(status)
        elif reason is not None:
            source = keys.dlq_by_reason(reason)
        else:
            source = keys.DLQ_INDEX
        filters = {
            k: v
            for k, v in (
                ("status", status),
                ("reason", reason),
                ("request_id", request_id),
                ("capability", capability),
            )
            if v is not None
        }
        reply = await self._run(
            "list_dead_letters",
            [source],
            [
                "" if after is None else str(int(after[0])),
                "" if after is None else after[1],
                json.dumps(filters, sort_keys=True),
                limit + 1,
                keys.dlq(""),
            ],
        )
        hits = [(_s(reply[i]), int(float(_s(reply[i + 1])))) for i in range(0, len(reply), 2)]
        page = hits[:limit]
        if not page:
            return DeadLetterPage([])
        async with self._r.pipeline(transaction=True) as pipe:
            for dlid, _score in page:
                pipe.hgetall(keys.dlq(dlid))
            raws = await pipe.execute()
        records = [
            _dead_letter_from_hash({_s(k): _s(v) for k, v in raw.items()}) for raw in raws if raw
        ]
        last_dlid, last_score = page[-1]
        next_after = (last_score, last_dlid) if len(hits) > limit else None
        return DeadLetterPage(records, next_after)

    async def replay_dead_letter(self, dlid: str, max_retries: int = 3) -> ReplayResult:
        """Accept or reject a Replay atomically (Req 13.1-13.3, 13.6-13.8).

        Check order is that of :func:`das.ops.replay.decide_replay`. A rejection
        changes nothing. On ``accepted`` the record is Replayed, a ``replay`` inbox
        event is appended and the dlid is added to ``pending_replays``; a Failed
        Request is also reopened to Pending (Req 13.2).
        """
        for _ in range(max_retries):
            rid, job_id = await self._r.hmget(keys.dlq(dlid), "request_id", "job_id")
            rid_s = _opt(rid) or ""
            job_s = _opt(job_id)
            reply = await self._run(
                "replay_dead_letter", replay_dead_letter_keys(dlid, rid_s), [dlid, rid_s]
            )
            code = _s(reply[0])
            if code == "CONFLICT":
                continue
            if code == "ACCEPTED":
                return ReplayResult(
                    "accepted",
                    request_id=rid_s,
                    job_id=job_s,
                    new_attempt=int(_s(reply[2])),
                    request_status_before=RequestStatus(_s(reply[1])),
                )
            current = _opt(reply[1]) if len(reply) > 1 else None
            outcome: ReplayOutcome = code.lower()  # type: ignore[assignment]
            return ReplayResult(
                outcome, current or None, request_id=rid_s or None, job_id=job_s
            )
        raise StoreError(f"replay_dead_letter for {dlid} kept conflicting")

    async def discard_dead_letter(self, dlid: str) -> DiscardResult:
        """Pending -> Discarded atomically; idempotent on Discarded (Req 13.11-13.13)."""
        reply = await self._run(
            "discard_dead_letter",
            [
                keys.dlq(dlid),
                keys.dlq_by_status("Pending"),
                keys.dlq_by_status("Discarded"),
                keys.DLQ_TERMINAL,
            ],
            [dlid],
        )
        code = _s(reply[0])
        if code == "NOT_FOUND":
            return DiscardResult("not_found")
        if code == "RECORD_NOT_PENDING":
            return DiscardResult("record_not_pending", _s(reply[1]))  # type: ignore[arg-type]
        at = _ms_datetime(_opt_int(_s(reply[1])))
        return DiscardResult("accepted" if code == "OK" else "already", "Discarded", at)

    async def reopen_request(self, lease: Lease) -> WriteResult:
        """Lease owner: a Pending Request with a Job_Plan (reopened by a Replay of a
        Failed Request) goes Pending -> Running without planning (design D7)."""
        rid = lease.request_id
        reply = await self._run(
            "reopen_request",
            [
                keys.request(rid),
                keys.lease(rid),
                keys.transitions(rid),
                keys.REQUESTS_PENDING,
                keys.REQUESTS_INDEX,
                keys.requests_by_status(RequestStatus.PENDING.value),
                keys.requests_by_status(RequestStatus.RUNNING.value),
            ],
            [rid, lease.owner, lease.epoch],
        )
        return _write_result(reply)

    # ------------------------------------------------------------------ retention

    async def finished_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]:
        """Up to ``limit`` terminal rid with ``finished_at + ttl <= now``, by finished_at
        then Request_ID ascending (Req 14.3). ``purge_request`` re-checks each one."""
        return await self._expired(keys.REQUESTS_FINISHED, now_ms, ttl_ms, limit)

    async def inbox_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]:
        """Up to ``limit`` terminal rid whose inbox passed ``inbox_ttl`` (Req 14.7)."""
        return await self._expired(keys.REQUESTS_INBOX_PURGE, now_ms, ttl_ms, limit)

    async def dlq_terminal_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]:
        """Up to ``limit`` Replayed/Discarded dlid past ``dlq_record_ttl`` (Req 14.8)."""
        return await self._expired(keys.DLQ_TERMINAL, now_ms, ttl_ms, limit)

    async def _expired(self, zset: str, now_ms: int, ttl_ms: int, limit: int) -> list[str]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        members = await self._r.zrangebyscore(
            zset, "-inf", int(now_ms) - int(ttl_ms), start=0, num=limit
        )
        return [_s(m) for m in members]

    async def purge_request(self, rid: str, ttl_ms: int) -> str:
        """Delete a terminal Request past ``finished_at + ttl`` with every key and index
        entry, atomically and by Redis time (Req 14.3, 14.4, 14.9).

        Returns :data:`PURGE_DELETED`, :data:`PURGE_KEEP` or :data:`PURGE_STAMPED`.
        Only one of several concurrent callers gets ``DELETED`` (Req 14.10).
        """
        reply = await self._run("purge_request", purge_request_keys(rid), [rid, int(ttl_ms)])
        return _s(reply[0])

    async def purge_inbox(self, rid: str, ttl_ms: int) -> str:
        """Delete the inbox stream of a terminal Request past ``finished_at + ttl``
        (Req 14.7). Returns ``DELETED`` when a stream was removed, else ``KEEP``."""
        reply = await self._run(
            "purge_inbox",
            [keys.request(rid), keys.inbox(rid), keys.REQUESTS_INBOX_PURGE],
            [rid, int(ttl_ms)],
        )
        return _s(reply[0])

    async def purge_dead_letter(self, dlid: str, ttl_ms: int, max_retries: int = 3) -> str:
        """Delete a Replayed/Discarded Dead_Letter_Record past ``status_changed_at + ttl``;
        Pending records are never deleted (Req 14.8). Returns ``DELETED`` or ``KEEP``."""
        for _ in range(max_retries):
            reason = _opt(await self._r.hget(keys.dlq(dlid), "reason")) or ""
            reply = await self._run(
                "purge_dead_letter",
                [
                    keys.dlq(dlid),
                    keys.DLQ_INDEX,
                    keys.dlq_by_status("Replayed"),
                    keys.dlq_by_status("Discarded"),
                    keys.dlq_by_reason(reason),
                    keys.DLQ_TERMINAL,
                ],
                [dlid, int(ttl_ms), reason],
            )
            code = _s(reply[0])
            if code != "CONFLICT":
                return code
        raise StoreError(f"purge_dead_letter for {dlid} kept conflicting")

    # ------------------------------------------------------------------ Request_Index

    async def index_request(self, rid: str) -> bool:
        """Add one existing (e.g. v1) Request to the Request_Index; stamps
        ``finished_at`` of a terminal Request without one (Req 14.9, 15.10).
        Returns False when the Request does not exist."""
        reply = await self._run("index_request", index_request_keys(rid), [rid])
        return _s(reply[0]) == "OK"

    async def index_ready(self) -> bool:
        """True once the index backfill completed (Req 15.10)."""
        return _opt(await self._r.get(keys.REQUESTS_INDEX_READY)) == "1"

    async def set_index_ready(self) -> None:
        await self._r.set(keys.REQUESTS_INDEX_READY, "1")

    async def acquire_backfill_lock(self, owner: str, ttl_ms: int) -> bool:
        """``SET das:requests:index:backfill_lock owner NX PX ttl_ms`` (Req 15.10)."""
        return bool(
            await self._r.set(keys.REQUESTS_INDEX_BACKFILL_LOCK, owner, nx=True, px=int(ttl_ms))
        )

    async def renew_backfill_lock(self, owner: str, ttl_ms: int) -> bool:
        """Extend the backfill lock to ``ttl_ms`` if ``owner`` still holds it."""
        reply = await self._r.eval(
            _RENEW_LOCK_LUA, 1, keys.REQUESTS_INDEX_BACKFILL_LOCK, owner, int(ttl_ms)
        )
        return int(reply) == 1

    async def release_backfill_lock(self, owner: str) -> bool:
        """Delete the backfill lock if ``owner`` still holds it."""
        reply = await self._r.eval(_RELEASE_LOCK_LUA, 1, keys.REQUESTS_INDEX_BACKFILL_LOCK, owner)
        return int(reply) == 1

    async def scan_request_ids(self, count: int = 500) -> AsyncIterator[str]:
        """Every Request_ID with a ``das:req:{rid}`` hash, via ``SCAN MATCH das:req:*
        TYPE hash``; keys with a suffix (``:jobs``, ``:results``, ...) are skipped.
        A key may be yielded more than once (SCAN semantics)."""
        prefix = keys.request("")
        async for key in self._r.scan_iter(match=f"{prefix}*", count=count, _type="hash"):
            rid = _s(key)[len(prefix) :]
            if rid and ":" not in rid:
                yield rid

    async def list_requests(
        self,
        *,
        statuses: Sequence[RequestStatus] | None = None,
        created_after_ms: int | None = None,
        created_before_ms: int | None = None,
        limit: int = 20,
        after: tuple[int, str] | None = None,
    ) -> RequestPage:
        """One page of Requests in (created_at desc, Request_ID desc) order (Req 15.1).

        ``statuses`` (1-5 values) matches the current status; ``created_after_ms`` is
        inclusive and ``created_before_ms`` exclusive (Req 15.2). ``after`` is the
        ``(created_at_ms, request_id)`` of the last item of the previous page; it need
        not exist any more (Req 15.7, 15.12). Requests deleted, or whose status left
        the filter, between the index read and the field read are skipped.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        wanted: list[RequestStatus] = []
        if statuses is not None:
            for s in statuses:
                st = RequestStatus(s)
                if st not in wanted:
                    wanted.append(st)
            if not 1 <= len(wanted) <= len(_STATUS_ORDER):
                raise ValueError("statuses must hold 1 to 5 Request_Status values")
        sources = (
            [keys.requests_by_status(s.value) for s in wanted] if wanted else [keys.REQUESTS_INDEX]
        )
        reply = await self._run(
            "list_index",
            sources,
            [
                "" if after is None else str(int(after[0])),
                "" if after is None else after[1],
                "" if created_after_ms is None else str(int(created_after_ms)),
                "" if created_before_ms is None else str(int(created_before_ms)),
                "1",
                limit + 1,
            ],
        )
        hits = [(_s(reply[i]), int(float(_s(reply[i + 1])))) for i in range(0, len(reply), 2)]
        page = hits[:limit]
        if not page:
            return RequestPage([])
        fields_ = (
            keys.F_STATUS,
            keys.F_FINISHED_AT,
            keys.F_JOB_COUNT,
            keys.F_USAGE_INPUT,
            keys.F_USAGE_OUTPUT,
            keys.F_USAGE_TOTAL,
        )
        async with self._r.pipeline(transaction=True) as pipe:
            for rid, _score in page:
                pipe.hmget(keys.request(rid), *fields_)
            rows = await pipe.execute()
        items: list[RequestListItem] = []
        for (rid, score), row in zip(page, rows, strict=True):
            status_raw, finished, job_count, u_in, u_out, u_total = (_opt(x) for x in row)
            if status_raw is None:
                continue  # deleted concurrently (Req 14.11)
            status = RequestStatus(status_raw)
            if wanted and status not in wanted:
                continue  # status changed concurrently; Req 15.5 holds at read time
            terminal = status in TERMINAL_REQUEST
            items.append(
                RequestListItem(
                    request_id=rid,
                    status=status,
                    created_at_ms=score,
                    finished_at_ms=_opt_int(finished) if terminal else None,
                    job_count=_opt_int(job_count) or 0,
                    usage=TokenUsage(
                        input_tokens=_opt_int(u_in) or 0,
                        output_tokens=_opt_int(u_out) or 0,
                        total_tokens=_opt_int(u_total) or 0,
                    ),
                )
            )
        last_rid, last_score = page[-1]
        next_after = (last_score, last_rid) if len(hits) > limit else None
        return RequestPage(items, next_after)

    # ------------------------------------------------------------------ removal

    async def delete_request(self, rid: str) -> None:
        """Remove the Request record and everything stored with it, including its
        transition log, which is never deleted any other way (Req 13.8).

        The Request also leaves its client's Active_Set (Phase 4 Req 3.6) and the
        Webhook_Outbox, so no dangling member or due entry is left behind.
        """
        fp = await self._r.hget(keys.request(rid), keys.F_CLIENT_FP)
        async with self._r.pipeline(transaction=True) as pipe:
            if fp:
                pipe.srem(keys.client_active(_s(fp)), rid)
            pipe.delete(*keys.request_keys(rid))
            pipe.zrem(keys.REQUESTS_PENDING, rid)
            pipe.zrem(keys.LEASES_EXPIRY, rid)
            pipe.zrem(keys.REQUESTS_INDEX, rid)
            for status in RequestStatus:
                pipe.zrem(keys.requests_by_status(status.value), rid)
            pipe.zrem(keys.REQUESTS_FINISHED, rid)
            pipe.zrem(keys.REQUESTS_INBOX_PURGE, rid)
            pipe.zrem(keys.WEBHOOKS_DUE, rid)
            await pipe.execute()

    # ------------------------------------------------------------------ reads

    async def get_transitions(self, rid: str) -> list[TransitionEntry]:
        """Job transition entries (request-level Phase 2 entries are skipped)."""
        raw = await self._r.lrange(keys.transitions(rid), 0, -1)
        return _split_transitions(raw)[0]

    async def get_request_transitions(self, rid: str) -> list[RequestTransitionEntry]:
        """Request-level transition entries (e.g. ``Failed -> Pending`` on Replay)."""
        raw = await self._r.lrange(keys.transitions(rid), 0, -1)
        return _split_transitions(raw)[1]

    async def get_webhook(self, rid: str) -> WebhookState | None:
        """Webhook_Delivery state of a Request, or ``None`` when it has none (Phase 4 Req 8.2)."""
        return _webhook_state(await self._r.hmget(keys.webhook(rid), *_WEBHOOK_FIELDS))

    async def claim_webhook(self, lease_seconds: float) -> WebhookClaim | None:
        """Claim the next due Webhook_Delivery for ``lease_seconds`` (Phase 4 Req 8.3).

        The delivery is re-scored to Redis ``now + lease_seconds``, so other senders do
        not see it as due until the lease ends. At most 10 due entries are inspected per
        call (non-pending ones are dropped from the outbox), so ``None`` means nothing
        was claimable among the first 10 due entries, not necessarily that nothing is due.
        """
        reply = await self._run(
            "claim_webhook", [keys.WEBHOOKS_DUE], [f"{lease_seconds:.17g}", keys.request("")]
        )
        if not reply:
            return None
        rid, claim, attempts, url, secret_name, event_id, event_type, data, now = (
            _s(x) for x in reply
        )
        return WebhookClaim(
            request_id=rid,
            claim=int(claim),
            attempts=int(attempts),
            url=url,
            secret_name=secret_name,
            event_id=event_id,
            event_type=event_type,
            data=json.loads(data),
            claimed_at=float(now),
        )

    async def next_webhook_due(self) -> float | None:
        """Lowest due time (Redis seconds) in the Webhook_Outbox, or ``None`` when it is
        empty: the input for the Webhook_Sender's idle wait (Phase 4 Req 8.3, 8.6)."""
        head = await self._r.zrange(keys.WEBHOOKS_DUE, 0, 0, withscores=True)
        return float(head[0][1]) if head else None

    async def complete_webhook(
        self,
        claim: WebhookClaim,
        outcome: WebhookOutcome,
        last_status: str,
        next_due: float | None = None,
    ) -> bool:
        """Record one delivery attempt of a claimed Webhook_Delivery (Phase 4 Req 8.5-8.7).

        ``next_due`` (Redis seconds) is required for ``retry``. Returns ``False`` when
        the claim is stale (re-claimed after its lease ended, reset by a later terminal
        write so its ``event_id`` no longer matches (Req 8.3), or no longer pending);
        nothing is changed then.
        """
        if outcome not in _WEBHOOK_OUTCOMES:
            raise ValueError(f"unknown webhook outcome: {outcome!r}")
        if outcome == "retry" and next_due is None:
            raise ValueError("next_due is required for a retry outcome")
        rid = claim.request_id
        reply = await self._run(
            "complete_webhook",
            [keys.WEBHOOKS_DUE, keys.webhook(rid)],
            [
                rid,
                claim.claim,
                outcome,
                last_status,
                "" if next_due is None else f"{next_due:.17g}",
                claim.event_id,
            ],
        )
        return _s(reply[0]) == "OK"

    async def load_usage(self, rid: str) -> list[UsageRecord]:
        """Every committed UsageRecord of the Request, in append order (Req 9.8).

        A single LRANGE: the Gateway wraps it in ``_bounded()`` so a slow or failed
        read becomes 503 rather than a summary of partial records (Req 9.12). A stored
        record that does not parse raises, for the same reason.
        """
        raw = await self._r.lrange(keys.usage(rid), 0, -1)
        return [UsageRecord.model_validate_json(_s(item)) for item in raw]

    async def load_snapshot(
        self,
        rid: str,
        include_transitions: bool = False,
        include_usage: bool = False,
        include_client: bool = False,
    ) -> StoredRequest | None:
        """Read the whole Request state consistently (one MULTI/EXEC).

        ``include_client`` adds the Request's Webhook_Delivery state and the Event_ID of its
        newest Event to the same MULTI, so the view and an event stream's first frame never
        mix two moments (Phase 4 Req 8.9, 7.1).
        """
        async with self._r.pipeline(transaction=True) as pipe:
            pipe.hgetall(keys.request(rid))
            pipe.hgetall(keys.jobs(rid))
            pipe.hgetall(keys.results(rid))
            pipe.zrange(keys.timers(rid), 0, -1, withscores=True)
            pipe.hgetall(keys.lease(rid))
            if include_transitions:
                pipe.lrange(keys.transitions(rid), 0, -1)
            if include_usage:
                pipe.lrange(keys.usage(rid), 0, -1)
            if include_client:
                pipe.hmget(keys.webhook(rid), *_WEBHOOK_FIELDS)
                pipe.xrevrange(keys.events(rid), count=1)
            replies = await pipe.execute()
        # Optional replies follow the five fixed ones, in the order they were queued.
        usage_at = 5 + int(include_transitions)
        client_at = usage_at + int(include_usage)
        req_raw, jobs_raw, results_raw, timers_raw, lease_raw = replies[:5]
        if not req_raw:
            return None
        req = {_s(k): _s(v) for k, v in req_raw.items()}
        plan = JobPlan.model_validate_json(req[keys.F_PLAN]) if req.get(keys.F_PLAN) else None
        records = {_s(k): JobRecord.model_validate_json(v) for k, v in jobs_raw.items()}
        if plan is not None:
            ordered = {jid: records[jid] for jid in plan.job_ids() if jid in records}
            ordered.update({jid: rec for jid, rec in records.items() if jid not in ordered})
            records = ordered
        results = {}
        for k, v in results_raw.items():
            results[keys.parse_result_field(_s(k))] = JobResultPayload.model_validate_json(v)
        timers = []
        for member, score in timers_raw:
            kind, job_id, attempt = keys.parse_timer_member(_s(member))
            timers.append(Timer(kind, job_id, attempt, float(score)))
        final = req.get(keys.F_FINAL_RESULT)
        transitions = None
        request_transitions = None
        if include_transitions:
            transitions, request_transitions = _split_transitions(replies[5])
        usage = None
        if include_usage:
            usage = [UsageRecord.model_validate_json(_s(x)) for x in replies[usage_at]]
        webhook = _webhook_state(replies[client_at]) if include_client else None
        last_event_id = (
            _s(replies[client_at + 1][0][0]) if include_client and replies[client_at + 1] else None
        )
        history_raw = req.get(keys.F_REPLAY_HISTORY)
        replay_history = (
            [ReplayHistoryEntry.model_validate(x) for x in json.loads(history_raw)]
            if history_raw
            else []
        )
        usage_totals = TokenUsage(
            input_tokens=_opt_int(req.get(keys.F_USAGE_INPUT)) or 0,
            output_tokens=_opt_int(req.get(keys.F_USAGE_OUTPUT)) or 0,
            total_tokens=_opt_int(req.get(keys.F_USAGE_TOTAL)) or 0,
        )
        return StoredRequest(
            request_id=rid,
            status=RequestStatus(req[keys.F_STATUS]),
            prompt=req.get(keys.F_PROMPT, ""),
            created_at=float(req.get(keys.F_CREATED_AT, "0")),
            error=req.get(keys.F_ERROR) or None,
            final_result=FinalResult.model_validate_json(final) if final else None,
            plan=plan,
            jobs=records,
            results=results,
            timers=timers,
            inbox_cursor=req.get(keys.F_INBOX_CURSOR, "0-0"),
            lease=_lease_from_hash(rid, lease_raw),
            transitions=transitions,
            trace_context=_decode_trace_context(req.get(keys.F_TRACE_CONTEXT)),
            finished_at=_opt_int(req.get(keys.F_FINISHED_AT)),
            job_count=_opt_int(req.get(keys.F_JOB_COUNT)),
            usage_totals=usage_totals,
            replay_history=replay_history,
            request_transitions=request_transitions,
            usage=usage,
            budget=Budget.from_json_obj(json.loads(budget_raw)) if (budget_raw := req.get(keys.F_BUDGET)) else None,
            client_fp=req.get(keys.F_CLIENT_FP) or None,
            webhook=webhook,
            last_event_id=last_event_id,
        )


def _lease_from_hash(rid: str, raw: Mapping[Any, Any]) -> Lease | None:
    h = {_s(k): _s(v) for k, v in raw.items()}
    if not h.get("owner") or not h.get("expires_at"):
        return None
    return Lease(
        request_id=rid, owner=h["owner"], epoch=int(h["epoch"]), expires_at=float(h["expires_at"])
    )


def _json_text(data: str | Mapping[str, Any]) -> str:
    return data if isinstance(data, str) else json.dumps(data, ensure_ascii=False, sort_keys=True)


def _budget_view_json(view: Mapping[str, Any] | None) -> str:
    """Budget view spliced into the Terminal_Event by the scripts; ``''`` for none."""
    return "" if view is None else json.dumps(view, ensure_ascii=False, allow_nan=False)


def _usage_json(record: UsageRecord) -> str:
    return record.model_dump_json(exclude_none=True)


def _usage_array(records: Sequence[UsageRecord]) -> str:
    """ARGV form of UsageRecords: a JSON array of UsageRecord JSON strings ('' for none)."""
    if not records:
        return ""
    return json.dumps([_usage_json(r) for r in records], ensure_ascii=False)


def _dlq_index_keys() -> list[str]:
    return [keys.dlq_by_status("Pending"), keys.dlq_by_status("Replayed"), keys.DLQ_TERMINAL]


def commit_plan_keys(rid: str) -> list[str]:
    """KEYS of ``commit_plan.lua``."""
    return [
        keys.request(rid),
        keys.lease(rid),
        keys.jobs(rid),
        keys.transitions(rid),
        keys.REQUESTS_PENDING,
        keys.usage(rid),
        keys.REQUESTS_INDEX,
        keys.requests_by_status(RequestStatus.PENDING.value),
        keys.requests_by_status(RequestStatus.RUNNING.value),
    ]


def apply_transition_keys(rid: str, dead_letter_reverts: Sequence[str] = ()) -> list[str]:
    """KEYS of ``apply_transition.lua``: one ``das:dlq:{dlid}`` per revert, in order."""
    return [
        keys.request(rid),
        keys.lease(rid),
        keys.jobs(rid),
        keys.results(rid),
        keys.transitions(rid),
        keys.timers(rid),
        keys.usage(rid),
        keys.pending_replays(rid),
        *_dlq_index_keys(),
        *(keys.dlq(d) for d in dead_letter_reverts),
    ]


def finalize_keys(rid: str, status: RequestStatus) -> list[str]:
    """KEYS of ``finalize.lua`` for the terminal ``status`` (Completed or Failed)."""
    return [
        keys.request(rid),
        keys.lease(rid),
        keys.LEASES_EXPIRY,
        keys.REQUESTS_PENDING,
        keys.usage(rid),
        keys.pending_replays(rid),
        keys.REQUESTS_INDEX,
        keys.REQUESTS_FINISHED,
        keys.REQUESTS_INBOX_PURGE,
        keys.requests_by_status(RequestStatus.PENDING.value),
        keys.requests_by_status(RequestStatus.RUNNING.value),
        keys.requests_by_status(status.value),
    ]


def cancel_request_keys(rid: str, dlids: Sequence[str] = ()) -> list[str]:
    """KEYS of ``cancel_request.lua``: one ``das:dlq:{dlid}`` per pending replay, in order."""
    return [
        keys.request(rid),
        keys.jobs(rid),
        keys.transitions(rid),
        keys.REQUESTS_PENDING,
        keys.pending_replays(rid),
        keys.REQUESTS_INDEX,
        keys.REQUESTS_FINISHED,
        keys.REQUESTS_INBOX_PURGE,
        keys.requests_by_status(RequestStatus.PENDING.value),
        keys.requests_by_status(RequestStatus.RUNNING.value),
        keys.requests_by_status(RequestStatus.CANCELLED.value),
        *_dlq_index_keys(),
        *(keys.dlq(d) for d in dlids),
    ]


def purge_request_keys(rid: str) -> list[str]:
    """KEYS of ``purge_request.lua``: every per-Request key, then every index zset."""
    return [
        *keys.request_keys(rid),
        keys.REQUESTS_INDEX,
        *(keys.requests_by_status(s.value) for s in _STATUS_ORDER),
        keys.REQUESTS_FINISHED,
        keys.REQUESTS_INBOX_PURGE,
        keys.REQUESTS_PENDING,
        keys.LEASES_EXPIRY,
    ]


def index_request_keys(rid: str) -> list[str]:
    """KEYS of ``index_request.lua``."""
    return [
        keys.request(rid),
        keys.REQUESTS_INDEX,
        *(keys.requests_by_status(s.value) for s in _STATUS_ORDER),
        keys.REQUESTS_FINISHED,
        keys.REQUESTS_INBOX_PURGE,
    ]


def replay_dead_letter_keys(dlid: str, rid: str) -> list[str]:
    """KEYS of ``replay_dead_letter.lua``; ``rid`` is the record's Request_ID ('' if none)."""
    return [
        keys.dlq(dlid),
        keys.request(rid),
        keys.jobs(rid),
        keys.lease(rid),
        keys.inbox(rid),
        keys.transitions(rid),
        keys.pending_replays(rid),
        keys.REQUESTS_PENDING,
        keys.REQUESTS_INDEX,
        keys.requests_by_status(RequestStatus.FAILED.value),
        keys.requests_by_status(RequestStatus.PENDING.value),
        keys.REQUESTS_FINISHED,
        keys.REQUESTS_INBOX_PURGE,
        keys.dlq_by_status("Pending"),
        keys.dlq_by_status("Replayed"),
        keys.DLQ_TERMINAL,
    ]


# Hash fields of das:dlq:{dlid} holding epoch ms timestamps / integers / booleans.
_DLQ_MS_FIELDS: Final = ("dead_lettered_at", "status_changed_at", "discarded_at")
_DLQ_INT_FIELDS: Final = ("attempt", "original_size")


def _dead_letter_args(fields: DeadLetterFields) -> list[str]:
    """Alternating field/value ARGV of ``create_dead_letter.lua`` (``None`` omitted)."""
    out: list[str] = []
    for name, value in fields.model_dump().items():
        if value is None or name == "dead_letter_id":
            continue
        if name == "capabilities":
            text = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, bool):
            text = "1" if value else "0"
        else:
            text = str(value)
        out += [name, text]
    return out


def _ms_datetime(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms // 1000, UTC) + timedelta(milliseconds=ms % 1000)


def _dead_letter_from_hash(h: Mapping[str, str]) -> DeadLetterRecord:
    data: dict[str, Any] = {}
    for name, value in h.items():
        if name not in DeadLetterRecord.model_fields:
            continue
        if name in _DLQ_MS_FIELDS:
            data[name] = _ms_datetime(_opt_int(value))
        elif name in _DLQ_INT_FIELDS:
            data[name] = int(value)
        elif name == "capabilities":
            data[name] = json.loads(value)
        elif name == "truncated":
            data[name] = value == "1"
        else:
            data[name] = value
    return DeadLetterRecord.model_validate(data)


def _write_result(reply: Sequence[Any]) -> WriteResult:
    status = _status(reply)
    if status is StoreStatus.OK:
        at = _parse_at(reply[1]) if len(reply) > 1 else None
        written = int(_s(reply[2])) if len(reply) > 2 else 0
        return WriteResult(status, at=at, transitions_written=written)
    detail = _s(reply[1]) if len(reply) > 1 else None
    return WriteResult(status, detail=detail)


__all__ = [
    "CANCEL_REASON",
    "DEFAULT_EVENTS_MAX_LEN",
    "INBOX_JOB_RESULT",
    "INBOX_REPLAY",
    "INBOX_UNACKED_EXHAUSTED",
    "LEASE_KIND_INITIAL",
    "LEASE_KIND_TAKEOVER",
    "PURGE_DELETED",
    "PURGE_KEEP",
    "PURGE_STAMPED",
    "AcquireResult",
    "CancelResult",
    "ClientRequestSpec",
    "CreateDeadLetterResult",
    "CreateResult",
    "DeadLetterPage",
    "DiscardResult",
    "InboxEntry",
    "RenewResult",
    "ReplayResult",
    "RequestListItem",
    "RequestPage",
    "StateStore",
    "StoreError",
    "StoreStatus",
    "StoredRequest",
    "Timer",
    "TimerKey",
    "TransitionBatch",
    "WebhookClaim",
    "WebhookOutcome",
    "WebhookState",
    "WriteResult",
    "apply_transition_keys",
    "cancel_request_keys",
    "commit_plan_keys",
    "finalize_keys",
    "index_request_keys",
    "purge_request_keys",
    "replay_dead_letter_keys",
]
