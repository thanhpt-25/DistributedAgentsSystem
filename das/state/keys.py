"""Redis key layout shared by the State_Store and the Agent_Registry.

==============================  ======  ==================================================
Key                             Type    Content
==============================  ======  ==================================================
``das:req:{rid}``               hash    status, prompt, created_at, error, final_result,
                                        plan, inbox_cursor; Phase 4: client_fp, budget,
                                        webhook_url, webhook_secret_name, events_max_len
``das:req:{rid}:jobs``          hash    job_id -> JobRecord JSON
``das:req:{rid}:results``       hash    ``{job_id}:{attempt}`` -> JobResultPayload JSON
``das:req:{rid}:inbox``         stream  ingested Job_Results and DLQ events
``das:req:{rid}:timers``        zset    ``{kind}:{job_id}:{attempt}`` scored by due time
``das:req:{rid}:transitions``   list    TransitionEntry JSON, append-only
``das:req:{rid}:usage``         list    UsageRecord JSON, append-only (Phase 2)
``das:req:{rid}:pending_replays`` set   Dead_Letter_IDs replayed but not yet applied
``das:req:{rid}:events``        stream  Event_Stream of the Request (Phase 4)
``das:req:{rid}:webhook``       hash    Webhook_Delivery of the Request (Phase 4)
``das:lease:{rid}``             hash    owner, epoch, expires_at, last_owner
``das:leases:expiry``           zset    rid scored by expires_at
``das:requests:pending``        zset    Pending rid scored by creation time
``das:requests:index``          zset    every rid scored by created_at ms
``das:requests:by_status:{s}``  zset    rid in its current status, scored by created_at ms
``das:requests:index:ready``    string  ``1`` once the index backfill completed
``das:requests:index:backfill_lock`` string  ``SET NX PX`` lock of IndexBackfill
``das:requests:finished``       zset    terminal rid scored by finished_at ms
``das:requests:inbox_purge``    zset    terminal rid with an inbox, scored by finished_at ms
``das:dlq:{dlid}``              hash    Dead_Letter_Record fields
``das:dlq:index``               zset    dlid scored by dead_lettered_at ms
``das:dlq:by_status:{s}``       zset    dlid per Record_Status
``das:dlq:by_reason:{r}``       zset    dlid per Dead_Letter_Reason
``das:dlq:terminal``            zset    Replayed/Discarded dlid scored by status_changed_at ms
``das:gateway:cursor_key``      string  random HMAC key of Page_Cursor
``das:client:{fp}:bucket``      hash    tokens, updated: rate-limit bucket (Phase 4)
``das:client:{fp}:active``      set     non-terminal rid of the client (Phase 4)
``das:client:{fp}:idem:{h}``    hash    rid, body_hash of an Idempotency-Key (Phase 4)
``das:client:{fp}:streams``     zset    open Event_Stream connections (Phase 4)
``das:webhooks:due``            zset    rid with a Webhook_Delivery, scored by due time
``das:agent:{id}``              hash    AgentRecord fields
``das:agents``                  zset    agent_id scored by last_heartbeat
``das:tenant:{t}:requests``     zset    Phase 5: twin of ``das:requests:index`` for tenant ``t``
``das:tenant:{t}:requests:by_status:{s}`` zset  Phase 5: twin of ``das:requests:by_status:{s}``
``das:tenant:{t}:active``       set     Phase 5: non-terminal rid of the tenant (Tenant_Quota)
``das:tenant:{t}:dlq:index``, ``…:dlq:by_status:{s}``, ``…:dlq:by_reason:{r}``  zsets  Phase 5: twins of the DLQ indexes
``das:tenant:{t}:usage:{yyyymmdd}`` hash  Phase 5: Usage_Rollup (counters, tokens per ``{provider}/{model}``)
``das:tenant:index:ready``      string  Phase 5: ``1`` once the Tenant_Backfill completed
``das:tenant:index:backfill_lock`` string  Phase 5: ``SET NX PX`` lock of the Tenant_Backfill
==============================  ======  ==================================================

Request hash fields added in Phase 5: ``tenant`` (absent = ``default``) and ``priority`` (stored only when ``high``).

Phase 2 supports a single Redis instance only (Req 20.7): scripts touch keys of
different Requests and the global indexes in one call.
"""

from __future__ import annotations

from typing import Final

PREFIX: Final = "das"

LEASES_EXPIRY: Final = f"{PREFIX}:leases:expiry"
REQUESTS_PENDING: Final = f"{PREFIX}:requests:pending"
AGENTS: Final = f"{PREFIX}:agents"

REQUESTS_INDEX: Final = f"{PREFIX}:requests:index"
REQUESTS_INDEX_READY: Final = f"{PREFIX}:requests:index:ready"
REQUESTS_INDEX_BACKFILL_LOCK: Final = f"{PREFIX}:requests:index:backfill_lock"
REQUESTS_FINISHED: Final = f"{PREFIX}:requests:finished"
REQUESTS_INBOX_PURGE: Final = f"{PREFIX}:requests:inbox_purge"
DLQ_INDEX: Final = f"{PREFIX}:dlq:index"
DLQ_TERMINAL: Final = f"{PREFIX}:dlq:terminal"
GATEWAY_CURSOR_KEY: Final = f"{PREFIX}:gateway:cursor_key"
WEBHOOKS_DUE: Final = f"{PREFIX}:webhooks:due"

# Fields of the request hash.
F_STATUS: Final = "status"
F_PROMPT: Final = "prompt"
F_CREATED_AT: Final = "created_at"
F_ERROR: Final = "error"
F_FINAL_RESULT: Final = "final_result"
F_PLAN: Final = "plan"
F_INBOX_CURSOR: Final = "inbox_cursor"
# Phase 2 fields of the request hash.
F_TRACE_CONTEXT: Final = "trace_context"  # JSON {"traceparent", "tracestate"?}
F_FINISHED_AT: Final = "finished_at"  # epoch ms
F_JOB_COUNT: Final = "job_count"
F_USAGE_INPUT: Final = "usage_input"
F_USAGE_OUTPUT: Final = "usage_output"
F_USAGE_TOTAL: Final = "usage_total"
F_REPLAY_HISTORY: Final = "replay_history"  # JSON list of ReplayHistoryEntry
# Phase 4 fields of the request hash.
F_CLIENT_FP: Final = "client_fp"
F_BUDGET: Final = "budget"  # Budget JSON
F_WEBHOOK_URL: Final = "webhook_url"
F_WEBHOOK_SECRET_NAME: Final = "webhook_secret_name"
F_EVENTS_MAX_LEN: Final = "events_max_len"
# Phase 5 fields of the request hash (das-multi-tenant Req 3.1, 7.3). Absent: tenant ``default``, priority ``normal``.
F_TENANT: Final = "tenant"
F_PRIORITY: Final = "priority"  # stored only when ``high``

#: The implicit tenant that owns every Request without a ``tenant`` field (Phase 5 Req 3.1).
DEFAULT_TENANT: Final = "default"
#: ``1`` once the Tenant_Backfill completed; the lock of the backfill (Phase 5 Req 9.2).
TENANT_INDEX_READY: Final = f"{PREFIX}:tenant:index:ready"
TENANT_INDEX_BACKFILL_LOCK: Final = f"{PREFIX}:tenant:index:backfill_lock"
#: Expiry in days of every Usage_Rollup hash (Phase 5 Req 6.6): one cluster-wide setting, written at startup by the
#: Gateway and Manager from ``usage.retention_days`` and read by the scripts that write usage. 400 when absent.
TENANT_USAGE_RETENTION: Final = f"{PREFIX}:tenant:usage_retention_days"

# Phase 2 field of the lease hash: agent of the latest acquire, kept across release.
F_LAST_OWNER: Final = "last_owner"

# Fields owned by dedicated scripts; generic request-field updates may not touch them.
RESERVED_REQUEST_FIELDS: Final = frozenset(
    {
        F_STATUS,
        F_PROMPT,
        F_CREATED_AT,
        F_FINAL_RESULT,
        F_PLAN,
        F_INBOX_CURSOR,
        F_TRACE_CONTEXT,
        F_FINISHED_AT,
        F_JOB_COUNT,
        F_USAGE_INPUT,
        F_USAGE_OUTPUT,
        F_USAGE_TOTAL,
        F_REPLAY_HISTORY,
        F_CLIENT_FP,
        F_BUDGET,
        F_WEBHOOK_URL,
        F_WEBHOOK_SECRET_NAME,
        F_EVENTS_MAX_LEN,
        F_TENANT,
        F_PRIORITY,
    }
)


def request(rid: str) -> str:
    return f"{PREFIX}:req:{rid}"


def jobs(rid: str) -> str:
    return f"{PREFIX}:req:{rid}:jobs"


def results(rid: str) -> str:
    return f"{PREFIX}:req:{rid}:results"


def inbox(rid: str) -> str:
    return f"{PREFIX}:req:{rid}:inbox"


def timers(rid: str) -> str:
    return f"{PREFIX}:req:{rid}:timers"


def transitions(rid: str) -> str:
    return f"{PREFIX}:req:{rid}:transitions"


def usage(rid: str) -> str:
    return f"{PREFIX}:req:{rid}:usage"


def pending_replays(rid: str) -> str:
    return f"{PREFIX}:req:{rid}:pending_replays"


def events(rid: str) -> str:
    """Event_Stream of one Request (Phase 4 Req 6.1)."""
    return f"{PREFIX}:req:{rid}:events"


def webhook(rid: str) -> str:
    """Webhook_Delivery hash of one Request (Phase 4 Req 8)."""
    return f"{PREFIX}:req:{rid}:webhook"


def lease(rid: str) -> str:
    return f"{PREFIX}:lease:{rid}"


def requests_by_status(status: str) -> str:
    """Index zset of the Requests currently in ``status`` (a Request_Status value)."""
    return f"{PREFIX}:requests:by_status:{status}"


def dlq(dlid: str) -> str:
    return f"{PREFIX}:dlq:{dlid}"


def dlq_by_status(status: str) -> str:
    """Index zset of the Dead_Letter_Records in ``status`` (a Record_Status value)."""
    return f"{PREFIX}:dlq:by_status:{status}"


def dlq_by_reason(reason: str) -> str:
    """Index zset of the Dead_Letter_Records with ``reason`` (a Dead_Letter_Reason)."""
    return f"{PREFIX}:dlq:by_reason:{reason}"


def client_bucket(fp: str) -> str:
    """Rate-limit token bucket of one client fingerprint (Phase 4 Req 2.1)."""
    return f"{PREFIX}:client:{fp}:bucket"


def client_active(fp: str) -> str:
    """Set of the client's non-terminal Request_IDs (Phase 4 Req 3)."""
    return f"{PREFIX}:client:{fp}:active"


def client_idem(fp: str, h: str) -> str:
    """Idempotency record of one client and Idempotency-Key hash (Phase 4 Req 4)."""
    return f"{PREFIX}:client:{fp}:idem:{h}"


def client_streams(fp: str) -> str:
    """Open Event_Stream connections of one client (Phase 4 Req 7)."""
    return f"{PREFIX}:client:{fp}:streams"


def tenant_requests(tenant: str) -> str:
    """Twin of ``das:requests:index`` for one tenant (Phase 5 Req 3.5)."""
    return f"{PREFIX}:tenant:{tenant}:requests"


def tenant_requests_by_status(tenant: str, status: str) -> str:
    """Twin of ``das:requests:by_status:{status}`` for one tenant."""
    return f"{PREFIX}:tenant:{tenant}:requests:by_status:{status}"


def tenant_active(tenant: str) -> str:
    """Non-terminal Request_IDs of one tenant: the Tenant_Quota's set (Phase 5 Req 3.7)."""
    return f"{PREFIX}:tenant:{tenant}:active"


def tenant_dlq_index(tenant: str) -> str:
    """Twin of ``das:dlq:index`` for one tenant (Phase 5 Req 5.1)."""
    return f"{PREFIX}:tenant:{tenant}:dlq:index"


def tenant_dlq_by_status(tenant: str, status: str) -> str:
    return f"{PREFIX}:tenant:{tenant}:dlq:by_status:{status}"


def tenant_dlq_by_reason(tenant: str, reason: str) -> str:
    return f"{PREFIX}:tenant:{tenant}:dlq:by_reason:{reason}"


def tenant_usage(tenant: str, day: str) -> str:
    """Usage_Rollup hash of one tenant and UTC day, ``day`` as ``yyyymmdd`` (Phase 5 Req 6.1)."""
    return f"{PREFIX}:tenant:{tenant}:usage:{day}"


def tenant_twin(global_key: str, tenant: str) -> str:
    """The tenant's twin of a global Request or DLQ index key; ``ValueError`` for a key that has none.

    Mirrors ``tenant_twin`` in ``_common.lua``, which derives the same names inside the scripts.
    """
    if global_key == REQUESTS_INDEX:
        return tenant_requests(tenant)
    if global_key == DLQ_INDEX:
        return tenant_dlq_index(tenant)
    for kind, build in (
        (f"{PREFIX}:requests:by_status:", tenant_requests_by_status),
        (f"{PREFIX}:dlq:by_status:", tenant_dlq_by_status),
        (f"{PREFIX}:dlq:by_reason:", tenant_dlq_by_reason),
    ):
        if global_key.startswith(kind):
            return build(tenant, global_key[len(kind) :])
    raise ValueError(f"no tenant twin for key {global_key!r}")


def agent(agent_id: str) -> str:
    return f"{PREFIX}:agent:{agent_id}"


def result_field(job_id: str, attempt: int) -> str:
    """Field of the results hash for one ``(job_id, attempt)``."""
    return f"{job_id}:{attempt}"


def parse_result_field(field: str) -> tuple[str, int]:
    job_id, _, attempt = field.rpartition(":")
    return job_id, int(attempt)


def timer_member(kind: str, job_id: str, attempt: int) -> str:
    """Member of the timers zset. ``kind`` and ``job_id`` must not contain ``:``."""
    if ":" in kind or ":" in job_id:
        raise ValueError("timer kind and job_id must not contain ':'")
    return f"{kind}:{job_id}:{attempt}"


def parse_timer_member(member: str) -> tuple[str, str, int]:
    kind, job_id, attempt = member.split(":")
    return kind, job_id, int(attempt)


def request_keys(rid: str) -> list[str]:
    """Every per-Request key (for removal or inspection)."""
    return [
        request(rid),
        jobs(rid),
        results(rid),
        inbox(rid),
        timers(rid),
        transitions(rid),
        lease(rid),
        usage(rid),
        pending_replays(rid),
        events(rid),
        webhook(rid),
    ]
