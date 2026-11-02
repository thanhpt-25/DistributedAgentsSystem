"""Shared Hypothesis strategies.

Populated by later tasks: job_plans, invalid_plan_mutations, messages,
job_results, registry_snapshots, config_dicts, secrets.
"""

# --------------------------------------------------------------------------- job_plans
# Task 4.1. Self-contained: other strategies are added below/above by other tasks.

from hypothesis import strategies as _st  # noqa: E402

from das.models.plan import MAX_INSTRUCTION_LENGTH as _MAX_INSTR  # noqa: E402
from das.models.plan import JobPlan as _JobPlan  # noqa: E402
from das.models.plan import PlannedJob as _PlannedJob  # noqa: E402

_JOB_ID_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"

job_ids = _st.text(alphabet=_JOB_ID_ALPHABET, min_size=1, max_size=64)
"""Job_IDs matching ``^[A-Za-z0-9_-]{1,64}$``."""

capability_names = _st.from_regex(r"\A[a-z][a-z0-9-]{0,23}\Z")
"""Capability labels such as ``research`` or ``code-review``."""

instructions = _st.text(min_size=1, max_size=min(300, _MAX_INSTR))
"""Non-empty instructions (any Unicode, including whitespace and newlines)."""


@_st.composite
def job_plans(
    draw: _st.DrawFn,
    max_jobs: int = 10,
    capabilities: frozenset[str] | None = None,
) -> _JobPlan:
    """A valid Job_Plan: 1..max_jobs Jobs with unique ids forming a DAG.

    Acyclicity comes from drawing a topological order first and letting each Job
    depend only on Jobs earlier in that order. Jobs are then shuffled so the printed
    order isn't always topological. When ``capabilities`` is given, every Job's
    required Capabilities are drawn from that set.
    """
    if max_jobs < 1:
        raise ValueError("max_jobs must be >= 1")
    n = draw(_st.integers(min_value=1, max_value=max_jobs))
    ids = draw(_st.lists(job_ids, min_size=n, max_size=n, unique=True))

    if capabilities is not None:
        if not capabilities:
            raise ValueError("capabilities must be non-empty")
        cap_pool = sorted(capabilities)
    else:
        cap_pool = draw(_st.lists(capability_names, min_size=1, max_size=6, unique=True))

    jobs: list[_PlannedJob] = []
    for index, job_id in enumerate(ids):
        earlier = ids[:index]
        deps = (
            draw(_st.lists(_st.sampled_from(earlier), unique=True, max_size=len(earlier)))
            if earlier
            else []
        )
        caps = draw(
            _st.lists(_st.sampled_from(cap_pool), min_size=1, max_size=len(cap_pool), unique=True)
        )
        jobs.append(
            _PlannedJob(
                job_id=job_id,
                instruction=draw(instructions),
                capabilities=caps,
                depends_on=deps,
            )
        )
    return _JobPlan(jobs=draw(_st.permutations(jobs)))


# --------------------------------------------------------------------------- messages
# Task 5.1. Self-contained: valid Messages of all four types for codec properties.

from datetime import UTC as _UTC  # noqa: E402
from datetime import datetime as _datetime  # noqa: E402

from das.codec.envelope import (  # noqa: E402
    MAX_MESSAGE_ID_LENGTH as _MAX_MSG_ID,
)
from das.codec.envelope import (  # noqa: E402
    ControlMessage as _ControlMessage,
)
from das.codec.envelope import (  # noqa: E402
    HeartbeatMessage as _HeartbeatMessage,
)
from das.codec.envelope import (  # noqa: E402
    JobDispatchMessage as _JobDispatchMessage,
)
from das.codec.envelope import (  # noqa: E402
    JobResultMessage as _JobResultMessage,
)
from das.models.results import ControlPayload as _ControlPayload  # noqa: E402
from das.models.results import HeartbeatPayload as _HeartbeatPayload  # noqa: E402
from das.models.results import JobDispatchPayload as _JobDispatchPayload  # noqa: E402
from das.models.results import JobResultPayload as _JobResultPayload  # noqa: E402
from das.models.results import TokenUsage as _TokenUsage  # noqa: E402

# Hypothesis' default text alphabet excludes surrogates, so every string is
# encodable as UTF-8.
_msg_text = _st.text(max_size=200)
_msg_nonempty = _st.text(min_size=1, max_size=64)
_request_ids = _st.uuids().map(str)
_msg_ids = _st.text(min_size=1, max_size=_MAX_MSG_ID)
_counts = _st.integers(min_value=0, max_value=10_000_000)

message_timestamps = _st.datetimes(
    min_value=_datetime(2000, 1, 1),
    max_value=_datetime(2100, 1, 1),
    timezones=_st.just(_UTC),
)
"""UTC datetimes; Message construction truncates them to milliseconds."""

token_usages = _st.builds(
    _TokenUsage, input_tokens=_counts, output_tokens=_counts, total_tokens=_counts
)

job_dispatch_payloads = _st.builds(
    _JobDispatchPayload,
    request_id=_request_ids,
    job_id=job_ids,
    attempt=_st.integers(min_value=1, max_value=10),
    instruction=_st.text(min_size=1, max_size=300),
    required_capabilities=_st.lists(capability_names, min_size=1, max_size=5, unique=True),
    dependency_results=_st.dictionaries(job_ids, _msg_text, max_size=5),
)

job_result_payloads = _st.builds(
    _JobResultPayload,
    request_id=_request_ids,
    job_id=job_ids,
    attempt=_st.integers(min_value=1, max_value=10),
    worker_id=_msg_nonempty,
    status=_st.sampled_from(["Succeeded", "Failed", "TimedOut", "Cancelled"]),
    content=_msg_text,
    error=_st.none() | _msg_text,
    usage=token_usages,
    duration_ms=_counts,
)

heartbeat_payloads = _st.builds(
    _HeartbeatPayload,
    agent_id=_msg_nonempty,
    role=_st.sampled_from(["manager", "worker", "gateway"]),
    capabilities=_st.lists(capability_names, max_size=5, unique=True),
)

control_payloads = _st.builds(
    _ControlPayload,
    command=_st.sampled_from(["cancel_job", "cancel_request"]),
    request_id=_request_ids,
    job_id=_st.none() | job_ids,
)


@_st.composite
def _request_scoped(draw: _st.DrawFn, cls: type, payloads: _st.SearchStrategy) -> object:
    payload = draw(payloads)
    return cls(
        message_id=draw(_msg_ids),
        correlation_id=payload.request_id,
        timestamp=draw(message_timestamps),
        payload=payload,
    )


@_st.composite
def _heartbeat_messages(draw: _st.DrawFn) -> _HeartbeatMessage:
    return _HeartbeatMessage(
        message_id=draw(_msg_ids),
        correlation_id="",
        timestamp=draw(message_timestamps),
        payload=draw(heartbeat_payloads),
    )


@_st.composite
def _control_messages(draw: _st.DrawFn) -> _ControlMessage:
    payload = draw(control_payloads)
    return _ControlMessage(
        message_id=draw(_msg_ids),
        # Control commands for a Request normally carry its Request_ID; "" is also valid.
        correlation_id=draw(_st.sampled_from([payload.request_id, ""])),
        timestamp=draw(message_timestamps),
        payload=payload,
    )


def messages(types: tuple[str, ...] | None = None) -> _st.SearchStrategy:
    """Valid typed Messages of every type (or only the given ``types``)."""
    by_type: dict[str, _st.SearchStrategy] = {
        "job_dispatch": _request_scoped(_JobDispatchMessage, job_dispatch_payloads),
        "job_result": _request_scoped(_JobResultMessage, job_result_payloads),
        "heartbeat": _heartbeat_messages(),
        "control": _control_messages(),
    }
    selected = types if types is not None else tuple(by_type)
    return _st.one_of(*(by_type[t] for t in selected))


# --------------------------------------------------------------------------- registry_snapshots
# Task 10.1. Self-contained.
from das.models.agent import AgentRecord as _AgentRecord  # noqa: E402
from das.registry.registry import RegistrySnapshot as _RegistrySnapshot  # noqa: E402

_REG_NOW = 1_700_000_000.0


@_st.composite
def agent_records(
    draw: _st.DrawFn,
    agent_id: str | None = None,
    capability_pool: list[str] | None = None,
    now: float = _REG_NOW,
) -> _AgentRecord:
    """An AgentRecord; capabilities drawn from ``capability_pool`` when given."""
    role = draw(_st.sampled_from(["worker", "worker", "worker", "manager", "gateway"]))
    caps_source = _st.sampled_from(capability_pool) if capability_pool else capability_names
    caps = (
        frozenset(draw(_st.lists(caps_source, min_size=1, max_size=4)))
        if role == "worker"
        else frozenset()
    )
    return _AgentRecord(
        agent_id=agent_id or draw(_st.text(alphabet=_JOB_ID_ALPHABET, min_size=1, max_size=32)),
        role=role,
        host=draw(_st.from_regex(r"\A[a-z][a-z0-9-]{0,15}\Z")),
        grpc_endpoint=draw(_st.none() | _st.just("host:50051")),
        capabilities=caps,
        concurrency_limit=draw(_st.integers(1, 100)) if role == "worker" else None,
        status=draw(_st.sampled_from(["Available", "Unavailable"])),
        last_heartbeat=now - draw(_st.floats(min_value=0, max_value=600, allow_nan=False)),
    )


@_st.composite
def registry_snapshots(draw: _st.DrawFn, max_agents: int = 8) -> _RegistrySnapshot:
    """A registry snapshot: unique agent ids sharing a small Capability pool.

    A small pool makes capability overlap between workers likely, which is what
    routing-capability selection and advertised-set properties care about.
    """
    pool = draw(_st.lists(capability_names, min_size=1, max_size=5, unique=True))
    ids = draw(
        _st.lists(
            _st.text(alphabet=_JOB_ID_ALPHABET, min_size=1, max_size=32),
            max_size=max_agents,
            unique=True,
        )
    )
    agents = tuple(draw(agent_records(agent_id=i, capability_pool=pool)) for i in ids)
    return _RegistrySnapshot(agents=agents, server_time=_REG_NOW)


# --------------------------------------------------------------------------- invalid_plan_mutations
# Task 4.5. Self-contained: candidate plans (as decoded JSON dicts) for Property 2.

from typing import Any as _Any  # noqa: E402

PLAN_MUTATIONS: tuple[str, ...] = (
    "duplicate_id",
    "empty_instruction",
    "blank_instruction",
    "long_instruction",
    "no_capabilities",
    "self_dependency",
    "unknown_dependency",
    "cycle",
    "unadvertised_capability",
    "too_many_jobs",
    "zero_jobs",
    "invalid_job_id",
)
"""Every mutation kind ``invalid_plan_mutations`` can apply."""


def _apply_plan_mutation(
    draw: _st.DrawFn, kind: str, jobs: list[dict[str, _Any]], advertised: set[str], max_jobs: int
) -> tuple[list[dict[str, _Any]], int]:
    """Apply one mutation in place (where possible); returns ``(jobs, max_jobs)``."""
    if kind == "zero_jobs":
        return [], max_jobs
    if not jobs:
        return jobs, max_jobs  # nothing left to mutate
    idx = _st.integers(0, len(jobs) - 1)
    ids = {j["job_id"] for j in jobs}

    if kind == "duplicate_id":
        if len(jobs) >= 2 and draw(_st.booleans()):
            a, b = draw(_st.lists(idx, min_size=2, max_size=2, unique=True))
            jobs[b]["job_id"] = jobs[a]["job_id"]
        else:
            copy = dict(draw(_st.sampled_from(jobs)))
            copy["capabilities"] = list(copy["capabilities"])
            copy["depends_on"] = list(copy["depends_on"])
            jobs.insert(draw(_st.integers(0, len(jobs))), copy)
    elif kind == "empty_instruction":
        jobs[draw(idx)]["instruction"] = ""
    elif kind == "blank_instruction":
        jobs[draw(idx)]["instruction"] = draw(
            _st.text(alphabet=" \t\n\r\u00a0\u2003", min_size=1, max_size=20)
        )
    elif kind == "long_instruction":
        extra = draw(_st.integers(1, 50))
        jobs[draw(idx)]["instruction"] = "x" * (_MAX_INSTR + extra)
    elif kind == "no_capabilities":
        jobs[draw(idx)]["capabilities"] = []
    elif kind == "self_dependency":
        job = jobs[draw(idx)]
        job["depends_on"].append(job["job_id"])
    elif kind == "unknown_dependency":
        unknown = draw(job_ids.filter(lambda j: j not in ids))
        jobs[draw(idx)]["depends_on"].append(unknown)
    elif kind == "cycle":
        if len(jobs) >= 2:
            ring = draw(_st.lists(idx, min_size=2, max_size=len(jobs), unique=True))
            for here, there in zip(ring, ring[1:] + ring[:1], strict=True):
                jobs[here]["depends_on"].append(jobs[there]["job_id"])
    elif kind == "unadvertised_capability":
        cap = draw(capability_names.filter(lambda c: c not in advertised))
        jobs[draw(idx)]["capabilities"].append(cap)
    elif kind == "too_many_jobs":
        if len(jobs) >= 2:
            max_jobs = draw(_st.integers(1, len(jobs) - 1))
    elif kind == "invalid_job_id":
        job = jobs[draw(idx)]
        job["job_id"] = job["job_id"] + draw(_st.sampled_from([" ", "!", ".", "/", "é"]))
    else:  # pragma: no cover - guarded by PLAN_MUTATIONS
        raise ValueError(f"unknown mutation {kind!r}")
    return jobs, max_jobs


@_st.composite
def invalid_plan_mutations(
    draw: _st.DrawFn, max_jobs: int = 10, max_mutations: int = 3
) -> tuple[dict[str, _Any], frozenset[str], int, tuple[str, ...]]:
    """A candidate plan, advertised Capabilities, ``max_jobs``, and the mutations applied.

    Starts from a valid ``job_plans`` plan (as a decoded JSON dict), an advertised set
    covering every Capability it uses (plus some extras), and a ``max_jobs`` in
    ``len(jobs)..100``. Then applies 0..``max_mutations`` mutations from
    :data:`PLAN_MUTATIONS`. With zero mutations the candidate is (normally) valid, so
    both outcomes of the validator are exercised.
    """
    plan = draw(job_plans(max_jobs=max_jobs))
    jobs: list[dict[str, _Any]] = plan.model_dump(mode="json")["jobs"]
    used = {c for j in jobs for c in j["capabilities"]}
    advertised = used | set(draw(_st.lists(capability_names, max_size=3)))
    limit = draw(_st.integers(len(jobs), 100))

    kinds = tuple(
        draw(_st.lists(_st.sampled_from(PLAN_MUTATIONS), min_size=0, max_size=max_mutations))
    )
    for kind in kinds:
        jobs, limit = _apply_plan_mutation(draw, kind, jobs, advertised, limit)
    return {"jobs": jobs}, frozenset(advertised), limit, kinds


# =========================================================================== Phase 2
# das-observability-operations, task 1.1. Shared strategies for Phase 2 properties.
#
# The models these describe (``TraceContextModel``, ``UsageRecord``, ``PriceTable``,
# ``DeadLetterRecord``) are added by later tasks. Every strategy therefore draws a
# plain JSON-compatible dict shaped like the design's Data Models, and accepts
# ``as_model=True`` to validate the dict into the real model. The model is imported
# lazily, only when ``as_model=True`` is requested.
import importlib as _importlib  # noqa: E402
from decimal import Decimal as _Decimal  # noqa: E402


def _as_model(module: str, name: str) -> type:
    return getattr(_importlib.import_module(module), name)


# --------------------------------------------------------------------------- trace_contexts
_HEX = "0123456789abcdef"


def _nonzero_hex(length: int) -> _st.SearchStrategy[str]:
    return _st.text(alphabet=_HEX, min_size=length, max_size=length).filter(
        lambda s: s != "0" * length
    )


traceparents = _st.builds(
    lambda trace_id, parent_id, flags: f"00-{trace_id}-{parent_id}-{flags}",
    _nonzero_hex(32),
    _nonzero_hex(16),
    _st.sampled_from(["00", "01"]) | _st.text(alphabet=_HEX, min_size=2, max_size=2),
)
"""Valid W3C ``traceparent`` values, version ``00`` (always 55 characters)."""

_ts_simple_key = _st.from_regex(r"\A[a-z][a-z0-9_\-*/]{0,15}\Z")
_ts_tenant_key = _st.builds(
    lambda tenant, system: f"{tenant}@{system}",
    _st.from_regex(r"\A[a-z0-9][a-z0-9_\-*/]{0,7}\Z"),
    _st.from_regex(r"\A[a-z][a-z0-9_\-*/]{0,5}\Z"),
)
# value chars: printable ASCII 0x20-0x7E except ',' and '='; must not end with a space.
_TS_VALUE_CHARS = "".join(chr(c) for c in range(0x20, 0x7F) if chr(c) not in ",=")
_ts_value = _st.text(alphabet=_TS_VALUE_CHARS, min_size=1, max_size=16).filter(
    lambda v: not v.endswith(" ")
)


@_st.composite
def tracestates(draw: _st.DrawFn, max_members: int = 10) -> str:
    """Valid W3C ``tracestate``: 1..``max_members`` unique keys, at most 512 characters."""
    keys = draw(
        _st.lists(_ts_simple_key | _ts_tenant_key, min_size=1, max_size=max_members, unique=True)
    )
    value = ",".join(f"{k}={draw(_ts_value)}" for k in keys)
    assert len(value) <= 512  # 10 members x (14 + 1 + 16) + 9 commas
    return value


@_st.composite
def trace_contexts(draw: _st.DrawFn, as_model: bool = False) -> object:
    """A valid Trace_Context: ``{"traceparent": str, "tracestate": str | None}``.

    ``as_model=True`` returns ``das.codec.envelope.TraceContextModel``.
    """
    value = {
        "traceparent": draw(traceparents),
        "tracestate": draw(_st.none() | tracestates()),
    }
    if as_model:
        return _as_model("das.codec.envelope", "TraceContextModel").model_validate(value)
    return value


# --------------------------------------------------------------------------- usage_records
LLM_OPERATIONS: tuple[str, ...] = ("plan", "summarize", "job")

# Small pools, so records collide on provider/model (summaries and price lookups
# then exercise grouping and matching). Case variants check case-sensitive matching.
usage_providers = _st.sampled_from(["openai", "anthropic", "bedrock", "OpenAI"])
usage_models = _st.sampled_from(["gpt-4o", "gpt-4o-mini", "claude-sonnet", "Claude-Sonnet", "m"])
_token_counts = _st.integers(min_value=0, max_value=10_000_000)


@_st.composite
def usage_records(
    draw: _st.DrawFn,
    job_id_pool: list[str] | None = None,
    operations: tuple[str, ...] = LLM_OPERATIONS,
    as_model: bool = False,
) -> object:
    """A normalized Usage_Record dict (design: ``UsageRecord``).

    ``job_id`` / ``attempt`` are set only for operation ``job``; ``job_id`` is drawn
    from ``job_id_pool`` when given. Token counts are non-negative and ``total``
    is independent of input/output (providers may report it that way).
    ``as_model=True`` returns ``das.models.usage.UsageRecord``.
    """
    operation = draw(_st.sampled_from(operations))
    value: dict[str, object] = {
        "operation": operation,
        "provider": draw(usage_providers),
        "model": draw(usage_models),
        "usage": {
            "input_tokens": draw(_token_counts),
            "output_tokens": draw(_token_counts),
            "total_tokens": draw(_token_counts),
        },
        "job_id": None,
        "attempt": None,
    }
    if operation == "job":
        value["job_id"] = draw(_st.sampled_from(job_id_pool) if job_id_pool else job_ids)
        value["attempt"] = draw(_st.integers(min_value=1, max_value=10))
    if as_model:
        return _as_model("das.models.usage", "UsageRecord").model_validate(value)
    return value


# --------------------------------------------------------------------------- price_tables
CURRENCY_CODES: tuple[str, ...] = ("USD", "EUR", "JPY", "VND", "GBP")
"""A few valid ISO 4217 codes (the full table lives in ``das.ops.iso4217``)."""

prices = _st.decimals(
    min_value=_Decimal(0),
    max_value=_Decimal(1_000_000),
    places=6,
    allow_nan=False,
    allow_infinity=False,
)
"""Valid per-million-token prices: 0..1,000,000 with at most 6 decimal places."""


@_st.composite
def price_tables(draw: _st.DrawFn, max_entries: int = 8, as_model: bool = False) -> object:
    """A valid Price_Table dict: ISO 4217 currency and unique (provider, model) entries.

    Pairs are drawn from the same pools as :func:`usage_records`, so tables both
    cover and miss pairs in generated records (exercising ``unpriced_models``).
    Prices are ``Decimal``. ``as_model=True`` returns ``das.ops.cost.PriceTable``.
    """
    pairs = draw(
        _st.lists(
            _st.tuples(usage_providers, usage_models),
            max_size=min(max_entries, 200),
            unique=True,
        )
    )
    value = {
        "currency": draw(_st.sampled_from(CURRENCY_CODES)),
        "models": [
            {
                "provider": provider,
                "model": model,
                "input_per_million": draw(prices),
                "output_per_million": draw(prices),
            }
            for provider, model in pairs
        ],
    }
    if as_model:
        return _as_model("das.ops.cost", "PriceTable").model_validate(value)
    return value


# --------------------------------------------------------------------------- dead_letter_records
DEAD_LETTER_REASONS: tuple[str, ...] = ("retries_exhausted", "delivery_limit", "malformed")
RECORD_STATUSES: tuple[str, ...] = ("Pending", "Replayed", "Discarded")
MAX_LAST_ERROR_LENGTH = 4096

_dl_ids = _st.text(alphabet=_HEX, min_size=32, max_size=32)
_dl_times_ms = _st.integers(min_value=946_684_800_000, max_value=4_102_444_800_000)  # 2000..2100


def _ms_to_datetime(ms: int) -> _datetime:
    return _datetime.fromtimestamp(ms / 1000, tz=_UTC)


@_st.composite
def dead_letter_records(
    draw: _st.DrawFn,
    reasons: tuple[str, ...] = DEAD_LETTER_REASONS,
    statuses: tuple[str, ...] = RECORD_STATUSES,
    as_model: bool = False,
) -> object:
    """A Dead_Letter_Record dict (design: ``DeadLetterRecord``), self-consistent.

    - ``malformed``: no envelope; Request/Job fields null; ``parse_*``, ``raw_b64``,
      ``original_size`` and ``truncated`` set.
    - otherwise: Request/Job fields set, ``envelope_json`` holds a canonical envelope.
    - ``Discarded`` sets ``discarded_at``; non-Pending sets ``status_changed_at``.
    Timestamps are UTC ``datetime`` at ms precision.
    ``as_model=True`` returns ``das.models.dlq.DeadLetterRecord``.
    """
    import base64
    import json

    reason = draw(_st.sampled_from(reasons))
    status = draw(_st.sampled_from(statuses))
    dead_lettered_ms = draw(_dl_times_ms)
    changed_ms = dead_lettered_ms + draw(_st.integers(min_value=0, max_value=86_400_000))
    value: dict[str, object] = {
        "dead_letter_id": draw(_dl_ids),
        "status": status,
        "reason": reason,
        "dead_lettered_at": _ms_to_datetime(dead_lettered_ms),
        "status_changed_at": None if status == "Pending" else _ms_to_datetime(changed_ms),
        "discarded_at": _ms_to_datetime(changed_ms) if status == "Discarded" else None,
        "request_id": None,
        "job_id": None,
        "attempt": None,
        "capabilities": None,
        "last_error": None,
        "message_id": None,
        "parse_reason": None,
        "parse_field": None,
        "original_size": None,
        "raw_b64": None,
        "truncated": False,
        "envelope_json": None,
    }
    if reason == "malformed":
        raw = draw(_st.binary(min_size=0, max_size=256))
        truncated = draw(_st.booleans())
        value.update(
            parse_reason=draw(_st.text(min_size=1, max_size=80)),
            parse_field=draw(_st.sampled_from(["$", "type", "payload", "payload.attempt"])),
            original_size=len(raw) + (draw(_st.integers(1, 1_048_576)) if truncated else 0),
            raw_b64=base64.b64encode(raw).decode("ascii"),
            truncated=truncated,
        )
    else:
        request_id = draw(_request_ids)
        job_id = draw(job_ids)
        attempt = draw(_st.integers(min_value=1, max_value=10))
        caps = draw(_st.lists(capability_names, min_size=1, max_size=4, unique=True))
        message_id = draw(_st.text(alphabet=_HEX, min_size=32, max_size=32))
        value.update(
            request_id=request_id,
            job_id=job_id,
            attempt=attempt,
            capabilities=caps,
            last_error=draw(_st.none() | _st.text(max_size=200)),
            message_id=message_id,
            envelope_json=json.dumps(
                {
                    "correlation_id": request_id,
                    "message_id": message_id,
                    "payload": {
                        "attempt": attempt,
                        "dependency_results": {},
                        "instruction": "replay me",
                        "job_id": job_id,
                        "request_id": request_id,
                        "required_capabilities": caps,
                    },
                    "schema_version": 1,
                    "timestamp": _ms_to_datetime(dead_lettered_ms)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z"),
                    "type": "job_dispatch",
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ),
        )
    if as_model:
        return _as_model("das.models.dlq", "DeadLetterRecord").model_validate(value)
    return value
