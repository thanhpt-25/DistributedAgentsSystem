"""Prometheus metrics: static catalog, recorder shell and typed facades (Req 1-5).

* :data:`CATALOG` is the single source of metric names, label names and the
  Bounded_Label_Value sets (design §2). Dashboards and tests check against it.
* :class:`MetricsRecorder` owns a private ``CollectorRegistry`` (no default
  ``process_*``/``python_*`` collectors, no ``_created`` series, D11), bounds
  every label set with a per-metric :class:`~das.observability.labels.LabelBounder`
  and never lets a recording error escape (Req 1.6).
* :class:`GatewayMetrics`, :class:`ManagerMetrics`, :class:`WorkerMetrics` and
  :class:`LLMMetrics` are the only call sites used by the agents. They take
  enums / Literals, Capability, queue, provider and model names only, so a
  Request_ID, Job_ID or any content can never become a label value (Req 1.5, 5.1).
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Literal, get_args

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    disable_created_metrics,
    generate_latest,
)

from das.clock import Clock
from das.config.schema import BucketsConfig, LabelLimitsConfig, MetricsConfig
from das.models.dlq import DeadLetterReason
from das.models.job import JobState
from das.models.request import TERMINAL_REQUEST, RequestStatus
from das.models.results import JobResultStatus, TokenUsage
from das.observability.labels import OVERFLOW_VALUE, LabelBounder, LabelPolicy
from das.observability.logging import get_logger
from das.observability.ratelimit import RateLimitedLog
from das.ops.cost import PriceEntry, PriceTable, record_cost

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
UNMATCHED_ROUTE = "_unmatched"
OTHER_METHOD = "_other"

MetricKind = Literal["counter", "gauge", "histogram"]
BucketsKey = Literal["http", "duration", "attempts"]

# --- Bounded_Label_Value types (Req 5.2) -------------------------------------------------

LLMOperation = Literal["plan", "summarize", "job"]
LLMOutcome = Literal["success", "rate_limited", "transient_error", "timeout", "non_transient_error"]
TokenDirection = Literal["input", "output"]
DlqAction = Literal["replay", "discard"]
DlqAdminOutcome = Literal[
    "accepted",
    "not_found",
    "record_not_pending",
    "not_replayable",
    "request_state",
    "request_gone",
    "budget_exceeded",
    "job_state",
    "stale_attempt",
    "lease_held",
    "service_unavailable",
]
LeaseKind = Literal["initial", "takeover"]
LeaseLossReason = Literal["held_by_other", "store_unreachable"]
RetentionKind = Literal["request", "inbox", "dlq_record"]
#: Same values as ``das.client.budget.ExceededKind`` (Phase 4 Req 10.2).
BudgetExceededKind = Literal["tokens", "cost", "unenforceable"]
#: Gateway rejections of client calls (Phase 4 Req 10.1); never a fingerprint, Request_ID or URL (10.3).
#: Phase 5 Req 8.2 adds ``forbidden`` (a 403 for a missing Scope or role), ``tenant_quota_exceeded`` and
#: ``priority_above_limit``; still never a key, fingerprint or tenant.
ClientRejectionReason = Literal[
    "rate_limited", "quota_exceeded", "too_many_streams", "budget_above_limit", "idempotency_key_reused",
    "forbidden", "tenant_quota_exceeded", "priority_above_limit",
]
WebhookOutcome = Literal["delivered", "retry", "failed"]
RegistryRole = Literal["manager", "worker"]
RegistryStatus = Literal["Available", "Unavailable", "Draining"]
HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "_other"]


def _values(literal: Any) -> frozenset[str]:
    return frozenset(str(v) for v in get_args(literal))


REQUEST_FINISH_STATUSES: frozenset[str] = frozenset(s.value for s in TERMINAL_REQUEST)
JOB_STATES: frozenset[str] = frozenset(s.value for s in JobState)
JOB_RESULT_STATUSES: frozenset[str] = _values(JobResultStatus)
LLM_OPERATIONS: frozenset[str] = _values(LLMOperation)
LLM_OUTCOMES: frozenset[str] = _values(LLMOutcome)
TOKEN_DIRECTIONS: frozenset[str] = _values(TokenDirection)
DLQ_ACTIONS: frozenset[str] = _values(DlqAction)
DLQ_ADMIN_OUTCOMES: frozenset[str] = _values(DlqAdminOutcome)
DEAD_LETTER_REASONS: frozenset[str] = _values(DeadLetterReason)
LEASE_KINDS: frozenset[str] = _values(LeaseKind)
LEASE_LOSS_REASONS: frozenset[str] = _values(LeaseLossReason)
RETENTION_KINDS: frozenset[str] = _values(RetentionKind)
BUDGET_EXCEEDED_KINDS: frozenset[str] = _values(BudgetExceededKind)
CLIENT_REJECTION_REASONS: frozenset[str] = _values(ClientRejectionReason)
WEBHOOK_OUTCOMES: frozenset[str] = _values(WebhookOutcome)
#: Phase 6 Req 5.12: the ``tool`` label is bounded by the tool names; ``unknown`` counts a call to a tool the Job
#: was not offered. ``run_python`` is the code worker's tool (Req 6).
TOOL_NAMES: frozenset[str] = frozenset({"calculator", "http_fetch", "current_time", "run_python", "unknown"})
TOOL_OUTCOMES: frozenset[str] = frozenset({"ok", "error", "timeout"})
REGISTRY_ROLES: frozenset[str] = _values(RegistryRole)
REGISTRY_STATUSES: frozenset[str] = _values(RegistryStatus)
HTTP_METHODS: frozenset[str] = _values(HttpMethod)
STATUS_CODES: frozenset[str] = frozenset(str(code) for code in range(100, 600))

#: Labels bounded by a configured distinct-value limit (Req 5.3).
DYNAMIC_LABELS: frozenset[str] = frozenset({"capability", "queue", "provider", "model", "tenant"})
#: Labels whose allowed set is only known at runtime (route templates, Price_Table currency).
RUNTIME_LABELS: frozenset[str] = frozenset({"route", "currency"})
#: Every label name the recorder may use (Req 5.1).
ALLOWED_LABEL_NAMES: frozenset[str] = frozenset(
    {
        "status", "state", "capability", "route", "method", "status_code", "queue", "kind",
        "reason", "role", "action", "outcome", "provider", "model", "operation", "direction",
        "currency", "tenant", "tool",
    }
)


@dataclass(frozen=True)
class MetricSpec:
    """One catalog entry. ``allowed`` holds the static Bounded_Label_Value sets."""

    name: str
    kind: MetricKind
    help: str
    labels: tuple[str, ...] = ()
    buckets_key: BucketsKey | None = None
    allowed: Mapping[str, frozenset[str]] = field(default_factory=dict)


def _spec(
    name: str,
    kind: MetricKind,
    help: str,
    labels: tuple[str, ...] = (),
    buckets_key: BucketsKey | None = None,
    /,
    **allowed: frozenset[str],
) -> MetricSpec:
    return MetricSpec(name, kind, help, labels, buckets_key, dict(allowed))


CATALOG: tuple[MetricSpec, ...] = (
    # The only two metrics with a ``tenant`` label (Phase 5 Req 8.1); the duration histogram below has none.
    _spec("das_requests_submitted_total", "counter", "Requests created by the Gateway_API.", ("tenant",)),
    _spec(
        "das_requests_finished_total", "counter", "Requests that reached a terminal status.",
        ("status", "tenant"), status=REQUEST_FINISH_STATUSES,
    ),
    _spec(
        "das_request_duration_seconds", "histogram",
        "Seconds from Request creation to its terminal status.",
        ("status",), "duration", status=REQUEST_FINISH_STATUSES,
    ),
    _spec(
        "das_job_transitions_total", "counter", "Committed Job state transitions.",
        ("capability", "state"), state=JOB_STATES,
    ),
    _spec(
        "das_job_attempts", "histogram", "Attempt count of Jobs reaching a terminal state.",
        ("capability",), "attempts",
    ),
    _spec(
        "das_job_execution_seconds", "histogram", "Seconds a Worker_Agent spent executing a Job attempt.",
        ("capability", "status"), "duration", status=JOB_RESULT_STATUSES,
    ),
    _spec("das_worker_jobs_in_flight", "gauge", "Jobs currently executing on this Worker_Agent."),
    _spec("das_worker_concurrency_limit", "gauge", "Configured concurrency limit of this Worker_Agent."),
    _spec(
        "das_http_requests_total", "counter", "HTTP calls completed on the Gateway_API http_port.",
        ("route", "method", "status_code"), method=HTTP_METHODS, status_code=STATUS_CODES,
    ),
    _spec(
        "das_http_request_duration_seconds", "histogram",
        "Seconds from receiving an HTTP call to sending its response.",
        ("route", "method"), "http", method=HTTP_METHODS,
    ),
    _spec("das_queue_depth", "gauge", "Ready messages in a queue at the last sample.", ("queue",)),
    _spec(
        "das_queue_depth_sample_errors_total", "counter", "Failed or timed-out queue depth samples.",
        ("queue",),
    ),
    _spec(
        "das_lease_acquisitions_total", "counter", "Leases acquired by this Manager_Agent.",
        ("kind",), kind=LEASE_KINDS,
    ),
    _spec(
        "das_lease_losses_total", "counter", "Leases this Manager_Agent considered lost.",
        ("reason",), reason=LEASE_LOSS_REASONS,
    ),
    _spec("das_leases_held", "gauge", "Unexpired Leases held by this Manager_Agent."),
    _spec(
        "das_registry_agents", "gauge", "Agents in the Agent_Registry by role and status.",
        ("role", "status"), role=REGISTRY_ROLES, status=REGISTRY_STATUSES,
    ),
    _spec(
        "das_capability_workers", "gauge", "Available Worker_Agents advertising a Capability.",
        ("capability",),
    ),
    _spec("das_registry_sample_errors_total", "counter", "Failed Agent_Registry reads."),
    _spec(
        "das_dlq_records_created_total", "counter", "Dead_Letter_Records created.",
        ("reason",), reason=DEAD_LETTER_REASONS,
    ),
    _spec(
        "das_dlq_admin_actions_total", "counter", "Admin_API Replay and Discard responses.",
        ("action", "outcome"), action=DLQ_ACTIONS, outcome=DLQ_ADMIN_OUTCOMES,
    ),
    _spec(
        "das_llm_calls_total", "counter", "Finished LLM completion calls.",
        ("provider", "model", "operation", "outcome"), operation=LLM_OPERATIONS, outcome=LLM_OUTCOMES,
    ),
    _spec(
        "das_llm_call_duration_seconds", "histogram",
        "Seconds per LLM completion call including retries and backoff.",
        ("provider", "model", "operation"), "duration", operation=LLM_OPERATIONS,
    ),
    _spec(
        "das_llm_retries_total", "counter", "LLM attempt retries scheduled.",
        ("provider", "model", "operation"), operation=LLM_OPERATIONS,
    ),
    _spec(
        "das_llm_fallbacks_total", "counter",
        "LLM calls repeated on the Fallback_Model after a permanent error, by the model that failed.",
        ("provider", "model"),
    ),
    _spec(
        "das_llm_tokens_total", "counter", "LLM tokens reported by successful calls.",
        ("provider", "model", "operation", "direction"),
        operation=LLM_OPERATIONS, direction=TOKEN_DIRECTIONS,
    ),
    _spec(
        "das_llm_estimated_cost_total", "counter", "Estimated LLM cost per the Price_Table.",
        ("provider", "model", "currency"),
    ),
    _spec(
        "das_retention_deleted_total", "counter", "Items deleted by the Retention_Sweeper.",
        ("kind",), kind=RETENTION_KINDS,
    ),
    _spec(
        "das_budget_exceeded_total", "counter", "Requests failed by a budget stop, by exceeded kind.",
        ("kind",), kind=BUDGET_EXCEEDED_KINDS,
    ),
    _spec(
        "das_client_rejections_total", "counter", "Client calls the Gateway_API rejected, by reason.",
        ("reason",), reason=CLIENT_REJECTION_REASONS,
    ),
    _spec("das_idempotent_replays_total", "counter", "Submits answered from an idempotency record."),
    _spec("das_sse_streams", "gauge", "Event streams open on this Gateway_API replica."),
    _spec(
        "das_webhook_deliveries_total", "counter", "Webhook delivery attempts, by outcome.",
        ("outcome",), outcome=WEBHOOK_OUTCOMES,
    ),
    _spec(
        "das_tool_calls_total", "counter", "Tool calls a Worker ran for the model, by tool and outcome.",
        ("tool", "outcome"), tool=TOOL_NAMES, outcome=TOOL_OUTCOMES,
    ),
)

CATALOG_BY_NAME: Mapping[str, MetricSpec] = {spec.name: spec for spec in CATALOG}


def policy_for(
    spec: MetricSpec,
    limits: LabelLimitsConfig,
    *,
    route_templates: Iterable[str] = (),
    currency: str | None = None,
) -> LabelPolicy:
    """The :class:`LabelPolicy` for one metric (every label is either enumerated or dynamic)."""
    enumerated: dict[str, frozenset[str]] = {}
    dynamic: dict[str, int] = {}
    for label in spec.labels:
        if label in DYNAMIC_LABELS:
            dynamic[label] = int(getattr(limits, label))
        elif label == "route":
            enumerated[label] = frozenset(route_templates) | {UNMATCHED_ROUTE}
        elif label == "currency":
            enumerated[label] = frozenset({currency}) if currency else frozenset()
        else:
            enumerated[label] = spec.allowed[label]
    return LabelPolicy(enumerated=enumerated, dynamic_limits=dynamic)


class RenderError(Exception):
    """Building the Metrics_Endpoint output failed (Req 1.9)."""


def _label_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


class MetricsRecorder:
    """Records observations into a private Prometheus registry; never raises on writes."""

    def __init__(
        self,
        catalog: Sequence[MetricSpec] = CATALOG,
        *,
        buckets: BucketsConfig | None = None,
        label_limits: LabelLimitsConfig | None = None,
        clock: Clock,
        log: Any = None,
        route_templates: Iterable[str] = (),
        currency: str | None = None,
    ) -> None:
        buckets = buckets or BucketsConfig()
        limits = label_limits or LabelLimitsConfig()
        routes = tuple(route_templates)
        self._log = log if log is not None else get_logger("das.observability.metrics")
        self._errors = RateLimitedLog(clock, 60.0)
        self._lock = threading.Lock()
        self._warned: set[tuple[str, str]] = set()
        self._currency = currency
        disable_created_metrics()  # before any metric is created (D11)
        self._registry = CollectorRegistry(auto_describe=False)
        self._specs: dict[str, MetricSpec] = {}
        self._metrics: dict[str, Any] = {}
        self._bounders: dict[str, LabelBounder] = {}
        self._policies: dict[str, LabelPolicy] = {}
        for spec in catalog:
            self._register(spec, buckets, limits, routes, currency)

    # -- setup ------------------------------------------------------------------------

    def _register(
        self,
        spec: MetricSpec,
        buckets: BucketsConfig,
        limits: LabelLimitsConfig,
        routes: tuple[str, ...],
        currency: str | None,
    ) -> None:
        if not spec.name.startswith("das_"):
            raise ValueError(f"metric {spec.name!r} must start with 'das_'")
        unknown = set(spec.labels) - ALLOWED_LABEL_NAMES
        if unknown:
            raise ValueError(f"metric {spec.name!r} uses unknown labels {sorted(unknown)}")
        common: dict[str, Any] = {"labelnames": spec.labels, "registry": self._registry}
        metric: Any
        if spec.kind == "counter":
            metric = Counter(spec.name, spec.help, **common)
        elif spec.kind == "gauge":
            metric = Gauge(spec.name, spec.help, **common)
        else:
            key = spec.buckets_key or "duration"
            metric = Histogram(spec.name, spec.help, buckets=list(getattr(buckets, key)), **common)
        policy = policy_for(spec, limits, route_templates=routes, currency=currency)
        self._specs[spec.name] = spec
        self._metrics[spec.name] = metric
        self._policies[spec.name] = policy
        self._bounders[spec.name] = LabelBounder(policy)

    @property
    def currency(self) -> str | None:
        return self._currency

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry

    # -- recording --------------------------------------------------------------------

    def _child(self, name: str, kind: MetricKind, labels: Mapping[str, Any]) -> Any:
        spec = self._specs.get(name)
        if spec is None:
            raise KeyError(f"unknown metric {name!r}")
        if spec.kind != kind:
            raise TypeError(f"metric {name!r} is a {spec.kind}, not a {kind}")
        metric = self._metrics[name]
        given = {k: _label_value(v) for k, v in labels.items()}
        if set(given) != set(spec.labels):
            raise ValueError(f"metric {name!r} expects labels {list(spec.labels)}")
        if not spec.labels:
            return metric
        with self._lock:
            result = self._bounders[name].bound(name, given)
            fresh = [lbl for lbl in result.overflowed if (name, lbl) not in self._warned]
            self._warned.update((name, lbl) for lbl in fresh)
        for label in fresh:
            self._warn_overflow(name, label)
        return metric.labels(**result.labels)

    def _warn_overflow(self, name: str, label: str) -> None:
        policy = self._policies[name]
        limit = policy.dynamic_limits.get(label)
        if limit is None:
            limit = len(policy.enumerated.get(label, ()))
        try:
            self._log.warning(
                "metric_label_overflow", metric=name, label=label, limit=limit,
                overflow_value=OVERFLOW_VALUE,
            )
        except Exception:
            pass

    def _failed(self, name: str, exc: BaseException) -> None:
        self._errors.warning(
            self._log, ("record", name), "metric_record_failed", metric=name,
            error_type=type(exc).__name__,
        )

    def inc(self, name: str, labels: Mapping[str, Any] | None = None, amount: float = 1.0) -> None:
        try:
            self._child(name, "counter", labels or {}).inc(float(amount))
        except Exception as exc:
            self._failed(name, exc)

    def observe(self, name: str, labels: Mapping[str, Any] | None, value: float) -> None:
        try:
            self._child(name, "histogram", labels or {}).observe(float(value))
        except Exception as exc:
            self._failed(name, exc)

    def set(self, name: str, labels: Mapping[str, Any] | None, value: float) -> None:
        try:
            self._child(name, "gauge", labels or {}).set(float(value))
        except Exception as exc:
            self._failed(name, exc)

    def set_function(self, name: str, fn: Callable[[], float]) -> None:
        """Compute an unlabeled gauge at scrape time; a failing ``fn`` keeps the last value."""
        try:
            gauge = self._child(name, "gauge", {})
            last = [0.0]

            def safe() -> float:
                try:
                    last[0] = float(fn())
                except Exception as exc:
                    self._failed(name, exc)
                return last[0]

            gauge.set_function(safe)
        except Exception as exc:
            self._failed(name, exc)

    def render(self) -> tuple[bytes, str]:
        """Return ``(body, content type)``; raise :class:`RenderError` on failure (Req 1.9)."""
        try:
            return generate_latest(self._registry), CONTENT_TYPE
        except Exception as exc:
            self._errors.warning(
                self._log, ("render",), "metrics_render_failed", error_type=type(exc).__name__
            )
            raise RenderError(type(exc).__name__) from exc


class NoopRecorder(MetricsRecorder):
    """Recorder used when ``metrics.enabled`` is false: records nothing, renders nothing."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D107 - nothing to set up
        self._currency = kwargs.get("currency")

    @property
    def registry(self) -> CollectorRegistry:  # pragma: no cover - not used when disabled
        raise RenderError("metrics disabled")

    def inc(self, name: str, labels: Mapping[str, Any] | None = None, amount: float = 1.0) -> None:
        return None

    def observe(self, name: str, labels: Mapping[str, Any] | None, value: float) -> None:
        return None

    def set(self, name: str, labels: Mapping[str, Any] | None, value: float) -> None:
        return None

    def set_function(self, name: str, fn: Callable[[], float]) -> None:
        return None

    def render(self) -> tuple[bytes, str]:
        return b"", CONTENT_TYPE


def build_recorder(
    config: MetricsConfig,
    *,
    clock: Clock,
    log: Any = None,
    route_templates: Iterable[str] = (),
    currency: str | None = None,
) -> MetricsRecorder:
    """A :class:`MetricsRecorder` per ``config``, or :class:`NoopRecorder` when disabled."""
    if not config.enabled:
        return NoopRecorder(currency=currency)
    return MetricsRecorder(
        CATALOG,
        buckets=config.buckets,
        label_limits=config.label_limits,
        clock=clock,
        log=log,
        route_templates=route_templates,
        currency=currency,
    )


# --- typed facades -----------------------------------------------------------------------


def _seconds(value: float) -> float:
    return max(0.0, float(value))


def _method(method: str) -> str:
    upper = method.upper()
    return upper if upper in HTTP_METHODS else OTHER_METHOD


class _Facade:
    def __init__(self, recorder: MetricsRecorder) -> None:
        self._r = recorder

    @property
    def recorder(self) -> MetricsRecorder:
        return self._r


class _RequestFinishMixin(_Facade):
    def request_finished(self, status: RequestStatus, duration_seconds: float, tenant: str) -> None:
        """Req 2.2 / 2.3: one increment plus one duration observation (clamped at 0).

        Only the counter is per tenant; the duration histogram keeps its ``status`` label alone (Phase 5 Req 8.1)."""
        labels = {"status": status}
        self._r.inc("das_requests_finished_total", {**labels, "tenant": tenant})
        self._r.observe("das_request_duration_seconds", labels, _seconds(duration_seconds))


class LLMMetrics(_Facade):
    """LLM call metrics (Req 4); used by the observer attached to ``LLMProvider``."""

    def call_finished(
        self,
        provider: str,
        model: str,
        operation: LLMOperation,
        outcome: LLMOutcome,
        duration_seconds: float,
    ) -> None:
        base = {"provider": provider, "model": model, "operation": operation}
        self._r.inc("das_llm_calls_total", {**base, "outcome": outcome})
        self._r.observe("das_llm_call_duration_seconds", base, _seconds(duration_seconds))

    def retry_scheduled(self, provider: str, model: str, operation: LLMOperation) -> None:
        self._r.inc(
            "das_llm_retries_total", {"provider": provider, "model": model, "operation": operation}
        )

    def fallback(self, provider: str, model: str) -> None:
        """A call is repeated on the Fallback_Model; ``model`` is the one that failed (Phase 6 Req 2.6)."""
        self._r.inc("das_llm_fallbacks_total", {"provider": provider, "model": model})

    def tokens(
        self, provider: str, model: str, operation: LLMOperation, input_tokens: int, output_tokens: int
    ) -> None:
        base = {"provider": provider, "model": model, "operation": operation}
        self._r.inc("das_llm_tokens_total", {**base, "direction": "input"}, max(0, int(input_tokens)))
        self._r.inc("das_llm_tokens_total", {**base, "direction": "output"}, max(0, int(output_tokens)))

    def estimated_cost(self, provider: str, model: str, amount: Decimal | float) -> None:
        currency = self._r.currency
        if currency is None:
            return
        self._r.inc(
            "das_llm_estimated_cost_total",
            {"provider": provider, "model": model, "currency": currency},
            float(amount),
        )


@dataclass(frozen=True)
class _PricedCall:
    """Minimal Usage_Record view for :func:`das.ops.cost.record_cost`."""

    provider: str
    model: str
    usage: TokenUsage


class LLMMetricsObserver:
    """``LLMCallObserver`` (``das.llm.retry``) that records Req 4 metrics.

    Structurally typed so ``das.observability`` never imports ``das.llm``. Bound to the
    configured provider/model of one ``LLMProvider``; never raises (Req 1.6).
    """

    def __init__(
        self,
        llm_metrics: LLMMetrics,
        provider: str,
        model: str,
        price_table: PriceTable | None = None,
    ) -> None:
        self._m = llm_metrics
        self._provider = provider
        self._model = model
        self._price_table = price_table
        self._entries: dict[str, PriceEntry | None] = {}

    def _entry(self, model: str) -> PriceEntry | None:
        """The Price_Table entry of ``model``, looked up once (a Capability_Model has its own price, Req 2.5)."""
        if model not in self._entries:
            table = self._price_table
            self._entries[model] = table.lookup(self._provider, model) if table is not None else None
        return self._entries[model]

    def on_retry_scheduled(self, operation: LLMOperation, model: str | None = None) -> None:
        try:
            self._m.retry_scheduled(self._provider, model or self._model, operation)
        except Exception:
            pass

    def on_fallback(self, operation: LLMOperation, model: str) -> None:
        try:
            self._m.fallback(self._provider, model)
        except Exception:
            pass

    def on_finished(
        self,
        operation: LLMOperation,
        outcome: LLMOutcome,
        duration: float,
        usage: TokenUsage | None,
        model: str | None = None,
    ) -> None:
        used = model or self._model  # a Capability_Model or the Fallback_Model, else the bound model
        try:
            self._m.call_finished(self._provider, used, operation, outcome, duration)
            if outcome != "success" or usage is None:  # Req 4.5
                return
            self._m.tokens(self._provider, used, operation, usage.input_tokens, usage.output_tokens)
            entry = self._entry(used)
            if entry is not None:  # Req 4.6: unrounded Estimated_Cost
                call = _PricedCall(self._provider, used, usage)
                self._m.estimated_cost(self._provider, used, record_cost(call, entry))
        except Exception:
            pass


class GatewayMetrics(_RequestFinishMixin):
    """Gateway_API metrics (Req 2.1, 2.3, 2.10, 3.9)."""

    def request_submitted(self, tenant: str) -> None:
        self._r.inc("das_requests_submitted_total", {"tenant": tenant})

    def http_request(
        self, route: str | None, method: str, status_code: int, duration_seconds: float
    ) -> None:
        """``route`` is the matched route template, or ``None`` for ``_unmatched``."""
        labels = {"route": route or UNMATCHED_ROUTE, "method": _method(method)}
        self._r.inc("das_http_requests_total", {**labels, "status_code": int(status_code)})
        self._r.observe("das_http_request_duration_seconds", labels, _seconds(duration_seconds))

    def dlq_admin_action(self, action: DlqAction, outcome: DlqAdminOutcome) -> None:
        self._r.inc("das_dlq_admin_actions_total", {"action": action, "outcome": outcome})

    def client_rejected(self, reason: ClientRejectionReason) -> None:
        """A client call answered 429/422/400 by a Phase 4 limit (Phase 4 Req 10.1)."""
        self._r.inc("das_client_rejections_total", {"reason": reason})

    def idempotent_replay(self) -> None:
        """A submit answered from an idempotency record; not a new Request (Phase 4 Req 4.3)."""
        self._r.inc("das_idempotent_replays_total")

    def sse_streams(self, fn: Callable[[], float]) -> None:
        """Gauge of the event streams open on this replica, read when scraped (Phase 4 Req 10.1)."""
        self._r.set_function("das_sse_streams", fn)

    def webhook_delivery(self, outcome: WebhookOutcome) -> None:
        """One delivery attempt ended ``delivered``, ``retry`` or ``failed`` (Phase 4 Req 10.1)."""
        self._r.inc("das_webhook_deliveries_total", {"outcome": outcome})


class ManagerMetrics(_RequestFinishMixin):
    """Manager_Agent metrics (Req 2.2, 2.4, 2.7, 3, 14.10) plus its LLM metrics."""

    def __init__(self, recorder: MetricsRecorder) -> None:
        super().__init__(recorder)
        self.llm = LLMMetrics(recorder)

    def job_transition(self, capability: str, state: JobState) -> None:
        self._r.inc("das_job_transitions_total", {"capability": capability, "state": state})

    def job_attempts(self, capability: str, attempts: int) -> None:
        self._r.observe("das_job_attempts", {"capability": capability}, max(0, int(attempts)))

    def queue_depth(self, queue: str, depth: int) -> None:
        self._r.set("das_queue_depth", {"queue": queue}, max(0, int(depth)))

    def queue_depth_sample_error(self, queue: str) -> None:
        self._r.inc("das_queue_depth_sample_errors_total", {"queue": queue})

    def lease_acquired(self, kind: LeaseKind) -> None:
        self._r.inc("das_lease_acquisitions_total", {"kind": kind})

    def lease_lost(self, reason: LeaseLossReason) -> None:
        self._r.inc("das_lease_losses_total", {"reason": reason})

    def leases_held(self, fn: Callable[[], float]) -> None:
        self._r.set_function("das_leases_held", fn)

    def registry_agents(self, role: RegistryRole, status: RegistryStatus, count: int) -> None:
        self._r.set("das_registry_agents", {"role": role, "status": status}, max(0, int(count)))

    def capability_workers(self, capability: str, count: int) -> None:
        self._r.set("das_capability_workers", {"capability": capability}, max(0, int(count)))

    def registry_sample_error(self) -> None:
        self._r.inc("das_registry_sample_errors_total")

    def dlq_record_created(self, reason: DeadLetterReason) -> None:
        self._r.inc("das_dlq_records_created_total", {"reason": reason})

    def retention_deleted(self, kind: RetentionKind, count: int) -> None:
        if count > 0:
            self._r.inc("das_retention_deleted_total", {"kind": kind}, int(count))

    def budget_exceeded(self, kind: BudgetExceededKind) -> None:
        """A budget-stop terminal write answered OK (Phase 4 Req 10.2)."""
        self._r.inc("das_budget_exceeded_total", {"kind": kind})


class WorkerMetrics(_Facade):
    """Worker_Agent metrics (Req 2.8, 2.9) plus its LLM metrics."""

    def __init__(self, recorder: MetricsRecorder) -> None:
        super().__init__(recorder)
        self.llm = LLMMetrics(recorder)

    def job_executed(self, capability: str, status: JobResultStatus, seconds: float) -> None:
        self._r.observe(
            "das_job_execution_seconds", {"capability": capability, "status": status}, _seconds(seconds)
        )

    def jobs_in_flight(self, fn: Callable[[], float]) -> None:
        self._r.set_function("das_worker_jobs_in_flight", fn)

    def concurrency_limit(self, limit: int) -> None:
        self._r.set("das_worker_concurrency_limit", None, int(limit))

    def tool_call(self, tool: str, outcome: str) -> None:
        """One tool call (Phase 6 Req 5.12); the loop's ``ToolObserver``."""
        self._r.inc("das_tool_calls_total", {"tool": tool, "outcome": outcome})


__all__ = [
    "ALLOWED_LABEL_NAMES",
    "CATALOG",
    "CATALOG_BY_NAME",
    "CONTENT_TYPE",
    "DYNAMIC_LABELS",
    "GatewayMetrics",
    "LLMMetrics",
    "LLMMetricsObserver",
    "ManagerMetrics",
    "MetricSpec",
    "MetricsRecorder",
    "NoopRecorder",
    "RenderError",
    "TOOL_NAMES",
    "TOOL_OUTCOMES",
    "UNMATCHED_ROUTE",
    "WorkerMetrics",
    "build_recorder",
    "policy_for",
]
