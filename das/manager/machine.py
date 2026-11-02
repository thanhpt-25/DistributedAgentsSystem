"""RequestMachine: the pure reducer at the heart of the Manager (Requirements 1.7, 2.6,
2.8, 4, 6, 7.1, 7.3, 13.7).

``step(snapshot, event, ctx) -> Decision`` takes the persisted state of one Request, one
event, and a context (time, limits, registry agents) and returns the State_Store updates
plus the side effects the driver must execute. It performs no I/O.

Store types. ``Decision.updates`` is a ``das.state.store.TransitionBatch`` and the
snapshot's timers are ``das.state.store.Timer`` values, so the driver commits a decision
with ``apply_transition`` unchanged (setting ``inbox_cursor`` with ``dataclasses.replace``
when the event came from the inbox). Those are plain frozen dataclasses; importing them
is allowed by the import-linter contract (only *direct* imports of ``redis`` and broker
clients are forbidden in ``das.manager``) and avoids a parallel batch type plus a
conversion layer.

Transition log (Req 13.7). ``apply_transition.lua`` appends one transition entry for
every Job whose stored state differs from the record in ``updates.jobs``. ``step``
therefore changes each Job's state at most once per call (enforced internally), so every
state change becomes exactly one entry. ``Decision.changes`` lists the same changes for
logging and tests.

Persisted markers. Besides ``JobRecord`` fields the machine keeps its bookkeeping in
the persisted timers zset, so a new lease owner can resume exactly:

- ``retry:{job}:{N}`` — attempt N failed or timed out and a re-dispatch is pending
  (Req 6.1, 6.2). While it exists a ``Failed`` Job is *not* terminal.
- ``publish:{job}:{N}`` — a publish of attempt N was not confirmed; retry pending (4.8).
- ``capwait:{job}:{N}`` — attempt N is Ready but no routing Capability exists yet; it
  fails with ``no capable worker`` once ``capability_wait`` has passed since
  ``ready_since`` (4.6). ``ready_since`` marks the start of the current wait.

A Ready Job with none of these timers has a publish in flight (awaiting
``PublishConfirmed`` / ``PublishFailed``).

Gaps filled beyond the design table (documented choices):

- A Job whose dependency ends ``Cancelled`` (via a Cancelled Job_Result) is Skipped with
  reason ``skipped: dependency <id> cancelled``; otherwise it would wait forever.
- Publish retries are re-armed after ``publish_retry_delay`` (default 1 s).
- ``Gateway`` cancellation (``cancel_request.lua``) treats ``Failed`` as terminal, so a
  Job in ``Failed`` awaiting a retry keeps ``Failed`` when its Request is cancelled.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Final

from das.client.budget import Budget, ExceededKind, budget_status
from das.manager.backoff import backoff
from das.manager.final_result import build_final_result, skip_reason
from das.manager.routing import choose_routing_capability, jobs_destination
from das.models import (
    TERMINAL_JOB,
    AgentRecord,
    FinalResult,
    JobDispatchPayload,
    JobPlan,
    JobRecord,
    JobResultPayload,
    JobState,
    RequestStatus,
)
from das.models.usage import UsageRecord
from das.ops.cost import PriceTable
from das.ops.replay import skipped_to_reset
from das.plan.dag import CycleError, topo_order
from das.state.store import Timer, TimerKey, TransitionBatch

__all__ = [
    "BUDGET_EXCEEDED",
    "BUDGET_UNENFORCEABLE",
    "MAX_PUBLISH_ATTEMPTS",
    "REASON_DISPATCH_FAILURE",
    "REASON_NO_CAPABLE_WORKER",
    "REASON_UNACKED_DELIVERY",
    "TIMER_CAPABILITY_WAIT",
    "TIMER_PUBLISH",
    "TIMER_RETRY",
    "CancelObserved",
    "Decision",
    "Effect",
    "Event",
    "FinalizeFailed",
    "JobResultReceived",
    "MachineConfig",
    "PlanCommitted",
    "PublishConfirmed",
    "PublishDeadLetter",
    "PublishDispatch",
    "PublishFailed",
    "ReplayRequested",
    "RequestSnapshot",
    "RunSummarizer",
    "ScheduleTimer",
    "SendCancel",
    "StateChange",
    "StepContext",
    "Tick",
    "TimerFired",
    "UnackedDeliveryExhausted",
    "UNKNOWN_LLM",
    "apply_decision",
    "attempt_limit",
    "budget_exceeded",
    "is_settled",
    "resume",
    "step",
]

# Timer kinds (must not contain ':' — see das.state.keys.timer_member).
TIMER_RETRY: Final = "retry"
TIMER_PUBLISH: Final = "publish"
TIMER_CAPABILITY_WAIT: Final = "capwait"

# One publish plus 3 retries (Req 4.8).
MAX_PUBLISH_ATTEMPTS: Final = 4

REASON_NO_CAPABLE_WORKER: Final = "no capable worker"
REASON_DISPATCH_FAILURE: Final = (
    f"dispatch failure: publish not confirmed after {MAX_PUBLISH_ATTEMPTS} attempts"
)
REASON_UNACKED_DELIVERY: Final = "repeated unacknowledged delivery"
NO_SUCCESS_ERROR: Final = "no job succeeded"

# Budget stop (Phase 4 Req 5.6, 5.7): Job reason and Request error, and the error of an
# ``unenforceable`` stop.
BUDGET_EXCEEDED: Final = "budget_exceeded"
BUDGET_UNENFORCEABLE: Final = "budget_unenforceable"

# Provider/model recorded when a Job_Result carries no LLM headers (Worker v1, D2).
UNKNOWN_LLM: Final = "unknown"


def cancelled_dependency_reason(dependency_id: str) -> str:
    return f"skipped: dependency {dependency_id} cancelled"


# --------------------------------------------------------------------------- inputs


@dataclass(frozen=True, slots=True)
class MachineConfig:
    """Limits used by the reducer (defaults match ``LimitsConfig``)."""

    max_attempts: int = 3
    base_backoff: float = 2.0
    max_backoff: float = 60.0
    capability_wait: float = 60.0
    publish_retry_delay: float = 1.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.capability_wait <= 0:
            raise ValueError("capability_wait must be positive")
        if self.publish_retry_delay < 0:
            raise ValueError("publish_retry_delay must be >= 0")

    @classmethod
    def from_limits(cls, limits: object) -> MachineConfig:
        """Build from a ``LimitsConfig``-like object."""
        return cls(
            max_attempts=int(limits.max_attempts),  # type: ignore[attr-defined]
            base_backoff=float(limits.base_backoff),  # type: ignore[attr-defined]
            max_backoff=float(limits.max_backoff),  # type: ignore[attr-defined]
            capability_wait=float(limits.capability_wait),  # type: ignore[attr-defined]
        )


@dataclass(frozen=True, slots=True)
class StepContext:
    """Time (Redis server time, seconds), limits, the registry's agents, and the
    Price_Table used to price Budget_Used (Phase 4 Req 5.6)."""

    now: float
    config: MachineConfig = field(default_factory=MachineConfig)
    agents: tuple[AgentRecord, ...] = ()
    price_table: PriceTable | None = None


@dataclass(frozen=True, slots=True)
class RequestSnapshot:
    """The persisted state of one Request that the reducer needs."""

    request_id: str
    status: RequestStatus
    prompt: str
    plan: JobPlan | None
    jobs: Mapping[str, JobRecord]
    results: Mapping[tuple[str, int], JobResultPayload] = field(default_factory=dict)
    timers: tuple[Timer, ...] = ()
    # Every persisted Usage_Record: Budget_Used of a budgeted Request (Phase 4 Req 5.7);
    # ``apply_decision`` appends to it like ``apply_transition.lua`` does.
    usage: tuple[UsageRecord, ...] = ()
    # Store-side state the reducer never reads, kept so ``apply_decision`` can model
    # ``apply_transition.lua``.
    pending_replays: frozenset[str] = frozenset()
    # Effective_Budget (Phase 4 Req 5.4); None means no budget enforcement.
    budget: Budget | None = None

    @classmethod
    def from_stored(cls, stored: object) -> RequestSnapshot:
        """Build from a ``das.state.store.StoredRequest``."""
        return cls(
            request_id=stored.request_id,  # type: ignore[attr-defined]
            status=stored.status,  # type: ignore[attr-defined]
            prompt=stored.prompt,  # type: ignore[attr-defined]
            plan=stored.plan,  # type: ignore[attr-defined]
            jobs=dict(stored.jobs),  # type: ignore[attr-defined]
            results=dict(stored.results),  # type: ignore[attr-defined]
            timers=tuple(stored.timers),  # type: ignore[attr-defined]
            usage=tuple(stored.usage or ()),  # type: ignore[attr-defined]
            budget=stored.budget,  # type: ignore[attr-defined]
        )

    def timer_keys(self) -> frozenset[TimerKey]:
        return frozenset(t.key for t in self.timers)


# --------------------------------------------------------------------------- events


@dataclass(frozen=True, slots=True)
class PlanCommitted:
    """The Job_Plan was committed (all Jobs Waiting); dispatch the roots."""


@dataclass(frozen=True, slots=True)
class Tick:
    """Periodic re-evaluation (readiness, capability wait, summarization trigger)."""


@dataclass(frozen=True, slots=True)
class JobResultReceived:
    """A Job_Result from the inbox.

    ``llm_meta`` is the inbox entry's ``meta`` (``{"provider": ..., "model": ...}`` from
    the Transport headers, D2). Missing keys or ``None`` (Worker v1) record
    ``provider = model = "unknown"`` in the Usage_Record.
    """

    result: JobResultPayload
    llm_meta: Mapping[str, str] | None = None


@dataclass(frozen=True, slots=True)
class ReplayRequested:
    """An accepted Replay of a dead-lettered Job, read from the inbox (Req 13.3, 13.4)."""

    dead_letter_id: str
    job_id: str
    attempt: int


@dataclass(frozen=True, slots=True)
class PublishConfirmed:
    job_id: str
    attempt: int


@dataclass(frozen=True, slots=True)
class PublishFailed:
    """A dispatch publish was rejected or not confirmed.

    ``n`` is informational (the driver's view of the failure count); the persisted
    ``JobRecord.publish_failures`` counter is authoritative.
    """

    job_id: str
    attempt: int
    n: int | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class TimerFired:
    kind: str
    job_id: str
    attempt: int


@dataclass(frozen=True, slots=True)
class UnackedDeliveryExhausted:
    """The broker dead-lettered the dispatch after repeated unacked delivery (Req 6.6)."""

    job_id: str


@dataclass(frozen=True, slots=True)
class CancelObserved:
    """The driver learned the Request was cancelled (e.g. ``cancel_request`` command)."""


Event = (
    PlanCommitted
    | Tick
    | JobResultReceived
    | PublishConfirmed
    | PublishFailed
    | TimerFired
    | UnackedDeliveryExhausted
    | CancelObserved
    | ReplayRequested
)


# --------------------------------------------------------------------------- effects


@dataclass(frozen=True, slots=True)
class PublishDispatch:
    """Publish one Job dispatch to ``jobs/<routing_cap>`` (Req 4.1, 4.2, 6.1)."""

    job_id: str
    attempt: int
    routing_cap: str
    instruction: str
    required_capabilities: tuple[str, ...]
    dependency_results: Mapping[str, str]  # dep Job_ID -> content

    @property
    def destination(self) -> str:
        return jobs_destination(self.routing_cap)

    def to_payload(self, request_id: str) -> JobDispatchPayload:
        return JobDispatchPayload(
            request_id=request_id,
            job_id=self.job_id,
            attempt=self.attempt,
            instruction=self.instruction,
            required_capabilities=list(self.required_capabilities),
            dependency_results=dict(self.dependency_results),
        )


@dataclass(frozen=True, slots=True)
class PublishDeadLetter:
    """Route the Job's dispatch to the Dead_Letter_Queue (Req 6.3)."""

    request_id: str
    job_id: str
    attempts: int
    last_error: str


@dataclass(frozen=True, slots=True)
class SendCancel:
    """Best-effort ``cancel_job`` control command to the Worker running the Job."""

    job_id: str
    attempt: int


@dataclass(frozen=True, slots=True)
class ScheduleTimer:
    """Arm an in-memory wakeup; the timer itself is persisted in ``updates``."""

    at: float
    kind: str
    job_id: str
    attempt: int


@dataclass(frozen=True, slots=True)
class RunSummarizer:
    """Every Job is terminal and ≥ 1 Succeeded (Req 7.1).

    Inputs are the original prompt and exactly the Succeeded Job_Result contents. The
    reducer emits this on every step while the condition holds; the driver runs the
    Summarizer once and then finalizes the Request.
    """

    prompt: str
    succeeded: Mapping[str, str]  # Job_ID -> content


@dataclass(frozen=True, slots=True)
class FinalizeFailed:
    """Every Job is terminal and none Succeeded: store this and fail the Request (7.3).

    Also emitted for a budget stop once no Job is in flight (Phase 4 Req 5.7); then
    ``budget_kind`` is set and ``final_result.error`` is ``budget_exceeded`` or
    ``budget_unenforceable``.
    """

    final_result: FinalResult
    budget_kind: ExceededKind | None = None


Effect = (
    PublishDispatch
    | PublishDeadLetter
    | SendCancel
    | ScheduleTimer
    | RunSummarizer
    | FinalizeFailed
)


# --------------------------------------------------------------------------- output


@dataclass(frozen=True, slots=True)
class StateChange:
    """One Job state change; mirrors the transition entry the store will append."""

    job_id: str
    from_state: JobState | None
    to_state: JobState
    attempt: int
    worker_id: str | None


@dataclass(frozen=True, slots=True)
class Decision:
    updates: TransitionBatch = field(default_factory=TransitionBatch)
    effects: tuple[Effect, ...] = ()
    changes: tuple[StateChange, ...] = ()
    # Messages the driver logs at warning level (e.g. a reverted replay, Req 13.15).
    warnings: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        u = self.updates
        return not (
            u.jobs
            or u.results
            or u.timers_add
            or u.timers_remove
            or u.request_fields
            or u.usage
            or u.replays_applied
            or u.dead_letter_reverts
        ) and not self.effects


_EMPTY: Final = Decision()


# --------------------------------------------------------------------------- helpers


def attempt_limit(record: JobRecord, config: MachineConfig) -> int:
    """Effective attempt limit: raised by a Replay (Replay_Budget), else ``max_attempts``."""
    return record.attempt_limit or config.max_attempts


def _awaiting_retry(record: JobRecord, timers: Iterable[TimerKey]) -> bool:
    return record.state in (JobState.FAILED, JobState.TIMED_OUT) and (
        TimerKey(TIMER_RETRY, record.job.job_id, record.attempt) in timers
    )


def budget_exceeded(snapshot: RequestSnapshot, ctx: StepContext) -> ExceededKind | None:
    """Exceeded kind of the snapshot's persisted Budget_Used, or None (Phase 4 Req 5.6, 5.7)."""
    if snapshot.budget is None:
        return None
    return budget_status(snapshot.usage, snapshot.budget, ctx.price_table).exceeded


def is_settled(record: JobRecord, timers: Iterable[TimerKey]) -> bool:
    """Terminal for scheduling purposes: in ``TERMINAL_JOB`` and not awaiting a retry."""
    keys = timers if isinstance(timers, (set, frozenset, dict)) else set(timers)
    return record.state in TERMINAL_JOB and not _awaiting_retry(record, keys)


def _plan_order(plan: JobPlan) -> list[str]:
    try:
        return topo_order(plan)
    except CycleError:  # unreachable for a validated plan; stay total anyway
        return list(dict.fromkeys(plan.job_ids()))


def _dependency_inputs(
    record: JobRecord,
    jobs: Mapping[str, JobRecord],
    results: Mapping[tuple[str, int], JobResultPayload],
) -> dict[str, str]:
    """``{dep_id: content}`` of each dependency's Succeeded Job_Result (Req 4.2)."""
    inputs: dict[str, str] = {}
    for dep in dict.fromkeys(record.job.depends_on):
        dep_rec = jobs.get(dep)
        result = results.get((dep, dep_rec.attempt)) if dep_rec is not None else None
        if result is None or result.status != "Succeeded":
            succeeded = sorted(
                (r for (jid, _), r in results.items() if jid == dep and r.status == "Succeeded"),
                key=lambda r: r.attempt,
            )
            if not succeeded:
                raise ValueError(f"dependency {dep!r} of {record.job.job_id!r} has no Succeeded result")
            result = succeeded[0]
        inputs[dep] = result.content
    return inputs


class _Work:
    """Mutable working copy of a snapshot while one step is computed."""

    def __init__(self, snapshot: RequestSnapshot, ctx: StepContext) -> None:
        self.snap = snapshot
        self.ctx = ctx
        self.now = ctx.now
        self.cfg = ctx.config
        self.jobs: dict[str, JobRecord] = dict(snapshot.jobs)
        self.results: dict[tuple[str, int], JobResultPayload] = dict(snapshot.results)
        self.timers: dict[TimerKey, Timer] = {t.key: t for t in snapshot.timers}
        self._snap_timer_keys = set(self.timers)
        self.new_results: list[JobResultPayload] = []
        self.timers_add: dict[TimerKey, Timer] = {}
        self.timers_remove: set[TimerKey] = set()
        self.effects: list[Effect] = []
        self.dispatched: set[str] = set()
        self.new_usage: list[UsageRecord] = []
        self.tag: str | None = None
        self.replays_applied: list[str] = []
        self.dead_letter_reverts: list[str] = []
        self.warnings: list[str] = []
        # Skipped Jobs reset to Waiting by a replay in this step; failure propagation
        # leaves them alone so each Job changes state at most once per step.
        self.reset_jobs: set[str] = set()

    # -- budget

    def budget_kind(self) -> ExceededKind | None:
        """Exceeded kind of Budget_Used including usage accepted earlier in this step.

        Not None means budget-stop mode (Phase 4 Req 5.7). Recomputed from persisted usage
        on every step, so a new lease owner reaches the same decision (v1 Req 2.5).
        """
        budget = self.snap.budget
        if budget is None:
            return None
        usage = (*self.snap.usage, *self.new_usage)
        return budget_status(usage, budget, self.ctx.price_table).exceeded

    # -- jobs

    def put(self, record: JobRecord) -> None:
        job_id = record.job.job_id
        original = self.snap.jobs.get(job_id)
        current = self.jobs.get(job_id)
        if (
            original is not None
            and current is not None
            and current.state != original.state
            and record.state != current.state
        ):
            raise RuntimeError(
                f"job {job_id} would change state twice in one step "
                f"({original.state} -> {current.state} -> {record.state})"
            )
        self.jobs[job_id] = record

    def update(self, job_id: str, **changes: object) -> JobRecord:
        record = self.jobs[job_id].model_copy(update=changes)
        self.put(record)
        return record

    # -- timers

    def add_timer(self, kind: str, job_id: str, attempt: int, due: float) -> None:
        timer = Timer(kind=kind, job_id=job_id, attempt=attempt, due=due)
        self.timers[timer.key] = timer
        self.timers_add[timer.key] = timer
        self.timers_remove.discard(timer.key)
        self.effects.append(ScheduleTimer(at=due, kind=kind, job_id=job_id, attempt=attempt))

    def has_timer(self, kind: str, job_id: str, attempt: int) -> bool:
        return TimerKey(kind, job_id, attempt) in self.timers

    def remove_timer(self, key: TimerKey) -> None:
        self.timers.pop(key, None)
        self.timers_add.pop(key, None)
        if key in self._snap_timer_keys:
            self.timers_remove.add(key)

    def remove_job_timers(self, job_id: str) -> None:
        for key in [k for k in self.timers if k.job_id == job_id]:
            self.remove_timer(key)

    # -- dispatch

    def try_dispatch(self, job_id: str) -> bool:
        """Emit ``PublishDispatch`` for a Ready Job if a routing Capability exists."""
        record = self.jobs[job_id]
        cap = choose_routing_capability(record.job.capabilities, self.ctx.agents)
        if cap is None:
            return False
        self.effects.append(_dispatch_effect(record, cap, self.jobs, self.results))
        self.dispatched.add(job_id)
        return True

    def start_capability_wait(self, job_id: str) -> None:
        record = self.update(job_id, ready_since=self.now)
        self.add_timer(
            TIMER_CAPABILITY_WAIT, job_id, record.attempt, self.now + self.cfg.capability_wait
        )

    def enter_ready(self, job_id: str, attempt: int) -> None:
        """Waiting/Failed/TimedOut -> Ready for ``attempt`` and try to dispatch it."""
        self.update(
            job_id,
            state=JobState.READY,
            attempt=attempt,
            ready_since=self.now,
            publish_failures=0,
            worker_id=None,
        )
        if not self.try_dispatch(job_id):
            self.start_capability_wait(job_id)

    def fail(self, job_id: str, reason: str) -> None:
        self.update(job_id, state=JobState.FAILED, reason=reason)
        self.remove_job_timers(job_id)

    # -- output

    def decision(self) -> Decision:
        jobs: dict[str, JobRecord] = {}
        changes: list[StateChange] = []
        order = self.snap.plan.job_ids() if self.snap.plan is not None else list(self.jobs)
        for job_id in dict.fromkeys([*order, *self.jobs]):
            new = self.jobs.get(job_id)
            old = self.snap.jobs.get(job_id)
            if new is None or new == old:
                continue
            jobs[job_id] = new
            if old is None or old.state != new.state:
                changes.append(
                    StateChange(
                        job_id=job_id,
                        from_state=old.state if old is not None else None,
                        to_state=new.state,
                        attempt=new.attempt,
                        worker_id=new.worker_id,
                    )
                )
        batch = TransitionBatch(
            jobs=jobs,
            results=tuple(self.new_results),
            timers_add=tuple(self.timers_add.values()),
            timers_remove=tuple(sorted(self.timers_remove, key=lambda k: k.member)),
            usage=tuple(self.new_usage),
            tag=self.tag,
            replays_applied=tuple(self.replays_applied),
            dead_letter_reverts=tuple(self.dead_letter_reverts),
        )
        return Decision(
            updates=batch,
            effects=tuple(self.effects),
            changes=tuple(changes),
            warnings=tuple(self.warnings),
        )


def _dispatch_effect(
    record: JobRecord,
    cap: str,
    jobs: Mapping[str, JobRecord],
    results: Mapping[tuple[str, int], JobResultPayload],
) -> PublishDispatch:
    return PublishDispatch(
        job_id=record.job.job_id,
        attempt=record.attempt,
        routing_cap=cap,
        instruction=record.job.instruction,
        required_capabilities=tuple(record.job.capabilities),
        dependency_results=_dependency_inputs(record, jobs, results),
    )


# --------------------------------------------------------------------------- handlers


def _job_usage(result: JobResultPayload, llm_meta: Mapping[str, str] | None) -> UsageRecord:
    """Usage_Record (operation ``job``) of an accepted Job_Result (Req 9.2)."""
    meta = llm_meta or {}
    provider = meta.get("provider") or UNKNOWN_LLM
    model = meta.get("model") or UNKNOWN_LLM
    return UsageRecord(
        operation="job",
        provider=str(provider),
        model=str(model),
        usage=result.usage,
        job_id=result.job_id,
        attempt=result.attempt,
    )


def _on_result(
    w: _Work, result: JobResultPayload, llm_meta: Mapping[str, str] | None = None
) -> None:
    record = w.jobs.get(result.job_id)
    if record is None or result.request_id != w.snap.request_id:
        return
    key = (result.job_id, result.attempt)
    # Idempotency (Req 2.8, 6.5): duplicates, stale or future attempts are discarded.
    if result.attempt != record.attempt or key in w.results:
        return
    if record.state not in (JobState.READY, JobState.DISPATCHED):
        return
    job_id = result.job_id
    w.results[key] = result
    w.new_results.append(result)
    # Only accepted results produce usage (Req 9.2, 9.5, 13.10).
    w.new_usage.append(_job_usage(result, llm_meta))
    w.remove_job_timers(job_id)  # pending publish / capability-wait timers of this attempt

    if result.status == "Succeeded":
        w.update(job_id, state=JobState.SUCCEEDED, worker_id=result.worker_id, reason=None)
    elif result.status == "Cancelled":
        # Req 6.7: no retry, no dead-letter.
        w.update(
            job_id,
            state=JobState.CANCELLED,
            worker_id=result.worker_id,
            reason=result.error or "cancelled",
        )
    else:  # Failed or TimedOut
        reason = result.error or f"job {result.status}"
        if w.budget_kind() is not None:
            # Phase 4 Req 5.7: in budget-stop mode (this result's usage included) a failed
            # Job is neither retried nor dead-lettered.
            w.update(job_id, state=JobState.FAILED, worker_id=result.worker_id, reason=BUDGET_EXCEEDED)
        elif result.attempt >= attempt_limit(record, w.cfg):
            # Req 6.3: final attempt exhausted (limit raised by a Replay, Req 13.4).
            w.update(job_id, state=JobState.FAILED, worker_id=result.worker_id, reason=reason)
            w.effects.append(
                PublishDeadLetter(
                    request_id=w.snap.request_id,
                    job_id=job_id,
                    attempts=result.attempt,
                    last_error=reason,
                )
            )
        else:
            # Req 6.1, 6.2: record the attempt's outcome and schedule the re-dispatch.
            w.update(
                job_id,
                state=JobState(result.status),
                worker_id=result.worker_id,
                reason=reason,
            )
            delay = backoff(result.attempt, w.cfg.base_backoff, w.cfg.max_backoff)
            w.add_timer(TIMER_RETRY, job_id, result.attempt, w.now + delay)


def _publish_outstanding(w: _Work, record: JobRecord) -> bool:
    job_id, attempt = record.job.job_id, record.attempt
    return (
        record.state is JobState.READY
        and not w.has_timer(TIMER_PUBLISH, job_id, attempt)
        and not w.has_timer(TIMER_CAPABILITY_WAIT, job_id, attempt)
    )


def _on_publish_confirmed(w: _Work, event: PublishConfirmed) -> None:
    record = w.jobs.get(event.job_id)
    if record is None or record.state is not JobState.READY or record.attempt != event.attempt:
        return
    w.update(event.job_id, state=JobState.DISPATCHED, publish_failures=0)
    w.remove_job_timers(event.job_id)


def _on_publish_failed(w: _Work, event: PublishFailed) -> None:
    record = w.jobs.get(event.job_id)
    if record is None or record.attempt != event.attempt or not _publish_outstanding(w, record):
        return
    if w.budget_kind() is not None:
        # Phase 4 Req 5.7: never re-published, so not in flight any more.
        w.update(event.job_id, state=JobState.SKIPPED, reason=BUDGET_EXCEEDED)
        w.remove_job_timers(event.job_id)
        return
    failures = record.publish_failures + 1
    if failures >= MAX_PUBLISH_ATTEMPTS:
        w.update(event.job_id, publish_failures=failures)
        w.fail(event.job_id, REASON_DISPATCH_FAILURE)
        return
    w.update(event.job_id, publish_failures=failures)
    w.add_timer(TIMER_PUBLISH, event.job_id, record.attempt, w.now + w.cfg.publish_retry_delay)


def _on_timer(w: _Work, event: TimerFired) -> None:
    key = TimerKey(event.kind, event.job_id, event.attempt)
    record = w.jobs.get(event.job_id)
    if key not in w.timers or record is None:
        return
    if event.kind == TIMER_CAPABILITY_WAIT:
        return  # evaluated by the capability-wait pass in _settle
    w.remove_timer(key)
    if record.attempt != event.attempt:
        return
    if w.budget_kind() is not None:
        # Phase 4 Req 5.7: no re-dispatch. A retry ends the Job; a Ready Job whose publish
        # timer fired is Skipped by _settle.
        if event.kind == TIMER_RETRY and record.state in (JobState.FAILED, JobState.TIMED_OUT):
            w.update(event.job_id, state=JobState.FAILED, reason=BUDGET_EXCEEDED)
        return
    if event.kind == TIMER_RETRY and record.state in (JobState.FAILED, JobState.TIMED_OUT):
        w.enter_ready(event.job_id, record.attempt + 1)
    elif event.kind == TIMER_PUBLISH and record.state is JobState.READY:
        if not w.try_dispatch(event.job_id):
            w.start_capability_wait(event.job_id)


def _replay_violation(w: _Work, event: ReplayRequested) -> str | None:
    """The violated condition that makes a replay event inapplicable (Req 13.15)."""
    record = w.jobs.get(event.job_id)
    if record is None:
        return "job not found"
    if record.state is not JobState.FAILED:
        return f"job state {record.state}"
    if record.attempt != event.attempt:
        return f"attempt {event.attempt} != current attempt {record.attempt}"
    if w.has_timer(TIMER_RETRY, event.job_id, record.attempt):
        return "job awaiting retry"
    return None


def _revert_replay(w: _Work, event: ReplayRequested, violation: str) -> None:
    w.dead_letter_reverts.append(event.dead_letter_id)
    w.replays_applied.append(event.dead_letter_id)
    w.warnings.append(
        f"replay {event.dead_letter_id} of job {event.job_id} not applied: {violation}"
    )


def _on_replay(w: _Work, event: ReplayRequested) -> None:
    """Apply an accepted Replay (Req 13.4) or revert its record to Pending (13.15).

    In budget-stop mode every Replay is reverted (Phase 4 Req 5.7).
    """
    violation = "budget exceeded" if w.budget_kind() is not None else _replay_violation(w, event)
    if violation is not None:
        _revert_replay(w, event, violation)
        return
    plan = w.snap.plan
    assert plan is not None
    record = w.jobs[event.job_id]
    # Computed while the replayed Job is still Failed.
    to_reset = skipped_to_reset(plan, w.jobs, event.job_id)
    w.tag = event.dead_letter_id
    w.update(
        event.job_id,
        attempt_limit=record.attempt + w.cfg.max_attempts,  # Replay_Budget
        publish_failures=0,
        reason=None,
    )
    w.remove_job_timers(event.job_id)
    w.enter_ready(event.job_id, record.attempt + 1)
    for job_id in plan.job_ids():
        if job_id in to_reset:
            w.update(job_id, state=JobState.WAITING, reason=None)
            w.reset_jobs.add(job_id)
    w.replays_applied.append(event.dead_letter_id)


def _on_unacked_exhausted(w: _Work, event: UnackedDeliveryExhausted) -> None:
    record = w.jobs.get(event.job_id)
    if record is None or record.state not in (JobState.READY, JobState.DISPATCHED):
        return
    w.fail(event.job_id, REASON_UNACKED_DELIVERY)


def _settle(w: _Work) -> None:
    """Re-evaluate the Request after an event: capability wait, failure propagation,
    readiness, and the summarization trigger."""
    plan = w.snap.plan
    assert plan is not None
    order = [j for j in _plan_order(plan) if j in w.jobs]

    kind = w.budget_kind()
    if kind is not None:
        _settle_budget_stop(w, plan, order, kind)
        return

    # Ready Jobs waiting for a routing Capability (Req 4.6).
    for job_id in order:
        record = w.jobs[job_id]
        if record.state is not JobState.READY or job_id in w.dispatched:
            continue
        key = TimerKey(TIMER_CAPABILITY_WAIT, job_id, record.attempt)
        if key not in w.timers:
            continue
        if w.try_dispatch(job_id):
            w.remove_timer(key)
        else:
            since = record.ready_since if record.ready_since is not None else w.now
            if w.now - since >= w.cfg.capability_wait:
                w.fail(job_id, REASON_NO_CAPABLE_WORKER)

    # Failure propagation (Req 4.4): topo order carries the skip down chains.
    timer_keys = w.timers.keys()
    for job_id in order:
        record = w.jobs[job_id]
        if record.state is not JobState.WAITING or job_id in w.reset_jobs:
            continue
        reason: str | None = None
        for dep in record.job.depends_on:
            dep_rec = w.jobs.get(dep)
            if dep_rec is None or not is_settled(dep_rec, timer_keys):
                continue
            if dep_rec.state is JobState.FAILED:
                reason = skip_reason(dep)
            elif dep_rec.state is JobState.CANCELLED:
                reason = cancelled_dependency_reason(dep)
            elif dep_rec.state is JobState.SKIPPED:
                reason = dep_rec.reason or skip_reason(dep)
            if reason is not None:
                break
        if reason is not None:
            w.update(job_id, state=JobState.SKIPPED, reason=reason)

    # Readiness (Req 4.1, 4.3).
    for job_id in order:
        record = w.jobs[job_id]
        if record.state is JobState.WAITING and all(
            w.jobs.get(d) is not None and w.jobs[d].state is JobState.SUCCEEDED
            for d in record.job.depends_on
        ):
            w.enter_ready(job_id, record.attempt + 1)

    # Summarization trigger (Req 7.1, 7.3).
    timer_keys = w.timers.keys()
    if order and all(is_settled(w.jobs[j], timer_keys) for j in order):
        succeeded = {
            j: w.results[(j, w.jobs[j].attempt)].content
            for j in plan.job_ids()
            if w.jobs[j].state is JobState.SUCCEEDED and (j, w.jobs[j].attempt) in w.results
        }
        if any(w.jobs[j].state is JobState.SUCCEEDED for j in order):
            w.effects.append(RunSummarizer(prompt=w.snap.prompt, succeeded=succeeded))
        else:
            final = build_final_result(plan, w.jobs, w.results, None, NO_SUCCESS_ERROR)
            w.effects.append(FinalizeFailed(final_result=final))


def _never_published(w: _Work, record: JobRecord) -> bool:
    """A Ready Job whose current attempt has (or had at the start of this step) a pending
    ``publish`` or ``capwait`` timer: not an In_Flight_Job (Phase 4 Req 5.7)."""
    keys = (
        TimerKey(TIMER_PUBLISH, record.job.job_id, record.attempt),
        TimerKey(TIMER_CAPABILITY_WAIT, record.job.job_id, record.attempt),
    )
    return record.state is JobState.READY and any(
        k in w.timers or k in w._snap_timer_keys for k in keys
    )


def _settle_budget_stop(w: _Work, plan: JobPlan, order: list[str], kind: ExceededKind) -> None:
    """Budget-stop mode (Phase 4 Req 5.7): replaces capability wait, failure propagation
    and readiness. Nothing is dispatched; every Job that is not in flight ends with reason
    ``budget_exceeded``; once no Job is in flight the Request fails without the Summarizer.
    """
    for job_id in order:
        record = w.jobs[job_id]
        if record.state is JobState.WAITING:
            w.update(job_id, state=JobState.SKIPPED, reason=BUDGET_EXCEEDED)
        elif _never_published(w, record):
            w.update(job_id, state=JobState.SKIPPED, reason=BUDGET_EXCEEDED)
            w.remove_job_timers(job_id)
        elif _awaiting_retry(record, w.timers.keys()):
            w.update(job_id, state=JobState.FAILED, reason=BUDGET_EXCEEDED)
            w.remove_timer(TimerKey(TIMER_RETRY, job_id, record.attempt))

    timer_keys = w.timers.keys()
    if order and all(is_settled(w.jobs[j], timer_keys) for j in order):
        error = BUDGET_UNENFORCEABLE if kind == "unenforceable" else BUDGET_EXCEEDED
        final = build_final_result(plan, w.jobs, w.results, None, error)
        w.effects.append(FinalizeFailed(final_result=final, budget_kind=kind))


def _in_flight(record: JobRecord, results: Mapping[tuple[str, int], JobResultPayload]) -> bool:
    return (
        record.state in (JobState.READY, JobState.DISPATCHED)
        and record.attempt >= 1
        and (record.job.job_id, record.attempt) not in results
    )


# --------------------------------------------------------------------------- API


def step(snapshot: RequestSnapshot, event: Event, ctx: StepContext) -> Decision:
    """Apply one event to a Request snapshot (rules: design "RequestMachine" table).

    Requests that are not Running (Pending without a plan, or terminal, in particular
    Cancelled — Req 1.7) are inert: the decision is empty.
    """
    if snapshot.status is not RequestStatus.RUNNING or snapshot.plan is None:
        if isinstance(event, ReplayRequested):
            # Req 13.15: the Request is no longer Running; give the record back.
            w = _Work(snapshot, ctx)
            _revert_replay(w, event, f"request status {snapshot.status}")
            return w.decision()
        return _EMPTY
    w = _Work(snapshot, ctx)
    match event:
        case CancelObserved():
            # Store-side cancellation is atomic (cancel_request.lua); only tell Workers.
            for job_id in snapshot.plan.job_ids():
                record = w.jobs.get(job_id)
                if record is not None and _in_flight(record, w.results):
                    w.effects.append(SendCancel(job_id=job_id, attempt=record.attempt))
            return w.decision()
        case JobResultReceived(result=result, llm_meta=llm_meta):
            _on_result(w, result, llm_meta)
        case ReplayRequested():
            _on_replay(w, event)
        case PublishConfirmed():
            _on_publish_confirmed(w, event)
        case PublishFailed():
            _on_publish_failed(w, event)
        case TimerFired():
            _on_timer(w, event)
        case UnackedDeliveryExhausted():
            _on_unacked_exhausted(w, event)
        case PlanCommitted() | Tick():
            pass
        case _:
            raise TypeError(f"unknown event: {event!r}")
    _settle(w)
    return w.decision()


def resume(snapshot: RequestSnapshot, agents: Iterable[AgentRecord] = ()) -> list[Effect]:
    """Effects a new lease owner executes before stepping a Request (Req 2.6).

    - Re-arms every persisted timer (``ScheduleTimer``).
    - Re-emits ``PublishDispatch`` for every Job in Ready or Dispatched with no Job_Result
      recorded for its current attempt, carrying the persisted Succeeded dependency
      contents. Exception: a Ready Job with a pending ``publish`` or ``capwait`` timer was
      never successfully published for this attempt and is re-driven by that timer or by
      the next ``Tick`` instead.
    - Jobs in Waiting or a terminal state are never re-dispatched.

    The routing Capability is chosen from ``agents``; when none qualifies right now the
    Job is re-published to ``jobs/min(required)`` (the worker-side guard requeues it from
    a Worker lacking a Capability). Duplicate deliveries are absorbed by result
    idempotency (Req 6.5, 2.8).
    """
    if snapshot.status is not RequestStatus.RUNNING or snapshot.plan is None:
        return []
    agents = tuple(agents)
    effects: list[Effect] = [
        ScheduleTimer(at=t.due, kind=t.kind, job_id=t.job_id, attempt=t.attempt)
        for t in sorted(snapshot.timers, key=lambda t: (t.due, t.key.member))
    ]
    timer_keys = snapshot.timer_keys()
    for job_id in snapshot.plan.job_ids():
        record = snapshot.jobs.get(job_id)
        if record is None or not _in_flight(record, snapshot.results):
            continue
        if record.state is JobState.READY and (
            TimerKey(TIMER_PUBLISH, job_id, record.attempt) in timer_keys
            or TimerKey(TIMER_CAPABILITY_WAIT, job_id, record.attempt) in timer_keys
        ):
            continue
        cap = choose_routing_capability(record.job.capabilities, agents) or min(
            record.job.capabilities
        )
        effects.append(_dispatch_effect(record, cap, snapshot.jobs, snapshot.results))
    return effects


def apply_decision(snapshot: RequestSnapshot, decision: Decision) -> RequestSnapshot:
    """The snapshot the State_Store holds after committing ``decision`` (pure model of
    ``apply_transition.lua``: Job records replaced, results written first-wins (HSETNX),
    a Usage_Record appended only when the result with the same field was new, timers
    removed then added, ``replays_applied`` removed from ``pending_replays``).

    ``dead_letter_reverts`` and ``tag`` touch Dead_Letter_Records and transition
    entries, which the snapshot does not hold.
    """
    u = decision.updates
    jobs = dict(snapshot.jobs)
    jobs.update(u.jobs)
    results = dict(snapshot.results)
    new_fields: set[tuple[str, int]] = set()
    for result in u.results:
        key = (result.job_id, result.attempt)
        if key not in results:
            results[key] = result
            new_fields.add(key)
    usage = list(snapshot.usage)
    for record in u.usage:
        key = (record.job_id or "", record.attempt or 0)
        if key in new_fields:
            usage.append(record)
            new_fields.discard(key)  # at most one Usage_Record per result
    removed = set(u.timers_remove)
    timers = {t.key: t for t in snapshot.timers if t.key not in removed}
    for t in u.timers_add:
        timers[t.key] = t
    return RequestSnapshot(
        request_id=snapshot.request_id,
        status=snapshot.status,
        prompt=snapshot.prompt,
        plan=snapshot.plan,
        jobs=jobs,
        results=results,
        timers=tuple(sorted(timers.values(), key=lambda t: (t.due, t.key.member))),
        usage=tuple(usage),
        pending_replays=snapshot.pending_replays - frozenset(u.replays_applied),
        budget=snapshot.budget,
    )
