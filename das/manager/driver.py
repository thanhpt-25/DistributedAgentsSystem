"""RequestDriver: the per-owned-Request task of a Manager_Agent (Requirements 1.6, 1.7,
2.6, 2.7, 3.1, 3.6, 3.9-3.12, 4.1, 4.2, 6.3, 6.6, 7.1, 7.3, 7.4, 7.6, 7.7, 13.4-13.5).

One :class:`RequestDriver` runs for every Request this Manager holds a Lease on. It is
the thin I/O shell around the pure reducer in :mod:`das.manager.machine`:

1. **Plan** (Pending Request). Read the advertised Capabilities from the Agent_Registry
   and call the Planner. A valid plan is persisted with ``commit_plan`` (plan + Running
   in one atomic script, 3.9) and stepped with ``PlanCommitted``. No Capabilities
   (3.10), a rejected plan (3.6), or a provider error (3.11) fail the Request through
   ``fail_request``, which also releases the Lease.
2. **Resume** (Running Request claimed from another Manager, 2.6). Execute
   ``machine.resume`` effects: re-dispatch in-flight Jobs without a result.
3. **Event loop**. Every 250 ms tick (or sooner when woken by :meth:`notify` or a due
   timer) the driver checks the Request status, drains unapplied inbox entries
   (Job_Results and ``UnackedDeliveryExhausted`` events), fires due persisted timers,
   and steps a ``Tick``. Every decision is committed with ``apply_transition`` (the
   inbox cursor rides in the same batch) *before* its effects run, so a Job is Ready in
   the State_Store before its dispatch is published and becomes Dispatched only after
   the publish confirmed (4.1). Publish outcomes are fed back as ``PublishConfirmed`` /
   ``PublishFailed`` events.
4. **Finish**. ``RunSummarizer`` runs the Summarizer once and ``finalize``s Completed
   (7.1, 7.4) or Failed with the summarization error (7.6). ``FinalizeFailed`` stores
   the no-success report (7.3). The final write is retried 3 more times at 1 s
   intervals; if every attempt fails the driver stops renewing the Lease and leaves the
   Request untouched so another Manager can claim it after expiry (7.7).

Lease loss (2.7, 3.12). The :class:`~das.manager.lease.LeaseKeeper` cancels the driver
task when renewal fails, which abandons any in-flight Planner or Summarizer call; the
driver also checks ``keeper.is_lost`` before every write and publish, so Planner or
Summarizer output finished after the loss is discarded. Every guarded script is fenced
by the Lease epoch as well, so a stale owner cannot write anyway.

Cancellation (1.6, 1.7). The Gateway cancels atomically in the State_Store and sends a
best-effort ``cancel_request`` control command to the owner (:meth:`request_cancel`).
The driver notices Cancelled through that command, its per-tick status check, or a
``REQUEST_TERMINAL`` reply, steps ``CancelObserved`` on its last Running snapshot, and
sends best-effort ``cancel_job`` commands for in-flight Jobs through the control
Transport before releasing the Lease.

Replay (Phase 2 Req 13.3-13.5, 13.15; design D7, D8). A ``replay`` inbox entry is
stepped as ``ReplayRequested``; the reducer re-readies the Job or reverts the
Dead_Letter_Record, and the batch is fenced like every other transition. A Pending
Request that already has a Job_Plan was reopened by the Replay of a Failed Request:
the new owner runs ``reopen_request`` (Pending -> Running) and resumes instead of
planning. ``finalize`` answering ``REPLAY_PENDING`` means a Replay is waiting in the
inbox: the driver drops the pending finish, drains the inbox again, and re-evaluates.

Usage (Phase 2 Req 9.1, 9.3, 9.4). The Planner's UsageRecords ride in ``commit_plan``
or in the ``fail_request``/``finalize`` that fails the Request; the Summarizer's ride
in ``finalize``. A write answered ``LEASE_LOST`` / ``REQUEST_TERMINAL`` persisted
nothing, and the records are dropped with the output they belong to.

Release (Phase 2 Req 17.5). :meth:`RequestDriver.request_release` makes the driver
stop at the next event boundary (an in-flight State_Store write completes first; an
in-flight Planner or Summarizer call is abandoned), then release the Lease with the
fenced ``release_lease`` script.

Observability (Phase 2 Req 2.2, 2.4-2.7, 6.2-6.4, 6.8, 6.10, 6.12). Request and Job
metrics are recorded only after the State_Store write that produced them answered
OK: ``finalize`` OK counts the finished Request with the script-returned duration,
and each ``Decision.changes`` entry (plus the Waiting Jobs of ``commit_plan``) counts
one transition. ``run`` wraps the Request in a ``manager.request`` Span parented on
the persisted Trace_Context (a new root when there is none, never written back);
planning and Summarizer attempts get ``manager.plan`` / ``manager.summarize`` through
the ``on_attempt`` hook, and each dispatch publish a ``manager.dispatch`` Span whose
context rides in the envelope. ``LEASE_LOST``, ``REQUEST_TERMINAL`` and planning or
summarization failures mark ``manager.request`` failed.

Timers. Persisted timers in the snapshot drive wakeups, so ``ScheduleTimer`` effects
need no in-memory bookkeeping: a timer whose due time has passed is fired as
``TimerFired`` (capability-wait timers are evaluated by every ``Tick`` instead).

Budget (Phase 4 Req 5.5, 5.7, 6.1, 10.2). Snapshots are loaded with every persisted
UsageRecord and the reducer gets the Price_Table through ``StepContext``, so budget-stop
mode is recomputed from persisted state (a new owner reaches the same decision). Before
the Planner runs, a Request with ``max_cost`` and no Price_Table fails with
``budget_unenforceable``. Every terminal write of a budgeted Request carries its
Budget_Used view for the Terminal_Event, and ``das_budget_exceeded_total`` counts a
budget stop once, when its terminal write answers OK.

This module talks to the network only through :class:`~das.transport.base.Transport`
and to Redis only through :class:`~das.state.store.StateStore` (import-linter
contract).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Protocol

from das.client.budget import ExceededKind, budget_status, budget_view
from das.clock import Clock, SystemClock
from das.codec import new_message
from das.manager.attempts import AttemptHook
from das.manager.final_result import build_final_result
from das.manager.lease import HeldLeases, LeaseKeeper
from das.manager.machine import (
    BUDGET_UNENFORCEABLE,
    NO_SUCCESS_ERROR,
    TIMER_CAPABILITY_WAIT,
    CancelObserved,
    Effect,
    Event,
    FinalizeFailed,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishDeadLetter,
    PublishDispatch,
    PublishFailed,
    ReplayRequested,
    RequestSnapshot,
    RunSummarizer,
    ScheduleTimer,
    SendCancel,
    StateChange,
    StepContext,
    Tick,
    TimerFired,
    UnackedDeliveryExhausted,
    apply_decision,
    resume,
    step,
)
from das.manager.planner import (
    PLAN_REJECTED_CODE,
    NoCapabilities,
    PlanAccepted,
    Planner,
    PlanLLMFailed,
    PlannerConfig,
    PlanRejected,
)
from das.manager.routing import choose_routing_capability
from das.manager.summarizer import SummarizationFailed, Summarizer
from das.models import (
    TERMINAL_JOB,
    AgentRecord,
    ControlPayload,
    FinalResult,
    JobDispatchPayload,
    JobPlan,
    JobState,
    Lease,
    RequestStatus,
)
from das.models.usage import UsageRecord
from das.observability import bind_job, bind_request, get_logger
from das.observability.metrics import ManagerMetrics, NoopRecorder
from das.observability.tracing import NoopTracer, SpanHandle, Tracer
from das.ops.cost import PriceTable
from das.registry import WORKER_ROLE, RegistrySnapshot
from das.state import (
    INBOX_JOB_RESULT,
    INBOX_REPLAY,
    INBOX_UNACKED_EXHAUSTED,
    InboxEntry,
    StateStore,
    StoredRequest,
    StoreStatus,
    TransitionBatch,
    WriteResult,
)
from das.transport.base import PRIORITY_HEADER, PRIORITY_HIGH, Transport

log = get_logger(__name__)

TICK_PERIOD: Final = 0.25
FINALIZE_EXTRA_ATTEMPTS: Final = 3  # Req 7.7: 3 additional attempts ...
FINALIZE_RETRY_DELAY: Final = 1.0  # ... at 1-second intervals
CANCEL_SEND_TIMEOUT: Final = 2.0
INBOX_BATCH: Final = 100

#: ``error.type`` of Spans ended by a fenced write rejection (Phase 2 Req 6.10).
LEASE_LOST_CODE: Final = "LEASE_LOST"
REQUEST_TERMINAL_CODE: Final = "REQUEST_TERMINAL"

DLQ_DESTINATION: Final = "dlq"
#: Headers on a Manager-published dead letter (Req 6.3).
DLQ_REQUEST_ID_HEADER: Final = "x-request-id"
DLQ_JOB_ID_HEADER: Final = "x-job-id"
DLQ_ATTEMPTS_HEADER: Final = "x-attempts"
DLQ_LAST_ERROR_HEADER: Final = "x-last-error"
DLQ_REASON_HEADER: Final = "x-death-reason"  # same key the transports set on dead letters
DLQ_REASON_MAX_ATTEMPTS: Final = "max_attempts"


class RegistrySource(Protocol):
    """What the driver needs from :class:`das.registry.AgentRegistry`."""

    async def snapshot(self) -> RegistrySnapshot: ...


TimeSource = Callable[[], Awaitable[float]]


class DriverOutcome(StrEnum):
    """How a driver run ended."""

    COMPLETED = "completed"  # Final_Result persisted, Request Completed
    FAILED = "failed"  # Request set Failed (planning, no success, summarization)
    CANCELLED = "cancelled"  # Request observed Cancelled
    LEASE_LOST = "lease_lost"  # stopped writing; output discarded
    STOPPED_RENEWING = "stopped_renewing"  # final write failed 4 times (Req 7.7)
    ABANDONED = "abandoned"  # Request missing, terminal elsewhere, or unexpected state
    RELEASED = "released"  # Lease released on request (drain, Req 17.5)


@dataclass(frozen=True, slots=True)
class DriverDeps:
    """Collaborators and settings shared by every driver of one Manager."""

    store: StateStore
    registry: RegistrySource
    transport: Transport  # Queue_Transport: dispatches and dead letters
    planner: Planner
    summarizer: Summarizer
    planner_config: PlannerConfig = dataclasses.field(default_factory=PlannerConfig)
    machine_config: MachineConfig = dataclasses.field(default_factory=MachineConfig)
    control_transport: Transport | None = None  # RPC_Transport: cancel_job (best effort)
    clock: Clock = dataclasses.field(default_factory=SystemClock)
    #: Seconds "now" for the reducer; defaults to Redis server time (store.server_time).
    time_source: TimeSource | None = None
    tick_period: float = TICK_PERIOD
    finalize_extra_attempts: int = FINALIZE_EXTRA_ATTEMPTS
    finalize_retry_delay: float = FINALIZE_RETRY_DELAY
    cancel_send_timeout: float = CANCEL_SEND_TIMEOUT
    #: Phase 2 Req 2.2, 2.4, 2.7: request/job metrics (no-op unless wired).
    metrics: ManagerMetrics = dataclasses.field(
        default_factory=lambda: ManagerMetrics(NoopRecorder())
    )
    #: Phase 2 Req 6.2-6.4, 6.8: manager.request/plan/dispatch/summarize Spans.
    tracer: Tracer = dataclasses.field(default_factory=NoopTracer)
    #: Phase 4 Req 5.5-5.7: prices ``max_cost`` budgets; ``None`` makes them unenforceable.
    price_table: PriceTable | None = None

    def __post_init__(self) -> None:
        if self.tick_period <= 0:
            raise ValueError("tick_period must be positive")
        if self.finalize_extra_attempts < 0:
            raise ValueError("finalize_extra_attempts must be >= 0")
        if self.finalize_retry_delay < 0:
            raise ValueError("finalize_retry_delay must be >= 0")


class _Stop(Exception):
    """Internal: end the driver with ``outcome``."""

    def __init__(self, outcome: DriverOutcome) -> None:
        super().__init__(outcome.value)
        self.outcome = outcome


def _has_writes(batch: TransitionBatch) -> bool:
    return bool(
        batch.jobs
        or batch.results
        or batch.timers_add
        or batch.timers_remove
        or batch.request_fields
        or batch.usage
        or batch.replays_applied
        or batch.dead_letter_reverts
        or batch.inbox_cursor is not None
    )


class RequestDriver:
    """Drives one owned Request from claim to a terminal status (see module docs)."""

    def __init__(self, lease: Lease, keeper: LeaseKeeper, deps: DriverDeps) -> None:
        self.lease = lease
        self.keeper = keeper
        self.deps = deps
        self.rid = lease.request_id
        self._store = deps.store
        self._clock = deps.clock
        self._now_source: TimeSource = deps.time_source or deps.store.server_time
        self._snap: RequestSnapshot | None = None
        self._tenant = "default"
        self._priority = "normal"
        self._cursor = "0-0"
        self._agents: tuple[AgentRecord, ...] = ()
        self._finish: RunSummarizer | FinalizeFailed | None = None
        self._wake = asyncio.Event()
        self._cancel_requested = False
        self._reload = False
        self._release_requested = asyncio.Event()
        # Summarizer usage of a finalize answered REPLAY_PENDING: still unpersisted,
        # it rides in the next final write (Req 9.3).
        self._carry_usage: list[UsageRecord] = []
        self._metrics = deps.metrics
        self._tracer = deps.tracer
        # manager.request Span (Req 6.2): opened in _run once the Request is loaded,
        # closed by run() whatever the outcome; ``_fail_code`` is its error.type.
        self._span_stack = contextlib.ExitStack()
        self._fail_code: str | None = None
        self._request_span: SpanHandle | None = None
        # Routing Capability each Job was last dispatched to (das_job_* labels).
        self._routing: dict[str, str] = {}
        self.outcome: DriverOutcome | None = None

    # ------------------------------------------------------------------ external signals

    def notify(self) -> None:
        """Wake the event loop early (e.g. a Job_Result was ingested for this Request)."""
        self._wake.set()

    def request_cancel(self) -> None:
        """A ``cancel_request`` control command arrived for this Request."""
        self._cancel_requested = True
        self._wake.set()

    def request_release(self) -> None:
        """Stop at the next event boundary and release the Lease (drain, Req 17.5)."""
        self._release_requested.set()
        self._wake.set()

    @property
    def release_requested(self) -> bool:
        return self._release_requested.is_set()

    # ------------------------------------------------------------------ entry point

    async def run(self) -> DriverOutcome:
        with bind_request(self.rid):
            try:
                try:
                    outcome = await self._run()
                except _Stop as stop:
                    outcome = stop.outcome
                except asyncio.CancelledError:
                    if not self.keeper.is_lost:
                        self._fail_code = "CancelledError"
                        raise  # shutdown: propagate
                    task = asyncio.current_task()
                    if task is not None:
                        task.uncancel()
                    log.warning("driver_lease_lost", detail="in-progress output discarded")
                    outcome = DriverOutcome.LEASE_LOST
                if outcome is DriverOutcome.LEASE_LOST:
                    self._fail_code = LEASE_LOST_CODE
            except BaseException as exc:
                self._fail_code = self._fail_code or type(exc).__name__
                raise
            finally:
                self._close_request_span()
            self.outcome = outcome
            log.info("driver_finished", outcome=outcome.value)
            return outcome

    def _open_request_span(self, stored: StoredRequest) -> None:
        """``manager.request`` (Req 6.2, 6.12): parented on the persisted Trace_Context,
        or a new root when the Request has none; nothing is written back."""
        span = self._span_stack.enter_context(
            self._tracer.span(
                "manager.request",
                parent=stored.trace_context,
                root=stored.trace_context is None,
                attributes={"das.request_id": self.rid, "das.tenant": stored.tenant},  # Phase 5 Req 8.3
            )
        )
        self._request_span = span

    def _close_request_span(self) -> None:
        span = self._request_span
        if span is not None and self._fail_code is not None:
            span.fail(self._fail_code)  # Req 6.10
        self._request_span = None
        # Closed without exception info: failures are reported via fail() above.
        self._span_stack.close()

    async def _run(self) -> DriverOutcome:
        stored = await self._retry(self._load)
        if stored is None:
            await self.keeper.release()
            return DriverOutcome.ABANDONED
        self._open_request_span(stored)
        self._adopt(stored)
        if stored.status is RequestStatus.PENDING:
            await self._plan(stored)
        elif stored.status is RequestStatus.RUNNING:
            await self._refresh_agents()
            assert self._snap is not None
            log.info("request_resumed")
            await self._start_events([], resume(self._snap, self._agents))
        else:
            await self.keeper.release()
            return DriverOutcome.ABANDONED
        while True:
            await self._loop()
            assert self._finish is not None
            outcome = await self._finalize(self._finish)
            if outcome is not None:
                return outcome
            # REPLAY_PENDING: the inbox holds a Replay; evaluate again (D8).

    # ------------------------------------------------------------------ helpers

    async def _load(self) -> StoredRequest | None:
        """The stored Request with every persisted UsageRecord, so the reducer computes
        Budget_Used from persisted state (Phase 4 Req 5.7, 5.8)."""
        return await self._store.load_snapshot(self.rid, include_usage=True)

    def _adopt(self, stored: StoredRequest) -> None:
        self._snap = RequestSnapshot.from_stored(stored)
        self._tenant = stored.tenant  # for the per-tenant finished counter (Phase 5 Req 8.1), not for the reducer
        self._priority = stored.priority  # re-read on every reload, so a Replay or takeover sees the stored value
        self._cursor = stored.inbox_cursor or "0-0"

    def _check_lease(self) -> None:
        if self.keeper.is_lost:
            raise _Stop(DriverOutcome.LEASE_LOST)

    async def _check_release(self) -> None:
        """Event boundary: honour :meth:`request_release` (Req 17.5)."""
        if self._release_requested.is_set():
            await self._release_now()

    async def _release_now(self) -> None:
        log.info("driver_releasing_lease")
        await self.keeper.release()
        raise _Stop(DriverOutcome.RELEASED)

    async def _until_release[T](self, call: Awaitable[T]) -> T:
        """Await an LLM call, abandoning it when a release is requested (Req 17.5)."""
        task = asyncio.ensure_future(call)
        waiter = asyncio.ensure_future(self._release_requested.wait())
        try:
            await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for fut in (waiter, task):
                if not fut.done():
                    fut.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await fut
        if self._release_requested.is_set():
            if not task.cancelled():
                task.exception()  # retrieved; the output is discarded
            await self._release_now()
        return task.result()

    async def _retry[T](self, fn: Callable[..., Awaitable[T]], *args: object) -> T:
        """Call ``fn`` until it succeeds (State_Store/registry briefly unreachable).

        Bounded by the Lease: if the store stays unreachable past the Lease expiry the
        keeper marks the Lease lost and cancels this task (Req 2.7).
        """
        while True:
            self._check_lease()
            await self._check_release()
            try:
                return await fn(*args)
            except Exception as exc:
                log.warning("driver_call_failed", call=getattr(fn, "__name__", "?"),
                            error=type(exc).__name__)
                await self._clock.sleep(self.deps.tick_period)

    async def _now(self) -> float:
        return float(await self._now_source())

    async def _refresh_agents(self) -> None:
        try:
            snapshot = await self.deps.registry.snapshot()
        except Exception as exc:  # keep the last known agents
            log.warning("registry_snapshot_failed", error=type(exc).__name__)
            return
        self._agents = tuple(snapshot.agents)

    async def _wait(self, timeout: float) -> None:
        if not self._wake.is_set() and timeout > 0:
            sleeper = asyncio.ensure_future(self._clock.sleep(timeout))
            waker = asyncio.ensure_future(self._wake.wait())
            try:
                await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for fut in (sleeper, waker):
                    if not fut.done():
                        fut.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await fut
        self._wake.clear()

    # ------------------------------------------------------------------ planning

    async def _plan(self, stored: StoredRequest) -> None:
        if stored.plan is not None:
            await self._reopen()
            return
        budget = stored.budget
        if budget is not None and budget.max_cost is not None and self.deps.price_table is None:
            # Phase 4 Req 5.5: a cost limit cannot be priced; fail before planning.
            log.warning("planning_budget_unenforceable")
            self._fail_code = BUDGET_UNENFORCEABLE
            await self._fail(BUDGET_UNENFORCEABLE, budget_kind="unenforceable")
        snapshot = await self._retry(self.deps.registry.snapshot)
        self._agents = tuple(snapshot.agents)
        capabilities = snapshot.advertised_capabilities()
        outcome = await self._until_release(
            self.deps.planner.plan(
                stored.prompt, capabilities, self.deps.planner_config,
                on_attempt=self._attempt_span("manager.plan"),
            )
        )
        self._discard_if_lost("planner")
        await self._check_release()
        if isinstance(outcome, PlanLLMFailed):
            log.warning("planning_llm_failed", provider=outcome.error.provider,
                        model=outcome.error.model)
            self._fail_code = type(outcome.error).__name__
            await self._fail(outcome.error_message, usage=outcome.usages)
        if isinstance(outcome, NoCapabilities):
            log.warning("planning_no_capabilities")
            self._fail_code = type(outcome).__name__
            await self._fail(outcome.error_message)
        if isinstance(outcome, PlanRejected):
            log.warning("planning_rejected", attempts=outcome.attempts,
                        checks=sorted(c.value for c in outcome.failed_checks))
            self._fail_code = PLAN_REJECTED_CODE  # Req 6.10
            await self._fail(outcome.error_message, usage=outcome.usages)
        assert isinstance(outcome, PlanAccepted)
        # LEASE_LOST / REQUEST_TERMINAL persist nothing: the plan and its usage are
        # dropped together (Req 9.4).
        result = await self._retry(
            self._store.commit_plan, self.lease, outcome.plan, outcome.usages
        )
        if result.status is StoreStatus.LEASE_LOST:
            self._fail_code = LEASE_LOST_CODE
            raise _Stop(DriverOutcome.LEASE_LOST)
        if result.status is StoreStatus.REQUEST_TERMINAL:
            self._fail_code = REQUEST_TERMINAL_CODE
            await self.keeper.release()  # cancelled while planning
            raise _Stop(DriverOutcome.CANCELLED)
        if result.ok:  # every Job written Waiting by commit_plan (Req 2.4)
            self._record_plan_committed(outcome.plan)
        reloaded = await self._retry(self._load)
        if reloaded is None or reloaded.status is not RequestStatus.RUNNING:
            log.error("commit_plan_unexpected", status=str(result.status))
            await self.keeper.release()
            raise _Stop(DriverOutcome.ABANDONED)
        self._adopt(reloaded)
        log.info("plan_committed", jobs=len(outcome.plan.jobs), attempts=outcome.attempts)
        assert self._snap is not None
        if result.ok:
            await self._start_events([(PlanCommitted(), None)])
        else:  # INVALID_STATE: already Running (committed by an earlier owner)
            await self._start_events([], resume(self._snap, self._agents))

    async def _reopen(self) -> None:
        """Pending with a Job_Plan: reopened by a Replay of a Failed Request (D7).

        ``reopen_request`` moves it to Running without planning; the Replay itself is
        waiting in the inbox and is applied by the event loop.
        """
        result = await self._retry(self._store.reopen_request, self.lease)
        if result.status is StoreStatus.LEASE_LOST:
            self._fail_code = LEASE_LOST_CODE
            raise _Stop(DriverOutcome.LEASE_LOST)
        reloaded = await self._retry(self._load)
        if reloaded is None:
            await self.keeper.release()
            raise _Stop(DriverOutcome.ABANDONED)
        if reloaded.status is not RequestStatus.RUNNING:
            log.warning("reopen_rejected", status=str(result.status),
                        request_status=reloaded.status.value)
            await self._on_terminal(reloaded.status)
        self._adopt(reloaded)
        await self._refresh_agents()
        log.info("request_reopened", status=str(result.status))
        assert self._snap is not None
        await self._start_events([], resume(self._snap, self._agents))

    # ------------------------------------------------------------------ observability

    def _attempt_span(self, name: str) -> AttemptHook:
        """``on_attempt`` hook opening ``name`` as a child of ``manager.request``."""
        tracer = self._tracer
        rid = self.rid

        def hook(attempt: int) -> contextlib.AbstractContextManager[SpanHandle]:
            return tracer.span(name, attributes={"das.request_id": rid, "das.attempt": attempt})

        return hook

    def _routing_cap(self, job_id: str) -> str | None:
        """Routing Capability of a Job: the one it was dispatched to, else the one the
        reducer would choose now (same rule as ``resume``)."""
        cap = self._routing.get(job_id)
        if cap is not None:
            return cap
        assert self._snap is not None
        record = self._snap.jobs.get(job_id)
        if record is None or not record.job.capabilities:
            return None
        caps = record.job.capabilities
        return choose_routing_capability(caps, self._agents) or min(caps)

    def _record_plan_committed(self, plan: JobPlan) -> None:
        """``commit_plan`` OK wrote one ``None -> Waiting`` entry per Job (Req 2.4)."""
        for job in plan.jobs:
            if not job.capabilities:
                continue
            cap = choose_routing_capability(job.capabilities, self._agents) or min(
                job.capabilities
            )
            self._metrics.job_transition(cap, JobState.WAITING)

    def _record_changes(self, changes: Sequence[StateChange]) -> None:
        """Called only after the write carrying ``changes`` answered OK (Req 2.4, 2.5, 2.7):
        the script appends exactly one transition entry per change."""
        for change in changes:
            cap = self._routing_cap(change.job_id)
            if cap is None:
                continue
            self._metrics.job_transition(cap, change.to_state)
            if change.to_state in TERMINAL_JOB:
                self._metrics.job_attempts(cap, change.attempt)

    def _discard_if_lost(self, what: str) -> None:
        if self.keeper.is_lost:
            log.warning("output_discarded_lease_lost", component=what)
            raise _Stop(DriverOutcome.LEASE_LOST)

    async def _fail(
        self,
        error: str,
        final: FinalResult | None = None,
        usage: Sequence[UsageRecord] = (),
        budget_kind: ExceededKind | None = None,
    ) -> None:
        """Fail the Request (and release the Lease) and stop the driver."""
        outcome = await self._write_final(final, RequestStatus.FAILED, error, usage,
                                          budget_kind=budget_kind)
        if outcome is None:  # REPLAY_PENDING without a plan: cannot happen
            log.error("finalize_unexpected_replay_pending")
            await self.keeper.release()
            outcome = DriverOutcome.ABANDONED
        raise _Stop(outcome)

    # ------------------------------------------------------------------ event loop

    async def _loop(self) -> None:
        while self._finish is None:
            self._check_lease()
            await self._check_release()
            try:
                if self._reload:
                    await self._resync()
                await self._check_status()
                await self._refresh_agents()
                await self._drain_inbox()
                if self._finish is None:
                    await self._fire_timers()
                if self._finish is None:
                    await self._run_events([(Tick(), None)])
            except _Stop:
                raise
            except Exception as exc:  # State_Store or Transport trouble; retry next tick
                log.warning("driver_iteration_failed", error=type(exc).__name__)
                self._reload = True
            if self._finish is None:
                await self._wait(await self._next_wait())

    async def _start_events(
        self,
        events: Iterable[tuple[Event | None, str | None]],
        effects: Sequence[Effect] = (),
    ) -> None:
        """``_run_events`` for the first decision; store trouble defers to the loop."""
        try:
            await self._run_events(events, effects)
        except _Stop:
            raise
        except Exception as exc:
            log.warning("driver_start_events_failed", error=type(exc).__name__)
            self._reload = True

    async def _resync(self) -> None:
        """After an unexpected error the local snapshot may be stale (a write may have
        landed without its reply, or a publish without its commit): reload it and
        re-drive in-flight Jobs like a new owner would (Req 2.6). Duplicate deliveries
        are absorbed by result idempotency."""
        stored = await self._load()
        if stored is None:
            await self.keeper.release()
            raise _Stop(DriverOutcome.ABANDONED)
        if stored.status is not RequestStatus.RUNNING:
            await self._on_terminal(stored.status)
        self._adopt(stored)
        self._reload = False
        assert self._snap is not None
        await self._run_events([], resume(self._snap, self._agents))

    async def _next_wait(self) -> float:
        timeout = self.deps.tick_period
        assert self._snap is not None
        dues = [t.due for t in self._snap.timers if t.kind != TIMER_CAPABILITY_WAIT]
        if dues:
            try:
                now = await self._now()
            except Exception:
                return timeout
            timeout = min(timeout, max(0.0, min(dues) - now))
        return timeout

    async def _check_status(self) -> None:
        # Checked every tick, so a Gateway cancel is seen within one tick even without
        # the cancel_request command (Req 1.6); the command only wakes us sooner.
        self._cancel_requested = False
        status = await self._store.get_status(self.rid)
        if status is None:
            await self.keeper.release()
            raise _Stop(DriverOutcome.ABANDONED)
        if status is not RequestStatus.RUNNING:
            await self._on_terminal(status)

    async def _on_terminal(self, status: RequestStatus | None = None) -> None:
        """The Request left Running behind our back (Gateway cancel, Req 1.6, 1.7)."""
        if status is None:
            status = await self._retry(self._store.get_status, self.rid)
        if status is RequestStatus.CANCELLED:
            log.info("request_cancel_observed")
            assert self._snap is not None
            decision = step(self._snap, CancelObserved(), await self._step_context())
            await self._send_cancels([e for e in decision.effects if isinstance(e, SendCancel)])
            await self.keeper.release()
            raise _Stop(DriverOutcome.CANCELLED)
        await self.keeper.release()
        raise _Stop(DriverOutcome.ABANDONED)

    async def _drain_inbox(self) -> None:
        while self._finish is None:
            entries = await self._store.read_inbox(self.rid, after=self._cursor, count=INBOX_BATCH)
            if not entries:
                return
            for entry in entries:
                await self._run_events([(self._inbox_event(entry), entry.entry_id)])
                if self._finish is not None:
                    return

    def _inbox_event(self, entry: InboxEntry) -> Event | None:
        try:
            if entry.kind == INBOX_JOB_RESULT:
                return JobResultReceived(entry.job_result(), _llm_meta(entry))
            if entry.kind == INBOX_REPLAY:
                data = entry.payload()
                return ReplayRequested(
                    dead_letter_id=str(data["dead_letter_id"]),
                    job_id=str(data["job_id"]),
                    attempt=int(data["attempt"]),
                )
            if entry.kind == INBOX_UNACKED_EXHAUSTED:
                data = entry.payload()
                job_id = str(data["job_id"])
                attempt = data.get("attempt")
                assert self._snap is not None
                record = self._snap.jobs.get(job_id)
                if attempt is not None and record is not None and int(attempt) != record.attempt:
                    return None  # dead letter of an older attempt
                return UnackedDeliveryExhausted(job_id)
        except Exception as exc:
            log.warning("inbox_entry_invalid", entry_id=entry.entry_id, error=type(exc).__name__)
            return None
        log.warning("inbox_entry_unknown_kind", entry_id=entry.entry_id, kind=entry.kind)
        return None

    async def _fire_timers(self) -> None:
        assert self._snap is not None
        now = await self._now()
        due = sorted(
            (t for t in self._snap.timers if t.kind != TIMER_CAPABILITY_WAIT and t.due <= now),
            key=lambda t: (t.due, t.key.member),
        )
        for timer in due:
            await self._run_events([(TimerFired(timer.kind, timer.job_id, timer.attempt), None)])
            if self._finish is not None:
                return

    async def _step_context(self) -> StepContext:
        return StepContext(now=await self._now(), config=self.deps.machine_config,
                           agents=self._agents, price_table=self.deps.price_table)

    async def _run_events(
        self,
        events: Iterable[tuple[Event | None, str | None]],
        effects: Sequence[Effect] = (),
    ) -> None:
        """Step, commit, and execute effects for ``events`` and their follow-ups."""
        queue: deque[tuple[Event | None, str | None]] = deque(events)
        queue.extend((e, None) for e in await self._execute(effects))
        while queue:
            await self._check_release()
            event, cursor = queue.popleft()
            assert self._snap is not None
            if event is None:
                decision = None
                batch = TransitionBatch(inbox_cursor=cursor)
            else:
                decision = step(self._snap, event, await self._step_context())
                batch = decision.updates
                if cursor is not None:
                    batch = dataclasses.replace(batch, inbox_cursor=cursor)
            committed = False
            if _has_writes(batch):
                self._check_lease()
                result = await self._store.apply_transition(self.lease, batch)
                await self._handle_write(result)  # raises unless result.ok
                committed = result.ok
            if cursor is not None:
                self._cursor = cursor
            if decision is None:
                continue
            if committed:
                self._record_changes(decision.changes)
            self._snap = apply_decision(self._snap, decision)
            for warning in decision.warnings:
                log.warning("request_machine_warning", detail=warning)
            for change in decision.changes:
                with bind_job(self.rid, change.job_id):
                    log.info(
                        "job_state_changed",
                        from_state=change.from_state.value if change.from_state else None,
                        to_state=change.to_state.value,
                        attempt=change.attempt,
                        worker_id=change.worker_id,
                    )
            queue.extend((e, None) for e in await self._execute(decision.effects))

    async def _handle_write(self, result: WriteResult) -> None:
        if result.ok:
            return
        if result.status is StoreStatus.LEASE_LOST:
            log.warning("write_rejected_lease_lost")
            self._fail_code = LEASE_LOST_CODE
            raise _Stop(DriverOutcome.LEASE_LOST)
        if result.status is StoreStatus.REQUEST_TERMINAL:
            self._fail_code = REQUEST_TERMINAL_CODE
            await self._on_terminal()
        log.error("write_rejected", status=str(result.status), detail=result.detail)
        await self.keeper.release()
        raise _Stop(DriverOutcome.ABANDONED)

    # ------------------------------------------------------------------ effects

    async def _execute(self, effects: Sequence[Effect]) -> list[Event]:
        follow: list[Event] = []
        cancels: list[SendCancel] = []
        if effects:
            await self._check_release()  # no new dispatch once a release is requested
        for effect in effects:
            match effect:
                case PublishDispatch():
                    follow.append(await self._publish_dispatch(effect))
                case PublishDeadLetter():
                    await self._publish_dead_letter(effect)
                case SendCancel():
                    cancels.append(effect)
                case ScheduleTimer():
                    pass  # persisted timers drive wakeups (see _next_wait/_fire_timers)
                case RunSummarizer() | FinalizeFailed():
                    if self._finish is None:
                        self._finish = effect
        if cancels:
            await self._send_cancels(cancels)
        return follow

    async def _publish_dispatch(self, effect: PublishDispatch) -> Event:
        self._check_lease()  # Req 2.7: no dispatch after the Lease is lost
        self._routing[effect.job_id] = effect.routing_cap
        attrs: dict[str, str | int] = {
            "das.request_id": self.rid,
            "das.job_id": effect.job_id,
            "das.attempt": effect.attempt,
            "das.capability": effect.routing_cap,
        }
        with (
            bind_job(self.rid, effect.job_id),
            self._tracer.span("manager.dispatch", attributes=attrs) as span,
        ):
            # The envelope carries this Span's context, sampled or not (Req 6.4).
            message = new_message(
                effect.to_payload(self.rid),
                correlation_id=self.rid,
                trace_context=self._tracer.current_context(),
            )
            # The one place a Job is dispatched, so first dispatches, retries, takeovers and Replays all carry the
            # Priority_Header of a ``high`` Request and nothing else does (Phase 5 Req 7.4).
            headers = {PRIORITY_HEADER: PRIORITY_HIGH} if self._priority == PRIORITY_HIGH else None
            try:
                await self.deps.transport.publish(effect.destination, message, headers)
            except Exception as exc:  # TransportUnavailable / PublishNotConfirmed / codec
                log.warning("dispatch_publish_failed", attempt=effect.attempt,
                            destination=effect.destination, error=type(exc).__name__)
                span.fail(type(exc).__name__)
                return PublishFailed(effect.job_id, effect.attempt, error=str(exc))
            log.info("job_dispatched", attempt=effect.attempt, destination=effect.destination)
            return PublishConfirmed(effect.job_id, effect.attempt)

    async def _publish_dead_letter(self, effect: PublishDeadLetter) -> None:
        """Route the exhausted Job's dispatch to the Dead_Letter_Queue (Req 6.3)."""
        assert self._snap is not None
        record = self._snap.jobs.get(effect.job_id)
        if record is None:
            return
        deps: dict[str, str] = {}
        for dep in record.job.depends_on:
            dep_rec = self._snap.jobs.get(dep)
            result = self._snap.results.get((dep, dep_rec.attempt)) if dep_rec else None
            if result is not None and result.status == "Succeeded":
                deps[dep] = result.content
        payload = JobDispatchPayload(
            request_id=self.rid,
            job_id=effect.job_id,
            attempt=max(1, effect.attempts),
            instruction=record.job.instruction,
            required_capabilities=list(record.job.capabilities),
            dependency_results=deps,
            output_schema=record.job.output_schema,  # a dead letter is a faithful copy of the dispatch
        )
        headers = {
            DLQ_REQUEST_ID_HEADER: self.rid,
            DLQ_JOB_ID_HEADER: effect.job_id,
            DLQ_ATTEMPTS_HEADER: str(effect.attempts),
            DLQ_LAST_ERROR_HEADER: effect.last_error,
            DLQ_REASON_HEADER: DLQ_REASON_MAX_ATTEMPTS,
        }
        with bind_job(self.rid, effect.job_id):
            try:
                await self.deps.transport.publish(
                    DLQ_DESTINATION, new_message(payload, correlation_id=self.rid), headers
                )
                log.info("job_dead_lettered", attempts=effect.attempts)
            except Exception as exc:  # best effort: the Job is already Failed in the store
                log.warning("dead_letter_publish_failed", error=type(exc).__name__)

    def _cancel_targets(self, job_id: str) -> list[str]:
        """The Worker running the Job if known, else every Available capable Worker."""
        assert self._snap is not None
        record = self._snap.jobs.get(job_id)
        if record is None:
            return []
        if record.worker_id:
            return [record.worker_id]
        required = set(record.job.capabilities)
        return sorted(
            a.agent_id
            for a in self._agents
            if a.role == WORKER_ROLE and a.status == "Available" and required <= a.capabilities
        )

    async def _send_cancels(self, cancels: Sequence[SendCancel]) -> None:
        """Best-effort ``cancel_job`` commands; errors are logged and swallowed."""
        control = self.deps.control_transport
        if control is None or not cancels:
            return

        async def send(job_id: str, target: str) -> None:
            with bind_job(self.rid, job_id):
                message = new_message(
                    ControlPayload(command="cancel_job", request_id=self.rid, job_id=job_id),
                    correlation_id=self.rid,
                    trace_context=self._tracer.current_context(),  # Req 6.8
                )
                try:
                    async with asyncio.timeout(self.deps.cancel_send_timeout):
                        await control.publish(target, message)
                    log.info("cancel_job_sent", worker_id=target)
                except Exception as exc:
                    log.warning("cancel_job_failed", worker_id=target, error=type(exc).__name__)

        await asyncio.gather(
            *(send(c.job_id, target) for c in cancels for target in self._cancel_targets(c.job_id))
        )

    # ------------------------------------------------------------------ finish

    async def _finalize(self, finish: RunSummarizer | FinalizeFailed) -> DriverOutcome | None:
        """Summarize (or not) and write the final status; ``None`` on REPLAY_PENDING."""
        snap = self._snap
        assert snap is not None and snap.plan is not None
        if isinstance(finish, FinalizeFailed):
            final = finish.final_result
            return await self._write_final(final, RequestStatus.FAILED,
                                           final.error or NO_SUCCESS_ERROR,
                                           budget_kind=finish.budget_kind)
        summarizer = self.deps.summarizer
        try:
            summary = await self._until_release(
                summarizer.summarize(
                    finish.prompt, finish.succeeded,
                    on_attempt=self._attempt_span("manager.summarize"),
                )
            )
        except SummarizationFailed as exc:
            self._discard_if_lost("summarizer")
            log.warning("summarization_failed", attempts=exc.attempts)
            self._fail_code = type(exc).__name__
            final = build_final_result(snap.plan, snap.jobs, snap.results, None, exc.error)
            return await self._write_final(final, RequestStatus.FAILED, exc.error, exc.usages)
        self._discard_if_lost("summarizer")
        final = build_final_result(snap.plan, snap.jobs, snap.results, summary, None)
        return await self._write_final(
            final, RequestStatus.COMPLETED, None, (summarizer.usage_of(summary),)
        )

    async def _write_final(
        self,
        final: FinalResult | None,
        status: RequestStatus,
        error: str | None,
        usage: Sequence[UsageRecord] = (),
        budget_kind: ExceededKind | None = None,
    ) -> DriverOutcome | None:
        """``finalize`` with 3 extra attempts at 1 s; stop renewing if all fail (7.4, 7.7).

        ``usage`` (Planner/Summarizer UsageRecords) is written in the same script and
        dropped when the write answers LEASE_LOST / REQUEST_TERMINAL (Req 9.4).
        A budgeted Request passes its Budget_Used view (persisted plus ``usage``) for the
        Terminal_Event (Phase 4 Req 6.1). ``budget_kind`` marks a budget stop; it is
        counted in ``das_budget_exceeded_total`` only when the write answers OK, so a
        REPLAY_PENDING or retried write never counts twice (Phase 4 Req 10.2).
        Returns ``None`` when ``finalize`` answered REPLAY_PENDING (D8): the pending
        finish is dropped and the inbox drained again; the caller re-runs the loop.
        """
        records = (*self._carry_usage, *usage)
        view = self._budget_view(records)
        attempts = 1 + self.deps.finalize_extra_attempts
        last_error: str | None = None
        for attempt in range(1, attempts + 1):
            self._check_lease()
            await self._check_release()
            try:
                result = await self._store.finalize(self.lease, final, status, error,
                                                    records, budget_view=view)
            except Exception as exc:
                log.warning("finalize_failed", attempt=attempt, error=type(exc).__name__)
                last_error = type(exc).__name__
                if attempt < attempts:
                    await self._clock.sleep(self.deps.finalize_retry_delay)
                continue
            if result.ok:
                self._carry_usage = []
                self._record_finished(status, result)
                if budget_kind is not None:
                    self._metrics.budget_exceeded(budget_kind)
                log.info("request_finalized", status=status.value)
                await self.keeper.release()  # finalize.lua already released; stop renewing
                return (
                    DriverOutcome.COMPLETED
                    if status is RequestStatus.COMPLETED
                    else DriverOutcome.FAILED
                )
            if result.status is StoreStatus.REPLAY_PENDING:
                log.info("finalize_replay_pending", status=status.value)
                self._carry_usage = list(records)  # still unpersisted (Req 9.3)
                self._finish = None
                try:
                    await self._drain_inbox()
                except _Stop:
                    raise
                except Exception as exc:  # the event loop retries after a resync
                    log.warning("driver_iteration_failed", error=type(exc).__name__)
                    self._reload = True
                return None
            if result.status is StoreStatus.LEASE_LOST:
                log.warning("finalize_rejected_lease_lost")
                self._fail_code = LEASE_LOST_CODE
                return DriverOutcome.LEASE_LOST
            if result.status is StoreStatus.REQUEST_TERMINAL:
                self._fail_code = REQUEST_TERMINAL_CODE
                current = await self._retry(self._store.get_status, self.rid)
                await self.keeper.release()
                return (
                    DriverOutcome.CANCELLED
                    if current is RequestStatus.CANCELLED
                    else DriverOutcome.ABANDONED
                )
            log.error("finalize_rejected", status=str(result.status), detail=result.detail)
            await self.keeper.release()
            return DriverOutcome.ABANDONED
        # Req 7.7: leave the Request unchanged and let the Lease lapse.
        log.error("finalize_gave_up", attempts=attempts)
        self._fail_code = last_error or self._fail_code
        self.keeper.stop_renewing()
        return DriverOutcome.STOPPED_RENEWING

    def _budget_view(self, records: Sequence[UsageRecord]) -> dict[str, Any] | None:
        """Budget_Used of a budgeted Request after ``records`` are written (Phase 4
        Req 5.9, 6.1); ``None`` without a budget, so the Terminal_Event has no view."""
        snap = self._snap
        if snap is None or snap.budget is None:
            return None
        status = budget_status((*snap.usage, *records), snap.budget, self.deps.price_table)
        return budget_view(snap.budget, status)

    def _record_finished(self, status: RequestStatus, result: WriteResult) -> None:
        """``finalize`` answered OK (Req 2.2): one count plus one duration observation,
        ``finished_at - created_at`` as returned by the script, clamped at 0."""
        finished, created = result.finished_at_ms, result.created_at_ms
        duration = 0.0
        if finished is not None and created is not None:
            duration = max(0, finished - created) / 1000.0
        self._metrics.request_finished(status, duration, self._tenant)


def _llm_meta(entry: InboxEntry) -> dict[str, str] | None:
    """Provider/model of a Job_Result from its inbox ``meta`` (Req 9.2, D2)."""
    meta = entry.meta_payload()
    if meta is None:
        return None
    return {k: str(v) for k, v in meta.items() if k in ("provider", "model") and v}


class RequestDrivers:
    """Starts and tracks the drivers of every Request this Manager owns.

    Pass :meth:`on_claimed` to :class:`~das.manager.claim.ClaimLoop`; it returns the
    driver task, so the Request counts as owned until the task finishes.
    """

    def __init__(
        self, deps: DriverDeps, lease_duration: float, held: HeldLeases | None = None
    ) -> None:
        """``held`` tracks the Leases behind ``das_leases_held`` (Phase 2 Req 3.5)."""
        if lease_duration <= 0:
            raise ValueError("lease_duration must be positive")
        self.deps = deps
        self.lease_duration = float(lease_duration)
        self.held = held if held is not None else HeldLeases()
        self._drivers: dict[str, RequestDriver] = {}
        self._tasks: dict[str, asyncio.Task[DriverOutcome]] = {}

    def on_claimed(self, lease: Lease) -> asyncio.Task[DriverOutcome]:
        rid = lease.request_id
        keeper = LeaseKeeper(
            self.deps.store,
            lease,
            self.lease_duration,
            clock=self.deps.clock,
            metrics=self.deps.metrics,
            held=self.held,
        )
        driver = RequestDriver(lease, keeper, self.deps)
        task = asyncio.create_task(self._supervise(driver), name=f"request-driver:{rid}")
        keeper.attach(task)
        keeper.start()
        self._drivers[rid] = driver
        self._tasks[rid] = task

        def _done(_t: asyncio.Task[DriverOutcome], rid: str = rid) -> None:
            self._forget(rid, task)

        task.add_done_callback(_done)
        return task

    async def _supervise(self, driver: RequestDriver) -> DriverOutcome:
        try:
            outcome = await driver.run()
            if outcome is DriverOutcome.LEASE_LOST and not driver.keeper.is_lost:
                # A guarded write answered LEASE_LOST before a renewal noticed (Req 3.4).
                driver.keeper.mark_rejected()
            return outcome
        except asyncio.CancelledError:
            await asyncio.shield(driver.keeper.release())
            raise
        except Exception:
            with bind_request(driver.rid):
                log.exception("driver_crashed")
            await driver.keeper.release()
            return DriverOutcome.ABANDONED
        finally:
            if driver.keeper.renewing:  # e.g. a write answered LEASE_LOST first
                driver.keeper.stop_renewing()
            await driver.keeper.wait_closed()

    def _forget(self, rid: str, task: asyncio.Task[DriverOutcome]) -> None:
        if self._tasks.get(rid) is task:
            self._tasks.pop(rid, None)
            self._drivers.pop(rid, None)

    def get(self, rid: str) -> RequestDriver | None:
        return self._drivers.get(rid)

    @property
    def owned(self) -> frozenset[str]:
        return frozenset(self._drivers)

    def notify(self, rid: str) -> None:
        """Wake the driver of ``rid`` if this Manager owns it (ingest callback)."""
        driver = self._drivers.get(rid)
        if driver is not None:
            driver.notify()

    def request_cancel(self, rid: str) -> bool:
        """Forward a ``cancel_request`` command; False when ``rid`` is not owned here."""
        driver = self._drivers.get(rid)
        if driver is None:
            return False
        driver.request_cancel()
        return True

    def request_release(self, rid: str) -> bool:
        """Ask the driver of ``rid`` to release its Lease (Req 17.5); False if not owned."""
        driver = self._drivers.get(rid)
        if driver is None:
            return False
        driver.request_release()
        return True

    def request_release_all(self) -> int:
        """Drain with ``release_leases``: every owned driver releases (Req 17.5)."""
        drivers = list(self._drivers.values())
        for driver in drivers:
            driver.request_release()
        return len(drivers)

    async def stop_all(self) -> None:
        """Shutdown: cancel every driver and release its Lease (best effort)."""
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


__all__ = [
    "CANCEL_SEND_TIMEOUT",
    "DLQ_ATTEMPTS_HEADER",
    "DLQ_DESTINATION",
    "DLQ_JOB_ID_HEADER",
    "DLQ_LAST_ERROR_HEADER",
    "DLQ_REASON_HEADER",
    "DLQ_REASON_MAX_ATTEMPTS",
    "DLQ_REQUEST_ID_HEADER",
    "FINALIZE_EXTRA_ATTEMPTS",
    "FINALIZE_RETRY_DELAY",
    "LEASE_LOST_CODE",
    "REQUEST_TERMINAL_CODE",
    "TICK_PERIOD",
    "DriverDeps",
    "DriverOutcome",
    "RegistrySource",
    "RequestDriver",
    "RequestDrivers",
    "TimeSource",
]
