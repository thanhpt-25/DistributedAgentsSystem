"""Worker_Agent process: registration, consumption, results, cancel, shutdown (Req 5, 11).

The Worker takes every dependency by injection so the CLI (task 17.2) can
build it from Agent_Config and tests can drive it with in-memory fakes:

- ``queue``: Queue_Transport carrying Job dispatch (``jobs/<capability>``) and
  Job_Result (``results``) Messages.
- ``control``: RPC_Transport receiving control commands on the destination
  equal to the Worker's own agent ID (``cancel_job``).
- ``registry``: Agent_Registry (``register`` / ``heartbeat`` / ``deregister``).
- ``executor``: runs one Job attempt (``JobExecutor``).

Transports are expected to be connected already: the CLI connects them with
the startup dependency retry (Req 14.7). The Worker closes them on shutdown.

Startup order (design "Worker_Agent"):

1. Register in the Agent_Registry, retrying until ``registration_timeout``
   (30 s) elapses; otherwise raise :class:`~das.errors.RegistrationFailed`
   (exit code 5) without subscribing to anything (Req 5.1, 5.10, 11.6).
2. Subscribe one pull consumer per Capability, all sharing one
   :class:`InFlightLimiter` sized ``concurrency_limit`` (Req 5.2, 5.3).
3. Subscribe the control destination and start the heartbeat (Req 11.1).

Per delivery:

1. Malformed bytes, a non-dispatch Message type, or a payload missing a Job_ID
   or instruction (the codec rejects those) are settled with ``REJECT`` so the
   broker dead-letters them; nothing else happens (Req 5.9, 9.5).
2. Worker-side capability guard (Req 4.5): a Job whose required Capabilities
   are not all declared by this Worker is returned with ``REQUEUE`` before any
   work, and that Capability's queue is paused for ``guard_pause`` (1 s).
   The Transport protocol has no pause operation, so pausing is implemented by
   cancelling that Capability's subscription *before* requeueing (so the same
   Message isn't handed straight back to us) and re-subscribing after the
   pause. Other Capability queues keep flowing.
3. Run the executor, publish the Job_Result to ``results`` (confirmed), and
   only then ``ACK`` the dispatch (Req 5.5). If the publish fails, the dispatch
   is not ACKed: it is settled with ``REQUEUE`` so it is redelivered with the
   same attempt number (Req 6.4) instead of holding a limiter slot until the
   connection drops. If the connection is already down, the broker returns it.

The inbound correlation ID is bound as the Request_ID for logs and copied to
the outbound Job_Result (Req 13.5); a missing one is logged as a warning and
the payload's ``request_id`` is used instead (Req 13.6).

Graceful shutdown (Req 11.3), all within ``shutdown_timeout``: stop pulling
(cancel every Capability subscription), wait for in-flight Jobs for
``DRAIN_FRACTION`` of the budget, cancel the unfinished ones and ``REQUEUE``
their dispatches, stop the heartbeat, deregister, close the transports.

Drain (Req 17.2, 17.3, 17.8, 17.9, 17.15): an ``agent_control`` request-reply
Message on the control destination calls :meth:`Worker.drain` or
:meth:`Worker.undrain` and the handler returns an ``agent_control_reply``.
Drain cancels every Capability subscription (and guard pause), so no new
dispatch is pulled; a dispatch already handed to the Worker but not yet in the
executor is settled with ``REQUEUE`` and no Job_Result. Running Jobs finish
normally and the heartbeat reports ``Draining`` with the in-flight count.
Undrain re-subscribes. ``release_leases`` is ignored. The consume Critical_Loop
stays alive while draining (it isn't an ended loop for the Liveness_Probe).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Final, Protocol

from das.clock import Clock, SystemClock
from das.codec.envelope import (
    AgentControlMessage,
    AgentControlPayload,
    AgentControlReplyPayload,
    ControlMessage,
    JobDispatchMessage,
    TraceContextModel,
    TypedMessage,
    new_message,
)
from das.errors import RegistrationFailed
from das.health.supervisor import LoopSupervisor, wait_subscription_closed
from das.models.results import JobDispatchPayload, JobResultPayload, TokenUsage
from das.observability import bind_correlation_id, bind_job, get_logger
from das.observability.metrics import NoopRecorder, WorkerMetrics
from das.observability.tracing import NoopTracer, SpanHandle, Tracer
from das.registry.availability import AVAILABLE, DRAINING
from das.registry.registry import AgentInfo, SettableStatus
from das.transport.base import Delivery, Outcome, SubscribeOptions, Subscription, Transport
from das.worker.limiter import InFlightLimiter

log = get_logger(__name__)

JOBS_PREFIX: Final = "jobs/"
RESULTS_DESTINATION: Final = "results"
#: Transport header carrying the sending agent ID (same key as das.manager.ingest).
SENDER_HEADER: Final = "x-sender"
DEFAULT_REGISTRATION_TIMEOUT: Final = 30.0
DEFAULT_GUARD_PAUSE: Final = 1.0
#: Share of ``shutdown_timeout`` spent waiting for in-flight Jobs; the rest is
#: kept for requeueing, deregistering, and closing transports.
DRAIN_FRACTION: Final = 0.8
REGISTRATION_RETRY_INTERVAL: Final = 1.0
MAX_CAPABILITIES: Final = 50
#: Attempts whose Job_Result publish failed and was already observed in
#: ``das_job_execution_seconds``; their redelivery is not observed again (Req 2.8).
MAX_UNPUBLISHED_TRACKED: Final = 1024
#: Critical_Loop names registered with the LoopSupervisor (Req 16.2).
CONSUME_LOOP: Final = "consume"
HEARTBEAT: Final = "heartbeat"
CRITICAL_LOOPS: Final = (CONSUME_LOOP, HEARTBEAT)
#: Bound on the Agent_Registry status write made while handling drain/undrain
#: (Req 17.2, 17.8: the status changes within 1 s).
STATUS_WRITE_TIMEOUT: Final = 1.0


def jobs_destination(capability: str) -> str:
    return f"{JOBS_PREFIX}{capability}"


class RegistryLike(Protocol):
    async def register(self, info: AgentInfo) -> float: ...
    async def heartbeat(
        self, agent_id: str, desired_status: SettableStatus = ..., in_flight: int = ...
    ) -> bool: ...
    async def set_status(
        self, agent_id: str, status: SettableStatus, in_flight: int | None = ...
    ) -> bool: ...
    async def deregister(self, agent_id: str) -> bool: ...


class ExecutorLike(Protocol):
    async def execute(
        self, job: JobDispatchPayload, cancel: asyncio.Event, routing_capability: str | None = None
    ) -> JobResultPayload: ...

    # Optional: ``result_headers() -> Mapping[str, str]`` (``x-llm-provider`` /
    # ``x-llm-model``, design D2). Executors without it publish no such headers.
    # Optional: ``headers_for(result) -> Mapping[str, str]`` (Phase 6 Req 2.5), the same headers for one
    # Job_Result, naming the model that answered it; preferred over ``result_headers`` when present.


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    """Worker values taken from Agent_Config (ranges are enforced by the config schema)."""

    agent_id: str
    host: str
    capabilities: frozenset[str]
    concurrency_limit: int
    heartbeat_interval: float = 5.0
    shutdown_timeout: float = 30.0
    registration_timeout: float = DEFAULT_REGISTRATION_TIMEOUT
    grpc_endpoint: str | None = None
    guard_pause: float = DEFAULT_GUARD_PAUSE

    def __post_init__(self) -> None:
        if not self.agent_id:
            raise ValueError("agent_id must be non-empty")
        caps = frozenset(self.capabilities)
        object.__setattr__(self, "capabilities", caps)
        if not 1 <= len(caps) <= MAX_CAPABILITIES:
            raise ValueError(f"a worker declares 1 to {MAX_CAPABILITIES} capabilities")
        if any(not isinstance(c, str) or not c for c in caps):
            raise ValueError("capabilities must be non-empty strings")
        for name in ("heartbeat_interval", "shutdown_timeout", "registration_timeout"):
            if not getattr(self, name) > 0:
                raise ValueError(f"{name} must be positive")
        if self.guard_pause < 0:
            raise ValueError("guard_pause must be >= 0")


@dataclass(eq=False, slots=True)
class _InFlight:
    delivery: Delivery
    request_id: str  # payload request_id, matched by cancel_job
    job_id: str
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[JobResultPayload] | None = None
    started: float = 0.0  # clock.monotonic() when execution began (Req 2.8)


class Worker:
    """One Worker_Agent process (see module docstring)."""

    def __init__(
        self,
        settings: WorkerSettings,
        *,
        queue: Transport,
        control: Transport,
        registry: RegistryLike,
        executor: ExecutorLike,
        clock: Clock | None = None,
        limiter: InFlightLimiter | None = None,
        metrics: WorkerMetrics | None = None,
        tracer: Tracer | None = None,
        supervisor: LoopSupervisor | None = None,
    ) -> None:
        self.settings = settings
        #: Receives the consume loop and heartbeat for the Liveness_Probe (Req 16.2).
        self.supervisor = supervisor if supervisor is not None else LoopSupervisor()
        self._queue = queue
        self._control = control
        self._registry = registry
        self._executor = executor
        self._clock: Clock = clock or SystemClock()
        self.limiter = limiter or InFlightLimiter(settings.concurrency_limit)
        self._metrics = metrics or WorkerMetrics(NoopRecorder())
        self._tracer: Tracer = tracer or NoopTracer()
        self._unpublished: OrderedDict[tuple[str, str, int], None] = OrderedDict()
        # Req 2.9: in-flight computed at scrape time, limit as configured.
        self._metrics.jobs_in_flight(lambda: float(self.in_flight))
        self._metrics.concurrency_limit(settings.concurrency_limit)
        self._subs: dict[str, Subscription] = {}
        self._control_sub: Subscription | None = None
        self._paused: dict[str, asyncio.Task[None]] = {}
        self._inflight: set[_InFlight] = set()
        self._idle = asyncio.Event()
        self._idle.set()
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._consume_task: asyncio.Task[None] | None = None
        self._stop_requested = asyncio.Event()
        self._stopping = False
        self._stopped = asyncio.Event()
        self._shutdown_started = False
        # Drain (Req 17): ``_accepting`` is set while not draining; the consume
        # watcher waits on it so draining isn't an ended Critical_Loop.
        self._draining = False
        self._accepting = asyncio.Event()
        self._accepting.set()
        self._drain_lock = asyncio.Lock()
        self.registered = False
        #: True once every Critical_Loop has been started (Readiness ``startup``, Req 16.4).
        self.started = False

    # ----------------------------------------------------------- inspection

    @property
    def agent_info(self) -> AgentInfo:
        s = self.settings
        return AgentInfo(
            agent_id=s.agent_id,
            role="worker",
            host=s.host,
            grpc_endpoint=s.grpc_endpoint,
            capabilities=s.capabilities,
            concurrency_limit=s.concurrency_limit,
            heartbeat_interval=s.heartbeat_interval,
        )

    @property
    def subscriptions(self) -> Mapping[str, Subscription]:
        """Active Capability subscriptions keyed by Capability."""
        return dict(self._subs)

    @property
    def in_flight(self) -> int:
        """Jobs currently executing or publishing their result."""
        return len(self._inflight)

    @property
    def stopping(self) -> bool:
        return self._stopping

    @property
    def draining(self) -> bool:
        """True while drained: no new Jobs are pulled (Readiness ``not_draining``)."""
        return self._draining

    @property
    def status(self) -> SettableStatus:
        """This Worker's own Agent_Registry status (Available or Draining)."""
        return DRAINING if self._draining else AVAILABLE

    # ----------------------------------------------------------- drain

    async def drain(self) -> AgentControlReplyPayload:
        """Stop pulling Jobs and set Draining (Req 17.2); idempotent (Req 17.9)."""
        async with self._drain_lock:
            if not self._draining:
                self._draining = True
                self._accepting.clear()
                # Pause tasks pop themselves from ``_paused`` as they finish.
                paused = list(self._paused.values())
                for task in paused:
                    task.cancel()
                subs, self._subs = list(self._subs.values()), {}
                await asyncio.gather(
                    *(_quietly(sub.cancel(), "cancel subscription for drain") for sub in subs)
                )
                await _gather_quietly(paused)
                log.info("worker draining", in_flight=self.in_flight)
                await self._write_status(DRAINING)
            return self._reply()

    async def undrain(self) -> AgentControlReplyPayload:
        """Resume pulling Jobs and set Available (Req 17.8); idempotent (Req 17.9)."""
        async with self._drain_lock:
            if self._draining and not self._stopping:
                self._draining = False
                for capability in sorted(self.settings.capabilities):
                    if capability in self._subs or capability in self._paused:
                        continue
                    try:
                        self._subs[capability] = await self._subscribe_capability(capability)
                    except Exception as exc:  # noqa: BLE001 - retried after a pause
                        log.warning(
                            "re-subscribe after undrain failed",
                            capability=capability,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                        self._paused[capability] = asyncio.create_task(
                            self._resume_after_pause(capability)
                        )
                self._accepting.set()
                log.info("worker undrained")
                await self._write_status(AVAILABLE)
            return self._reply()

    def _reply(self) -> AgentControlReplyPayload:
        return AgentControlReplyPayload(
            agent_id=self.settings.agent_id, status=self.status, in_flight=self.in_flight
        )

    async def _write_status(self, status: SettableStatus) -> None:
        """Make the drain/undrain visible before the next heartbeat; the heartbeat
        carries the same status, so a failed write is repaired within one interval."""
        try:
            await asyncio.wait_for(
                self._registry.set_status(self.settings.agent_id, status, self.in_flight),
                STATUS_WRITE_TIMEOUT,
            )
        except Exception as exc:  # noqa: BLE001 - heartbeat retries the status
            log.warning(
                "registry status write failed",
                status=status,
                error=f"{type(exc).__name__}: {exc}",
            )

    # ----------------------------------------------------------- lifecycle

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Start, wait for ``stop_event`` (or :meth:`request_stop`), then shut down."""
        await self.start()
        waiters = [asyncio.ensure_future(self._stop_requested.wait())]
        if stop_event is not None:
            waiters.append(asyncio.ensure_future(stop_event.wait()))
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()
            await self.stop()

    def request_stop(self) -> None:
        """Ask :meth:`run` to shut down gracefully (e.g. from a signal handler)."""
        self._stop_requested.set()

    async def start(self) -> None:
        """Register, subscribe per Capability, then control and heartbeat."""
        await self._register_within_deadline()
        self.registered = True
        for capability in sorted(self.settings.capabilities):
            self._subs[capability] = await self._subscribe_capability(capability)
        self._control_sub = await self._control.subscribe(self.settings.agent_id, self._on_control)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._consume_task = asyncio.create_task(self._watch_consumers(), name="worker-consume")
        self.supervisor.register(CONSUME_LOOP, self._consume_task)
        self.supervisor.register(HEARTBEAT, self._heartbeat_task)
        self.started = True
        log.info(
            "worker started",
            capabilities=sorted(self.settings.capabilities),
            concurrency_limit=self.settings.concurrency_limit,
        )

    async def stop(self) -> None:
        """Graceful shutdown (Req 11.3); safe to call more than once."""
        if self._shutdown_started:
            await self._stopped.wait()
            return
        self._shutdown_started = True
        self._stopping = True
        # Loops stopped by the shutdown must not fail the Liveness_Probe (Req 16.9).
        self.supervisor.mark_shutting_down()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.settings.shutdown_timeout
        try:
            # 1. Stop accepting new Jobs.
            for task in self._paused.values():
                task.cancel()
            await _gather_quietly(self._paused.values())
            self._paused.clear()
            subs, self._subs = list(self._subs.values()), {}
            for sub in subs:
                await _quietly(sub.cancel(), "cancel subscription")

            # 2. Finish in-flight Jobs within the drain budget.
            drain = self.settings.shutdown_timeout * DRAIN_FRACTION
            if not await _wait_event(self._idle, drain):
                # 3. Abandon the rest; their handlers REQUEUE the dispatches.
                unfinished = list(self._inflight)
                log.warning("requeueing unfinished jobs at shutdown", count=len(unfinished))
                for entry in unfinished:
                    if entry.task is not None:
                        entry.task.cancel()
                await _wait_event(self._idle, max(0.0, deadline - loop.time()) / 2)

            # 4. Heartbeat off first so it can't re-register, then deregister and close.
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                await _gather_quietly([self._heartbeat_task])
            if self._consume_task is not None:
                self._consume_task.cancel()
                await _gather_quietly([self._consume_task])
            if self.registered:
                remaining = max(0.1, deadline - loop.time())
                await _quietly(
                    asyncio.wait_for(self._registry.deregister(self.settings.agent_id), remaining),
                    "deregister",
                )
                self.registered = False
            if self._control_sub is not None:
                await _quietly(self._control_sub.cancel(), "cancel control subscription")
            for transport in _unique([self._queue, self._control]):
                remaining = max(0.1, deadline - loop.time())
                await _quietly(asyncio.wait_for(transport.close(), remaining), "close transport")
            log.info("worker stopped")
        finally:
            self._stopped.set()

    # ----------------------------------------------------------- registration

    async def _register_within_deadline(self) -> None:
        """Register before consuming anything, or fail within the deadline (Req 5.10)."""
        timeout = self.settings.registration_timeout
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        info = self.agent_info
        last_error = "no attempt completed"
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(self._registry.register(info), remaining)
                log.info("registered in agent registry")
                return
            except TimeoutError:
                last_error = "registry did not respond"
                break
            except Exception as exc:  # noqa: BLE001 - retried until the deadline
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning("registration attempt failed", error=last_error)
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(REGISTRATION_RETRY_INTERVAL, remaining))
        raise RegistrationFailed(
            self.settings.agent_id, f"not registered within {timeout:g}s ({last_error})"
        )

    async def _heartbeat_loop(self) -> None:
        """Heartbeat once per interval; re-register if the registry forgot us (Req 11.1)."""
        while not self._stopping:
            await self._clock.sleep(self.settings.heartbeat_interval)
            if self._stopping:
                return
            try:
                # Req 17.3, 17.7: own status (Draining while drained) and Jobs in flight.
                if not await self._registry.heartbeat(
                    self.settings.agent_id, desired_status=self.status, in_flight=self.in_flight
                ):
                    log.warning("agent missing from registry; re-registering")
                    await self._registry.register(self.agent_info)
                    if self._draining:
                        await self._registry.set_status(
                            self.settings.agent_id, DRAINING, self.in_flight
                        )
            except Exception as exc:  # noqa: BLE001 - keep heartbeating
                log.warning("heartbeat failed", error=f"{type(exc).__name__}: {exc}")

    # ----------------------------------------------------------- subscriptions

    async def _watch_consumers(self) -> None:
        """Liveness handle of the consume loop (Req 16.2).

        Runs while every Capability is either consuming (an active subscription) or
        paused by the capability guard (its resume task is pending). Returns once a
        Capability subscription was cancelled outside a pause, or no consumer is left.
        While draining (Req 17.2) it waits for undrain instead, so a drained Worker's
        consume loop isn't reported as ended.
        """
        while True:
            if self._draining or not self._accepting.is_set():
                await self._accepting.wait()
                continue
            subs = list(self._subs.values())
            paused = [t for t in self._paused.values() if not t.done()]
            if any(not getattr(sub, "active", True) for sub in subs):
                return
            if not subs and not paused:
                return
            closed = [asyncio.ensure_future(wait_subscription_closed(sub)) for sub in subs]
            try:
                # Paused tasks are only awaited here, never cancelled.
                await asyncio.wait([*closed, *paused], return_when=asyncio.FIRST_COMPLETED)
            finally:
                for waiter in closed:
                    waiter.cancel()
            # Let a pause that just cancelled a subscription finish its bookkeeping.
            await asyncio.sleep(0)

    async def _subscribe_capability(self, capability: str) -> Subscription:
        return await self._queue.subscribe(
            jobs_destination(capability),
            self._on_dispatch,
            SubscribeOptions(limiter=self.limiter),
        )

    async def _pause_capability(self, capability: str) -> None:
        """Pause one Capability queue for ``guard_pause`` (cancel, then re-subscribe)."""
        if self._stopping or self._draining or capability in self._paused:
            return
        sub = self._subs.pop(capability, None)
        if sub is None:
            return
        self._paused[capability] = asyncio.create_task(self._resume_after_pause(capability))
        await _quietly(sub.cancel(), "pause subscription")

    async def _resume_after_pause(self, capability: str) -> None:
        try:
            while not self._stopping:
                await self._clock.sleep(self.settings.guard_pause)
                if self._stopping or self._draining:
                    return
                try:
                    self._subs[capability] = await self._subscribe_capability(capability)
                    return
                except Exception as exc:  # noqa: BLE001 - try again after another pause
                    log.warning(
                        "re-subscribe after pause failed",
                        capability=capability,
                        error=f"{type(exc).__name__}: {exc}",
                    )
        finally:
            self._paused.pop(capability, None)

    # ----------------------------------------------------------- handlers

    async def _on_dispatch(self, delivery: Delivery) -> None:
        message = delivery.message
        if not isinstance(message, JobDispatchMessage):
            await self._reject_malformed(delivery)
            return
        job = message.payload
        correlation_id = message.correlation_id
        if not correlation_id:
            log.warning(
                "message without correlation id",
                message_type=message.type,
                message_id=message.message_id,
                sender=delivery.headers.get(SENDER_HEADER, "unknown"),
            )
        request_id = correlation_id or job.request_id
        with bind_job(request_id, job.job_id):
            if self._stopping or self._draining:
                # Received but not started: back to the queue, no Job_Result (Req 17.2).
                await self._settle(delivery, Outcome.REQUEUE)
                return
            missing = set(job.required_capabilities) - self.settings.capabilities
            if missing:
                await self._capability_guard(delivery, missing)
                return
            await self._execute(delivery, job, request_id, message.trace_context)

    async def _reject_malformed(self, delivery: Delivery) -> None:
        error = delivery.parse_error
        if error is not None:
            reason, fld = error.reason, error.field
        else:
            msg_type = getattr(delivery.message, "type", "unknown")
            reason, fld = f"unexpected message type {msg_type!r}", "type"
        log.warning(
            "rejecting malformed job dispatch",
            destination=delivery.destination,
            reason=reason,
            field=fld,
        )
        await self._settle(delivery, Outcome.REJECT)

    async def _capability_guard(self, delivery: Delivery, missing: set[str]) -> None:
        """Return a Job this Worker can't fully serve and pause its queue (Req 4.5)."""
        log.warning(
            "job requires undeclared capabilities; requeueing",
            destination=delivery.destination,
            missing=sorted(missing),
        )
        dest = delivery.destination
        if dest.startswith(JOBS_PREFIX):
            # Pause first so the requeued Message isn't redelivered to us at once.
            await self._pause_capability(dest[len(JOBS_PREFIX):])
        await self._settle(delivery, Outcome.REQUEUE)

    async def _execute(
        self,
        delivery: Delivery,
        job: JobDispatchPayload,
        request_id: str,
        parent: TraceContextModel | None = None,
    ) -> None:
        entry = _InFlight(delivery=delivery, request_id=job.request_id, job_id=job.job_id)
        self._inflight.add(entry)
        self._idle.clear()
        capability = self._capability_of(delivery, job)
        attributes: dict[str, str | int] = {
            "das.request_id": job.request_id,
            "das.job_id": job.job_id,
            "das.attempt": job.attempt,
            "das.capability": capability,
        }
        try:
            # Req 6.5: ``worker.job`` is a child of the dispatch's Trace_Context.
            with self._tracer.span("worker.job", parent=parent, attributes=attributes) as span:
                result = await self._run_executor(entry, job, span)
                if result is None:
                    await self._settle(delivery, Outcome.REQUEUE)
                    return
                # Req 2.8: one observation per Job attempt; a publish retry
                # (redelivery of the un-ACKed dispatch) is not observed again.
                key = (job.request_id, job.job_id, job.attempt)
                if key not in self._unpublished:
                    self._metrics.job_executed(
                        capability, result.status, self._clock.monotonic() - entry.started
                    )
                trace_context = self._tracer.current_context()
            if await self._publish_result(result, request_id, trace_context):
                self._unpublished.pop(key, None)
                await self._settle(delivery, Outcome.ACK)
            else:
                self._unpublished[key] = None
                self._unpublished.move_to_end(key)
                while len(self._unpublished) > MAX_UNPUBLISHED_TRACKED:
                    self._unpublished.popitem(last=False)
                await self._settle(delivery, Outcome.REQUEUE)
        finally:
            self._inflight.discard(entry)
            if not self._inflight:
                self._idle.set()

    async def _run_executor(
        self, entry: _InFlight, job: JobDispatchPayload, span: SpanHandle
    ) -> JobResultPayload | None:
        """Run one attempt; ``None`` means it was abandoned at shutdown (requeue it)."""
        entry.started = self._clock.monotonic()
        # The task copies the current context, so ``llm.completion`` nests under ``worker.job``.
        routing = self._capability_of(entry.delivery, job)  # Phase 6 Req 2.2: the queue names the Capability
        entry.task = asyncio.create_task(self._executor.execute(job, entry.cancel, routing))
        try:
            return await entry.task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise  # this handler itself is being cancelled
            # Abandoned at shutdown: return the dispatch unchanged (Req 11.3, 6.4).
            log.info("requeueing unfinished job")
            return None
        except Exception as exc:  # noqa: BLE001 - executor bug fails the attempt
            log.error("job executor raised", error=f"{type(exc).__name__}: {exc}")
            span.fail(type(exc).__name__)
            return JobResultPayload(
                request_id=job.request_id,
                job_id=job.job_id,
                attempt=job.attempt,
                worker_id=self.settings.agent_id,
                status="Failed",
                content="",
                error=f"worker error: {type(exc).__name__}",
                usage=TokenUsage.zero(),
                duration_ms=0,
            )

    @staticmethod
    def _capability_of(delivery: Delivery, job: JobDispatchPayload) -> str:
        """Routing Capability: the ``jobs/<capability>`` queue the dispatch came from."""
        dest = delivery.destination
        if dest.startswith(JOBS_PREFIX) and len(dest) > len(JOBS_PREFIX):
            return dest[len(JOBS_PREFIX):]
        caps = sorted(job.required_capabilities)
        return caps[0] if caps else "unknown"

    def _result_headers(self, result: JobResultPayload) -> dict[str, str]:
        per_result = getattr(self._executor, "headers_for", None)
        getter = getattr(self._executor, "result_headers", None)
        if per_result is None and getter is None:
            return {}
        try:
            headers = per_result(result) if per_result is not None else getter()  # type: ignore[misc]
            return {str(k): str(v) for k, v in dict(headers).items()}
        except Exception as exc:  # noqa: BLE001 - headers are best effort (D2)
            log.warning("job result headers unavailable", error=type(exc).__name__)
            return {}

    async def _publish_result(
        self,
        result: JobResultPayload,
        request_id: str,
        trace_context: TraceContextModel | None = None,
    ) -> bool:
        message = new_message(result, correlation_id=request_id, trace_context=trace_context)
        try:
            await self._queue.publish(
                RESULTS_DESTINATION, message, headers=self._result_headers(result) or None
            )
        except Exception as exc:  # noqa: BLE001 - dispatch stays un-ACKed
            log.warning(
                "job result publish failed; dispatch not acknowledged",
                error=f"{type(exc).__name__}: {exc}",
            )
            return False
        log.info("job result published", status=result.status, attempt=result.attempt)
        return True

    async def _on_control(self, delivery: Delivery) -> TypedMessage | None:
        """Handle ``cancel_job`` (unknown Job_IDs are ignored, Req 5.7) and
        ``agent_control`` drain/undrain (request-reply, Req 17)."""
        message = delivery.message
        if isinstance(message, AgentControlMessage):
            return await self._on_agent_control(delivery, message)
        if not isinstance(message, ControlMessage):
            log.warning(
                "rejecting malformed control message",
                reason=delivery.parse_error.reason if delivery.parse_error else "unexpected type",
            )
            await self._settle(delivery, Outcome.REJECT, self._control)
            return None
        payload = message.payload
        with bind_correlation_id(message.correlation_id or payload.request_id):
            if not message.correlation_id:
                log.warning(
                    "message without correlation id",
                    message_type=message.type,
                    message_id=message.message_id,
                    sender=delivery.headers.get(SENDER_HEADER, "unknown"),
                )
            if payload.command == "cancel_job" and payload.job_id:
                matched = [
                    e
                    for e in self._inflight
                    if e.job_id == payload.job_id and e.request_id == payload.request_id
                ]
                for entry in matched:
                    entry.cancel.set()
                log.info("cancel_job received", job_id=payload.job_id, matched=len(matched))
            await self._settle(delivery, Outcome.ACK, self._control)
        return None

    async def _on_agent_control(
        self, delivery: Delivery, message: AgentControlMessage
    ) -> TypedMessage | None:
        """Apply drain/undrain and return the confirmation (Req 17.2, 17.8, 17.9).

        ``release_leases`` is ignored by Workers (Req 17.15). A command addressed to
        another agent gets no reply, so the caller times out instead of being misled.
        """
        payload: AgentControlPayload = message.payload
        await self._settle(delivery, Outcome.ACK, self._control)
        if payload.agent_id != self.settings.agent_id:
            log.warning("agent_control misrouted", target=payload.agent_id)
            return None
        if payload.command == "drain":
            reply = await self.drain()
        else:
            reply = await self.undrain()
        log.info(
            "agent_control applied",
            command=payload.command,
            status=reply.status,
            in_flight=reply.in_flight,
        )
        return new_message(reply, correlation_id=message.correlation_id)

    async def _settle(
        self, delivery: Delivery, outcome: Outcome, transport: Transport | None = None
    ) -> None:
        transport = transport or self._queue
        try:
            await transport.acknowledge(delivery, outcome)
        except Exception as exc:  # noqa: BLE001 - a lost connection returns it anyway
            log.warning(
                "acknowledge failed",
                outcome=outcome.value,
                error=f"{type(exc).__name__}: {exc}",
            )


# --------------------------------------------------------------------------- helpers


async def _quietly(awaitable: object, what: str) -> None:
    try:
        await awaitable  # type: ignore[misc]
    except Exception as exc:  # noqa: BLE001 - shutdown continues
        log.warning("shutdown step failed", step=what, error=f"{type(exc).__name__}: {exc}")


async def _gather_quietly(tasks: Iterable[asyncio.Task[None]]) -> None:
    tasks = list(tasks)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _wait_event(event: asyncio.Event, timeout: float) -> bool:
    if event.is_set():
        return True
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(event.wait(), timeout)
    return event.is_set()


def _unique(items: Iterable[Transport]) -> list[Transport]:
    seen: list[Transport] = []
    for item in items:
        if not any(item is s for s in seen):
            seen.append(item)
    return seen


__all__ = [
    "CRITICAL_LOOPS",
    "DEFAULT_GUARD_PAUSE",
    "DEFAULT_REGISTRATION_TIMEOUT",
    "DRAIN_FRACTION",
    "JOBS_PREFIX",
    "RESULTS_DESTINATION",
    "STATUS_WRITE_TIMEOUT",
    "ExecutorLike",
    "RegistryLike",
    "Worker",
    "WorkerSettings",
    "jobs_destination",
]
