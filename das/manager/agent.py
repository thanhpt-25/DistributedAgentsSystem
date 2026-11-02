"""Manager_Agent process wiring (Requirements 2.1, 8.4, 11.1-11.3, 11.6, 15.4).

``ManagerAgent`` assembles the Manager runtime from already-built collaborators,
so the CLI (task 17.2) can construct it from Agent_Config and tests can drive it
with in-memory fakes:

- ``queue``: Queue_Transport for Job dispatch, ``results``, ``dlq`` and
  ``dlq-events``.
- ``control``: RPC_Transport. Subscribing to the Manager's own agent ID is what
  makes the gRPC control server serve ``cancel_request`` commands.
- ``store``: State_Store; ``registry``: Agent_Registry.
- ``llm``: the agent's ``LLMProvider``; the Planner and Summarizer are built from
  it unless passed explicitly.

Config loading, transport loading by name (Req 8.4: before any subscribe or
publish) and the startup dependency retry happen in the CLI before
:meth:`ManagerAgent.start` runs; transports must already be connected. The agent
closes them on shutdown.

Startup order (:meth:`ManagerAgent.start`):

1. Register in the Agent_Registry (role ``manager``, status Available, gRPC
   endpoint), retrying until ``registration_timeout``; otherwise raise
   :class:`~das.errors.RegistrationFailed` without subscribing to anything
   (Req 11.6).
2. Subscribe ``ResultIngest`` (``results``), ``DeadLetterIngest``
   (``dlq-events``), ``DeadLetterRecorder`` (``dlq``, Phase 2 Req 12.1) and the
   ``ControlCommandHandler`` (own agent ID on the control transport, i.e. the
   gRPC control server).
3. Start the loops: ``ClaimLoop`` (1 s, Req 2.1), ``RegistrySweeper`` and
   ``Heartbeat`` (both every ``heartbeat_interval``, Req 11.1, 11.2). The
   heartbeat re-registers when the registry no longer knows this agent.
   Then the non-Critical_Loops (Phase 2): ``QueueDepthSampler`` (only when the
   queue transport is a ``QueueInspector``, Req 3.1), ``RegistryGauges``
   (Req 3.6), ``RetentionSweeper`` (only when ``retention.enabled``, Req 14.3)
   and the one-shot ``IndexBackfill`` (Req 15.10). A non-Critical_Loop that
   ends with an error is logged and started again after
   ``NON_CRITICAL_RESTART_DELAY`` (Req 14.13).

Observability (Phase 2): ``recorder`` and ``tracer`` default to no-ops. The
agent builds ``ManagerMetrics`` from the recorder, hands metrics and Tracer to
the drivers, ClaimLoop, LeaseKeepers (``das_leases_held``) and ResultIngest, and
attaches an ``LLMMetricsObserver`` with the agent's Price_Table to its
``LLMProvider``.

Graceful shutdown (:meth:`ManagerAgent.stop`, Req 11.3), within
``shutdown_timeout``: stop claiming, cancel the ingest subscriptions so new
Job_Results stay queued for other Managers, stop every RequestDriver (their
Leases are released through the epoch-fenced ``release_lease``; one that cannot be
released simply lapses and another Manager resumes the Request from the
State_Store), stop the sweeper and heartbeat, deregister, cancel the control
subscription and close the transports.

Drain (Phase 2 Req 17): :meth:`ManagerAgent.drain` pauses the ClaimLoop and sets
the registry status to Draining (heartbeats then carry ``desired_status=Draining``
and ``in_flight``); with ``release_leases`` every RequestDriver releases its Lease.
:meth:`ManagerAgent.undrain` reverses it. Both are reached through
``agent_control`` request-reply calls on the agent's RPC destination and are
idempotent. Draining is not persisted, so a restart registers Available.

Messaging goes only through :class:`~das.transport.base.Transport` and the
:class:`~das.codec.MessageCodec` used by the transports (Req 15.4); this module
imports no broker, RPC or Redis client (import-linter contract).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

from das.clock import Clock, SystemClock
from das.codec import AgentControlPayload, AgentControlReplyPayload
from das.config.schema import RetentionConfig
from das.errors import RegistrationFailed
from das.health.supervisor import LoopSupervisor
from das.llm.retry import LLMProvider
from das.manager.claim import CLAIM_PERIOD, ClaimLoop
from das.manager.driver import DriverDeps, RequestDrivers
from das.manager.ingest import (
    DEFAULT_MAX_MESSAGE_BYTES,
    ControlCommandHandler,
    DeadLetterIngest,
    DeadLetterRecorder,
    ResultIngest,
)
from das.manager.lease import HeldLeases
from das.manager.machine import MachineConfig
from das.manager.planner import Planner, PlannerConfig
from das.manager.retention import IndexBackfill, RetentionSweeper
from das.manager.samplers import QueueDepthSampler, RegistryGauges
from das.manager.summarizer import Summarizer
from das.observability import get_logger
from das.observability.metrics import (
    LLMMetricsObserver,
    ManagerMetrics,
    MetricsRecorder,
    NoopRecorder,
)
from das.observability.tracing import NoopTracer, Tracer
from das.ops.cost import PriceTable
from das.registry import AVAILABLE, DRAINING, AgentInfo, RegistrySnapshot
from das.registry.registry import SettableStatus
from das.state import StateStore
from das.transport.base import QueueInspector, Subscription, Transport

log = get_logger(__name__)

MANAGER_ROLE: Final = "manager"
#: Critical_Loop names registered with the LoopSupervisor (Req 16.2).
CLAIM_LOOP: Final = "claim_loop"
RESULT_INGEST: Final = "result_ingest"
DEAD_LETTER_INGEST: Final = "dead_letter_ingest"
DEAD_LETTER_RECORDER: Final = "dead_letter_recorder"
REGISTRY_SWEEPER: Final = "registry_sweeper"
HEARTBEAT: Final = "heartbeat"
CRITICAL_LOOPS: Final = (
    CLAIM_LOOP,
    RESULT_INGEST,
    DEAD_LETTER_INGEST,
    DEAD_LETTER_RECORDER,
    REGISTRY_SWEEPER,
    HEARTBEAT,
)
DEFAULT_REGISTRATION_TIMEOUT: Final = 30.0
REGISTRATION_RETRY_INTERVAL: Final = 1.0
#: Share of ``shutdown_timeout`` spent stopping RequestDrivers; the rest is kept
#: for deregistering and closing transports.
DRIVER_STOP_FRACTION: Final = 0.6
#: Seconds before a non-Critical_Loop that ended with an error is started again
#: (Req 14.13; design "Các vòng lặp mới của Manager_Agent").
NON_CRITICAL_RESTART_DELAY: Final = 1.0
DEFAULT_QUEUE_SAMPLE_INTERVAL: Final = 15.0
#: Bound for writing Draining/Available to the registry on drain/undrain (Req 17.4, 17.8).
STATUS_WRITE_TIMEOUT: Final = 1.0
#: Background loops that are not Critical_Loops: their failures are logged and the
#: loop starts again; they never stop the agent.
NON_CRITICAL_LOOPS: Final = frozenset(
    {"queue_sampler", "registry_gauges", "retention", "index_backfill"}
)


class ManagerRegistry(Protocol):
    """What the Manager needs from :class:`das.registry.AgentRegistry`."""

    async def register(self, info: AgentInfo) -> float: ...
    async def heartbeat(
        self, agent_id: str, desired_status: SettableStatus = ..., in_flight: int = ...
    ) -> bool: ...
    async def set_status(
        self, agent_id: str, status: SettableStatus, in_flight: int | None = ...
    ) -> bool: ...
    async def deregister(self, agent_id: str) -> bool: ...
    async def sweep(self, now: float | None = None) -> list[str]: ...
    async def snapshot(self) -> RegistrySnapshot: ...


@dataclass(frozen=True, slots=True)
class ManagerSettings:
    """Manager values taken from Agent_Config (ranges are enforced by the config schema)."""

    agent_id: str
    host: str
    grpc_endpoint: str | None = None
    lease_duration: float = 30.0
    heartbeat_interval: float = 5.0
    shutdown_timeout: float = 30.0
    registration_timeout: float = DEFAULT_REGISTRATION_TIMEOUT
    planner_config: PlannerConfig = field(default_factory=PlannerConfig)
    machine_config: MachineConfig = field(default_factory=MachineConfig)
    claim_period: float = CLAIM_PERIOD
    max_concurrent: int | None = None
    #: Seconds between registry sweeps; defaults to ``heartbeat_interval``.
    sweep_interval: float | None = None
    #: ``metrics.queue_sample_interval`` (Req 3.1).
    queue_sample_interval: float = DEFAULT_QUEUE_SAMPLE_INTERVAL
    #: ``limits.max_message_bytes``: raw-bytes bound of Dead_Letter_Records (Req 12.1).
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES
    #: Retention_Policy (Req 14); disabled by default.
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    #: The agent's Price_Table for ``das_llm_estimated_cost_total`` (Req 4.6, 10.7) and
    #: for pricing ``max_cost`` budgets (Phase 4 Req 5.5-5.7).
    price_table: PriceTable | None = None

    def __post_init__(self) -> None:
        if not self.agent_id:
            raise ValueError("agent_id must be non-empty")
        for name in (
            "lease_duration",
            "heartbeat_interval",
            "shutdown_timeout",
            "registration_timeout",
            "claim_period",
            "queue_sample_interval",
            "max_message_bytes",
        ):
            if not getattr(self, name) > 0:
                raise ValueError(f"{name} must be positive")
        if self.sweep_interval is not None and not self.sweep_interval > 0:
            raise ValueError("sweep_interval must be positive")

    @property
    def effective_sweep_interval(self) -> float:
        return self.sweep_interval or self.heartbeat_interval

    @classmethod
    def from_config(cls, config: Any) -> ManagerSettings:
        """Build from a validated :class:`~das.config.schema.AgentConfig`."""
        limits = config.limits
        grpc_cfg = config.grpc
        endpoint = (
            f"{grpc_cfg.advertise_host}:{grpc_cfg.advertise_port}" if grpc_cfg is not None else None
        )
        return cls(
            agent_id=str(config.agent_id),
            host=str(config.host),
            grpc_endpoint=endpoint,
            lease_duration=float(limits.lease_duration),
            heartbeat_interval=float(limits.heartbeat_interval),
            shutdown_timeout=float(limits.shutdown_timeout),
            planner_config=PlannerConfig(
                max_jobs=int(limits.max_jobs), planning_retries=int(limits.planning_retries)
            ),
            machine_config=MachineConfig.from_limits(limits),
            queue_sample_interval=float(config.metrics.queue_sample_interval),
            max_message_bytes=int(limits.max_message_bytes),
            retention=config.retention,
            price_table=config.pricing.price_table() if config.pricing is not None else None,
        )


class ManagerAgent:
    """One Manager_Agent process (see module docstring)."""

    def __init__(
        self,
        settings: ManagerSettings,
        *,
        queue: Transport,
        control: Transport,
        store: StateStore,
        registry: ManagerRegistry,
        llm: Any = None,
        planner: Planner | None = None,
        summarizer: Summarizer | None = None,
        clock: Clock | None = None,
        tweak_deps: Callable[[DriverDeps], DriverDeps] | None = None,
        recorder: MetricsRecorder | None = None,
        tracer: Tracer | None = None,
        supervisor: LoopSupervisor | None = None,
    ) -> None:
        """``recorder`` and ``tracer`` default to no-ops (the CLI wires real ones).

        ``supervisor`` receives every Critical_Loop for the Liveness_Probe; a private
        one is created when omitted.
        """
        if planner is None or summarizer is None:
            if llm is None:
                raise ValueError("llm is required unless planner and summarizer are given")
            planner = planner or Planner(llm)
            summarizer = summarizer or Summarizer(llm)
        self.settings = settings
        self._queue = queue
        self._control = control
        self._store = store
        self._registry = registry
        self._clock: Clock = clock or SystemClock()
        self.supervisor = supervisor if supervisor is not None else LoopSupervisor()
        self.recorder: MetricsRecorder = recorder if recorder is not None else NoopRecorder()
        self.metrics = ManagerMetrics(self.recorder)
        self.tracer: Tracer = tracer if tracer is not None else NoopTracer()
        #: One LLMMetricsObserver per LLMProvider used by this agent (Req 4).
        self.llm_observers = _attach_llm_observability(
            [llm, getattr(planner, "_llm", None), getattr(summarizer, "_llm", None)],
            self.metrics,
            tracer,
            settings.price_table,
        )
        deps = DriverDeps(
            store=store,
            registry=registry,
            transport=queue,
            planner=planner,
            summarizer=summarizer,
            planner_config=settings.planner_config,
            machine_config=settings.machine_config,
            control_transport=control,
            clock=self._clock,
            metrics=self.metrics,
            tracer=self.tracer,
            price_table=settings.price_table,  # Phase 4 Req 5.5-5.7
        )
        if tweak_deps is not None:
            deps = tweak_deps(deps)
        self.held_leases = HeldLeases()
        self.metrics.leases_held(self.held_leases.count)  # Req 3.5
        self.drivers = RequestDrivers(deps, settings.lease_duration, held=self.held_leases)
        self.result_ingest = ResultIngest(
            queue, store, on_ingested=self.drivers.notify, tracer=self.tracer
        )
        self.dead_letter_ingest = DeadLetterIngest(queue, store, on_ingested=self.drivers.notify)
        self.dead_letter_recorder = DeadLetterRecorder(
            queue, store, self.metrics, max_message_bytes=settings.max_message_bytes
        )
        self.control_handler = ControlCommandHandler(
            control, settings.agent_id, self.drivers, on_agent_control=self.handle_agent_control
        )
        self.claim_loop = ClaimLoop(
            store,
            settings.agent_id,
            settings.lease_duration,
            self.drivers.on_claimed,
            clock=self._clock,
            period=settings.claim_period,
            max_concurrent=settings.max_concurrent,
            metrics=self.metrics,
        )
        # Non-Critical_Loops (Req 3.1, 3.6, 14.3, 15.10). The queue sampler needs the
        # optional QueueInspector capability of the queue transport.
        self.queue_sampler: QueueDepthSampler | None = (
            QueueDepthSampler(
                queue,
                registry,
                self.metrics,
                self._clock,
                interval=settings.queue_sample_interval,
            )
            if isinstance(queue, QueueInspector)
            else None
        )
        self.registry_gauges = RegistryGauges(
            registry, self.metrics, self._clock, interval=settings.heartbeat_interval
        )
        self.retention_sweeper = RetentionSweeper(
            store, self.metrics, self._clock, settings.retention
        )
        self.index_backfill = IndexBackfill(store, self._clock, owner=settings.agent_id)
        self._subs: list[Subscription] = []
        self._tasks: dict[str, asyncio.Task[None]] = {}
        #: Watcher tasks standing in for the ingest subscriptions in the supervisor.
        self._watchers: list[asyncio.Task[None]] = []
        self._stop_requested = asyncio.Event()
        self._stopping = False
        self._shutdown_started = False
        self._stopped = asyncio.Event()
        #: Draining is in-memory only: a restarted agent registers Available (Req 17.16).
        self._draining = False
        self._drain_lock = asyncio.Lock()
        self.registered = False
        #: True once every Critical_Loop has been started (Readiness ``startup``, Req 16.4).
        self.started = False
        #: Startup steps in the order they happened (inspection/tests).
        self.startup_steps: list[str] = []

    # ----------------------------------------------------------- inspection

    @property
    def agent_info(self) -> AgentInfo:
        s = self.settings
        return AgentInfo(
            agent_id=s.agent_id,
            role=MANAGER_ROLE,
            host=s.host,
            grpc_endpoint=s.grpc_endpoint,
            heartbeat_interval=s.heartbeat_interval,
        )

    @property
    def stopping(self) -> bool:
        return self._stopping

    @property
    def draining(self) -> bool:
        """True while drained (Readiness ``not_draining``, Req 16.4, 17.4)."""
        return self._draining

    @property
    def status(self) -> SettableStatus:
        """The agent's own registry status: Draining or Available."""
        return DRAINING if self._draining else AVAILABLE

    @property
    def in_flight(self) -> int:
        """Leases currently owned (heartbeat ``in_flight``, Req 17.7)."""
        return len(self.drivers.owned)

    @property
    def loops(self) -> frozenset[str]:
        """Names of the background loops currently running."""
        return frozenset(name for name, task in self._tasks.items() if not task.done())

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
        """Register, subscribe, then start the claim, sweep and heartbeat loops."""
        await self._register_within_deadline()
        self.registered = True
        self.startup_steps.append("register")
        ingests: list[tuple[str, Subscription]] = []
        try:
            sub = await self.result_ingest.start()
            self._subs.append(sub)
            ingests.append((RESULT_INGEST, sub))
            self.startup_steps.append("subscribe:results")
            sub = await self.dead_letter_ingest.start()
            self._subs.append(sub)
            ingests.append((DEAD_LETTER_INGEST, sub))
            self.startup_steps.append("subscribe:dlq-events")
            sub = await self.dead_letter_recorder.start()
            self._subs.append(sub)
            ingests.append((DEAD_LETTER_RECORDER, sub))
            self.startup_steps.append("subscribe:dlq")
            self._subs.append(await self.control_handler.start())
            self.startup_steps.append("subscribe:control")
        except BaseException:
            await self._cancel_subscriptions()
            raise
        self._tasks["claim"] = asyncio.create_task(self.claim_loop.run(), name="manager-claim")
        self._tasks["sweeper"] = asyncio.create_task(self._sweep_loop(), name="manager-sweeper")
        self._tasks["heartbeat"] = asyncio.create_task(
            self._heartbeat_loop(), name="manager-heartbeat"
        )
        # Critical_Loops for the Liveness_Probe (Req 16.2).
        self.supervisor.register(CLAIM_LOOP, self._tasks["claim"])
        for name, sub in ingests:
            self._watchers.append(self.supervisor.register_subscription(name, sub))
        self.supervisor.register(REGISTRY_SWEEPER, self._tasks["sweeper"])
        self.supervisor.register(HEARTBEAT, self._tasks["heartbeat"])
        for name, factory in self._non_critical_loops().items():
            self._tasks[name] = asyncio.create_task(
                self._restarting(name, factory), name=f"manager-{name}"
            )
        self.startup_steps.append("loops")
        self.started = True
        log.info(
            "manager started",
            grpc_endpoint=self.settings.grpc_endpoint,
            lease_duration=self.settings.lease_duration,
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

        def remaining() -> float:
            return max(0.1, deadline - loop.time())

        try:
            # 1. Stop claiming new Requests.
            self.claim_loop.stop()
            await _cancel_tasks([self._tasks.pop("claim")] if "claim" in self._tasks else [])

            # 2. Stop ingesting: unacknowledged results stay queued for other Managers.
            for ingest in (self.result_ingest, self.dead_letter_ingest, self.dead_letter_recorder):
                await _quietly(ingest.stop(), "cancel ingest subscription")

            # 3. Stop the RequestDrivers; Leases are released (fenced) or lapse.
            budget = self.settings.shutdown_timeout * DRIVER_STOP_FRACTION
            await _quietly(asyncio.wait_for(self.drivers.stop_all(), budget), "stop drivers")

            # 4. Sweeper, heartbeat (so nothing re-registers) and the non-critical
            #    loops off, then deregister.
            for stoppable in self._stoppable_loops():
                stoppable.stop()
            await _cancel_tasks(list(self._tasks.values()))
            self._tasks.clear()
            await _cancel_tasks(self._watchers)
            self._watchers.clear()
            if self.registered:
                await _quietly(
                    asyncio.wait_for(
                        self._registry.deregister(self.settings.agent_id), remaining()
                    ),
                    "deregister",
                )
                self.registered = False

            # 5. Control subscription (gRPC control server) and transports.
            await self._cancel_subscriptions()
            for transport in _unique([self._queue, self._control]):
                await _quietly(asyncio.wait_for(transport.close(), remaining()), "close transport")
            log.info("manager stopped")
        finally:
            self._stopped.set()

    async def _cancel_subscriptions(self) -> None:
        subs, self._subs = self._subs, []
        for sub in subs:
            await _quietly(sub.cancel(), "cancel subscription")

    # ----------------------------------------------------------- drain (Req 17)

    async def drain(self, release_leases: bool = False) -> AgentControlReplyPayload:
        """Stop acquiring Leases and report Draining (Req 17.4, 17.5, 17.9).

        Owned Requests keep running with their Leases renewed, and every other
        loop (ingests, sweeper, heartbeat, RetentionSweeper) keeps running. With
        ``release_leases`` every driver is asked to release its Lease at its next
        event boundary (fenced ``release_lease``), so another Manager takes the
        Request over. Draining again only confirms (idempotent).
        """
        async with self._drain_lock:
            self.claim_loop.pause()  # immediate: no new acquire from here on
            was_draining = self._draining
            self._draining = True
            if release_leases:
                released = self.drivers.request_release_all()
                log.info("drain releasing leases", requests=released)
            if not was_draining:
                await self._publish_status()
                log.info("manager draining", release_leases=release_leases)
            return self._control_reply()

    async def undrain(self) -> AgentControlReplyPayload:
        """Report Available and acquire Leases again (Req 17.8, 17.9; idempotent)."""
        async with self._drain_lock:
            was_draining = self._draining
            self._draining = False
            self.claim_loop.resume()
            if was_draining:
                await self._publish_status()
                log.info("manager undrained")
            return self._control_reply()

    async def handle_agent_control(self, payload: AgentControlPayload) -> AgentControlReplyPayload:
        """``agent_control`` callback of the ControlCommandHandler."""
        if payload.command == "drain":
            return await self.drain(payload.release_leases)
        return await self.undrain()

    def _control_reply(self) -> AgentControlReplyPayload:
        return AgentControlReplyPayload(
            agent_id=self.settings.agent_id, status=self.status, in_flight=self.in_flight
        )

    async def _publish_status(self) -> None:
        """Write the own status to the registry within STATUS_WRITE_TIMEOUT.

        A failed write is logged; the next heartbeat carries the status anyway.
        """
        try:
            await asyncio.wait_for(
                self._registry.set_status(
                    self.settings.agent_id, self.status, in_flight=self.in_flight
                ),
                STATUS_WRITE_TIMEOUT,
            )
        except Exception as exc:  # noqa: BLE001 - heartbeat retries it
            log.warning("status write failed", status=self.status, error=type(exc).__name__)

    # ----------------------------------------------------------- registration

    async def _register_within_deadline(self) -> None:
        """Register before consuming anything, or fail within the deadline (Req 11.6)."""
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

    # ----------------------------------------------------------- loops

    async def _heartbeat_loop(self) -> None:
        """Heartbeat once per interval; re-register if the registry forgot us (Req 11.1)."""
        while not self._stopping:
            await self._clock.sleep(self.settings.heartbeat_interval)
            if self._stopping:
                return
            try:
                if not await self._registry.heartbeat(
                    self.settings.agent_id,
                    desired_status=self.status,
                    in_flight=self.in_flight,
                ):
                    log.warning("agent missing from registry; re-registering")
                    await self._registry.register(self.agent_info)
                    if self._draining:  # register always writes Available
                        await self._publish_status()
            except Exception as exc:  # noqa: BLE001 - keep heartbeating
                log.warning("heartbeat failed", error=f"{type(exc).__name__}: {exc}")

    async def _sweep_loop(self) -> None:
        """Mark agents silent for 3 intervals Unavailable (Req 11.2; idempotent)."""
        while not self._stopping:
            await self.sweep_once()
            if self._stopping:
                return
            await self._clock.sleep(self.settings.effective_sweep_interval)

    def _non_critical_loops(self) -> dict[str, Callable[[], Awaitable[None]]]:
        """Name -> ``run`` of each non-Critical_Loop this agent runs."""
        loops: dict[str, Callable[[], Awaitable[None]]] = {}
        if self.queue_sampler is not None:
            loops["queue_sampler"] = self.queue_sampler.run
        loops["registry_gauges"] = self.registry_gauges.run
        if self.retention_sweeper.enabled:  # Req 14.12: disabled -> no loop at all
            loops["retention"] = self.retention_sweeper.run
        loops["index_backfill"] = self.index_backfill.run
        return loops

    def _stoppable_loops(self) -> list[Any]:
        loops: list[Any] = [self.registry_gauges, self.retention_sweeper, self.index_backfill]
        if self.queue_sampler is not None:
            loops.append(self.queue_sampler)
        return loops

    async def _restarting(self, name: str, run: Callable[[], Awaitable[None]]) -> None:
        """Run a non-Critical_Loop; start it again after an error (Req 14.13).

        A normal return (stop requested, or a one-shot loop such as the index
        backfill that finished) ends it for good.
        """
        while not self._stopping:
            try:
                await run()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - never stops the agent
                log.warning(
                    "background loop failed; restarting",
                    loop=name,
                    error=f"{type(exc).__name__}: {exc}",
                )
            if self._stopping:
                return
            await self._clock.sleep(NON_CRITICAL_RESTART_DELAY)

    async def sweep_once(self) -> list[str]:
        try:
            marked = await self._registry.sweep()
        except Exception as exc:  # noqa: BLE001 - retried next interval
            log.warning("registry sweep failed", error=f"{type(exc).__name__}: {exc}")
            return []
        if marked:
            log.warning("agents marked unavailable", agent_ids=sorted(marked))
        return marked


# --------------------------------------------------------------------------- helpers


def _attach_llm_observability(
    providers: Iterable[object],
    metrics: ManagerMetrics,
    tracer: Tracer | None,
    price_table: PriceTable | None,
) -> list[LLMMetricsObserver]:
    """Give each distinct ``LLMProvider`` an ``LLMMetricsObserver`` (with the agent's
    Price_Table) unless it already has an observer, and the injected Tracer when one
    was passed (Req 4, 6.6)."""
    observers: list[LLMMetricsObserver] = []
    seen: list[object] = []
    for provider in providers:
        if not isinstance(provider, LLMProvider) or any(provider is s for s in seen):
            continue
        seen.append(provider)
        if provider.observer is None:
            observer = LLMMetricsObserver(
                metrics.llm, provider.provider, provider.params.model, price_table
            )
            provider.observer = observer
            observers.append(observer)
        if tracer is not None:
            provider.tracer = tracer
    return observers


async def _quietly(awaitable: Awaitable[object], what: str) -> None:
    try:
        await awaitable
    except Exception as exc:  # noqa: BLE001 - shutdown continues
        log.warning("shutdown step failed", step=what, error=f"{type(exc).__name__}: {exc}")


async def _cancel_tasks(tasks: Iterable[asyncio.Task[None]]) -> None:
    tasks = list(tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks, return_exceptions=True)


def _unique(items: Iterable[Transport]) -> list[Transport]:
    seen: list[Transport] = []
    for item in items:
        if not any(item is s for s in seen):
            seen.append(item)
    return seen


__all__ = [
    "CRITICAL_LOOPS",
    "DEFAULT_REGISTRATION_TIMEOUT",
    "DRIVER_STOP_FRACTION",
    "MANAGER_ROLE",
    "NON_CRITICAL_LOOPS",
    "NON_CRITICAL_RESTART_DELAY",
    "STATUS_WRITE_TIMEOUT",
    "ManagerAgent",
    "ManagerRegistry",
    "ManagerSettings",
]
