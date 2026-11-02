"""Manager ingest loops: Job_Results, dead letters, and control commands
(Requirements 1.6, 6.6, 9.5, 13.4-13.6).

- :class:`ResultIngest` consumes ``results`` (competing consumers across all
  Managers). Each Job_Result is appended to its Request's inbox stream in the
  State_Store and only then acknowledged, so a result survives a crash of the Manager
  that received it; only the lease owner applies it (design "Manager internals").
  Unparseable Messages are rejected without requeue and change nothing (9.5).
- :class:`DeadLetterIngest` consumes ``dlq-events`` and turns dispatches the broker
  dead-lettered because of its delivery limit (repeated unacknowledged delivery) into
  ``unacked_delivery_exhausted`` inbox events (6.6). Other dead letters, notably the
  copies a Manager publishes itself for exhausted retries (reason ``max_attempts``),
  are acknowledged and ignored, so nothing loops.
- :class:`DeadLetterRecorder` consumes ``dlq`` and creates exactly one
  Dead_Letter_Record per dead-lettered Message for operators (Phase 2 Req 12).
- :class:`ControlCommandHandler` receives ``cancel_request`` commands from the Gateway
  on the Manager's RPC destination and wakes the owning driver (1.6).

Every handler binds the inbound correlation ID as the Request_ID for logging and
outbound Messages (13.5) and warns when a request-scoped Message lacks one (13.6).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Final, Protocol

from das.codec import (
    DEFAULT_MAX_BYTES,
    AgentControlMessage,
    AgentControlPayload,
    AgentControlReplyPayload,
    ControlMessage,
    JobDispatchMessage,
    JobResultMessage,
    TypedMessage,
    new_message,
)
from das.manager.routing import jobs_destination
from das.observability import bind_correlation_id, bind_job, get_logger
from das.observability.metrics import ManagerMetrics, NoopRecorder
from das.observability.tracing import NoopTracer, Tracer
from das.ops.dlq import build_record, classify_reason, dead_letter_id, dead_letter_identity
from das.state import INBOX_UNACKED_EXHAUSTED, StateStore
from das.transport.base import Delivery, Outcome, Subscription, Transport

log = get_logger(__name__)

RESULTS_DESTINATION: Final = "results"
DLQ_EVENTS_DESTINATION: Final = "dlq-events"
#: The Dead_Letter_Queue ``das.dlq`` (design D4).
DLQ_DESTINATION: Final = "dlq"
DEFAULT_MAX_MESSAGE_BYTES: Final = DEFAULT_MAX_BYTES

#: Job_Result Transport headers naming the LLM (design D2; set by the Worker executor,
#: duplicated here so das.manager never imports das.worker).
LLM_PROVIDER_HEADER: Final = "x-llm-provider"
LLM_MODEL_HEADER: Final = "x-llm-model"
UNKNOWN_LLM: Final = "unknown"

# Header keys every transport exposes on dead-lettered Messages (see
# das.transport.rabbitmq; duplicated so das.manager never imports a broker client).
DEAD_LETTER_REASON_HEADER: Final = "x-death-reason"
ORIGINAL_DESTINATION_HEADER: Final = "x-original-destination"
DELIVERY_LIMIT_REASON: Final = "delivery_limit"
SENDER_HEADER: Final = "x-sender"

JOBS_DESTINATION_PREFIX: Final = jobs_destination("")

OnIngested = Callable[[str], object]


def _sender(delivery: Delivery, fallback: str | None = None) -> str:
    return fallback or delivery.headers.get(SENDER_HEADER) or "unknown"


def warn_missing_correlation(message: TypedMessage, sender: str) -> None:
    """Req 13.6: a request-scoped Message arrived without a correlation ID."""
    log.warning("message_missing_correlation_id", message_type=message.type, sender=sender)


def llm_meta(headers: Mapping[str, str]) -> dict[str, str]:
    """Provider/model of a Job_Result from its Transport headers (design D2, Req 9.2).

    A missing or empty header (e.g. a v1 Worker) becomes ``"unknown"``."""
    return {
        "provider": headers.get(LLM_PROVIDER_HEADER) or UNKNOWN_LLM,
        "model": headers.get(LLM_MODEL_HEADER) or UNKNOWN_LLM,
    }


class ResultIngest:
    """``results`` -> Request inbox (XADD), then ACK."""

    def __init__(
        self,
        transport: Transport,
        store: StateStore,
        on_ingested: OnIngested | None = None,
        destination: str = RESULTS_DESTINATION,
        tracer: Tracer | None = None,
    ) -> None:
        self._transport = transport
        self._store = store
        self._on_ingested = on_ingested
        self.destination = destination
        self._tracer: Tracer = tracer or NoopTracer()
        self.subscription: Subscription | None = None

    async def start(self) -> Subscription:
        self.subscription = await self._transport.subscribe(self.destination, self.handle)
        return self.subscription

    async def stop(self) -> None:
        if self.subscription is not None:
            await self.subscription.cancel()

    async def handle(self, delivery: Delivery) -> None:
        message = delivery.message
        if message is None:
            error = delivery.parse_error
            log.warning(
                "result_rejected_unparseable",
                reason=getattr(error, "reason", None),
                field=getattr(error, "field", None),
            )
            await self._transport.acknowledge(delivery, Outcome.REJECT)
            return
        if not isinstance(message, JobResultMessage):
            log.warning("result_rejected_wrong_type", message_type=message.type)
            await self._transport.acknowledge(delivery, Outcome.REJECT)
            return
        payload = message.payload
        attributes: dict[str, str | int] = {
            "das.request_id": payload.request_id,
            "das.job_id": payload.job_id,
            "das.attempt": payload.attempt,
        }
        # Req 6.7: ``manager.ingest_result`` is a child of the Job_Result's context,
        # or a new root when the envelope carries none.
        with (
            self._tracer.span(
                "manager.ingest_result", parent=message.trace_context, attributes=attributes
            ) as span,
            bind_correlation_id(message.correlation_id) as bound,
        ):
            if not bound:
                warn_missing_correlation(message, _sender(delivery, payload.worker_id))
            elif message.correlation_id != payload.request_id:
                log.warning("result_correlation_mismatch", payload_request_id=payload.request_id)
            rid = payload.request_id
            with bind_job(rid, payload.job_id):
                try:
                    entry_id = await self._store.record_result_inbox(
                        rid, payload, meta=llm_meta(delivery.headers)
                    )
                except Exception as exc:  # State_Store unreachable: redeliver later
                    log.warning("result_ingest_failed", error=type(exc).__name__)
                    span.fail(type(exc).__name__)
                    await self._transport.acknowledge(delivery, Outcome.REQUEUE)
                    return
                if entry_id is None:
                    log.warning("result_for_unknown_request", attempt=payload.attempt)
                else:
                    log.info("result_ingested", attempt=payload.attempt, status=payload.status,
                             entry_id=entry_id)
                await self._transport.acknowledge(delivery, Outcome.ACK)
                if entry_id is not None and self._on_ingested is not None:
                    self._on_ingested(rid)


class DeadLetterIngest:
    """``dlq-events`` -> ``unacked_delivery_exhausted`` inbox events (Req 6.6)."""

    def __init__(
        self,
        transport: Transport,
        store: StateStore,
        on_ingested: OnIngested | None = None,
        destination: str = DLQ_EVENTS_DESTINATION,
    ) -> None:
        self._transport = transport
        self._store = store
        self._on_ingested = on_ingested
        self.destination = destination
        self.subscription: Subscription | None = None

    async def start(self) -> Subscription:
        self.subscription = await self._transport.subscribe(self.destination, self.handle)
        return self.subscription

    async def stop(self) -> None:
        if self.subscription is not None:
            await self.subscription.cancel()

    @staticmethod
    def is_delivery_limit(delivery: Delivery) -> bool:
        headers = delivery.headers
        return headers.get(DEAD_LETTER_REASON_HEADER) == DELIVERY_LIMIT_REASON and headers.get(
            ORIGINAL_DESTINATION_HEADER, ""
        ).startswith(JOBS_DESTINATION_PREFIX)

    async def handle(self, delivery: Delivery) -> None:
        message = delivery.message
        if not self.is_delivery_limit(delivery):
            # Malformed rejects, Manager-published max_attempts copies, etc.: nothing to do.
            await self._transport.acknowledge(delivery, Outcome.ACK)
            return
        if not isinstance(message, JobDispatchMessage):
            # Already a dead letter: drop it rather than reject it again (9.5).
            log.warning("dead_letter_unusable", message_type=getattr(message, "type", None))
            await self._transport.acknowledge(delivery, Outcome.ACK)
            return
        payload = message.payload
        with bind_correlation_id(message.correlation_id) as bound:
            if not bound:
                warn_missing_correlation(message, _sender(delivery))
            rid = payload.request_id
            with bind_job(rid, payload.job_id):
                try:
                    entry_id = await self._store.record_inbox_event(
                        rid,
                        INBOX_UNACKED_EXHAUSTED,
                        {"job_id": payload.job_id, "attempt": payload.attempt},
                    )
                except Exception as exc:
                    log.warning("dead_letter_ingest_failed", error=type(exc).__name__)
                    await self._transport.acknowledge(delivery, Outcome.REQUEUE)
                    return
                log.warning("dispatch_delivery_exhausted", attempt=payload.attempt,
                            recorded=entry_id is not None)
                await self._transport.acknowledge(delivery, Outcome.ACK)
                if entry_id is not None and self._on_ingested is not None:
                    self._on_ingested(rid)


class DeadLetterRecorder:
    """``dlq`` -> Dead_Letter_Record (``create_dead_letter.lua``), then ACK
    (Req 3.8, 12.1-12.3, 12.10, 12.11; design D4, D6).

    Competing consumers across all Managers: the Dead_Letter_ID is derived from the
    Message identity, so a redelivery to any Manager hits ``EXISTS`` and only the
    first write counts in ``das_dlq_records_created_total``. A failed write leaves
    the Message on the queue (``REQUEUE``) so the record is created on redelivery.
    Logs carry only the Dead_Letter_ID, reason, Request_ID and Job_ID (plus the
    message ID on a failed write), never payload, raw bytes or last error.
    """

    def __init__(
        self,
        transport: Transport,
        store: StateStore,
        metrics: ManagerMetrics | None = None,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
        destination: str = DLQ_DESTINATION,
    ) -> None:
        self._transport = transport
        self._store = store
        self._metrics = metrics or ManagerMetrics(NoopRecorder())
        self._max_bytes = max_message_bytes
        self.destination = destination
        self.subscription: Subscription | None = None

    async def start(self) -> Subscription:
        self.subscription = await self._transport.subscribe(self.destination, self.handle)
        return self.subscription

    async def stop(self) -> None:
        if self.subscription is not None:
            await self.subscription.cancel()

    async def handle(self, delivery: Delivery) -> None:
        message = delivery.message
        message_id = None if message is None else message.message_id
        reason = classify_reason(delivery.headers)
        try:
            dlid = dead_letter_id(dead_letter_identity(message, delivery.body))
            fields = build_record(
                dlid,
                reason,
                message,
                delivery.body,
                delivery.headers,
                delivery.parse_error,
                self._max_bytes,
            )
            result = await self._store.create_dead_letter(fields)
        except Exception as exc:  # State_Store unreachable / write failed (Req 12.11)
            log.warning(
                "dead_letter_record_failed",
                reason=reason,
                message_id=message_id,
                error=type(exc).__name__,
            )
            await self._transport.acknowledge(delivery, Outcome.REQUEUE)
            return
        if result.created:
            self._metrics.dlq_record_created(reason)  # Req 3.8: CREATED only
            log.info(
                "dead_letter_recorded",
                dead_letter_id=dlid,
                reason=reason,
                request_id=fields.request_id,
                job_id=fields.job_id,
            )
        else:
            log.debug(
                "dead_letter_record_exists",
                dead_letter_id=dlid,
                reason=reason,
                request_id=fields.request_id,
                job_id=fields.job_id,
            )
        await self._transport.acknowledge(delivery, Outcome.ACK)


class CancelTarget(Protocol):
    def request_cancel(self, rid: str) -> bool: ...


#: Applies an ``agent_control`` command and returns the reply payload (Req 17).
AgentControlCallback = Callable[[AgentControlPayload], Awaitable[AgentControlReplyPayload]]


class ControlCommandHandler:
    """Handles control commands addressed to this Manager's agent ID.

    ``control`` (``cancel_request``) is fire-and-forget. ``agent_control``
    (drain/undrain, Req 17) arrives as a request-reply call: the handler applies it
    through ``on_agent_control`` and returns the ``agent_control_reply`` Message,
    which the RPC_Transport sends back to the caller.
    """

    def __init__(
        self,
        transport: Transport,
        agent_id: str,
        drivers: CancelTarget,
        on_agent_control: AgentControlCallback | None = None,
    ) -> None:
        self._transport = transport
        self.agent_id = agent_id
        self._drivers = drivers
        self._on_agent_control = on_agent_control
        self.subscription: Subscription | None = None

    async def start(self) -> Subscription:
        self.subscription = await self._transport.subscribe(self.agent_id, self.handle)
        return self.subscription

    async def stop(self) -> None:
        if self.subscription is not None:
            await self.subscription.cancel()

    async def handle(self, delivery: Delivery) -> TypedMessage | None:
        message = delivery.message
        if isinstance(message, AgentControlMessage):
            return await self._handle_agent_control(delivery, message)
        if not isinstance(message, ControlMessage):
            log.warning("control_rejected", message_type=getattr(message, "type", None),
                        reason=getattr(delivery.parse_error, "reason", None))
            await self._transport.acknowledge(delivery, Outcome.REJECT)
            return None
        payload = message.payload
        with bind_correlation_id(message.correlation_id or payload.request_id):
            if not message.correlation_id:
                warn_missing_correlation(message, _sender(delivery))
            if payload.command == "cancel_request":
                owned = self._drivers.request_cancel(payload.request_id)
                log.info("cancel_request_received", owned=owned)
            else:
                log.warning("control_command_ignored", command=payload.command)
            await self._transport.acknowledge(delivery, Outcome.ACK)
        return None

    async def _handle_agent_control(
        self, delivery: Delivery, message: AgentControlMessage
    ) -> TypedMessage | None:
        """Drain/undrain (Req 17.4, 17.5, 17.8, 17.9); no reply means caller timeout."""
        payload = message.payload
        await self._transport.acknowledge(delivery, Outcome.ACK)
        if payload.agent_id != self.agent_id:
            log.warning("agent_control_misrouted", target=payload.agent_id)
            return None
        if self._on_agent_control is None:
            log.warning("agent_control_ignored", command=payload.command)
            return None
        reply = await self._on_agent_control(payload)
        log.info(
            "agent_control_applied",
            command=payload.command,
            release_leases=payload.release_leases,
            status=reply.status,
            in_flight=reply.in_flight,
        )
        return new_message(reply, correlation_id=message.correlation_id)


__all__ = [
    "DEAD_LETTER_REASON_HEADER",
    "DELIVERY_LIMIT_REASON",
    "DLQ_DESTINATION",
    "DLQ_EVENTS_DESTINATION",
    "LLM_MODEL_HEADER",
    "LLM_PROVIDER_HEADER",
    "ORIGINAL_DESTINATION_HEADER",
    "RESULTS_DESTINATION",
    "UNKNOWN_LLM",
    "AgentControlCallback",
    "CancelTarget",
    "ControlCommandHandler",
    "DeadLetterIngest",
    "DeadLetterRecorder",
    "OnIngested",
    "ResultIngest",
    "llm_meta",
    "warn_missing_correlation",
]
