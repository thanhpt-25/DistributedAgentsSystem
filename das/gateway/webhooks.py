"""Webhook_Sender: delivers a Request's Terminal_Event to its Webhook_Target (Phase 4 Req 8.3-8.8, 8.10).

Each worker loops: claim a due delivery (an atomic Lua script that leases it to this worker), sign and
POST the canonical body, then record the outcome. Everything that decides *when* uses Redis time: the
claim carries ``claimed_at`` and a retry is scheduled from it, so a skewed Gateway clock cannot move a
retry. The Gateway clock only stamps ``DAS-Webhook-Timestamp``, which receivers compare to their own.

At-least-once: a worker that dies after the POST leaves its claim to lapse and the delivery is sent
again, so receivers dedupe on ``DAS-Webhook-Id``. Secrets and signatures are never logged.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Final

import httpx
from pydantic import SecretStr

from das.client.webhook import next_due, sign, webhook_body
from das.clock import Clock
from das.config.schema import WebhookConfig
from das.observability import get_logger
from das.observability.metrics import GatewayMetrics
from das.state.store import StateStore, WebhookClaim, WebhookOutcome

log = get_logger(__name__)

#: A claim is held for ``webhook.timeout`` plus this margin (Req 8.3).
CLAIM_MARGIN: Final = 5.0
#: At most this much of a response body is read; the status decides the outcome (Req 8.8).
MAX_RESPONSE_BYTES: Final = 64 * 1024
#: Longest idle wait of a worker, and the pause after an error (design: Webhook_Sender).
IDLE_CAP: Final = 1.0
#: Floor for the idle wait, so a delivery that is due but not claimable cannot make a busy loop.
IDLE_FLOOR: Final = 0.05
ERROR_PAUSE: Final = 1.0


class WebhookSender:
    """Claims, sends and settles Webhook_Deliveries; shared by the workers of one Gateway."""

    def __init__(
        self,
        *,
        store: StateStore,
        client: httpx.AsyncClient,
        secrets: Mapping[str, SecretStr],
        config: WebhookConfig,
        clock: Clock,
        metrics: Callable[[], GatewayMetrics],
        rand: Callable[[], float] = random.random,
    ) -> None:
        self._store = store
        self._client = client
        self._secrets = secrets
        self._config = config
        self._clock = clock
        self._metrics = metrics
        self._rand = rand

    async def deliver_one(self) -> bool:
        """Claim and settle one due delivery; ``False`` when none could be claimed."""
        claim = await self._store.claim_webhook(self._config.timeout + CLAIM_MARGIN)
        if claim is None:
            return False
        outcome, last_status, due = await self._settle(claim)
        if await self._store.complete_webhook(claim, outcome, last_status, due):
            self._metrics().webhook_delivery(outcome)  # a stale claim changed nothing: not counted
        return True

    async def _settle(self, claim: WebhookClaim) -> tuple[WebhookOutcome, str, float | None]:
        """The outcome of one attempt, its ``last_status`` and, for a retry, the next due time (Req 8.5-8.8)."""
        # The secret is looked up now, so removing it from the config stops deliveries (Req 8.8).
        secret = self._secrets.get(claim.secret_name)
        if secret is None:
            log.warning("webhook secret is not configured", secret_name=claim.secret_name)
            return "failed", "secret_missing", None
        delivered, last_status = await self._post(claim, secret.get_secret_value().encode("utf-8"))
        if delivered:
            return "delivered", last_status, None
        attempts = claim.attempts + 1
        if attempts >= self._config.max_attempts:
            return "failed", last_status, None
        due = next_due(
            claim.claimed_at, attempts, self._config.base_backoff, self._config.max_backoff, self._rand()
        )
        return "retry", last_status, due

    async def _post(self, claim: WebhookClaim, secret: bytes) -> tuple[bool, str]:
        """POST the signed body; returns (2xx?, ``last_status``): an HTTP status, ``timeout`` or ``connect_error``."""
        body = webhook_body(claim.request_id, claim.event_id, claim.event_type, claim.data)
        timestamp = int(self._clock.now())
        headers = {
            "Content-Type": "application/json",
            "DAS-Webhook-Id": f"{claim.request_id}:{claim.event_id}",
            "DAS-Webhook-Timestamp": str(timestamp),
            "DAS-Webhook-Signature": sign(secret, timestamp, body),
        }
        try:
            async with asyncio.timeout(self._config.timeout):
                async with self._client.stream("POST", claim.url, content=body, headers=headers) as response:
                    status = response.status_code
                    # Redirects are not followed (the client is built with follow_redirects=False), so
                    # a 3xx is a failure like any non-2xx.
                    with contextlib.suppress(httpx.HTTPError):
                        await _discard_body(response)
        except (httpx.TimeoutException, TimeoutError):
            return False, "timeout"
        except (httpx.HTTPError, httpx.InvalidURL):
            return False, "connect_error"
        return 200 <= status < 300, str(status)

    async def run(self, stop: asyncio.Event) -> None:
        """One worker: deliver while there is work, otherwise wait for the next due time, until ``stop``.

        A send that has started finishes first; an unfinished claim would lapse and be retried anyway.
        """
        while not stop.is_set():
            try:
                if await self.deliver_one():
                    continue
                pause = await self._idle_wait()
            except Exception as exc:  # one bad delivery or a Redis blip must not end the worker
                log.warning("webhook sender iteration failed", error=type(exc).__name__)
                pause = ERROR_PAUSE
            await self._pause(stop, pause)

    async def _idle_wait(self) -> float:
        """``min(1 s, time to the next due delivery)`` in Redis time, never below a small floor."""
        due = await self._store.next_webhook_due()
        if due is None:
            return IDLE_CAP
        return max(IDLE_FLOOR, min(IDLE_CAP, due - await self._store.server_time()))

    async def _pause(self, stop: asyncio.Event, seconds: float) -> None:
        """Sleep on the injected clock, but wake at once when ``stop`` is set."""
        sleeper = asyncio.ensure_future(self._clock.sleep(seconds))
        stopper = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, stopper):
                task.cancel()
            await asyncio.gather(sleeper, stopper, return_exceptions=True)


async def _discard_body(response: httpx.Response) -> None:
    read = 0
    async for chunk in response.aiter_bytes():
        read += len(chunk)
        if read >= MAX_RESPONSE_BYTES:
            return


@asynccontextmanager
async def webhook_senders(
    *,
    store: StateStore,
    secrets: Mapping[str, SecretStr],
    config: WebhookConfig,
    clock: Clock,
    metrics: Callable[[], GatewayMetrics],
) -> AsyncIterator[None]:
    """Run ``config.workers`` sender tasks for the lifetime of the block (Req 8.3, 8.6).

    ``workers == 0`` runs none. On exit the workers are asked to stop and given the time a send can
    take; any that are still busy are cancelled, and their claims lapse and are retried.
    """
    if config.workers == 0:
        yield
        return
    stop = asyncio.Event()
    async with httpx.AsyncClient(follow_redirects=False, timeout=config.timeout) as client:
        sender = WebhookSender(
            store=store, client=client, secrets=secrets, config=config, clock=clock, metrics=metrics
        )
        tasks = [
            asyncio.create_task(sender.run(stop), name=f"das-webhook-sender-{i}")
            for i in range(config.workers)
        ]
        try:
            yield
        finally:
            stop.set()
            _, pending = await asyncio.wait(tasks, timeout=config.timeout + CLAIM_MARGIN)
            for task in pending:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


__all__ = ["MAX_RESPONSE_BYTES", "WebhookSender", "webhook_senders"]
