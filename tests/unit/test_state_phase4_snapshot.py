"""``StateStore.load_snapshot(include_client=True)``: webhook state and last Event_ID in the same
MULTI as the Request (das-client-api task 7.3; Phase 4 Req 5.9, 8.9)."""

from __future__ import annotations

from typing import Any

from das.client.budget import Budget
from das.state import StateStore, WebhookState, keys
from tests.unit.test_state_phase4_events import (
    PLAN,
    events,
    leased,
    running,
    submitted,
    usage_rec,
)


async def last_event_id(store: StateStore, rid: str) -> str:
    entries = await events(store, rid)
    return entries[-1][0]


async def test_default_snapshot_does_not_read_client_fields(state_store: StateStore):
    rid = await submitted(state_store, webhook=True)
    assert (await state_store.cancel_request(rid)).ok
    snap = await state_store.load_snapshot(rid)
    assert snap is not None
    assert snap.webhook is None and snap.last_event_id is None


async def test_include_client_reads_webhook_state_and_last_event_id(state_store: StateStore):
    rid = await submitted(state_store, webhook=True)
    snap = await state_store.load_snapshot(rid, include_client=True)
    assert snap is not None
    assert snap.webhook is None  # a delivery exists only after a terminal write (Req 8.2)
    assert snap.last_event_id == await last_event_id(state_store, rid)

    assert (await state_store.cancel_request(rid)).ok
    snap = await state_store.load_snapshot(rid, include_client=True)
    assert snap is not None
    assert snap.webhook == WebhookState(state="pending", attempts=0, last_status=None)
    assert snap.last_event_id == await last_event_id(state_store, rid)
    assert (await events(state_store, rid))[-1][1] == "request.cancelled"


async def test_request_without_webhook_target_has_no_delivery(state_store: StateStore):
    rid = await submitted(state_store)
    assert (await state_store.cancel_request(rid)).ok
    snap = await state_store.load_snapshot(rid, include_client=True)
    assert snap is not None and snap.webhook is None


async def test_request_created_before_phase4_has_no_last_event_id(state_store: StateStore):
    """Req 11.2: a Request without an Event_Stream yields ``None`` rather than an error."""
    rid = await submitted(state_store)
    await state_store.client.delete(keys.events(rid))
    snap = await state_store.load_snapshot(rid, include_client=True)
    assert snap is not None and snap.last_event_id is None


async def test_include_client_keeps_transitions_and_usage_intact(state_store: StateStore):
    """The usage list is read by position, so extra commands must not shift it."""
    rid = await submitted(state_store, budget=Budget(max_total_tokens=100))
    lease = await leased(state_store, rid)
    assert (await state_store.commit_plan(lease, PLAN, usage=[usage_rec(3, 4)])).ok
    snap = await state_store.load_snapshot(
        rid, include_transitions=True, include_usage=True, include_client=True
    )
    assert snap is not None
    assert snap.usage is not None and [u.usage.total_tokens for u in snap.usage] == [7]
    assert snap.transitions is not None and len(snap.transitions) == 2
    assert snap.budget == Budget(max_total_tokens=100)
    assert snap.last_event_id == await last_event_id(state_store, rid)


async def test_client_reads_share_one_multi_with_the_snapshot(state_store: StateStore, monkeypatch):
    rid, _ = await running(state_store, webhook=True)
    opened: list[Any] = []
    real = state_store.client.pipeline

    def counting(*args: Any, **kwargs: Any) -> Any:
        opened.append(kwargs.get("transaction", True))
        return real(*args, **kwargs)

    monkeypatch.setattr(state_store.client, "pipeline", counting)
    snap = await state_store.load_snapshot(rid, include_usage=True, include_client=True)
    assert snap is not None
    assert opened == [True]  # one transactional pipeline, not a read per field
