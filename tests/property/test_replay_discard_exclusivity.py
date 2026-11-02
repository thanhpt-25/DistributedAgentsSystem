# Feature: das-observability-operations, Property 21: Tính loại trừ của Replay và Discard
"""Property 21: Replay and Discard are mutually exclusive.

For any 2 to 20 Replay and Discard calls interleaved in random order on the same
Pending Dead_Letter_Record (real ``replay_dead_letter.lua`` / ``discard_dead_letter.lua``
on fakeredis[lua] through ``StateStore``, issued concurrently with ``asyncio.gather`` so
Redis serializes the scripts):

- exactly one call is accepted (the record starts Pending and is replayable);
- every other call gets ``record_not_pending``, except a Discard after an accepted
  Discard, which gets the idempotent answer carrying the original ``discarded_at``;
- when a Replay is accepted the inbox holds exactly one ``replay`` event, and after the
  reducer applies the inbox there is at most one ``PublishDispatch`` for the new attempt.

**Validates: Requirements 13.9, 13.12**
"""

from __future__ import annotations

import asyncio
import dataclasses

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, given
from hypothesis import strategies as st

from das.manager.machine import (
    MachineConfig,
    PublishDispatch,
    ReplayRequested,
    RequestSnapshot,
    StepContext,
    step,
)
from das.models import AgentRecord, Lease, RequestStatus
from das.state import INBOX_REPLAY, StateStore, keys
from tests.unit.test_state_phase2_dlq import FINAL, fields, running_with_failed_job

DLID = "e" * 32
AGENTS = (
    AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=frozenset({"research", "summarize"}),
        status="Available",
        last_heartbeat=0.0,
    ),
)

calls = st.lists(st.sampled_from(["replay", "discard"]), min_size=2, max_size=20)


async def _setup(store: StateStore, request_status: str) -> tuple[str, Lease | None]:
    rid, lease = await running_with_failed_job(store)
    if request_status == "Failed":
        assert (await store.fail_request(lease, "job a failed", FINAL)).ok
        await store.client.delete(keys.lease(rid))  # no lease held (Req 13.7)
        lease = None
    assert (await store.create_dead_letter(fields(DLID, rid))).created
    return rid, lease


async def _apply_inbox(store: StateStore, rid: str, lease: Lease | None) -> list[PublishDispatch]:
    """Run the reducer over the inbox like the Manager driver does; returns dispatches."""
    stored = await store.load_snapshot(rid)
    assert stored is not None
    if stored.status is RequestStatus.PENDING:  # reopened by a Replay of a Failed Request
        lease = (await store.acquire_lease(rid, "m2", 3600.0)).lease
        assert lease is not None
        assert (await store.reopen_request(lease)).ok
    assert lease is not None
    dispatches: list[PublishDispatch] = []
    cfg = MachineConfig(max_attempts=3)
    entries = await store.read_inbox(rid, after="0-0")
    # Each entry stepped twice: the second stands for a redelivery of the same event.
    for entry in [e for e in entries for _ in range(2)]:
        if entry.kind != INBOX_REPLAY:
            continue
        data = entry.payload()
        ev = ReplayRequested(
            str(data["dead_letter_id"]), str(data["job_id"]), int(data["attempt"])
        )
        stored = await store.load_snapshot(rid)
        assert stored is not None
        snap = RequestSnapshot.from_stored(stored)
        ctx = StepContext(now=await store.server_time(), config=cfg, agents=AGENTS)
        d = step(snap, ev, ctx)
        dispatches += [e for e in d.effects if isinstance(e, PublishDispatch)]
        updates = dataclasses.replace(d.updates, inbox_cursor=entry.entry_id)
        assert (await store.apply_transition(lease, updates)).ok
    return dispatches


async def _scenario(ops: list[str], request_status: str) -> None:
    store = StateStore(FakeRedis(server=FakeServer(), decode_responses=True))
    rid, lease = await _setup(store, request_status)

    def call(op: str):
        if op == "replay":
            return store.replay_dead_letter(DLID)
        return store.discard_dead_letter(DLID)

    results = await asyncio.gather(*(call(op) for op in ops))

    accepted = [(op, r) for op, r in zip(ops, results, strict=True) if r.outcome == "accepted"]
    assert len(accepted) == 1, results
    winner, win = accepted[0]
    event(f"winner: {winner} on {request_status}")

    record = await store.get_dead_letter(DLID)
    assert record is not None
    for op, r in zip(ops, results, strict=True):
        if r is win:
            continue
        if op == "discard" and winner == "discard":
            assert r.outcome == "already", r
            assert r.discarded_at == win.discarded_at
        else:
            assert r.outcome == "record_not_pending", r
            expected = "Replayed" if winner == "replay" else "Discarded"
            got = r.status if op == "discard" else r.current_state
            assert got == expected, r

    entries = await store.read_inbox(rid, after="0-0")
    replay_events = [e for e in entries if e.kind == INBOX_REPLAY]
    if winner == "replay":
        assert record.status == "Replayed"
        assert len(replay_events) == 1
        assert win.new_attempt == 2
        dispatches = await _apply_inbox(store, rid, lease)
        new = [d for d in dispatches if d.job_id == "a" and d.attempt == win.new_attempt]
        assert len(new) <= 1, dispatches
    else:
        assert record.status == "Discarded"
        assert record.discarded_at == win.discarded_at
        assert replay_events == []


@given(ops=calls, request_status=st.sampled_from(["Running", "Failed"]))
def test_replay_discard_exclusive(ops: list[str], request_status: str) -> None:
    asyncio.run(_scenario(ops, request_status))
