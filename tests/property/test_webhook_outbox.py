# Feature: das-client-api, Property 7: Webhook outbox delivery
"""Property 7: Webhook outbox delivery.

1 to 5 Requests, each with or without a Webhook_Target, are terminated with
``finalize``, ``fail_request`` (Job ``a`` Failed) or ``cancel_request`` (real
``das/state/lua`` scripts through ``StateStore`` on a fresh fakeredis server per example, ``TIME`` from
:class:`FakeRedisTime`). Each terminal write enqueues exactly one pending delivery for
a Request with a target and none for the others.

Then 2 or 3 simulated senders run a step list: ``claim``, ``crash`` (forget the held
claim), ``advance`` Redis time by 0..2*lease, ``reset`` a Failed Request (Replay of a
Dead_Letter_Record for its Failed Job, ``reopen_request``, then ``fail_request`` again:
a second terminal write), or ``complete`` the held claim with a random outcome
(``delivered``, or else ``failed`` once ``attempts + 1 == MAX_ATTEMPTS``
and ``retry`` at ``now + 1`` before that). A model tracks, per delivery, its state,
attempts, claim counter, event_id, due time and the lease expiry of the latest claim.
A reset gives the delivery a new event_id, claim 0, attempts 0 and due now, whatever
its state was; every claim senders held on it is kept as stale and completed right
after the delivery is next claimed (or at the end), so a pre-reset claim whose
counter equals the new claim's must still be fenced by event_id (Req 8.3):

- a claim succeeds only for a pending, due delivery, never while an earlier claim's
  lease is live, and ``None`` only when nothing is due;
- a ``complete_webhook`` with a superseded claim (older counter, or an event_id from
  before a reset) returns ``False`` and changes neither the delivery hash nor the
  outbox.

A well-behaved sender then drains the outbox. Every delivery ends ``delivered`` or
``failed`` with ``1 <= attempts <= MAX_ATTEMPTS``.

**Validates: Requirements 8.2, 8.3, 8.5, 8.6, 8.7**
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, given
from hypothesis import strategies as st

from das.models import FinalResult, JobRecord, JobState
from das.state import ClientRequestSpec, StateStore, TransitionBatch, WebhookClaim, WebhookOutcome, WebhookState, keys
from tests.fixtures.redis_time import FakeRedisTime
from tests.unit.test_state_phase2_dlq import fields as dlq_fields
from tests.unit.test_state_store import PLAN

LEASE = 10.0
MAX_ATTEMPTS = 3
FINAL = FinalResult(summary="done", jobs=[])

requests = st.lists(
    st.tuples(st.booleans(), st.sampled_from(["finalize", "fail", "cancel"])), min_size=1, max_size=5
)
steps = st.lists(
    st.one_of(
        st.tuples(st.integers(0, 2), st.just("claim"), st.none()),
        st.tuples(st.integers(0, 2), st.just("crash"), st.none()),
        st.tuples(st.integers(0, 2), st.just("advance"), st.floats(0, 2 * LEASE)),
        st.tuples(st.integers(0, 2), st.just("complete"), st.booleans()),  # True: delivered
        st.tuples(st.integers(0, 2), st.just("reset"), st.integers(0, 4)),  # Request index
    ),
    max_size=40,
)


@dataclass
class Delivery:
    state: str = "pending"
    attempts: int = 0
    claim: int = 0
    event_id: str = ""
    due: float = 0.0
    lease_until: float | None = None  # lease expiry of the latest claim


class Scenario:
    def __init__(self, store: StateStore, clock: FakeRedisTime) -> None:
        self.store = store
        self.clock = clock
        self.model: dict[str, Delivery] = {}
        self.rids: list[str] = []
        self.failed_attempt: dict[str, int] = {}  # attempt of Job ``a`` of a Failed Request
        self.stale: list[WebhookClaim] = []  # claims held when their delivery was reset
        self.dl_seq = 0

    async def terminate(self, with_target: bool, how: str) -> None:
        spec = ClientRequestSpec(
            fingerprint="aaaaaaaa",
            webhook_url="https://hooks.example.test/das" if with_target else None,
            webhook_secret_name="hook-secret" if with_target else None,
        )
        res = await self.store.submit_request("p", spec)
        assert res.request_id is not None
        rid = res.request_id
        self.rids.append(rid)
        if how == "cancel":
            assert (await self.store.cancel_request(rid)).ok
        else:
            lease = (await self.store.acquire_lease(rid, "m1", 3600.0)).lease
            assert lease is not None
            assert (await self.store.commit_plan(lease, PLAN)).ok
            if how == "fail":
                await self.fail(rid, lease, attempt=1, replays=())
            else:
                assert (await self.store.apply_transition(lease, TransitionBatch())).ok
                assert (await self.store.finalize(lease, FINAL)).ok
        r = self.store.client
        if with_target:
            self.model[rid] = await self.fresh_delivery(rid)
        else:
            assert await self.store.get_webhook(rid) is None
            assert await r.zscore(keys.WEBHOOKS_DUE, rid) is None
        # Exactly one outbox entry per Request with a target, none for the others.
        assert await r.zcard(keys.WEBHOOKS_DUE) == len(self.model)

    async def fail(self, rid: str, lease, attempt: int, replays: tuple[str, ...]) -> None:
        failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=attempt, reason="x")
        batch = TransitionBatch(jobs={"a": failed}, replays_applied=replays)
        assert (await self.store.apply_transition(lease, batch)).ok
        assert (await self.store.fail_request(lease, "boom")).ok
        self.failed_attempt[rid] = attempt

    async def fresh_delivery(self, rid: str) -> Delivery:
        """The model of a delivery a terminal write just (re)created: pending, claim 0,
        attempts 0, due at the Redis time of the write."""
        r = self.store.client
        assert await self.store.get_webhook(rid) == WebhookState("pending", 0, None)
        h = await r.hgetall(keys.webhook(rid))
        assert h["claim"] == "0" and h["event_id"]
        due = await r.zscore(keys.WEBHOOKS_DUE, rid)
        assert due is not None and abs(due - self.clock.now_ms() / 1000) < 1e-6
        return Delivery(due=due, event_id=h["event_id"])

    async def reset(self, rid: str, held: dict[int, WebhookClaim | None]) -> None:
        """A second terminal write on a Failed Request: Replay, reopen, fail again."""
        attempt = self.failed_attempt.get(rid)
        if attempt is None:
            return
        self.dl_seq += 1
        dlid = f"{self.dl_seq:032x}"
        assert (await self.store.create_dead_letter(dlq_fields(dlid, rid, attempt=attempt))).created
        assert (await self.store.replay_dead_letter(dlid)).outcome == "accepted"
        lease = (await self.store.acquire_lease(rid, "m1", 3600.0)).lease
        assert lease is not None
        assert (await self.store.reopen_request(lease)).ok
        await self.fail(rid, lease, attempt=attempt + 1, replays=(dlid,))
        if rid not in self.model:
            assert await self.store.get_webhook(rid) is None
            return
        old = self.model[rid]
        new = await self.fresh_delivery(rid)
        assert new.event_id != old.event_id
        self.model[rid] = new
        for sender, c in list(held.items()):
            if c is not None and c.request_id == rid:
                self.stale.append(c)
                held[sender] = None
        assert await self.store.client.zcard(keys.WEBHOOKS_DUE) == sum(
            1 for d in self.model.values() if d.state == "pending"
        )
        event(f"reset a {old.state} delivery")

    async def complete_stale(self, rid: str | None = None) -> None:
        """Complete the kept pre-reset claims (of ``rid``, or all): each must be fenced."""
        for old in [x for x in self.stale if rid is None or x.request_id == rid]:
            self.stale.remove(old)
            d = self.model[old.request_id]
            if d.claim == old.claim and d.state == "pending":
                event("stale: pre-reset claim with the current counter")
            await self.complete(old, delivered=True, expect_stale=True)

    async def claim(self) -> WebhookClaim | None:
        now = self.clock.now()
        due = {rid for rid, d in self.model.items() if d.state == "pending" and d.due <= now}
        c = await self.store.claim_webhook(LEASE)
        if c is None:
            assert not due, due
            return None
        d = self.model[c.request_id]
        assert c.request_id in due
        assert d.lease_until is None or d.lease_until <= now, "claimed during a live lease"
        assert c.claim == d.claim + 1 and c.attempts == d.attempts
        assert c.event_id == d.event_id
        d.claim, d.due, d.lease_until = c.claim, now + LEASE, now + LEASE
        await self.complete_stale(c.request_id)
        return c

    async def snapshot(self, rid: str) -> tuple[dict[str, str], list[tuple[str, float]]]:
        r = self.store.client
        return await r.hgetall(keys.webhook(rid)), await r.zrange(keys.WEBHOOKS_DUE, 0, -1, withscores=True)

    async def complete(self, c: WebhookClaim, delivered: bool, expect_stale: bool = False) -> None:
        d = self.model[c.request_id]
        outcome: WebhookOutcome
        next_due: float | None
        if delivered:
            outcome, next_due = "delivered", None
        elif c.attempts + 1 == MAX_ATTEMPTS:
            outcome, next_due = "failed", None
        else:
            outcome, next_due = "retry", self.clock.now() + 1
        stale = d.state != "pending" or d.claim != c.claim or d.event_id != c.event_id
        assert stale or not expect_stale
        before = await self.snapshot(c.request_id)
        ok = await self.store.complete_webhook(c, outcome, "200" if delivered else "503", next_due)
        assert ok is (not stale)
        if stale:
            if d.event_id != c.event_id:
                event("stale: claim from before a reset")
            elif d.claim != c.claim:
                event("stale: superseded claim")
            else:
                event("stale: no longer pending")
            assert await self.snapshot(c.request_id) == before
            return
        d.attempts += 1
        if outcome == "retry":
            assert next_due is not None
            d.due = next_due
        else:
            d.state = outcome
        d.lease_until = None

    async def drain(self, late: list[WebhookClaim]) -> None:
        """Claim and complete until the outbox is empty. ``late`` holds claims senders
        still had when the steps ended; each is completed (by a slow sender) right
        after the drain sender re-claims its delivery, so it must be fenced."""
        r = self.store.client
        for _ in range(10 * MAX_ATTEMPTS * max(1, len(self.model))):
            c = await self.claim()
            if c is not None:
                for old in [x for x in late if x.request_id == c.request_id]:
                    late.remove(old)
                    await self.complete(old, delivered=True)
                await self.complete(c, delivered=False)
                continue
            first = await r.zrange(keys.WEBHOOKS_DUE, 0, 0, withscores=True)
            if not first:
                return
            self.clock.advance(max(0.0, first[0][1] - self.clock.now()) + 0.001)
        raise AssertionError("outbox did not drain")


async def _run(
    reqs: list[tuple[bool, str]], senders: int, step_list: list[tuple], clock: FakeRedisTime
) -> None:
    sc = Scenario(StateStore(FakeRedis(server=FakeServer(), decode_responses=True)), clock)
    for with_target, how in reqs:
        await sc.terminate(with_target, how)
    held: dict[int, WebhookClaim | None] = {}
    for pick, action, arg in step_list:
        sender = pick % senders
        if action == "claim":
            c = await sc.claim()
            if c is not None:
                held[sender] = c
        elif action == "crash":
            held[sender] = None
        elif action == "advance":
            clock.advance(arg)
        elif action == "reset":
            # The sender's held delivery when it can be reset (so pre-reset claims
            # are common), else the ``arg``-th Failed Request.
            c = held.get(sender)
            failed = [rid for rid in sc.rids if rid in sc.failed_attempt]
            if c is not None and c.request_id in sc.failed_attempt:
                await sc.reset(c.request_id, held)
            elif failed:
                await sc.reset(failed[arg % len(failed)], held)
        elif (c := held.get(sender)) is not None:
            await sc.complete(c, arg)
            held[sender] = None
    await sc.drain([c for c in held.values() if c is not None])
    await sc.complete_stale()
    for rid, d in sc.model.items():
        got = await sc.store.get_webhook(rid)
        assert got is not None and got.state in {"delivered", "failed"}, (rid, got)
        assert got.state == d.state and got.attempts == d.attempts
        assert 1 <= got.attempts <= MAX_ATTEMPTS
        event(f"ended {got.state}")
    assert await sc.store.client.zcard(keys.WEBHOOKS_DUE) == 0


@given(reqs=requests, senders=st.integers(2, 3), step_list=steps)
def test_webhook_outbox_delivery(reqs: list[tuple[bool, str]], senders: int, step_list: list[tuple]) -> None:
    with FakeRedisTime() as clock:
        asyncio.run(_run(reqs, senders, step_list, clock))
