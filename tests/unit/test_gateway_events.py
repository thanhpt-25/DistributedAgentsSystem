"""SSE endpoint ``GET /v1/requests/{id}/events`` (das-client-api tasks 7.4, 7.5; Phase 4 Req 6.2,
7.1-7.9). httpx cannot hold a response open, so the live loop is driven through ``event_frames``
with a scripted source and a FakeClock; the HTTP tests use Requests whose stream closes."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from das.clock import FakeClock
from das.config.schema import ClientLimitsConfig, ClientSettingsConfig, EventsConfig
from das.gateway.app import create_app
from das.gateway.events import (
    KEEPALIVE_FRAME,
    READ_COUNT,
    StreamGate,
    event_frame,
    event_frames,
    parse_stream_id,
    snapshot_frame,
)
from das.models import RequestStatus
from das.registry import AgentRegistry
from das.state import StateStore, keys
from tests.unit.test_gateway_limits import Recorders, fp
from tests.unit.test_state_phase4_events import INSTR_A, INSTR_B, PROMPT, events, running, submitted

KEY = "client-key-0001"
AUTH = {"X-API-Key": KEY}
LAST_EVENT_ID = "Last-Event-ID"


# --------------------------------------------------------------------------- SSE parsing


def parse_sse(text: str) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for block in text.split("\n\n"):
        if not block:
            continue
        frame: dict[str, Any] = {"comment": None, "event": None, "id": None, "data": None}
        for line in block.split("\n"):
            if line.startswith(":"):
                frame["comment"] = line[1:].strip()
            elif line.startswith("event: "):
                frame["event"] = line[len("event: "):]
            elif line.startswith("id: "):
                frame["id"] = line[len("id: "):]
            elif line.startswith("data: "):
                frame["data"] = json.loads(line[len("data: "):])
        frames.append(frame)
    return frames


# --------------------------------------------------------------------------- pure helpers


@pytest.mark.parametrize("value", ["0-0", "1-0", "1700000000000-12", "5-99999"])
def test_valid_stream_ids_parse(value):
    ms, seq = value.split("-")
    assert parse_stream_id(value) == (int(ms), int(seq))


@pytest.mark.parametrize(
    "value", ["", "abc", "1", "1-", "-1", "1-2-3", "a-b", "1-+2", " 1-2", "1-2 ", "1.5-2", "é-1"]
)
def test_invalid_stream_ids_do_not_parse(value):
    assert parse_stream_id(value) is None


def test_stream_ids_compare_like_redis():
    assert parse_stream_id("9-0") < parse_stream_id("10-0")  # numeric, not lexicographic
    assert parse_stream_id("5-2") < parse_stream_id("5-10")


def test_event_frame_carries_type_id_and_the_envelope():
    frame = event_frame("rid-1", "17-3", {"type": "job.transition", "data": json.dumps({"job_id": "a"})})
    assert frame is not None and frame.endswith("\n\n")
    [parsed] = parse_sse(frame)
    assert parsed["event"] == "job.transition" and parsed["id"] == "17-3"
    assert parsed["data"] == {
        "request_id": "rid-1", "event_id": "17-3", "type": "job.transition", "data": {"job_id": "a"}
    }


@pytest.mark.parametrize(
    "fields",
    [{"data": "{}"}, {"type": "x"}, {"type": "x", "data": "{not json"}, {"type": "x", "data": "[1]"}, {}],
)
def test_malformed_entries_are_skipped(fields):
    assert event_frame("rid-1", "1-0", fields) is None


def test_snapshot_frame_has_id_only_when_an_event_exists():
    with_id = parse_sse(snapshot_frame({"request_id": "r", "status": "Running"}, "9-1", resync=False))[0]
    assert with_id["event"] == "snapshot" and with_id["id"] == "9-1"
    assert with_id["data"] == {"request_id": "r", "status": "Running", "event_id": "9-1"}
    without = parse_sse(snapshot_frame({"request_id": "r"}, None, resync=False))[0]
    assert without["id"] is None and without["data"]["event_id"] is None


def test_snapshot_frame_marks_resync_only_when_true():
    assert "resync" not in parse_sse(snapshot_frame({}, "1-0", resync=False))[0]["data"]
    assert parse_sse(snapshot_frame({}, "1-0", resync=True))[0]["data"]["resync"] is True


# --------------------------------------------------------------------------- the frame loop


def fields(event_type: str, **data: Any) -> dict[str, str]:
    return {"type": event_type, "data": json.dumps(data)}


class ScriptedSource:
    """Replies to successive reads from ``batches``; a blocking read that finds nothing takes its block time."""

    def __init__(self, clock: FakeClock, batches=(), statuses=(RequestStatus.RUNNING,)):
        self.clock = clock
        self.batches = list(batches)
        self.statuses = list(statuses)
        self.reads: list[tuple[str, int | None]] = []
        self.status_calls = 0
        self.renews = 0

    async def read(self, after: str, block_ms: int | None):
        self.reads.append((after, block_ms))
        if self.batches:
            return self.batches.pop(0)
        if block_ms:
            await self.clock.sleep(block_ms / 1000)
        return []

    async def status(self):
        self.status_calls += 1
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]

    async def renew(self):
        self.renews += 1


async def never() -> bool:
    return False


def frames_of(
    source, clock, *, drain_only=False, keepalive=15.0, renew_every=1000.0, disconnected=never, cursor="1-0"
):
    return event_frames(
        request_id="rid-1", snapshot="SNAPSHOT", cursor=cursor, drain_only=drain_only, source=source,
        clock=clock, keepalive=keepalive, renew_every=renew_every, is_disconnected=disconnected,
    )


async def collect(gen: AsyncIterator[str]) -> list[str]:
    return [f async for f in gen]


async def take(gen: AsyncIterator[str], n: int) -> list[str]:
    out = []
    async for frame in gen:
        out.append(frame)
        if len(out) == n:
            break
    await gen.aclose()  # type: ignore[attr-defined]
    return out


async def test_snapshot_is_the_first_frame_before_any_read():
    clock = FakeClock()
    source = ScriptedSource(clock)
    gen = frames_of(source, clock)
    assert await gen.__anext__() == "SNAPSHOT"
    assert source.reads == []
    await gen.aclose()


async def test_events_follow_in_order_and_the_stream_closes_after_the_terminal_event():
    clock = FakeClock()
    batch = [
        ("2-0", fields("job.transition", job_id="a")),
        ("3-0", fields("request.completed", status="Completed")),
    ]
    source = ScriptedSource(clock, [batch], statuses=[RequestStatus.COMPLETED])
    out = await collect(frames_of(source, clock))
    assert out[0] == "SNAPSHOT"
    assert [parse_sse(f)[0]["id"] for f in out[1:]] == ["2-0", "3-0"]
    assert source.reads == [("1-0", 2000)]  # blocks for 2 s and starts after the cursor


async def test_cursor_advances_to_the_last_event_read():
    clock = FakeClock()
    source = ScriptedSource(
        clock,
        [
            [("2-0", fields("job.transition"))],
            [("3-0", fields("job.transition"))],
            [("4-0", fields("request.failed"))],
        ],
        statuses=[RequestStatus.FAILED],
    )
    await collect(frames_of(source, clock))
    assert [r[0] for r in source.reads] == ["1-0", "2-0", "3-0"]


async def test_a_terminal_event_of_a_reopened_request_does_not_close_the_stream():
    """Req 6.4: after Replay the Request is Pending again and ``request.reopened`` follows."""
    clock = FakeClock()
    source = ScriptedSource(
        clock,
        [
            [("2-0", fields("request.failed"))],
            [("3-0", fields("request.reopened"))],
            [("4-0", fields("request.completed"))],
        ],
        statuses=[RequestStatus.PENDING, RequestStatus.COMPLETED],
    )
    out = await collect(frames_of(source, clock))
    kinds = [parse_sse(f)[0]["event"] for f in out[1:]]
    assert kinds == ["request.failed", "request.reopened", "request.completed"]


async def test_a_full_batch_is_drained_before_the_stream_may_close():
    clock = FakeClock()
    full = [(f"{i}-0", fields("job.transition")) for i in range(2, 2 + READ_COUNT - 1)]
    full.insert(0, ("1-5", fields("request.failed")))  # terminal first, more events behind it
    tail = [("900-0", fields("request.reopened")), ("901-0", fields("request.cancelled"))]
    source = ScriptedSource(clock, [full, tail], statuses=[RequestStatus.CANCELLED])
    assert len(full) == READ_COUNT
    out = await collect(frames_of(source, clock))
    assert len(out) == 1 + READ_COUNT + 2
    assert source.status_calls == 1  # not consulted after the full batch


async def test_keepalive_after_15_seconds_without_an_event_and_not_before():
    clock = FakeClock()
    start = clock.now()
    source = ScriptedSource(clock)
    out = await take(frames_of(source, clock), 3)
    assert out == ["SNAPSHOT", KEEPALIVE_FRAME, KEEPALIVE_FRAME]
    assert clock.now() - start >= 30  # 15 s per keepalive, in 2 s blocks
    assert clock.now() - start < 34


async def test_an_event_resets_the_keepalive_timer():
    clock = FakeClock()
    source = ScriptedSource(clock)
    gen = frames_of(source, clock)
    await gen.__anext__()  # snapshot
    clock.advance(10)
    source.batches.append([("2-0", fields("job.transition"))])
    assert parse_sse(await gen.__anext__())[0]["event"] == "job.transition"
    t = clock.now()
    assert await gen.__anext__() == KEEPALIVE_FRAME
    assert clock.now() - t >= 14  # counted from the event, not from the snapshot
    await gen.aclose()


async def test_a_short_keepalive_shortens_the_blocking_read():
    clock = FakeClock()
    source = ScriptedSource(clock)
    await take(frames_of(source, clock, keepalive=0.5), 2)
    assert source.reads[0][1] == 500


async def test_the_stream_closes_at_a_keepalive_tick_when_the_request_is_gone():
    clock = FakeClock()
    source = ScriptedSource(clock, statuses=[None])  # type: ignore[list-item]
    assert await collect(frames_of(source, clock)) == ["SNAPSHOT"]


async def test_the_stream_closes_at_a_keepalive_tick_when_the_request_is_terminal():
    clock = FakeClock()
    source = ScriptedSource(clock, statuses=[RequestStatus.COMPLETED])
    assert await collect(frames_of(source, clock)) == ["SNAPSHOT"]


async def test_drain_only_sends_every_page_without_blocking_then_closes():
    clock = FakeClock()
    pages = [
        [(f"{i}-0", fields("job.transition")) for i in range(2, 2 + READ_COUNT)],
        [("900-0", fields("request.completed"))],
    ]
    source = ScriptedSource(clock, pages)
    out = await collect(frames_of(source, clock, drain_only=True))
    assert len(out) == 1 + READ_COUNT + 1
    assert all(block is None for _, block in source.reads)
    assert source.status_calls == 0


async def test_malformed_entries_are_skipped_but_the_rest_is_delivered():
    clock = FakeClock()
    batch = [("2-0", {"type": "x", "data": "{bad"}), ("3-0", fields("request.completed"))]
    source = ScriptedSource(clock, [batch], statuses=[RequestStatus.COMPLETED])
    out = await collect(frames_of(source, clock))
    assert [parse_sse(f)[0]["id"] for f in out[1:]] == ["3-0"]


async def test_a_disconnected_client_ends_the_stream():
    clock = FakeClock()
    source = ScriptedSource(clock, [[("2-0", fields("job.transition"))]])

    async def gone() -> bool:
        return True

    assert await collect(frames_of(source, clock, disconnected=gone)) == ["SNAPSHOT"]
    assert source.reads == []


async def test_the_slot_is_renewed_periodically():
    clock = FakeClock()
    source = ScriptedSource(clock)
    await take(frames_of(source, clock, renew_every=10.0), 3)  # about 30 s of waiting
    assert source.renews >= 2


# --------------------------------------------------------------------------- the stream gate


def test_gate_refuses_beyond_its_limit_and_counts_releases():
    gate = StreamGate(2)
    assert gate.try_acquire() and gate.try_acquire()
    assert not gate.try_acquire()
    assert gate.open == 2
    gate.release()
    assert gate.open == 1 and gate.try_acquire()


def test_gate_never_goes_below_zero():
    gate = StreamGate(1)
    gate.release()
    assert gate.open == 0


# --------------------------------------------------------------------------- HTTP


def settings(**fields_: Any) -> ClientLimitsConfig:
    base: dict[str, Any] = {"rate": 0.0, "max_streams": 10}
    return ClientLimitsConfig(default=ClientSettingsConfig(**{**base, **fields_}))


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


def make_app(state_store, fake_redis, recorders, limits=None, events_config=None):
    return create_app(
        state_store, AgentRegistry(fake_redis), None, [KEY], 50,
        client_limits=limits or settings(), events=events_config, metrics_factory=recorders,
    )


def http(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


async def cancelled_request(store: StateStore) -> tuple[str, list[tuple[str, str, dict]]]:
    rid, _ = await running(store)
    assert (await store.cancel_request(rid)).ok
    return rid, await events(store, rid)


async def stream(app, rid: str, **headers: str) -> httpx.Response:
    async with http(app) as c:
        return await c.get(f"/v1/requests/{rid}/events", headers={**AUTH, **headers})


async def test_response_headers(state_store, fake_redis, fake_redis_time, recorders):
    rid, _ = await cancelled_request(state_store)
    r = await stream(make_app(state_store, fake_redis, recorders), rid)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    assert r.headers["cache-control"] == "no-cache"
    assert r.headers["x-accel-buffering"] == "no"


async def test_snapshot_comes_first_and_is_the_get_view_without_final_result(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, stored = await cancelled_request(state_store)
    frames = parse_sse((await stream(make_app(state_store, fake_redis, recorders), rid)).text)
    snap = frames[0]
    assert snap["event"] == "snapshot" and snap["id"] == stored[-1][0]
    data = snap["data"]
    assert data["request_id"] == rid and data["status"] == "Cancelled"
    assert data["event_id"] == stored[-1][0]
    assert {"jobs", "transitions", "token_usage", "budget", "webhook"} <= set(data)
    assert "final_result" not in data and "resync" not in data
    assert len(frames) == 1  # a terminal Request with no client position: nothing after the snapshot


async def test_resume_sends_only_events_after_last_event_id_then_closes(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, stored = await cancelled_request(state_store)
    app = make_app(state_store, fake_redis, recorders)
    frames = parse_sse((await stream(app, rid, **{LAST_EVENT_ID: stored[2][0]})).text)
    assert frames[0]["event"] == "snapshot" and "resync" not in frames[0]["data"]
    assert [(f["id"], f["event"]) for f in frames[1:]] == [(sid, t) for sid, t, _ in stored[3:]]
    assert frames[-1]["event"] == "request.cancelled"
    for frame, (sid, t, data) in zip(frames[1:], stored[3:], strict=True):
        assert frame["data"] == {"request_id": rid, "event_id": sid, "type": t, "data": data}


async def test_resume_from_the_last_event_sends_just_the_snapshot(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, stored = await cancelled_request(state_store)
    app = make_app(state_store, fake_redis, recorders)
    frames = parse_sse((await stream(app, rid, **{LAST_EVENT_ID: stored[-1][0]})).text)
    assert [f["event"] for f in frames] == ["snapshot"]


async def test_a_position_beyond_the_stream_is_not_a_resync(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, stored = await cancelled_request(state_store)
    ms = int(stored[-1][0].split("-")[0]) + 10_000
    app = make_app(state_store, fake_redis, recorders)
    frames = parse_sse((await stream(app, rid, **{LAST_EVENT_ID: f"{ms}-0"})).text)
    assert [f["event"] for f in frames] == ["snapshot"] and "resync" not in frames[0]["data"]


@pytest.mark.parametrize("value", ["abc", "1-", "-1", "1-2-3", "x-y"])
async def test_an_invalid_last_event_id_resyncs(state_store, fake_redis, fake_redis_time, recorders, value):
    rid, stored = await cancelled_request(state_store)
    app = make_app(state_store, fake_redis, recorders)
    frames = parse_sse((await stream(app, rid, **{LAST_EVENT_ID: value})).text)
    assert [f["event"] for f in frames] == ["snapshot"]  # continues after the snapshot's Event_ID
    assert frames[0]["data"]["resync"] is True and frames[0]["id"] == stored[-1][0]


async def test_a_last_event_id_older_than_the_first_retained_event_resyncs(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, stored = await cancelled_request(state_store)
    # The first three are gone, as after a trim (fakeredis' XTRIM MINID removes nothing).
    await state_store.client.xdel(keys.events(rid), *(sid for sid, _, _ in stored[:3]))
    app = make_app(state_store, fake_redis, recorders)
    frames = parse_sse((await stream(app, rid, **{LAST_EVENT_ID: stored[1][0]})).text)
    assert frames[0]["data"]["resync"] is True and len(frames) == 1
    # A position that is still retained is served normally.
    frames = parse_sse((await stream(app, rid, **{LAST_EVENT_ID: stored[3][0]})).text)
    assert "resync" not in frames[0]["data"]
    assert [f["id"] for f in frames[1:]] == [sid for sid, _, _ in stored[4:]]


async def test_an_empty_last_event_id_is_treated_as_absent(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, _ = await cancelled_request(state_store)
    app = make_app(state_store, fake_redis, recorders)
    frames = parse_sse((await stream(app, rid, **{LAST_EVENT_ID: ""})).text)
    assert "resync" not in frames[0]["data"]


async def test_a_request_without_an_event_stream_gets_a_snapshot_and_closes(
    state_store, fake_redis, fake_redis_time, recorders
):
    """Req 11.2: a Request created before the upgrade has no events."""
    rid = await submitted(state_store)
    assert (await state_store.cancel_request(rid)).ok
    await state_store.client.delete(keys.events(rid))
    frames = parse_sse((await stream(make_app(state_store, fake_redis, recorders), rid)).text)
    assert [f["event"] for f in frames] == ["snapshot"]
    assert frames[0]["id"] is None and frames[0]["data"]["event_id"] is None


async def test_no_frame_contains_the_prompt_or_job_instructions(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, stored = await cancelled_request(state_store)
    app = make_app(state_store, fake_redis, recorders)
    body = (await stream(app, rid, **{LAST_EVENT_ID: stored[0][0]})).text
    assert stored[0][1] == "request.created" and "request.planned" in body
    for secret in (PROMPT, INSTR_A, INSTR_B):
        assert secret not in body  # Req 6.2


async def test_unknown_request_is_404_and_takes_no_slot(state_store, fake_redis, fake_redis_time, recorders):
    app = make_app(state_store, fake_redis, recorders)
    r = await stream(app, "nope")
    assert (r.status_code, r.json()) == (404, {"error": "not_found"})
    assert await fake_redis.zcard(keys.client_streams(fp(KEY))) == 0
    assert app.state.event_gate.open == 0


async def test_authentication_is_required(state_store, fake_redis, fake_redis_time, recorders):
    rid, _ = await cancelled_request(state_store)
    async with http(make_app(state_store, fake_redis, recorders)) as c:
        r = await c.get(f"/v1/requests/{rid}/events")
    assert r.status_code == 401


async def test_the_rate_limit_applies(state_store, fake_redis, fake_redis_time, recorders):
    rid, _ = await cancelled_request(state_store)
    limits = ClientLimitsConfig(default=ClientSettingsConfig(rate=1.0, burst=1))
    app = make_app(state_store, fake_redis, recorders, limits)
    assert (await stream(app, rid)).status_code == 200
    limited = await stream(app, rid)
    assert (limited.status_code, limited.json()) == (429, {"error": "rate_limited"})


# --------------------------------------------------------------------------- caps and release


async def fill_key_slots(state_store: StateStore, count: int) -> None:
    for i in range(count):
        assert await state_store.acquire_stream(fp(KEY), f"other-{i}", count, 600.0)


async def test_the_per_key_cap_is_429_and_registers_nothing(
    state_store, fake_redis, fake_redis_time, recorders
):
    rid, _ = await cancelled_request(state_store)
    await fill_key_slots(state_store, 2)
    app = make_app(state_store, fake_redis, recorders, settings(max_streams=2))
    r = await stream(app, rid)
    assert (r.status_code, r.json()) == (429, {"error": "too_many_streams"})
    assert await fake_redis.zcard(keys.client_streams(fp(KEY))) == 2
    assert app.state.event_gate.open == 0  # the replica slot taken first is given back
    assert recorders.value("das_client_rejections_total", reason="too_many_streams") == 1


async def test_the_per_replica_cap_is_503(state_store, fake_redis, fake_redis_time, recorders):
    rid, _ = await cancelled_request(state_store)
    app = make_app(state_store, fake_redis, recorders, events_config=EventsConfig(max_connections=1))
    assert app.state.event_gate.try_acquire()  # another stream holds the replica's only slot
    r = await stream(app, rid)
    assert (r.status_code, r.json()) == (503, {"error": "too_many_streams"})
    assert await fake_redis.zcard(keys.client_streams(fp(KEY))) == 0
    assert recorders.value("das_client_rejections_total", reason="too_many_streams") == 1


async def test_both_counts_are_released_when_the_stream_ends(
    state_store, fake_redis, fake_redis_time, recorders, monkeypatch
):
    rid, stored = await cancelled_request(state_store)
    calls: list[tuple[str, str]] = []
    acquire, release = state_store.acquire_stream, state_store.release_stream

    async def spy_acquire(fingerprint, member, *args):
        calls.append(("acquire", member))
        return await acquire(fingerprint, member, *args)

    async def spy_release(fingerprint, member):
        calls.append(("release", member))
        await release(fingerprint, member)

    monkeypatch.setattr(state_store, "acquire_stream", spy_acquire)
    monkeypatch.setattr(state_store, "release_stream", spy_release)
    app = make_app(state_store, fake_redis, recorders)
    assert (await stream(app, rid, **{LAST_EVENT_ID: stored[0][0]})).status_code == 200
    assert [c[0] for c in calls] == ["acquire", "release"] and calls[0][1] == calls[1][1]
    assert await fake_redis.zcard(keys.client_streams(fp(KEY))) == 0
    assert app.state.event_gate.open == 0
    assert recorders.value("das_sse_streams") == 0


async def test_max_streams_zero_never_touches_the_per_key_slots(
    state_store, fake_redis, fake_redis_time, recorders, monkeypatch
):
    rid, _ = await cancelled_request(state_store)

    async def boom(*_: Any, **__: Any):
        raise AssertionError("slot used although max_streams is 0")

    monkeypatch.setattr(state_store, "acquire_stream", boom)
    app = make_app(state_store, fake_redis, recorders, settings(max_streams=0))
    assert (await stream(app, rid)).status_code == 200


async def test_a_store_failure_before_streaming_is_503_and_releases_the_replica_slot(
    state_store, fake_redis, fake_redis_time, recorders, monkeypatch
):
    rid, _ = await cancelled_request(state_store)

    async def down(*_: Any, **__: Any):
        raise ConnectionError("redis down")

    monkeypatch.setattr(state_store, "acquire_stream", down)
    app = make_app(state_store, fake_redis, recorders)
    r = await stream(app, rid)
    assert (r.status_code, r.json()) == (503, {"error": "service_unavailable"})
    assert app.state.event_gate.open == 0


async def test_a_failed_snapshot_read_is_503(state_store, fake_redis, fake_redis_time, recorders, monkeypatch):
    rid, _ = await cancelled_request(state_store)

    async def down(*_: Any, **__: Any):
        raise ConnectionError("redis down")

    monkeypatch.setattr(state_store, "load_snapshot", down)
    r = await stream(make_app(state_store, fake_redis, recorders), rid)
    assert (r.status_code, r.json()) == (503, {"error": "service_unavailable"})
