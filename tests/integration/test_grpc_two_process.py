"""gRPC Transport between two OS processes (task 8.8).

Spawns ``grpc_peer.py`` as a separate Python process serving agent
``worker-peer`` and talks to it over a real localhost socket: request-reply
returns the peer's reply, and ``publish`` completes once the peer
acknowledges. No Docker needed.

Requirements: 8.3
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from das.codec import new_message
from das.models.results import ControlPayload
from das.transport.base import TransportUnavailable
from das.transport.grpc_rpc import GrpcTransport

pytestmark = pytest.mark.integration

PEER = Path(__file__).with_name("grpc_peer.py")
ROOT = Path(__file__).resolve().parents[2]
TS = datetime(2025, 1, 1, tzinfo=UTC)


@pytest.fixture
async def peer() -> AsyncIterator[tuple[str, asyncio.subprocess.Process]]:
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(PEER), "worker-peer",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, cwd=ROOT, env=env,
    )
    try:
        assert process.stdout is not None
        line = (await asyncio.wait_for(process.stdout.readline(), 30)).decode().strip()
        assert line.startswith("READY "), f"peer failed to start: {line!r}"
        yield f"127.0.0.1:{int(line.split()[1])}", process
    finally:
        if process.returncode is None:
            assert process.stdin is not None
            process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), 10)
            except TimeoutError:
                process.kill()
                await process.wait()


async def test_request_reply_between_two_processes(peer) -> None:
    endpoint, process = peer
    endpoints = {"worker-peer": endpoint}
    manager = GrpcTransport(
        agent_id="manager-it", bind="127.0.0.1:0", resolver=endpoints.get,
        request_reply_timeout=5,
    )
    await manager.start()
    try:
        request = new_message(
            ControlPayload(command="cancel_job", request_id="r1", job_id="j-42"),
            correlation_id="r1", message_id="m-1", timestamp=TS,
        )
        reply = await manager.request_reply("worker-peer", request)
        assert reply.payload.command == "cancel_request"
        assert reply.payload.job_id == "j-42"
        assert reply.payload.request_id == f"peer-{process.pid}"  # answered by the other process
        assert reply.correlation_id == "r1"

        # Fire-and-forget control command: confirmed once the peer acknowledges.
        await manager.publish("worker-peer", request, headers={"x-test": "1"})

        # Peer gone -> TransportUnavailable.
        assert process.stdin is not None
        process.stdin.close()
        await asyncio.wait_for(process.wait(), 10)
        with pytest.raises(TransportUnavailable):
            await manager.publish("worker-peer", request)
    finally:
        await manager.close()
