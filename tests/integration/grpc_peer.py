"""Helper process for the two-process gRPC integration test (task 8.8).

Starts a :class:`~das.transport.grpc_rpc.GrpcTransport` for agent ``argv[1]`` on
an ephemeral localhost port, prints ``READY <port>`` on stdout, and serves until
stdin is closed. Every Message it receives is acknowledged; request-reply calls
get a ``cancel_request`` Control Message echoing the received ``job_id`` with
``request_id = "peer-<pid>"`` so the caller can see the reply came from here.

Run as a script: ``python tests/integration/grpc_peer.py <agent_id>``.
"""

from __future__ import annotations

import asyncio
import os
import sys

from das.codec import new_message
from das.models.results import ControlPayload
from das.transport.base import Delivery
from das.transport.grpc_rpc import GrpcTransport


async def main(agent_id: str) -> None:
    transport = GrpcTransport(
        agent_id=agent_id, bind="127.0.0.1:0", resolver=lambda _: None, request_reply_timeout=5
    )
    await transport.start()

    async def handle(delivery: Delivery):
        await transport.acknowledge(delivery)
        message = delivery.message
        job_id = getattr(getattr(message, "payload", None), "job_id", None)
        return new_message(
            ControlPayload(command="cancel_request", request_id=f"peer-{os.getpid()}", job_id=job_id),
            correlation_id=getattr(message, "correlation_id", ""),
        )

    await transport.subscribe(agent_id, handle)
    print(f"READY {transport.bound_port}", flush=True)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, sys.stdin.read)  # returns on EOF
    await transport.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
