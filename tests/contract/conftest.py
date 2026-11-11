"""Fixtures for the contract test: the real Gateway_API app on fakeredis, and both clients for it."""

from __future__ import annotations

from typing import Any

import pytest

from das.config.schema import ClientLimitsConfig
from das.gateway.app import create_app
from das.registry import AgentRegistry
from tests.contract.harness import Contract
from tests.unit.test_gateway_limits import KEY
from tests.unit.test_gateway_submit_client import settings


@pytest.fixture(params=["async", "sync"])
async def contract(request: pytest.FixtureRequest, state_store, fake_redis, fake_redis_time):
    """``contract(limits=..., key=..., **client_kwargs)`` builds an app and a client for it, and tears them down.

    Every test using it runs once per client. By default there is no rate limit and no quota, and the client does
    not retry (``max_retries=0``), so an error reaches the test at once.
    """
    await state_store.set_index_ready()  # list_requests answers 503 until the Request_Index is backfilled
    built: list[Contract] = []

    def make(limits: ClientLimitsConfig | None = None, key: str = KEY, **client_kwargs: Any) -> Contract:
        app = create_app(
            state_store,
            AgentRegistry(fake_redis),
            None,
            [KEY],
            50,
            client_limits=limits or settings(),
        )
        client_kwargs.setdefault("max_retries", 0)
        made = Contract(str(request.param), app, key, **client_kwargs)
        built.append(made)
        return made

    yield make
    for made in built:
        await made.close()
