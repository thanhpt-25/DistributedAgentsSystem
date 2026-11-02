"""Capture the Phase 3 baseline (das-hardening-ci Requirement 11.1-11.3, design H11).

Run once, before the first Phase 3 change in ``das/``::

    uv run python -m tests.fixtures.phase3_baseline.capture

Writes two files next to this module:

``codec_corpus.jsonl``
    One Message per line: ``type``, ``message`` (``model_dump(mode="json")``) and
    ``bytes_b64`` (``MessageCodec().encode``). Messages come from the strategies in
    ``tests/strategies.py`` with a fixed Hypothesis seed: every message type, each
    with and without a Trace_Context (Envelope_V2 and v1).

``contract.json``
    ``metrics`` (catalog of ``das.observability.metrics``), ``config_schema``
    (``AgentConfig.model_json_schema()``), ``env_vars`` (``DAS__`` names the loader
    maps onto an Agent_Config field), ``routes`` (Gateway_API with Admin_API, and
    the Ops server) and ``exit_codes``.

Output is deterministic: JSON keys are sorted and lists have a fixed order, so a
diff against the committed files is readable.
"""

from __future__ import annotations

import base64
import json
import types
import typing
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from hypothesis import HealthCheck, Phase, given, seed, settings
from hypothesis import strategies as st
from pydantic import BaseModel, TypeAdapter

from tests.fixtures.phase3_baseline import CODEC_CORPUS, CONTRACT
from tests.strategies import message_timestamps, messages, trace_contexts

SEED = 20251028
PER_BUCKET = 25  # 6 types x {v1, v2} x 25 = 300 Messages
_V1_TYPES = ("job_dispatch", "job_result", "heartbeat", "control")
_AGENT_CONTROL_TYPES = ("agent_control", "agent_control_reply")


# --------------------------------------------------------------------------- codec corpus


_ids = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=64)


def _agent_control_messages(kind: str) -> st.SearchStrategy[Any]:
    """``agent_control`` / ``agent_control_reply`` Messages.

    ``tests.strategies.messages`` covers the four v1 types only; this mirrors the
    strategy used in ``tests/property/test_codec_bytes_transport_v1.py``.
    """
    from das.codec.envelope import (
        AgentControlMessage,
        AgentControlPayload,
        AgentControlReplyMessage,
        AgentControlReplyPayload,
    )

    if kind == "agent_control":
        return st.builds(
            AgentControlMessage,
            message_id=_ids,
            correlation_id=st.just(""),
            timestamp=message_timestamps,
            payload=st.builds(
                AgentControlPayload,
                command=st.sampled_from(["drain", "undrain"]),
                agent_id=_ids,
                release_leases=st.booleans(),
            ),
        )
    return st.builds(
        AgentControlReplyMessage,
        message_id=_ids,
        correlation_id=st.just(""),
        timestamp=message_timestamps,
        payload=st.builds(
            AgentControlReplyPayload,
            agent_id=_ids,
            status=st.sampled_from(["Available", "Draining"]),
            in_flight=st.integers(min_value=0, max_value=10_000),
        ),
    )


def _with_trace(base_strategy: st.SearchStrategy[Any]) -> st.SearchStrategy[Any]:
    @st.composite
    def build(draw: st.DrawFn) -> Any:
        base = draw(base_strategy)
        trace = draw(trace_contexts(as_model=True))
        return type(base).model_validate({**base.model_dump(), "trace_context": trace})

    return build()


def _draw(strategy: st.SearchStrategy[Any], count: int, bucket_seed: int) -> list[Any]:
    """Draw ``count`` distinct examples of ``strategy`` deterministically."""
    from das.codec.message_codec import MessageCodec

    codec = MessageCodec()
    found: dict[bytes, Any] = {}

    @seed(bucket_seed)
    @settings(
        max_examples=count * 20,
        database=None,
        deadline=None,
        phases=[Phase.generate],
        suppress_health_check=list(HealthCheck),
    )
    @given(strategy)
    def collect(message: Any) -> None:
        if len(found) < count:
            found.setdefault(codec.encode(message), message)

    collect()
    if len(found) < count:
        raise RuntimeError(f"only {len(found)} distinct examples drawn, wanted {count}")
    return list(found.values())


def build_corpus() -> list[dict[str, Any]]:
    from das.codec.envelope import TypedMessage
    from das.codec.message_codec import MessageCodec

    codec = MessageCodec()
    adapter: TypeAdapter[Any] = TypeAdapter(TypedMessage)
    rows: list[dict[str, Any]] = []
    bucket = 0
    for kind in (*_V1_TYPES, *_AGENT_CONTROL_TYPES):
        base = messages((kind,)) if kind in _V1_TYPES else _agent_control_messages(kind)
        for traced in (False, True):
            bucket += 1
            strategy = _with_trace(base) if traced else base
            for message in _draw(strategy, PER_BUCKET, SEED + bucket):
                assert message.type == kind
                assert (message.trace_context is not None) is traced
                assert message.schema_version == (2 if traced else 1)
                data = codec.encode(message)
                dumped = message.model_dump(mode="json")
                # Self-check: the row can be rebuilt and decoded back to the same Message.
                assert adapter.validate_python(dumped) == message
                assert codec.decode(data) == message
                rows.append(
                    {
                        "type": kind,
                        "message": dumped,
                        "bytes_b64": base64.b64encode(data).decode("ascii"),
                    }
                )
    rows.sort(key=lambda r: (r["type"], r["message"]["schema_version"], r["bytes_b64"]))
    return rows


# --------------------------------------------------------------------------- contract


def capture_metrics() -> list[dict[str, Any]]:
    from das.observability.metrics import CATALOG

    return sorted(
        (
            {
                "name": spec.name,
                "type": spec.kind,
                "labels": list(spec.labels),
                "help": spec.help,
                "buckets_key": spec.buckets_key,
                "allowed": {k: sorted(v) for k, v in sorted(spec.allowed.items())},
            }
            for spec in CATALOG
        ),
        key=lambda m: m["name"],
    )


def capture_config_schema() -> dict[str, Any]:
    from das.config.schema import AgentConfig

    return AgentConfig.model_json_schema()


def _section_model(annotation: Any) -> type[BaseModel] | None:
    """The BaseModel section behind ``annotation`` (``X``, ``X | None``, ``Annotated[X, ...]``)."""
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        return _section_model(typing.get_args(annotation)[0])
    if origin in (typing.Union, types.UnionType):
        models = [m for m in map(_section_model, typing.get_args(annotation)) if m is not None]
        others = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(models) == 1 and len(others) == 1:
            return models[0]
    return None


def _leaf_paths(model: type[BaseModel], prefix: tuple[str, ...] = ()) -> Iterable[tuple[str, ...]]:
    for name, info in model.model_fields.items():
        path = (*prefix, name)
        section = _section_model(info.annotation)
        if section is None:
            yield path
        else:
            yield from _leaf_paths(section, path)


def capture_env_vars() -> list[str]:
    """``DAS__`` names that the loader maps onto an Agent_Config field."""
    from das.config.loader import env_path
    from das.config.schema import AgentConfig
    from das.config.secrets import env_name

    names = []
    for path in _leaf_paths(AgentConfig):
        name = env_name(".".join(path))
        assert env_path(name) == path, name
        names.append(name)
    return sorted(names)


def _routes(app: Any) -> list[list[str]]:
    """``[method, path]`` of every route, sorted by path then method.

    FastAPI keeps an included router as one wrapper entry in ``app.routes``;
    ``iter_route_contexts`` expands it into the effective routes (full path).
    """
    from fastapi.routing import iter_route_contexts

    out: set[tuple[str, str]] = set()
    for ctx in iter_route_contexts(list(app.routes)):
        try:
            route = ctx.original_route
        except AttributeError:
            route = ctx
        path = getattr(ctx, "path", None) or route.path
        for method in getattr(route, "methods", None) or ():
            out.add((method, path))
    return [list(r) for r in sorted(out, key=lambda r: (r[1], r[0]))]


def capture_routes() -> dict[str, list[list[str]]]:
    import fakeredis.aioredis

    from das.clock import SystemClock
    from das.gateway.app import create_app
    from das.health.server import (
        LivenessSource,
        create_health_app,
        create_ops_app,
        gateway_readiness,
    )
    from das.health.supervisor import EventLoopWatchdog, LoopSupervisor
    from das.registry import AgentRegistry
    from das.state import StateStore

    client = fakeredis.aioredis.FakeRedis()
    gateway = create_app(
        StateStore(client),
        AgentRegistry(client),
        None,
        ["baseline-api-key-0001"],
        10,
        admin_api_keys=["baseline-admin-key-0001"],
    )

    async def ok() -> bool:
        return True

    clock = SystemClock()

    def ops(healthz: dict[str, Any] | None) -> Any:
        return create_ops_app(
            metrics=None,
            liveness=LivenessSource(LoopSupervisor(), EventLoopWatchdog(clock), clock),
            readiness=gateway_readiness(startup=lambda: True, state_store=ok),
            healthz=healthz,
        )

    return {
        # Gateway_API on http_port, Admin_API included.
        "gateway": _routes(gateway),
        # Ops server of a Manager_Agent / Worker_Agent (keeps v1 /healthz).
        "ops_agent": _routes(ops({"transport": ok})),
        # Ops server of the Gateway_API (its /healthz is served on http_port).
        "ops_gateway": _routes(ops(None)),
        # v1 health app.
        "health": _routes(create_health_app({"transport": ok})),
    }


def capture_exit_codes() -> dict[str, Any]:
    import das.cli
    import das.errors as errors

    constants = {
        name: getattr(errors, name) for name in sorted(dir(errors)) if name.startswith("EXIT_")
    }
    exceptions = {
        name: obj.exit_code
        for name, obj in sorted(vars(errors).items())
        if isinstance(obj, type) and issubclass(obj, errors.DASError)
    }
    cli = {name: getattr(das.cli, name) for name in sorted(dir(das.cli)) if name.startswith("EXIT_")}
    return {"constants": constants, "exceptions": exceptions, "cli": cli}


def build_contract() -> dict[str, Any]:
    return {
        "metrics": capture_metrics(),
        "config_schema": capture_config_schema(),
        "env_vars": capture_env_vars(),
        "routes": capture_routes(),
        "exit_codes": capture_exit_codes(),
    }


# --------------------------------------------------------------------------- main


def _dumps(obj: Any, **kw: Any) -> str:
    # ASCII-only output: Message text may hold U+2028 and similar, which
    # str.splitlines() would treat as line breaks.
    return json.dumps(obj, sort_keys=True, ensure_ascii=True, **kw)


def write(corpus_path: Path = CODEC_CORPUS, contract_path: Path = CONTRACT) -> None:
    rows = build_corpus()
    corpus_path.write_text("".join(_dumps(r) + "\n" for r in rows), encoding="utf-8")
    contract_path.write_text(_dumps(build_contract(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(rows)} messages to {corpus_path}")
    print(f"wrote contract to {contract_path}")


if __name__ == "__main__":
    write()
