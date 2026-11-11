"""Loads the State_Store Lua scripts, each prefixed with the shared ``_common.lua``."""

from __future__ import annotations

from functools import cache
from importlib import resources
from typing import Any, Final

SCRIPT_NAMES: Final = (
    "create_request",
    "acquire_lease",
    "renew_lease",
    "release_lease",
    "commit_plan",
    "apply_transition",
    "finalize",
    "cancel_request",
    "record_inbox",
    "create_dead_letter",
    "replay_dead_letter",
    "discard_dead_letter",
    "reopen_request",
    "list_dead_letters",
    "purge_request",
    "purge_inbox",
    "purge_dead_letter",
    "index_request",
    "list_index",
    "rate_check",
    "claim_webhook",
    "complete_webhook",
    "stream_acquire",
)


def _read(name: str) -> str:
    return resources.files("das.state").joinpath("lua", f"{name}.lua").read_text(encoding="utf-8")


@cache
def script_source(name: str) -> str:
    """Full source of one script: the common helpers followed by the script body."""
    if name not in SCRIPT_NAMES:
        raise KeyError(f"unknown State_Store script: {name}")
    return _read("_common") + "\n" + _read(name)


def register_all(client: Any) -> dict[str, Any]:
    """Register every script on a redis(-compatible) client; returns name -> Script."""
    return {name: client.register_script(script_source(name)) for name in SCRIPT_NAMES}
