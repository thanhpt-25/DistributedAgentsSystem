"""Idempotent submit helpers (Phase 4 Req 4). Pure."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Final

from das.client.budget import Budget

MAX_IDEMPOTENCY_KEY_LENGTH: Final = 128
_KEY: Final = re.compile(rf"[\x21-\x7e]{{1,{MAX_IDEMPOTENCY_KEY_LENGTH}}}")


def validate_idempotency_key(value: str) -> bool:
    """1 to 128 printable ASCII characters, 0x21-0x7E (Req 4.1)."""
    return _KEY.fullmatch(value) is not None


def body_hash(prompt: str, budget: Budget | None) -> str:
    """Body_Hash: sha256 of the canonical JSON of ``{prompt, budget}`` as submitted (Req 4.3, 4.4).

    An empty Budget counts as no Budget, and ``max_cost`` uses its canonical text, so
    ``1.50`` and ``1.5`` hash the same.
    """
    obj = {
        "prompt": prompt,
        "budget": None if budget is None or budget.is_empty() else budget.to_json_obj(),
    }
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def idem_key_hash(value: str) -> str:
    """Hex sha256 of an Idempotency_Key, used in the Idempotency_Record key name."""
    return hashlib.sha256(value.encode("ascii")).hexdigest()
