# Feature: das-hardening-ci, Property 1: Bytes của Message_Codec giữ nguyên so với baseline
"""Property 1: Message_Codec bytes are unchanged against the Phase 3 baseline.

For every Message in the baseline corpus (``codec_corpus.jsonl``, generated from
the existing Message strategies, covering every message type with and without a
Trace_Context), ``MessageCodec().encode(message)`` is byte-identical to the
recorded bytes, and ``MessageCodec().decode(recorded bytes)`` returns a Message
equal to the recorded one.

The corpus is iterated exhaustively instead of sampled: it was produced by
Hypothesis at baseline, and the baseline codec is not available at test time.

**Validates: Requirements 11.2**
"""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest
from pydantic import TypeAdapter

from das.codec.envelope import TypedMessage
from das.codec.message_codec import MessageCodec
from tests.fixtures.phase3_baseline import CODEC_CORPUS

_MIN_CORPUS_SIZE = 300


def _load_corpus() -> list[dict[str, Any]]:
    text = CODEC_CORPUS.read_text(encoding="ascii")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


_ROWS = _load_corpus()
_adapter: TypeAdapter[Any] = TypeAdapter(TypedMessage)
_codec = MessageCodec()


def test_corpus_is_complete() -> None:
    assert len(_ROWS) >= _MIN_CORPUS_SIZE
    for row in _ROWS:
        assert set(row) == {"type", "message", "bytes_b64"}


@pytest.mark.parametrize(
    "row",
    _ROWS,
    ids=[f"{i:03d}-{row['type']}" for i, row in enumerate(_ROWS)],
)
def test_codec_bytes_match_baseline(row: dict[str, Any]) -> None:
    message = _adapter.validate_python(row["message"])
    assert message.type == row["type"]
    baseline = base64.b64decode(row["bytes_b64"], validate=True)

    assert _codec.encode(message) == baseline
    assert _codec.decode(baseline) == message
