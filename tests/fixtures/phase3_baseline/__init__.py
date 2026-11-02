"""Phase 3 baseline snapshot (das-hardening-ci Requirement 11, design H11).

The files in this directory were captured from the code before the first
Phase 3 change in ``das/``. Regenerate them only when a Developer confirms a
behaviour change is intentional::

    uv run python -m tests.fixtures.phase3_baseline.capture
"""

from pathlib import Path

BASELINE_DIR = Path(__file__).resolve().parent
CODEC_CORPUS = BASELINE_DIR / "codec_corpus.jsonl"
CONTRACT = BASELINE_DIR / "contract.json"
