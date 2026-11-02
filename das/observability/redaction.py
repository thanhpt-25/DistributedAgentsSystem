"""Secret redaction for log output (Requirement 12.7).

The :class:`Redactor` guarantees that, after :meth:`Redactor.redact`, the text
contains no substring of length >= 8 of any registered secret, and no whole
occurrence of any registered secret shorter than 8 characters.

Because every substring of length >= 8 contains a substring of length exactly 8,
it is sufficient to remove every 8-character window of each long secret. Matched
spans (which may overlap) are merged and each merged span is replaced by the
redaction marker. Replacement can create new matches at the seams (the marker
plus neighbouring characters may form a secret window), so redaction repeats
until a fixpoint is reached.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, runtime_checkable

REDACTION_MARKER = "[REDACTED]"

#: Secrets of at least this length are redacted by sliding window; shorter
#: secrets are redacted as whole occurrences.
WINDOW = 8

# Markers tried in order when a registered secret collides with the preferred
# marker (e.g. a secret that itself contains "[REDACTED]"). The empty string is
# the last resort: deletion strictly shrinks the text, so the loop terminates.
_FALLBACK_MARKERS: tuple[str, ...] = (REDACTION_MARKER, "<redacted>", "***", "")

# After this many marker-based passes, switch to deletion to guarantee
# termination even for adversarial secret sets.
_MAX_MARKER_PASSES = 16


@runtime_checkable
class _SecretLike(Protocol):
    def get_secret_value(self) -> str: ...


def _as_str(secret: str | _SecretLike) -> str:
    if isinstance(secret, str):
        return secret
    if isinstance(secret, _SecretLike):
        return secret.get_secret_value()
    raise TypeError(f"unsupported secret type: {type(secret).__name__}")


class Redactor:
    """Replaces registered secret values (and their long substrings) in text."""

    def __init__(self, secrets: Iterable[str | _SecretLike] = ()) -> None:
        self._windows: set[str] = set()
        self._short: set[str] = set()
        self._marker = REDACTION_MARKER
        for secret in secrets:
            self.add(secret)

    # ------------------------------------------------------------------ setup
    def add(self, secret: str | _SecretLike | None) -> None:
        """Register a secret. ``None`` and empty values are ignored."""
        if secret is None:
            return
        value = _as_str(secret)
        if not value:
            return
        if len(value) >= WINDOW:
            for i in range(len(value) - WINDOW + 1):
                self._windows.add(value[i : i + WINDOW])
        else:
            self._short.add(value)
        self._marker = self._choose_marker()

    def __bool__(self) -> bool:
        return bool(self._windows or self._short)

    @property
    def marker(self) -> str:
        """The marker currently used (normally ``[REDACTED]``)."""
        return self._marker

    def _choose_marker(self) -> str:
        for marker in _FALLBACK_MARKERS:
            if not self._spans(marker):
                return marker
        return ""

    # -------------------------------------------------------------- matching
    def _spans(self, text: str) -> list[tuple[int, int]]:
        """Merged ``[start, end)`` spans of ``text`` covered by any match."""
        covered: list[tuple[int, int]] = []
        if self._windows and len(text) >= WINDOW:
            windows = self._windows
            for i in range(len(text) - WINDOW + 1):
                if text[i : i + WINDOW] in windows:
                    covered.append((i, i + WINDOW))
        for short in self._short:
            start = text.find(short)
            while start != -1:
                covered.append((start, start + len(short)))
                start = text.find(short, start + 1)
        if not covered:
            return []
        covered.sort()
        merged = [covered[0]]
        for start, end in covered[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        return merged

    def contains_secret(self, text: str) -> bool:
        """True if ``text`` still contains any forbidden substring."""
        return bool(self._spans(text))

    # ------------------------------------------------------------- redaction
    def redact(self, text: str) -> str:
        """Return ``text`` with every secret occurrence replaced by the marker."""
        if not self or not text:
            return text
        marker = self._marker
        passes = 0
        while True:
            spans = self._spans(text)
            if not spans:
                return text
            passes += 1
            if passes > _MAX_MARKER_PASSES:
                marker = ""
            parts: list[str] = []
            pos = 0
            for start, end in spans:
                parts.append(text[pos:start])
                parts.append(marker)
                pos = end
            parts.append(text[pos:])
            text = "".join(parts)
