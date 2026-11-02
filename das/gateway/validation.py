"""Pure prompt validation for the Gateway_API (Requirement 1.4, design Property 7)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

PromptErrorCode = Literal["empty_prompt", "prompt_too_long"]


@dataclass(frozen=True, slots=True)
class PromptError:
    """Which prompt constraint was violated; ``max_length`` is set for ``prompt_too_long``."""

    error: PromptErrorCode
    max_length: int | None = None

    def to_body(self) -> dict[str, Any]:
        """The HTTP 400 response body: ``{error}`` or ``{error, max_length}``."""
        body: dict[str, Any] = {"error": self.error}
        if self.max_length is not None:
            body["max_length"] = self.max_length
        return body


def validate_prompt(prompt: str, max_len: int) -> PromptError | None:
    """Return the violated constraint for ``prompt``, or ``None`` when it is acceptable.

    Empty or whitespace-only (Python Unicode ``str.strip`` semantics) yields
    ``empty_prompt``; otherwise a length above ``max_len`` characters yields
    ``prompt_too_long`` carrying ``max_len``.
    """
    if prompt.strip() == "":
        return PromptError("empty_prompt")
    if len(prompt) > max_len:
        return PromptError("prompt_too_long", max_length=max_len)
    return None
