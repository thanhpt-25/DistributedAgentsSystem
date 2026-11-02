"""Unit tests for das.gateway.validation (Requirement 1.4)."""

from __future__ import annotations

import pytest

from das.gateway.validation import PromptError, validate_prompt


@pytest.mark.parametrize("prompt", ["", " ", "\t\n\r ", "\u3000\u2003", "\x1c\x85"])
def test_empty_or_whitespace_only_is_empty_prompt(prompt: str) -> None:
    err = validate_prompt(prompt, 10)
    assert err == PromptError("empty_prompt")
    assert err is not None and err.to_body() == {"error": "empty_prompt"}


def test_whitespace_only_beyond_max_is_still_empty_prompt() -> None:
    assert validate_prompt(" " * 50, 10) == PromptError("empty_prompt")


def test_too_long_carries_configured_max() -> None:
    err = validate_prompt("a" * 11, 10)
    assert err == PromptError("prompt_too_long", max_length=10)
    assert err is not None
    assert err.to_body() == {"error": "prompt_too_long", "max_length": 10}


@pytest.mark.parametrize("prompt", ["a", "a" * 10, "  padded  ", "é" * 10])
def test_valid_prompts_up_to_max(prompt: str) -> None:
    assert validate_prompt(prompt, 10) is None


def test_length_counts_characters_including_surrounding_whitespace() -> None:
    assert validate_prompt(" abc ", 4) == PromptError("prompt_too_long", max_length=4)
    assert validate_prompt("x", 1) is None
