"""Smoke tests for the LLM provider factory (Requirements 10.2, 10.3, 10.7). No network."""

from __future__ import annotations

import pytest

from das.config.schema import LLMConfig
from das.errors import ConfigError
from das.llm.adapters.anthropic import AnthropicAdapter
from das.llm.adapters.bedrock import BedrockAdapter
from das.llm.adapters.ollama import OllamaAdapter
from das.llm.adapters.openai import OpenAIAdapter
from das.llm.factory import build_llm_provider
from das.llm.retry import LLMProvider

CASES = [
    ({"provider": "openai", "api_key": "sk-test"}, OpenAIAdapter),
    ({"provider": "anthropic", "api_key": "sk-ant-test"}, AnthropicAdapter),
    ({"provider": "bedrock", "aws_region": "us-east-1"}, BedrockAdapter),
    ({"provider": "ollama", "base_url": "http://ollama:11434"}, OllamaAdapter),
]


@pytest.mark.parametrize(("settings", "adapter_type"), CASES)
async def test_builds_one_provider_bound_to_configured_adapter_and_model(settings, adapter_type):
    cfg = LLMConfig(model="m-1", temperature=0.5, max_output_tokens=256, timeout=30, retry_count=2, **settings)
    provider = build_llm_provider(cfg)
    assert isinstance(provider, LLMProvider)
    assert isinstance(provider._adapter, adapter_type)
    assert provider.provider == settings["provider"]
    assert provider.params.model == "m-1"
    assert provider.params.temperature == 0.5
    assert provider.params.max_output_tokens == 256
    assert provider.retry_count == 2
    assert provider.timeout == 30
    await provider._adapter.aclose()


@pytest.mark.parametrize(
    ("settings", "field"),
    [
        ({"provider": "openai"}, "llm.api_key"),
        ({"provider": "anthropic"}, "llm.api_key"),
        ({"provider": "ollama"}, "llm.base_url"),
        ({"provider": "bedrock", "aws_access_key_id": "AKIA"}, "llm.aws_secret_access_key"),
    ],
)
def test_missing_provider_settings_raise_config_error(settings, field):
    cfg = LLMConfig(model="m-1", **settings)
    with pytest.raises(ConfigError) as info:
        build_llm_provider(cfg)
    assert info.value.fields == (field,)
