"""Build the single LLMProvider an agent uses, from Agent_Config (Req 10.2, 10.3, 10.7).

Only the four configured provider names are selectable. Test doubles such as
``tests.fakes.llm_adapter.FakeAdapter`` are injected in code and have no entry
here (Req 10.7).
"""

from __future__ import annotations

from das.config.schema import PROVIDERS, AgentConfig, LLMConfig
from das.errors import ConfigError, FieldError
from das.llm.base import ModelParams, ProviderAdapter
from das.llm.retry import LLMProvider


def _secret(value: object) -> str | None:
    if value is None:
        return None
    raw = value.get_secret_value()  # type: ignore[attr-defined]
    return raw or None


def _require(cfg: LLMConfig, field: str, value: object) -> None:
    if value in (None, ""):
        raise ConfigError(
            [FieldError(f"llm.{field}", f"field required for provider {cfg.provider}")],
            reason="invalid configuration",
        )


def build_adapter(cfg: LLMConfig) -> ProviderAdapter:
    """Instantiate the Provider_Adapter named by ``cfg.provider``.

    SDK modules are imported lazily so an agent loads only its own vendor SDK.
    """
    if cfg.provider == "openai":
        api_key = _secret(cfg.api_key)
        _require(cfg, "api_key", api_key)
        from das.llm.adapters.openai import OpenAIAdapter

        return OpenAIAdapter(api_key=api_key, base_url=cfg.base_url)
    if cfg.provider == "anthropic":
        api_key = _secret(cfg.api_key)
        _require(cfg, "api_key", api_key)
        from das.llm.adapters.anthropic import AnthropicAdapter

        return AnthropicAdapter(api_key=api_key, base_url=cfg.base_url)
    if cfg.provider == "bedrock":
        key_id = _secret(cfg.aws_access_key_id)
        secret = _secret(cfg.aws_secret_access_key)
        if (key_id is None) != (secret is None):
            missing = "aws_access_key_id" if key_id is None else "aws_secret_access_key"
            _require(cfg, missing, None)
        from das.llm.adapters.bedrock import BedrockAdapter

        return BedrockAdapter(
            region=cfg.aws_region,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            aws_session_token=_secret(cfg.aws_session_token),
            read_timeout=cfg.timeout,
        )
    if cfg.provider == "ollama":
        _require(cfg, "base_url", cfg.base_url)
        from das.llm.adapters.ollama import OllamaAdapter

        return OllamaAdapter(base_url=cfg.base_url)  # type: ignore[arg-type]
    raise ConfigError(
        [FieldError("llm.provider", f"must be one of {', '.join(PROVIDERS)}")],
        reason="invalid configuration",
    )


def build_llm_provider(config: AgentConfig | LLMConfig) -> LLMProvider:
    """Build exactly one LLMProvider bound to the configured adapter and model.

    Raises :class:`das.errors.ConfigError` when the ``llm`` section is missing or
    incomplete for the selected provider.
    """
    cfg = config.llm if isinstance(config, AgentConfig) else config
    if cfg is None:
        raise ConfigError([FieldError("llm", "field required")], reason="invalid configuration")
    params = ModelParams(
        model=cfg.model,
        temperature=cfg.temperature,
        max_output_tokens=cfg.max_output_tokens,
    )
    return LLMProvider(
        build_adapter(cfg), params, retry_count=cfg.retry_count, timeout=cfg.timeout,
        fallback_model=cfg.fallback_model,  # Phase 6 Req 2.3, 2.7: every role's provider has it, Planner included
    )


__all__ = ["build_adapter", "build_llm_provider"]
