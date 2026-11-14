"""Agent_Config schema (Requirement 12, 6.8, 10.7, 10.8, 14.4, 14.8).

Per-field types and ranges live on the pydantic models below. Role-dependent
requirements and cross-field rules live in :func:`cross_field_errors`, which
works on the raw resolved mapping so that every problem is reported at once,
even when unrelated fields are also invalid (Requirement 12.2).

Network addresses (RabbitMQ, Redis, gRPC, Ollama) have no defaults: they must
come from the configuration file or the environment (Requirement 14.8).
"""

from __future__ import annotations

import re
import socket
import uuid
from collections.abc import Mapping
from decimal import Decimal
from urllib.parse import urlsplit
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SecretStr,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from das.errors import FieldError
from das.ops.cost import MAX_PRICE_ENTRIES, PriceEntry, PriceTable
from das.ops.iso4217 import ISO_4217_CODES
from das.ops.keys import key_fingerprint
from das.sandbox.limits import CEILINGS as SANDBOX_CEILINGS
from das.sandbox.limits import FLOORS as SANDBOX_FLOORS

Role = Literal["gateway", "manager", "worker"]
ROLES: tuple[str, ...] = ("gateway", "manager", "worker")

Provider = Literal["openai", "anthropic", "bedrock", "ollama"]
PROVIDERS: tuple[str, ...] = ("openai", "anthropic", "bedrock", "ollama")

CAPABILITY_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"

#: Dotted paths of every credential field. These are accepted only from the
#: environment or the secrets file, never from the YAML file (Req 12.4, 12.5,
#: Phase 2 Req 8.5, 8.6, 11.1, 11.10).
SECRET_FIELDS: tuple[str, ...] = (
    "api_keys",
    "admin_api_keys",
    "llm.api_key",
    "llm.aws_access_key_id",
    "llm.aws_secret_access_key",
    "llm.aws_session_token",
    "tracing.otlp_headers",
    "gateway.cursor_secret",
    "webhook_secrets",
    "redis.password",
    "redis.sentinel_password",
)

#: Credential fields that hold a map of named secrets. Each entry comes from
#: ``DAS__<FIELD>__<NAME>`` in the environment or the secrets file (Phase 4 Req 1.7).
SECRET_PREFIX_FIELDS: tuple[str, ...] = ("webhook_secrets",)

#: Default histogram bucket boundaries (Req 2.11).
DEFAULT_HTTP_BUCKETS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
)
DEFAULT_DURATION_BUCKETS: tuple[float, ...] = (
    0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
)
DEFAULT_ATTEMPTS_BUCKETS: tuple[float, ...] = (1, 2, 3, 5, 10, 20)
MAX_BUCKETS = 50

#: Admin_API_Key limits (Req 11.10).
MAX_ADMIN_API_KEYS = 32
ADMIN_KEY_MIN_LENGTH = 16
ADMIN_KEY_MAX_LENGTH = 256


def _split_csv(value: Any) -> Any:
    """Accept ``"a,b"`` (environment form) as well as a YAML list."""
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return value


def _check_scheme(value: str, schemes: tuple[str, ...]) -> str:
    scheme = value.split("://", 1)[0].lower() if "://" in value else ""
    if scheme not in schemes or not value.split("://", 1)[1]:
        raise ValueError(f"must be a URL with scheme {' or '.join(schemes)}")
    return value


Port = Annotated[int, Field(ge=1, le=65535)]
Capability = Annotated[str, Field(pattern=CAPABILITY_PATTERN)]


class _Section(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class TransportsConfig(_Section):
    """Transport implementation names, resolved by the transport loader (Req 8.4)."""

    jobs: str = Field(min_length=1)
    control: str = Field(min_length=1)


class RabbitMQConfig(_Section):
    url: str
    management_url: str | None = None

    @field_validator("url")
    @classmethod
    def _url(cls, v: str) -> str:
        return _check_scheme(v, ("amqp", "amqps"))

    @field_validator("management_url")
    @classmethod
    def _mgmt(cls, v: str | None) -> str | None:
        return None if v is None else _check_scheme(v, ("http", "https"))


#: Why ``redis.mode: cluster`` is refused (Phase 7 Req 1.3).
CLUSTER_REFUSAL = (
    "redis.mode cluster is not supported: the State_Store scripts update keys in different slots at once, "
    "so Redis Cluster cannot run them; use standalone or sentinel"
)
_SENTINEL_ADDRESS = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?:[1-9][0-9]{0,4}$")
#: The Redis keys that only ``mode: sentinel`` accepts; ``mode: standalone`` takes ``url`` alone (Req 1.1).
SENTINEL_ONLY_FIELDS: tuple[str, ...] = ("sentinels", "master_name", "db", "sentinel_tls", "sentinel_password")


class RedisConfig(_Section):
    """Where the State_Store lives: one Redis by ``url`` (``standalone``), or the master that Redis Sentinel names
    (``sentinel``, Phase 7 Req 1). The mode-dependent rules, each naming its field, are in ``cross_field_errors``."""

    mode: str = "standalone"
    url: str | None = None
    sentinels: Annotated[list[str], BeforeValidator(_split_csv), Field(min_length=1, max_length=10)] | None = None
    master_name: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")] | None = None
    db: int = Field(default=0, ge=0, le=15)
    #: Secrets: environment or secrets file only (``DAS__REDIS__PASSWORD``, ``DAS__REDIS__SENTINEL_PASSWORD``).
    password: SecretStr | None = None
    sentinel_password: SecretStr | None = None
    sentinel_tls: bool = False

    @field_validator("mode")
    @classmethod
    def _mode(cls, v: str) -> str:
        mode = v.strip().lower()
        if mode == "cluster":
            raise ValueError(CLUSTER_REFUSAL)
        if mode not in ("standalone", "sentinel"):
            raise ValueError("must be standalone or sentinel")
        return mode

    @field_validator("url")
    @classmethod
    def _url(cls, v: str | None) -> str | None:
        return None if v is None else _check_scheme(v, ("redis", "rediss", "unix"))

    @field_validator("sentinels")
    @classmethod
    def _sentinels(cls, v: list[str] | None) -> list[str] | None:
        for address in v or []:
            if _SENTINEL_ADDRESS.fullmatch(address) is None or int(address.rsplit(":", 1)[1]) > 65535:
                raise ValueError("each sentinel must be host:port with a port from 1 to 65535")
        return v


class GrpcConfig(_Section):
    bind: str
    advertise_host: str = Field(min_length=1)
    advertise_port: Port

    @field_validator("bind")
    @classmethod
    def _bind(cls, v: str) -> str:
        host, sep, port = v.rpartition(":")
        if not sep or not host or not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("must be host:port with port between 1 and 65535")
        return v


def _trim(value: object) -> object:
    return value.strip() if isinstance(value, str) else value


#: A model name: trimmed, never empty (Phase 6 Req 2.8).
ModelName = Annotated[str, BeforeValidator(_trim), Field(min_length=1)]


class LLMConfig(_Section):
    provider: Provider
    model: str = Field(min_length=1)
    #: Model per Capability (Phase 6 Req 2.1, 2.2); a Job runs on the model of its routing Capability, else ``model``.
    capability_models: dict[Capability, ModelName] = Field(default_factory=dict)
    #: Tried once after a permanent, non-authentication provider error (Phase 6 Req 2.3).
    fallback_model: ModelName | None = None
    temperature: float = Field(default=0.2, ge=0.0, le=2.0)
    max_output_tokens: int = Field(default=4096, ge=1)
    timeout: float = Field(default=60, ge=1, le=600)
    retry_count: int = Field(default=3, ge=0, le=10)
    base_url: str | None = None
    aws_region: str | None = None
    # Credentials: env or secrets file only.
    api_key: SecretStr | None = None
    aws_access_key_id: SecretStr | None = None
    aws_secret_access_key: SecretStr | None = None
    aws_session_token: SecretStr | None = None

    @field_validator("base_url")
    @classmethod
    def _base_url(cls, v: str | None) -> str | None:
        return None if v is None else _check_scheme(v, ("http", "https"))


class LimitsConfig(_Section):
    max_prompt_length: int = Field(default=32_000, ge=1, le=1_000_000)
    max_jobs: int = Field(default=10, ge=1, le=100)
    planning_retries: int = Field(default=2, ge=0, le=5)
    lease_duration: float = Field(default=30, ge=5, le=300)
    max_attempts: int = Field(default=3, ge=1, le=10)
    base_backoff: float = Field(default=2, ge=0.1, le=60)
    max_backoff: float = Field(default=60, ge=1, le=600)
    capability_wait: float = Field(default=60, ge=1, le=3600)
    job_timeout: int = Field(default=300, ge=1, le=3600)
    #: How often the Manager may replace the Jobs that failed with a new plan before failing the Request
    #: (Phase 6 Req 3.1). 0 turns replanning off.
    max_replans: int = Field(default=0, ge=0, le=3)
    max_message_bytes: int = Field(default=1_048_576, ge=1024, le=134_217_728)
    heartbeat_interval: float = Field(default=5, ge=1, le=60)
    shutdown_timeout: float = Field(default=30, ge=1, le=300)
    request_reply_timeout: float = Field(default=30, ge=1, le=300)
    max_reconnect_interval: float = Field(default=30, ge=1, le=300)
    startup_timeout: float = Field(default=60, ge=1, le=3600)
    startup_retry_interval: float = Field(default=2, ge=0.1, le=60)


#: The built-in tools a Worker can enable (Phase 6 Req 5.3).
ToolName = Literal["calculator", "http_fetch", "current_time"]
#: A host name or IPv4 literal: letters, digits, dots and hyphens; no scheme, port, wildcard or path.
HostName = Annotated[str, Field(pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")]


class HttpFetchConfig(_Section):
    """``http_fetch``: GET only, https only, the allowed hosts only (Phase 6 Req 5.9)."""

    allowed_hosts: Annotated[list[HostName], BeforeValidator(_split_csv), Field(max_length=50)] = Field(
        default_factory=list
    )
    allow_http: bool = False
    timeout: float = Field(default=10, ge=1, le=60)
    max_bytes: int = Field(default=1_048_576, ge=1024, le=10_485_760)


#: The `code-execution` Capability of the Code_Worker (Phase 6 Req 6.1).
CODE_EXECUTION = "code-execution"


class SandboxConfig(_Section):
    """The Code_Worker's sandbox (Phase 6 Req 6.1, 6.4, 6.5): off by default.

    Only the container CLI, an image, an optional OCI runtime by name and five bounded limits can be set. There is no
    setting for the network, mounts, devices, privileges, the user or the runtime's socket: ``extra="forbid"`` makes
    each of them a configuration error rather than something silently ignored."""

    enabled: bool = False
    container_cli: Literal["docker", "podman"] = "docker"
    image: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9._/:@-]{0,199}$")] = "python:3.12-slim"
    oci_runtime: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_.-]{0,30}$")] | None = None
    timeout: float = Field(default=10, ge=SANDBOX_FLOORS["timeout"], le=SANDBOX_CEILINGS["timeout"])
    memory_mb: int = Field(default=256, ge=SANDBOX_FLOORS["memory_mb"], le=SANDBOX_CEILINGS["memory_mb"])
    cpus: float = Field(default=1, ge=SANDBOX_FLOORS["cpus"], le=SANDBOX_CEILINGS["cpus"])
    pids: int = Field(default=64, ge=SANDBOX_FLOORS["pids"], le=SANDBOX_CEILINGS["pids"])
    output_bytes: int = Field(default=16_384, ge=SANDBOX_FLOORS["output_bytes"], le=SANDBOX_CEILINGS["output_bytes"])


class ToolsConfig(_Section):
    """The tools a Worker lets the model call (Phase 6 Req 5.3). Nothing is enabled by default."""

    enabled: Annotated[list[ToolName], BeforeValidator(_split_csv), Field(max_length=3)] = Field(default_factory=list)
    max_rounds: int = Field(default=4, ge=1, le=10)
    max_result_bytes: int = Field(default=16_384, ge=256, le=262_144)
    http_fetch: HttpFetchConfig = Field(default_factory=HttpFetchConfig)

    @field_validator("enabled")
    @classmethod
    def _unique(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("must not list a tool twice")
        return v


class WorkerConfig(_Section):
    capabilities: Annotated[
        list[Capability], BeforeValidator(_split_csv), Field(min_length=1, max_length=50)
    ]
    concurrency_limit: int = Field(default=4, ge=1, le=100)

    @field_validator("capabilities")
    @classmethod
    def _unique(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("must not contain duplicate capabilities")
        return v


FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
_Buckets = Annotated[list[FiniteFloat], BeforeValidator(_split_csv)]


class LabelLimitsConfig(_Section):
    """Distinct-value limit per unbounded Metric_Label (Req 5.3, 5.8)."""

    capability: int = Field(default=50, ge=1, le=500)
    queue: int = Field(default=50, ge=1, le=500)
    provider: int = Field(default=50, ge=1, le=500)
    model: int = Field(default=50, ge=1, le=500)
    #: Phase 5 Req 8.1: the ``tenant`` label of ``das_requests_submitted_total`` and ``das_requests_finished_total``.
    tenant: int = Field(default=100, ge=1, le=500)


class BucketsConfig(_Section):
    """Histogram bucket boundaries (Req 2.11). Shape is checked in :func:`cross_field_errors`."""

    http: _Buckets = Field(default_factory=lambda: list(DEFAULT_HTTP_BUCKETS))
    duration: _Buckets = Field(default_factory=lambda: list(DEFAULT_DURATION_BUCKETS))
    attempts: _Buckets = Field(default_factory=lambda: list(DEFAULT_ATTEMPTS_BUCKETS))


class MetricsConfig(_Section):
    enabled: bool = True
    queue_sample_interval: float = Field(default=15, ge=5, le=300)
    label_limits: LabelLimitsConfig = Field(default_factory=LabelLimitsConfig)
    buckets: BucketsConfig = Field(default_factory=BucketsConfig)


def parse_otlp_headers(value: str) -> list[tuple[str, str]]:
    """Parse ``"k=v,k2=v2"`` into pairs; raise ``ValueError`` (without the value) if malformed."""
    pairs: list[tuple[str, str]] = []
    for item in value.split(","):
        if not item.strip():
            continue
        name, sep, val = item.partition("=")
        if not sep or not name.strip():
            raise ValueError("must be a comma-separated list of name=value pairs")
        pairs.append((name.strip(), val.strip()))
    return pairs


class TracingConfig(_Section):
    """OpenTelemetry tracing (Req 8.1). ``otlp_endpoint`` is checked in :func:`cross_field_errors`."""

    enabled: bool = False
    otlp_endpoint: str | None = None
    otlp_protocol: Literal["grpc", "http/protobuf"] = "grpc"
    sample_ratio: float = Field(default=1.0, ge=0.0, le=1.0)
    #: ``None`` in input; :class:`AgentConfig` fills in ``das-<role>``.
    service_name: str | None = Field(default=None, min_length=1, max_length=255)
    max_queue_size: int = Field(default=2048, ge=1, le=100_000)
    # Credential: env or secrets file only (Req 8.5, 8.6).
    otlp_headers: SecretStr | None = None

    @field_validator("otlp_headers")
    @classmethod
    def _headers(cls, v: SecretStr | None) -> SecretStr | None:
        if v is not None:
            parse_otlp_headers(v.get_secret_value())
        return v

    def header_pairs(self) -> list[tuple[str, str]]:
        """OTLP exporter headers as ``(name, value)`` pairs."""
        if self.otlp_headers is None:
            return []
        return parse_otlp_headers(self.otlp_headers.get_secret_value())


def _price_decimal(value: Any) -> Any:
    """YAML floats become ``Decimal(repr(x))`` (shortest form, so 0.1 -> Decimal("0.1"))."""
    if isinstance(value, bool):
        raise ValueError("price must be a decimal number")
    if isinstance(value, float):
        return Decimal(repr(value))
    return value


class PricingEntryConfig(PriceEntry):
    """One Price_Table entry as read from Agent_Config (Req 10.1)."""

    @field_validator("input_per_million", "output_per_million", mode="before")
    @classmethod
    def _from_yaml(cls, value: Any) -> Any:
        return _price_decimal(value)


class PricingConfig(_Section):
    """Optional Price_Table (Req 10.1). Duplicate pairs are checked in :func:`cross_field_errors`."""

    currency: str = "USD"
    models: list[PricingEntryConfig] = Field(
        default_factory=list, min_length=0, max_length=MAX_PRICE_ENTRIES
    )

    @field_validator("currency")
    @classmethod
    def _iso_4217(cls, value: str) -> str:
        if value not in ISO_4217_CODES:
            raise ValueError("must be an ISO 4217 currency code (3 upper-case letters)")
        return value

    def price_table(self) -> PriceTable:
        """The pure-core :class:`PriceTable` for cost estimation."""
        return PriceTable(
            currency=self.currency,
            models=[PriceEntry.model_validate(e.model_dump()) for e in self.models],
        )


class RetentionConfig(_Section):
    """Retention_Policy (Req 14.1). TTLs are in seconds."""

    enabled: bool = False
    terminal_request_ttl: int = Field(default=604_800, ge=3600, le=31_536_000)
    inbox_ttl: int = Field(default=3600, ge=60, le=604_800)
    dlq_record_ttl: int = Field(default=2_592_000, ge=3600, le=31_536_000)
    sweep_interval: float = Field(default=60, ge=10, le=3600)
    batch_size: int = Field(default=500, ge=1, le=10_000)


# --------------------------------------------------------------------------- Phase 4 client API

#: Webhook secret names: letters and digits in groups joined by single underscores
#: (``partner_a``). Names are lower-cased because environment names are upper case.
SECRET_NAME_PATTERN = r"^[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*$"
WEBHOOK_SECRET_MIN_LENGTH = 16
MAX_CLIENT_OVERRIDES = 1000
#: Smallest non-zero ``rate``; 0 disables the Rate_Limit (Phase 4 Req 1.2).
MIN_RATE = 0.001


def _rate(value: float) -> float:
    if 0 < value < MIN_RATE:
        raise ValueError(f"must be 0 (no limit) or at least {MIN_RATE}")
    return value


def _webhook_url(value: str) -> str:
    if not _is_http_url(value):
        raise ValueError("must be an absolute http or https URL with a host")
    return value


def _webhook_secret(value: SecretStr) -> SecretStr:
    if len(value.get_secret_value()) < WEBHOOK_SECRET_MIN_LENGTH:
        raise ValueError(f"must be at least {WEBHOOK_SECRET_MIN_LENGTH} characters")
    return value


def _budget_decimal(value: Any) -> Any:
    """YAML floats become ``Decimal(repr(x))``, as for prices; booleans are rejected."""
    if isinstance(value, bool):
        raise ValueError("must be a decimal number")
    if isinstance(value, float):
        return Decimal(repr(value))
    return value


Rate = Annotated[float, Field(ge=0, le=100_000, allow_inf_nan=False), AfterValidator(_rate)]
Burst = Annotated[int, Field(ge=1, le=1_000_000)]
ActiveRequests = Annotated[int, Field(ge=0, le=1_000_000)]
Streams = Annotated[int, Field(ge=0, le=10_000)]
SecretName = Annotated[
    str, Field(min_length=1, max_length=64, pattern=SECRET_NAME_PATTERN), AfterValidator(str.lower)
]
WebhookUrl = Annotated[str, Field(max_length=2048), AfterValidator(_webhook_url)]
WebhookSecret = Annotated[SecretStr, AfterValidator(_webhook_secret)]


class BudgetConfig(_Section):
    """A key's ``default_budget`` (Phase 4 Req 5.2). ``max_cost`` is in the Price_Table currency."""

    max_total_tokens: int | None = Field(default=None, ge=1)
    max_cost: Decimal | None = Field(
        default=None, gt=0, max_digits=18, decimal_places=6, allow_inf_nan=False
    )

    @field_validator("max_cost", mode="before")
    @classmethod
    def _from_yaml(cls, value: Any) -> Any:
        return _budget_decimal(value)


class ClientSettingsConfig(_Section):
    """Client_Settings fields (Phase 4 Req 1.2). 0 disables ``rate``, ``max_active_requests`` or ``max_streams``."""

    rate: Rate = 60.0
    burst: Burst = 120
    max_active_requests: ActiveRequests = 100
    max_streams: Streams = 10
    default_budget: BudgetConfig | None = None
    webhook_url: WebhookUrl | None = None
    webhook_secret_name: SecretName | None = None


class ClientOverride(_Section):
    """Settings for one Key_Fingerprint. A field left unset (``None``) inherits the default."""

    fingerprint: str = Field(pattern=r"^[0-9a-f]{8}$")
    rate: Rate | None = None
    burst: Burst | None = None
    max_active_requests: ActiveRequests | None = None
    max_streams: Streams | None = None
    default_budget: BudgetConfig | None = None
    webhook_url: WebhookUrl | None = None
    webhook_secret_name: SecretName | None = None


class ClientLimitsConfig(_Section):
    """``client_limits`` (Phase 4 Req 1.1). Overrides are matched by Key_Fingerprint."""

    default: ClientSettingsConfig = Field(default_factory=ClientSettingsConfig)
    overrides: list[ClientOverride] = Field(default_factory=list, max_length=MAX_CLIENT_OVERRIDES)
    idempotency_ttl: int = Field(default=86_400, ge=60, le=604_800)
    allow_insecure_webhooks: bool = False

    def merged(self, override: ClientOverride | TenantSettings | None) -> ClientSettingsConfig:
        """Effective settings: each field the override sets, else the default (Phase 4 Req 1.3)."""
        if override is None:
            return self.default
        update = {
            name: getattr(override, name)
            for name in ClientSettingsConfig.model_fields
            if getattr(override, name) is not None
        }
        return self.default.model_copy(update=update)

    def override_for(self, fingerprint: str) -> ClientOverride | None:
        return next((o for o in self.overrides if o.fingerprint == fingerprint), None)

    def settings_for(self, fingerprint: str) -> ClientSettingsConfig:
        return self.merged(self.override_for(fingerprint))


#: Tenant_ID (Phase 5 Req 1.1). ``default`` is reserved for the Default_Tenant (checked in ``tenant_errors``).
TENANT_ID_PATTERN = r"^[a-z0-9][a-z0-9-]{0,31}$"
#: The implicit tenant, which no ``tenants`` entry may take (Phase 5 Req 1.1); equals ``das.state.keys.DEFAULT_TENANT``.
DEFAULT_TENANT_ID = "default"
MAX_TENANTS = 100
MAX_TENANT_KEYS = 50
TenantScope = Literal["submit", "read", "admin"]
PriorityClass = Literal["normal", "high"]
_PRIORITY_RANK = {"normal": 0, "high": 1}


class TenantKeyConfig(_Section):
    """One key of a tenant, by Key_Fingerprint (not a secret), with the Scopes it holds (Phase 5 Req 1.1)."""

    fingerprint: str = Field(pattern=r"^[0-9a-f]{8}$")
    scopes: list[TenantScope] = Field(min_length=1, max_length=3)

    @field_validator("scopes")
    @classmethod
    def _distinct(cls, value: list[str]) -> list[str]:
        if len(set(value)) != len(value):
            raise ValueError("scopes must not repeat a scope")
        return value


class TenantSettings(_Section):
    """The Client_Settings layer of a tenant, between ``client_limits.default`` and a key override (Req 4.1, 4.2).

    The fields of :class:`ClientOverride` without the fingerprint; a field left unset (``None``) inherits.
    """

    rate: Rate | None = None
    burst: Burst | None = None
    max_active_requests: ActiveRequests | None = None
    max_streams: Streams | None = None
    default_budget: BudgetConfig | None = None
    webhook_url: WebhookUrl | None = None
    webhook_secret_name: SecretName | None = None


class TenantConfig(_Section):
    """One entry of the ``tenants`` list (Phase 5 Req 1, 4, 7).

    ``max_active_requests`` here is the Tenant_Quota (0 = none), distinct from the per-key field of ``settings``.
    """

    id: str = Field(pattern=TENANT_ID_PATTERN)
    keys: list[TenantKeyConfig] = Field(min_length=1, max_length=MAX_TENANT_KEYS)
    settings: TenantSettings = Field(default_factory=TenantSettings)
    max_active_requests: ActiveRequests = 0
    default_priority: PriorityClass = "normal"
    max_priority: PriorityClass = "normal"

    @model_validator(mode="after")
    def _default_priority_within_maximum(self) -> TenantConfig:
        if _PRIORITY_RANK[self.default_priority] > _PRIORITY_RANK[self.max_priority]:
            raise ValueError("default_priority must not be above max_priority")
        return self


class UsageConfig(_Section):
    """``usage`` (Phase 5 Req 6.6): how long each per-day Usage_Rollup lives."""

    retention_days: int = Field(default=400, ge=30, le=3650)


class EventsConfig(_Section):
    """Event_Stream and SSE_Endpoint limits (Phase 4 Req 6.5, 7.5, 7.7)."""

    max_len: int = Field(default=1000, ge=100, le=100_000)
    max_connections: int = Field(default=500, ge=1, le=10_000)
    keepalive: float = Field(default=15.0, gt=0, le=300)


class WebhookConfig(_Section):
    """Webhook_Sender settings (Phase 4 Req 8.6). ``workers`` 0 disables the sender on this replica."""

    workers: int = Field(default=2, ge=0, le=32)
    timeout: float = Field(default=10.0, gt=0, le=60)
    base_backoff: float = Field(default=30.0, gt=0, le=3600)
    max_backoff: float = Field(default=3600.0, gt=0, le=86_400)
    max_attempts: int = Field(default=8, ge=1, le=20)


class GatewayConfig(_Section):
    # Credential: env or secrets file only (D10).
    cursor_secret: SecretStr | None = None


def _default_host() -> str:
    return socket.gethostname()


class AgentConfig(_Section):
    """Fully validated Agent_Config for one agent process."""

    role: Role
    agent_id: str | None = Field(default=None, min_length=1, max_length=128)
    host: str = Field(default_factory=_default_host, min_length=1)
    transports: TransportsConfig | None = None
    rabbitmq: RabbitMQConfig | None = None
    redis: RedisConfig | None = None
    grpc: GrpcConfig | None = None
    llm: LLMConfig | None = None
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    worker: WorkerConfig | None = None
    #: Phase 6 Req 5.3: the tools a Worker enables; the default enables none.
    tools: ToolsConfig = Field(default_factory=ToolsConfig)
    #: Phase 6 Req 6.1: the Code_Worker's sandbox; off by default.
    sandbox: SandboxConfig = Field(default_factory=SandboxConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    tracing: TracingConfig = Field(default_factory=TracingConfig)
    #: ``None`` means no Price_Table (Req 10.7).
    pricing: PricingConfig | None = None
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    #: Ops_Port (``/metrics``, ``/livez``, ``/readyz``, ``/healthz``). On the
    #: Gateway it must differ from ``http_port`` (Req 1.8).
    health_port: Port = 8081
    #: Gateway_API listening port (gateway role only).
    http_port: Port = 8080
    secrets_file: Path | None = None
    # Gateway API keys: env or secrets file only.
    api_keys: Annotated[list[SecretStr], BeforeValidator(_split_csv)] = Field(
        default_factory=list
    )
    # Admin_API_Keys: DAS__ADMIN_API_KEYS or secrets file only (Req 11.1).
    # Count, length, charset and overlap are checked in cross_field_errors.
    admin_api_keys: Annotated[list[SecretStr], BeforeValidator(_split_csv)] = Field(
        default_factory=list
    )
    # Phase 4 client API (Req 1).
    client_limits: ClientLimitsConfig = Field(default_factory=ClientLimitsConfig)
    events: EventsConfig = Field(default_factory=EventsConfig)
    webhook: WebhookConfig = Field(default_factory=WebhookConfig)
    # Webhook secrets by name: DAS__WEBHOOK_SECRETS__<NAME> or the secrets file only (Req 1.7).
    webhook_secrets: dict[SecretName, WebhookSecret] = Field(default_factory=dict)
    # Phase 5 (Req 1, 6): tenants by Key_Fingerprint, and the Usage_Rollup retention.
    tenants: list[TenantConfig] = Field(default_factory=list, max_length=MAX_TENANTS)
    usage: UsageConfig = Field(default_factory=UsageConfig)

    @model_validator(mode="after")
    def _defaults(self) -> AgentConfig:
        if self.agent_id is None:
            short = re.sub(r"[^A-Za-z0-9_.-]", "-", self.host)
            object.__setattr__(
                self, "agent_id", f"{self.role}-{short}-{uuid.uuid4().hex[:8]}"
            )
        if self.tracing.service_name is None:
            object.__setattr__(
                self,
                "tracing",
                self.tracing.model_copy(update={"service_name": f"das-{self.role}"}),
            )
        return self

    def secret_values(self) -> list[str]:
        """Every configured credential value, for log redaction (Req 12.7, 11.9, 19.3)."""
        values = [k.get_secret_value() for k in self.api_keys]
        values.extend(k.get_secret_value() for k in self.admin_api_keys)
        values.extend(v for _, v in self.tracing.header_pairs())
        if self.gateway.cursor_secret is not None:
            values.append(self.gateway.cursor_secret.get_secret_value())
        values.extend(s.get_secret_value() for s in self.webhook_secrets.values())
        if self.redis is not None:
            values.extend(
                s.get_secret_value() for s in (self.redis.password, self.redis.sentinel_password) if s is not None
            )
        if self.llm is not None:
            for s in (
                self.llm.api_key,
                self.llm.aws_access_key_id,
                self.llm.aws_secret_access_key,
                self.llm.aws_session_token,
            ):
                if s is not None:
                    values.append(s.get_secret_value())
        return [v for v in values if v]


# --------------------------------------------------------------------------- validation


#: Sections required per role (Req 12.2). Redis is needed by every role.
_REQUIRED_SECTIONS: dict[str, tuple[str, ...]] = {
    "gateway": ("redis",),
    "manager": ("transports", "rabbitmq", "redis", "grpc", "llm"),
    "worker": ("transports", "rabbitmq", "redis", "grpc", "llm", "worker"),
}

_MISSING = object()


def _lookup(data: Mapping[str, Any], path: str) -> Any:
    node: Any = data
    for part in path.split("."):
        if not isinstance(node, Mapping) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _valid(model: type[BaseModel], name: str, value: Any) -> Any:
    """Validate one field in isolation; return the value or ``_MISSING`` if invalid."""
    field = model.model_fields[name]
    if value is _MISSING:
        if field.is_required():
            return _MISSING
        return field.get_default(call_default_factory=True)
    tp: Any = field.annotation
    if field.metadata:
        tp = Annotated[tp, *field.metadata]
    try:
        return TypeAdapter(tp).validate_python(value)
    except ValidationError:
        return _MISSING


def _section_present(data: Mapping[str, Any], name: str) -> bool:
    value = _lookup(data, name)
    return value is not _MISSING and value is not None


def _redis_mode_errors(data: Mapping[str, Any]) -> list[FieldError]:
    """The Redis rules that depend on ``mode`` (Phase 7 Req 1.1, 1.2), each naming the field it is about.

    A mode that is not ``standalone`` or ``sentinel`` is reported by the field itself, so this says nothing then."""
    if not _section_present(data, "redis"):
        return []
    raw = _lookup(data, "redis.mode")
    mode = "standalone" if raw is _MISSING else raw.strip().lower() if isinstance(raw, str) else None
    present = {name: _lookup(data, f"redis.{name}") not in (_MISSING, None) for name in ("url", *SENTINEL_ONLY_FIELDS)}
    errors: list[FieldError] = []
    if mode == "standalone":
        if not present["url"]:
            errors.append(FieldError("redis.url", "field required for redis.mode standalone"))
        errors.extend(
            FieldError(f"redis.{name}", "only allowed with redis.mode sentinel")
            for name in SENTINEL_ONLY_FIELDS
            if present[name]
        )
    elif mode == "sentinel":
        if present["url"]:
            errors.append(FieldError("redis.url", "must not be set with redis.mode sentinel; use sentinels"))
        if not present["sentinels"]:
            errors.append(FieldError("redis.sentinels", "field required for redis.mode sentinel (1 to 10 host:port)"))
        if not present["master_name"]:
            errors.append(FieldError("redis.master_name", "field required for redis.mode sentinel"))
    return errors


def cross_field_errors(data: Mapping[str, Any]) -> list[FieldError]:
    """Role-dependent and cross-field checks on the raw resolved mapping.

    Each check runs only when its inputs are individually valid, so it never
    duplicates a per-field error.
    """
    errors: list[FieldError] = []
    role = _valid(AgentConfig, "role", _lookup(data, "role"))

    if role is not _MISSING:
        for section in _REQUIRED_SECTIONS[role]:
            if not _section_present(data, section):
                errors.append(FieldError(section, f"field required for role {role}"))
        if role == "manager" and _section_present(data, "rabbitmq"):
            if _lookup(data, "rabbitmq.management_url") in (_MISSING, None):
                errors.append(
                    FieldError(
                        "rabbitmq.management_url",
                        "field required for role manager (used to apply the das-jobs policy)",
                    )
                )
        if role == "gateway":
            keys = _lookup(data, "api_keys")
            if keys is _MISSING or not _split_csv(keys):
                errors.append(
                    FieldError(
                        "api_keys",
                        "at least 1 API key required for role gateway "
                        "(set DAS__API_KEYS or the secrets file)",
                    )
                )

    errors.extend(_redis_mode_errors(data))

    # Tools (Phase 6 Req 5.3, 5.9): only a Worker enables them, and http_fetch needs an allow-list.
    enabled = _valid(ToolsConfig, "enabled", _lookup(data, "tools.enabled"))
    if enabled is not _MISSING and enabled:
        if role is not _MISSING and role != "worker":
            errors.append(FieldError("tools.enabled", "tools can only be enabled for role worker"))
        if "http_fetch" in enabled:
            hosts = _valid(HttpFetchConfig, "allowed_hosts", _lookup(data, "tools.http_fetch.allowed_hosts"))
            if hosts is not _MISSING and not hosts:
                errors.append(
                    FieldError(
                        "tools.http_fetch.allowed_hosts",
                        "at least 1 host required to enable http_fetch (it fetches only from allowed hosts)",
                    )
                )

    # Sandbox (Phase 6 Req 6.1): `code-execution` needs the sandbox enabled, and only a Worker can have one.
    sandbox_on = _valid(SandboxConfig, "enabled", _lookup(data, "sandbox.enabled"))
    if sandbox_on is not _MISSING:
        if sandbox_on and role is not _MISSING and role != "worker":
            errors.append(FieldError("sandbox.enabled", "the sandbox can only be enabled for role worker"))
        listed = _split_csv(_lookup(data, "worker.capabilities"))
        if not sandbox_on and isinstance(listed, list) and CODE_EXECUTION in listed:
            errors.append(
                FieldError("sandbox.enabled", f"capability {CODE_EXECUTION} needs sandbox.enabled to be true")
            )

    # base_backoff <= max_backoff (Req 6.8)
    base = _valid(LimitsConfig, "base_backoff", _lookup(data, "limits.base_backoff"))
    top = _valid(LimitsConfig, "max_backoff", _lookup(data, "limits.max_backoff"))
    if base is not _MISSING and top is not _MISSING and base > top:
        errors.append(
            FieldError(
                "limits.base_backoff",
                "must be less than or equal to limits.max_backoff "
                "(permitted range 0.1 to 60 seconds, and not above max_backoff)",
            )
        )

    # Provider credentials (Req 10.7). Ollama needs an address; Bedrock may use
    # the default AWS credential chain but a partial static key pair is invalid.
    if _section_present(data, "llm"):
        provider = _valid(LLMConfig, "provider", _lookup(data, "llm.provider"))

        def absent(path: str) -> bool:
            return _lookup(data, path) in (_MISSING, None, "")

        if provider in ("openai", "anthropic") and absent("llm.api_key"):
            errors.append(
                FieldError(
                    "llm.api_key",
                    f"credential required for provider {provider} "
                    "(set DAS__LLM__API_KEY or the secrets file)",
                )
            )
        elif provider == "ollama" and absent("llm.base_url"):
            errors.append(FieldError("llm.base_url", "field required for provider ollama"))
        elif provider == "bedrock":
            key_id, secret = "llm.aws_access_key_id", "llm.aws_secret_access_key"
            if absent(key_id) != absent(secret):
                missing = key_id if absent(key_id) else secret
                errors.append(
                    FieldError(
                        missing,
                        "credential required when the other AWS static key is set "
                        "(set both, or neither to use the default AWS credential chain)",
                    )
                )

    errors.extend(_bucket_errors(data))
    errors.extend(_tracing_errors(data))
    errors.extend(_pricing_errors(data))
    errors.extend(_admin_key_errors(data))

    # Gateway Ops_Port must differ from http_port (Req 1.8).
    if role == "gateway":
        health = _valid(AgentConfig, "health_port", _lookup(data, "health_port"))
        http = _valid(AgentConfig, "http_port", _lookup(data, "http_port"))
        if health is not _MISSING and http is not _MISSING and health == http:
            errors.append(
                FieldError(
                    "health_port",
                    "must differ from http_port (permitted range 1 to 65535, not equal to http_port)",
                )
            )
    return errors


def _bucket_errors(data: Mapping[str, Any]) -> list[FieldError]:
    """Histogram buckets: 1..50 values, all > 0, strictly increasing (Req 2.12)."""
    errors: list[FieldError] = []
    for name in BucketsConfig.model_fields:
        path = f"metrics.buckets.{name}"
        values = _valid(BucketsConfig, name, _lookup(data, path))
        if values is _MISSING:
            continue
        ok = (
            1 <= len(values) <= MAX_BUCKETS
            and all(v > 0 for v in values)
            and all(a < b for a, b in zip(values, values[1:], strict=False))
        )
        if not ok:
            errors.append(
                FieldError(
                    path,
                    f"must be a strictly increasing list of 1 to {MAX_BUCKETS} "
                    "bucket boundaries, each greater than 0",
                )
            )
    return errors


def _is_http_url(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parts = urlsplit(value)
        host = parts.hostname
    except ValueError:
        return False
    return parts.scheme.lower() in ("http", "https") and bool(host)


def _tracing_errors(data: Mapping[str, Any]) -> list[FieldError]:
    """``tracing.otlp_endpoint`` is required and must be an absolute http(s) URL (Req 8.3)."""
    enabled = _valid(TracingConfig, "enabled", _lookup(data, "tracing.enabled"))
    if enabled is not True:
        return []
    if _is_http_url(_lookup(data, "tracing.otlp_endpoint")):
        return []
    return [
        FieldError(
            "tracing.otlp_endpoint",
            "required when tracing.enabled is true; must be an absolute http or https URL "
            "with a host",
        )
    ]


def _pricing_errors(data: Mapping[str, Any]) -> list[FieldError]:
    """Duplicate provider/model pairs, reported by entry index (Req 10.2)."""
    models = _lookup(data, "pricing.models")
    if not isinstance(models, list):
        return []
    errors: list[FieldError] = []
    first: dict[tuple[str, str], int] = {}
    for index, entry in enumerate(models):
        if not isinstance(entry, Mapping):
            continue
        provider, model = entry.get("provider"), entry.get("model")
        if not isinstance(provider, str) or not isinstance(model, str):
            continue
        pair = (provider, model)
        if pair in first:
            errors.append(
                FieldError(
                    f"pricing.models[{index}].model",
                    f"duplicate provider/model pair (same as pricing.models[{first[pair]}])",
                )
            )
        else:
            first[pair] = index
    return errors


def _secret_list(value: Any) -> list[str] | None:
    """Raw key list as strings, or ``None`` when it is not a usable list."""
    if value is _MISSING or value is None:
        return []
    value = _split_csv(value)
    if not isinstance(value, list):
        return None
    out: list[str] = []
    for item in value:
        if isinstance(item, SecretStr):
            item = item.get_secret_value()
        if not isinstance(item, str):
            return None
        out.append(item)
    return out


def _admin_key_errors(data: Mapping[str, Any]) -> list[FieldError]:
    """Admin_API_Key count, length, charset, and overlap with API_Keys (Req 11.10).

    Errors name ``admin_api_keys[i]`` and never include a key value.
    """
    keys = _secret_list(_lookup(data, "admin_api_keys"))
    if not keys:
        return []
    errors: list[FieldError] = []
    if len(keys) > MAX_ADMIN_API_KEYS:
        errors.append(
            FieldError(
                "admin_api_keys",
                f"too many keys; at most {MAX_ADMIN_API_KEYS} Admin_API_Keys are permitted",
            )
        )
    api_keys = set(_secret_list(_lookup(data, "api_keys")) or ())
    for index, key in enumerate(keys):
        path = f"admin_api_keys[{index}]"
        if not ADMIN_KEY_MIN_LENGTH <= len(key) <= ADMIN_KEY_MAX_LENGTH or any(
            not 0x21 <= ord(c) <= 0x7E for c in key
        ):
            errors.append(
                FieldError(
                    path,
                    f"must be {ADMIN_KEY_MIN_LENGTH} to {ADMIN_KEY_MAX_LENGTH} printable ASCII "
                    "characters (0x21 to 0x7E)",
                )
            )
        elif key in api_keys:
            errors.append(FieldError(path, "must not equal an API key in api_keys"))
    return errors


def client_api_errors(config: AgentConfig) -> list[FieldError]:
    """Phase 4 checks that need validated values (Req 1.4-1.6, 8.6).

    For role ``gateway``: Key_Fingerprint collisions, unknown or duplicate override
    fingerprints, and webhook settings. For every role: ``webhook.base_backoff`` is
    at most ``webhook.max_backoff``. Errors name field paths and never include a
    key, secret or fingerprint value.
    """
    errors: list[FieldError] = []
    if config.webhook.base_backoff > config.webhook.max_backoff:
        errors.append(
            FieldError("webhook.base_backoff", "must be less than or equal to webhook.max_backoff")
        )
    if config.role != "gateway":
        return errors

    owners: dict[str, str] = {}
    values: set[str] = set()
    for field, keys in (("api_keys", config.api_keys), ("admin_api_keys", config.admin_api_keys)):
        for index, key in enumerate(keys):
            value = key.get_secret_value()
            if value in values:
                continue  # the same key listed twice is not a collision
            values.add(value)
            path = f"{field}[{index}]"
            fp = key_fingerprint(value.encode("utf-8"))
            if fp in owners:
                errors.append(
                    FieldError(path, f"has the same Key_Fingerprint as {owners[fp]}; replace one of the two keys")
                )
            else:
                owners[fp] = path

    limits = config.client_limits
    first: dict[str, int] = {}
    for index, override in enumerate(limits.overrides):
        path = f"client_limits.overrides[{index}].fingerprint"
        if override.fingerprint in first:
            errors.append(
                FieldError(
                    path,
                    f"duplicate fingerprint (same as client_limits.overrides[{first[override.fingerprint]}])",
                )
            )
            continue
        first[override.fingerprint] = index
        if override.fingerprint not in owners:
            errors.append(FieldError(path, "matches no configured API key or Admin_API_Key"))

    errors.extend(_webhook_errors("client_limits.default", limits.default, limits.default, config))
    for index, override in enumerate(limits.overrides):
        errors.extend(
            _webhook_errors(f"client_limits.overrides[{index}]", override, limits.merged(override), config)
        )
    return errors


def tenant_errors(config: AgentConfig) -> list[FieldError]:
    """Phase 5 checks of the ``tenants`` list that need validated values (Req 1.1, 1.4, 4.2).

    Gateway role only, like :func:`client_api_errors`. A tenant fingerprint must match a configured key of
    ``api_keys`` (not an Admin_API_Key), appear once across all tenants, and a tenant id is unique and not the
    reserved ``default``. A webhook target in a tenant's ``settings`` follows the Phase 4 rules. Errors name field
    paths and never include a key, a secret or a fingerprint value.
    """
    if config.role != "gateway" or not config.tenants:
        return []
    api = {key_fingerprint(k.get_secret_value().encode("utf-8")) for k in config.api_keys}
    admin = {key_fingerprint(k.get_secret_value().encode("utf-8")) for k in config.admin_api_keys}
    errors: list[FieldError] = []
    first_id: dict[str, int] = {}
    first_key: dict[str, str] = {}
    for t_index, tenant in enumerate(config.tenants):
        path = f"tenants[{t_index}]"
        if tenant.id == DEFAULT_TENANT_ID:
            errors.append(FieldError(f"{path}.id", f"{DEFAULT_TENANT_ID!r} is reserved for the implicit tenant"))
        elif tenant.id in first_id:
            errors.append(FieldError(f"{path}.id", f"duplicate id (same as tenants[{first_id[tenant.id]}].id)"))
        else:
            first_id[tenant.id] = t_index
        for k_index, key in enumerate(tenant.keys):
            key_path = f"{path}.keys[{k_index}].fingerprint"
            if key.fingerprint in first_key:
                errors.append(FieldError(key_path, f"already listed at {first_key[key.fingerprint]}"))
                continue
            first_key[key.fingerprint] = f"{path}.keys[{k_index}]"
            if key.fingerprint in admin:
                errors.append(FieldError(key_path, "matches an Admin_API_Key, which no tenant may claim"))
            elif key.fingerprint not in api:
                errors.append(FieldError(key_path, "matches no configured key in api_keys"))
        errors.extend(
            _webhook_errors(f"{path}.settings", tenant.settings, config.client_limits.merged(tenant.settings), config)
        )
    return errors


def _webhook_errors(
    prefix: str,
    own: ClientSettingsConfig | ClientOverride | TenantSettings,
    effective: ClientSettingsConfig,
    config: AgentConfig,
) -> list[FieldError]:
    """Webhook_Target checks for one settings entry (Req 1.5, 1.6).

    ``own`` is the entry as written at ``prefix``; ``effective`` is it merged with the
    default. Only fields the entry sets itself are reported, so an inherited problem
    is reported once, at ``client_limits.default``.
    """
    if own.webhook_url is None and own.webhook_secret_name is None:
        return []
    if (effective.webhook_url is None) != (effective.webhook_secret_name is None):
        missing = "webhook_url" if effective.webhook_url is None else "webhook_secret_name"
        reason = "required when the other webhook field is set (set both, or neither)"
        return [FieldError(f"{prefix}.{missing}", reason)]
    errors: list[FieldError] = []
    name = own.webhook_secret_name
    if name is not None and name not in config.webhook_secrets:
        errors.append(
            FieldError(
                f"{prefix}.webhook_secret_name",
                f"names no configured webhook secret (set DAS__WEBHOOK_SECRETS__{name.upper()} "
                "in the environment or the secrets file)",
            )
        )
    url = own.webhook_url
    insecure_ok = config.client_limits.allow_insecure_webhooks
    if url is not None and urlsplit(url).scheme.lower() != "https" and not insecure_ok:
        errors.append(
            FieldError(
                f"{prefix}.webhook_url",
                "must use https (set client_limits.allow_insecure_webhooks to allow http)",
            )
        )
    return errors


# --------------------------------------------------------------------------- error formatting


def _field_info(loc: tuple[Any, ...]) -> Any:
    """Find the pydantic FieldInfo addressed by an error location, if any."""
    model: Any = AgentConfig
    info = None
    for part in loc:
        if isinstance(part, int):
            continue  # list index: stay on the list's item model
        if not isinstance(part, str) or model is None:
            return None
        fields = getattr(model, "model_fields", None)
        if not fields or part not in fields:
            return None
        info = fields[part]
        model = None
        ann = info.annotation
        for cand in (ann, *getattr(ann, "__args__", ())):
            if isinstance(cand, type) and issubclass(cand, BaseModel):
                model = cand
                break
    return info


def _bounds(info: Any) -> tuple[Any, Any]:
    lo = hi = None
    for m in getattr(info, "metadata", ()):
        lo = getattr(m, "ge", getattr(m, "gt", lo))
        hi = getattr(m, "le", getattr(m, "lt", hi))
    return lo, hi


def _fmt(n: Any) -> str:
    return str(int(n)) if isinstance(n, float) and n.is_integer() else str(n)


_RANGE_TYPES = {"greater_than", "greater_than_equal", "less_than", "less_than_equal"}
_LENGTH_TYPES = {"too_short", "too_long"}


def _path(loc: tuple[Any, ...]) -> str:
    """``("pricing", "models", 3, "model")`` -> ``pricing.models[3].model``."""
    out = ""
    for part in loc:
        if isinstance(part, int):
            out += f"[{part}]"
        else:
            out += f".{part}" if out else str(part)
    return out or "<root>"


def pydantic_errors(exc: ValidationError) -> list[FieldError]:
    """Convert a pydantic ValidationError into FieldErrors without input values."""
    out: list[FieldError] = []
    for err in exc.errors(include_input=False, include_url=False):
        loc = tuple(err["loc"])
        path = _path(loc)
        etype = err["type"]
        reason = err["msg"]
        if etype == "missing":
            reason = "field required"
        elif etype == "extra_forbidden":
            reason = "unknown field"
        elif etype in _RANGE_TYPES:
            lo, hi = _bounds(_field_info(loc))
            if lo is not None and hi is not None:
                reason = f"out of range; permitted range is {_fmt(lo)} to {_fmt(hi)}"
            elif lo is not None:
                reason = f"out of range; must be at least {_fmt(lo)}"
        elif etype in _LENGTH_TYPES:
            info = _field_info(loc)
            lo = hi = None
            for m in getattr(info, "metadata", ()):
                lo = getattr(m, "min_length", lo)
                hi = getattr(m, "max_length", hi)
            if lo is not None and hi is not None:
                reason = f"must contain between {lo} and {hi} items"
        elif etype == "value_error":
            reason = reason.removeprefix("Value error, ")
        out.append(FieldError(path, reason))
    return out


def validate_config(data: Mapping[str, Any]) -> AgentConfig:
    """Validate a resolved mapping, reporting every invalid field (Req 12.2).

    Raises :class:`das.errors.ConfigError`.
    """
    from das.errors import ConfigError

    errors: list[FieldError] = []
    config: AgentConfig | None = None
    try:
        config = AgentConfig.model_validate(dict(data))
    except ValidationError as exc:
        errors.extend(pydantic_errors(exc))
    seen = {e.field for e in errors}
    errors.extend(e for e in cross_field_errors(data) if e.field not in seen)
    if config is not None:
        seen = {e.field for e in errors}
        errors.extend(e for e in client_api_errors(config) if e.field not in seen)
        seen = {e.field for e in errors}
        errors.extend(e for e in tenant_errors(config) if e.field not in seen)
    if errors or config is None:
        raise ConfigError(errors, reason="invalid configuration")
    return config


__all__ = [
    "CAPABILITY_PATTERN",
    "PROVIDERS",
    "ROLES",
    "SECRET_FIELDS",
    "DEFAULT_ATTEMPTS_BUCKETS",
    "DEFAULT_DURATION_BUCKETS",
    "DEFAULT_HTTP_BUCKETS",
    "MAX_ADMIN_API_KEYS",
    "MAX_BUCKETS",
    "MAX_CLIENT_OVERRIDES",
    "MAX_TENANTS",
    "MAX_TENANT_KEYS",
    "MIN_RATE",
    "SECRET_NAME_PATTERN",
    "SECRET_PREFIX_FIELDS",
    "TENANT_ID_PATTERN",
    "WEBHOOK_SECRET_MIN_LENGTH",
    "AgentConfig",
    "BucketsConfig",
    "BudgetConfig",
    "ClientLimitsConfig",
    "ClientOverride",
    "ClientSettingsConfig",
    "EventsConfig",
    "WebhookConfig",
    "GatewayConfig",
    "GrpcConfig",
    "LLMConfig",
    "LabelLimitsConfig",
    "LimitsConfig",
    "MetricsConfig",
    "PricingConfig",
    "PricingEntryConfig",
    "RabbitMQConfig",
    "RedisConfig",
    "RetentionConfig",
    "TenantConfig",
    "TenantKeyConfig",
    "TenantSettings",
    "TracingConfig",
    "TransportsConfig",
    "UsageConfig",
    "WorkerConfig",
    "client_api_errors",
    "cross_field_errors",
    "parse_otlp_headers",
    "pydantic_errors",
    "tenant_errors",
    "validate_config",
]
