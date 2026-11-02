### Task 2: `resolve_settings` (spec task 2.1, `das/client/settings.py`)

**Files:**
- Create: `das/client/settings.py`
- Test: `tests/unit/test_client_settings.py`

**Interfaces:**
- Consumes: `das.client.budget.Budget`; `ClientLimitsConfig`, `BudgetConfig`, `ClientOverride`, `ClientSettingsConfig` from Task 1.
- Produces: `ClientSettings(fingerprint, rate, burst, max_active_requests, max_streams, default_budget: Budget | None, webhook_url, webhook_secret_name)` (frozen dataclass); `resolve_settings(fingerprint: str, limits: ClientLimitsConfig) -> ClientSettings`. The Gateway (task group 7) calls it once per authenticated call.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_client_settings.py` with exactly this content:

```python
"""Effective Client_Settings (Phase 4 Req 1.2, 1.3)."""
from __future__ import annotations

from decimal import Decimal

from das.client.budget import Budget
from das.client.settings import ClientSettings, resolve_settings
from das.config.schema import BudgetConfig, ClientLimitsConfig, ClientOverride, ClientSettingsConfig

FP = "cbabecbd"


def test_default_settings() -> None:
    assert resolve_settings(FP, ClientLimitsConfig()) == ClientSettings(
        fingerprint=FP,
        rate=60.0,
        burst=120,
        max_active_requests=100,
        max_streams=10,
        default_budget=None,
        webhook_url=None,
        webhook_secret_name=None,
    )


def test_override_fields_win_and_others_inherit() -> None:
    limits = ClientLimitsConfig(
        default=ClientSettingsConfig(rate=10, max_streams=3, default_budget=BudgetConfig(max_total_tokens=500)),
        overrides=[ClientOverride(fingerprint=FP, rate=0, webhook_url="https://h.example.com/x",
                                  webhook_secret_name="partner_a")],
    )
    s = resolve_settings(FP, limits)
    assert (s.rate, s.burst, s.max_streams) == (0.0, 120, 3)
    assert s.default_budget == Budget(max_total_tokens=500)
    assert (s.webhook_url, s.webhook_secret_name) == ("https://h.example.com/x", "partner_a")


def test_other_fingerprint_gets_the_default() -> None:
    limits = ClientLimitsConfig(
        default=ClientSettingsConfig(burst=7), overrides=[ClientOverride(fingerprint=FP, burst=99)]
    )
    assert resolve_settings("ffffffff", limits).burst == 7


def test_budget_conversion() -> None:
    limits = ClientLimitsConfig(
        default=ClientSettingsConfig(default_budget=BudgetConfig(max_cost=Decimal("1.25")))
    )
    assert resolve_settings(FP, limits).default_budget == Budget(max_cost=Decimal("1.25"))


def test_empty_budget_is_none() -> None:
    limits = ClientLimitsConfig(default=ClientSettingsConfig(default_budget=BudgetConfig()))
    assert resolve_settings(FP, limits).default_budget is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_settings.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.settings'`

- [ ] **Step 3: Write the implementation**

Create `das/client/settings.py` with exactly this content:

```python
"""Effective Client_Settings for one Key_Fingerprint (Phase 4 Req 1.2, 1.3). Pure."""
from __future__ import annotations

from dataclasses import dataclass

from das.client.budget import Budget
from das.config.schema import BudgetConfig, ClientLimitsConfig


@dataclass(frozen=True, slots=True)
class ClientSettings:
    """Limits, default Budget and Webhook_Target that apply to one API key.

    ``rate``, ``max_active_requests`` or ``max_streams`` equal to 0 disables that limit.
    """

    fingerprint: str
    rate: float
    burst: int
    max_active_requests: int
    max_streams: int
    default_budget: Budget | None
    webhook_url: str | None
    webhook_secret_name: str | None


def _budget(config: BudgetConfig | None) -> Budget | None:
    if config is None:
        return None
    budget = Budget(max_total_tokens=config.max_total_tokens, max_cost=config.max_cost)
    return None if budget.is_empty() else budget


def resolve_settings(fingerprint: str, limits: ClientLimitsConfig) -> ClientSettings:
    """``client_limits.default`` merged with the override for ``fingerprint``, if any (Req 1.3)."""
    s = limits.settings_for(fingerprint)
    return ClientSettings(
        fingerprint=fingerprint,
        rate=s.rate,
        burst=s.burst,
        max_active_requests=s.max_active_requests,
        max_streams=s.max_streams,
        default_budget=_budget(s.default_budget),
        webhook_url=s.webhook_url,
        webhook_secret_name=s.webhook_secret_name,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_settings.py -q -W error`
Expected: PASS (5 tests)

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_settings.py && uv run mypy && uv run lint-imports`
Expected: `All checks passed!`, `Success: no issues found`, `Contracts: 1 kept, 0 broken.`

- [ ] **Step 6: Tick the spec task**

In `.kiro/specs/das-client-api/tasks.md` tick `2.1 Add config sections and validation` (its `das/client/settings.py` deliverable lands here) and the parent `2. Configuration` (2.2 was ticked in Task 1).

---

