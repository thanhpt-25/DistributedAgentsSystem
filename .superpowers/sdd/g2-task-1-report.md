# G2 Task 1 report: client API config sections and validation (spec 2.1, 2.2)

Status: DONE

## What changed
- `tests/unit/test_config_client_api.py`: new, copied verbatim from `.superpowers/sdd/g2/test_config_client_api.py`.
- `das/config/schema.py`: applied `.superpowers/sdd/g2/schema.diff` (BudgetConfig, ClientSettingsConfig, ClientOverride, ClientLimitsConfig, EventsConfig, WebhookConfig, AgentConfig fields, webhook_secrets in SECRET_FIELDS, SECRET_PREFIX_FIELDS, client_api_errors wired into validate_config).
- `das/config/secrets.py`: applied `.superpowers/sdd/g2/secrets.diff` (secret_path, prefix-field handling).
- `tests/fixtures/phase3_baseline/contract.json`: regenerated (Step 6).
- `.kiro/specs/das-client-api/tasks.md`: ticked 2.1 and 2.2 only.
Both patches applied cleanly with no fuzz, .orig or .rej files.

## TDD evidence
RED: `uv run pytest tests/unit/test_config_client_api.py -q` -> `44 failed, 4 passed`. Failures were `client_limits: unknown field` / `webhook_secrets: unknown field` (and missing attribute), as expected. The 4 passing before the change are ones whose assertions hold either way: test_same_key_listed_twice_is_not_a_collision, test_secrets_file_rejects_unknown_names, test_errors_never_contain_secret_values, test_invalid_client_config_exits_2_before_connecting (an unknown field already fails config with exit 2).
GREEN: `uv run pytest tests/unit/test_config_client_api.py -q -W error` -> `48 passed in 0.26s`.

## Step 6 baseline comparison
Before: `2 failed, 4 passed` (config_schema, env_vars).
```
corpus unchanged
config_schema CHANGED
env_vars CHANGED
exit_codes same
metrics same
routes same
added env vars: [the exact 20 names listed in the brief]
removed env vars: []
```
After: `6 passed`.

## Step 7
68 passed in 4.97s.

## Step 8
- `uv run mypy`: Success: no issues found in 106 source files.
- `uv run ruff check`: 2 errors, both in `.superpowers/sdd/snap-g2-1/das/transport/proto/` (a snapshot copy of generated stubs inside the workflow folder, not project code). `uv run ruff check --extend-exclude .superpowers`: All checks passed!

## Self-review / concerns
- Implementation is exactly the provided diffs; nothing extra added. No new warnings (-W error run clean).
- Concern (minor, not caused by this task): bare `uv run ruff check` picks up the `.superpowers/sdd/snap-g2-1/` snapshot. Excluding `.superpowers` in ruff config, or deleting the snapshot, would make it pass; I changed neither.
