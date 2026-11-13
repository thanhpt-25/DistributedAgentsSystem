"""``limits.max_replans`` (das-smarter-agents task 3.1; Phase 6 Req 3.1): 0 to 3, default 0 (replanning off)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from das.config import load_config
from das.errors import ConfigError

ENV = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}


def manager_cfg(**limits: object) -> dict:
    return {
        "role": "manager",
        "host": "10.0.0.7",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/", "management_url": "http://rabbit.internal:15672"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "big-model"},
        "limits": limits,
    }


def load(tmp_path: Path, cfg: dict, env: dict[str, str] | None = None):  # noqa: ANN201
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return load_config(path, ENV if env is None else {**ENV, **env})


def test_replanning_is_off_by_default(tmp_path: Path) -> None:
    assert load(tmp_path, manager_cfg()).limits.max_replans == 0


@pytest.mark.parametrize("value", [0, 1, 2, 3])
def test_the_allowed_values_load(tmp_path: Path, value: int) -> None:
    assert load(tmp_path, manager_cfg(max_replans=value)).limits.max_replans == value


@pytest.mark.parametrize("value", [-1, 4, 100])
def test_a_value_out_of_range_names_the_field(tmp_path: Path, value: int) -> None:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, manager_cfg(max_replans=value))
    assert "limits.max_replans" in info.value.fields


def test_the_value_can_come_from_the_environment(tmp_path: Path) -> None:
    assert load(tmp_path, manager_cfg(), {"DAS__LIMITS__MAX_REPLANS": "2"}).limits.max_replans == 2


def test_the_manager_settings_carry_it_to_the_planner(tmp_path: Path) -> None:
    from das.manager.agent import ManagerSettings

    assert ManagerSettings.from_config(load(tmp_path, manager_cfg(max_replans=2))).planner_config.max_replans == 2
    assert ManagerSettings.from_config(load(tmp_path, manager_cfg())).planner_config.max_replans == 0


def test_a_non_number_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, manager_cfg(max_replans="often"))
    assert "limits.max_replans" in info.value.fields
