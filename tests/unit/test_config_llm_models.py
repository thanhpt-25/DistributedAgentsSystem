"""``llm.capability_models`` and ``llm.fallback_model`` (das-smarter-agents task 2.2; Req 2.1, 2.8)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from das.config import load_config
from das.errors import ConfigError

ENV = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}


def worker_cfg(**llm: object) -> dict:
    return {
        "role": "worker",
        "host": "10.0.0.7",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "big-model", **llm},
        "worker": {"capabilities": ["research", "summarize"]},
    }


def load(tmp_path: Path, cfg: dict, env: dict[str, str] | None = None):  # noqa: ANN201
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return load_config(path, ENV if env is None else {**ENV, **env})


def refused(tmp_path: Path, cfg: dict) -> tuple[str, ...]:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, cfg)
    return tuple(info.value.fields)


# --------------------------------------------------------------------------- defaults (Req 2.1)


def test_both_are_absent_by_default(tmp_path: Path) -> None:
    llm = load(tmp_path, worker_cfg()).llm
    assert llm is not None
    assert llm.capability_models == {} and llm.fallback_model is None


def test_both_can_be_set(tmp_path: Path) -> None:
    llm = load(
        tmp_path, worker_cfg(capability_models={"summarize": "small-model"}, fallback_model="backup-model")
    ).llm
    assert llm is not None
    assert llm.capability_models == {"summarize": "small-model"}
    assert llm.fallback_model == "backup-model"


def test_the_fallback_can_come_from_the_environment(tmp_path: Path) -> None:
    llm = load(tmp_path, worker_cfg(), {"DAS__LLM__FALLBACK_MODEL": "from-env"}).llm
    assert llm is not None and llm.fallback_model == "from-env"


def test_a_fallback_equal_to_the_model_is_allowed_and_simply_never_used(tmp_path: Path) -> None:
    llm = load(tmp_path, worker_cfg(fallback_model="big-model")).llm
    assert llm is not None and llm.fallback_model == "big-model"


def test_a_capability_model_for_a_capability_this_worker_lacks_is_not_an_error(tmp_path: Path) -> None:
    llm = load(tmp_path, worker_cfg(capability_models={"translate": "other-model"})).llm
    assert llm is not None and llm.capability_models == {"translate": "other-model"}


# --------------------------------------------------------------------------- empty names (Req 2.8)


@pytest.mark.parametrize("blank", ["", "   "])
def test_an_empty_fallback_model_names_its_field(tmp_path: Path, blank: str) -> None:
    assert "llm.fallback_model" in refused(tmp_path, worker_cfg(fallback_model=blank))


@pytest.mark.parametrize("blank", ["", "  "])
def test_an_empty_capability_model_names_the_capability(tmp_path: Path, blank: str) -> None:
    fields = refused(tmp_path, worker_cfg(capability_models={"summarize": blank}))
    assert any(f.startswith("llm.capability_models") and "summarize" in f for f in fields), fields


def test_a_capability_name_that_is_not_a_valid_capability_is_refused(tmp_path: Path) -> None:
    fields = refused(tmp_path, worker_cfg(capability_models={"not a capability!": "m"}))
    assert any(f.startswith("llm.capability_models") for f in fields), fields


def test_an_unknown_llm_field_is_still_refused(tmp_path: Path) -> None:
    assert "llm.capability_model" in refused(tmp_path, worker_cfg(capability_model={"a": "b"}))


def test_a_model_name_is_trimmed_of_surrounding_space(tmp_path: Path) -> None:
    llm = load(tmp_path, worker_cfg(fallback_model="  backup  ", capability_models={"research": " small "})).llm
    assert llm is not None and llm.fallback_model == "backup" and llm.capability_models == {"research": "small"}
