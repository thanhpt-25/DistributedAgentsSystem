"""The chart accepts Redis Sentinel (das-scale-resilience task 11.1; Phase 7 Req 1.1, 1.2, 1.3, 1.9).

Rendered with the real ``helm``: ``redis.mode: sentinel`` with ``redis.sentinels`` and ``redis.masterName`` reaches
every role as the ``DAS__REDIS__*`` variables the agent config reads, the required ``REDIS_URL`` secret entry is left
out, and the values that cannot work are refused with a message that names the value and never repeats a credential."""

from __future__ import annotations

import subprocess

import pytest

from tests.unit.test_chart_skeleton import CHART, MIN, needs_helm
from tests.unit.test_chart_templates import container, env, of_kind, render, secret_ref

pytestmark = needs_helm

SENTINELS = "redis.sentinels={s-0.redis:26379,s-1.redis:26379,s-2.redis:26379}"
SENTINEL_SETS = ["redis.mode=sentinel", SENTINELS, "redis.masterName=das"]
ROLES = ("release-das-gateway", "release-das-manager", "release-das-worker-research")


def refusal(sets: list[str]) -> str:
    cmd = ["helm", "template", "release", str(CHART), "-f", str(MIN)]
    for s in sets:
        cmd += ["--set", s]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert out.returncode != 0, "the chart rendered values that should have been refused"
    return out.stderr


# --------------------------------------------------------------------------- what it renders


def test_the_default_is_unchanged_one_instance_with_its_url_from_the_secret() -> None:
    for name, deployment in of_kind(render(), "Deployment").items():
        variables = env(deployment)
        assert secret_ref(variables["DAS__REDIS__URL"]) is not None, name
        assert not [v for v in variables if v.startswith("DAS__REDIS__") and v != "DAS__REDIS__URL"], name


def test_every_role_gets_the_sentinel_settings_as_environment_variables() -> None:
    deployments = of_kind(render(sets=SENTINEL_SETS), "Deployment")
    assert sorted(deployments) == sorted(ROLES)
    for name, deployment in deployments.items():
        variables = env(deployment)
        assert variables["DAS__REDIS__MODE"]["value"] == "sentinel", name
        assert variables["DAS__REDIS__SENTINELS"]["value"] == "s-0.redis:26379,s-1.redis:26379,s-2.redis:26379", name
        assert variables["DAS__REDIS__MASTER_NAME"]["value"] == "das" and variables["DAS__REDIS__DB"]["value"] == "0"


def test_in_sentinel_mode_there_is_no_redis_url_and_the_secret_need_not_hold_one() -> None:
    for name, deployment in of_kind(render(sets=SENTINEL_SETS), "Deployment").items():
        assert "DAS__REDIS__URL" not in env(deployment), name
    # the other required secrets are still required
    manager = env(of_kind(render(sets=SENTINEL_SETS), "Deployment")["release-das-manager"])
    assert secret_ref(manager["DAS__RABBITMQ__URL"]) is not None


def test_the_database_is_passed_on() -> None:
    gateway = of_kind(render(sets=[*SENTINEL_SETS, "redis.db=5"]), "Deployment")["release-das-gateway"]
    assert env(gateway)["DAS__REDIS__DB"]["value"] == "5"


def test_the_passwords_come_from_the_secret_like_every_other_secret() -> None:
    sets = [*SENTINEL_SETS, "secretEnv.manager.DAS__REDIS__PASSWORD=REDIS_PASSWORD",
            "secretEnv.manager.DAS__REDIS__SENTINEL_PASSWORD=SENTINEL_PASSWORD"]
    manager = env(of_kind(render(sets=sets), "Deployment")["release-das-manager"])
    for name, key in (
        ("DAS__REDIS__PASSWORD", "REDIS_PASSWORD"), ("DAS__REDIS__SENTINEL_PASSWORD", "SENTINEL_PASSWORD"),
    ):
        ref = secret_ref(manager[name])
        assert ref is not None and ref["key"] == key and ref["optional"] is True and "value" not in manager[name]


def test_nothing_of_sentinel_is_rendered_into_the_config_maps_as_a_secret() -> None:
    sets = [*SENTINEL_SETS, "secretEnv.manager.DAS__REDIS__PASSWORD=REDIS_PASSWORD"]
    assert "REDIS_PASSWORD" not in "".join(str(d) for d in of_kind(render(sets=sets), "ConfigMap").values())


def test_a_standalone_url_is_still_a_plain_value_when_given() -> None:
    gateway = of_kind(render(sets=["redis.url=redis://redis.infra:6379/0"]), "Deployment")["release-das-gateway"]
    assert env(gateway)["DAS__REDIS__URL"]["value"] == "redis://redis.infra:6379/0"
    assert "DAS__REDIS__MODE" not in env(gateway)
    assert container(gateway)["name"]


# --------------------------------------------------------------------------- what it refuses (Req 1.1, 1.2, 1.3)


def test_cluster_is_refused_with_the_reason() -> None:
    message = refusal(["redis.mode=cluster"])
    assert "not supported" in message and "different slots" in message


def test_an_unknown_mode_is_refused() -> None:
    assert "redis.mode must be standalone or sentinel" in refusal(["redis.mode=primary"])


def test_a_url_beside_sentinels_is_refused() -> None:
    assert "redis.url must not be set" in refusal([*SENTINEL_SETS, "redis.url=redis://redis:6379/0"])


def test_sentinel_without_a_master_name_is_refused() -> None:
    assert "redis.masterName is required" in refusal(["redis.mode=sentinel", SENTINELS])


def test_sentinel_without_sentinels_is_refused() -> None:
    assert "redis.sentinels needs 1 to 10" in refusal(["redis.mode=sentinel", "redis.masterName=das"])


def test_eleven_sentinels_are_refused() -> None:
    many = "redis.sentinels={" + ",".join(f"s{n}:26379" for n in range(11)) + "}"
    assert "maxItems" in refusal(["redis.mode=sentinel", "redis.masterName=das", many])  # the schema says so first


def test_ten_sentinels_are_accepted() -> None:
    many = "redis.sentinels={" + ",".join(f"s{n}:26379" for n in range(10)) + "}"
    assert render(sets=["redis.mode=sentinel", "redis.masterName=das", many])


@pytest.mark.parametrize("address", ["host", "host:0", "host:65536", ":26379", "bad host:26379", "host:026379"])
def test_a_sentinel_address_that_is_not_host_and_port_is_refused(address: str) -> None:
    message = refusal(["redis.mode=sentinel", "redis.masterName=das", f"redis.sentinels={{{address}}}"])
    assert "redis.sentinels" in message


def test_a_sentinel_address_with_credentials_is_refused_without_repeating_them() -> None:
    message = refusal(["redis.mode=sentinel", "redis.masterName=das", "redis.sentinels={user:hunter2secret@s:26379}"])
    assert "without credentials" in message and "hunter2secret" not in message


@pytest.mark.parametrize("sets", [["redis.sentinels={s:26379}"], ["redis.masterName=das"]])
def test_sentinel_values_without_sentinel_mode_are_refused(sets: list[str]) -> None:
    assert "only allowed with redis.mode sentinel" in refusal(sets)


@pytest.mark.parametrize("value", ["16", "-1"])
def test_a_database_outside_0_to_15_is_refused_by_the_schema(value: str) -> None:
    assert "redis" in refusal([*SENTINEL_SETS, f"redis.db={value}"])
