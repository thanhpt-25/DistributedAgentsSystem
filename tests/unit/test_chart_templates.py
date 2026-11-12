"""The chart's workload templates, rendered with the real ``helm`` (das-multi-tenant task 10.2;
Phase 5 Req 10.1, 10.2, 10.3, 10.5). The optional features (KEDA, preStop drain) are task 10.3."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.unit.test_chart_skeleton import ALL, CHART, MIN, needs_helm

pytestmark = needs_helm

SECRET = "das-secrets"


def render(
    values: list[Path] | None = None, sets: list[str] | None = None, release: str = "release"
) -> list[dict[str, Any]]:
    cmd = ["helm", "template", release, str(CHART)]
    for v in values or [MIN]:
        cmd += ["-f", str(v)]
    for s in sets or []:
        cmd += ["--set", s]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def of_kind(docs: list[dict[str, Any]], kind: str) -> dict[str, dict[str, Any]]:
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == kind}


def container(deployment: dict[str, Any]) -> dict[str, Any]:
    (c,) = deployment["spec"]["template"]["spec"]["containers"]
    return c


def env(deployment: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {e["name"]: e for e in container(deployment).get("env", [])}


def secret_ref(e: dict[str, Any]) -> dict[str, Any] | None:
    return e.get("valueFrom", {}).get("secretKeyRef")


def config(docs: list[dict[str, Any]], role: str) -> dict[str, Any]:
    return yaml.safe_load(of_kind(docs, "ConfigMap")[f"release-das-{role}"]["data"]["config.yaml"])


# --------------------------------------------------------------------------- what is rendered


def test_the_minimal_values_render_the_three_roles_and_their_companions() -> None:
    docs = render()
    assert sorted(of_kind(docs, "Deployment")) == [
        "release-das-gateway", "release-das-manager", "release-das-worker-research",
    ]  # fmt: skip
    assert sorted(of_kind(docs, "ConfigMap")) == ["release-das-gateway", "release-das-manager", "release-das-worker"]
    assert sorted(of_kind(docs, "Service")) == ["release-das-gateway"]
    assert sorted(of_kind(docs, "PodDisruptionBudget")) == [
        "release-das-gateway", "release-das-manager", "release-das-worker-research",
    ]  # fmt: skip
    assert sorted(of_kind(docs, "ServiceAccount")) == ["release-das"]
    assert {d["kind"] for d in docs} == {"Deployment", "ConfigMap", "Service", "PodDisruptionBudget", "ServiceAccount"}


def test_there_is_one_worker_deployment_per_workers_entry_with_its_capabilities_and_replicas() -> None:
    deployments = of_kind(render([ALL], sets=["keda.enabled=false"]), "Deployment")  # with KEDA it owns the replicas
    research, review = deployments["release-das-worker-research"], deployments["release-das-worker-review"]
    assert env(research)["DAS__WORKER__CAPABILITIES"]["value"] == "research,summarize"
    assert env(review)["DAS__WORKER__CAPABILITIES"]["value"] == "code-review"
    assert (research["spec"]["replicas"], review["spec"]["replicas"]) == (2, 1)
    assert deployments["release-das-gateway"]["spec"]["replicas"] == 3
    assert deployments["release-das-manager"]["spec"]["replicas"] == 2


def test_no_redis_rabbitmq_or_job_is_deployed() -> None:
    docs = render([ALL])
    assert not {d["kind"] for d in docs} & {"StatefulSet", "Job", "CronJob", "PersistentVolumeClaim", "Secret"}
    names = " ".join(d["metadata"]["name"] for d in docs)
    assert "redis" not in names and "rabbitmq" not in names


def test_workers_may_be_empty() -> None:
    docs = render(sets=["workers=null"])
    assert sorted(of_kind(docs, "Deployment")) == ["release-das-gateway", "release-das-manager"]


# --------------------------------------------------------------------------- every pod


@pytest.mark.parametrize("values", [[MIN], [ALL]], ids=["min", "all"])
def test_every_pod_advertises_its_ip_names_itself_and_probes_the_health_port(values: list[Path]) -> None:
    for name, d in of_kind(render(values), "Deployment").items():
        e = env(d)
        assert e["DAS__GRPC__ADVERTISE_HOST"]["valueFrom"] == {"fieldRef": {"fieldPath": "status.podIP"}}, name
        assert e["DAS__AGENT_ID"]["valueFrom"] == {"fieldRef": {"fieldPath": "metadata.name"}}, name
        c = container(d)
        assert c["startupProbe"]["httpGet"] == {"path": "/livez", "port": "health"}, name
        assert c["livenessProbe"]["httpGet"] == {"path": "/livez", "port": "health"}, name
        assert c["readinessProbe"]["httpGet"] == {"path": "/readyz", "port": "health"}, name
        ports = {p["name"]: p["containerPort"] for p in c["ports"]}
        assert ports["health"] == 8081 and ports["grpc"] == 50051, name
        assert c["args"] == ["run", "--config", "/etc/das/config.yaml"], name


def test_the_gateway_also_exposes_its_http_port_and_the_service_targets_it() -> None:
    docs = render()
    gateway = of_kind(docs, "Deployment")["release-das-gateway"]
    assert {p["name"]: p["containerPort"] for p in container(gateway)["ports"]}["http"] == 8080
    (port,) = of_kind(docs, "Service")["release-das-gateway"]["spec"]["ports"]
    assert (port["port"], port["targetPort"]) == (8080, "http")


@pytest.mark.parametrize(("sets", "grace"), [([], 45), (["terminationGracePeriodSeconds=60"], 60)])
def test_the_grace_period_comes_from_the_values(sets: list[str], grace: int) -> None:
    for d in of_kind(render(sets=sets), "Deployment").values():
        assert d["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == grace


def test_the_pods_run_with_a_restricted_security_context_and_no_api_token() -> None:
    for d in of_kind(render(), "Deployment").values():
        pod = d["spec"]["template"]["spec"]
        assert pod["automountServiceAccountToken"] is False
        assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
        sc = container(d)["securityContext"]
        assert sc["allowPrivilegeEscalation"] is False and sc["capabilities"] == {"drop": ["ALL"]}


# --------------------------------------------------------------------------- secrets and configuration


def test_secrets_only_ever_come_from_the_named_secret_through_secret_key_ref() -> None:
    for name, d in of_kind(render([ALL]), "Deployment").items():
        for var, e in env(d).items():
            ref = secret_ref(e)
            if ref is not None:
                assert ref["name"] == SECRET, (name, var)
            if var.startswith(("DAS__API", "DAS__ADMIN", "DAS__LLM__API", "DAS__WEBHOOK_SECRETS")):
                assert "value" not in e, f"{name}: {var} must not carry a plain value"


def test_the_required_connection_secrets_are_required_and_the_rest_are_optional() -> None:
    deployments = of_kind(render(), "Deployment")
    gateway, manager = env(deployments["release-das-gateway"]), env(deployments["release-das-manager"])
    assert secret_ref(gateway["DAS__API_KEYS"]) == {"name": SECRET, "key": "API_KEYS"}
    assert secret_ref(gateway["DAS__REDIS__URL"]) == {"name": SECRET, "key": "REDIS_URL"}
    assert secret_ref(manager["DAS__RABBITMQ__URL"]) == {"name": SECRET, "key": "RABBITMQ_URL"}
    assert secret_ref(gateway["DAS__ADMIN_API_KEYS"]) == {"name": SECRET, "key": "ADMIN_API_KEYS", "optional": True}
    assert secret_ref(manager["DAS__LLM__API_KEY"])["optional"] is True  # type: ignore[index]


def test_a_credential_free_url_value_replaces_the_secret_entry_of_the_same_name() -> None:
    docs = render(sets=["redis.url=redis://redis.data.svc:6379/0", "rabbitmq.url=amqp://rabbitmq.data.svc:5672/"])
    deployments = of_kind(docs, "Deployment")
    for role in ("gateway", "manager", "worker-research"):
        assert env(deployments[f"release-das-{role}"])["DAS__REDIS__URL"] == {
            "name": "DAS__REDIS__URL", "value": "redis://redis.data.svc:6379/0",
        }  # fmt: skip
    assert env(deployments["release-das-manager"])["DAS__RABBITMQ__URL"]["value"] == "amqp://rabbitmq.data.svc:5672/"
    assert "DAS__RABBITMQ__URL" not in env(deployments["release-das-gateway"])  # the Gateway does not use RabbitMQ


def test_the_manager_alone_gets_the_rabbitmq_management_url() -> None:
    deployments = of_kind(render(sets=["rabbitmq.managementUrl=http://rabbitmq.data.svc:15672"]), "Deployment")
    manager = env(deployments["release-das-manager"])
    assert manager["DAS__RABBITMQ__MANAGEMENT_URL"]["value"] == "http://rabbitmq.data.svc:15672"
    for role in ("gateway", "worker-research"):
        assert "DAS__RABBITMQ__MANAGEMENT_URL" not in env(deployments[f"release-das-{role}"])


def test_the_config_files_carry_the_role_the_baseline_and_the_common_settings() -> None:
    docs = render()
    gateway, manager, worker = (config(docs, r) for r in ("gateway", "manager", "worker"))
    assert (gateway["role"], manager["role"], worker["role"]) == ("gateway", "manager", "worker")
    assert gateway["http_port"] == 8080 and "http_port" not in manager
    for cfg in (gateway, manager, worker):
        assert cfg["health_port"] == 8081
        assert cfg["transports"] == {"jobs": "rabbitmq", "control": "grpc"}
        assert cfg["grpc"] == {"bind": "0.0.0.0:50051", "advertise_port": 50051}
        assert cfg["limits"]["shutdown_timeout"] == 30  # from config.common


def test_role_settings_override_common_and_the_baseline_but_never_the_role() -> None:
    docs = render(
        sets=[
            "config.common.limits.job_timeout=120",
            "config.manager.limits.job_timeout=90",
            "config.gateway.http_port=9090",
            "config.gateway.role=manager",
        ]
    )
    assert config(docs, "manager")["limits"]["job_timeout"] == 90
    assert config(docs, "worker")["limits"]["job_timeout"] == 120
    assert config(docs, "gateway")["http_port"] == 9090
    assert config(docs, "gateway")["role"] == "gateway"


def test_the_tenant_configuration_reaches_the_gateway_config_only() -> None:
    docs = render([ALL])
    assert [t["id"] for t in config(docs, "gateway")["tenants"]] == ["team-a"]
    assert "tenants" not in config(docs, "manager") and "tenants" not in config(docs, "worker")


def test_the_config_maps_hold_no_secret_reference_or_value() -> None:
    for cm in of_kind(render([ALL]), "ConfigMap").values():
        text = yaml.safe_dump(cm["data"])
        for forbidden in ("secretKeyRef", "API_KEYS", "REDIS_URL", "RABBITMQ_URL", SECRET):
            assert forbidden not in text


def test_a_config_change_changes_the_pod_template_so_the_pods_roll() -> None:
    def checksum(sets: list[str]) -> dict[str, str]:
        return {
            n: d["spec"]["template"]["metadata"]["annotations"]["checksum/config"]
            for n, d in of_kind(render(sets=sets), "Deployment").items()
        }

    before = checksum([])
    # A change to one role's config rolls that role's pods only, for every role.
    for role, deployment in (
        ("gateway", "release-das-gateway"),
        ("manager", "release-das-manager"),
        ("worker", "release-das-worker-research"),
    ):
        after = checksum([f"config.{role}.limits.max_jobs=11"])
        assert after[deployment] != before[deployment], role
        assert {n: c for n, c in after.items() if n != deployment} == {
            n: c for n, c in before.items() if n != deployment
        }, role
    # A change to the common config rolls them all.
    assert all(checksum(["config.common.limits.shutdown_timeout=31"])[n] != c for n, c in before.items())


# --------------------------------------------------------------------------- names, labels, images


def test_images_follow_the_role_and_the_tag_defaults_to_the_app_version() -> None:
    d = of_kind(render(), "Deployment")
    assert container(d["release-das-gateway"])["image"] == "das-gateway:0.1.0"
    assert container(d["release-das-worker-research"])["image"] == "das-worker:0.1.0"
    d = of_kind(render([ALL]), "Deployment")
    assert container(d["release-das-manager"])["image"] == "das-manager:ci"
    assert container(d["release-das-manager"])["imagePullPolicy"] == "Never"
    pod = d["release-das-manager"]["spec"]["template"]["spec"]
    assert pod["imagePullSecrets"] == [{"name": "registry-credentials"}]


def test_selectors_match_pod_labels_and_are_distinct_per_deployment() -> None:
    docs = render([ALL])
    deployments = of_kind(docs, "Deployment")
    seen: set[frozenset[tuple[str, str]]] = set()
    for name, d in deployments.items():
        selector = d["spec"]["selector"]["matchLabels"]
        assert selector.items() <= d["spec"]["template"]["metadata"]["labels"].items(), name
        seen.add(frozenset(selector.items()))
    assert len(seen) == 4  # gateway, manager, two workers: no selector overlaps another
    for name, pdb in of_kind(docs, "PodDisruptionBudget").items():
        assert pdb["spec"]["selector"]["matchLabels"] == deployments[name]["spec"]["selector"]["matchLabels"]
        assert pdb["spec"]["maxUnavailable"] == 1
    service = of_kind(docs, "Service")["release-das-gateway"]
    assert service["spec"]["selector"] == deployments["release-das-gateway"]["spec"]["selector"]["matchLabels"]


def test_every_object_carries_the_standard_labels() -> None:
    for d in render([ALL]):
        labels = d["metadata"]["labels"]
        assert labels["app.kubernetes.io/name"] == "das", d["kind"]
        assert labels["app.kubernetes.io/instance"] == "release", d["kind"]
        assert labels["app.kubernetes.io/managed-by"] == "Helm" and labels["app.kubernetes.io/version"] == "0.1.0"


def test_the_service_account_can_be_switched_off_or_named() -> None:
    off = render(sets=["serviceAccount.create=false"])
    assert not of_kind(off, "ServiceAccount")
    used = {d["spec"]["template"]["spec"]["serviceAccountName"] for d in of_kind(off, "Deployment").values()}
    assert used == {"default"}
    named = render(sets=["serviceAccount.name=das-agents"])
    assert sorted(of_kind(named, "ServiceAccount")) == ["das-agents"]
    used = {d["spec"]["template"]["spec"]["serviceAccountName"] for d in of_kind(named, "Deployment").values()}
    assert used == {"das-agents"}


def test_a_release_named_after_the_chart_is_not_doubled() -> None:
    names = {d["metadata"]["name"] for d in render(release="das")}
    assert "das-gateway" in names and "das-das-gateway" not in names


# --------------------------------------------------------------------------- KEDA (task 10.3, Req 10.1)

RESEARCH, REVIEW = "release-das-worker-research", "release-das-worker-review"


def scaled_objects(docs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return of_kind(docs, "ScaledObject")


def test_keda_is_off_by_default_and_renders_nothing() -> None:
    assert scaled_objects(render()) == {}
    assert scaled_objects(render([ALL], sets=["keda.enabled=false"])) == {}


def test_keda_renders_one_scaled_object_per_worker_deployment_and_targets_it() -> None:
    docs = render([ALL])
    objects, deployments = scaled_objects(docs), of_kind(docs, "Deployment")
    assert sorted(objects) == [RESEARCH, REVIEW]  # not the Gateway, not the Manager
    for name, obj in objects.items():
        assert obj["apiVersion"] == "keda.sh/v1alpha1"
        target = {"name": name, "envSourceContainerName": container(deployments[name])["name"]}
        assert obj["spec"]["scaleTargetRef"] == target
        assert deployments[name]["kind"] == "Deployment"
        assert obj["metadata"]["labels"]["app.kubernetes.io/component"] == name.removeprefix("release-das-")


def test_every_capability_queue_of_a_worker_is_a_trigger() -> None:
    objects = scaled_objects(render([ALL]))
    research = objects[RESEARCH]["spec"]["triggers"]
    review = objects[REVIEW]["spec"]["triggers"]
    assert [t["metadata"]["queueName"] for t in research] == ["das.jobs.research", "das.jobs.summarize"]
    assert [t["metadata"]["queueName"] for t in review] == ["das.jobs.code-review"]
    for trigger in research + review:
        queue = trigger["metadata"]["queueName"]
        assert trigger == {
            "type": "rabbitmq",
            "metadata": {
                "protocol": "amqp", "queueName": queue, "mode": "QueueLength", "value": "5",
                "hostFromEnv": "DAS__RABBITMQ__URL",
            },
        }  # fmt: skip


def test_the_trigger_reads_the_broker_url_from_the_env_of_the_scaled_container() -> None:
    docs = render([ALL])
    for name, obj in scaled_objects(docs).items():
        for trigger in obj["spec"]["triggers"]:
            assert trigger["metadata"]["hostFromEnv"] in env(of_kind(docs, "Deployment")[name])
    assert "amqp://" not in yaml.safe_dump(list(scaled_objects(docs).values()))  # a name, never the credentialed URL


def test_keda_settings_come_from_the_values() -> None:
    spec = scaled_objects(render([ALL]))[RESEARCH]["spec"]
    assert (spec["pollingInterval"], spec["minReplicaCount"], spec["maxReplicaCount"]) == (10, 1, 8)
    sets = ["keda.queueLength=20", "keda.pollingInterval=30", "keda.minReplicaCount=0", "keda.maxReplicaCount=3"]
    spec = scaled_objects(render([ALL], sets=sets))[REVIEW]["spec"]
    assert (spec["pollingInterval"], spec["minReplicaCount"], spec["maxReplicaCount"]) == (30, 0, 3)
    assert {t["metadata"]["value"] for t in spec["triggers"]} == {"20"}


def test_with_keda_the_worker_deployments_leave_the_replica_count_to_it() -> None:
    on = of_kind(render([ALL]), "Deployment")
    off = of_kind(render([ALL], sets=["keda.enabled=false"]), "Deployment")
    for name in (RESEARCH, REVIEW):
        assert "replicas" not in on[name]["spec"], name  # a helm upgrade must not reset what KEDA scaled
        assert "replicas" in off[name]["spec"], name
    for name in ("release-das-gateway", "release-das-manager"):
        assert "replicas" in on[name]["spec"], name


# --------------------------------------------------------------------------- preStop drain (task 10.3, Req 10.6)

AGENTS = ("release-das-manager", RESEARCH, REVIEW)


def drain_command(deployment: dict[str, Any]) -> list[str]:
    return container(deployment)["lifecycle"]["preStop"]["exec"]["command"]


def test_the_prestop_drain_is_off_by_default() -> None:
    for name, d in of_kind(render(), "Deployment").items():
        assert "lifecycle" not in container(d), name
        assert "DAS_ADMIN_API_KEY" not in env(d), name


def test_the_prestop_drain_is_added_to_the_agents_and_not_to_the_gateway() -> None:
    d = of_kind(render([ALL]), "Deployment")
    assert "lifecycle" not in container(d["release-das-gateway"])  # the Gateway is not an agent in the registry
    assert "DAS_ADMIN_API_KEY" not in env(d["release-das-gateway"])
    for name in AGENTS:
        assert drain_command(d[name])[:2] == ["/bin/sh", "-c"], name


def test_the_admin_key_of_the_hook_comes_from_the_secret_never_from_the_manifest() -> None:
    d = of_kind(render([ALL]), "Deployment")
    for name in AGENTS:
        ref = secret_ref(env(d[name])["DAS_ADMIN_API_KEY"])
        assert ref == {"name": SECRET, "key": "ADMIN_API_KEYS", "optional": True}
    custom = of_kind(render([ALL], sets=["lifecycle.preStopDrain.adminKeySecretKey=DRAIN_KEY"]), "Deployment")
    assert secret_ref(env(custom[RESEARCH])["DAS_ADMIN_API_KEY"])["key"] == "DRAIN_KEY"


def run_hook(
    command: list[str], tmp_path: Path, exit_code: int = 0, key: str = "key-one,key-two"
) -> tuple[int, list[str], str]:
    """Execute the rendered hook with a stub ``das`` that records its arguments and the admin key it was given."""
    (tmp_path / "bin").mkdir(exist_ok=True)
    stub = tmp_path / "bin" / "das"
    stub.write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$@" > "{tmp_path}/args"\n'
        f'printf "%s" "$DAS_ADMIN_API_KEY" > "{tmp_path}/key"\nexit {exit_code}\n'
    )
    stub.chmod(0o755)
    env_ = {"PATH": f"{tmp_path / 'bin'}:/usr/bin:/bin", "DAS__AGENT_ID": "pod-7", "DAS_ADMIN_API_KEY": key}
    done = subprocess.run(command, env=env_, capture_output=True, text=True, timeout=30)
    args = (tmp_path / "args").read_text().splitlines() if (tmp_path / "args").exists() else []
    return done.returncode, args, (tmp_path / "key").read_text() if (tmp_path / "key").exists() else ""


def test_the_hook_drains_this_pod_through_the_gateway_service_with_the_first_admin_key(tmp_path: Path) -> None:
    command = drain_command(of_kind(render([ALL]), "Deployment")[RESEARCH])
    code, args, key = run_hook(command, tmp_path)
    assert code == 0
    assert args == ["drain", "http://release-das-gateway:8080", "pod-7"]
    assert key == "key-one"  # a Secret key that holds the whole admin list yields its first key


def test_a_failing_drain_never_keeps_the_pod_from_stopping(tmp_path: Path) -> None:
    command = drain_command(of_kind(render([ALL]), "Deployment")["release-das-manager"])
    code, args, _ = run_hook(command, tmp_path, exit_code=1)
    assert args and code == 0


def test_the_gateway_url_follows_the_service_port_and_can_be_overridden(tmp_path: Path) -> None:
    by_port = of_kind(render([ALL], sets=["gateway.service.port=9000"]), "Deployment")
    assert run_hook(drain_command(by_port[RESEARCH]), tmp_path)[1][1] == "http://release-das-gateway:9000"
    explicit = of_kind(render([ALL], sets=["lifecycle.preStopDrain.gatewayUrl=http://gw.example.com:81"]), "Deployment")
    assert run_hook(drain_command(explicit[REVIEW]), tmp_path)[1][1] == "http://gw.example.com:81"


def test_the_hook_command_holds_no_secret_value() -> None:
    for name, d in of_kind(render([ALL]), "Deployment").items():
        assert "key-one" not in yaml.safe_dump(d), name


def test_replicas_and_resources_come_from_the_values() -> None:
    d = of_kind(render([ALL]), "Deployment")
    assert container(d["release-das-gateway"])["resources"] == {
        "requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "512Mi"},
    }  # fmt: skip
    assert container(d["release-das-worker-review"])["resources"] == {}
