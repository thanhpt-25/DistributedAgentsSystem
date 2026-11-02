"""Unit tests for the metric catalog, MetricsRecorder and typed facades (Req 1, 2.11, 5)."""

from __future__ import annotations

from typing import Any

import pytest
from prometheus_client.parser import text_string_to_metric_families

from das.clock import FakeClock
from das.config.schema import BucketsConfig, LabelLimitsConfig, MetricsConfig
from das.models.job import JobState
from das.models.request import RequestStatus
from das.observability.metrics import (
    ALLOWED_LABEL_NAMES,
    CATALOG,
    CONTENT_TYPE,
    GatewayMetrics,
    ManagerMetrics,
    MetricsRecorder,
    NoopRecorder,
    RenderError,
    WorkerMetrics,
    build_recorder,
)
from das.observability.ratelimit import RateLimitedLog


class ListLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **fields: Any) -> None:
        self.entries.append((event, fields))

    def events(self, name: str) -> list[dict[str, Any]]:
        return [f for e, f in self.entries if e == name]


def make(**kwargs: Any) -> tuple[MetricsRecorder, ListLog, FakeClock]:
    clock = FakeClock()
    log = ListLog()
    rec = MetricsRecorder(clock=clock, log=log, **kwargs)
    return rec, log, clock


def samples(rec: MetricsRecorder) -> dict[str, list[Any]]:
    body, _ = rec.render()
    out: dict[str, list[Any]] = {}
    for fam in text_string_to_metric_families(body.decode()):
        out[fam.name] = list(fam.samples)
    return out


def value(rec: MetricsRecorder, sample_name: str, **labels: str) -> float | None:
    body, _ = rec.render()
    for fam in text_string_to_metric_families(body.decode()):
        for s in fam.samples:
            if s.name == sample_name and s.labels == labels:
                return s.value
    return None


# --- catalog / exposition ---------------------------------------------------------------


def test_catalog_names_and_labels_are_allowed() -> None:
    names = [s.name for s in CATALOG]
    assert len(names) == len(set(names)) == 27  # + das_budget_exceeded_total (Phase 4 Req 10.2)
    for spec in CATALOG:
        assert spec.name.startswith("das_")
        assert set(spec.labels) <= ALLOWED_LABEL_NAMES
        if spec.kind == "histogram":
            assert spec.buckets_key in {"http", "duration", "attempts"}


def test_render_content_type_and_one_help_type_per_metric() -> None:
    rec, _, _ = make()
    body, ctype = rec.render()
    assert ctype == CONTENT_TYPE == "text/plain; version=0.0.4; charset=utf-8"
    text = body.decode()
    lines = text.splitlines()
    for spec in CATALOG:
        assert sum(1 for ln in lines if ln.startswith(f"# HELP {spec.name} ")) == 1, spec.name
        assert sum(1 for ln in lines if ln.startswith(f"# TYPE {spec.name} ")) == 1, spec.name
    rec.inc("das_requests_submitted_total")
    rec.inc("das_requests_finished_total", {"status": "Failed"})
    rec.observe("das_job_attempts", {"capability": "c"}, 1)
    for fam in text_string_to_metric_families(rec.render()[0].decode()):
        assert not any(s.name.endswith("_created") for s in fam.samples)
    assert "process_" not in text and "python_" not in text
    help_type_names = {ln.split()[2] for ln in lines if ln.startswith("# ")}
    assert help_type_names == {s.name for s in CATALOG}


def test_default_and_configured_buckets() -> None:
    rec, _, _ = make()
    rec.observe("das_http_request_duration_seconds", {"route": "_unmatched", "method": "GET"}, 0.2)
    rec.observe("das_job_attempts", {"capability": "c"}, 2)
    rec.observe("das_request_duration_seconds", {"status": "Completed"}, 7)
    s = samples(rec)
    http_le = [x.labels["le"] for x in s["das_http_request_duration_seconds"] if x.name.endswith("_bucket")]
    assert http_le[:3] == ["0.005", "0.01", "0.025"] and http_le[-2:] == ["10.0", "+Inf"]
    att_le = [x.labels["le"] for x in s["das_job_attempts"] if x.name.endswith("_bucket")]
    assert att_le == ["1.0", "2.0", "3.0", "5.0", "10.0", "20.0", "+Inf"]
    dur_le = [x.labels["le"] for x in s["das_request_duration_seconds"] if x.name.endswith("_bucket")]
    assert dur_le[0] == "0.1" and dur_le[-2] == "600.0"

    custom, _, _ = make(buckets=BucketsConfig(attempts=[1, 4]))
    custom.observe("das_job_attempts", {"capability": "c"}, 3)
    le = [x.labels["le"] for x in samples(custom)["das_job_attempts"] if x.name.endswith("_bucket")]
    assert le == ["1.0", "4.0", "+Inf"]


# --- recording / bounding ---------------------------------------------------------------


def test_counter_and_histogram_recording() -> None:
    rec, _, _ = make()
    rec.inc("das_requests_submitted_total")
    rec.inc("das_requests_submitted_total", amount=2)
    rec.observe("das_request_duration_seconds", {"status": RequestStatus.FAILED}, 1.5)
    assert value(rec, "das_requests_submitted_total") == 3
    assert value(rec, "das_request_duration_seconds_count", status="Failed") == 1
    assert value(rec, "das_request_duration_seconds_sum", status="Failed") == 1.5


def test_enumerated_label_outside_set_becomes_other_and_warns_once() -> None:
    rec, log, _ = make()
    for _ in range(3):
        rec.inc("das_requests_finished_total", {"status": "Running"})
    rec.inc("das_http_requests_total", {"route": "/x/123", "method": "GET", "status_code": 700})
    assert value(rec, "das_requests_finished_total", status="_other") == 3
    assert value(
        rec, "das_http_requests_total", route="_other", method="GET", status_code="_other"
    ) == 1
    warns = log.events("metric_label_overflow")
    pairs = sorted((w["metric"], w["label"]) for w in warns)
    assert pairs == [
        ("das_http_requests_total", "route"),
        ("das_http_requests_total", "status_code"),
        ("das_requests_finished_total", "status"),
    ]


def test_route_templates_and_unmatched() -> None:
    rec, _, _ = make(route_templates=["/v1/requests/{request_id}"])
    gw = GatewayMetrics(rec)
    gw.http_request("/v1/requests/{request_id}", "get", 200, 0.01)
    gw.http_request(None, "BREW", 404, 0.01)
    assert value(
        rec, "das_http_requests_total", route="/v1/requests/{request_id}", method="GET",
        status_code="200",
    ) == 1
    assert value(
        rec, "das_http_requests_total", route="_unmatched", method="_other", status_code="404"
    ) == 1


def test_dynamic_label_limit_overflow_keeps_seen_values() -> None:
    rec, log, _ = make(label_limits=LabelLimitsConfig(capability=2))
    for cap in ["a", "b", "c", "d", "a"]:
        rec.inc("das_job_transitions_total", {"capability": cap, "state": JobState.READY})
    assert value(rec, "das_job_transitions_total", capability="a", state="Ready") == 2
    assert value(rec, "das_job_transitions_total", capability="b", state="Ready") == 1
    assert value(rec, "das_job_transitions_total", capability="_other", state="Ready") == 2
    warns = log.events("metric_label_overflow")
    assert warns == [
        {"metric": "das_job_transitions_total", "label": "capability", "limit": 2,
         "overflow_value": "_other"}
    ]
    # limits are per metric: another metric still accepts new capabilities
    rec.observe("das_job_attempts", {"capability": "c"}, 1)
    assert value(rec, "das_job_attempts_count", capability="c") == 1


def test_recording_errors_are_swallowed_and_rate_limited() -> None:
    rec, log, clock = make()
    rec.inc("das_unknown_total")
    rec.inc("das_unknown_total")
    rec.inc("das_requests_submitted_total", amount=-1)  # prometheus rejects negative
    rec.observe("das_requests_submitted_total", None, 1)  # wrong kind
    rec.inc("das_requests_finished_total", {"bogus": "x"})  # wrong labels
    events = log.events("metric_record_failed")
    assert [e["metric"] for e in events] == [
        "das_unknown_total", "das_requests_submitted_total", "das_requests_finished_total"
    ]
    clock.advance(59)
    rec.inc("das_unknown_total")
    assert len(log.events("metric_record_failed")) == 3
    clock.advance(1)
    rec.inc("das_unknown_total")
    assert len(log.events("metric_record_failed")) == 4
    # other observations still recorded
    rec.inc("das_requests_submitted_total")
    assert value(rec, "das_requests_submitted_total") == 1


def test_set_function_and_failing_fn_keeps_last_value() -> None:
    rec, log, _ = make()
    state = {"n": 3, "fail": False}

    def fn() -> float:
        if state["fail"]:
            raise RuntimeError("boom")
        return state["n"]

    WorkerMetrics(rec).jobs_in_flight(fn)
    assert value(rec, "das_worker_jobs_in_flight") == 3
    state["fail"] = True
    assert value(rec, "das_worker_jobs_in_flight") == 3
    assert log.events("metric_record_failed")[0]["metric"] == "das_worker_jobs_in_flight"


def test_render_error_raises_and_is_rate_limited(monkeypatch: pytest.MonkeyPatch) -> None:
    rec, log, _ = make()

    def broken(_registry: Any) -> bytes:
        raise RuntimeError("x")

    monkeypatch.setattr("das.observability.metrics.generate_latest", broken)
    for _ in range(2):
        with pytest.raises(RenderError):
            rec.render()
    assert len(log.events("metrics_render_failed")) == 1


def test_noop_recorder_and_build_recorder() -> None:
    clock = FakeClock()
    noop = build_recorder(MetricsConfig(enabled=False), clock=clock)
    assert isinstance(noop, NoopRecorder)
    noop.inc("das_requests_submitted_total")
    GatewayMetrics(noop).request_submitted()
    assert noop.render() == (b"", CONTENT_TYPE)
    real = build_recorder(MetricsConfig(), clock=clock, log=ListLog())
    assert not isinstance(real, NoopRecorder)


def test_two_recorders_are_independent() -> None:
    a, _, _ = make()
    b, _, _ = make()
    a.inc("das_requests_submitted_total")
    assert value(a, "das_requests_submitted_total") == 1
    assert value(b, "das_requests_submitted_total") == 0


# --- facades ----------------------------------------------------------------------------


def test_manager_facade() -> None:
    rec, _, _ = make()
    m = ManagerMetrics(rec)
    m.request_finished(RequestStatus.COMPLETED, -4)
    m.job_transition("research", JobState.SUCCEEDED)
    m.job_attempts("research", 2)
    m.queue_depth("das.results", 5)
    m.queue_depth_sample_error("das.dlq")
    m.lease_acquired("takeover")
    m.lease_lost("store_unreachable")
    m.leases_held(lambda: 2)
    m.registry_agents("worker", "Draining", 1)
    m.capability_workers("research", 0)
    m.registry_sample_error()
    m.dlq_record_created("malformed")
    m.retention_deleted("inbox", 3)
    m.retention_deleted("request", 0)
    assert value(rec, "das_request_duration_seconds_sum", status="Completed") == 0
    assert value(rec, "das_requests_finished_total", status="Completed") == 1
    assert value(rec, "das_job_transitions_total", capability="research", state="Succeeded") == 1
    assert value(rec, "das_job_attempts_sum", capability="research") == 2
    assert value(rec, "das_queue_depth", queue="das.results") == 5
    assert value(rec, "das_queue_depth_sample_errors_total", queue="das.dlq") == 1
    assert value(rec, "das_lease_acquisitions_total", kind="takeover") == 1
    assert value(rec, "das_lease_losses_total", reason="store_unreachable") == 1
    assert value(rec, "das_leases_held") == 2
    assert value(rec, "das_registry_agents", role="worker", status="Draining") == 1
    assert value(rec, "das_capability_workers", capability="research") == 0
    assert value(rec, "das_registry_sample_errors_total") == 1
    assert value(rec, "das_dlq_records_created_total", reason="malformed") == 1
    assert value(rec, "das_retention_deleted_total", kind="inbox") == 3
    assert value(rec, "das_retention_deleted_total", kind="request") is None


def test_worker_and_llm_facades() -> None:
    rec, _, _ = make(currency="EUR")
    w = WorkerMetrics(rec)
    w.job_executed("review", "TimedOut", 1.25)
    w.concurrency_limit(4)
    w.llm.call_finished("openai", "gpt", "job", "timeout", 3.0)
    w.llm.retry_scheduled("openai", "gpt", "job")
    w.llm.tokens("openai", "gpt", "job", 10, 5)
    w.llm.estimated_cost("openai", "gpt", 0.5)
    base = {"provider": "openai", "model": "gpt", "operation": "job"}
    assert value(rec, "das_job_execution_seconds_count", capability="review", status="TimedOut") == 1
    assert value(rec, "das_worker_concurrency_limit") == 4
    assert value(rec, "das_llm_calls_total", **base, outcome="timeout") == 1
    assert value(rec, "das_llm_call_duration_seconds_sum", **base) == 3.0
    assert value(rec, "das_llm_retries_total", **base) == 1
    assert value(rec, "das_llm_tokens_total", **base, direction="input") == 10
    assert value(rec, "das_llm_tokens_total", **base, direction="output") == 5
    assert value(
        rec, "das_llm_estimated_cost_total", provider="openai", model="gpt", currency="EUR"
    ) == 0.5


def test_gateway_dlq_admin_and_cancel() -> None:
    rec, _, _ = make()
    g = GatewayMetrics(rec)
    g.dlq_admin_action("replay", "lease_held")
    g.request_finished(RequestStatus.CANCELLED, 2)
    assert value(rec, "das_dlq_admin_actions_total", action="replay", outcome="lease_held") == 1
    assert value(rec, "das_requests_finished_total", status="Cancelled") == 1


def test_output_has_no_secret_like_values() -> None:
    rec, _, _ = make()
    secret = "sk-SECRET-123"
    rec.inc("das_requests_finished_total", {"status": secret})
    body, _ = rec.render()
    assert secret.encode() not in body


# --- RateLimitedLog ---------------------------------------------------------------------


def test_rate_limited_log_per_key() -> None:
    clock = FakeClock()
    rl = RateLimitedLog(clock, 60)
    assert rl.allow("a") and rl.allow("b")
    assert not rl.allow("a")
    clock.advance(60)
    assert rl.allow("a")
