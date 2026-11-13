"""Phase 4 Gateway metrics: catalog entries, facade methods and label bounds (Req 10.1, 10.3)."""

from __future__ import annotations

from prometheus_client.parser import text_string_to_metric_families

from das.clock import FakeClock
from das.observability.metrics import CATALOG_BY_NAME, GatewayMetrics, MetricsRecorder

CLIENT_METRICS = (
    "das_client_rejections_total",
    "das_idempotent_replays_total",
    "das_sse_streams",
    "das_webhook_deliveries_total",
)


def _gateway() -> tuple[GatewayMetrics, MetricsRecorder]:
    rec = MetricsRecorder(clock=FakeClock())
    return GatewayMetrics(rec), rec


def _value(rec: MetricsRecorder, sample_name: str, **labels: str) -> float | None:
    body, _ = rec.render()
    for fam in text_string_to_metric_families(body.decode()):
        for s in fam.samples:
            if s.name == sample_name and s.labels == labels:
                return s.value
    return None


def test_catalog_entries_match_requirement_10_1():
    rejections = CATALOG_BY_NAME["das_client_rejections_total"]
    assert (rejections.kind, rejections.labels) == ("counter", ("reason",))
    assert rejections.allowed["reason"] == frozenset(
        {"rate_limited", "quota_exceeded", "too_many_streams", "budget_above_limit", "idempotency_key_reused"}
        # Phase 5 Req 8.2: the three reasons of tenants, Scopes and priority.
        | {"forbidden", "tenant_quota_exceeded", "priority_above_limit"}
    )
    replays = CATALOG_BY_NAME["das_idempotent_replays_total"]
    assert (replays.kind, replays.labels) == ("counter", ())
    streams = CATALOG_BY_NAME["das_sse_streams"]
    assert (streams.kind, streams.labels) == ("gauge", ())
    deliveries = CATALOG_BY_NAME["das_webhook_deliveries_total"]
    assert (deliveries.kind, deliveries.labels) == ("counter", ("outcome",))
    assert deliveries.allowed["outcome"] == frozenset({"delivered", "retry", "failed"})


def test_no_client_metric_carries_fingerprint_request_id_or_url_label():
    """Req 10.3: only the bounded ``reason`` and ``outcome`` labels are used."""
    for name in CLIENT_METRICS:
        assert set(CATALOG_BY_NAME[name].labels) <= {"reason", "outcome"}


def test_client_rejected_counts_per_reason():
    gw, rec = _gateway()
    gw.client_rejected("rate_limited")
    gw.client_rejected("rate_limited")
    gw.client_rejected("quota_exceeded")
    assert _value(rec, "das_client_rejections_total", reason="rate_limited") == 2
    assert _value(rec, "das_client_rejections_total", reason="quota_exceeded") == 1
    assert _value(rec, "das_client_rejections_total", reason="too_many_streams") is None


def test_unknown_rejection_reason_is_bounded_not_a_new_series():
    gw, rec = _gateway()
    gw.client_rejected("fp-1234")  # type: ignore[arg-type]  # a stray value must not mint a series
    assert _value(rec, "das_client_rejections_total", reason="fp-1234") is None
    assert _value(rec, "das_client_rejections_total", reason="_other") == 1


def test_idempotent_replay_counter():
    gw, rec = _gateway()
    gw.idempotent_replay()
    gw.idempotent_replay()
    assert _value(rec, "das_idempotent_replays_total") == 2


def test_sse_streams_gauge_reads_the_callable_at_render_time():
    gw, rec = _gateway()
    open_streams = [0]
    gw.sse_streams(lambda: open_streams[0])
    assert _value(rec, "das_sse_streams") == 0
    open_streams[0] = 3
    assert _value(rec, "das_sse_streams") == 3


def test_webhook_delivery_counts_per_outcome():
    gw, rec = _gateway()
    gw.webhook_delivery("delivered")
    gw.webhook_delivery("retry")
    gw.webhook_delivery("retry")
    gw.webhook_delivery("failed")
    assert _value(rec, "das_webhook_deliveries_total", outcome="delivered") == 1
    assert _value(rec, "das_webhook_deliveries_total", outcome="retry") == 2
    assert _value(rec, "das_webhook_deliveries_total", outcome="failed") == 1
