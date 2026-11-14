"""``das_redis_failovers_total`` and ``das_redis_retries_total{reason}`` (das-scale-resilience task 8.3; Phase 7
Req 1.8): a master switch and each retry through a Failover are counted, ``reason`` is ``connection`` or
``readonly``, and the counts made before the metrics existed are not lost."""

from __future__ import annotations

from das.clock import FakeClock
from das.cli import _bind_redis_metrics
from das.observability.metrics import CATALOG_BY_NAME, REDIS_RETRY_REASONS, MetricsRecorder, RedisMetrics
from das.state.redisclient import RedisEvents


def make() -> tuple[MetricsRecorder, RedisMetrics]:
    recorder = MetricsRecorder(clock=FakeClock())
    return recorder, RedisMetrics(recorder)


def value(recorder: MetricsRecorder, name: str, **labels: str) -> float | None:
    return recorder.registry.get_sample_value(name, labels)


def test_the_failover_counter_has_no_labels_and_the_retry_counter_has_a_reason() -> None:
    failovers = CATALOG_BY_NAME["das_redis_failovers_total"]
    retries = CATALOG_BY_NAME["das_redis_retries_total"]
    assert (failovers.kind, failovers.labels) == ("counter", ())
    assert (retries.kind, retries.labels) == ("counter", ("reason",))


def test_the_reasons_are_connection_and_readonly() -> None:
    assert REDIS_RETRY_REASONS == frozenset({"connection", "readonly"})


def test_each_master_switch_is_counted() -> None:
    recorder, metrics = make()
    metrics.failover()
    metrics.failover()
    assert value(recorder, "das_redis_failovers_total") == 2


def test_each_retry_is_counted_under_its_reason() -> None:
    recorder, metrics = make()
    for reason in ("connection", "connection", "readonly"):
        metrics.retry(reason)
    assert value(recorder, "das_redis_retries_total", reason="connection") == 2
    assert value(recorder, "das_redis_retries_total", reason="readonly") == 1


def test_a_reason_outside_the_two_is_not_counted_and_does_not_raise() -> None:
    recorder, metrics = make()
    metrics.retry("exploded")
    assert value(recorder, "das_redis_retries_total", reason="exploded") is None


def test_binding_passes_on_what_was_counted_before_the_metrics_existed() -> None:
    recorder = MetricsRecorder(clock=FakeClock())
    events = RedisEvents()
    events.on_retry("connection")
    events.on_retry("readonly")
    events.on_failover()
    _bind_redis_metrics(recorder, events)
    assert value(recorder, "das_redis_retries_total", reason="connection") == 1
    assert value(recorder, "das_redis_retries_total", reason="readonly") == 1
    assert value(recorder, "das_redis_failovers_total") == 1
    events.on_failover()  # and what comes after goes straight to them
    assert value(recorder, "das_redis_failovers_total") == 2
