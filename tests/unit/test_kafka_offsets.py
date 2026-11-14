"""OffsetTracker examples (das-scale-resilience task 10.1; Phase 7 Req 3.4, 3.6). The property test in
``tests/property/test_offset_tracker.py`` covers every interleaving; these pin the cases a reader looks for."""

from __future__ import annotations

from das.transport.kafka_offsets import OffsetTracker


def delivered(tracker: OffsetTracker, key: str, *offsets: int) -> None:
    for offset in offsets:
        tracker.deliver(key, offset)


def test_acknowledging_in_order_commits_one_past_each() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0, 1, 2)
    assert [t.ack("p", 0), t.ack("p", 1), t.ack("p", 2)] == [1, 2, 3]


def test_an_acknowledgement_ahead_of_an_unfinished_message_commits_nothing() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0, 1, 2)
    assert t.ack("p", 2) is None and t.ack("p", 1) is None
    assert t.committed("p") is None and t.in_flight("p") == 1


def test_finishing_the_oldest_releases_everything_behind_it_at_once() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0, 1, 2)
    t.ack("p", 2)
    t.ack("p", 1)
    assert t.ack("p", 0) == 3


def test_a_gap_in_the_offsets_does_not_hold_the_commit_back() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0, 1, 5, 6)  # Kafka skipped 2 to 4 (a transaction marker, a compacted record)
    assert [t.ack("p", 0), t.ack("p", 1)] == [1, 5]  # nothing to wait for at 2 to 4
    assert t.ack("p", 6) is None and t.ack("p", 5) == 7


def test_the_commit_point_after_a_pause_continues_from_new_deliveries() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0)
    assert t.ack("p", 0) == 1
    delivered(t, "p", 1, 2)
    assert t.ack("p", 2) is None and t.ack("p", 1) == 3


def test_a_second_acknowledgement_or_an_unknown_one_changes_nothing() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0, 1)
    assert t.ack("p", 0) == 1
    assert t.ack("p", 0) is None and t.ack("p", 9) is None and t.ack("other", 0) is None
    assert t.in_flight("p") == 1


def test_an_offset_below_the_commit_point_is_a_duplicate_and_is_ignored() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0, 1)
    t.ack("p", 0)
    t.ack("p", 1)
    t.deliver("p", 1)
    assert t.in_flight("p") == 0 and t.ack("p", 1) is None


def test_partitions_are_independent_and_in_flight_counts_them_all() -> None:
    t = OffsetTracker()
    delivered(t, "a", 0, 1)
    delivered(t, "b", 0)
    assert t.in_flight() == 3 and t.in_flight("a") == 2
    assert t.ack("b", 0) == 1 and t.committed("a") is None


def test_forget_drops_a_partition_and_reports_what_it_abandoned() -> None:
    t = OffsetTracker()
    delivered(t, "p", 0, 1, 2)
    t.ack("p", 0)
    assert t.forget("p") == 2 and t.forget("p") == 0
    assert t.in_flight() == 0 and t.committed("p") is None and t.ack("p", 1) is None
