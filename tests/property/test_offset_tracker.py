"""Property: the commit point never passes an unacknowledged message (das-scale-resilience task 10.1; Phase 7 Req 3.4,
3.6).

Whatever order messages are delivered and acknowledged in, with gaps in the offsets (Kafka skips some), repeated and
unknown acknowledgements, and several partitions at once: what the tracker says to commit is never above the lowest
offset still in flight (so a crash loses nothing), it never moves backwards, and after an acknowledgement it is exactly
the oracle's point, so nothing that could be committed is held back."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from das.transport.kafka_offsets import OffsetTracker

picks = st.lists(st.integers(min_value=0, max_value=10_000), min_size=1, max_size=120)
gaps = st.lists(st.integers(min_value=1, max_value=3), min_size=1, max_size=25)
noise = st.lists(st.integers(-3, 90), max_size=20)


def offsets_from(gap_list: list[int]) -> list[int]:
    """Increasing offsets starting at 0 with the given gaps between them (a gap of 1 is the next offset)."""
    out = [0]
    for gap in gap_list[1:]:
        out.append(out[-1] + gap)
    return out


def oracle_point(delivered: set[int], acked: set[int]) -> int | None:
    """What may be committed, from the sets alone: the lowest delivered offset not acknowledged, else one past the
    highest delivered; ``None`` before anything was delivered."""
    if not delivered:
        return None
    unacked = delivered - acked
    return min(unacked) if unacked else max(delivered) + 1


@given(gaps, picks, noise)
def test_the_commit_point_is_the_oracles_and_never_passes_an_unacknowledged_offset(
    gap_list: list[int], choices: list[int], junk: list[int]
) -> None:
    to_deliver = offsets_from(gap_list)
    tracker = OffsetTracker()
    delivered: set[int] = set()
    acked: set[int] = set()
    last = 0
    steps = iter(choices)
    noise_steps = iter(junk)

    def check(returned: int | None, acked_now: bool) -> None:
        nonlocal last
        if returned is not None:
            assert returned > last, "the commit point only moves forward"
            last = returned
        in_flight = delivered - acked
        assert not in_flight or last <= min(in_flight), "never past an unacknowledged message"
        assert last <= (max(delivered) + 1 if delivered else 0)
        assert tracker.in_flight() == len(in_flight)
        if acked_now:
            assert last == oracle_point(delivered, acked), "nothing that could be committed is held back"

    while to_deliver or delivered - acked:
        pick = next(steps, 0)
        in_flight = sorted(delivered - acked)
        if to_deliver and (not in_flight or pick % 2 == 0):
            offset = to_deliver.pop(0)
            tracker.deliver("p0", offset)
            delivered.add(offset)
            check(None, False)
        else:
            offset = in_flight[pick % len(in_flight)]
            acked.add(offset)
            check(tracker.ack("p0", offset), True)
        bogus = next(noise_steps, -1)
        if bogus not in delivered - acked:  # never delivered, or already acknowledged
            assert tracker.ack("p0", bogus) is None
        check(None, False)

    assert tracker.in_flight() == 0
    assert tracker.committed("p0") == max(delivered) + 1


@given(gaps, gaps, picks)
def test_partitions_do_not_affect_one_another(gap_a: list[int], gap_b: list[int], choices: list[int]) -> None:
    a_offsets, b_offsets = offsets_from(gap_a), offsets_from(gap_b)
    both = OffsetTracker()
    only_a = OffsetTracker()
    for offset in a_offsets:
        both.deliver("a", offset)
        only_a.deliver("a", offset)
    for offset in b_offsets:
        both.deliver("b", offset)
    order = list(a_offsets)
    for pick in choices:
        if not order:
            break
        offset = order.pop(pick % len(order))
        assert both.ack("a", offset) == only_a.ack("a", offset)
    assert both.in_flight("b") == len(b_offsets) and both.committed("b") is None


@given(gaps)
def test_forgetting_a_partition_abandons_exactly_what_was_in_flight(gap_list: list[int]) -> None:
    tracker = OffsetTracker()
    offsets = offsets_from(gap_list)
    for offset in offsets:
        tracker.deliver("p", offset)
    done = offsets[: len(offsets) // 2]
    for offset in done:
        tracker.ack("p", offset)
    assert tracker.forget("p") == len(offsets) - len(done)
    assert tracker.in_flight("p") == 0 and tracker.committed("p") is None
    assert tracker.ack("p", offsets[-1]) is None, "a message of a partition that was taken away is not committed"
