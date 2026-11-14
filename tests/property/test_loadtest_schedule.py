"""The arrival schedule is open-loop and exact (das-scale-resilience task 12.1; Phase 7 Req 4.2): for any profile, each
step offers rate x seconds Requests (at least one), spaced 1/rate apart inside the step, in non-decreasing time, with
every prompt of the mix used in turn."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from das.loadtest.profile import Profile, Step, arrivals, requests_in

steps = st.builds(
    Step,
    rate=st.floats(min_value=0.05, max_value=200, allow_nan=False),
    seconds=st.floats(min_value=0.05, max_value=20, allow_nan=False),
)
profiles = st.builds(
    Profile,
    name=st.just("p"),
    steps=st.lists(steps, min_size=1, max_size=6).map(tuple),
    prompts=st.lists(st.text(min_size=1, max_size=5), min_size=1, max_size=5).map(tuple),
)


@given(profiles)
def test_every_step_offers_its_requests_inside_its_own_time_span(profile: Profile) -> None:
    schedule = list(arrivals(profile))
    start = 0.0
    for number, step in enumerate(profile.steps):
        own = [a for a in schedule if a.step == number]
        assert len(own) == requests_in(step) == max(1, round(step.rate * step.seconds))
        assert abs(own[0].at - start) < 1e-9
        gaps = [b.at - a.at for a, b in zip(own, own[1:], strict=False)]
        assert all(abs(g - 1 / step.rate) < 1e-6 for g in gaps), "spaced 1/rate apart"
        assert own[-1].at < start + max(step.seconds, 1 / step.rate) + 1e-9, "inside the step"
        start += step.seconds


@given(profiles)
def test_the_schedule_never_goes_back_in_time_and_numbers_every_request_once(profile: Profile) -> None:
    schedule = list(arrivals(profile))
    assert [a.index for a in schedule] == list(range(len(schedule)))
    assert all(a.at <= b.at + 1e-9 for a, b in zip(schedule, schedule[1:], strict=False))


@given(profiles)
def test_the_prompts_are_used_in_turn(profile: Profile) -> None:
    schedule = list(arrivals(profile))
    assert [a.prompt for a in schedule] == [profile.prompts[i % len(profile.prompts)] for i in range(len(schedule))]
