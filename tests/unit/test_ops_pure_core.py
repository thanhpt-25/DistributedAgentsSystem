"""Gap-filling unit tests for the das.ops pure core (Req 11.7, 12.1, 12.3, 13.4, 14.9).

Complements test_ops_retention.py, test_ops_dlq.py, test_ops_replay.py and test_ops_keys.py.
"""

from __future__ import annotations

import base64
import hmac
from datetime import UTC, datetime

import pytest

from das.codec import ParseError, new_message
from das.models import JobPlan, JobRecord, JobState, PlannedJob, RequestStatus
from das.models.results import JobDispatchPayload
from das.ops import keys as keys_mod
from das.ops.dlq import build_record, classify_reason
from das.ops.keys import KeyClass, classify_key
from das.ops.replay import skipped_to_reset
from das.ops.retention import retention_decision

TERMINAL = [RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED]
DLID = "0123456789abcdef0123456789abcdef"
TS = datetime(2025, 1, 1, tzinfo=UTC)


# ------------------------------------------------------------------ retention (Req 14.9)


@pytest.mark.parametrize("status", TERMINAL)
def test_every_terminal_status_without_finished_at_is_stamped(status: RequestStatus) -> None:
    # Stamping wins regardless of how large now or ttl are; never delete in this sweep.
    assert retention_decision(status, None, 0, 0) == "stamp"
    assert retention_decision(status, None, 10**15, 1) == "stamp"


@pytest.mark.parametrize("status", TERMINAL)
def test_zero_ttl_boundary(status: RequestStatus) -> None:
    assert retention_decision(status, 1000, 1000, 0) == "delete"
    assert retention_decision(status, 1001, 1000, 0) == "keep"


# ------------------------------------------------------------------ classify_reason


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"x-death-reason": "max_attempts", "x-last-error": "boom"}, "retries_exhausted"),
        ({"x-death-reason": "delivery_limit", "other": "max_attempts"}, "delivery_limit"),
        ({"x-death-reason": "MAX_ATTEMPTS"}, "malformed"),  # value is case-sensitive
        ({"x-death-reason": ""}, "malformed"),
        ({"x-last-error": "boom"}, "malformed"),
    ],
)
def test_classify_reason_three_reasons(headers: dict[str, str], expected: str) -> None:
    assert classify_reason(headers) == expected


# ------------------------------------------------------------------ build_record


def _dispatch():  # type: ignore[no-untyped-def]
    payload = JobDispatchPayload(
        request_id="r1", job_id="j1", attempt=1, instruction="x", required_capabilities=["c"]
    )
    return new_message(payload, correlation_id="r1", message_id="m1", timestamp=TS)


def test_last_error_exactly_4096_kept_whole() -> None:
    rec = build_record(
        DLID, "retries_exhausted", _dispatch(), b"", {"x-last-error": "e" * 4096}, None, 10
    )
    assert rec.last_error == "e" * 4096


def test_last_error_header_overrides_delivery_limit_default() -> None:
    long = "d" * 4097
    rec = build_record(DLID, "delivery_limit", _dispatch(), b"", {"x-last-error": long}, None, 10)
    assert rec.last_error == long[:4096]


def test_malformed_header_error_preferred_over_parse_reason() -> None:
    rec = build_record(
        DLID, "malformed", None, b"abc", {"x-last-error": "hdr"}, ParseError("bad json"), 10
    )
    assert rec.last_error == "hdr"
    assert rec.parse_reason == "bad json"


def test_malformed_without_parse_error_and_zero_limit() -> None:
    rec = build_record(DLID, "malformed", None, b"abc", {}, None, 0)
    assert rec.last_error is None
    assert rec.parse_reason is None and rec.parse_field is None
    assert rec.original_size == 3
    assert base64.b64decode(rec.raw_b64 or "") == b""
    assert rec.truncated is True


def test_malformed_empty_body_not_truncated() -> None:
    rec = build_record(DLID, "malformed", None, b"", {}, ParseError("empty"), 0)
    assert rec.original_size == 0
    assert rec.raw_b64 == ""
    assert rec.truncated is False


def test_non_malformed_never_keeps_raw_bytes() -> None:
    rec = build_record(DLID, "retries_exhausted", _dispatch(), b"x" * 50, {}, None, 10)
    assert rec.raw_b64 is None and rec.original_size is None and rec.truncated is False


def test_negative_max_bytes_rejected() -> None:
    with pytest.raises(ValueError):
        build_record(DLID, "malformed", None, b"x", {}, None, -1)


# ------------------------------------------------------------------ skipped_to_reset (Req 13.4)


def _job(jid: str, deps: list[str] | None = None) -> PlannedJob:
    return PlannedJob(job_id=jid, instruction="do", capabilities=["c"], depends_on=deps or [])


def _states(plan: JobPlan, states: dict[str, JobState]) -> dict[str, JobRecord]:
    return {pj.job_id: JobRecord(job=pj, state=states[pj.job_id]) for pj in plan.jobs}


S, F, X, OK = JobState.SKIPPED, JobState.FAILED, JobState.CANCELLED, JobState.SUCCEEDED


def test_diamond_with_second_root_cause_on_one_branch() -> None:
    # a -> b -> d ; x -> c -> d : d is reached from two different root causes.
    plan = JobPlan(
        jobs=[_job("a"), _job("x"), _job("b", ["a"]), _job("c", ["x"]), _job("d", ["b", "c"])]
    )
    states = {"a": F, "x": X, "b": S, "c": S, "d": S}
    assert skipped_to_reset(plan, _states(plan, states), "a") == {"b"}
    assert skipped_to_reset(plan, _states(plan, states), "x") == {"c"}


def test_diamond_both_branches_same_cause_then_tail() -> None:
    # a -> {b, c} -> d -> e, with extra direct edge a -> e.
    plan = JobPlan(
        jobs=[
            _job("a"), _job("b", ["a"]), _job("c", ["a"]),
            _job("d", ["b", "c"]), _job("e", ["d", "a"]),
        ]
    )
    states = {"a": F, "b": S, "c": S, "d": S, "e": S}
    assert skipped_to_reset(plan, _states(plan, states), "a") == {"b", "c", "d", "e"}


def test_failed_direct_parent_beyond_skipped_adds_cause() -> None:
    # e depends on Skipped d (cause a) and directly on Failed y: causes {a, y}.
    plan = JobPlan(jobs=[_job("a"), _job("y"), _job("d", ["a"]), _job("e", ["d", "y"])])
    states = {"a": F, "y": F, "d": S, "e": S}
    assert skipped_to_reset(plan, _states(plan, states), "a") == {"d"}
    assert skipped_to_reset(plan, _states(plan, states), "y") == frozenset()


def test_non_skipped_jobs_are_never_reset() -> None:
    plan = JobPlan(jobs=[_job("a"), _job("b", ["a"]), _job("c", ["a"])])
    states = {"a": F, "b": OK, "c": JobState.WAITING}
    assert skipped_to_reset(plan, _states(plan, states), "a") == frozenset()


# ------------------------------------------------------------------ classify_key (Req 11.7)


def test_keys_sharing_prefix_across_lists() -> None:
    api = [b"shared-prefix-key-API-01"]
    admin = [b"shared-prefix-key-ADM-01", b"shared-prefix-key-API-0"]
    assert classify_key("shared-prefix-key-API-01", api, admin) == (KeyClass.API, 0)
    assert classify_key("shared-prefix-key-ADM-01", api, admin) == (KeyClass.ADMIN, 0)
    assert classify_key("shared-prefix-key-API-0", api, admin) == (KeyClass.ADMIN, 1)
    assert classify_key("shared-prefix-key-", api, admin) == (KeyClass.NONE, None)


def test_empty_admin_list_compares_every_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    api = [b"k-000000000000001", b"k-000000000000002", b"k-000000000000003"]
    calls: list[bytes] = []
    real = hmac.compare_digest

    def spy(a: bytes, b: bytes) -> bool:
        calls.append(b)
        return real(a, b)

    monkeypatch.setattr(keys_mod.hmac, "compare_digest", spy)
    assert classify_key("k-000000000000001", api, []) == (KeyClass.API, 0)
    assert calls == api  # no early exit after the first-position match
    calls.clear()
    assert classify_key(None, api, []) == (KeyClass.NONE, None)
    assert calls == api
