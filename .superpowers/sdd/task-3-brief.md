### Task 3: Idempotency helpers and Event payloads (spec task 1.4)

**Files:**
- Create: `das/client/idempotency.py`
- Create: `das/client/events.py`
- Test: `tests/unit/test_client_idempotency.py`
- Test: `tests/unit/test_client_events.py`

**Interfaces:**
- Consumes: `Budget`, `budget_status`, `budget_view` (Task 2); `das.models.job.TransitionEntry`, `JobState`; `das.models.plan.JobPlan`, `PlannedJob`; `das.models.request.RequestStatus`; `das.models.usage.UsageRecord`; `das.ops.cost.PriceTable`.
- Produces:
  - `MAX_IDEMPOTENCY_KEY_LENGTH = 128`
  - `validate_idempotency_key(value: str) -> bool`
  - `body_hash(prompt: str, budget: Budget | None) -> str` (64 hex chars)
  - `idem_key_hash(value: str) -> str` (64 hex chars)
  - `EventType` (Literal of 7 names), `EVENT_TYPES`, `TERMINAL_EVENT_TYPES`
  - `terminal_event_type(status: RequestStatus) -> EventType`
  - `created_event_data() -> dict[str, Any]`
  - `planned_event_data(plan: JobPlan) -> dict[str, Any]`
  - `transition_event_data(entry: TransitionEntry) -> dict[str, Any]`
  - `reopened_event_data() -> dict[str, Any]`
  - `terminal_event_data(status: RequestStatus, error: str | None, usage: Sequence[UsageRecord], budget: Budget | None, prices: PriceTable | None) -> dict[str, Any]`

- [ ] **Step 1: Write the failing idempotency tests**

Create `tests/unit/test_client_idempotency.py`:

```python
"""Idempotency_Key validation and Body_Hash (Phase 4 Req 4)."""
from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from das.client.budget import Budget
from das.client.idempotency import body_hash, idem_key_hash, validate_idempotency_key


@pytest.mark.parametrize("value", ["a", "abc-123_XYZ", "!" + "~" * 127, "a" * 128])
def test_valid_keys(value: str) -> None:
    assert validate_idempotency_key(value)


@pytest.mark.parametrize("value", ["", "a" * 129, "a b", "é", "a\n", "\n", "\x7f", "tab\tkey"])
def test_invalid_keys(value: str) -> None:
    assert not validate_idempotency_key(value)


def test_body_hash_known_value() -> None:
    expected = hashlib.sha256(b'{"budget":null,"prompt":"hi"}').hexdigest()
    assert body_hash("hi", None) == expected


def test_body_hash_ignores_decimal_spelling_and_empty_budget() -> None:
    assert body_hash("p", Budget(max_cost=Decimal("1.50"))) == body_hash("p", Budget(max_cost=Decimal("1.5")))
    assert body_hash("p", Budget()) == body_hash("p", None)


def test_body_hash_changes_with_prompt_and_budget() -> None:
    base = body_hash("p", Budget(max_total_tokens=10))
    assert body_hash("q", Budget(max_total_tokens=10)) != base
    assert body_hash("p", Budget(max_total_tokens=11)) != base
    assert body_hash("p", None) != base


def test_body_hash_accepts_lone_surrogates() -> None:
    # JSON "\ud800" decodes to a lone surrogate; hashing must not raise.
    assert len(body_hash("\ud800", None)) == 64


def test_idem_key_hash() -> None:
    assert idem_key_hash("k") == hashlib.sha256(b"k").hexdigest()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_idempotency.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.idempotency'`

- [ ] **Step 3: Write the idempotency module**

Create `das/client/idempotency.py`:

```python
"""Idempotent submit helpers (Phase 4 Req 4). Pure."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Final

from das.client.budget import Budget

MAX_IDEMPOTENCY_KEY_LENGTH: Final = 128
_KEY: Final = re.compile(rf"[\x21-\x7e]{{1,{MAX_IDEMPOTENCY_KEY_LENGTH}}}")


def validate_idempotency_key(value: str) -> bool:
    """1 to 128 printable ASCII characters, 0x21-0x7E (Req 4.1)."""
    return _KEY.fullmatch(value) is not None


def body_hash(prompt: str, budget: Budget | None) -> str:
    """Body_Hash: sha256 of the canonical JSON of ``{prompt, budget}`` as submitted (Req 4.3, 4.4).

    An empty Budget counts as no Budget, and ``max_cost`` uses its canonical text, so
    ``1.50`` and ``1.5`` hash the same.
    """
    obj = {
        "prompt": prompt,
        "budget": None if budget is None or budget.is_empty() else budget.to_json_obj(),
    }
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def idem_key_hash(value: str) -> str:
    """Hex sha256 of an Idempotency_Key, used in the Idempotency_Record key name."""
    return hashlib.sha256(value.encode("ascii")).hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_idempotency.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Write the failing events tests**

Create `tests/unit/test_client_events.py`:

```python
"""Event_Stream payloads (Phase 4 Req 6.1, 6.2)."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from das.client.budget import Budget
from das.client.events import (
    EVENT_TYPES,
    TERMINAL_EVENT_TYPES,
    created_event_data,
    planned_event_data,
    reopened_event_data,
    terminal_event_data,
    terminal_event_type,
    transition_event_data,
)
from das.models.job import JobState, TransitionEntry
from das.models.plan import JobPlan, PlannedJob
from das.models.request import RequestStatus
from das.models.results import TokenUsage
from das.models.usage import UsageRecord

USAGE = [
    UsageRecord(
        operation="job",
        provider="openai",
        model="m",
        usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15),
        job_id="j1",
        attempt=1,
    )
]


def test_terminal_types_are_event_types() -> None:
    assert TERMINAL_EVENT_TYPES <= set(EVENT_TYPES)
    assert len(EVENT_TYPES) == 7


@pytest.mark.parametrize(
    ("status", "event_type"),
    [
        (RequestStatus.COMPLETED, "request.completed"),
        (RequestStatus.FAILED, "request.failed"),
        (RequestStatus.CANCELLED, "request.cancelled"),
    ],
)
def test_terminal_event_type(status: RequestStatus, event_type: str) -> None:
    assert terminal_event_type(status) == event_type


@pytest.mark.parametrize("status", [RequestStatus.PENDING, RequestStatus.RUNNING])
def test_terminal_event_type_rejects_non_terminal(status: RequestStatus) -> None:
    with pytest.raises(ValueError):
        terminal_event_type(status)


def test_created_and_reopened() -> None:
    assert created_event_data() == {"status": "Pending"}
    assert reopened_event_data() == {"status": "Running"}


def test_planned_event_omits_instructions() -> None:
    plan = JobPlan(
        jobs=[
            PlannedJob(job_id="j1", instruction="secret instruction", capabilities=["research"]),
            PlannedJob(job_id="j2", instruction="other", capabilities=["summarize"], depends_on=["j1"]),
        ]
    )
    data = planned_event_data(plan)
    assert data == {
        "jobs": [
            {"job_id": "j1", "capabilities": ["research"], "depends_on": []},
            {"job_id": "j2", "capabilities": ["summarize"], "depends_on": ["j1"]},
        ]
    }
    assert "secret instruction" not in json.dumps(data)


def test_transition_event() -> None:
    entry = TransitionEntry(
        job_id="j1",
        from_state=JobState.READY,
        to_state=JobState.DISPATCHED,
        attempt=1,
        worker_id=None,
        at=datetime(2026, 10, 30, 1, 2, 3, 456000, tzinfo=UTC),
    )
    assert transition_event_data(entry) == {
        "job_id": "j1",
        "from": "Ready",
        "to": "Dispatched",
        "attempt": 1,
        "worker_id": None,
        "at": "2026-10-30T01:02:03.456Z",
    }


def test_transition_event_from_none() -> None:
    entry = TransitionEntry(
        job_id="j1",
        from_state=None,
        to_state=JobState.WAITING,
        attempt=0,
        worker_id="w1",
        at=datetime(2026, 10, 30, tzinfo=UTC),
    )
    data = transition_event_data(entry)
    assert data["from"] is None
    assert data["worker_id"] == "w1"
    assert data["at"] == "2026-10-30T00:00:00.000Z"


def test_terminal_event_without_budget() -> None:
    assert terminal_event_data(RequestStatus.COMPLETED, None, USAGE, None, None) == {
        "status": "Completed",
        "error": None,
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    }


def test_terminal_event_with_budget() -> None:
    data = terminal_event_data(RequestStatus.FAILED, "budget_exceeded", USAGE, Budget(max_total_tokens=10), None)
    assert data["error"] == "budget_exceeded"
    assert data["budget"] == {
        "limits": {"max_total_tokens": 10},
        "used": {"total_tokens": 15, "cost": None},
        "exceeded": True,
    }


def test_terminal_event_with_no_usage() -> None:
    data = terminal_event_data(RequestStatus.CANCELLED, None, [], Budget(max_cost=Decimal("1")), None)
    assert data["usage"] == {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    assert data["budget"]["exceeded"] is True  # max_cost without a Price_Table is unenforceable


def test_terminal_event_rejects_non_terminal_status() -> None:
    with pytest.raises(ValueError):
        terminal_event_data(RequestStatus.RUNNING, None, [], None, None)
```

- [ ] **Step 6: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_events.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.events'`

- [ ] **Step 7: Write the events module**

Create `das/client/events.py`:

```python
"""Event_Stream payloads (Phase 4 Req 6.1, 6.2). Pure.

Payloads never carry the prompt, Job instructions, Job_Result content or the
Final_Result (Req 6.2). The I/O shell passes them to the Lua scripts, which append
them in the same atomic write as the state change they describe.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC
from typing import Any, Final, Literal

from das.client.budget import Budget, budget_status, budget_view
from das.models.job import TransitionEntry
from das.models.plan import JobPlan
from das.models.request import RequestStatus
from das.models.usage import UsageRecord
from das.ops.cost import PriceTable

EventType = Literal[
    "request.created",
    "request.planned",
    "job.transition",
    "request.reopened",
    "request.completed",
    "request.failed",
    "request.cancelled",
]
EVENT_TYPES: Final[tuple[EventType, ...]] = (
    "request.created",
    "request.planned",
    "job.transition",
    "request.reopened",
    "request.completed",
    "request.failed",
    "request.cancelled",
)
TERMINAL_EVENT_TYPES: Final[frozenset[EventType]] = frozenset(
    {"request.completed", "request.failed", "request.cancelled"}
)

_TERMINAL: Final[dict[RequestStatus, EventType]] = {
    RequestStatus.COMPLETED: "request.completed",
    RequestStatus.FAILED: "request.failed",
    RequestStatus.CANCELLED: "request.cancelled",
}


def terminal_event_type(status: RequestStatus) -> EventType:
    """Terminal_Event type for a terminal Request_Status."""
    try:
        return _TERMINAL[status]
    except KeyError:
        raise ValueError(f"not a terminal status: {status.value}") from None


def created_event_data() -> dict[str, Any]:
    return {"status": RequestStatus.PENDING.value}


def planned_event_data(plan: JobPlan) -> dict[str, Any]:
    """Job_IDs, Capabilities and dependencies; never instructions."""
    return {
        "jobs": [
            {"job_id": j.job_id, "capabilities": list(j.capabilities), "depends_on": list(j.depends_on)}
            for j in plan.jobs
        ]
    }


def transition_event_data(entry: TransitionEntry) -> dict[str, Any]:
    """One ``job.transition`` Event per transition-log entry."""
    return {
        "job_id": entry.job_id,
        "from": entry.from_state.value if entry.from_state is not None else None,
        "to": entry.to_state.value,
        "attempt": entry.attempt,
        "worker_id": entry.worker_id,
        "at": entry.at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def reopened_event_data() -> dict[str, Any]:
    return {"status": RequestStatus.RUNNING.value}


def _usage_total(usage: Sequence[UsageRecord]) -> dict[str, int]:
    return {
        "input_tokens": sum(r.usage.input_tokens for r in usage),
        "output_tokens": sum(r.usage.output_tokens for r in usage),
        "total_tokens": sum(r.usage.total_tokens for r in usage),
    }


def terminal_event_data(
    status: RequestStatus,
    error: str | None,
    usage: Sequence[UsageRecord],
    budget: Budget | None,
    prices: PriceTable | None,
) -> dict[str, Any]:
    """Terminal_Event data: status, stored error, token usage total, and Budget_Used when budgeted."""
    terminal_event_type(status)
    data: dict[str, Any] = {"status": status.value, "error": error, "usage": _usage_total(usage)}
    if budget is not None:
        data["budget"] = budget_view(budget, budget_status(usage, budget, prices))
    return data
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_client_idempotency.py tests/unit/test_client_events.py -v`
Expected: PASS (all tests)

- [ ] **Step 9: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_idempotency.py tests/unit/test_client_events.py && uv run mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 10: Tick the spec task**

In `.kiro/specs/das-client-api/tasks.md` tick `1.4 Implement idempotency helpers and event payloads`.

---

