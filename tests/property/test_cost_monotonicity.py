"""Property 15: Tính đơn điệu của Estimated_Cost.

Feature: das-observability-operations, Property 15: For any set of Usage_Records,
any valid Price_Table and any additional Usage_Record, the unrounded total
Estimated_Cost after adding the record is greater than or equal to the total before.

**Validates: Requirements 10.9**
"""
from __future__ import annotations

from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from das.ops.cost import estimate_cost, unrounded_costs
from tests.strategies import price_tables, usage_records


@settings(max_examples=200)
@given(
    table=price_tables(as_model=True),
    records=st.lists(usage_records(as_model=True), max_size=20),
    extra=usage_records(as_model=True),
    data=st.data(),
)
def test_adding_a_usage_record_never_decreases_estimated_cost(
    table, records, extra, data
) -> None:
    # The extra record may be appended anywhere in the sequence.
    index = data.draw(st.integers(min_value=0, max_value=len(records)), label="index")
    after_records = [*records[:index], extra, *records[index:]]

    before, _, _ = unrounded_costs(records, table)
    after, _, _ = unrounded_costs(after_records, table)

    assert after >= before

    # Rounding is monotone, so the reported amount must not decrease either.
    assert Decimal(estimate_cost(after_records, table).amount) >= Decimal(
        estimate_cost(records, table).amount
    )
