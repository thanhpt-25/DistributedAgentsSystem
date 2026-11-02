# Feature: das-observability-operations, Property 14: Tính cộng tính và định dạng của Estimated_Cost
"""Property 14: additivity and formatting of Estimated_Cost.

*For any* set of Usage_Records (provider/model including case variants of table
entries) and any valid Price_Table:

- unrounded total = sum of unrounded per-pair amounts = sum of the Estimated_Cost
  of the parts of a random partition of the records;
- a record is priced iff its provider and model match an entry exactly;
- ``complete`` is false iff there is an Unpriced_Model;
- ``unpriced_models`` has no duplicates and is sorted by (provider, model);
- every reported amount matches ``^\\d+\\.\\d{6}$`` and equals the 6-place
  round-half-even of the unrounded value.

**Validates: Requirements 10.4, 10.5, 10.6, 10.8**
"""

from __future__ import annotations

import re
from decimal import Decimal
from fractions import Fraction

from hypothesis import given
from hypothesis import strategies as st

from das.ops.cost import PriceTable, estimate_cost, unrounded_costs
from tests.strategies import price_tables, usage_records

_AMOUNT_RE = re.compile(r"^\d+\.\d{6}$")
_SCALE = 1_000_000


def _round_half_even_6(x: Fraction) -> str:
    """Independent oracle: round a non-negative Fraction to 6 places, half-even."""
    scaled = x * _SCALE
    floor = scaled.numerator // scaled.denominator
    rest = scaled - floor
    if rest > Fraction(1, 2) or (rest == Fraction(1, 2) and floor % 2 == 1):
        floor += 1
    return f"{floor // _SCALE}.{floor % _SCALE:06d}"


def _exact_costs(records: list, table: PriceTable) -> tuple[dict, set]:
    """Exact (Fraction) cost per priced pair and set of unpriced pairs, by linear scan."""
    by_pair: dict[tuple[str, str], Fraction] = {}
    unpriced: set[tuple[str, str]] = set()
    for r in records:
        match = [e for e in table.models if e.provider == r.provider and e.model == r.model]
        pair = (r.provider, r.model)
        if not match:
            unpriced.add(pair)
            continue
        e = match[0]
        cost = (
            Fraction(r.usage.input_tokens) * Fraction(e.input_per_million)
            + Fraction(r.usage.output_tokens) * Fraction(e.output_per_million)
        ) / _SCALE
        by_pair[pair] = by_pair.get(pair, Fraction(0)) + cost
    return by_pair, unpriced


@st.composite
def _records_and_partition(draw: st.DrawFn) -> tuple[list, list[list]]:
    records = draw(st.lists(usage_records(as_model=True), max_size=20))
    n_parts = draw(st.integers(min_value=1, max_value=4))
    labels = draw(
        st.lists(
            st.integers(min_value=0, max_value=n_parts - 1),
            min_size=len(records),
            max_size=len(records),
        )
    )
    parts = [[r for r, lbl in zip(records, labels, strict=True) if lbl == i] for i in range(n_parts)]
    return records, parts


@given(table=price_tables(as_model=True), data=_records_and_partition())
def test_estimated_cost_additivity_and_format(
    table: PriceTable, data: tuple[list, list[list]]
) -> None:
    records, parts = data
    total, by_pair, unpriced = unrounded_costs(records, table)
    report = estimate_cost(records, table)
    exp_by_pair, exp_unpriced = _exact_costs(records, table)

    # Req 10.5: priced iff exact (case-sensitive) match.
    assert set(by_pair) == set(exp_by_pair)
    assert unpriced == exp_unpriced
    assert {(m.provider, m.model) for m in report.by_model} == set(exp_by_pair)

    # Req 10.4 / 10.8: unrounded amounts are exact; total = sum of per-pair amounts.
    for pair, amount in by_pair.items():
        assert Fraction(amount) == exp_by_pair[pair]
    assert Fraction(total) == sum(exp_by_pair.values(), Fraction(0))
    assert total == sum(by_pair.values(), Decimal(0))

    # Req 10.8: total = sum of the unrounded totals of the parts of a partition.
    part_totals = [unrounded_costs(p, table)[0] for p in parts]
    assert Fraction(total) == sum((Fraction(t) for t in part_totals), Fraction(0))

    # Req 10.6: complete iff no Unpriced_Model; unpriced list unique and sorted.
    pairs = [(m.provider, m.model) for m in report.unpriced_models]
    assert report.complete == (not exp_unpriced)
    assert len(pairs) == len(set(pairs))
    assert pairs == sorted(pairs)
    assert set(pairs) == exp_unpriced
    assert report.currency == table.currency

    # Req 10.4: reported amounts are the rounded unrounded values, fixed 6-place format.
    assert _AMOUNT_RE.match(report.amount)
    assert report.amount == _round_half_even_6(sum(exp_by_pair.values(), Fraction(0)))
    for m in report.by_model:
        assert _AMOUNT_RE.match(m.amount)
        assert m.amount == _round_half_even_6(exp_by_pair[(m.provider, m.model)])
