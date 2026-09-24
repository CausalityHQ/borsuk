"""Check the source oracle's truth and physical accounting."""

import pytest
from itertools import combinations

from scripts.v165_unit_interval_resources import UNIT_BYTES
from scripts.v166_surrogate_probe import modeled_plan
from scripts.v178_source_oracle_physical import (
    ARMS, _oracle, _score_plan, decision_for, truth_unit_masses,
)


def test_source_truth_units_preserve_multiple_neighbors_per_unit() -> None:
    ids = list(range(1, 101))
    units = {value: value // 2 for value in ids}
    masses = truth_unit_masses(ids, 0, units)
    assert sum(masses.values()) == 100
    assert masses[1] == 2
    with pytest.raises(ValueError):
        truth_unit_masses(ids, 1, units)


def test_oracle_charges_gaps_and_requires_primary_coverage() -> None:
    plan = _score_plan(((1, 3), (10, 10)), {1: 2, 2: 3, 10: 1}, [2, 10])
    assert plan["source_hits"] == 6
    assert plan["units"] == 4
    assert plan["bytes"] == 4 * UNIT_BYTES
    assert plan["gets"] == 2
    with pytest.raises(ValueError):
        _score_plan(((1, 3),), {1: 2}, [10])


def test_existing_interval_dp_matches_small_mandatory_oracle() -> None:
    mandatory = {0, 5}
    for masses in ({0: 1, 1: 2, 3: 3, 5: 1},
                   {1: 8, 2: 1, 4: 6, 5: 1}):
        intervals = modeled_plan(
            masses, mandatory, page_count=6, max_gets=2, max_units=4,
            nominee_count=6, unit_rows=1)
        selected = {unit for start, end in intervals for unit in range(start, end + 1)}
        actual = sum(masses.get(unit, 0) for unit in selected)
        possible = []
        for count in range(1, 5):
            for chosen in combinations(range(6), count):
                units = set(chosen)
                gets = sum(unit - 1 not in units for unit in units)
                if mandatory <= units and gets <= 2:
                    possible.append(sum(masses.get(unit, 0) for unit in units))
        assert actual == max(possible)


def test_oracle_reports_unbridgeable_primary_geometry() -> None:
    mandatory = [900 * index for index in range(33)]
    assert _oracle({0: 100}, mandatory) == {"infeasible_primary": True}
    assert _oracle({0: 100}, [mandatory[-1]])["source_hits"] == 100


def test_decision_attributes_first_failing_layer() -> None:
    hits = {arm: 12745 for arm in ARMS}
    p05 = {arm: 98 for arm in ARMS}
    assert decision_for(hits, p05, 12745) == "advance-to-gt-blind-calibration"
    for arm, expected in (("unit_transport", "unit-transport-killed"),
                          ("primary_constrained", "primary-layout-killed"),
                          ("width8_weighted", "candidate-witness-failed")):
        less = hits.copy()
        less[arm] = 12744
        assert decision_for(less, p05, 12745) == expected
    low_tail = p05.copy()
    low_tail["unit_transport"] = 97
    assert decision_for(hits, low_tail, 12745) == "unit-transport-killed"
