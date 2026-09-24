"""Check exact mandatory-gap lower-bound arithmetic."""

import pytest

from scripts.certify_v178_primary_gap import minimum_primary_cover


def test_shortest_gaps_are_required_to_meet_get_cap() -> None:
    # Runs [1,2], [5], [20,21]; bridging the first gap costs 2 units.
    assert minimum_primary_cover([1, 2, 5, 20, 21], 2) == {
        "primary_units": 5,
        "runs": 3,
        "bridged_units_floor": 2,
        "minimum_units": 7,
        "minimum_bytes": 7 * 24_960,
    }
    with pytest.raises(ValueError):
        minimum_primary_cover([2, 1], 2)
