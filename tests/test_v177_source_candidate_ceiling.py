"""Geometry and label isolation checks for the V177 source-only gate."""

import numpy as np
import pytest

from scripts.v177_source_candidate_ceiling import coverage, expanded_units


def test_candidate_neighborhood_clips_at_physical_boundaries() -> None:
    assert expanded_units([0, 3], 1, 5) == (0, 1, 2, 3, 4)
    assert expanded_units([2], 0, 5) == (2,)
    with pytest.raises(ValueError):
        expanded_units([2, 2], 1, 5)


def test_source_truth_coverage_uses_relaid_32_row_units() -> None:
    truth = np.arange(100, dtype=np.int64)
    inverse = np.arange(100, dtype=np.int64)
    assert coverage(truth, inverse, [0], 4) == {
        "0": 32, "1": 64, "2": 96, "4": 100, "8": 100,
    }
    with pytest.raises(ValueError):
        coverage(np.full(100, 0, dtype=np.int64), inverse, [0], 4)
