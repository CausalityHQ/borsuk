"""Narrow geometry checks for the V193 sealed transfer planner."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from scripts.optional_rank_utility import fit_optional_rank_utility
from scripts.source_rank_utility import RankUtility
from scripts.v193_optional_100k_transfer import _feature_plan


class Optional100kTransferTests(unittest.TestCase):
    def test_paired_cap_preserves_mandatory_interval_endpoint(self):
        request = {
            "query_ordinal": 0, "nominees": list(range(512)),
            "query": [0.01] * 768,
        }
        reference = {"query_ordinal": 0, "primary": list(range(100))}
        control = {"query_ordinal": 0, "pq_bytes": 4 * 24_960,
                   "pq_gets": 1}
        units = tuple(range(5))
        field = SimpleNamespace(
            units=units,
            scores=np.repeat(np.arange(5, dtype=np.float32), 32),
        )
        optional = fit_optional_rank_utility(
            [((0, 1, 2, 3, 4), (0, 1, 2, 3), {0: 99, 4: 1})],
            result_count=100)
        full = RankUtility((1.0,) * 5, (1,) * 5)
        indices = np.arange(512, dtype=np.int64)
        with patch(
                "scripts.v193_optional_100k_transfer.score_neighbor_field",
                return_value=field):
            plan = _feature_plan(
                request, reference, control,
                np.empty((0,)), np.empty((0,)), indices,
                indices, indices, (optional, full, optional))
        self.assertEqual(plan["mandatory_units"], [0, 1, 2, 3])
        for arm in plan["arms"].values():
            self.assertTrue(arm["feasible"])
            self.assertEqual(arm["intervals"], [[0, 3]])
            self.assertEqual(arm["units"], 4)
            self.assertEqual(arm["gets"], 1)
            self.assertEqual(arm["bytes"], control["pq_bytes"])


if __name__ == "__main__":
    unittest.main()
