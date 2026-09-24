"""Narrow deterministic checks for the GT-blind V170 physical rule."""

import unittest
from unittest.mock import patch

import numpy as np

from scripts.v170_pq_field_100k import (
    UNIT_BYTES, _one_plan, matched_plan, neighbor_rank_scores, ranked_unit_weights,
    validate_paired_plan,
)
from scripts.v168_scored_neighbor_field import ScoredNeighborField


class V170PhysicalRuleTests(unittest.TestCase):
    def test_rank_weights_and_primary_priority(self):
        weights = ranked_unit_weights(
            {7: 1.0, 3: 1.0, 4: 2.0}, {4}, page_count=10,
        )
        self.assertGreater(weights[3], weights[7])
        self.assertGreater(weights[7], 0)
        self.assertGreater(weights[4], sum(weights[u] for u in (3, 7)))

    def test_matched_plan_forces_primary_and_keeps_budget(self):
        plan = matched_plan(
            {0: 0.1, 1: 5.0, 5: 0.2, 8: 0.3}, {5},
            page_count=10, max_gets=2, max_units=3,
        )
        covered = {u for a, b in plan for u in range(a, b + 1)}
        self.assertIn(5, covered)
        self.assertLessEqual(len(plan), 2)
        self.assertLessEqual(len(covered), 3)

    def test_rejects_bad_score_or_unrepresented_primary(self):
        with self.assertRaises(ValueError):
            ranked_unit_weights({0: float("nan")}, {0}, page_count=2)
        with self.assertRaises(ValueError):
            ranked_unit_weights({0: 1.0}, {1}, page_count=2)
        with self.assertRaises(ValueError):
            matched_plan({0: np.float32(1.0)}, {0},
                         page_count=1, max_gets=1, max_units=1)

    def test_neighbor_rank_uses_old_to_new_permutation(self):
        inverse = np.array([0, 96, 32, 64], dtype=np.int64)
        scores = neighbor_rank_scores(
            nominees=[1, 2], inverse=inverse, units=(0, 1, 2, 3),
            unit_rows=32,
        )
        self.assertEqual(scores, {0: 2.0, 1: 2.0, 2: 1.0, 3: 1.0})

    def test_independent_recount_rejects_missing_primary(self):
        inverse = np.arange(100_000, dtype=np.int64)
        inverse[0], inverse[32] = inverse[32], inverse[0]
        control = {"bytes": 2 * UNIT_BYTES, "gets": 1}
        planned = {
            "pq_ranges": [[0, UNIT_BYTES]], "pq_bytes": UNIT_BYTES,
            "pq_gets": 1,
            "neighbor_ranges": [[UNIT_BYTES, 2 * UNIT_BYTES]],
            "neighbor_bytes": UNIT_BYTES, "neighbor_gets": 1,
        }
        with self.assertRaises(ValueError):
            validate_paired_plan(planned, [0], inverse, control)
        planned["pq_ranges"] = [[UNIT_BYTES, 2 * UNIT_BYTES]]
        self.assertIsNone(validate_paired_plan(planned, [0], inverse, control))

    def test_one_plan_uses_nonidentity_old_to_new_rows(self):
        inverse = np.arange(100_000, dtype=np.int64)
        inverse[:100], inverse[1000:1100] = (
            inverse[1000:1100].copy(), inverse[:100].copy(),
        )
        order = np.empty_like(inverse)
        order[inverse] = np.arange(inverse.size)
        field = ScoredNeighborField(
            (31, 32, 33, 34), np.arange(128, dtype=np.int64),
            np.linspace(0, 1, 128, dtype=np.float32),
        )
        request = {"query_ordinal": 0, "query": [0.0] * 768,
                   "nominees": list(range(512))}
        reference = {"query_ordinal": 0, "primary": list(range(100))}
        control = {"query_ordinal": 0, "ranges": [
            [31 * UNIT_BYTES, 35 * UNIT_BYTES]], "bytes": 4 * UNIT_BYTES}
        with patch("scripts.v170_pq_field_100k.score_neighbor_field",
                   return_value=field):
            result = _one_plan(request, reference, control,
                               np.empty((0,)), np.empty((0,)),
                               order, inverse, np.arange(100_000))
        self.assertEqual(result["mandatory_units"], 4)
        self.assertEqual(result["pq_ranges"], control["ranges"])
        self.assertEqual(result["neighbor_ranges"], control["ranges"])


if __name__ == "__main__":
    unittest.main()
