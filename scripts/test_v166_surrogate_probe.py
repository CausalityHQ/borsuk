"""Small geometry and accounting checks for the V166 remote probe."""

import unittest
import json
import tempfile
from pathlib import Path

import numpy as np

from scripts.v166_surrogate_probe import (
    candidate_units, source_unit_moments, unit_moment_without,
    modeled_plan, captured_exceedances, select_pseudoqueries,
)
from scripts.v166_surrogate_ranking_run import (
    QUERY_COUNT, UNIT_BYTES, ROUTER_MANIFEST_SHA, _score, canonical, check, plan,
)
from scripts.v155_relaion_returned_quality import sha256
from scripts.v155_relaion_returned_quality import SOURCE_SHA, SQ8_SHA, LAYOUT_SHA
from scripts.v165_unit_interval_resources import V164_ORDER_SHA, V164_TERMINAL_SHA
from scripts.v114_exact_local_100k import score_reference


class SurrogateProbeTests(unittest.TestCase):
    def test_candidate_neighbors_and_deduplication(self):
        old = np.arange(16, dtype=np.int64)
        inverse = np.arange(16, dtype=np.int64)
        self.assertEqual(candidate_units([0, 1, 15], old, inverse, 16, 4),
                         (0, 1, 2, 3))

    def test_leave_one_out_recomputes_own_unit(self):
        vectors = np.asarray([[1., 0.], [1., 0.], [0., 1.], [0., 1.]],
                             dtype=np.float32)
        order = np.arange(4, dtype=np.int64)
        mean, residual = source_unit_moments(vectors, order, 4)
        self.assertEqual(mean.shape, (1, 2))
        self.assertAlmostEqual(float(residual[0]), 0.5)
        own_mean, own_residual = unit_moment_without(vectors, order, 0, 0, 4)
        np.testing.assert_allclose(own_mean, [1. / 3., 2. / 3.], rtol=0, atol=1e-6)
        self.assertAlmostEqual(own_residual, 4. / 9.)

    def test_raw_l2_moments_preserve_source_scale(self):
        vectors = np.asarray([[2., 0.], [4., 0.]], dtype=np.float32)
        mean, residual = source_unit_moments(
            vectors, np.asarray([0, 1], dtype=np.int64), 2)
        np.testing.assert_array_equal(mean[0], np.asarray([3., 0.], dtype=np.float16))
        self.assertEqual(float(residual[0]), 1.0)

    def test_modeled_plan_covers_primary_with_matched_caps(self):
        weights = {0: 100, 2: 100, 3: 3, 4: 4}
        intervals = modeled_plan(weights, (0, 2), page_count=8,
                                 max_gets=2, max_units=4, nominee_count=4,
                                 unit_rows=1)
        covered = {page for start, end in intervals for page in range(start, end + 1)}
        self.assertTrue({0, 2}.issubset(covered))
        self.assertLessEqual(sum(end - start + 1 for start, end in intervals), 4)
        self.assertLessEqual(len(intervals), 2)

    def test_capture_counts_only_units_in_intervals(self):
        actual = {0: 2, 2: 4, 3: 1}
        self.assertEqual(captured_exceedances(actual, ((0, 0), (2, 3))), 7)
        self.assertEqual(captured_exceedances(actual, ((1, 2),)), 4)

    def test_pseudoquery_selection_is_stable_under_row_permutation(self):
        left = select_pseudoqueries([10, 20, 30, 40], 4)
        right = select_pseudoqueries([40, 30, 10, 20], 4)
        self.assertEqual(left, right)
        self.assertEqual(len(set(left)), 4)

    def test_plan_artifacts_replay_without_ground_truth(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (root / "cases.jsonl").open("w") as output:
                for ordinal in range(QUERY_COUNT):
                    row = {"query_ordinal": ordinal, "source_id": ordinal,
                           "threshold": 1.2, "nominees": [0, 1, 2, 3],
                           "primary_units": [0],
                           "unit_cases": [[0, 1.0, 0.2, 2, 1, 1],
                                          [1, 1.0, 0.2, 2, 2, 2]],
                           "candidate_rows": 4, "eligible_rows": 4,
                           "source_sq8_abs_error_sum": 0.0,
                           "source_sq8_abs_error_max": 0.0,
                           "source_sq8_threshold_crossings": 0}
                    if ordinal >= QUERY_COUNT // 2:
                        row.update(baseline_intervals=[[0, 0]],
                                   baseline_bytes=UNIT_BYTES, baseline_gets=1)
                    output.write(canonical(row))
            (root / "means.npy").write_bytes(b"synthetic-mean")
            (root / "residuals.npy").write_bytes(b"synthetic-residual")
            (root / "prepare-seal.json").write_text(canonical({
                "schema": "borsuk-v166-surrogate-ranking-v1-prepare-seal",
                "gt_opened": False,
                "source_sha256": SOURCE_SHA,
                "old_sq8_sha256": SQ8_SHA,
                "router_manifest_sha256": ROUTER_MANIFEST_SHA,
                "old_layout_sha256": LAYOUT_SHA,
                "order_sha256": V164_ORDER_SHA,
                "v164_terminal_sha256": V164_TERMINAL_SHA,
                "pseudo_ids": list(range(QUERY_COUNT)),
                "cases_sha256": sha256(root / "cases.jsonl"),
                "means_sha256": sha256(root / "means.npy"),
                "residuals_sha256": sha256(root / "residuals.npy"),
            }))
            plan(root)
            result = check(root)
            self.assertEqual(result["status"], "pass")
            self.assertEqual(result["decision"], "killed")
            self.assertFalse(json.loads((root / "summary.json").read_text())["gt_opened"])

    def test_compact_scoring_preserves_reference_on_scattered_rows(self):
        dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (2,))])
        sq8 = np.zeros(6, dtype=dtype)
        sq8["id"] = np.asarray([9, 8, 7, 6, 5, 4], dtype=np.int64)
        sq8["norm"] = np.asarray([1, 2, 3, 4, 5, 6], dtype=np.float32)
        sq8["code"] = np.arange(12, dtype=np.uint8).reshape(6, 2)
        selected = np.asarray([5, 1, 3], dtype=np.int64)
        query = np.asarray([0.1, 0.2], dtype=np.float32)
        low = np.asarray([0.0, 0.0], dtype=np.float32)
        step = np.asarray([0.01, 0.02], dtype=np.float32)
        expected = score_reference(query=query, nominees=selected,
                                   ids=sq8["id"], norms=sq8["norm"],
                                   codes=sq8["code"], low=low, step=step,
                                   primary_count=2)
        actual = _score(sq8, query, selected, low, step, 2)
        self.assertEqual(actual[0], expected[0])
        np.testing.assert_array_equal(actual[1].view(np.uint32),
                                      expected[1].view(np.uint32))


if __name__ == "__main__":
    unittest.main()
