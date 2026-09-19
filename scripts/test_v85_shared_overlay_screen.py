import unittest

import numpy as np

from scripts.v85_shared_overlay_screen import evaluate_overlay


class V85SharedOverlayScreenTests(unittest.TestCase):
    def test_resident_delta_closes_base_shortlist_and_run_count_is_invariant(self) -> None:
        base = np.asarray(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.9, 0.1, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.9, 0.1, 0.0],
                [0.0, 0.0, 1.0, 0.0],
                [0.0, 0.0, 0.9, 0.1],
                [0.0, 0.0, 0.0, 1.0],
                [0.1, 0.0, 0.0, 0.9],
            ],
            dtype=np.float32,
        )
        delta = np.asarray([[0.99, 0.01, 0.0, 0.0]], dtype=np.float32)
        queries = np.asarray([[1.0, 0.0, 0.0, 0.0]], dtype=np.float32)
        result = evaluate_overlay(
            base,
            delta,
            queries,
            page_rows=2,
            neighbors=2,
            subspaces=2,
            clusters=4,
            shortlists=(1, 2),
            logical_run_counts=(1, 10, 100),
            seed=85,
        )

        self.assertEqual(result["training_rows"], 8)
        self.assertEqual(result["delta_rows"], 1)
        self.assertEqual(result["cpu_parallelism"], "sequential-per-query")
        for cell in result["cells"]:
            self.assertEqual(cell["logical_run_results"], [cell["result_ids"]] * 3)
            self.assertIn(8, cell["result_ids"])
            self.assertLessEqual(cell["base_gets_max"], 2)
            self.assertGreaterEqual(cell["sq8_recall_ppm"], 500_000)

    def test_nonfinite_or_incompatible_inputs_fail_closed(self) -> None:
        base = np.eye(4, dtype=np.float32)
        delta = np.eye(4, dtype=np.float32)[:1]
        queries = np.eye(4, dtype=np.float32)[:1]
        with self.assertRaisesRegex(ValueError, "finite"):
            broken = base.copy()
            broken[0, 0] = np.nan
            evaluate_overlay(broken, delta, queries, subspaces=2, clusters=2)
        with self.assertRaisesRegex(ValueError, "shape"):
            evaluate_overlay(base, delta[:, :3], queries, subspaces=2, clusters=2)

    def test_layout_order_preserves_registered_ids_and_frozen_truth(self) -> None:
        base = np.asarray(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
                [0.0, 0.0, 0.0, 1.0],
            ],
            dtype=np.float32,
        )
        delta = np.asarray([[0.99, 0.01, 0.0, 0.0]], dtype=np.float32)
        queries = np.asarray([[1.0, 0.0, 0.0, 0.0]], dtype=np.float32)

        result = evaluate_overlay(
            base,
            delta,
            queries,
            base_ids=np.asarray([401, 101, 301, 201], dtype=np.int64),
            delta_ids=np.asarray([901], dtype=np.int64),
            truth_ids=np.asarray([[401, 901]], dtype=np.int64),
            page_rows=2,
            neighbors=2,
            subspaces=2,
            clusters=2,
            shortlists=(1,),
            training_sample_rows=2,
            encode_chunk_rows=2,
        )

        self.assertEqual(result["training_sample_rows"], 2)
        self.assertEqual(result["delta_resident_bytes"], 48)
        self.assertEqual(result["delta_exact_resident_bytes"], 24)
        self.assertEqual(result["cells"][0]["exact_recall_ppm"], 1_000_000)
        self.assertEqual(result["cells"][0]["hybrid_recall_ppm"], 1_000_000)
        self.assertEqual(result["cells"][0]["base_gets_p50"], 1)
        self.assertEqual(result["cells"][0]["base_gets_p95"], 1)
        self.assertEqual(result["cells"][0]["base_bytes_p50"], 32)
        self.assertEqual(result["cells"][0]["base_bytes_p95"], 32)
        self.assertEqual(set(result["cells"][0]["result_ids"]), {401, 901})
        self.assertEqual(
            result["promotion_gate"],
            {
                "max_base_bytes": 16 * 1024 * 1024,
                "max_base_gets": 32,
                "min_sq8_recall_ppm": 990_000,
                "passing_shortlists": [1],
                "passed": True,
            },
        )

    def test_per_page_sq8_preserves_local_resolution_lost_by_global_sq8(self) -> None:
        base = np.asarray(
            [
                [0.0, 0.0],
                [0.001, 0.0],
                [1_000.0, 1_000.0],
                [1_000.001, 1_000.0],
            ],
            dtype=np.float32,
        )
        result = evaluate_overlay(
            base,
            np.asarray([[2_000.0, 2_000.0]], dtype=np.float32),
            np.asarray([[0.001, 0.0]], dtype=np.float32),
            base_ids=np.asarray([10, 20, 30, 40], dtype=np.int64),
            delta_ids=np.asarray([50], dtype=np.int64),
            truth_ids=np.asarray([[20]], dtype=np.int64),
            page_rows=2,
            neighbors=1,
            subspaces=2,
            clusters=2,
            shortlists=(1,),
            training_sample_rows=4,
            encode_chunk_rows=2,
        )

        cell = result["cells"][0]
        self.assertEqual(cell["sq8_recall_ppm"], 0)
        self.assertEqual(cell["page_sq8_recall_ppm"], 1_000_000)
        self.assertEqual(cell["result_ids"], [20])
        self.assertEqual(result["base_quantizer"], "per-page-sq8")
        self.assertEqual(result["base_quantizer_resident_bytes"], 32)


if __name__ == "__main__":
    unittest.main()
