import itertools
import unittest
from unittest import mock

import numpy as np

from scripts.v85_shared_overlay_screen import (
    _build_landmark_incidence,
    _landmark_page_scores,
    _maximum_physical_oracle_hits,
    _page_payload_bytes,
    _select_optimal_weighted_pages,
    _select_rank_weighted_pages,
    evaluate_overlay,
    evaluate_physical_oracle,
)


class V85SharedOverlayScreenTests(unittest.TestCase):
    def test_landmark_incidence_routes_by_base_neighborhood_without_queries(
        self,
    ) -> None:
        base = np.asarray(
            [
                [0.0, 0.0],
                [0.1, 0.0],
                [10.0, 0.0],
                [10.1, 0.0],
                [0.0, 10.0],
                [0.0, 10.1],
                [10.0, 10.0],
                [10.1, 10.0],
            ],
            dtype=np.float32,
        )

        artifact = _build_landmark_incidence(
            base,
            np.arange(10, 90, 10, dtype=np.int64),
            page_rows=2,
            landmark_count=8,
            neighbor_count=1,
            pages_per_landmark=1,
            seed=85,
            chunk_rows=3,
        )
        scores = _landmark_page_scores(
            np.asarray([0.0, 0.0], dtype=np.float32),
            artifact,
            query_landmarks=2,
        )

        self.assertEqual(artifact["landmarks"].shape, (8, 2))
        self.assertEqual(artifact["page_ordinals"].shape, (8, 1))
        self.assertTrue(np.all(artifact["discarded_mass"] >= 0.0))
        self.assertEqual(int(np.argmax(scores)), 0)
        self.assertGreater(scores[0], 0.99)

    def test_overlay_evaluates_landmark_incidence_under_physical_budget(self) -> None:
        base = np.asarray(
            [
                [0.0, 0.0],
                [0.1, 0.0],
                [10.0, 0.0],
                [10.1, 0.0],
                [0.0, 10.0],
                [0.0, 10.1],
                [10.0, 10.0],
                [10.1, 10.0],
            ],
            dtype=np.float32,
        )
        result = evaluate_overlay(
            base,
            np.asarray([[20.0, 20.0]], dtype=np.float32),
            np.asarray([[0.0, 0.0]], dtype=np.float32),
            base_ids=np.arange(10, 90, 10, dtype=np.int64),
            delta_ids=np.asarray([90], dtype=np.int64),
            truth_ids=np.asarray([[10, 20]], dtype=np.int64),
            page_rows=2,
            neighbors=2,
            subspaces=2,
            clusters=2,
            shortlists=(1,),
            rank_top_rows=(1,),
            rank_page_caps=(1,),
            training_sample_rows=8,
            encode_chunk_rows=4,
            max_base_bytes=108,
            max_base_gets=1,
            landmark_count=8,
            landmark_neighbors=1,
            pages_per_landmark=1,
            query_landmarks=2,
            landmark_chunk_rows=3,
        )

        self.assertEqual(len(result["landmark_incidence_cells"]), 1)
        self.assertEqual(
            result["landmark_incidence_cells"][0]["page_sq8_recall_ppm"],
            1_000_000,
        )
        self.assertTrue(result["landmark_incidence_gate"]["passed"])

    def test_physical_oracle_charges_gaps_and_maximizes_hits_exactly(self) -> None:
        page_hits = {0: 3, 3: 2, 4: 5, 8: 4}

        self.assertEqual(
            _maximum_physical_oracle_hits(
                page_hits, page_count=10, max_pages=3, max_ranges=2
            ),
            11,
        )
        self.assertEqual(
            _maximum_physical_oracle_hits(
                page_hits, page_count=10, max_pages=2, max_ranges=1
            ),
            7,
        )

        for max_pages, max_ranges in ((2, 1), (3, 1), (3, 2), (5, 2)):
            exhaustive = 0
            for mask in itertools.product((False, True), repeat=6):
                selected = [index for index, take in enumerate(mask) if take]
                if len(selected) > max_pages:
                    continue
                ranges = sum(
                    index == 0 or not mask[index - 1]
                    for index, take in enumerate(mask)
                    if take
                )
                if ranges <= max_ranges:
                    exhaustive = max(
                        exhaustive,
                        sum({0: 3, 2: 5, 5: 4}.get(page, 0) for page in selected),
                    )
            self.assertEqual(
                _maximum_physical_oracle_hits(
                    {0: 3, 2: 5, 5: 4},
                    page_count=6,
                    max_pages=max_pages,
                    max_ranges=max_ranges,
                ),
                exhaustive,
            )

    def test_page_payload_prices_header_bounds_and_padded_records(self) -> None:
        self.assertEqual(_page_payload_bytes(dimensions=768, page_rows=256), 205_888)

    def test_optimal_weighted_pages_matches_exhaustive_physical_planner(
        self,
    ) -> None:
        weights = np.asarray([5, 0, 4, 0, 0, 6], dtype=np.uint64)

        pages, ranges = _select_optimal_weighted_pages(
            weights, max_span_pages=3, max_ranges=2
        )

        self.assertEqual(pages.tolist(), [0, 5])
        self.assertEqual(ranges, [(0, 0), (5, 5)])
        self.assertEqual(int(np.sum(weights[pages])), 11)

        pages, ranges = _select_optimal_weighted_pages(
            np.asarray([0.6, 0.0, 0.5], dtype=np.float64),
            max_span_pages=1,
            max_ranges=1,
        )
        self.assertEqual(pages.tolist(), [0])
        self.assertEqual(ranges, [(0, 0)])

        for max_pages, max_ranges in ((2, 1), (3, 1), (3, 2), (5, 2)):
            exhaustive = 0
            for mask in itertools.product((False, True), repeat=weights.size):
                selected = [index for index, take in enumerate(mask) if take]
                if len(selected) > max_pages:
                    continue
                range_count = sum(
                    index == 0 or not mask[index - 1]
                    for index, take in enumerate(mask)
                    if take
                )
                if range_count <= max_ranges:
                    exhaustive = max(exhaustive, int(np.sum(weights[selected])))
            pages, ranges = _select_optimal_weighted_pages(
                weights, max_span_pages=max_pages, max_ranges=max_ranges
            )
            self.assertEqual(int(np.sum(weights[pages])), exhaustive)
            self.assertLessEqual(pages.size, max_pages)
            self.assertLessEqual(len(ranges), max_ranges)

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

        self.assertEqual(result["schema"], "borsuk-v85-shared-overlay-screen-v6")
        self.assertEqual(result["pq_lloyd_iterations"], 10)
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
            rank_top_rows=(1,),
            rank_page_caps=(1,),
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
        self.assertEqual(result["cells"][0]["base_bytes_p50"], 128)
        self.assertEqual(result["cells"][0]["base_bytes_p95"], 128)
        self.assertEqual(set(result["cells"][0]["result_ids"]), {401, 901})
        self.assertEqual(
            result["rank_weighted_cells"][0]["page_sq8_recall_ppm"], 1_000_000
        )
        self.assertEqual(
            {cell["planner"] for cell in result["rank_weighted_cells"]},
            {"exact", "greedy"},
        )
        self.assertEqual(result["rank_weighted_cells"][0]["base_gets_max"], 1)
        self.assertEqual(result["rank_weighted_cells"][0]["base_bytes_max"], 128)
        self.assertTrue(result["rank_weighted_gate"]["passed"])
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
        self.assertEqual(result["base_quantizer_resident_bytes"], 0)
        self.assertEqual(result["base_quantizer_location"], "page-payload")

    def test_overlay_reports_exact_joint_physical_budget_oracle(self) -> None:
        base = np.eye(4, dtype=np.float32)
        delta = np.asarray([[0.5, 0.5, 0.5, 0.5]], dtype=np.float32)
        result = evaluate_overlay(
            base,
            delta,
            np.asarray([[1.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            base_ids=np.asarray([10, 20, 30, 40], dtype=np.int64),
            delta_ids=np.asarray([50], dtype=np.int64),
            truth_ids=np.asarray([[10, 30, 40, 50]], dtype=np.int64),
            page_rows=1,
            neighbors=4,
            subspaces=2,
            clusters=2,
            shortlists=(1,),
            rank_top_rows=(1,),
            rank_page_caps=(2,),
            training_sample_rows=4,
            encode_chunk_rows=2,
            max_base_bytes=224,
            max_base_gets=1,
        )

        self.assertEqual(result["page_payload_bytes"], 112)
        self.assertEqual(
            result["physical_oracle"],
            {
                "base_truth_hits": 3,
                "delta_truth_hits": 1,
                "hits": 3,
                "max_base_bytes": 224,
                "max_base_gets": 1,
                "max_base_pages": 2,
                "min_recall_ppm": 995_000,
                "passed": False,
                "recall_ppm": 750_000,
                "worst_query_recall_ppm": 750_000,
            },
        )

    def test_physical_oracle_runs_without_vectors_or_router_training(self) -> None:
        result = evaluate_physical_oracle(
            base_ids=np.asarray([10, 20, 30, 40], dtype=np.int64),
            delta_ids=np.asarray([50], dtype=np.int64),
            truth_ids=np.asarray([[10, 30, 40, 50]], dtype=np.int64),
            dimensions=4,
            page_rows=1,
            max_base_bytes=224,
            max_base_gets=1,
        )

        self.assertEqual(result["schema"], "borsuk-v85-physical-oracle-v1")
        self.assertFalse(result["claim_eligible"])
        self.assertEqual(result["page_payload_bytes"], 112)
        self.assertEqual(result["physical_oracle"]["recall_ppm"], 750_000)

    def test_physical_oracle_does_not_require_keyword_aware_zip(self) -> None:
        builtin_zip = zip

        def legacy_zip(*iterables: object, **keywords: object) -> object:
            if keywords:
                raise TypeError("zip() takes no keyword arguments")
            return builtin_zip(*iterables)

        with mock.patch("builtins.zip", side_effect=legacy_zip):
            result = evaluate_physical_oracle(
                base_ids=np.asarray([10, 20], dtype=np.int64),
                delta_ids=np.asarray([30], dtype=np.int64),
                truth_ids=np.asarray([[10, 30]], dtype=np.int64),
                dimensions=2,
                page_rows=1,
                max_base_bytes=96,
                max_base_gets=1,
            )

        self.assertEqual(result["physical_oracle"]["recall_ppm"], 1_000_000)

    def test_rank_weighted_pages_preserve_nearest_evidence_under_hard_budget(
        self,
    ) -> None:
        scores = np.asarray(
            [0.01, 9.0, 0.02, 9.0, 0.03, 9.0, 0.04, 0.05],
            dtype=np.float32,
        )

        pages, ranges = _select_rank_weighted_pages(
            scores,
            page_rows=2,
            top_rows=5,
            max_span_pages=1,
            max_ranges=1,
            gap=0,
        )

        self.assertEqual(pages.tolist(), [0])
        self.assertEqual(ranges, [(0, 0)])


if __name__ == "__main__":
    unittest.main()
