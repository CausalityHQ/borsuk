"""Focused contracts for the V103 metric-aware PQ48 spike."""

import dataclasses
import json
import unittest

import numpy as np

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v102_two_wave_pq48_refinement import PQ48X8
from scripts.v103_metric_aware_pq48 import (
    canonical_v103_result_bytes,
    evaluate_v103,
    metric_aware_adc_scores,
    project_v103_resident_bytes_100m,
    score_retained_rows_metric_aware,
    vector_norm_evidence,
)


class V103MetricAwarePq48Tests(unittest.TestCase):
    @staticmethod
    def config() -> RankedGapConfig:
        return RankedGapConfig(
            pages_per_root=8,
            maximum_root_groups=65_536,
            maximum_exposed_pages=4_096,
            retained_pages=1_024,
            maximum_scanned_rows=262_144,
            shortlist_rows=8_192,
            maximum_gets=32,
            maximum_bytes=16 * 1024**2,
        )

    def test_source_norm_score_removes_reconstructed_norm_bias(self) -> None:
        # Break caught: the challenger accidentally keeps the reconstructed
        # centroid norm, reducing to the already-rejected V102 L2 arm.
        query = np.array([1.0, 1.0], dtype=np.float32)
        books = np.zeros((2, 256, 1), dtype=np.float32)
        books[:, 0, 0] = 1.0
        books[:, 1, 0] = 0.8
        codes = np.array([[0, 0], [1, 1]], dtype=np.uint8)
        source_norms = np.array([4.0, 1.0], dtype=np.float32)

        corrected = metric_aware_adc_scores(
            query, books, codes, source_norms, subspaces=2, centroid_bits=8
        )

        np.testing.assert_array_equal(
            corrected.view(np.uint32),
            np.array([0.0, -2.2], dtype=np.float32).view(np.uint32),
        )

    def test_bounded_scorer_uses_score_then_id_total_order(self) -> None:
        # Break caught: block-local ordering or row position, rather than the
        # registered global (score,id) order, decides a boundary tie.
        query = np.array([1.0, 1.0], dtype=np.float32)
        books = np.zeros((2, 256, 1), dtype=np.float32)
        books[:, 0, 0] = 1.0
        books[:, 1, 0] = 0.8
        codes = np.array([[0, 0], [1, 1], [1, 1]], dtype=np.uint8)
        source_norms = np.array([4.0, 1.0, 1.0], dtype=np.float32)

        ranked = score_retained_rows_metric_aware(
            query,
            np.array([30, 20, 10], dtype=np.int64),
            codes,
            source_norms,
            books,
            subspaces=2,
            centroid_bits=8,
            maximum_rows=3,
            shortlist_rows=2,
            block_rows=1,
        )

        self.assertEqual(ranked, (10, 20))

    def test_norm_evidence_and_projection_charge_the_complete_representation(self) -> None:
        # Break caught: the four-byte source norm is omitted from either the
        # authenticated evidence or the 100M object-storage worksheet.
        vectors = np.array([[3.0, 4.0], [0.0, 2.0]], dtype=np.float32)
        evidence = vector_norm_evidence(vectors)
        projection = project_v103_resident_bytes_100m(self.config())

        self.assertEqual(evidence.rows, 2)
        self.assertEqual(evidence.minimum_squared_norm, 4.0)
        self.assertEqual(evidence.maximum_squared_norm, 25.0)
        self.assertEqual(projection.refinement_row_bytes, 52)
        self.assertEqual(projection.refinement_codebook_bytes, 786_432)
        self.assertEqual(projection.refinement_code_plane_bytes, 5_200_000_000)
        self.assertEqual(projection.row_codes_resident_bytes, 0)
        self.assertTrue(projection.resident_eligible)
        self.assertEqual(PQ48X8.row_bytes + 4, projection.refinement_row_bytes)

    def test_evaluation_compares_same_pq48_codes_under_both_score_rules(self) -> None:
        # Break caught: the causal comparison retrains or changes codes, or
        # silently falls back to the PQ16 control instead of changing only the
        # row-score objective over the same retained rows.
        inputs = v98_tests.V98ProducerTests.inputs(
            dimensions=48,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        inputs = dataclasses.replace(
            inputs,
            vectors=np.ascontiguousarray(inputs.vectors + np.float32(0.000001)),
            queries=np.ascontiguousarray(inputs.queries + np.float32(0.000001)),
            training_rows=256,
            training_iterations=1,
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024)
                for key in inputs.pages
            },
        )

        result = evaluate_v103(
            inputs,
            v98_tests.V98ProducerTests.authority(inputs),
            self.config(),
        )

        self.assertEqual(result.query_count, 2)
        self.assertEqual(result.refinement_row_bytes, 52)
        self.assertEqual(len(result.l2_control_samples), 2)
        self.assertEqual(len(result.metric_aware_samples), 2)
        self.assertEqual(result.source_norms_identity.shape, (inputs.vectors.shape[0],))
        body = canonical_v103_result_bytes(result)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        self.assertEqual(json.loads(body)["schema"], "borsuk-v103-metric-aware-pq48-v1")


if __name__ == "__main__":
    unittest.main()
