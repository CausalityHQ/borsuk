"""Tests for the V101 two-stage residual PQ screen."""

import dataclasses
import json
import unittest

import numpy as np

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v101_two_stage_residual_pq_screen import (
    TwoStageResidualPqArtifact,
    build_two_stage_residual_pq,
    canonical_v101_result_bytes,
    evaluate_v101,
    project_v101_resident_bytes_100m,
    score_two_stage_retained_rows,
    two_stage_adc_scores,
)


class V101TwoStageResidualPqTests(unittest.TestCase):
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

    def test_two_stage_adc_matches_reconstructed_squared_distance(self) -> None:
        # Break caught: the additive cross term is omitted or given the wrong
        # sign, so the compact score no longer orders reconstructed vectors.
        first_books = np.zeros((8, 256, 1), dtype=np.float32)
        second_books = np.zeros((8, 256, 1), dtype=np.float32)
        for subspace in range(8):
            first_books[subspace, 1, 0] = np.float32(subspace + 1)
            second_books[subspace, 2, 0] = np.float32(0.25 * (subspace + 1))
        first_codes = np.asarray([[1] * 8, [0] * 8], dtype=np.uint8)
        second_codes = np.asarray([[2] * 8, [2] * 8], dtype=np.uint8)
        artifact = TwoStageResidualPqArtifact.from_arrays(
            first_books=first_books,
            second_books=second_books,
            first_codes=first_codes,
            second_codes=second_codes,
        )
        query = np.linspace(0.5, 4.0, 8, dtype=np.float32)

        actual = two_stage_adc_scores(query, artifact)
        reconstructed = np.stack(
            [
                np.concatenate(
                    [
                        first_books[s, first_codes[row, s]]
                        + second_books[s, second_codes[row, s]]
                        for s in range(8)
                    ]
                )
                for row in range(2)
            ]
        )
        expected = np.sum((reconstructed - query[None, :]) ** 2, axis=1)

        np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=1e-6)
        self.assertEqual(artifact.row_bytes, 16)

    def test_projection_includes_second_books_and_cross_terms_under_three_gib(self) -> None:
        # Break caught: V101 appears feasible only because its second codebook
        # or the resident 8x256x256 f32 cross-term tables are omitted.
        projection = project_v101_resident_bytes_100m(
            self.config(), dimensions=768
        )

        self.assertEqual(projection.row_bytes, 16)
        self.assertEqual(projection.second_codebooks_bytes, 8 * 256 * 96 * 4)
        self.assertEqual(projection.cross_terms_bytes, 8 * 256 * 256 * 4)
        self.assertEqual(
            projection.total_bytes,
            projection.pq16_control.total_bytes
            + projection.second_codebooks_bytes
            + projection.cross_terms_bytes,
        )
        self.assertLess(projection.total_bytes, 3 * 1024**3)
        self.assertTrue(projection.eligible)

    def test_ragged_dimensions_are_zero_padded_without_changing_distance(self) -> None:
        # Break caught: V101 is accidentally restricted to 768d/96d, or padded
        # lanes contribute nonzero distance for a generic dimension count.
        first_books = np.zeros((8, 256, 2), dtype=np.float32)
        second_books = np.zeros((8, 256, 2), dtype=np.float32)
        first_books[0, 1] = np.asarray([1.0, 2.0], dtype=np.float32)
        first_codes = np.asarray([[1] + [0] * 7], dtype=np.uint8)
        second_codes = np.zeros((1, 8), dtype=np.uint8)
        artifact = TwoStageResidualPqArtifact.from_arrays(
            first_books=first_books,
            second_books=second_books,
            first_codes=first_codes,
            second_codes=second_codes,
            dimensions=9,
        )

        query = np.arange(9, dtype=np.float32)
        actual = two_stage_adc_scores(query, artifact)
        reconstructed = np.zeros(9, dtype=np.float32)
        reconstructed[:2] = [1.0, 2.0]

        np.testing.assert_allclose(
            actual,
            [np.sum((reconstructed - query) ** 2)],
            rtol=1e-6,
            atol=1e-6,
        )

    def test_builder_is_query_blind_deterministic_and_scores_bounded_top_rows(self) -> None:
        # Break caught: training depends on queries, materializes a corpus-sized
        # residual matrix, or the bounded shortlist differs from full ordering.
        generator = np.random.default_rng(7216)
        vectors = generator.normal(size=(512, 9)).astype(np.float32)
        first = build_two_stage_residual_pq(
            training_vectors=vectors[:384],
            vectors=vectors,
            seed=7216,
            sample_rows=256,
            iterations=1,
            block_rows=64,
        )
        second = build_two_stage_residual_pq(
            training_vectors=vectors[:384],
            vectors=vectors,
            seed=7216,
            sample_rows=256,
            iterations=1,
            block_rows=31,
        )

        np.testing.assert_array_equal(first.first_books, second.first_books)
        np.testing.assert_array_equal(first.second_books, second.second_books)
        np.testing.assert_array_equal(first.first_codes, second.first_codes)
        np.testing.assert_array_equal(first.second_codes, second.second_codes)
        query = generator.normal(size=9).astype(np.float32)
        row_ids = np.arange(10_000, 10_512, dtype=np.int64)
        expected_order = np.lexsort((row_ids, two_stage_adc_scores(query, first)))[:17]

        actual = score_two_stage_retained_rows(
            query,
            row_ids=row_ids,
            row_positions=np.arange(512, dtype=np.int64),
            artifact=first,
            maximum_rows=512,
            shortlist_rows=17,
            block_rows=29,
        )

        self.assertEqual(actual, tuple(int(row_ids[index]) for index in expected_order))

    def test_evaluation_emits_matched_paired_canonical_gate_evidence(self) -> None:
        # Break caught: V101 is compared under another hierarchy/planner, omits
        # raw query evidence or CIs, or can qualify without every release gate.
        inputs = v98_tests.V98ProducerTests.inputs(
            dimensions=16,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        inputs = dataclasses.replace(
            inputs,
            training_rows=256,
            training_iterations=1,
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024)
                for key in inputs.pages
            },
        )

        result = evaluate_v101(
            inputs,
            v98_tests.V98ProducerTests.authority(inputs),
            self.config(),
        )

        self.assertEqual(result.query_count, 2)
        self.assertEqual(result.bootstrap_resamples, 10_000)
        self.assertEqual(len(result.control_samples), 2)
        self.assertEqual(len(result.challenger_samples), 2)
        self.assertEqual(result.paired.name, "two-stage-residual-pq8x8")
        self.assertTrue(result.control_aggregate.quality_gate_passed)
        self.assertTrue(result.challenger_aggregate.resource_gate_passed)
        self.assertTrue(result.projection.eligible)
        body = canonical_v101_result_bytes(result)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        decoded = json.loads(body)
        self.assertEqual(decoded["schema"], "borsuk-v101-two-stage-residual-pq-v1")
        self.assertEqual(decoded["query_count"], 2)


if __name__ == "__main__":
    unittest.main()
