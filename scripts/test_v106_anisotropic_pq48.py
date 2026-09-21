"""Contracts for the V106 score-aware PQ48 fail-fast spike."""

from __future__ import annotations

import dataclasses
import json
import unittest

import numpy as np

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, PqSpec, RoutedPage, encode_pq
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v106_anisotropic_pq48 import (
    anisotropy_eta,
    canonical_v106_result_bytes,
    encode_anisotropic_pq,
    evaluate_v106,
    rank_anisotropic_rows,
)


class V106AnisotropicPq48Tests(unittest.TestCase):
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

    @staticmethod
    def fixture() -> tuple[np.ndarray, np.ndarray, PqSpec]:
        spec = PqSpec("test-avq", 1, 8, 1)
        vectors = np.zeros((2, 768), dtype=np.float32)
        vectors[:, 0] = 1.0
        books = np.full((1, 256, 768), 10.0, dtype=np.float32)
        books[0, 0] = 0.0
        books[0, 0, :2] = (1.0, 0.3)
        books[0, 1] = 0.0
        books[0, 1, :2] = (0.8, 0.0)
        return vectors, books, spec

    def test_theory_derived_weight_is_frozen_from_threshold_and_dimension(self) -> None:
        # Break caught: the spike silently tunes its anisotropy on burned
        # development queries instead of deriving it from the registered 0.2.
        expected = 767.0 * 0.2**2 / (1.0 - 0.2**2) ** 2

        self.assertAlmostEqual(anisotropy_eta(768, 1.0, 0.2), expected)
        with self.assertRaisesRegex(ValueError, "anisotropic threshold differs"):
            anisotropy_eta(768, 0.2, 0.2)

    def test_noise_shaping_penalizes_parallel_error_and_is_block_invariant(self) -> None:
        # Break caught: V106 remains ordinary reconstruction PQ, or batching
        # changes its coordinate-descent assignment.
        vectors, books, spec = self.fixture()
        self.assertEqual(int(encode_pq(vectors[:1], books, spec)[0, 0]), 1)

        one = encode_anisotropic_pq(
            vectors, books, spec, threshold=0.2, passes=1, block_rows=1
        )
        two = encode_anisotropic_pq(
            vectors, books, spec, threshold=0.2, passes=1, block_rows=2
        )

        np.testing.assert_array_equal(one, np.zeros((2, 1), dtype=np.uint8))
        np.testing.assert_array_equal(one, two)

    def test_dot_product_ranking_uses_stable_row_identity_ties(self) -> None:
        # Break caught: the score-aware codes are ranked with reconstruction
        # L2, or shortlist-boundary ties depend on input order.
        _, books, spec = self.fixture()
        query = np.zeros(768, dtype=np.float32)
        query[0] = 1.0
        row_ids = np.asarray([9, 3, 7], dtype=np.int64)
        codes = np.asarray([[0], [0], [1]], dtype=np.uint8)

        ranked = rank_anisotropic_rows(
            query,
            row_ids,
            codes,
            books,
            spec,
            maximum_rows=3,
            shortlist_rows=3,
            block_rows=1,
        )

        self.assertEqual(ranked, (3, 9, 7))

    def test_evaluation_pairs_score_aware_codes_with_the_frozen_pq48_control(
        self,
    ) -> None:
        # Break caught: V106 changes the hierarchy, I/O budget, width, or
        # control rather than isolating one score-aware encoding objective.
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
            # The frozen ReLAION source rows are unit-normalized.  The shared
            # router fixture deliberately starts at the all-zero vector, which
            # is outside the AVQ threshold authority and would stop before the
            # evaluator contract exercised below.
            vectors=np.ascontiguousarray(inputs.vectors + np.float32(1.0)),
            training_rows=256,
            training_iterations=1,
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024)
                for key in inputs.pages
            },
        )

        result = evaluate_v106(
            inputs,
            v98_tests.V98ProducerTests.authority(inputs),
            self.config(),
        )

        self.assertEqual(result.query_count, 2)
        self.assertEqual(result.threshold, 0.2)
        self.assertEqual(result.coordinate_passes, 1)
        self.assertEqual(result.refinement_row_bytes, 48)
        self.assertEqual(len(result.control_samples), 2)
        self.assertEqual(len(result.challenger_samples), 2)
        self.assertNotEqual(
            result.control_codes_identity.sha256,
            result.challenger_codes_identity.sha256,
        )
        body = canonical_v106_result_bytes(result)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        self.assertEqual(json.loads(body)["schema"], "borsuk-v106-avq-pq48-v1")


if __name__ == "__main__":
    unittest.main()
