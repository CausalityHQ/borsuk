"""Tests for the V100 page-relative residual range screen."""

import dataclasses
import json
import unittest

import numpy as np

from scripts import test_v98_hierarchical_row_router as v98_tests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v98_hierarchical_row_router import build_hierarchy
from scripts.v99_ranked_gap_range_router import RankedGapConfig
from scripts.v100_page_residual_range_screen import (
    build_page_residual_artifact,
    canonical_v100_result_bytes,
    evaluate_v100,
    project_v100_resident_bytes_100m,
    rank_page_residuals,
    select_page_residual_ranges,
)


class V100PageResidualRangeTests(unittest.TestCase):
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
    def books() -> np.ndarray:
        books = np.zeros((16, 256, 1), dtype=np.float32)
        books[0, 1, 0] = np.float32(1.0)
        books[0, 2, 0] = np.float32(3.0)
        return books

    def test_page_relative_residuals_use_page_mean_and_second_minimum(self) -> None:
        # Break caught: V100 silently ranks the globally encoded row codes or
        # ignores the second-best row when two pages share the same minimum.
        keys = (PageKey("base", 0), PageKey("base", 1), PageKey("base", 2))
        means = np.zeros((3, 16), dtype=np.float32)
        means[1, 0] = np.float32(10.0)
        query = np.zeros(16, dtype=np.float32)
        query[0] = np.float32(10.0)
        codes = np.zeros((3, 2, 16), dtype=np.uint8)
        codes[0, 1, 0] = 1
        codes[1, 1, 0] = 2
        codes[2, 1, 0] = 1

        ranked = rank_page_residuals(
            query,
            page_keys=keys,
            page_means=means,
            page_codes=codes,
            page_row_counts=np.asarray([2, 2, 2], dtype=np.int64),
            books=self.books(),
        )

        self.assertEqual(ranked, (keys[1], keys[0], keys[2]))

    def test_residual_order_uses_the_unchanged_ranked_gap_planner(self) -> None:
        # Break caught: V100 introduces a second physical planner or accounts
        # ranked pages as individual GETs instead of V99 adjacent ranges.
        keys = tuple(PageKey("base", ordinal) for ordinal in range(6))
        pages = {
            key: RoutedPage(key=key, offset=key.ordinal * 100, encoded_bytes=100)
            for key in keys
        }

        selection = select_page_residual_ranges(
            (keys[0], keys[3], keys[1]),
            pages,
            max_gets=2,
            max_bytes=400,
        )

        self.assertEqual(selection.gets, 2)
        self.assertEqual(selection.bytes, 300)
        self.assertEqual(selection.pages, (keys[0], keys[1], keys[3]))

    def test_projection_keeps_dense_page_means_out_of_resident_memory(self) -> None:
        # Break caught: page-relative evidence is made feasible only by keeping
        # all 100M-scale dense page means resident or by omitting query scratch.
        projection = project_v100_resident_bytes_100m(self.config(), dimensions=768)

        self.assertEqual(projection.row_bytes, 16)
        self.assertEqual(projection.decoded_page_means_resident_bytes, 0)
        self.assertEqual(projection.decoded_page_mean_scratch_bytes, 1_024 * 768 * 4)
        self.assertEqual(
            projection.total_bytes,
            projection.v99_pq16.total_bytes
            + projection.decoded_page_mean_scratch_bytes,
        )
        self.assertLess(projection.total_bytes, 3 * 1024**3)
        self.assertTrue(projection.eligible)

    def test_artifact_is_query_blind_and_binds_hierarchy_and_rows(self) -> None:
        # Break caught: V100 training consumes the evaluation queries or its
        # residual codes can be replayed under another hierarchy/layout.
        inputs = v98_tests.V98ProducerTests.inputs(
            dimensions=16,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        hierarchy = build_hierarchy(inputs, self.config())
        first = build_page_residual_artifact(inputs, hierarchy)
        changed_queries = dataclasses.replace(
            inputs, queries=np.full_like(inputs.queries, np.float32(99.0))
        )
        second = build_page_residual_artifact(changed_queries, hierarchy)

        self.assertEqual(first.digest(), second.digest())
        self.assertEqual(first.hierarchy_ipc_sha256, hierarchy.ipc_sha256)
        self.assertEqual(first.row_codes.shape, (len(inputs.source_ids), 16))
        self.assertEqual(first.row_codes.dtype, np.uint8)
        self.assertEqual(first.row_offsets[0], 0)
        self.assertEqual(first.row_offsets[-1], len(inputs.source_ids))

    def test_evaluation_emits_paired_control_and_canonical_gate_evidence(self) -> None:
        # Break caught: the new representation is reported without a matched
        # PQ16 control, 10k paired CI, full query samples, or canonical gates.
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
            pages={
                key: RoutedPage(key, key.ordinal * 1_024, 1_024) for key in inputs.pages
            },
        )

        result = evaluate_v100(
            inputs, v98_tests.V98ProducerTests.authority(inputs), self.config()
        )

        self.assertEqual(result.query_count, 2)
        self.assertEqual(result.bootstrap_resamples, 10_000)
        self.assertEqual(len(result.control_samples), 2)
        self.assertEqual(len(result.residual_samples), 2)
        self.assertEqual(result.paired.name, "page-residual-pq16x8")
        self.assertTrue(result.control_aggregate.quality_gate_passed)
        self.assertTrue(result.residual_aggregate.resource_gate_passed)
        self.assertTrue(result.projection.eligible)
        body = canonical_v100_result_bytes(result)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        decoded = json.loads(body)
        self.assertEqual(decoded["schema"], "borsuk-v100-page-residual-range-v1")
        self.assertEqual(decoded["query_count"], 2)


if __name__ == "__main__":
    unittest.main()
