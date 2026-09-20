"""Tests for the V99 ranked-gap range router."""

from __future__ import annotations

import dataclasses
import json
import unittest

import numpy as np

from scripts.test_v98_hierarchical_row_router import V98ProducerTests
from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import (
    PageRange,
    RangeSample,
    RangeSelection,
    RankedGapConfig,
    canonical_v99_result_bytes,
    evaluate_v99,
    select_ranked_gap_ranges,
)


class V99RangePlannerTests(unittest.TestCase):
    @staticmethod
    def pages() -> dict[PageKey, RoutedPage]:
        pages = {
            PageKey("base", ordinal): RoutedPage(
                PageKey("base", ordinal), offset=ordinal * 4, encoded_bytes=4
            )
            for ordinal in range(5)
        }
        pages.update(
            {
                PageKey("delta", ordinal): RoutedPage(
                    PageKey("delta", ordinal), offset=ordinal * 3, encoded_bytes=3
                )
                for ordinal in range(2)
            }
        )
        return pages

    def test_merges_cheapest_gap_with_literal_tie_order_and_complete_union(
        self,
    ) -> None:
        # Break caught: the planner merges the right-hand tie, omits the gap
        # page, or charges pages rather than physical ranges as GETs.
        pages = self.pages()
        selection = select_ranked_gap_ranges(
            [PageKey("base", 0), PageKey("base", 2), PageKey("base", 4)],
            pages,
            max_gets=2,
            max_bytes=16,
        )
        self.assertEqual(
            selection.ranges,
            (
                PageRange("base", 0, 2, offset=0, bytes=12),
                PageRange("base", 4, 4, offset=16, bytes=4),
            ),
        )
        self.assertEqual(
            selection.pages,
            tuple(PageKey("base", ordinal) for ordinal in (0, 1, 2, 4)),
        )
        self.assertEqual(selection.gets, 2)
        self.assertEqual(selection.bytes, 16)

    def test_never_merges_roles_and_accepts_a_partial_object_tail(self) -> None:
        # Break caught: equal offsets make base and delta appear adjacent, or
        # an odd final page is discarded because it has no successor.
        selection = select_ranked_gap_ranges(
            [PageKey("base", 0), PageKey("delta", 0), PageKey("base", 4)],
            self.pages(),
            max_gets=2,
            max_bytes=23,
        )
        self.assertEqual(
            selection.ranges,
            (
                PageRange("base", 0, 4, offset=0, bytes=20),
                PageRange("delta", 0, 0, offset=0, bytes=3),
            ),
        )
        self.assertEqual(selection.gets, 2)
        self.assertEqual(selection.bytes, 23)

    def test_reverts_only_expensive_candidate_then_accepts_later_cheap_page(
        self,
    ) -> None:
        # Break caught: one over-budget gap terminates ranked consumption and
        # prevents a later adjacent page from extending the accepted range.
        selection = select_ranked_gap_ranges(
            [
                PageKey("base", 0),
                PageKey("base", 4),
                PageKey("base", 1),
                PageKey("base", 1),
            ],
            self.pages(),
            max_gets=1,
            max_bytes=12,
        )
        self.assertEqual(
            selection,
            RangeSelection(
                ranges=(PageRange("base", 0, 1, offset=0, bytes=8),),
                pages=(PageKey("base", 0), PageKey("base", 1)),
                gets=1,
                bytes=8,
            ),
        )

    def test_accepts_exact_caps_and_rejects_unregistered_or_noncontiguous_pages(
        self,
    ) -> None:
        # Break caught: the planner applies caps after selection or silently
        # spans a hole whose bytes cannot be authenticated.
        pages = {
            PageKey("base", 0): RoutedPage(PageKey("base", 0), 0, 8_388_608),
            PageKey("base", 1): RoutedPage(PageKey("base", 1), 8_388_608, 8_388_608),
            PageKey("base", 2): RoutedPage(PageKey("base", 2), 16_777_216, 1),
        }
        selection = select_ranked_gap_ranges(
            [PageKey("base", 0), PageKey("base", 1), PageKey("base", 2)],
            pages,
            max_gets=32,
            max_bytes=16_777_216,
        )
        self.assertEqual(selection.pages, (PageKey("base", 0), PageKey("base", 1)))
        self.assertEqual(selection.bytes, 16_777_216)
        with self.assertRaisesRegex(ValueError, "registered"):
            select_ranked_gap_ranges(
                [PageKey("delta", 9)], pages, max_gets=32, max_bytes=16_777_216
            )
        broken = dict(pages)
        broken[PageKey("base", 1)] = RoutedPage(
            PageKey("base", 1), 8_388_609, 8_388_608
        )
        with self.assertRaisesRegex(ValueError, "contiguous"):
            select_ranked_gap_ranges(
                [PageKey("base", 0), PageKey("base", 2)],
                broken,
                max_gets=1,
                max_bytes=16_777_216,
            )

    def test_enforces_32_ranges_and_rejects_selection_order_overlap_or_union_drift(
        self,
    ) -> None:
        # Break caught: range cardinality exceeds the S3 cap, or mutated
        # evidence can claim a different order/union than its intervals.
        pages = {
            PageKey("base", ordinal): RoutedPage(PageKey("base", ordinal), ordinal, 1)
            for ordinal in range(34)
        }
        selection = select_ranked_gap_ranges(
            [PageKey("base", ordinal) for ordinal in range(34)],
            pages,
            max_gets=32,
            max_bytes=34,
        )
        self.assertEqual(selection.gets, 32)
        self.assertEqual(selection.bytes, 34)
        self.assertEqual(len(selection.pages), 34)
        with self.assertRaisesRegex(ValueError, "range order"):
            dataclasses.replace(selection, ranges=tuple(reversed(selection.ranges)))
        with self.assertRaisesRegex(ValueError, "page union"):
            dataclasses.replace(selection, pages=selection.pages[:-1])
        with self.assertRaisesRegex(ValueError, "range overlap"):
            RangeSelection(
                ranges=(
                    PageRange("base", 0, 2, 0, 3),
                    PageRange("base", 2, 3, 2, 2),
                ),
                pages=tuple(PageKey("base", ordinal) for ordinal in range(4)),
                gets=2,
                bytes=5,
            )


class V99EvaluationTests(unittest.TestCase):
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
    def contiguous(inputs, *, page_bytes: int = 1_024):
        return dataclasses.replace(
            inputs,
            pages={
                key: RoutedPage(key, key.ordinal * page_bytes, page_bytes)
                for key in inputs.pages
            },
        )

    def test_containment_failure_stops_before_exact_and_width_arms(self) -> None:
        # Break caught: a failed hierarchy executes range/PQ work or emits
        # width evidence that can be mistaken for a G1 comparison.
        inputs = V98ProducerTests.inputs(
            dimensions=16,
            base_pages=1_024,
            delta_pages=1,
            query_count=3,
            truth_key=PageKey("delta", 0),
            far_truth=False,
        )
        inputs = self.contiguous(inputs)
        result = evaluate_v99(inputs, V98ProducerTests.authority(inputs), self.config())
        self.assertEqual(result.classification, "hierarchy-containment-rejected")
        self.assertEqual(len(result.containment_samples), 3)
        self.assertEqual(result.exact_samples, ())
        self.assertEqual(result.arms, ())

    def test_exact_range_failure_stops_before_widths_and_accounts_ranges(self) -> None:
        # Break caught: exact failure is hidden by page-count GET accounting,
        # or compressed arms run after the exact range ceiling failed.
        inputs = V98ProducerTests.inputs(
            dimensions=16,
            base_pages=1_024,
            delta_pages=1,
            query_count=1,
            truth_key=PageKey("base", 1_000),
            far_truth=True,
        )
        mib = 1024**2
        inputs = self.contiguous(inputs, page_bytes=mib)
        result = evaluate_v99(inputs, V98ProducerTests.authority(inputs), self.config())
        self.assertEqual(result.classification, "range-exact-ceiling-rejected")
        sample = result.exact_samples[0]
        self.assertIsInstance(sample, RangeSample)
        self.assertEqual(sample.gets, len(sample.selected_ranges))
        self.assertEqual(
            sample.bytes, sum(item.bytes for item in sample.selected_ranges)
        )
        self.assertEqual(
            sample.selected_pages,
            tuple(
                PageKey(item.object_role, ordinal)
                for item in sample.selected_ranges
                for ordinal in range(item.first_page, item.last_page + 1)
            ),
        )
        self.assertLessEqual(sample.gets, 32)
        self.assertLessEqual(sample.bytes, 16 * mib)
        self.assertEqual(sample.hit_ids, ())
        self.assertEqual(result.arms, ())

    def test_passing_ceiling_runs_five_arms_with_one_range_planner(self) -> None:
        # Break caught: a width is skipped/reordered, summary-only uses a
        # different planner, or range evidence is not canonical typed output.
        inputs = V98ProducerTests.inputs(
            dimensions=96,
            base_pages=128,
            delta_pages=9,
            query_count=2,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        inputs = self.contiguous(inputs)
        result = evaluate_v99(inputs, V98ProducerTests.authority(inputs), self.config())
        self.assertEqual(result.classification, "widths-evaluated")
        self.assertEqual(result.exact_samples[0].hit_ids, (1_000,))
        self.assertEqual(len(result.page_directory), len(inputs.pages))
        self.assertEqual(
            (
                result.page_directory[0].object_role,
                result.page_directory[0].ordinal,
                result.page_directory[0].offset,
                result.page_directory[0].bytes,
            ),
            ("base", 0, 0, 1_024),
        )
        self.assertEqual(
            tuple(arm.name for arm in result.arms),
            ("pq16x8", "pq24x8", "pq32x8", "pq32x4", "summary-only-pq16x8"),
        )
        self.assertTrue(all(len(arm.samples) == 2 for arm in result.arms))
        for samples in (result.exact_samples, *(arm.samples for arm in result.arms)):
            for sample in samples:
                self.assertEqual(sample.gets, len(sample.selected_ranges))
                self.assertLessEqual(sample.gets, 32)
                self.assertLessEqual(sample.bytes, 16 * 1024**2)
        body = canonical_v99_result_bytes(result)
        self.assertTrue(body.endswith(b"\n"))
        self.assertNotIn(b" ", body)
        self.assertEqual(
            json.loads(body)["schema"], "borsuk-v99-ranked-gap-range-router-v1"
        )

    def test_full_development_contract_emits_1000_unique_query_ordinals(self) -> None:
        # Break caught: the result claims the full development split while
        # silently emitting a short or duplicate query cohort.
        inputs = V98ProducerTests.inputs(
            dimensions=16,
            base_pages=1_024,
            delta_pages=1,
            query_count=1_000,
            truth_key=PageKey("delta", 0),
            far_truth=False,
        )
        inputs = self.contiguous(inputs)
        result = evaluate_v99(inputs, V98ProducerTests.authority(inputs), self.config())
        self.assertEqual(result.query_count, 1_000)
        self.assertEqual(
            tuple(sample.query_ordinal for sample in result.containment_samples),
            tuple(range(1_000)),
        )

    def test_8192_shortlist_refits_complete_100m_workspace(self) -> None:
        # Break caught: V99 silently keeps V98's 2,048-row workspace or omits
        # the additional bounded heap bytes from every arm projection.
        inputs = V98ProducerTests.inputs(
            dimensions=96,
            base_pages=128,
            delta_pages=9,
            query_count=1,
            truth_key=PageKey("base", 0),
            far_truth=False,
        )
        inputs = self.contiguous(inputs)
        result = evaluate_v99(inputs, V98ProducerTests.authority(inputs), self.config())
        self.assertEqual(result.config.shortlist_rows, 8_192)
        by_name = {arm.name: arm for arm in result.arms}
        self.assertEqual(
            by_name["pq16x8"].projection.shortlist_workspace_bytes,
            8_192 * (np.dtype(np.float32).itemsize + np.dtype(np.int64).itemsize),
        )
        self.assertTrue(by_name["pq16x8"].projection.eligible)
        self.assertFalse(by_name["pq32x8"].projection.eligible)


if __name__ == "__main__":
    unittest.main()
