"""Tests for the V99 ranked-gap range router."""

from __future__ import annotations

import dataclasses
import unittest

from scripts.v97_row_width_screen import PageKey, RoutedPage
from scripts.v99_ranked_gap_range_router import (
    PageRange,
    RangeSelection,
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
            PageKey("base", 1): RoutedPage(
                PageKey("base", 1), 8_388_608, 8_388_608
            ),
            PageKey("base", 2): RoutedPage(
                PageKey("base", 2), 16_777_216, 1
            ),
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
            PageKey("base", ordinal): RoutedPage(
                PageKey("base", ordinal), ordinal, 1
            )
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


if __name__ == "__main__":
    unittest.main()
