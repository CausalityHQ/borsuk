"""Behavioral tests for bounded code reads and row-score page nomination."""

from __future__ import annotations

import unittest

from scripts.native_geometric_layout_screen import EvaluationLimits
from scripts.native_row_score_nomination import nominate_pages, plan_code_blocks


class RowScoreNominationTests(unittest.TestCase):
    def test_code_plan_fetches_whole_blocks_containing_retained_pages(self) -> None:
        plan = plan_code_blocks(
            retained_pages=(1, 16, 18),
            page_row_counts=(2,) * 20,
            block_pages=16,
            maximum_gets=2,
            maximum_bytes=2_000,
        )
        self.assertEqual(plan.blocks, ((0, 16, 0, 1_536), (16, 20, 1_536, 384)))
        self.assertEqual((plan.gets, plan.bytes), (2, 1_920))

    def test_code_plan_rejects_over_budget_instead_of_dropping_pages(self) -> None:
        with self.assertRaisesRegex(ValueError, "code wave budget"):
            plan_code_blocks(
                retained_pages=(0, 16),
                page_row_counts=(2,) * 20,
                block_pages=16,
                maximum_gets=1,
                maximum_bytes=2_000,
            )

    def test_code_plan_charges_residual_row_width(self) -> None:
        plan = plan_code_blocks(
            retained_pages=(0, 17),
            page_row_counts=(2,) * 20,
            code_row_bytes=72,
            maximum_bytes=2_880,
        )
        self.assertEqual(plan.blocks, ((0, 16, 0, 2_304), (16, 20, 2_304, 576)))
        with self.assertRaisesRegex(ValueError, "code wave budget"):
            plan_code_blocks(
                retained_pages=(0, 17),
                page_row_counts=(2,) * 20,
                code_row_bytes=72,
                maximum_bytes=2_879,
            )

    def test_top100_counts_nominate_page_with_more_near_rows(self) -> None:
        # Page 0 has the nearest individual row, while page 1 owns more of
        # the best 100. A count-first selector must choose page 1.
        # Give page 1 51 sub-threshold rows and page 0 49, then distant rows.
        scores = [float(index) for index in range(49)] + [float(index) + 0.25 for index in range(51)] + [1000.0]
        pages = [0] * 49 + [1] * 51 + [0]
        selected = nominate_pages(
            row_scores=scores,
            row_source_ordinals=tuple(range(101)),
            row_page_ordinals=pages,
            page_byte_sizes=(100, 100),
            limits=EvaluationLimits(maximum_pages=1, maximum_bytes=100),
        )
        self.assertEqual(selected, (1,))

    def test_equal_scores_use_source_ordinal_then_page_ordinal(self) -> None:
        selected = nominate_pages(
            row_scores=(1.0, 1.0),
            row_source_ordinals=(9, 2),
            row_page_ordinals=(0, 1),
            page_byte_sizes=(100, 100),
            limits=EvaluationLimits(maximum_pages=1, maximum_bytes=100),
            top_rows=1,
        )
        self.assertEqual(selected, (1,))

    def test_page_byte_cap_skips_large_page_and_fills_from_next(self) -> None:
        selected = nominate_pages(
            row_scores=(0.0, 1.0, 2.0),
            row_source_ordinals=(0, 1, 2),
            row_page_ordinals=(0, 1, 2),
            page_byte_sizes=(200, 60, 40),
            limits=EvaluationLimits(maximum_pages=2, maximum_bytes=100),
            top_rows=1,
        )
        self.assertEqual(selected, (1, 2))


if __name__ == "__main__":
    unittest.main()
