import unittest

from scripts.v109_range_plan import plan_page_ranges


class RangePlanTests(unittest.TestCase):
    def test_coalescing_charges_gap_pages(self):
        plan = plan_page_ranges([0, 2, 8], gap_pages=1, rows=2100,
                                page_rows=256, row_bytes=780)
        self.assertEqual(plan.ranges, ((0, 3 * 256 * 780),
                                       (8 * 256 * 780, 2100 * 780)))
        self.assertEqual(plan.gets, 2)
        self.assertEqual(plan.bytes, (3 * 256 + 2100 - 8 * 256) * 780)

    def test_final_partial_page_is_not_overcharged(self):
        plan = plan_page_ranges([3], gap_pages=0, rows=1000,
                                page_rows=256, row_bytes=780)
        self.assertEqual(plan.ranges, ((3 * 256 * 780, 1000 * 780),))
        self.assertEqual(plan.bytes, 232 * 780)

    def test_rejects_unsorted_duplicate_and_out_of_range_pages(self):
        for pages in ([1, 1], [2, 1], [4], [-1]):
            with self.subTest(pages=pages), self.assertRaises(ValueError):
                plan_page_ranges(pages, gap_pages=0, rows=1000,
                                 page_rows=256, row_bytes=780)

    def test_budget_is_on_full_coalesced_intervals(self):
        plan = plan_page_ranges([0, 2], gap_pages=1, rows=1000,
                                page_rows=256, row_bytes=780)
        self.assertTrue(plan.within(gets=1, bytes_limit=3 * 256 * 780))
        self.assertFalse(plan.within(gets=1, bytes_limit=2 * 256 * 780))


if __name__ == "__main__":
    unittest.main()
