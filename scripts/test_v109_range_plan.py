import unittest

from scripts.v109_range_plan import admit_ranked_pages, plan_page_ranges


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

    def test_ranked_admission_merges_cheapest_gap_under_get_cap(self):
        selected = admit_ranked_pages([0, 3, 10], rows=16, page_rows=1,
                                      row_bytes=10, max_gets=2,
                                      max_bytes=60)
        self.assertEqual(selected.nominated_pages, (0, 3, 10))
        self.assertEqual(selected.plan.ranges, ((0, 40), (100, 110)))
        self.assertEqual(selected.plan.bytes, 50)
        self.assertEqual(selected.rejected_pages, ())

    def test_ranked_admission_rejects_page_when_bridge_exceeds_byte_cap(self):
        selected = admit_ranked_pages([0, 3, 10], rows=16, page_rows=1,
                                      row_bytes=10, max_gets=2,
                                      max_bytes=40)
        self.assertEqual(selected.nominated_pages, (0, 3))
        self.assertEqual(selected.rejected_pages, (10,))
        self.assertEqual(selected.plan.gets, 2)
        self.assertEqual(selected.plan.bytes, 20)

    def test_ranked_admission_charges_final_partial_page(self):
        selected = admit_ranked_pages([3, 0], rows=10, page_rows=3,
                                      row_bytes=2, max_gets=1,
                                      max_bytes=20)
        self.assertEqual(selected.plan.ranges, ((0, 20),))
        self.assertEqual(selected.plan.bytes, 20)


if __name__ == "__main__":
    unittest.main()
