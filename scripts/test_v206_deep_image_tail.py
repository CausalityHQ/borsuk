"""Independent exhaustive checks of the GT-aware interval-bound DP."""

import unittest

import numpy as np

from scripts.check_v206_deep_image_tail import min_pages_by_hits


class OracleBoundTest(unittest.TestCase):
    def test_small_layouts_against_all_page_subsets(self) -> None:
        generator = np.random.default_rng(206)
        for case in range(50):
            length = int(generator.integers(3, 11))
            pages = np.flatnonzero(generator.integers(0, 2, length))
            if not len(pages):
                pages = np.array([0])
            weights = generator.integers(1, 4, len(pages))
            cap = int(generator.integers(1, 4))
            hits = int(weights.sum())
            dynamic = min_pages_by_hits(pages, weights, cap)
            exhaustive = [length + 1] * (hits + 1)
            for bitmap in range(1 << length):
                gets = sum(
                    (bitmap >> page) & 1
                    and (page == 0 or not ((bitmap >> (page - 1)) & 1))
                    for page in range(length)
                )
                if gets > cap:
                    continue
                count = sum(
                    int(weight) for page, weight in zip(pages, weights)
                    if (bitmap >> int(page)) & 1
                )
                exhaustive[count] = min(exhaustive[count], bitmap.bit_count())
            for goal in range(hits + 1):
                with self.subTest(case=case, goal=goal):
                    self.assertEqual(int(min(dynamic[goal:])),
                                     min(exhaustive[goal:]))


if __name__ == "__main__":
    unittest.main()
