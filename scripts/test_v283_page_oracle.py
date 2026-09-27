"""The diagnostic oracle respects contiguous GETs and its page budget."""

import unittest
import hashlib
import tempfile
from itertools import product
from pathlib import Path

import numpy as np

from scripts.v283_page_oracle import best_coverage, page_oracle


class PageOracleTests(unittest.TestCase):
    def test_exact_interval_and_page_caps(self) -> None:
        hits = [5, 0, 5, 0, 5]
        self.assertEqual(best_coverage(hits, max_gets=1, max_pages=1), 5)
        self.assertEqual(best_coverage(hits, max_gets=2, max_pages=2), 10)
        self.assertEqual(best_coverage(hits, max_gets=1, max_pages=5), 15)

    def test_matches_exhaustive_small_page_plans(self) -> None:
        hits = [1, 4, 0, 3, 2, 5]
        for gets, pages in product(range(1, 4), range(1, 5)):
            exact = max(
                sum(hit for hit, selected in zip(hits, mask) if selected)
                for mask in product((0, 1), repeat=len(hits))
                if sum(mask) <= pages
                and sum(selected and (index == 0 or not mask[index - 1])
                        for index, selected in enumerate(mask)) <= gets
            )
            self.assertEqual(best_coverage(hits, max_gets=gets, max_pages=pages), exact)

    def test_sealed_gt100_roster_runs_the_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = root / "layout.npy"
            truth = root / "truth.u32"
            np.save(layout, np.arange(100_000), allow_pickle=False)
            np.tile(np.arange(100, dtype="<u4"), 1000).tofile(truth)
            result = page_oracle(
                layout, truth,
                layout_sha=hashlib.sha256(layout.read_bytes()).hexdigest(),
                truth_sha=hashlib.sha256(truth.read_bytes()).hexdigest(),
            )
            self.assertEqual(result["mean_fetched_gt_hits_upper_bound"], 100)
            self.assertEqual(result["queries"], 64)


if __name__ == "__main__":
    unittest.main()
