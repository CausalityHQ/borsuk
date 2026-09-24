import itertools
import unittest

from scripts.v157_primary_source_feasibility import minimum_cover


class MinimumCoverTests(unittest.TestCase):
    def test_matches_exhaustive_contiguous_cover_on_small_geometries(self) -> None:
        rows = 7 * 256 + 13
        width = 108
        for count in range(1, 6):
            for subset in itertools.combinations(range(8), count):
                pages = set(subset)
                for max_gets in range(1, 4):
                    actual, _, gets = minimum_cover(pages, rows, width, max_gets)
                    candidates = []
                    # Enumerate every way to group the selected pages into
                    # at most max_gets contiguous ranges.
                    for mask in range(1 << (count - 1)):
                        groups = []
                        start = subset[0]
                        for index in range(count - 1):
                            if mask & (1 << index):
                                groups.append((start, subset[index] + 1))
                                start = subset[index + 1]
                        groups.append((start, subset[-1] + 1))
                        if len(groups) <= max_gets:
                            candidates.append(sum(
                                (min(stop * 256, rows) - first * 256) * width
                                for first, stop in groups
                            ))
                    self.assertEqual(actual, min(candidates))
                    self.assertLessEqual(gets, max_gets)

    def test_short_final_page_and_gap_bridge(self) -> None:
        self.assertEqual(minimum_cover({0, 1}, 273, 13), (273 * 13, 1, 1))
        pages = set(range(0, 66, 2))
        self.assertEqual(minimum_cover(pages, 100_000, 108),
                         (34 * 256 * 108, 33, 32))


if __name__ == "__main__":
    unittest.main()
