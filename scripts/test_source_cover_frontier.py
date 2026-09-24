"""Exact gap floor and GT-blind ranked admission checks."""

import itertools
import random
import unittest

from scripts.source_cover_frontier import CoverFrontier


def brute_floor(pages: tuple[int, ...], page_count: int,
                max_gets: int) -> int:
    best = page_count + 1
    for covered_bits in range(1 << page_count):
        covered = {page for page in range(page_count)
                   if covered_bits & (1 << page)}
        if not set(pages).issubset(covered):
            continue
        runs = sum(page in covered and page - 1 not in covered
                   for page in range(page_count))
        if runs <= max_gets:
            best = min(best, len(covered))
    return best


class CoverFrontierTests(unittest.TestCase):
    def test_gap_floor_matches_small_exhaustive_cover(self):
        for page_count in range(1, 7):
            for size in range(1, page_count + 1):
                for pages in itertools.combinations(range(page_count), size):
                    for gets in range(1, 4):
                        frontier = CoverFrontier(page_count, gets,
                                                 page_count, pages)
                        self.assertEqual(frontier.minimum_units,
                                         brute_floor(pages, page_count, gets))
                        intervals = frontier.intervals()
                        self.assertLessEqual(len(intervals), gets)
                        self.assertEqual(sum(end - start + 1
                                             for start, end in intervals),
                                         frontier.minimum_units)

    def test_admits_ranked_units_only_when_exact_floor_fits(self):
        frontier = CoverFrontier(12, 2, 3, (0, 10))
        accepted = frontier.admit_ranked((1, 9, 5))
        self.assertEqual(accepted, (1,))
        self.assertEqual(frontier.selected, (0, 1, 10))
        self.assertEqual(frontier.intervals(), ((0, 1), (10, 10)))

    def test_incremental_gap_updates_match_brute_force(self):
        rng = random.Random(183)
        for _ in range(80):
            page_count = rng.randrange(2, 8)
            gets = rng.randrange(1, 4)
            cap = rng.randrange(1, page_count + 1)
            mandatory = (rng.randrange(page_count),)
            frontier = CoverFrontier(page_count, gets, cap, mandatory)
            order = list(range(page_count))
            rng.shuffle(order)
            for unit in order:
                before = set(frontier.selected)
                expected = (unit not in before and
                            brute_floor(tuple(sorted(before | {unit})),
                                        page_count, gets) <= cap)
                self.assertEqual(frontier.try_add(unit), expected)
                self.assertEqual(frontier.minimum_units,
                                 brute_floor(frontier.selected,
                                             page_count, gets))
                intervals = frontier.intervals()
                self.assertEqual(sum(end - start + 1
                                     for start, end in intervals),
                                 frontier.minimum_units)

    def test_rejects_insufficient_mandatory_cap(self):
        with self.assertRaises(ValueError):
            CoverFrontier(12, 1, 10, (0, 10))


if __name__ == "__main__":
    unittest.main()
