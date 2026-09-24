"""Exact cost frontier against exhaustive whole-unit intervals."""

import random
import unittest

from scripts.sparse_cover_frontier import min_units_frontier, reconstruct_min_cover
from scripts.priced_interval_oracle import priced_cover
from scripts.source_cover_frontier import CoverFrontier


def brute_at_most(masses: dict[int, int], mandatory: tuple[int, ...],
                  page_count: int, max_gets: int) -> dict[tuple[int, int], int]:
    answer = {}
    for bits in range(1 << page_count):
        covered = {page for page in range(page_count) if bits & (1 << page)}
        if not set(mandatory).issubset(covered):
            continue
        gets = sum(page in covered and page - 1 not in covered
                   for page in range(page_count))
        if gets > max_gets:
            continue
        hits = sum(value for page, value in masses.items() if page in covered)
        units = len(covered)
        for cap in range(gets, max_gets + 1):
            key = (cap, hits)
            answer[key] = min(units, answer.get(key, page_count + 1))
    return answer


class SparseCoverFrontierTests(unittest.TestCase):
    def test_mandatory_only_cost_matches_independent_gap_floor(self):
        mandatory = (0, 2, 7, 11)
        for gets in range(1, 5):
            frontier = min_units_frontier({}, mandatory,
                                          page_count=12, max_gets=gets)
            floor = CoverFrontier(12, gets, 12,
                                  mandatory).minimum_units
            self.assertEqual(int(frontier[gets, 0]), floor)

    def test_frontier_matches_independent_priced_oracle(self):
        masses = {0: 2, 4: 1, 8: 3, 11: 1}
        mandatory = (0, 6)
        frontier = min_units_frontier(masses, mandatory,
                                      page_count=12, max_gets=3)
        for unit_price, get_price in ((0, 0), (1, 5), (8, 2), (30, 30)):
            expected = priced_cover(masses, mandatory,
                                    page_count=12, max_gets=3,
                                    hit_scale=100, unit_price=unit_price,
                                    get_price=get_price).objective
            actual = max(
                hits * 100 - unit_price * int(frontier[gets, hits])
                - get_price * gets
                for gets in range(1, 4)
                for hits in range(sum(masses.values()) + 1)
                if frontier[gets, hits] < 12**2)
            self.assertEqual(actual, expected)

    def test_random_small_frontiers_match_brute_force(self):
        rng = random.Random(184)
        for _ in range(100):
            page_count = rng.randrange(2, 9)
            masses = {page: rng.randrange(1, 4)
                      for page in range(page_count) if rng.randrange(2)}
            mandatory = tuple(sorted(rng.sample(
                range(page_count), rng.randrange(1, min(4, page_count) + 1))))
            gets = rng.randrange(1, min(4, page_count) + 1)
            actual = min_units_frontier(masses, mandatory,
                                        page_count=page_count, max_gets=gets)
            expected = brute_at_most(masses, mandatory, page_count, gets)
            for cap in range(1, gets + 1):
                for hits in range(sum(masses.values()) + 1):
                    value = int(actual[cap, hits])
                    self.assertEqual(value if value <= page_count else None,
                                     expected.get((cap, hits)))
                    if value <= page_count:
                        witness = reconstruct_min_cover(
                            masses, mandatory, page_count=page_count,
                            max_gets=cap, target_hits=hits)
                        self.assertEqual(sum(end - start + 1
                                             for start, end in witness), value)


if __name__ == "__main__":
    unittest.main()
