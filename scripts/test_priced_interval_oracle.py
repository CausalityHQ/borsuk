"""Exact small-instance checks for sparse priced interval optimization."""

import random
import unittest

from scripts.priced_interval_oracle import priced_cover


def brute(masses: dict[int, int], mandatory: tuple[int, ...],
          page_count: int, max_gets: int, scale: int,
          unit_price: int, get_price: int) -> int:
    best = None
    for bits in range(1, 1 << page_count):
        covered = {page for page in range(page_count) if bits & (1 << page)}
        if not set(mandatory).issubset(covered):
            continue
        gets = sum(page in covered and page - 1 not in covered
                   for page in range(page_count))
        if gets > max_gets:
            continue
        hits = sum(value for page, value in masses.items() if page in covered)
        score = scale * hits - unit_price * len(covered) - get_price * gets
        best = score if best is None else max(best, score)
    assert best is not None
    return best


class PricedIntervalOracleTests(unittest.TestCase):
    def test_priced_optimum_matches_exhaustive_small_covers(self):
        for page_count in range(1, 7):
            for mandatory in ((0,), (page_count - 1,),
                              tuple(sorted({0, page_count - 1}))):
                for gets in (1, 2, 3):
                    for unit_price, get_price in ((0, 0), (1, 0),
                                                  (2, 3), (7, 5)):
                        masses = {page: (page * 3 + page_count) % 4
                                  for page in range(page_count)}
                        expected = brute(masses, mandatory, page_count,
                                         gets, 10, unit_price, get_price)
                        result = priced_cover(masses, mandatory,
                                              page_count=page_count,
                                              max_gets=gets, hit_scale=10,
                                              unit_price=unit_price,
                                              get_price=get_price)
                        self.assertEqual(result.objective, expected)
                        self.assertLessEqual(result.gets, gets)
                        self.assertTrue(set(mandatory).issubset(
                            {page for start, end in result.intervals
                             for page in range(start, end + 1)}))
                        self.assertEqual(result.units,
                                         sum(end - start + 1
                                             for start, end in result.intervals))

    def test_optional_truth_outside_mandatory_can_be_skipped(self):
        result = priced_cover({0: 0, 5: 1}, (0,), page_count=6,
                              max_gets=1, hit_scale=10,
                              unit_price=3, get_price=0)
        self.assertEqual(result.intervals, ((0, 0),))
        self.assertEqual(result.hits, 0)

    def test_random_sparse_cases_match_brute_force(self):
        rng = random.Random(184)
        for _ in range(120):
            page_count = rng.randrange(2, 9)
            masses = {page: rng.randrange(4) for page in range(page_count)
                      if rng.randrange(2)}
            mandatory = tuple(sorted(rng.sample(
                range(page_count), rng.randrange(1, min(4, page_count) + 1))))
            gets = rng.randrange(1, min(4, page_count) + 1)
            scale = rng.randrange(1, 20)
            unit_price = rng.randrange(0, 10)
            get_price = rng.randrange(0, 15)
            expected = brute(masses, mandatory, page_count, gets,
                             scale, unit_price, get_price)
            result = priced_cover(masses, mandatory,
                                  page_count=page_count, max_gets=gets,
                                  hit_scale=scale, unit_price=unit_price,
                                  get_price=get_price)
            self.assertEqual(result.objective, expected)


if __name__ == "__main__":
    unittest.main()
