"""Small exhaustive optimum checks for the linear priced-cover fast path."""

from __future__ import annotations

import itertools
import unittest

from scripts.unconstrained_priced_interval import unconstrained_priced_cover


def brute(weights: dict[int, int], mandatory: tuple[int, ...], count: int,
          unit_price: int, get_price: int) -> tuple[int, int, int, int]:
    best = None
    for mask in range(1 << count):
        covered = {unit for unit in range(count) if mask & (1 << unit)}
        if not set(mandatory).issubset(covered):
            continue
        units = len(covered)
        gets = sum(unit in covered and unit - 1 not in covered
                   for unit in range(count))
        mass = sum(weight for unit, weight in weights.items() if unit in covered)
        key = (mass - unit_price * units - get_price * gets,
               -units, -gets, mass)
        if best is None or key > best:
            best = key
    assert best is not None
    return best


class UnconstrainedPricedIntervalTests(unittest.TestCase):
    def test_exhaustive_small_geometry(self) -> None:
        for count in range(1, 7):
            for sparse in itertools.product((0, 2, 5), repeat=count):
                weights = {unit: weight for unit, weight in enumerate(sparse)
                           if weight > 0}
                for mandatory in ((), (0,), (count - 1,),
                                  tuple(range(0, count, 2))):
                    mandatory = tuple(sorted(set(mandatory)))
                    for unit_price, get_price in ((0, 0), (1, 3), (4, 2)):
                        actual = unconstrained_priced_cover(
                            weights, mandatory, page_count=count,
                            unit_price=unit_price, get_price=get_price)
                        self.assertEqual(
                            (actual.objective, -actual.units, -actual.gets,
                             actual.mass),
                            brute(weights, mandatory, count,
                                  unit_price, get_price))
                        self.assertEqual(actual.units, sum(
                            end - start + 1 for start, end in actual.intervals))
                        self.assertEqual(actual.gets, len(actual.intervals))


if __name__ == "__main__":
    unittest.main()
