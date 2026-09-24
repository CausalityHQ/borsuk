"""Global source-oracle pricing and miss/tail accounting."""

import unittest

from scripts.global_source_oracle import allocate_priced, search_get_prices
from scripts.sparse_cover_frontier import min_units_frontier


class GlobalSourceOracleTests(unittest.TestCase):
    def test_global_price_preserves_hit_and_tail_constraints(self):
        first = min_units_frontier({0: 1, 3: 1, 5: 1}, (0,),
                                   page_count=6, max_gets=2)
        second = min_units_frontier({0: 1, 4: 1, 5: 1}, (0,),
                                    page_count=6, max_gets=2)
        result = allocate_priced((first, second), truth_per_query=3,
                                 max_misses=1, max_tail_queries=0,
                                 tail_min_hits=2, get_price=0)
        self.assertEqual(result.hits, 5)
        self.assertEqual(result.misses, 1)
        self.assertEqual(result.tail_queries, 0)
        self.assertEqual(sum(choice.units for choice in result.choices),
                         result.units)

    def test_pricing_can_trade_units_for_gets(self):
        frontier = min_units_frontier({0: 1, 8: 1}, (0,),
                                      page_count=9, max_gets=2)
        cheap_gets = allocate_priced((frontier,), truth_per_query=2,
                                     max_misses=0, max_tail_queries=0,
                                     tail_min_hits=1, get_price=0)
        costly_gets = allocate_priced((frontier,), truth_per_query=2,
                                      max_misses=0, max_tail_queries=0,
                                      tail_min_hits=1, get_price=10)
        self.assertEqual((cheap_gets.units, cheap_gets.gets), (2, 2))
        self.assertEqual((costly_gets.units, costly_gets.gets), (9, 1))
        self.assertLessEqual(costly_gets.gets, cheap_gets.gets)

    def test_relaxed_value_is_lower_bound_under_get_budget(self):
        first = min_units_frontier({0: 1, 4: 1}, (0,),
                                   page_count=5, max_gets=2)
        second = min_units_frontier({0: 1, 3: 1}, (0,),
                                    page_count=5, max_gets=2)
        price = 3
        relaxed = allocate_priced((first, second), truth_per_query=2,
                                  max_misses=0, max_tail_queries=0,
                                  tail_min_hits=1, get_price=price)
        min_exact = min(
            int(first[g1, 2] + second[g2, 2])
            for g1 in (1, 2) for g2 in (1, 2)
            if g1 + g2 <= 3)
        self.assertLessEqual(relaxed.priced_units - price * 3, min_exact)

    def test_search_distinguishes_witness_and_certified_impossibility(self):
        frontier = min_units_frontier({0: 1, 8: 1}, (0,),
                                      page_count=9, max_gets=2)
        witness = search_get_prices(
            (frontier,), truth_per_query=2, max_misses=0,
            max_tail_queries=0, tail_min_hits=1,
            aggregate_get_budget=1, aggregate_unit_budget=9)
        self.assertEqual(witness.decision, "priced-feasibility-witness")
        self.assertIsNotNone(witness.witness_price)
        impossible = search_get_prices(
            (frontier,), truth_per_query=2, max_misses=0,
            max_tail_queries=0, tail_min_hits=1,
            aggregate_get_budget=1, aggregate_unit_budget=1)
        self.assertEqual(impossible.decision, "certified-layout-infeasible")
        self.assertGreater(impossible.strongest_unit_lower_bound, 1)


if __name__ == "__main__":
    unittest.main()
