"""Geometry and preregistered width decision for V181."""

import unittest

from scripts.v181_neighborhood_reach import (
    expanded_units, select_qualified_width,
)


class NeighborhoodReachTests(unittest.TestCase):
    def test_width_expansion_is_clipped_and_nested(self):
        base = [1, 9]
        w8 = expanded_units(base, 8, 40)
        w16 = expanded_units(base, 16, 40)
        w32 = expanded_units(base, 32, 40)
        self.assertEqual(w8, tuple(range(18)))
        self.assertTrue(set(w8).issubset(w16))
        self.assertEqual(w32, tuple(range(40)))

    def test_selects_smallest_width_passing_both_gates(self):
        totals = {"8": 12744, "16": 12745, "32": 12750}
        p05 = {"8": 98, "16": 97, "32": 98}
        self.assertEqual(select_qualified_width(totals, p05), 32)
        self.assertIsNone(select_qualified_width(totals,
                                                {"8": 97, "16": 97, "32": 97}))
        with self.assertRaises(ValueError):
            select_qualified_width({"8": 12745}, {"8": 98})


if __name__ == "__main__":
    unittest.main()
