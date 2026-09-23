"""Adversarial checks for the source-only final-page necessary bound."""

from __future__ import annotations

import itertools
import random
import unittest

from scripts.native_one_million_page_feasibility import (
    oracle_page_hit_ceiling,
    oracle_page_selection,
)


class OraclePageHitCeilingTest(unittest.TestCase):
    def test_matches_exhaustive_page_choice_on_small_layouts(self) -> None:
        generator = random.Random(17)
        for page_count in range(1, 8):
            page_groups = tuple(index // 2 for index in range(page_count))
            for _ in range(30):
                truth_pages = tuple(generator.randrange(page_count) for _ in range(12))
                selected = tuple(
                    group for group in set(page_groups) if generator.randrange(2)
                )
                for cap in range(1, 4):
                    possible = tuple(
                        page for page, group in enumerate(page_groups) if group in selected
                    )
                    exact = max(
                        (
                            sum(page in choice for page in truth_pages)
                            for width in range(min(cap, len(possible)) + 1)
                            for choice in itertools.combinations(possible, width)
                        ),
                        default=0,
                    )
                    self.assertEqual(
                        oracle_page_hit_ceiling(
                            truth_pages, selected, page_groups, maximum_pages=cap
                        ),
                        exact,
                    )

    def test_rejects_unknown_owner_and_duplicate_group(self) -> None:
        with self.assertRaises(ValueError):
            oracle_page_hit_ceiling((2,), (0,), (0, 0))
        with self.assertRaises(ValueError):
            oracle_page_hit_ceiling((0,), (0, 0), (0,))

    def test_ties_choose_lower_page_and_never_leave_selected_groups(self) -> None:
        self.assertEqual(
            oracle_page_selection((3, 1, 0, 2), (0,), (0, 0, 1, 1), maximum_pages=1),
            (0,),
        )
        self.assertEqual(
            oracle_page_selection((3, 1, 0, 2), (1,), (0, 0, 1, 1), maximum_pages=2),
            (2, 3),
        )


if __name__ == "__main__":
    unittest.main()
