"""Source-only router and runtime roster parity checks."""

import unittest

import numpy as np

from scripts.validate_v115_router_parity import compare_planes, compare_rosters


class RouterParityTests(unittest.TestCase):
    def test_plane_comparison_names_a_changed_section(self) -> None:
        sections = {
            "summaries": np.zeros((2, 64), np.float32),
            "books": np.zeros((64, 256, 1), np.float32),
            "codes": np.zeros((256, 64), np.uint8),
            "low": np.zeros(64, np.float32),
            "step": np.ones(64, np.float32),
        }
        historical = dict(sections)
        historical["span_step"] = historical.pop("step")
        self.assertEqual(compare_planes(historical, sections), [])
        changed = dict(sections)
        changed["codes"] = sections["codes"].copy()
        changed["codes"][0, 0] = 1
        self.assertEqual(compare_planes(historical, changed), ["codes"])

    def test_roster_comparison_distinguishes_order_and_set(self) -> None:
        expected = [{"query_ordinal": 0, "nominees": [1, 2, 3]}]
        self.assertEqual(compare_rosters(expected, expected), (0, 0))
        self.assertEqual(compare_rosters(
            expected, [{"query_ordinal": 0, "nominees": [2, 1, 3]}]
        ), (1, 0))
        self.assertEqual(compare_rosters(
            expected, [{"query_ordinal": 0, "nominees": [2, 1, 4]}]
        ), (1, 1))


if __name__ == "__main__":
    unittest.main()
