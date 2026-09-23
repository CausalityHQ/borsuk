"""Focused geometry checks for mirrored 104/96-byte page waves."""

from __future__ import annotations

import unittest

from scripts.native_progressive_code_wave import page_lengths, plan_mirrored_wave


class ProgressiveCodeWaveTest(unittest.TestCase):
    def test_mirrored_bridge_is_charged_in_both_planes(self) -> None:
        sign, magnitude = page_lengths((1, 1, 1, 1), base_pages=4)
        plan = plan_mirrored_wave((0, 2), sign, magnitude, maximum_gets=1,
                                  maximum_bytes=312)
        self.assertEqual(plan["sign"]["target_pages"], [["base", 0], ["base", 2]])
        self.assertEqual(plan["sign"]["included_pages"],
                         [["base", 0], ["base", 1], ["base", 2]])
        self.assertEqual(plan["sign"]["encoded_bytes"], 312)
        self.assertEqual(plan["magnitude"]["encoded_bytes"], 288)
        self.assertEqual(plan["magnitude"]["ranges"], [["base", 0, 3]])

    def test_second_plane_never_exceeds_first(self) -> None:
        sign, magnitude = page_lengths((4, 1, 7, 3, 2), base_pages=3)
        plan = plan_mirrored_wave((0, 2, 4, 1, 3), sign, magnitude,
                                  maximum_gets=2, maximum_bytes=1600)
        self.assertLessEqual(plan["magnitude"]["encoded_bytes"],
                             plan["sign"]["encoded_bytes"])
        self.assertEqual(plan["magnitude"]["gets"], plan["sign"]["gets"])

    def test_rejects_mismatched_page_geometry(self) -> None:
        sign, magnitude = page_lengths((1, 2), base_pages=2)
        with self.assertRaises(ValueError):
            plan_mirrored_wave((0,), sign, {"base": magnitude["base"][:1]})


if __name__ == "__main__":
    unittest.main()
