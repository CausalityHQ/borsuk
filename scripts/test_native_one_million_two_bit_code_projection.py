"""Focused code-wave geometry tests for the page-local 200-byte format."""

from __future__ import annotations

import unittest

from scripts.native_one_million_two_bit_code_projection import (
    code_page_lengths,
    plan_code_wave,
)


class CodeWaveProjectionTest(unittest.TestCase):
    def test_bridged_code_page_consumes_real_bytes(self) -> None:
        lengths = code_page_lengths((1, 1, 1, 1), base_pages=4)
        plan = plan_code_wave((0, 2), lengths, maximum_gets=1, maximum_bytes=600)
        self.assertEqual(plan["target_pages"], [["base", 0], ["base", 2]])
        self.assertEqual(plan["included_pages"], [["base", 0], ["base", 1], ["base", 2]])
        self.assertEqual(plan["ranges"], [["base", 0, 3]])
        self.assertEqual(plan["gets"], 1)
        self.assertEqual(plan["encoded_bytes"], 600)


    def test_code_page_lengths_reject_empty_page(self) -> None:
        with self.assertRaisesRegex(ValueError, "code page rows"):
            code_page_lengths((1, 0, 2), base_pages=2)
