"""Reconstruct exactly the old authenticated code-wave candidate rows."""

from __future__ import annotations

import unittest
from types import SimpleNamespace

from scripts import native_two_bit_returned_replay as replay


def fixture():
    first = (0, 1, 0, 408, "a" * 64)
    second = (1, 2, 408, 408, "b" * 64)
    codes = SimpleNamespace(page_row_counts=(2, 2), group_ranges=(first, second))
    sample = SimpleNamespace(group_ranges=(second, first), code_gets=2, code_bytes=816)
    return sample, codes


class SelectedPositionsTests(unittest.TestCase):
    def test_returns_physical_rows_in_selected_group_order(self) -> None:
        sample, codes = fixture()
        self.assertEqual(replay.selected_positions(sample, codes), (2, 3, 0, 1))

    def test_rejects_repeated_or_corrupt_group(self) -> None:
        sample, codes = fixture()
        sample.group_ranges = (codes.group_ranges[0], codes.group_ranges[0])
        with self.assertRaisesRegex(ValueError, "group"):
            replay.selected_positions(sample, codes)
        sample.group_ranges = ((0, 1, 0, 408, "0" * 64),)
        sample.code_gets = 1
        sample.code_bytes = 408
        with self.assertRaisesRegex(ValueError, "group"):
            replay.selected_positions(sample, codes)

    def test_rejects_budget_or_accounting_drift(self) -> None:
        sample, codes = fixture()
        sample.code_bytes = 817
        with self.assertRaisesRegex(ValueError, "budget"):
            replay.selected_positions(sample, codes)
