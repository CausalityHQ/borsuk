#!/usr/bin/env python3
"""Tests for the Parquet/Vortex BORSUK table-type compatibility gate."""

from __future__ import annotations

import unittest

import probe_table_format_compatibility as probe


class TableFormatCompatibilityTest(unittest.TestCase):
    def test_cases_cover_every_borsuk_table_shape_that_can_change_the_decision(
        self,
    ) -> None:
        cases = probe.compatibility_cases(32)

        self.assertEqual(
            {case.name for case in cases},
            {
                "binary",
                "boolean",
                "fixed_list_f32_16",
                "fixed_list_u8_16",
                "fixed_size_binary_64",
                "float16",
                "float32",
                "int64",
                "list_f32",
                "list_u32",
                "uint16",
                "uint32",
                "uint64",
                "uint8",
                "utf8",
            },
        )
        for case in cases:
            self.assertEqual(case.table.num_rows, 32)
            self.assertEqual(case.table.column_names, ["row_id", "value"])

    def test_format_names_are_strict(self) -> None:
        self.assertEqual(
            probe.parse_formats("parquet,vortex-default,vortex-compact"),
            ("parquet", "vortex-default", "vortex-compact"),
        )
        with self.assertRaisesRegex(ValueError, "unsupported"):
            probe.parse_formats("parquet,custom")


if __name__ == "__main__":
    unittest.main()
