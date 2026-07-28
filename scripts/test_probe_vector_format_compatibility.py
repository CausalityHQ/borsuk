#!/usr/bin/env python3
"""Tests for the Arrow/Vortex typed-vector compatibility gate."""

from __future__ import annotations

import unittest

import probe_vector_format_compatibility as probe


class VectorFormatCompatibilityTest(unittest.TestCase):
    def test_cases_cover_every_public_typed_vector_shape(self) -> None:
        cases = probe.compatibility_cases(rows=32, dimensions=64)

        self.assertEqual(
            {case.name for case in cases},
            {"float32", "float16", "bfloat16", "int8", "binary"},
        )
        for case in cases:
            self.assertEqual(case.table.num_rows, 32)
            self.assertEqual(case.table.column_names, ["row_id", "vector"])

    def test_format_names_are_strict(self) -> None:
        self.assertEqual(
            probe.parse_formats("arrow-ipc,vortex-default,vortex-compact"),
            ("arrow-ipc", "vortex-default", "vortex-compact"),
        )
        with self.assertRaisesRegex(ValueError, "unsupported"):
            probe.parse_formats("arrow-ipc,custom")


if __name__ == "__main__":
    unittest.main()
