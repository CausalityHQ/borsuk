#!/usr/bin/env python3
"""Unit tests for the Parquet/Vortex durable-table benchmark."""

from __future__ import annotations

import unittest

import benchmark_table_formats as benchmark


class TableFormatBenchmarkTest(unittest.TestCase):
    def test_workloads_cover_projection_filters_point_and_scan(self) -> None:
        workloads = benchmark.workload_specs(1_000_000)

        self.assertEqual(
            [workload.name for workload in workloads],
            [
                "narrow_projection",
                "tenant_filter_1pct",
                "row_range_1pct",
                "point_lookup",
                "full_table_scan",
            ],
        )
        self.assertEqual(workloads[0].expected_rows, 1_000_000)
        self.assertEqual(workloads[1].expected_rows, 10_000)
        self.assertEqual(workloads[2].expected_rows, 10_000)
        self.assertEqual(workloads[3].expected_rows, 1)

    def test_workloads_reject_too_few_rows_for_stable_selectivity(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least 100"):
            benchmark.workload_specs(99)

    def test_formats_are_strictly_parquet_and_vortex(self) -> None:
        self.assertEqual(
            benchmark.parse_formats("parquet,vortex-default,vortex-compact"),
            ("parquet", "vortex-default", "vortex-compact"),
        )
        with self.assertRaisesRegex(ValueError, "unsupported"):
            benchmark.parse_formats("arrow-ipc")

    def test_code_type_is_never_silently_changed(self) -> None:
        with self.assertRaisesRegex(ValueError, "variable or fixed"):
            benchmark.create_table(100, 8, 1, "float32")

    def test_s3_backend_requires_an_explicit_bucket_and_prefix(self) -> None:
        with self.assertRaisesRegex(ValueError, "bucket and prefix"):
            benchmark.validate_backend("s3", "", "")
        benchmark.validate_backend("s3", "bench-bucket", "format/run-1")
        benchmark.validate_backend("local_disk", "", "")

    def test_vortex_result_is_materialized_before_the_timed_row_count(self) -> None:
        class CompressedResult:
            def __init__(self) -> None:
                self.materialized = False

            def __len__(self) -> int:
                raise AssertionError(
                    "compressed result length is not completed decode work"
                )

            def to_arrow_table(self):
                self.materialized = True
                return type("ArrowTable", (), {"num_rows": 17})()

        result = CompressedResult()

        rows = benchmark.materialized_vortex_row_count(result)

        self.assertTrue(result.materialized)
        self.assertEqual(rows, 17)


if __name__ == "__main__":
    unittest.main()
