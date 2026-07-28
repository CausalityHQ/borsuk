#!/usr/bin/env python3
"""Unit tests for the exact-vector physical-format benchmark."""

from __future__ import annotations

import math
import unittest

import benchmark_vector_formats as benchmark


class VectorFormatBenchmarkTest(unittest.TestCase):
    def test_clustered_queries_are_contiguous_and_deterministic(self) -> None:
        first = benchmark.make_query_indices(
            rows=100,
            selected_rows=8,
            repetitions=4,
            pattern="clustered",
            seed=19,
        )
        second = benchmark.make_query_indices(
            rows=100,
            selected_rows=8,
            repetitions=4,
            pattern="clustered",
            seed=19,
        )

        self.assertEqual(first, second)
        self.assertEqual(len(first), 4)
        for indices in first:
            self.assertEqual(indices, list(range(indices[0], indices[0] + 8)))

    def test_scattered_queries_are_sorted_unique_and_not_contiguous(self) -> None:
        queries = benchmark.make_query_indices(
            rows=10_000,
            selected_rows=64,
            repetitions=3,
            pattern="scattered",
            seed=7,
        )

        for indices in queries:
            self.assertEqual(indices, sorted(set(indices)))
            self.assertEqual(len(indices), 64)
            self.assertGreater(indices[-1] - indices[0], 63)

    def test_sample_summary_uses_sample_standard_deviation(self) -> None:
        summary = benchmark.summarize_samples([1.0, 2.0, 3.0, 4.0])

        self.assertEqual(summary["samples"], 4)
        self.assertEqual(summary["mean_ms"], 2.5)
        self.assertAlmostEqual(summary["stddev_ms"], math.sqrt(5 / 3))
        self.assertEqual(summary["p50_ms"], 2.5)
        self.assertAlmostEqual(summary["p95_ms"], 3.85)
        self.assertAlmostEqual(summary["p99_ms"], 3.97)

    def test_rejects_invalid_query_shapes_before_running_formats(self) -> None:
        with self.assertRaisesRegex(ValueError, "selected_rows"):
            benchmark.make_query_indices(
                rows=10,
                selected_rows=11,
                repetitions=1,
                pattern="clustered",
                seed=1,
            )
        with self.assertRaisesRegex(ValueError, "pattern"):
            benchmark.make_query_indices(
                rows=10,
                selected_rows=1,
                repetitions=1,
                pattern="random",
                seed=1,
            )

    def test_format_names_are_strict_and_include_both_vortex_layouts(self) -> None:
        self.assertEqual(
            benchmark.parse_formats("arrow-ipc,parquet,vortex-default,vortex-compact"),
            ("arrow-ipc", "parquet", "vortex-default", "vortex-compact"),
        )
        with self.assertRaisesRegex(ValueError, "unsupported format"):
            benchmark.parse_formats("arrow-ipc,not-a-format")

    def test_s3_backend_requires_an_explicit_bucket_and_prefix(self) -> None:
        with self.assertRaisesRegex(ValueError, "bucket and prefix"):
            benchmark.validate_backend("s3", "", "")
        benchmark.validate_backend("s3", "bench-bucket", "format/run-1")
        benchmark.validate_backend("local_disk", "", "")

    def test_vortex_result_is_materialized_before_the_timed_row_count(self) -> None:
        class ArrowTable:
            num_rows = 7

        class CompressedResult:
            def __init__(self) -> None:
                self.materialized = False

            def __len__(self) -> int:
                raise AssertionError(
                    "compressed Vortex length is not a decode benchmark"
                )

            def to_arrow_table(self) -> ArrowTable:
                self.materialized = True
                return ArrowTable()

        result = CompressedResult()

        rows = benchmark.materialized_vortex_row_count(result)

        self.assertEqual(rows, 7)
        self.assertTrue(result.materialized)

    def test_arrow_exact_ranges_coalesce_only_adjacent_rows_within_a_batch(
        self,
    ) -> None:
        layouts = (
            benchmark.ArrowBatchLayout(
                row_start=0,
                rows=5,
                vector_offset=100,
                vector_length=40,
            ),
            benchmark.ArrowBatchLayout(
                row_start=5,
                rows=4,
                vector_offset=200,
                vector_length=32,
            ),
        )

        ranges = benchmark.arrow_exact_ranges(
            [1, 2, 4, 5, 7],
            layouts,
            row_bytes=8,
        )

        self.assertEqual(
            ranges,
            (
                benchmark.ArrowReadRange(offset=108, length=16, rows=2),
                benchmark.ArrowReadRange(offset=132, length=8, rows=1),
                benchmark.ArrowReadRange(offset=200, length=8, rows=1),
                benchmark.ArrowReadRange(offset=216, length=8, rows=1),
            ),
        )

    def test_arrow_exact_ranges_reject_rows_outside_the_descriptor(self) -> None:
        layouts = (
            benchmark.ArrowBatchLayout(
                row_start=0,
                rows=2,
                vector_offset=64,
                vector_length=16,
            ),
        )

        with self.assertRaisesRegex(IndexError, "descriptor"):
            benchmark.arrow_exact_ranges([2], layouts, row_bytes=8)

    def test_arrow_physical_ranges_match_borsuk_64kib_coalescing(self) -> None:
        logical = (
            benchmark.ArrowReadRange(offset=100, length=8, rows=1),
            benchmark.ArrowReadRange(offset=60_000, length=16, rows=2),
            benchmark.ArrowReadRange(offset=130_000, length=8, rows=1),
        )

        physical = benchmark.coalesce_arrow_ranges(logical, max_gap_bytes=64 * 1024)

        self.assertEqual(
            physical,
            (
                benchmark.ArrowReadRange(
                    offset=100,
                    length=59_916,
                    rows=3,
                ),
                benchmark.ArrowReadRange(offset=130_000, length=8, rows=1),
            ),
        )

    def test_arrow_coalescing_caps_one_physical_range_without_losing_rows(self) -> None:
        logical = (
            benchmark.ArrowReadRange(offset=0, length=8, rows=1),
            benchmark.ArrowReadRange(offset=100, length=8, rows=1),
            benchmark.ArrowReadRange(offset=200, length=8, rows=1),
        )

        physical = benchmark.coalesce_arrow_ranges(
            logical,
            max_gap_bytes=1024,
            max_range_bytes=150,
        )

        self.assertEqual(
            physical,
            (
                benchmark.ArrowReadRange(offset=0, length=108, rows=2),
                benchmark.ArrowReadRange(offset=200, length=8, rows=1),
            ),
        )

    def test_access_method_makes_the_arrow_and_vortex_paths_explicit(self) -> None:
        self.assertEqual(
            benchmark.access_method("arrow-ipc", False),
            "borsuk-range-64k-gap-10-parallel",
        )
        self.assertEqual(
            benchmark.access_method(
                "arrow-ipc",
                False,
                arrow_max_gap_bytes=1024 * 1024,
                arrow_max_parallel=6,
                arrow_max_range_bytes=8 * 1024 * 1024,
            ),
            "borsuk-range-1024k-gap-8192k-cap-6-parallel",
        )
        self.assertEqual(
            benchmark.access_method("vortex-default", True),
            "native-indices-no-segment-cache",
        )

    def test_arrow_io_options_are_positive_and_gap_is_byte_exact(self) -> None:
        benchmark.validate_arrow_io_options(0, 1, 0)
        benchmark.validate_arrow_io_options(64 * 1024, 10, 8 * 1024 * 1024)
        with self.assertRaisesRegex(ValueError, "gap"):
            benchmark.validate_arrow_io_options(-1, 10, 0)
        with self.assertRaisesRegex(ValueError, "parallel"):
            benchmark.validate_arrow_io_options(0, 0, 0)
        with self.assertRaisesRegex(ValueError, "range"):
            benchmark.validate_arrow_io_options(0, 1, -1)


if __name__ == "__main__":
    unittest.main()
