import csv
import tempfile
import unittest
from pathlib import Path

from scripts.production_bench_schema import (
    PHYSICAL_EXACT_LAYOUT_FIELDS,
    QUERY_STAGE_TIMING_FIELDS,
)
from scripts.validate_benchmark_artifacts import validate_directory


def query_stage_values(
    *,
    head_attempts: int = 1,
    head_bytes: int = 40,
    exact_attempts: int = 1,
    exact_bytes: int = 60,
) -> list[int]:
    values = {
        "global_base_approximate_us": 1,
        "global_base_head_admission_us": 0,
        "global_base_head_fetch_us": 2,
        "global_base_head_read_attempts": head_attempts,
        "global_base_head_read_successes": head_attempts,
        "global_base_head_read_response_bytes": head_bytes,
        "global_base_head_read_us_max": 1 if head_attempts else 0,
        "global_base_head_read_us_sum": head_attempts,
        "global_base_head_read_queue_us_max": 1 if head_attempts else 0,
        "global_base_head_read_queue_us_sum": head_attempts,
        "global_base_head_reads_over_20ms": 0,
        "global_base_head_reads_over_30ms": 0,
        "global_base_head_reads_over_50ms": 0,
        "global_base_head_reads_over_100ms": 0,
        "global_base_head_decode_admission_us": 1,
        "global_base_head_decode_us": 0,
        "global_base_exact_admission_us": 10,
        "global_base_exact_fetch_us": 8,
        "global_base_exact_read_attempts": exact_attempts,
        "global_base_exact_read_successes": exact_attempts,
        "global_base_exact_read_response_bytes": exact_bytes,
        "global_base_exact_read_queue_us_max": 1 if exact_attempts else 0,
        "global_base_exact_read_queue_us_sum": exact_attempts,
        "global_base_exact_read_us_max": 1 if exact_attempts else 0,
        "global_base_exact_read_us_sum": exact_attempts,
        "global_base_exact_reads_over_20ms": 0,
        "global_base_exact_reads_over_30ms": 0,
        "global_base_exact_reads_over_50ms": 0,
        "global_base_exact_reads_over_100ms": 0,
        "global_base_exact_cpu_us": 2,
        "global_base_exact_rerank_us": 12,
    }
    return [values[field] for field in QUERY_STAGE_TIMING_FIELDS]


class ValidateBenchmarkArtifactsTests(unittest.TestCase):
    def write_csv(
        self, root: Path, name: str, header: list[str], rows: list[list[object]]
    ) -> None:
        with (root / name).open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)

    def write_valid_query_artifacts(self, root: Path) -> None:
        self.write_csv(
            root,
            "bench_recall_latency.csv",
            [
                "schema_version",
                "scan_codec",
                "cache_execution",
                "phase",
                "mode",
                "nprobe",
                "max_candidates",
                "recall_at_10",
                "samples",
                "mean_ms",
                "stddev_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
            ],
            [
                [
                    "borsuk-production-bench-v20",
                    "srht-pq-scan",
                    "auto",
                    "uncached",
                    "approximate",
                    8,
                    256,
                    0.99,
                    2,
                    11,
                    1.414,
                    10,
                    12,
                    12,
                    12,
                ]
            ],
        )
        self.write_csv(
            root,
            "bench_query_samples.csv",
            [
                "schema_version",
                "scan_codec",
                "cache_execution",
                "phase",
                "mode",
                "nprobe",
                "max_candidates",
                "sample_index",
                "cache_cohort_index",
                "cache_cohort_size",
                "cache_cohort_count",
                "latency_ms",
                "recall_at_10",
                "ram_budget_bytes",
                "collection_resident_bytes",
                "retained_bytes",
                "retained_capacity_bytes",
                "retained_peak_bytes",
                "transient_bytes",
                "transient_capacity_bytes",
                "transient_peak_bytes",
                "global_leaf_directory_reads",
                "global_leaf_directory_bytes",
                "global_leaf_code_pages_read",
                "global_leaf_code_bytes",
                "global_leaf_pages_read",
                "global_leaf_page_bytes",
                "global_leaf_waves",
                "global_leaf_continuations",
                "global_leaf_exact_scores",
                "decoded_cache_bytes_read",
                "backing_reads",
                "backing_bytes_read",
                *PHYSICAL_EXACT_LAYOUT_FIELDS,
                *QUERY_STAGE_TIMING_FIELDS,
            ],
            [
                [
                    "borsuk-production-bench-v20",
                    "srht-pq-scan",
                    "auto",
                    "uncached",
                    "approximate",
                    8,
                    256,
                    0,
                    0,
                    0,
                    0,
                    10,
                    0.98,
                    4096,
                    1024,
                    100,
                    2048,
                    200,
                    50,
                    1024,
                    100,
                    1,
                    16,
                    1,
                    100,
                    1,
                    100,
                    1,
                    0,
                    0,
                    2,
                    1,
                    200,
                    *[1, 1, 1, 1, 1, 1, 100, 0],
                    *query_stage_values(head_bytes=100, exact_bytes=100),
                ],
                [
                    "borsuk-production-bench-v20",
                    "srht-pq-scan",
                    "auto",
                    "uncached",
                    "approximate",
                    8,
                    256,
                    1,
                    0,
                    0,
                    0,
                    12,
                    1.0,
                    4096,
                    1024,
                    120,
                    2048,
                    220,
                    70,
                    1024,
                    120,
                    1,
                    100,
                    1,
                    16,
                    1,
                    100,
                    1,
                    0,
                    2,
                    0,
                    1,
                    200,
                    *[1, 1, 1, 1, 1, 1, 100, 0],
                    *query_stage_values(head_bytes=16, exact_bytes=100),
                ],
            ],
        )

    def test_query_samples_require_exact_current_schema_and_telemetry(self) -> None:
        mutations = (
            ("missing", lambda rows: [row.pop("schema_version") for row in rows]),
            (
                "unknown",
                lambda rows: [
                    row.__setitem__("schema_version", "unknown") for row in rows
                ],
            ),
            (
                "V14",
                lambda rows: [
                    row.__setitem__("schema_version", "borsuk-production-bench-v14")
                    for row in rows
                ],
            ),
            (
                "telemetry",
                lambda rows: [row.pop("global_leaf_exact_scores") for row in rows],
            ),
            (
                "decoded tier",
                lambda rows: [row.pop("decoded_cache_bytes_read") for row in rows],
            ),
            (
                "cache cohort",
                lambda rows: [row.pop("cache_cohort_count") for row in rows],
            ),
            (
                "empty timing",
                lambda rows: [
                    row.__setitem__("global_base_exact_fetch_us", "") for row in rows
                ],
            ),
            (
                "negative timing",
                lambda rows: [
                    row.__setitem__("global_base_exact_fetch_us", "-1") for row in rows
                ],
            ),
            (
                "nonnumeric timing",
                lambda rows: [
                    row.__setitem__("global_base_exact_fetch_us", "abc") for row in rows
                ],
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.write_valid_query_artifacts(root)
                path = root / "bench_query_samples.csv"
                with path.open(newline="") as handle:
                    rows = list(csv.DictReader(handle))
                mutate(rows)
                self.write_csv(
                    root, path.name, list(rows[0]), [list(row.values()) for row in rows]
                )
                with self.assertRaisesRegex(
                    ValueError, "schema|telemetry|required columns"
                ):
                    validate_directory(root, None, ("bench_query_samples.csv",))

    def test_accepts_complete_distribution_artifacts_with_matching_codec(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_csv(
                root,
                "bench_build.csv",
                [
                    "vector_element_type",
                    "scan_codec",
                    "records",
                    "segment_bytes",
                    "vector_sidecar_bytes",
                    "global_scan_bytes",
                    "total_active_index_bytes",
                    "bytes_per_vector",
                    "resident_bytes_estimate",
                    "ram_budget_bytes",
                    "collection_resident_bytes",
                    "retained_bytes",
                    "retained_capacity_bytes",
                    "retained_peak_bytes",
                    "transient_bytes",
                    "transient_capacity_bytes",
                    "transient_peak_bytes",
                    "ingest_ms",
                ],
                [
                    [
                        "float32",
                        "srht-pq-scan",
                        10,
                        100,
                        200,
                        300,
                        600,
                        60,
                        50,
                        4096,
                        50,
                        100,
                        2048,
                        200,
                        50,
                        1024,
                        100,
                        12,
                    ]
                ],
            )
            self.write_valid_query_artifacts(root)
            validate_directory(
                root,
                "srht-pq-scan",
                (
                    "bench_build.csv",
                    "bench_recall_latency.csv",
                    "bench_query_samples.csv",
                ),
            )

    def test_concurrency_requires_current_authority_and_routing_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            summary_header = [
                "schema_version",
                "scan_codec",
                "cache_execution",
                "execution_engine",
                "nprobe",
                "max_candidates",
                "workers",
                "total_queries",
                "qps",
                "mean_ms",
                "stddev_ms",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
            ]
            sample_header = [
                "schema_version",
                "scan_codec",
                "cache_execution",
                "execution_engine",
                "nprobe",
                "max_candidates",
                "workers",
                "sample_index",
                "latency_ms",
                "recall_at_10",
                "ram_budget_bytes",
                "collection_resident_bytes",
                "retained_bytes",
                "retained_capacity_bytes",
                "retained_peak_bytes",
                "transient_bytes",
                "transient_capacity_bytes",
                "transient_peak_bytes",
                *PHYSICAL_EXACT_LAYOUT_FIELDS,
                *QUERY_STAGE_TIMING_FIELDS,
            ]
            summary = [
                "borsuk-production-bench-v20",
                "fast-turboquant-scan",
                "scan",
                "bounded-cell-card-v20",
                64,
                512,
                1,
                1,
                10,
                5,
                0,
                5,
                5,
                5,
                5,
            ]
            sample = [
                "borsuk-production-bench-v20",
                "fast-turboquant-scan",
                "scan",
                "bounded-cell-card-v20",
                64,
                512,
                1,
                0,
                5,
                0.99,
                4096,
                1024,
                100,
                2048,
                200,
                50,
                1024,
                100,
                *([0] * len(PHYSICAL_EXACT_LAYOUT_FIELDS)),
                *query_stage_values(
                    head_attempts=0,
                    head_bytes=0,
                    exact_attempts=0,
                    exact_bytes=0,
                ),
            ]
            self.write_csv(root, "bench_concurrency.csv", summary_header, [summary])
            self.write_csv(
                root, "bench_concurrency_samples.csv", sample_header, [sample]
            )
            required = ("bench_concurrency.csv", "bench_concurrency_samples.csv")
            validate_directory(root, "fast-turboquant-scan", required)

            timing_index = sample_header.index("global_base_exact_read_us_sum")
            for invalid in ("", -1, "abc"):
                with self.subTest(invalid_timing=invalid):
                    corrupted = list(sample)
                    corrupted[timing_index] = invalid
                    self.write_csv(
                        root,
                        "bench_concurrency_samples.csv",
                        sample_header,
                        [corrupted],
                    )
                    with self.assertRaisesRegex(ValueError, "timing telemetry"):
                        validate_directory(root, "fast-turboquant-scan", required)
            self.write_csv(
                root, "bench_concurrency_samples.csv", sample_header, [sample]
            )

            summary[0] = "borsuk-production-bench-v17"
            sample[0] = "borsuk-production-bench-v17"
            self.write_csv(root, "bench_concurrency.csv", summary_header, [summary])
            self.write_csv(
                root, "bench_concurrency_samples.csv", sample_header, [sample]
            )
            with self.assertRaisesRegex(ValueError, "schema"):
                validate_directory(root, "fast-turboquant-scan", required)

            summary[0] = "borsuk-production-bench-v20"
            sample[0] = "borsuk-production-bench-v20"
            sample[4] = 32
            self.write_csv(root, "bench_concurrency.csv", summary_header, [summary])
            self.write_csv(
                root, "bench_concurrency_samples.csv", sample_header, [sample]
            )
            with self.assertRaisesRegex(ValueError, "sample count mismatch"):
                validate_directory(root, "fast-turboquant-scan", required)

            sample[4] = 64
            summary[3] = "mixed"
            sample[3] = "graph"
            self.write_csv(root, "bench_concurrency.csv", summary_header, [summary])
            self.write_csv(
                root, "bench_concurrency_samples.csv", sample_header, [sample]
            )
            validate_directory(root, "fast-turboquant-scan", required)

    def test_rejects_ragged_or_empty_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "bench_build.csv").write_text("scan_codec,value\nsrht-pq-scan\n")
            with self.assertRaisesRegex(ValueError, "columns"):
                validate_directory(root, "srht-pq-scan", ("bench_build.csv",))
            (root / "bench_build.csv").write_text("scan_codec,value\n")
            with self.assertRaisesRegex(ValueError, "no data rows"):
                validate_directory(root, "srht-pq-scan", ("bench_build.csv",))

    def test_rejects_a_codec_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "bench_build.csv").write_text("scan_codec,value\npq-scan,1\n")
            with self.assertRaisesRegex(ValueError, "codec"):
                validate_directory(root, "srht-pq-scan", ("bench_build.csv",))

    def test_rejects_summary_without_required_distribution_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_csv(
                root,
                "bench_recall_latency.csv",
                ["scan_codec", "mean_ms", "p95_ms"],
                [["srht-pq-scan", 10, 12]],
            )
            with self.assertRaisesRegex(
                ValueError, "missing required columns.*stddev_ms"
            ):
                validate_directory(root, "srht-pq-scan", ("bench_recall_latency.csv",))

    def test_rejects_invalid_percentile_order_or_non_finite_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_valid_query_artifacts(root)
            path = root / "bench_recall_latency.csv"
            with path.open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            rows[0]["p95_ms"] = "9"
            self.write_csv(root, path.name, list(rows[0]), [list(rows[0].values())])
            with self.assertRaisesRegex(ValueError, "percentiles are not ordered"):
                validate_directory(
                    root,
                    "srht-pq-scan",
                    ("bench_recall_latency.csv", "bench_query_samples.csv"),
                )

            rows[0]["p50_ms"] = "10"
            rows[0]["p95_ms"] = "12"
            rows[0]["stddev_ms"] = "nan"
            self.write_csv(root, path.name, list(rows[0]), [list(rows[0].values())])
            with self.assertRaisesRegex(ValueError, "finite"):
                validate_directory(
                    root,
                    "srht-pq-scan",
                    ("bench_recall_latency.csv", "bench_query_samples.csv"),
                )

    def test_rejects_summary_when_raw_sample_count_does_not_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_valid_query_artifacts(root)
            path = root / "bench_query_samples.csv"
            with path.open(newline="") as handle:
                rows = list(csv.reader(handle))
            self.write_csv(root, path.name, rows[0], [rows[1]])
            with self.assertRaisesRegex(ValueError, "sample count mismatch"):
                validate_directory(
                    root,
                    "srht-pq-scan",
                    ("bench_recall_latency.csv", "bench_query_samples.csv"),
                )

    def test_rejects_memory_envelope_that_exceeds_capacity_or_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_valid_query_artifacts(root)
            path = root / "bench_query_samples.csv"
            with path.open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            rows[0]["retained_peak_bytes"] = "2049"
            self.write_csv(
                root, path.name, list(rows[0]), [list(row.values()) for row in rows]
            )
            with self.assertRaisesRegex(ValueError, "retained peak exceeds capacity"):
                validate_directory(root, None, ("bench_query_samples.csv",))

            rows[0]["retained_peak_bytes"] = "200"
            rows[0]["collection_resident_bytes"] = "2048"
            rows[0]["retained_capacity_bytes"] = "2048"
            rows[0]["transient_capacity_bytes"] = "1024"
            self.write_csv(
                root, path.name, list(rows[0]), [list(row.values()) for row in rows]
            )
            with self.assertRaisesRegex(
                ValueError, "governed memory exceeds RAM budget"
            ):
                validate_directory(root, None, ("bench_query_samples.csv",))

    def test_rejects_resource_timeline_without_process_and_disk_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_csv(
                root,
                "resources.csv",
                ["elapsed_ms", "cpu_percent", "rss_bytes"],
                [[0, 0, 1024]],
            )
            with self.assertRaisesRegex(
                ValueError, "missing required columns.*cache_disk_bytes"
            ):
                validate_directory(root, None, ("resources.csv",))

    def test_write_batch_distribution_requires_raw_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_csv(
                root,
                "bench_write_costs.csv",
                [
                    "op",
                    "configured_batch_records",
                    "ops",
                    "batches",
                    "wall_ms",
                    "ops_per_s",
                    "mean_batch_ms",
                    "stddev_batch_ms",
                    "p50_batch_ms",
                    "p95_batch_ms",
                    "p99_batch_ms",
                    "max_batch_ms",
                    "mean_amortized_ms",
                    "gets",
                    "puts",
                ],
                [
                    [
                        "upsert",
                        1024,
                        2048,
                        2,
                        20,
                        102400,
                        10,
                        1,
                        9,
                        11,
                        11,
                        11,
                        0.01,
                        2,
                        4,
                    ]
                ],
            )
            self.write_csv(
                root,
                "bench_write_samples.csv",
                [
                    "op",
                    "batch_index",
                    "batch_records",
                    "batch_latency_ms",
                    "amortized_ms",
                    "gets",
                    "puts",
                ],
                [["upsert", 0, 1024, 9, 0.009, 1, 2]],
            )
            with self.assertRaisesRegex(ValueError, "sample count mismatch"):
                validate_directory(
                    root,
                    None,
                    ("bench_write_costs.csv", "bench_write_samples.csv"),
                )

    def test_filter_distribution_requires_publication_columns_and_matching_samples(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            group = ["filter-15m", "mixed_coverage", 50, 4, 0.01]
            self.write_csv(
                root,
                "filter_summary.csv",
                [
                    "dataset",
                    "cache_profile",
                    "target_cache_coverage_percent",
                    "client_concurrency",
                    "selectivity",
                    "samples",
                    "mean_ms",
                    "stddev_ms",
                    "p50_ms",
                    "p95_ms",
                    "p99_ms",
                    "max_ms",
                    "recall_at_10",
                    "fallback_exact_ratio",
                    "avg_segments_searched",
                    "avg_segments_pruned",
                    "avg_rows_evaluated",
                    "avg_rows_passed",
                    "avg_bytes_read",
                    "avg_disk_reads",
                    "avg_backing_reads",
                    "avg_disk_bytes",
                    "avg_backing_bytes",
                    "avg_network_gets",
                ],
                [
                    [
                        *group,
                        2,
                        10,
                        1,
                        9,
                        11,
                        12,
                        12,
                        1,
                        0,
                        2,
                        8,
                        100,
                        10,
                        4096,
                        1,
                        2,
                        1024,
                        3072,
                        2,
                    ]
                ],
            )
            self.write_csv(
                root,
                "filter_samples.csv",
                [
                    "dataset",
                    "cache_profile",
                    "target_cache_coverage_percent",
                    "client_concurrency",
                    "selectivity",
                    "sample_index",
                    "latency_ms",
                    "recall_at_10",
                    "fallback_exact",
                    "leaf_mode",
                    "segments_searched",
                    "segments_pruned",
                    "rows_evaluated",
                    "rows_passed",
                    "bytes_read",
                    "disk_reads",
                    "backing_reads",
                    "disk_bytes",
                    "backing_bytes",
                    "network_gets",
                    "ram_budget_bytes",
                    "collection_resident_bytes",
                    "retained_bytes",
                    "retained_capacity_bytes",
                    "retained_peak_bytes",
                    "transient_bytes",
                    "transient_capacity_bytes",
                    "transient_peak_bytes",
                ],
                [
                    [
                        *group,
                        0,
                        10,
                        1,
                        False,
                        "srht-pq-scan",
                        2,
                        8,
                        100,
                        10,
                        4096,
                        1,
                        2,
                        1024,
                        3072,
                        2,
                        4096,
                        1024,
                        100,
                        2048,
                        200,
                        50,
                        1024,
                        100,
                    ]
                ],
            )
            with self.assertRaisesRegex(ValueError, "sample count mismatch"):
                validate_directory(
                    root,
                    None,
                    ("filter_summary.csv", "filter_samples.csv"),
                )


if __name__ == "__main__":
    unittest.main()
