import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

import bench_graph_promotion as promotion


class GraphPromotionRunnerTest(unittest.TestCase):
    def test_plan_covers_public_and_controlled_datasets(self):
        rows = promotion.plan_matrix(bucket="s3://bucket", repetitions=3)
        public = [row for row in rows if row.dataset_kind == "public"]
        controlled = [row for row in rows if row.dataset_kind == "controlled"]

        self.assertEqual(len(public), 6 * 4)
        self.assertEqual(
            {row.dataset for row in controlled},
            {
                "sklearn-digits",
                "synthetic-uniform",
                "synthetic-clustered",
                "synthetic-adversarial",
            },
        )
        self.assertTrue(all(row.repetitions == 3 for row in rows))

    def test_graph_requires_graph_enabled_index(self):
        for row in promotion.plan_matrix("s3://bucket", 3):
            if row.method == "graph":
                self.assertEqual(row.index_capability, "graph-enabled")

    def test_public_plan_has_graph_free_and_graph_enabled_pq_controls(self):
        rows = [
            row
            for row in promotion.plan_matrix("s3://bucket", 3)
            if row.dataset == "glove-100"
        ]
        self.assertEqual(
            {(row.method, row.index_capability) for row in rows},
            {
                ("pq-scan", "pq-scan-only"),
                ("flat-scan", "pq-scan-only"),
                ("pq-scan", "graph-enabled"),
                ("graph", "graph-enabled"),
            },
        )

    def test_execution_rejects_placeholder_bucket(self):
        with self.assertRaisesRegex(ValueError, "real s3 bucket"):
            promotion.validate_execution("s3://dry-run", execute=True)
        promotion.validate_execution("s3://real-bucket", execute=True)
        promotion.validate_execution("s3://dry-run", execute=False)

    def test_select_recall_matched_prefers_lowest_p95_then_less_work(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bench_recall_latency.csv"
            with path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "mode",
                        "nprobe",
                        "max_candidates",
                        "recall_at_10",
                        "p50_ms",
                        "p95_ms",
                        "p99_ms",
                    ]
                )
                writer.writerows(
                    [
                        ["graph", 16, 512, 0.949, 5, 6, 7],
                        ["graph", 32, 1024, 0.951, 8, 10, 11],
                        ["graph", 24, 2048, 0.952, 7, 9, 10],
                        ["graph", 32, 2048, 0.952, 7, 9, 10],
                    ]
                )

            selected = promotion.select_recall_matched(path, 0.950)

        self.assertEqual(selected.nprobe, 24)
        self.assertEqual(selected.max_candidates, 2048)
        self.assertEqual(selected.recall_at_10, 0.952)
        self.assertTrue(selected.meets_target)

    def test_select_recall_matched_uses_explicit_disk_cached_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bench_recall_latency.csv"
            with path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "phase",
                        "mode",
                        "nprobe",
                        "max_candidates",
                        "recall_at_10",
                        "p50_ms",
                        "p95_ms",
                        "p99_ms",
                    ]
                )
                writer.writerows(
                    [
                        ["uncached", "pq-scan", 8, 64, 0.96, 50, 80, 90],
                        ["disk_cached", "pq-scan", 8, 64, 0.96, 5, 8, 9],
                    ]
                )

            selected = promotion.select_recall_matched(path, 0.95)

        self.assertEqual(selected.p95_ms, 8.0)

    def test_select_recall_matched_keeps_best_observed_row_when_target_is_missed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bench_recall_latency.csv"
            with path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "mode",
                        "nprobe",
                        "max_candidates",
                        "recall_at_10",
                        "p50_ms",
                        "p95_ms",
                        "p99_ms",
                    ]
                )
                writer.writerows(
                    [
                        ["graph", 16, 512, 0.940, 5, 6, 7],
                        ["graph", 32, 1024, 0.949, 8, 10, 11],
                    ]
                )

            selected = promotion.select_recall_matched(path, 0.950)

        self.assertEqual(selected.recall_at_10, 0.949)
        self.assertFalse(selected.meets_target)

    def test_graph_selection_excludes_full_cell_scan_disguised_as_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bench_recall_latency.csv"
            with path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "mode",
                        "nprobe",
                        "max_candidates",
                        "recall_at_10",
                        "p50_ms",
                        "p95_ms",
                        "p99_ms",
                    ]
                )
                writer.writerows(
                    [
                        ["graph", 6, 256, 0.986, 1.0, 1.1, 1.2],
                        ["graph", 6, 512, 0.989, 1.3, 1.4, 1.5],
                    ]
                )

            selected = promotion.select_recall_matched(
                path,
                0.989,
                max_candidates_exclusive=512,
            )

        self.assertEqual(selected.max_candidates, 256)
        self.assertEqual(selected.recall_at_10, 0.986)
        self.assertFalse(selected.meets_target)

    def test_non_graph_selection_canonicalizes_candidates_to_cell_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bench_recall_latency.csv"
            with path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "mode",
                        "nprobe",
                        "max_candidates",
                        "recall_at_10",
                        "p50_ms",
                        "p95_ms",
                        "p99_ms",
                    ]
                )
                writer.writerows(
                    [
                        ["flat-scan", 6, 512, 0.989, 1.3, 1.5, 1.6],
                        ["flat-scan", 6, 1024, 0.989, 1.2, 1.4, 1.5],
                    ]
                )

            selected = promotion.select_recall_matched(
                path,
                0.989,
                max_candidates_exclusive=513,
                full_cell_scan_excluded=False,
            )

        self.assertEqual(selected.max_candidates, 512)
        self.assertFalse(selected.full_cell_scan_excluded)

    def test_public_environment_records_exact_capability_and_method(self):
        row = next(
            row
            for row in promotion.plan_matrix("s3://bucket", 3)
            if row.dataset == "fashion-mnist-784" and row.method == "graph"
        )
        env = promotion.public_environment(
            row=row,
            dataset_dir=Path("/datasets/fashion-mnist-784"),
            output_dir=Path("/results/fashion/graph"),
            cache_dir=Path("/cache/fashion/graph"),
        )

        self.assertEqual(env["BORSUK_BENCH_LEAF_CAPABILITY"], "graph-enabled")
        self.assertEqual(env["BORSUK_BENCH_RECALL_LEAF_MODE"], "graph")
        self.assertEqual(env["BORSUK_BENCH_SERVING_LEAF_MODE"], "graph")
        self.assertNotIn("BORSUK_BENCH_REUSE_INDEX", env)
        self.assertIn("/fresh/", env["BORSUK_BENCH_URI"])
        self.assertEqual(env["BORSUK_BENCH_READ_ONLY"], "1")
        self.assertEqual(env["BORSUK_BENCH_QUERIES"], "100")
        self.assertEqual(env["BORSUK_BENCH_RAM_BUDGET_BYTES"], str(8 * 1024**3))
        self.assertEqual(env["BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES"], "0")
        self.assertEqual(env["AWS_REGION"], "eu-central-1")
        self.assertEqual(env["AWS_DEFAULT_REGION"], "eu-central-1")

    def test_production_matrix_disables_decoded_segment_retention(self):
        rows = [
            row
            for row in promotion.plan_matrix("s3://bucket", 3)
            if row.dataset == "glove-100"
        ]
        for row in rows:
            with self.subTest(method=row.method, capability=row.index_capability):
                env = promotion.public_environment(
                    row=row,
                    dataset_dir=Path("/datasets/glove-100"),
                    output_dir=Path("/results/glove"),
                    cache_dir=Path("/cache/glove"),
                )
                self.assertEqual(env["BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES"], "0")

    def test_logged_command_marks_only_successful_runs_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            success = root / "success"
            promotion.run_logged([sys.executable, "-c", "print('ok')"], {}, success)
            self.assertTrue(promotion.is_complete(success))

            failure = root / "failure"
            with self.assertRaisesRegex(RuntimeError, "benchmark failed"):
                promotion.run_logged(
                    [sys.executable, "-c", "raise SystemExit(7)"], {}, failure
                )
            self.assertFalse(promotion.is_complete(failure))

    def test_consolidates_cache_latency_concurrency_and_resource_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = next(
                row
                for row in promotion.plan_matrix("s3://bucket", 1)
                if row.dataset == "fashion-mnist-784" and row.method == "graph"
            )
            row_root = root / "public" / row.dataset / row.index_variant / row.method
            (row_root / "selected.json").parent.mkdir(parents=True)
            (row_root / "selected.json").write_text(
                json.dumps(
                    {
                        "nprobe": 32,
                        "max_candidates": 2560,
                        "recall_at_10": 0.989,
                        "p50_ms": 50,
                        "p95_ms": 60,
                        "p99_ms": 70,
                        "meets_target": True,
                        "source_sha": "abc",
                    }
                )
            )
            run_dir = row_root / "production" / "run-1"
            run_dir.mkdir(parents=True)
            (run_dir / "command.json").write_text(
                json.dumps(
                    {
                        "command": ["production_bench"],
                        "environment": {
                            "BORSUK_BENCH_MAX_ACTIVE_SEARCHES": "4",
                            "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS": "24",
                            "BORSUK_BENCH_RAM_BUDGET_BYTES": str(8 * 1024**3),
                        },
                    }
                )
            )
            self._write_csv(
                run_dir / "bench_cache_states.csv",
                [
                    "phase",
                    "queries",
                    "p50_ms",
                    "p95_ms",
                    "p99_ms",
                    "max_ms",
                    "avg_bytes_read",
                    "avg_object_cache_misses",
                    "avg_network_gets",
                    "dollars_per_million_queries",
                ],
                [
                    ["uncached", 100, 50, 60, 70, 80, 1024, 1, 2, 0.86],
                    ["disk_cached", 100, 10, 12, 14, 15, 4096, 0, 0, 0],
                ],
            )
            self._write_csv(
                run_dir / "bench_concurrency.csv",
                [
                    "workers",
                    "total_queries",
                    "qps",
                    "p50_ms",
                    "p95_ms",
                    "p99_ms",
                    "max_ms",
                    "avg_bytes_read",
                ],
                [[4, 100, 200, 10, 15, 20, 25, 0]],
            )
            self._write_csv(
                run_dir / "resources.csv",
                [
                    "elapsed_ms",
                    "cpu_percent",
                    "rss_bytes",
                    "vms_bytes",
                    "process_read_bytes",
                    "process_write_bytes",
                    "cache_disk_bytes",
                ],
                [
                    [0, 10, 300_000_000, 0, 0, 0, 1000],
                    [1, 120, 450_000_000, 0, 5, 6, 2000],
                ],
            )

            output = root / "consolidated.csv"
            promotion.consolidate_public_results(root, [row], "abc", output)
            with output.open(newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 2)
        cached = next(item for item in rows if item["cache_state"] == "disk_cached")
        self.assertEqual(cached["max_ms"], "15.000")
        self.assertEqual(cached["qps"], "200.000")
        self.assertEqual(cached["peak_rss_bytes"], "450000000")
        self.assertEqual(cached["peak_cpu_percent"], "120.000")
        self.assertEqual(cached["network_gets"], "0.000")
        self.assertEqual(cached["network_bytes"], "0.000")
        self.assertEqual(cached["logical_bytes_read"], "4096.000")
        self.assertEqual(cached["nprobe"], "32")

    @staticmethod
    def _write_csv(path, columns, rows):
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
