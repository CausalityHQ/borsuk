#!/usr/bin/env python3
"""Unit tests for SIMD datatype cell evidence normalization."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from scripts.normalize_simd_datatype_cell import CellIdentity, normalize_cell


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class NormalizeSimdDatatypeCellTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.identity = CellIdentity(
            architecture="arm",
            instance_type="c7g.8xlarge",
            source_sha256="1" * 64,
            manifest_sha256="2" * 64,
            dataset_identity_sha256="4" * 64,
            build="simd",
            binary_sha256="3" * 64,
            path="dense-float32",
            element_type="float32",
            repetition=1,
            cache_state="mixed-50",
            target_cache_coverage_percent=50,
            client_concurrency=2,
            query_seed=101,
        )
        write_csv(
            self.root / "resources.csv",
            [
                "elapsed_ms",
                "cpu_percent",
                "rss_bytes",
                "child_cpu_seconds",
                "child_max_rss_bytes",
            ],
            [
                {
                    "elapsed_ms": "100",
                    "cpu_percent": "100",
                    "rss_bytes": "2048",
                    "child_cpu_seconds": "",
                    "child_max_rss_bytes": "",
                },
                {
                    "elapsed_ms": "200",
                    "cpu_percent": "0",
                    "rss_bytes": "1024",
                    "child_cpu_seconds": "0.2",
                    "child_max_rss_bytes": "4096",
                },
            ],
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_production_samples_preserve_query_io_and_amortize_process_cost(
        self,
    ) -> None:
        write_csv(
            self.root / "bench_concurrency_samples.csv",
            [
                "workers",
                "sample_index",
                "query_source_index",
                "target_hot_set_member",
                "latency_ms",
                "recall_at_10",
                "bytes_read",
                "disk_cache_reads",
                "backing_reads",
                "disk_cache_bytes_read",
                "backing_bytes_read",
            ],
            [
                {
                    "workers": "2",
                    "sample_index": "0",
                    "query_source_index": "1",
                    "target_hot_set_member": "0",
                    "latency_ms": "1.5",
                    "recall_at_10": "0.9",
                    "bytes_read": "100",
                    "disk_cache_reads": "0",
                    "backing_reads": "1",
                    "disk_cache_bytes_read": "0",
                    "backing_bytes_read": "100",
                },
                {
                    "workers": "2",
                    "sample_index": "1",
                    "query_source_index": "0",
                    "target_hot_set_member": "1",
                    "latency_ms": "0.5",
                    "recall_at_10": "1.0",
                    "bytes_read": "80",
                    "disk_cache_reads": "1",
                    "backing_reads": "0",
                    "disk_cache_bytes_read": "80",
                    "backing_bytes_read": "0",
                },
            ],
        )

        normalize_cell(
            kind="primary-dense",
            directory=self.root,
            identity=self.identity,
            expected_queries=2,
        )

        with (self.root / "queries.csv").open(newline="", encoding="utf-8") as handle:
            queries = list(csv.DictReader(handle))
        self.assertEqual([row["query_id"] for row in queries], ["source-1", "source-0"])
        self.assertEqual(
            [row["observed_cache_coverage_percent"] for row in queries],
            ["0.000000", "100.000000"],
        )
        self.assertEqual({row["cpu_seconds"] for row in queries}, {"0.100000000"})
        self.assertEqual({row["rss_bytes"] for row in queries}, {"4096"})
        self.assertEqual(queries[0]["backing_requests"], "1.000000")
        self.assertEqual(queries[1]["disk_cache_bytes"], "80.000000")

        with (self.root / "summary.csv").open(newline="", encoding="utf-8") as handle:
            summary = list(csv.DictReader(handle))
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["samples"], "2")
        self.assertEqual(summary[0]["qps"], "10.000000")
        self.assertEqual(summary[0]["cpu_seconds_per_query"], "0.100000000")
        self.assertEqual(summary[0]["p90_ms"], "1.500000")

    def test_hybrid_and_late_rows_use_real_query_ids_and_report_counters(
        self,
    ) -> None:
        hybrid = self.root / "hybrid"
        hybrid.mkdir()
        (hybrid / "resources.csv").write_bytes(
            (self.root / "resources.csv").read_bytes()
        )
        write_csv(
            hybrid / "hybrid_queries.csv",
            [
                "query_position",
                "query_id",
                "query_class",
                "latency_ms",
                "recall_at_10",
                "bytes_read",
                "disk_cache_reads",
                "backing_reads",
                "disk_cache_bytes_read",
                "backing_bytes_read",
            ],
            [
                {
                    "query_position": ordinal,
                    "query_id": f"h{ordinal}",
                    "query_class": "target-hot" if ordinal == 0 else "target-outside",
                    "latency_ms": "1",
                    "recall_at_10": "0.5",
                    "bytes_read": "64",
                    "disk_cache_reads": "1",
                    "backing_reads": "0",
                    "disk_cache_bytes_read": "64",
                    "backing_bytes_read": "0",
                }
                for ordinal in range(2)
            ],
        )
        normalize_cell(
            kind="named-sparse",
            directory=hybrid,
            identity=self.identity,
            expected_queries=2,
        )
        with (hybrid / "queries.csv").open(newline="", encoding="utf-8") as handle:
            hybrid_rows = list(csv.DictReader(handle))
        self.assertEqual([row["query_id"] for row in hybrid_rows], ["h0", "h1"])

        late = self.root / "late"
        late.mkdir()
        (late / "resources.csv").write_bytes((self.root / "resources.csv").read_bytes())
        write_csv(
            late / "late_interaction_samples.csv",
            [
                "frontier",
                "sample_index",
                "query_id",
                "latency_ms",
                "recall_at_50",
                "bytes_read",
                "disk_cache_reads",
                "backing_reads",
                "disk_bytes",
                "backing_bytes",
            ],
            [
                {
                    "frontier": "128",
                    "sample_index": ordinal,
                    "query_id": f"l{ordinal}",
                    "latency_ms": "2",
                    "recall_at_50": "0.75",
                    "bytes_read": "96",
                    "disk_cache_reads": "0",
                    "backing_reads": "1",
                    "disk_bytes": "0",
                    "backing_bytes": "96",
                }
                for ordinal in range(2)
            ],
        )
        normalize_cell(
            kind="late-interaction",
            directory=late,
            identity=self.identity,
            expected_queries=2,
            late_frontier=128,
        )
        with (late / "queries.csv").open(newline="", encoding="utf-8") as handle:
            late_rows = list(csv.DictReader(handle))
        self.assertEqual([row["query_id"] for row in late_rows], ["l0", "l1"])
        self.assertEqual({row["backing_requests"] for row in late_rows}, {"1.000000"})


if __name__ == "__main__":
    unittest.main()
