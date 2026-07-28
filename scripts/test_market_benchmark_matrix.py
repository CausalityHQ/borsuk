#!/usr/bin/env python3
"""Contract tests for the publication benchmark matrix."""

from __future__ import annotations

import csv
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "docs/research/market-benchmark-matrix.csv"


class MarketBenchmarkMatrixTest(unittest.TestCase):
    def test_required_workloads_cache_profiles_and_resources_are_present(self) -> None:
        with MATRIX.open(newline="") as handle:
            rows = list(csv.DictReader(handle))

        datasets = {row["dataset"] for row in rows}
        self.assertTrue(
            {
                "dbpedia-entities-openai-1M",
                "cohere-medium-1M",
                "cohere-large-10M",
                "laion-100M",
                "msmarco-v2-138M",
                "synthetic-filter-15M",
                "synthetic-namespaces-10k-1M",
                "msmarco-passage-colbert",
            }.issubset(datasets)
        )
        workloads = {row["workload"] for row in rows}
        self.assertTrue(
            {"dense_sparse", "dense_bm25", "sparse_bm25"}.issubset(workloads)
        )
        by_dataset = {row["dataset"]: row for row in rows}
        self.assertEqual(by_dataset["cohere-medium-1M"]["dimensions"], "768")
        self.assertEqual(by_dataset["cohere-large-10M"]["dimensions"], "768")
        required_resources = {
            "peak_rss_bytes",
            "mean_cpu_percent",
            "process_read_bytes",
            "process_write_bytes",
            "cache_disk_bytes",
            "scratch_disk_bytes",
            "s3_gets",
            "s3_bytes",
        }
        for row in rows:
            with self.subTest(dataset=row["dataset"]):
                self.assertEqual(
                    set(row["cache_profiles"].split(";")),
                    {"uncached", "disk_cached", "mixed_coverage"},
                )
                self.assertTrue(
                    required_resources.issubset(set(row["resource_metrics"].split(";")))
                )
                self.assertIn("stddev_ms", row["latency_metrics"].split(";"))
                self.assertEqual(row["status"], "planned")


if __name__ == "__main__":
    unittest.main()
