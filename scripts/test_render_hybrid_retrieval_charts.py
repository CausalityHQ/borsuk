import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "render_hybrid_retrieval_charts.py"


class RenderHybridRetrievalChartsTests(unittest.TestCase):
    def test_renders_effectiveness_std_and_mixed_cache_charts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            experiment = root / "experiment"
            query_dir = experiment / "scifact" / "srht" / "query" / "dense"
            query_dir.mkdir(parents=True)
            with (experiment / "coverage.csv").open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "stage",
                        "dataset",
                        "profile",
                        "status",
                        "scan_codec",
                        "index_uri",
                        "mode",
                        "candidate_depth",
                        "max_segments",
                        "fusion",
                        "campaign_repetition",
                        "target_hot_query_fraction",
                        "cache_profile",
                        "artifact_dir",
                        "resource_path",
                    ]
                )
                writer.writerow(
                    [
                        "query",
                        "scifact",
                        "srht",
                        "measured",
                        "srht-pq-scan",
                        "s3://bucket/index",
                        "dense",
                        "128",
                        "32",
                        "rrf",
                        "1",
                        "0.5",
                        "mixed-cache-0.5",
                        query_dir,
                        query_dir / "resources.csv",
                    ]
                )
            with (query_dir / "hybrid_summary.csv").open("w", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "dataset",
                        "scan_codec",
                        "k",
                        "candidate_depth",
                        "max_candidates",
                        "max_segments",
                        "fusion",
                        "cache_profile",
                        "target_hot_query_fraction",
                        "mode",
                        "samples",
                        "mean_ms",
                        "stddev_ms",
                        "p50_ms",
                        "p95_ms",
                        "p99_ms",
                        "max_ms",
                        "ndcg_at_10",
                        "recall_at_10",
                        "precision_at_10",
                        "mrr_at_10",
                        "mean_bytes_read",
                        "mean_disk_cache_bytes_read",
                        "mean_backing_bytes_read",
                        "mean_network_gets",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "dataset": "scifact",
                        "scan_codec": "srht-pq-scan",
                        "k": 10,
                        "candidate_depth": 128,
                        "max_candidates": 128,
                        "max_segments": 32,
                        "fusion": "rrf-k60",
                        "cache_profile": "mixed-cache-0.5",
                        "target_hot_query_fraction": 0.5,
                        "mode": "dense",
                        "samples": 4,
                        "mean_ms": 10,
                        "stddev_ms": 2,
                        "p50_ms": 9,
                        "p95_ms": 14,
                        "p99_ms": 15,
                        "max_ms": 15,
                        "ndcg_at_10": 0.72,
                        "recall_at_10": 0.8,
                        "precision_at_10": 0.7,
                        "mrr_at_10": 0.9,
                        "mean_bytes_read": 1000,
                        "mean_disk_cache_bytes_read": 400,
                        "mean_backing_bytes_read": 600,
                        "mean_network_gets": 2,
                    }
                )
            with (query_dir / "hybrid_queries.csv").open("w", newline="") as handle:
                fieldnames = [
                    "target_hot_query_fraction",
                    "query_class",
                    "latency_ms",
                    "decoded_cache_bytes_read",
                    "disk_cache_bytes_read",
                    "backing_bytes_read",
                ]
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "target_hot_query_fraction": 0.5,
                        "query_class": "target-hot",
                        "latency_ms": 5,
                        "decoded_cache_bytes_read": 0,
                        "disk_cache_bytes_read": 1000,
                        "backing_bytes_read": 0,
                    }
                )
                writer.writerow(
                    {
                        "target_hot_query_fraction": 0.5,
                        "query_class": "target-outside",
                        "latency_ms": 20,
                        "decoded_cache_bytes_read": 0,
                        "disk_cache_bytes_read": 100,
                        "backing_bytes_read": 900,
                    }
                )
            output = root / "charts"
            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--experiment-root",
                    str(experiment),
                    "--output-dir",
                    str(output),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            effectiveness = output / "effectiveness-scifact-srht-hot-0.5.svg"
            cache = output / "cache-scifact-srht-dense-c128-p32.svg"
            self.assertTrue(effectiveness.is_file())
            self.assertTrue(cache.is_file())
            self.assertIn("nDCG@10", effectiveness.read_text())
            self.assertIn(
                "independent process/cache repetitions", effectiveness.read_text()
            )
            self.assertIn("disk cache", cache.read_text())


if __name__ == "__main__":
    unittest.main()
