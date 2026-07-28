import csv
import tempfile
import unittest
from pathlib import Path

from render_cache_coverage_charts import aggregate_rows, load_rows, render, slug


class CacheCoverageChartTest(unittest.TestCase):
    def write_fixture(self, root: Path) -> Path:
        path = root / "bench_cache_coverage.csv"
        fieldnames = [
            "target_hot_query_fraction",
            "query_class",
            "latency_ms",
            "decoded_access_fraction",
            "disk_access_fraction",
            "backing_access_fraction",
        ]
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "target_hot_query_fraction": 0.5,
                        "query_class": "hot",
                        "latency_ms": 4,
                        "decoded_access_fraction": 0.75,
                        "disk_access_fraction": 0.25,
                        "backing_access_fraction": 0,
                    },
                    {
                        "target_hot_query_fraction": 0.5,
                        "query_class": "outside_hot_set",
                        "latency_ms": 20,
                        "decoded_access_fraction": 0.25,
                        "disk_access_fraction": 0.25,
                        "backing_access_fraction": 0.5,
                    },
                    {
                        "target_hot_query_fraction": 1,
                        "query_class": "hot",
                        "latency_ms": 3,
                        "decoded_access_fraction": 1,
                        "disk_access_fraction": 0,
                        "backing_access_fraction": 0,
                    },
                ]
            )
        return path

    def test_aggregates_observed_tiers_separately_from_requested_hot_mix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            grouped = aggregate_rows(load_rows(self.write_fixture(Path(directory))))

        fifty = grouped[0]
        self.assertEqual(fifty["target_hot_fraction"], 0.5)
        self.assertAlmostEqual(fifty["decoded_fraction"], 0.5)
        self.assertAlmostEqual(fifty["disk_fraction"], 0.25)
        self.assertAlmostEqual(fifty["backing_fraction"], 0.25)
        self.assertEqual(fifty["hot_p95_ms"], 4)
        self.assertEqual(fifty["outside_p95_ms"], 20)
        self.assertEqual(fifty["all_mean_ms"], 12)
        self.assertAlmostEqual(fifty["all_stddev_ms"], 11.313708499)

    def test_svg_names_requested_mix_observed_residency_and_query_classes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            rows = load_rows(self.write_fixture(Path(directory)))
            svg = render(rows, "alpha / bounded-graph-cache-64m")

        self.assertIn("requested hot-query fraction", svg)
        self.assertIn("observed data-access fraction", svg)
        self.assertIn("decoded RAM", svg)
        self.assertIn("disk cache", svg)
        self.assertIn("backing storage", svg)
        self.assertIn("hot-query p95 + μ±σ", svg)
        self.assertIn("outside-hot-set p95 + μ±σ", svg)
        self.assertIn("mean ±1 sample SD", svg)
        self.assertIn('class="std-whisker"', svg)
        self.assertEqual(slug("srht_pq / 64 MiB"), "srht-pq-64-MiB")


if __name__ == "__main__":
    unittest.main()
