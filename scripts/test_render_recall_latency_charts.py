import csv
import tempfile
import unittest
from pathlib import Path

from render_recall_latency_charts import load_series, render


class RecallLatencyChartTest(unittest.TestCase):
    def write_fixture(self, root: Path) -> Path:
        path = root / "recall.csv"
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "dataset",
                    "nprobe",
                    "max_candidates",
                    "recall_at_10",
                    "mean_ms",
                    "stddev_ms",
                    "p95_ms",
                ]
            )
            writer.writerow(["alpha", 4, 16, 0.95, 8.0, 2.0, 12.5])
            writer.writerow(["alpha", 8, 16, 0.98, 14.0, 3.0, 20.0])
            writer.writerow(["beta", 2, 32, 0.96, 3.0, 1.0, 5.0])
        return path

    def test_loads_one_ordered_series_per_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            series = load_series(self.write_fixture(Path(directory)))
            self.assertEqual(list(series), ["alpha", "beta"])
            self.assertEqual([row["nprobe"] for row in series["alpha"]], [4.0, 8.0])

    def test_renders_recall_latency_axes_and_probe_labels(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            series = load_series(self.write_fixture(Path(directory)))
            svg = render("alpha", series["alpha"])
            self.assertIn("recall@10", svg)
            self.assertIn("latency (ms)", svg)
            self.assertIn("mean ±1 sample SD", svg)
            self.assertIn('class="std-whisker"', svg)
            self.assertIn("nprobe=4", svg)

    def test_renders_graph_and_pq_as_distinct_publication_series(self) -> None:
        rows = [
            {
                "method": "pq-scan",
                "nprobe": 8.0,
                "max_candidates": 16.0,
                "recall_at_10": 0.95,
                "p95_ms": 20.0,
                "label": "pq selected",
            },
            {
                "method": "graph",
                "nprobe": 8.0,
                "max_candidates": 16.0,
                "recall_at_10": 0.95,
                "p95_ms": 10.0,
                "label": "graph selected",
            },
        ]

        svg = render("alpha", rows)

        self.assertEqual(svg.count("<polyline"), 2)
        self.assertIn(">pq-scan</text>", svg)
        self.assertIn(">graph</text>", svg)

    def test_loads_global_candidate_sweep_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "global.csv"
            self._write_global_fixture(path)

            rows = load_series(path)["gist-960"]

            self.assertEqual(rows[0]["max_candidates"], 256.0)
            self.assertEqual(rows[0]["p95_ms"], 220.5)
            self.assertEqual(rows[0]["label"], "cand=256")

    def test_cache_phases_become_distinct_publication_series(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phases.csv"
            with path.open("w", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "dataset",
                        "phase",
                        "method",
                        "nprobe",
                        "max_candidates",
                        "recall_at_10",
                        "p95_ms",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "dataset": "alpha",
                        "phase": "uncached",
                        "method": "pq-scan",
                        "nprobe": 8,
                        "max_candidates": 64,
                        "recall_at_10": 0.96,
                        "p95_ms": 80,
                    }
                )
                writer.writerow(
                    {
                        "dataset": "alpha",
                        "phase": "disk_cached",
                        "method": "pq-scan",
                        "nprobe": 8,
                        "max_candidates": 64,
                        "recall_at_10": 0.96,
                        "p95_ms": 8,
                    }
                )

            rows = load_series(path)["alpha"]

            self.assertEqual(
                {row["method"] for row in rows},
                {"pq-scan · uncached", "pq-scan · disk_cached"},
            )

    def test_accepts_production_bench_csv_with_dataset_supplied_by_cli(self) -> None:
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
                        "p95_ms",
                    ]
                )
                writer.writerow(["uncached", "pq-scan", 96, 184, 0.984, 475.9])

            rows = load_series(path, dataset="glove-100")["glove-100"]

            self.assertEqual(rows[0]["method"], "pq-scan · uncached")
            self.assertEqual(rows[0]["nprobe"], 96.0)

    @staticmethod
    def _write_global_fixture(path: Path) -> None:
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "dataset",
                    "subspaces",
                    "candidates",
                    "recall_at_10",
                    "uncached_p95_ms",
                ]
            )
            writer.writerow(["gist-960", 64, 256, 0.875, 220.5])


if __name__ == "__main__":
    unittest.main()
