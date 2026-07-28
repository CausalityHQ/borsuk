import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from render_borsuk_table_format_charts import load_summary, render


class BorsukTableFormatChartTest(unittest.TestCase):
    fieldnames = (
        "object",
        "backend",
        "family",
        "format",
        "layout",
        "execution_mode",
        "workload",
        "status",
        "blocker",
        "samples",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "bytes",
        "rows",
    )

    def write_fixture(
        self, root: Path, *, execution_mode: str = "materialized_arrow"
    ) -> Path:
        path = root / "summary.csv"
        rows = []
        for workload, multiplier in (("projection", 1.0), ("full_scan", 4.0)):
            for format_name, layout, size, latency in (
                ("parquet", "source", 1_000_000, 10.0),
                ("vortex", "default", 700_000, 6.0),
                ("vortex", "compact", 500_000, 8.0),
            ):
                rows.append(
                    {
                        "object": "segments/segment-000.parquet",
                        "backend": "s3",
                        "family": "segments",
                        "format": format_name,
                        "layout": layout,
                        "execution_mode": execution_mode,
                        "workload": workload,
                        "status": "complete",
                        "blocker": "",
                        "samples": 30,
                        "mean_ms": latency * multiplier,
                        "stddev_ms": latency * multiplier * 0.1,
                        "p50_ms": latency * multiplier * 0.9,
                        "p95_ms": latency * multiplier * 1.3,
                        "p99_ms": latency * multiplier * 1.6,
                        "bytes": size,
                        "rows": 10_000,
                    }
                )
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_loads_materialized_rows_and_deduplicates_storage_per_object(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = load_summary(self.write_fixture(Path(directory)))

        self.assertEqual(list(data.workloads), ["projection", "full_scan"])
        self.assertEqual(data.storage_bytes["parquet/source"], 1_000_000)
        self.assertEqual(data.storage_bytes["vortex/default"], 700_000)
        self.assertEqual(data.storage_bytes["vortex/compact"], 500_000)
        self.assertEqual(len(data.latencies["projection"]["parquet/source"]), 1)

    def test_rejects_any_non_materialized_arrow_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_fixture(
                Path(directory), execution_mode="compressed_native"
            )
            with self.assertRaisesRegex(ValueError, "materialized_arrow"):
                load_summary(path)

    def test_renders_storage_and_unaggregated_distribution_markers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = load_summary(self.write_fixture(Path(directory)))
            svg = render(data, title="Real segment replay")

        self.assertIn("Real segment replay", svg)
        self.assertIn("Storage footprint", svg)
        self.assertIn("Latency distributions by workload", svg)
        self.assertIn("materialized_arrow only", svg)
        self.assertIn("projection", svg)
        self.assertIn("full_scan", svg)
        self.assertIn("Parquet", svg)
        self.assertIn("Vortex default", svg)
        self.assertIn("Vortex compact", svg)
        self.assertIn('class="mean-std"', svg)
        self.assertIn('class="p50-marker"', svg)
        self.assertIn('class="p95-marker"', svg)
        self.assertIn('class="p99-marker"', svg)
        self.assertIn("one glyph per object summary", svg)

    def test_cli_writes_dependency_free_svg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_fixture(root)
            output = root / "chart.svg"
            result = subprocess.run(
                [
                    sys.executable,
                    str(
                        Path(__file__).resolve().parent
                        / "render_borsuk_table_format_charts.py"
                    ),
                    "--input",
                    str(source),
                    "--output",
                    str(output),
                    "--title",
                    "AWS corrected replay",
                ],
                text=True,
                capture_output=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output.is_file())
            self.assertIn("AWS corrected replay", output.read_text())


if __name__ == "__main__":
    unittest.main()
