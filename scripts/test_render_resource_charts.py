import csv
import tempfile
import unittest
from pathlib import Path

from render_resource_charts import downsample, load_rows, render, render_experiment_tree


class ResourceChartTest(unittest.TestCase):
    def write_fixture(self, root: Path) -> Path:
        path = root / "resources.csv"
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "elapsed_ms",
                    "cpu_percent",
                    "rss_bytes",
                    "vms_bytes",
                    "process_read_bytes",
                    "process_write_bytes",
                    "cache_disk_bytes",
                    "scratch_disk_bytes",
                    "network_receive_bytes",
                    "network_transmit_bytes",
                    "child_cpu_seconds",
                    "child_max_rss_bytes",
                ]
            )
            writer.writerow([0, 0, 1024, 2048, 0, 0, 0, 0, 0, 0, "", ""])
            writer.writerow(
                [
                    100,
                    150,
                    2048,
                    4096,
                    512,
                    1024,
                    2048,
                    4096,
                    8192,
                    1024,
                    1.25,
                    2048,
                ]
            )
        return path

    def test_renders_cpu_ram_and_disk_panels(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_fixture(Path(directory))
            svg = render(path, "publication run")
            self.assertIn("CPU utilization", svg)
            self.assertIn("Process memory", svg)
            self.assertIn("Disk and cache footprint", svg)
            self.assertIn("Network I/O", svg)
            self.assertIn("build scratch", svg)
            self.assertIn("publication run", svg)

    def test_downsample_retains_requested_size_and_endpoints(self) -> None:
        rows = [{"elapsed_ms": float(index)} for index in range(100)]
        sampled = downsample(rows, 10)
        self.assertEqual(len(sampled), 10)
        self.assertEqual(sampled[0], rows[0])
        self.assertEqual(sampled[-1], rows[-1])

    def test_load_rows_parses_numeric_csv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_fixture(Path(directory))
            rows = load_rows(path)
            self.assertEqual(rows[1]["cpu_percent"], 150.0)
            self.assertEqual(rows[1]["cache_disk_bytes"], 2048.0)
            self.assertEqual(rows[1]["scratch_disk_bytes"], 4096.0)
            self.assertEqual(rows[0]["child_cpu_seconds"], 0.0)
            self.assertEqual(rows[0]["child_max_rss_bytes"], 0.0)
            self.assertEqual(rows[1]["child_cpu_seconds"], 1.25)

    def test_tree_render_skips_header_only_interrupted_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = root / "valid"
            valid.mkdir()
            self.write_fixture(valid)
            empty = root / "interrupted"
            empty.mkdir()
            (empty / "resources.csv").write_text("elapsed_ms,cpu_percent\n")
            output = root / "charts"

            rendered, skipped = render_experiment_tree(root, output, "resources")

            self.assertEqual((rendered, skipped), (1, 1))
            self.assertEqual(len(list(output.glob("*.svg"))), 1)


if __name__ == "__main__":
    unittest.main()
