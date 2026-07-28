import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("bench_standard_method_matrix.sh")
DATASETS = (
    "fashion-mnist-784",
    "glove-100",
    "sift-128",
    "nytimes-256",
    "gist-960",
    "deep-image-96",
)
METHODS = (
    "pq-scan",
    "srht-pq-scan",
    "fast-turboquant-mse-scan",
    "fast-turboquant-scan",
    "exact",
    "flat-scan",
    "sq-scan",
    "graph",
    "vamana-pq",
)


class StandardMethodMatrixRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.datasets = self.root / "datasets"
        self.output = self.root / "output"
        for dataset in DATASETS:
            directory = self.datasets / dataset
            directory.mkdir(parents=True)
            (directory / "meta.json").write_text('{"dim": 128}')

    def tearDown(self):
        self.temp.cleanup()

    def run_script(self, **extra):
        env = os.environ.copy()
        env.update(
            {
                "DATASETS": str(self.datasets),
                "OUT": str(self.output),
                "BORSUK_S3_BUCKET": "s3://example/research",
            }
        )
        env.update(extra)
        return subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=SCRIPT.parent.parent,
            env=env,
            text=True,
            capture_output=True,
        )

    def test_dry_run_enumerates_every_dataset_and_fresh_method(self):
        completed = self.run_script()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        with (self.output / "coverage.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), len(DATASETS) * len(METHODS))
        self.assertEqual({row["dataset"] for row in rows}, set(DATASETS))
        self.assertEqual({row["method"] for row in rows}, set(METHODS))
        self.assertEqual({row["status"] for row in rows}, {"planned"})

    def test_every_method_has_a_distinct_fresh_object_prefix(self):
        completed = self.run_script()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        with (self.output / "coverage.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        uris = [row["index_uri"] for row in rows]
        self.assertEqual(len(uris), len(set(uris)))
        self.assertTrue(all("standard-method" in uri for uri in uris))
        self.assertNotIn("BORSUK_BENCH_REUSE_INDEX", SCRIPT.read_text())

    def test_requires_explicit_paid_run_flag(self):
        completed = self.run_script(BORSUK_MATRIX_EXECUTE="1")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("BORSUK_RUN_STANDARD_MATRIX=1", completed.stderr)

    def test_resource_sampler_runs_the_benchmark_binary_directly(self):
        source = SCRIPT.read_text()
        sampled_command = source.split(
            "python3 scripts/benchmark_with_resources.py", 1
        )[1]
        self.assertIn("target/release/examples/production_bench", sampled_command)
        self.assertNotIn("-- cargo run", sampled_command)

    def test_paid_runner_selects_graph_capability_only_for_graph_controls(self):
        source = SCRIPT.read_text()
        self.assertIn("graph|vamana-pq) capability='graph-enabled'", source)
        self.assertIn('BORSUK_BENCH_LEAF_CAPABILITY="$capability"', source)
        self.assertIn("AWS_REGION=eu-central-1", source)
        self.assertIn("AWS_DEFAULT_REGION=eu-central-1", source)


if __name__ == "__main__":
    unittest.main()
