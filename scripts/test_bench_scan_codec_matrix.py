import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("bench_scan_codec_matrix.sh")
DATASETS = (
    "fashion-mnist-784",
    "glove-100",
    "sift-128",
    "nytimes-256",
    "gist-960",
    "deep-image-96",
)
PROFILES = (
    "pq-adaptive",
    "pq-32b",
    "pq-64b",
    "srht-adaptive",
    "srht-32b",
    "srht-64b",
    "fast-turboquant-mse-2bit",
    "fast-turboquant-mse-3bit",
    "fast-turboquant-mse-4bit",
    "fast-turboquant-mse-4bit-shards3",
    "fast-turboquant-prod-2bit",
    "fast-turboquant-prod-3bit",
    "fast-turboquant-prod-4bit",
)


class ScanCodecMatrixRunnerTest(unittest.TestCase):
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

    def rows(self):
        with (self.output / "coverage.csv").open() as handle:
            return list(csv.DictReader(handle))

    def test_dry_run_enumerates_six_datasets_and_only_scan_profiles(self):
        completed = self.run_script()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        rows = self.rows()
        self.assertEqual(len(rows), len(DATASETS) * len(PROFILES))
        self.assertEqual({row["dataset"] for row in rows}, set(DATASETS))
        self.assertEqual({row["profile"] for row in rows}, set(PROFILES))
        self.assertEqual({row["status"] for row in rows}, {"planned"})
        self.assertEqual(
            {row["scan_codec"] for row in rows},
            {
                "pq-scan",
                "srht-pq-scan",
                "fast-turboquant-mse-scan",
                "fast-turboquant-scan",
            },
        )
        self.assertEqual(
            {row["leaf_mode"] for row in rows},
            {
                "pq-scan",
                "srht-pq-scan",
                "fast-turboquant-mse-scan",
                "fast-turboquant-scan",
            },
        )
        self.assertEqual({row["measured_codec"] for row in rows}, {"pending"})
        self.assertEqual(
            {row["turboquant_qjl_bits"] for row in rows},
            {"0"},
            "the production residual stage is full-width and not configured as partial QJL",
        )

    def test_each_configuration_has_an_independent_immutable_index_uri(self):
        completed = self.run_script()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        rows = self.rows()
        uris = [row["index_uri"] for row in rows]
        self.assertEqual(len(uris), len(set(uris)))
        self.assertTrue(all("scan-codec" in uri for uri in uris))
        self.assertTrue(all("v9" not in uri and "v10" not in uri for uri in uris))

    def test_matrix_records_resource_and_recall_curve_controls(self):
        completed = self.run_script()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        rows = self.rows()
        self.assertTrue(all(row["cache_execution"] == "scan" for row in rows))
        self.assertTrue(
            all(row["resource_path"].endswith("resources.csv") for row in rows)
        )
        self.assertTrue(all(row["nprobes"] for row in rows))
        self.assertTrue(all(row["candidates"] for row in rows))

    def test_requires_explicit_paid_run_flag(self):
        completed = self.run_script(BORSUK_SCAN_MATRIX_EXECUTE="1")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("BORSUK_RUN_SCAN_MATRIX=1", completed.stderr)

    def test_paid_runner_uses_bounded_production_defaults_and_resource_sampler(self):
        source = SCRIPT.read_text()
        self.assertIn("BORSUK_BENCH_LEAF_CAPABILITY=pq-scan-only", source)
        self.assertIn("BORSUK_BENCH_CACHE_EXECUTION=scan", source)
        self.assertIn("BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4", source)
        self.assertIn("BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24", source)
        self.assertIn("BORSUK_BENCH_QUERIES=${BORSUK_SCAN_QUERIES:-100}", source)
        self.assertIn(
            "BORSUK_BENCH_SKIP_EXACT_RECALL=${BORSUK_SCAN_SKIP_EXACT_RECALL:-1}", source
        )
        self.assertNotIn("BORSUK_BENCH_REUSE_INDEX", source)
        self.assertIn("render_resource_charts.py", source)
        self.assertIn("render_recall_latency_charts.py", source)
        sampled = source.split("python3 scripts/benchmark_with_resources.py", 1)[1]
        self.assertIn("target/release/examples/production_bench", sampled)
        self.assertIn("scan codec mismatch", source)


if __name__ == "__main__":
    unittest.main()
