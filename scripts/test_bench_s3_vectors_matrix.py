import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("bench_s3_vectors_matrix.sh")
DATASETS = ("fashion-mnist-784", "glove-100", "deep-image-96")


class S3VectorsMatrixTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.datasets = self.root / "datasets"
        for dataset in DATASETS:
            (self.datasets / dataset).mkdir(parents=True)

    def tearDown(self):
        self.temp.cleanup()

    def run_script(self, **extra):
        output = self.root / "output"
        env = os.environ.copy()
        env.update(
            {
                "DATASETS": str(self.datasets),
                "OUT": str(output),
                "BORSUK_S3V_DATASETS": " ".join(DATASETS),
                "BORSUK_S3V_RUN_ID": "Fresh_Run_20260722",
                "BORSUK_S3V_REPETITIONS": "1",
            }
        )
        env.update(extra)
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=SCRIPT.parent.parent,
            env=env,
            text=True,
            capture_output=True,
        )
        return output, result

    def test_dry_run_uses_distinct_valid_fresh_bucket_names(self) -> None:
        output, result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        with (output / "coverage.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), len(DATASETS))
        buckets = [row["vector_bucket"] for row in rows]
        self.assertEqual(len(buckets), len(set(buckets)))
        self.assertTrue(
            all(len(name) <= 63 and name == name.lower() for name in buckets)
        )
        self.assertEqual({row["status"] for row in rows}, {"planned"})

    def test_paid_execution_is_double_gated(self) -> None:
        _, result = self.run_script(BORSUK_S3V_EXECUTE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BORSUK_RUN_S3V_MATRIX=1", result.stderr)

    def test_runner_samples_client_resources_and_validates_outputs(self) -> None:
        source = SCRIPT.read_text()
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("benchmark_s3_vectors.py", source)
        self.assertIn("validate_benchmark_artifacts.py", source)
        self.assertIn("query_samples.csv", source)

    def test_each_repetition_has_fresh_bucket_output_and_query_seed(self) -> None:
        output, result = self.run_script(
            BORSUK_S3V_DATASETS="fashion-mnist-784",
            BORSUK_S3V_REPETITIONS="2",
            BORSUK_S3V_MASTER_SEED="200",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        with (output / "coverage.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual([row["repetition_id"] for row in rows], ["r01", "r02"])
        self.assertEqual([row["query_seed"] for row in rows], ["201", "202"])
        self.assertEqual(len({row["vector_bucket"] for row in rows}), 2)
        self.assertEqual(len({row["output_dir"] for row in rows}), 2)


if __name__ == "__main__":
    unittest.main()
