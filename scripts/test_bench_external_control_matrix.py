import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("bench_external_control_matrix.sh")


class ExternalControlMatrixTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.datasets = self.root / "datasets"
        for name, metric in (("cosine-data", "cosine"), ("l2-data", "euclidean")):
            directory = self.datasets / name
            directory.mkdir(parents=True)
            (directory / "meta.json").write_text(f'{{"metric":"{metric}"}}')

    def tearDown(self):
        self.temp.cleanup()

    def run_script(self, **extra):
        output = self.root / "output"
        env = os.environ.copy()
        env.update(
            {
                "DATASETS": str(self.datasets),
                "OUT": str(output),
                "BORSUK_EXTERNAL_DATASETS": "cosine-data l2-data",
                "BORSUK_EXTERNAL_PROFILES": "dense-tq-mse-4 turbovec-4 faiss-exact",
                "BORSUK_TQ_SEEDS": "17 23",
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

    def test_dry_run_expands_dense_seeds_and_metric_applicability(self) -> None:
        output, result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        with (output / "coverage.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 8)
        skipped = [row for row in rows if row["status"] == "not-applicable"]
        self.assertEqual(
            [(row["dataset"], row["profile"]) for row in skipped],
            [("l2-data", "turbovec-4")],
        )
        dense = [row for row in rows if row["profile"] == "dense-tq-mse-4"]
        self.assertEqual({row["seed"] for row in dense}, {"17", "23"})
        self.assertEqual(
            len({row["output_dir"] for row in rows if row["output_dir"]}), 7
        )

    def test_paid_execution_is_double_gated(self) -> None:
        _, result = self.run_script(BORSUK_EXTERNAL_EXECUTE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BORSUK_RUN_EXTERNAL_MATRIX=1", result.stderr)

    def test_every_control_is_resource_sampled_and_validated(self) -> None:
        source = SCRIPT.read_text()
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("validate_benchmark_artifacts.py", source)
        self.assertIn("benchmark_turboquant_reference.py", source)
        self.assertIn("benchmark_turbovec.py", source)
        self.assertIn("benchmark_faiss.py", source)


if __name__ == "__main__":
    unittest.main()
