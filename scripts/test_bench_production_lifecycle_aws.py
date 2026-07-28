import os
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/bench_production_lifecycle_aws.sh"


class BenchProductionLifecycleAwsTests(unittest.TestCase):
    def test_dry_run_freezes_the_production_lifecycle_contract(self) -> None:
        environment = os.environ.copy()
        environment.update(
            {
                "BORSUK_LIFECYCLE_EXECUTE": "0",
                "BORSUK_LIFECYCLE_RUN_ID": "lifecycle-v16-test",
                "BORSUK_LIFECYCLE_DATASET": "/does/not/exist/sift-128",
            }
        )
        completed = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("dataset=/does/not/exist/sift-128", completed.stdout)
        self.assertIn("queries=100", completed.stdout)
        self.assertIn("nprobes=8", completed.stdout)
        self.assertIn("candidates=320", completed.stdout)
        self.assertIn("segment_table_format=parquet", completed.stdout)
        self.assertIn("wal_table_format=parquet", completed.stdout)

    def test_paid_execution_requires_an_explicit_second_opt_in(self) -> None:
        environment = os.environ.copy()
        environment.update(
            {
                "BORSUK_LIFECYCLE_EXECUTE": "1",
                "BORSUK_RUN_PRODUCTION_LIFECYCLE": "0",
            }
        )
        completed = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("requires BORSUK_RUN_PRODUCTION_LIFECYCLE=1", completed.stderr)

    def test_runner_requires_all_write_and_lifecycle_artifacts(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        for name in (
            "bench_write_costs.csv",
            "bench_write_samples.csv",
            "bench_lifecycle.csv",
            "bench_mutation_queries.csv",
            "bench_mutation_query_samples.csv",
        ):
            self.assertIn(name, source)
        self.assertIn("BORSUK_BENCH_READ_ONLY=0", source)
        self.assertIn("BORSUK_BENCH_SKIP_EXACT_RECALL=1", source)
        self.assertIn("source_sha256=$SOURCE_SHA256", source)
        self.assertIn("runner_sha256=$RUNNER_SHA256", source)
        self.assertIn('sha256_file "$SOURCE_ARCHIVE"', source)
        self.assertIn("PRODUCTION_LIFECYCLE_FAILED", source)
        self.assertIn("BORSUK_LIFECYCLE_RESULT_URI", source)
        self.assertIn("trap finalize EXIT", source)
        self.assertIn("refusing to overwrite", source)


if __name__ == "__main__":
    unittest.main()
