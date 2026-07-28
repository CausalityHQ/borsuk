import os
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/bench_100m_code_ranges.sh"


class Bench100mCodeRangesTests(unittest.TestCase):
    def test_dry_run_prints_the_frozen_qualification_grid(self) -> None:
        environment = os.environ.copy()
        environment.update(
            {
                "BORSUK_100M_EXECUTE": "0",
                "BORSUK_100M_RUN_ID": "100m-v16-test",
                "BORSUK_100M_DATASET": "/does/not/exist/100m",
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
        self.assertIn("probes=4,8,12,16,24,32,48,64", completed.stdout)
        self.assertIn("candidates=100,200", completed.stdout)
        self.assertIn("queries=100", completed.stdout)

    def test_paid_execution_requires_second_opt_in(self) -> None:
        environment = os.environ.copy()
        environment.update(
            {
                "BORSUK_100M_EXECUTE": "1",
                "BORSUK_RUN_100M_QUALIFICATION": "0",
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
        self.assertIn("requires BORSUK_RUN_100M_QUALIFICATION=1", completed.stderr)

    def test_runner_is_fresh_prefix_and_completion_fail_closed(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("refusing to reuse non-empty 100M index prefix", source)
        self.assertIn("protocol.txt", source)
        self.assertIn("QUALIFICATION_100M_COMPLETE", source)
        self.assertIn("BORSUK_SEGMENT_TABLE_FORMAT=parquet", source)
        self.assertIn("BORSUK_WAL_TABLE_FORMAT=parquet", source)
        self.assertIn("source_sha256=$SOURCE_SHA256", source)
        self.assertIn("runner_sha256=$RUNNER_SHA256", source)
        self.assertIn('sha256_file "$SOURCE_ARCHIVE"', source)
        self.assertIn("QUALIFICATION_100M_FAILED", source)
        self.assertIn("BORSUK_100M_RESULT_URI", source)
        self.assertIn("trap finalize EXIT", source)


if __name__ == "__main__":
    unittest.main()
