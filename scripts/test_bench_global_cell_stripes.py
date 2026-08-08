import json
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "bench_global_cell_stripes.sh"
LAUNCHER = ROOT / "scripts" / "launch_aws_global_cell_stripes.sh"
MANIFEST = ROOT / "docs" / "research" / "global-cell-stripe-qualification.json"


class GlobalCellStripeRunnerTest(unittest.TestCase):
    def test_runner_is_fail_closed_and_preserves_paired_methodology(self):
        source = RUNNER.read_text()
        manifest = json.loads(MANIFEST.read_text())
        self.assertIn("GLOBAL_CELL_STRIPE_QUALIFICATION_FAILED", source)
        self.assertIn("GLOBAL_CELL_STRIPE_QUALIFICATION_COMPLETE", source)
        self.assertIn("BORSUK_GROUP_COMMIT_PROTOCOL=read-qualification", source)
        self.assertIn("BORSUK_GROUP_COMMIT_PREFETCH_STRIPE_BYTES", source)
        self.assertIn("BORSUK_GROUP_COMMIT_READ_REPETITION", source)
        self.assertIn("BORSUK_GROUP_COMMIT_READ_ORDER_POSITION", source)
        self.assertIn("BORSUK_STORAGE_TRACE", source)
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("validate_global_cell_stripes.py", source)
        self.assertIn("prefix_is_empty", source)
        self.assertEqual(manifest["repetitions"], 5)
        self.assertEqual(manifest["stripe_bytes"], [1048576, 2097152, 4194304])
        self.assertEqual(
            manifest["arm_orders"],
            [
                [1048576, 2097152, 4194304],
                [2097152, 4194304, 1048576],
                [4194304, 1048576, 2097152],
                [1048576, 2097152, 4194304],
                [2097152, 4194304, 1048576],
            ],
        )

    def test_launcher_requires_exclusive_causality_worker_and_retains_session(self):
        source = LAUNCHER.read_text()
        self.assertIn('PROFILE="${AWS_PROFILE:-causality}"', source)
        self.assertIn("c7g.8xlarge", source)
        self.assertIn("another non-shell tmux workload is active", source)
        self.assertIn("pgrep -af", source)
        self.assertIn("tmux new-session -d", source)
        self.assertIn("remain-on-exit", source)
        self.assertIn("BORSUK_RUN_GLOBAL_CELL_STRIPES=1", source)
        self.assertNotIn("--force", source)


if __name__ == "__main__":
    unittest.main()
