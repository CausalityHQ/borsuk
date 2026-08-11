import json
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "bench_global_cell_stripe_confirmation.sh"
COMMON_RUNNER = ROOT / "scripts" / "bench_global_cell_stripes.sh"
LAUNCHER = ROOT / "scripts" / "launch_aws_global_cell_stripe_confirmation.sh"
COMMON_LAUNCHER = ROOT / "scripts" / "launch_aws_global_cell_stripes.sh"
MANIFEST = ROOT / "docs" / "research" / "global-cell-stripe-confirmation.json"


class GlobalCellStripeConfirmationRunnerTest(unittest.TestCase):
    def test_manifest_freezes_the_higher_sample_paired_contract(self):
        manifest = json.loads(MANIFEST.read_text())
        mib = 1024 * 1024
        self.assertEqual(manifest["campaign_id"], "global-cell-stripe-confirmation-v1")
        self.assertEqual(manifest["queries_per_arm"], 500)
        self.assertEqual(manifest["repetitions"], 5)
        self.assertEqual(manifest["stripe_bytes"], [mib, 4 * mib])
        self.assertEqual(
            manifest["arm_orders"],
            [
                [mib, 4 * mib],
                [4 * mib, mib],
                [mib, 4 * mib],
                [4 * mib, mib],
                [mib, 4 * mib],
            ],
        )
        self.assertEqual(manifest["required_nonworse_paired_repetitions"], 4)
        self.assertEqual(manifest["minimum_pooled_p95_improvement_fraction"], 0.10)
        self.assertEqual(manifest["maximum_pooled_p50_regression_fraction"], 0.05)
        self.assertEqual(manifest["max_pooled_p95_ms"], 200.0)
        self.assertEqual(manifest["max_worst_repetition_p95_ms"], 200.0)

    def test_runner_is_fail_closed_and_preserves_confirmation_evidence(self):
        source = RUNNER.read_text() + COMMON_RUNNER.read_text()
        self.assertIn("GLOBAL_CELL_STRIPE_CONFIRMATION_FAILED", source)
        self.assertIn("GLOBAL_CELL_STRIPE_CONFIRMATION_COMPLETE", source)
        self.assertIn(
            'BORSUK_GLOBAL_CELL_STRIPE_PROTOCOL="read-stripe-confirmation"', source
        )
        self.assertIn('BORSUK_GROUP_COMMIT_PROTOCOL="$READ_PROTOCOL"', source)
        self.assertIn("BORSUK_GROUP_COMMIT_PREFETCH_STRIPE_BYTES", source)
        self.assertIn("BORSUK_GROUP_COMMIT_READ_REPETITION", source)
        self.assertIn("BORSUK_GROUP_COMMIT_READ_ORDER_POSITION", source)
        self.assertIn("BORSUK_STORAGE_TRACE", source)
        self.assertIn("benchmark_with_resources.py", source)
        self.assertIn("validate_global_cell_stripes.py", source)
        self.assertIn("prefix_is_empty", source)
        self.assertIn('BORSUK_GROUP_COMMIT_READ_QUERIES="$READ_QUERIES"', source)

    def test_launcher_requires_exclusive_causality_worker_and_retains_session(self):
        source = LAUNCHER.read_text() + COMMON_LAUNCHER.read_text()
        self.assertIn('PROFILE="${AWS_PROFILE:-causality}"', source)
        self.assertIn("c7g.8xlarge", source)
        self.assertIn("another non-shell tmux workload is active", source)
        self.assertIn("pgrep -af", source)
        self.assertIn("tmux new-session -d", source)
        self.assertIn("remain-on-exit", source)
        self.assertIn("BORSUK_RUN_GLOBAL_CELL_STRIPE_CONFIRMATION=1", source)
        self.assertNotIn("--force", source)


if __name__ == "__main__":
    unittest.main()
