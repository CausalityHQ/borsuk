import json
import os
import pathlib
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "bench_global_range_hedge_qualification.sh"
LAUNCHER = ROOT / "scripts" / "launch_aws_global_range_hedge_qualification.sh"
MANIFEST = ROOT / "docs" / "research" / "global-range-hedge-qualification.json"
EXACT_MANIFEST = (
    ROOT / "docs" / "research" / "global-exact-rerank-hedge-qualification.json"
)


class GlobalRangeHedgeHarnessTest(unittest.TestCase):
    def test_manifest_freezes_the_uncached_paired_contract(self):
        manifest = json.loads(MANIFEST.read_text())
        self.assertEqual(manifest["repetitions"], 5)
        self.assertEqual(
            manifest["arm_orders"],
            [
                ["control", "candidate"],
                ["candidate", "control"],
                ["control", "candidate"],
                ["candidate", "control"],
                ["control", "candidate"],
            ],
        )
        self.assertFalse(manifest["disk_cache_enabled"])
        self.assertEqual(manifest["queries_per_arm"], 500)
        self.assertEqual(manifest["read_writer"], 0)
        self.assertEqual(
            manifest["hedge_after_ms"], {"control": "none", "candidate": "75"}
        )

    def test_shell_harnesses_parse_and_require_explicit_authority(self):
        for script in (RUNNER, LAUNCHER):
            parsed = subprocess.run(
                ["bash", "-n", str(script)], text=True, capture_output=True, check=False
            )
            self.assertEqual(parsed.returncode, 0, parsed.stderr)

        runner = subprocess.run(
            ["bash", str(RUNNER)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            env={**os.environ, "BORSUK_RUN_GLOBAL_RANGE_HEDGE": "0"},
        )
        self.assertEqual(runner.returncode, 2)
        self.assertIn("BORSUK_RUN_GLOBAL_RANGE_HEDGE=1", runner.stderr)

        launcher = subprocess.run(
            ["bash", str(LAUNCHER)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            env={**os.environ, "AWS_PROFILE": "not-causality"},
        )
        self.assertEqual(launcher.returncode, 2)
        self.assertIn("causality", launcher.stderr)

    def test_exact_rerank_manifest_freezes_v35_and_absolute_latency_gate(self):
        manifest = json.loads(EXACT_MANIFEST.read_text())
        self.assertEqual(manifest["base_run_id"], "20260809T034709Z-v35-8e09070")
        self.assertEqual(manifest["repetitions"], 5)
        self.assertEqual(manifest["required_better_paired_repetitions"], 4)
        self.assertEqual(manifest["minimum_pooled_p95_improvement_ms"], 5.0)
        self.assertFalse(manifest["disk_cache_enabled"])

    def test_launcher_forwards_the_selected_manifest_to_the_remote_runner(self):
        launcher = LAUNCHER.read_text()
        runner = RUNNER.read_text()
        self.assertIn("campaign_rel=", launcher)
        self.assertIn("BORSUK_GLOBAL_RANGE_HEDGE_MANIFEST=", launcher)
        self.assertIn("BORSUK_GLOBAL_RANGE_HEDGE_CAMPAIGN:?set", launcher)
        self.assertIn("BORSUK_GLOBAL_RANGE_HEDGE_MANIFEST:?set", runner)
        self.assertIn("--validate-manifest-only", launcher)
        self.assertIn("--validate-manifest-only", runner)


if __name__ == "__main__":
    unittest.main()
