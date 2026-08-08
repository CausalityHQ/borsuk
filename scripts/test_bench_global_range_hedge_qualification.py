import json
import os
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "bench_global_range_hedge_qualification.sh"
LAUNCHER = ROOT / "scripts" / "launch_aws_global_range_hedge_qualification.sh"
MANIFEST = ROOT / "docs" / "research" / "global-range-hedge-qualification.json"


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
        self.assertEqual(manifest["hedge_after_ms"], {"control": "none", "candidate": "75"})

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


if __name__ == "__main__":
    unittest.main()
