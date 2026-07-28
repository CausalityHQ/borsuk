import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CARGO_TOML = ROOT / "Cargo.toml"
SCRIPT = ROOT / "scripts" / "check_rust_test_build.sh"
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


class RustTestBuildGateTests(unittest.TestCase):
    def test_test_profile_avoids_expensive_debug_link_artifacts(self) -> None:
        source = CARGO_TOML.read_text()
        self.assertIn("[profile.test]", source)
        self.assertIn("debug = 0", source)
        self.assertIn("incremental = false", source)
        self.assertIn('split-debuginfo = "off"', source)

    def test_ci_builds_all_test_binaries_through_the_bounded_gate(self) -> None:
        source = WORKFLOW.read_text()
        self.assertIn("scripts/check_rust_test_build.sh", source)

    def test_gate_reports_elapsed_time_and_propagates_the_command_status(self) -> None:
        env = os.environ.copy()
        env["BORSUK_TEST_BUILD_COMMAND"] = "true"
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertRegex(
            result.stdout,
            r"rust-test-build status=0 elapsed_seconds=\d+ jobs=\d+",
        )

        env["BORSUK_TEST_BUILD_COMMAND"] = "false"
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertRegex(
            result.stdout,
            r"rust-test-build status=\d+ elapsed_seconds=\d+ jobs=\d+",
        )


if __name__ == "__main__":
    unittest.main()
