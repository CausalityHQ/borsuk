import sys
import tempfile
import unittest
from pathlib import Path

from scripts import check_v26_fast


class V26FastGateTests(unittest.TestCase):
    def test_fast_layer_contains_only_focused_v26_checks(self) -> None:
        commands = check_v26_fast.fast_commands(sys.executable)

        rendered = [" ".join(command) for command in commands]
        self.assertEqual(len(commands), 7)
        self.assertIn("cargo test -p borsuk-v26 --lib v26_fast_", rendered[0])
        self.assertIn(
            "cargo test -p borsuk-v26 --example v26_page_layout v26_",
            rendered[1],
        )
        self.assertIn(
            "cargo test -p borsuk-v26 --example v26_pq16_serving_build v26_",
            rendered[2],
        )
        self.assertIn(
            "cargo test -p borsuk-v26 --example v26_pq16_serving v26_",
            rendered[3],
        )
        self.assertIn(
            "-m unittest scripts.test_run_v26_page_layout "
            "scripts.test_launch_v26_page_layout_spot",
            rendered[4],
        )
        self.assertEqual(rendered[5], "cargo fmt --all -- --check")
        self.assertEqual(rendered[6], "git diff --check")
        self.assertFalse(any("--workspace" in command for command in rendered))
        self.assertFalse(any("--all-targets" in command for command in rendered))

    def test_gate_stops_at_first_failure_and_does_not_run_later_work(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "commands.txt"
            commands = [
                [
                    sys.executable,
                    "-c",
                    f"from pathlib import Path; Path({str(marker)!r}).write_text('one\\n')",
                ],
                [sys.executable, "-c", "raise SystemExit(7)"],
                [
                    sys.executable,
                    "-c",
                    f"from pathlib import Path; Path({str(marker)!r}).write_text('three\\n')",
                ],
            ]

            result = check_v26_fast.run_gate(commands, Path(temporary))

            self.assertEqual(result, 7)
            self.assertEqual(marker.read_text(encoding="utf-8"), "one\n")

    def test_milestone_layer_is_explicit_and_adds_full_assurance_last(self) -> None:
        fast = check_v26_fast.fast_commands(sys.executable)
        milestone = check_v26_fast.milestone_commands(sys.executable)

        self.assertEqual(milestone[: len(fast)], fast)
        self.assertEqual(
            milestone[-2],
            [
                "cargo",
                "clippy",
                "--locked",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        )
        self.assertEqual(
            milestone[-1],
            ["cargo", "test", "--locked", "--workspace", "--all-targets"],
        )


if __name__ == "__main__":
    unittest.main()
