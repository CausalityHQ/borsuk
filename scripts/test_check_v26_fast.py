import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import check_v26_fast


class V26FastGateTests(unittest.TestCase):
    def test_smoke_layer_contains_only_contract_and_static_checks(self) -> None:
        commands = check_v26_fast.smoke_commands(sys.executable)

        rendered = [" ".join(command) for command in commands]
        self.assertEqual(len(commands), 4)
        self.assertEqual(
            rendered[0],
            f"{sys.executable} -m unittest scripts.test_check_v26_fast",
        )
        self.assertIn(
            "cargo test -p borsuk-pq4 --lib v26_release_contract_pq4_core_", rendered[1]
        )
        self.assertEqual(rendered[2], "cargo fmt --all -- --check")
        self.assertEqual(rendered[3], "git diff --check")
        self.assertFalse(any("--workspace" in command for command in rendered))
        self.assertFalse(any("--all-targets" in command for command in rendered))

    def test_affected_layer_contains_all_focused_v26_checks(self) -> None:
        commands = check_v26_fast.affected_commands(sys.executable)

        rendered = [" ".join(command) for command in commands]
        self.assertEqual(len(commands), 5)
        self.assertEqual(
            rendered[0],
            f"{sys.executable} -m unittest scripts.test_check_v26_fast",
        )
        self.assertEqual(rendered[1], "cargo test -p borsuk-pq4 --lib -- --nocapture")
        self.assertEqual(
            rendered[2],
            "cargo test -p borsuk --example pq4_qualify pq4_qualify_ -- --nocapture",
        )
        self.assertEqual(rendered[3], "cargo fmt --all -- --check")
        self.assertEqual(rendered[4], "git diff --check")
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

    def test_gate_rejects_a_cargo_test_command_that_executes_zero_tests(self) -> None:
        # Break caught: a stale Cargo filter exits zero while silently exercising no contract.
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            cargo = temporary_path / "cargo"
            cargo.write_text(
                "#!/bin/sh\nprintf '%s\\n' "
                "'cargo test: 0 passed, 62 filtered out (1 suite, 0.00s)'\n",
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            marker = temporary_path / "later.txt"
            commands = [
                ["cargo", "test", "-p", "borsuk-v26", "missing-filter"],
                [
                    sys.executable,
                    "-c",
                    f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')",
                ],
            ]

            with mock.patch.dict(
                os.environ,
                {"PATH": f"{temporary}{os.pathsep}{os.environ['PATH']}"},
            ):
                result = check_v26_fast.run_gate(commands, temporary_path)

            self.assertNotEqual(result, 0)
            self.assertFalse(marker.exists())

    def test_milestone_layer_is_explicit_and_adds_full_assurance_last(self) -> None:
        affected = check_v26_fast.affected_commands(sys.executable)
        milestone = check_v26_fast.milestone_commands(sys.executable)

        self.assertEqual(milestone[: len(affected)], affected)
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
