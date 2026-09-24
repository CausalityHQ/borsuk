"""Exercise the V121 command boundary that failed in Spot attempt a0001."""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import v121_deep_image_paired as paired
from scripts import launch_v121_deep_image_paired_spot as launcher


class V121CliWiringTests(unittest.TestCase):
    def test_compose_uses_queries_argument(self) -> None:
        argv = ["v121_deep_image_paired", "compose", "--router", "router",
                "--queries", "queries.jsonl", "--rosters", "rosters.jsonl",
                "--output", "requests.jsonl"]
        with patch.object(sys, "argv", argv), patch.object(paired, "compose") as compose:
            paired.main()
        compose.assert_called_once_with(Path("router"), Path("queries.jsonl"),
                                        Path("rosters.jsonl"), Path("requests.jsonl"))

    def test_launcher_exits_nonzero_for_failed_terminal(self) -> None:
        argv = ["launch_v121", "--source-commit", "0" * 40,
                "--archive-uri", "s3://bucket/source.tar.gz",
                "--archive-sha256", "0" * 64, "--archive-bytes", "1",
                "--output-prefix", "s3://bucket/run"]
        terminal = {"status": "failed", "phase": "compose", "exit_code": 1}
        with patch.object(sys, "argv", argv), patch.object(
            launcher, "launch_and_monitor", return_value=terminal,
        ):
            with self.assertRaises(SystemExit) as exit_status:
                launcher.main()
        self.assertEqual(exit_status.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
