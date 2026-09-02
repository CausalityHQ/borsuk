import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

from scripts.run_v25_page_containment import (
    MonitorLimits,
    cleanup_known_files,
    monitor_process_group,
    offline_environment,
)


class V25ContainmentMonitorTests(unittest.TestCase):
    def test_offline_environment_and_named_cleanup_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            environment = offline_environment(
                root,
                {
                    "PATH": "/bin",
                    "AWS_ACCESS_KEY_ID": "forbidden",
                    "AWS_SECRET_ACCESS_KEY": "forbidden",
                    "HTTPS_PROXY": "forbidden",
                    "NO_PROXY": "forbidden",
                },
            )
            self.assertEqual(environment["PATH"], "/bin")
            self.assertNotIn("AWS_ACCESS_KEY_ID", environment)
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
            self.assertNotIn("HTTPS_PROXY", environment)
            self.assertNotIn("NO_PROXY", environment)

            (root / "known.json").write_text("{}", encoding="utf-8")
            (root / "unknown.json").write_text("{}", encoding="utf-8")
            with self.assertRaises(ValueError):
                cleanup_known_files(root, ("known.json",))
            self.assertFalse((root / "known.json").exists())
            self.assertTrue((root / "unknown.json").exists())

    def test_monitor_stops_one_process_group_at_the_wall_cap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            process = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
                env=offline_environment(root, os.environ),
            )
            receipt = monitor_process_group(
                process,
                MonitorLimits(
                    wall_seconds=0.1,
                    rss_bytes=256 * 1024 * 1024,
                    psi_full_avg10=0.5,
                    swap_growth_bytes=64 * 1024 * 1024,
                    progress_seconds=1.0,
                    terminate_grace_seconds=0.2,
                ),
                progress_path=root / "progress.json",
            )
            self.assertEqual(receipt.reason, "wall-time-stop")
            self.assertIsNotNone(process.poll())


if __name__ == "__main__":
    unittest.main()
