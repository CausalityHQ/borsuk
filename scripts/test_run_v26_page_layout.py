import dataclasses
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

from scripts.run_v26_page_layout import (
    MonitorLimits,
    MonitorReceipt,
    cleanup_known_files,
    monitor_process_group,
    offline_environment,
    seal_v26_layout_receipt,
)


class V26PageLayoutMonitorTests(unittest.TestCase):
    def test_v26_offline_environment_and_named_cleanup_fail_closed(self) -> None:
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
            with self.assertRaisesRegex(ValueError, "unknown files"):
                cleanup_known_files(root, ("known.json",))
            self.assertFalse((root / "known.json").exists())
            self.assertTrue((root / "unknown.json").exists())

    def test_v26_monitor_stops_one_process_group_at_first_limit(self) -> None:
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
                    swap_growth_bytes=0,
                    progress_seconds=1.0,
                    terminate_grace_seconds=0.2,
                ),
                progress_path=root / "progress.json",
            )
            self.assertEqual(receipt.reason, "wall-time-stop")
            self.assertIsNotNone(process.poll())

    def test_v26_receipt_seals_only_a_clean_original_process_exit(self) -> None:
        build = {
            "authority": {"schema": "borsuk-v26-dual-tree-layout-v1"},
            "inputs": [{"role": "construction-parquet"}],
            "outputs": [{"role": "page-assignments-parquet"}],
            "row_count": 4096,
            "leaves_per_tree": 6,
            "page_count": 12,
            "projection_steps": 1_000_000,
            "worker_count": 4,
        }
        monitor = MonitorReceipt(
            reason="process-exit",
            exit_code=0,
            elapsed_seconds=1.25,
            cpu_ns=4_000_000_000,
            peak_rss_bytes=128 * 1024 * 1024,
            peak_psi_full_avg10=0.125,
            swap_start_bytes=64,
            swap_end_bytes=64,
        )
        encoded = seal_v26_layout_receipt(build, monitor)
        self.assertEqual(encoded[-1:], b"\n")
        receipt = __import__("json").loads(encoded)
        self.assertEqual(receipt["elapsed_ns"], 1_250_000_000)
        self.assertEqual(receipt["peak_psi_full_avg10_milli_percent"], 125)
        self.assertFalse(receipt["claim_eligible"])

        for changed in (
            dataclasses.replace(monitor, reason="rss-stop"),
            dataclasses.replace(monitor, exit_code=1),
            dataclasses.replace(monitor, swap_end_bytes=65),
            dataclasses.replace(monitor, peak_psi_full_avg10=0.501),
        ):
            with self.assertRaises(ValueError):
                seal_v26_layout_receipt(build, changed)


if __name__ == "__main__":
    unittest.main()
