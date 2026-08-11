import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_publication_disk.py"


class PublicationDiskGuardTests(unittest.TestCase):
    def test_accepts_zero_reserve_and_reports_observed_free_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--root",
                    temporary,
                    "--minimum-free-bytes",
                    "0",
                    "--phase",
                    "test-success",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertRegex(
            completed.stdout, r"^publication_disk_free_bytes=[1-9][0-9]*\n$"
        )

    def test_rejects_an_unavailable_reserve_with_phase_and_byte_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--root",
                    temporary,
                    "--minimum-free-bytes",
                    str(2**63 - 1),
                    "--phase",
                    "test-failure",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
        self.assertEqual(completed.returncode, 1)
        self.assertIn(
            "insufficient publication disk before test-failure", completed.stderr
        )
        self.assertRegex(completed.stderr, r"free_bytes=[0-9]+")
        self.assertIn(f"required_bytes={2**63 - 1}", completed.stderr)


if __name__ == "__main__":
    unittest.main()
