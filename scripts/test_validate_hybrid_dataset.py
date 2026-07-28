import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GENERATE = ROOT / "scripts" / "generate_hybrid_synthetic_dataset.py"
VALIDATE = ROOT / "scripts" / "validate_hybrid_dataset.py"


class ValidateHybridDatasetTests(unittest.TestCase):
    def test_accepts_intact_dataset_and_rejects_corruption(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            dataset = Path(temporary) / "dataset"
            subprocess.run(
                [
                    sys.executable,
                    str(GENERATE),
                    "--output",
                    str(dataset),
                    "--dataset",
                    "fixture",
                    "--documents",
                    "12",
                    "--queries",
                    "6",
                    "--topics",
                    "3",
                    "--dense-dimensions",
                    "8",
                    "--sparse-dimensions",
                    "64",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            completed = subprocess.run(
                [sys.executable, str(VALIDATE), str(dataset)],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            report = json.loads(completed.stdout)
            self.assertEqual(report["dataset"], "fixture")
            self.assertEqual(report["documents"], 12)
            self.assertEqual(report["queries"], 6)
            self.assertEqual(report["status"], "valid")

            with (dataset / "corpus.dense.f32").open("ab") as handle:
                handle.write(b"corruption")
            corrupted = subprocess.run(
                [sys.executable, str(VALIDATE), str(dataset)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(corrupted.returncode, 0)
            self.assertIn("sha256", corrupted.stderr.lower())


if __name__ == "__main__":
    unittest.main()
