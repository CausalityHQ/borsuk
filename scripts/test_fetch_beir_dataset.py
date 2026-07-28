import hashlib
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "fetch_beir_dataset.py"


class FetchBeirDatasetTests(unittest.TestCase):
    def test_fetches_verifies_and_extracts_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "fixture.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("fixture/corpus.jsonl", '{"_id":"d","text":"x"}\n')
                bundle.writestr("fixture/queries.jsonl", '{"_id":"q","text":"x"}\n')
                bundle.writestr(
                    "fixture/qrels/test.tsv",
                    "query-id\tcorpus-id\tscore\nq\td\t1\n",
                )
            digest = hashlib.md5(archive.read_bytes()).hexdigest()
            output = root / "datasets"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--dataset",
                    "fixture",
                    "--output",
                    str(output),
                    "--url",
                    archive.as_uri(),
                    "--md5",
                    digest,
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            extracted = Path(completed.stdout.strip())
            self.assertEqual(extracted, (output / "fixture").resolve())
            self.assertTrue((extracted / "corpus.jsonl").is_file())
            self.assertTrue((extracted / "qrels" / "test.tsv").is_file())

    def test_rejects_checksum_mismatch_without_extracting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "fixture.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("fixture/corpus.jsonl", "{}\n")
            output = root / "datasets"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--dataset",
                    "fixture",
                    "--output",
                    str(output),
                    "--url",
                    archive.as_uri(),
                    "--md5",
                    "0" * 32,
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("checksum", completed.stderr.lower())
            self.assertFalse((output / "fixture").exists())

    def test_rejects_zip_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "fixture.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("../escaped", "bad")
            digest = hashlib.md5(archive.read_bytes()).hexdigest()
            output = root / "datasets"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--dataset",
                    "fixture",
                    "--output",
                    str(output),
                    "--url",
                    archive.as_uri(),
                    "--md5",
                    digest,
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("unsafe", completed.stderr.lower())
            self.assertFalse((root / "escaped").exists())


if __name__ == "__main__":
    unittest.main()
