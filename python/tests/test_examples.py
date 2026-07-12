import os
import runpy
import unittest
from pathlib import Path


class PythonExampleTests(unittest.TestCase):
    def test_local_index_example_runs(self) -> None:
        example = Path(__file__).resolve().parents[1] / "examples" / "local_index.py"
        runpy.run_path(str(example), run_name="__main__")

    def test_docs_ladder_example_runs(self) -> None:
        example = Path(__file__).resolve().parents[1] / "examples" / "docs_ladder.py"
        runpy.run_path(str(example), run_name="__main__")

    def test_cookbook_example_runs(self) -> None:
        example = Path(__file__).resolve().parents[1] / "examples" / "cookbook.py"
        runpy.run_path(str(example), run_name="__main__")

    def test_s3_index_example_runs_when_configured(self) -> None:
        if not os.environ.get("BORSUK_S3_TEST_URI"):
            self.skipTest("BORSUK_S3_TEST_URI is not set")

        example = Path(__file__).resolve().parents[1] / "examples" / "s3_index.py"
        runpy.run_path(str(example), run_name="__main__")


if __name__ == "__main__":
    unittest.main()
