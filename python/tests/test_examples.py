import runpy
import unittest
from pathlib import Path


class PythonExampleTests(unittest.TestCase):
    def test_local_index_example_runs(self) -> None:
        example = Path(__file__).resolve().parents[1] / "examples" / "local_index.py"
        runpy.run_path(str(example), run_name="__main__")


if __name__ == "__main__":
    unittest.main()
