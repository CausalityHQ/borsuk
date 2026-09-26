import unittest

from scripts.launch_v248_cohere_transfer_100k_spot import worker


class V255WorkerTest(unittest.TestCase):
    def test_single_million_row_arm_is_frozen(self):
        script = worker("0" * 40, "1" * 64, "source", "attempt", million=True)
        self.assertIn("--rows 1000000 --generation 255", script)
        self.assertIn("for n in $(seq 0 45)", script)
        self.assertIn("graph.bin build.json --diverse", script)
        self.assertIn("borsuk-v255-cohere-diverse-graph-1m-spot-v1", script)
        self.assertNotIn("@@", script)


if __name__ == "__main__":
    unittest.main()
