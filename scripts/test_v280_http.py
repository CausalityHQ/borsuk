import unittest
import struct
import tempfile
from pathlib import Path

from scripts.v280_http import score_rows
from scripts.v220_bench_graph_http import load_f32_queries


class V280HttpTest(unittest.TestCase):
    def test_score_requires_exact_serving_parity_and_cosine_truth(self):
        reference = [
            {"ordinal": 0, "ids": [1, 2]},
            {"ordinal": 1, "ids": [3, 4]},
        ]
        serving = [
            {"ordinal": 0, "returned_ids": [1, 2], "whole_ns": 10, "vector_body_gets": 0},
            {"ordinal": 1, "returned_ids": [3, 4], "whole_ns": 20, "vector_body_gets": 0},
        ]
        truth = [(1, 5), (3, 6)]
        self.assertEqual(score_rows(serving, reference, truth, 2), {"hits": 2, "parity": 2})
        serving[1]["returned_ids"] = [4, 3]
        self.assertEqual(score_rows(serving, reference, truth, 2), {"hits": 2, "parity": 1})
        serving[1]["vector_body_gets"] = 1
        with self.assertRaises(ValueError):
            score_rows(serving, reference, truth, 2)

    def test_binary_queries_preserve_v279_unit_vectors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.f32"
            row = [1.0] + [0.0] * 767
            path.write_bytes(struct.pack("<768f", *row) * 2)
            self.assertEqual(load_f32_queries(path, 2), [row, row])
            path.write_bytes(path.read_bytes()[:-1])
            with self.assertRaises(ValueError):
                load_f32_queries(path, 2)


if __name__ == "__main__":
    unittest.main()
