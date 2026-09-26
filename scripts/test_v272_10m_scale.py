import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

import numpy as np

from scripts.v272_10m_scale import QUERIES, score


class ScaleScoreTests(unittest.TestCase):
    def test_exact_ids_and_same_sample_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            truth, raw, output = (
                root / name for name in ("truth.u32", "raw.jsonl", "quality.json")
            )
            np.tile(np.arange(100, dtype="<u4"), (QUERIES, 1)).tofile(truth)
            with raw.open("w") as samples:
                for ordinal in range(QUERIES):
                    samples.write(
                        json.dumps(
                            {
                                "ordinal": ordinal,
                                "ids": list(range(100)),
                                "latency_ns": (ordinal + 1) * 1_000_000,
                            }
                        )
                        + "\n"
                    )
            score(Namespace(raw=raw, truth=truth, output=output))
            result = json.loads(output.read_text())
            self.assertEqual(result["combined"]["hits"], 100_000)
            self.assertEqual((result["p50_ms"], result["p95_ms"]), (500.0, 950.0))
            self.assertEqual(
                result["validation_remaining744_prior_used"]["p05_hits"], 100
            )


if __name__ == "__main__":
    unittest.main()
