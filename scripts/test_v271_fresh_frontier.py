import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

import numpy as np

from scripts.v271_fresh_frontier import percentile, score, unit


class FreshFrontierTests(unittest.TestCase):
    def test_score_uses_same_raw_query_samples_for_recall_and_percentiles(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            truth = root / "truth.u32"
            raw = root / "raw.jsonl"
            output = root / "summary.json"
            np.array([range(100), range(100)], dtype="<u4").tofile(truth)
            raw.write_text(
                "".join(
                    json.dumps(
                        {
                            "ordinal": ordinal,
                            "ids": list(range(100))
                            if ordinal == 0
                            else list(range(1, 101)),
                            "latency_ns": ns,
                        }
                    )
                    + "\n"
                    for ordinal, ns in enumerate((1_000_000, 3_000_000))
                )
            )
            score(Namespace(raw=raw, truth=truth, output=output, label="rust"))
            result = json.loads(output.read_text())["rust"]
            self.assertEqual(result["hits"], 199)
            self.assertEqual((result["p50_ms"], result["p95_ms"]), (1.0, 3.0))
            self.assertEqual(percentile([3, 1, 2], 90), 3)

    def test_unit_normalizes_without_mutating_readonly_source(self):
        source = np.array([[3.0, 4.0]], dtype=np.float32)
        source.flags.writeable = False
        self.assertEqual(
            unit(source).tolist(), [[0.6000000238418579, 0.800000011920929]]
        )
        self.assertEqual(source.tolist(), [[3.0, 4.0]])


if __name__ == "__main__":
    unittest.main()
