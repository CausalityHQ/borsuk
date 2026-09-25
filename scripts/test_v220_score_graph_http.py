import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import v220_score_graph_http as score


class ScoreTest(unittest.TestCase):
    def test_exact_parity_and_frozen_quality(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(raw=root / "raw", summary=root / "summary",
                previous=root / "previous", truth=root / "truth", output=root / "quality")
            with args.raw.open("w") as raw, args.previous.open("w") as previous, \
                    args.truth.open("w") as truth:
                for index in range(1000):
                    ids = list(range(100))
                    gold = ids[:98] + [100, 101] if index < 50 else \
                           ids[:99] + [100] if index < 286 else ids
                    raw.write(json.dumps({"ordinal": index, "returned_ids": ids,
                                          "vector_body_gets": 0}) + "\n")
                    previous.write(json.dumps({"ordinal": index,
                        "arms": {"4096-4096": {"returned_ids": ids}}}) + "\n")
                    truth.write(json.dumps({"ordinal": index, "gold_ids": gold}) + "\n")
            args.summary.write_text(json.dumps({
                "schema": "borsuk-v220-graph-http-1m-v1", "raw_sha256": score.digest(args.raw),
                "queries": 1000, "concurrency": 8}))
            with patch.object(score, "V198_SHA", score.digest(args.truth)):
                score.run(args)
            result = json.loads(args.output.read_text())
            self.assertEqual((result["hits"], result["p05_hits"]), (99664, 98))


if __name__ == "__main__":
    unittest.main()
