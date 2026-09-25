import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import v222_score_external_graph_http as score


class ScoreTest(unittest.TestCase):
    def test_negative_quality_is_preserved_as_a_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(raw=root / "raw", summary=root / "summary",
                previous=root / "previous", truth=root / "truth", output=root / "quality")
            with args.raw.open("w") as raw, args.previous.open("w") as previous, \
                    args.truth.open("w") as truth:
                for index in range(1000):
                    expected = list(range(100))
                    got = expected if index else expected[:99] + [100]
                    gold = expected[:98] + [100, 101] if index < 50 else \
                           expected[:99] + [100] if index < 286 else expected
                    raw.write(json.dumps({"ordinal": index, "returned_ids": got,
                                          "vector_body_gets": 0}) + "\n")
                    previous.write(json.dumps({"ordinal": index,
                        "arms": {"4096-4096": {"returned_ids": expected}}}) + "\n")
                    truth.write(json.dumps({"ordinal": index, "gold_ids": gold}) + "\n")
            args.summary.write_text(json.dumps({
                "schema": "borsuk-v222-graph-http-client-1m-v1",
                "raw_sha256": score.digest(args.raw), "queries": 1000, "concurrency": 8,
                "transport": "persistent HTTP/1.1 VPC peer", "pass_label": "first_pass",
                "p95_ns": 20_000_000, "p99_ns": 25_000_000, "qps": 400}))
            with patch.object(score, "V198_SHA", score.digest(args.truth)):
                score.run(args)
            result = json.loads(args.output.read_text())
            self.assertEqual(result["exact_v219_id_lists"], 999)
            self.assertFalse(result["pass"])
            self.assertTrue(result["performance_pass"])


if __name__ == "__main__":
    unittest.main()
