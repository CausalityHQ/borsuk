import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import v230_score_mutation_graph_http as scorer


class MutationScoreTest(unittest.TestCase):
    def test_split_loss_fails_even_when_total_recall_is_unchanged(self):
        ids = list(range(100))
        previous = [{"ordinal": i, "arms": {"4096-4096": {"returned_ids": ids}}}
                    for i in range(1000)]
        current = [{"ordinal": i, "returned_ids": ids,
                    "vector_body_gets": 0} for i in range(1000)]
        truth = [{"ordinal": i, "gold_ids": ids} for i in range(1000)]
        current[0] = {**current[0], "returned_ids": ids[:-1] + [100]}
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            paths = {name: root / name for name in
                     ("raw", "summary", "previous", "truth", "output")}
            paths["summary"].write_text(json.dumps({
                "schema": "borsuk-v222-graph-http-client-1m-v1",
                "raw_sha256": "identity", "queries": 1000, "concurrency": 8,
                "transport": "persistent HTTP/1.1 VPC peer", "pass_label": "first_pass",
                "p95_ns": 10_000_000, "p99_ns": 11_000_000, "qps": 500,
            }))
            args = argparse.Namespace(**paths)
            with (patch.object(scorer, "digest", return_value="identity"),
                  patch.object(scorer, "V198_SHA", "identity"),
                  patch.object(scorer, "BASE_HITS", 100_000),
                  patch.object(scorer, "records", side_effect=[current, previous, truth])):
                scorer.run(args)
            value = json.loads(paths["output"].read_text())
            self.assertEqual(value["candidate_hits"], 99_999)
            self.assertFalse(value["quality_pass"])
            self.assertFalse(value["pass"])


if __name__ == "__main__":
    unittest.main()
