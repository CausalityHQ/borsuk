"""Check paired GT accounting and the physical cap before a Spot campaign."""

import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v116_validation_paired import reduce


class ValidationPairedTest(unittest.TestCase):
    def test_both_arms_use_same_truth_and_enforce_caps(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            requests = root / "requests.jsonl"
            replay = root / "replay.jsonl"
            truth = root / "truth.parquet"
            evidence = root / "evidence.jsonl"
            summary = root / "summary.json"
            requests.write_text("".join(json.dumps({
                "query_ordinal": index, "nominees": [index],
                "baseline_ranges": [[0, 101]],
            }) + "\n" for index in range(1000)))
            replay.write_text("".join(json.dumps({
                "query_ordinal": index, "nominees": [index],
                "baseline_ranges": [[0, 101]],
                "baseline_bytes": 101,
                "baseline_returned_ids": list(range(99)) + [100],
                "ranges": [[0, 101]], "plan_bytes": 101,
                "returned_ids": list(range(100)),
            }) + "\n" for index in range(1000)))
            pq.write_table(pa.table({"feature_row_id": list(range(100)) * 1000}), truth)
            reduce(requests, replay, truth, evidence, summary)
            result = json.loads(summary.read_text())
            self.assertEqual(result["total_hits"], {"candidate": 100_000,
                                                    "baseline": 99_000})
            self.assertEqual(result["paired_queries_candidate_better"], 1000)
            self.assertTrue(result["qualifies_validation_quality"])
            altered = replay.read_text().splitlines()
            row = json.loads(altered[0]); row["plan_bytes"] = 16_777_217
            altered[0] = json.dumps(row)
            replay.write_text("\n".join(altered) + "\n")
            with self.assertRaises(ValueError):
                reduce(requests, replay, truth, evidence, summary)


if __name__ == "__main__":
    unittest.main()
