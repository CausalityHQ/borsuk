import json
import tempfile
import unittest
from pathlib import Path

from scripts.v282_summarize_falsifier import summarize


class SummarizeTest(unittest.TestCase):
    def test_paired_gate_and_physical_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.jsonl"
            rows = [
                {"ordinal": ordinal, "arm": arm,
                 "router_shortlist_gt_hits": 97, "fetched_gt_hits": 99,
                 "sq8_returned_gt_hits": 98,
                 "gets": 8, "bytes": 1_597_440, "plan_us": 100, "rank_us": 500,
                 "rss_bytes": 100_000_000}
                for ordinal in range(1000) for arm in ("graph", "flat")
            ]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            result = summarize(path)
            self.assertTrue(result["development"]["v282_pass"])
            self.assertTrue(result["validation"]["v282_pass"])
            rows[0]["gets"] = 33
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            with self.assertRaisesRegex(ValueError, "physical cap"):
                summarize(path)


if __name__ == "__main__":
    unittest.main()
