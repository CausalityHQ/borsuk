"""A short development replay may kill a route but cannot promote it."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.v283_dev64_summary import summarize


def rows(hits, arm):
    return [dict(ordinal=i, arm=arm, router_shortlist_gt_hits=85,
                 fetched_gt_hits=max(hit, 98), sq8_returned_gt_hits=hit,
                 gets=32, bytes=16_773_120, plan_us=3000, rank_us=12000,
                 rss_bytes=100_000_000) for i, hit in enumerate(hits)]


class Dev64SummaryTests(unittest.TestCase):
    def test_kill_only_decision_on_exact_paired_roster(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.jsonl"
            candidate = root / "candidate.jsonl"
            control = [92] * 4 + [96] * 28 + [97] * 32
            full_control = control + [100] * 936
            baseline.write_text("".join(json.dumps(x) + "\n" for x in
                                        rows(full_control, "graph") + rows(full_control, "flat")))
            digest = hashlib.sha256(baseline.read_bytes()).hexdigest()
            candidate.write_text("".join(json.dumps(x) + "\n" for x in
                                         rows([98] * 64, "graph") + rows([98] * 64, "flat")))
            self.assertTrue(summarize(candidate, baseline, digest)["advance_dev256"])
            candidate.write_text("".join(json.dumps(x) + "\n" for x in
                                         rows([96] * 64, "graph") + rows([96] * 64, "flat")))
            self.assertFalse(summarize(candidate, baseline, digest)["advance_dev256"])


if __name__ == "__main__":
    unittest.main()
