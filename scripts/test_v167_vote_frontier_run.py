"""Decision-rule tests for the sealed V167 pseudoquery panel."""

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts.v167_vote_frontier_run import (
    FRACTIONS, _read_labels_for_check, _summary, _validate_label_geometry, holdout_pass,
    plan_roster, quality_pass, select_fraction,
)


class VoteFrontierDecisionTests(unittest.TestCase):
    def test_checker_accepts_matching_candidate_units(self):
        order = np.arange(64, dtype=np.int64)
        roster = {"query_ordinal": 256, "source_id": 10, "nominees": [0, 1]}
        label = {"query_ordinal": 256, "source_id": 10,
                 "actual_by_unit": [[0, 1], [1, 0]],
                 "candidate_rows": 64, "eligible_rows": 62}
        _validate_label_geometry(roster, label, order, order, 64, 32)

    def test_planning_uses_votes_and_primary_units_without_proxy_labels(self):
        planned = plan_roster({0: 513, 1: 1, 4: 2}, (0,),
                              primary_count=1, nominee_count=4,
                              page_count=5, max_gets=2, max_units=3,
                              unit_bytes=24_960)
        self.assertEqual(planned["baseline"]["score"], 516)
        self.assertEqual(planned["baseline"]["bytes"], 74_880)
        self.assertEqual(planned["fractions"]["4/5"]["target_score"], 516)
        self.assertEqual(planned["fractions"]["4/5"]["bytes"], 74_880)

    def test_selects_first_fraction_meeting_aggregate_and_tail_gates(self):
        rows = {
            fraction: [(100, 98 if fraction == (4, 5) else 99)] * 128
            for fraction in FRACTIONS
        }
        self.assertEqual(select_fraction(rows), (9, 10))

    def test_tail_gate_rejects_seven_queries_with_two_lost_rows(self):
        captures = [(100, 99)] * 121 + [(100, 98)] * 7
        self.assertFalse(quality_pass(captures))

    def test_aggregate_gate_rejects_ninety_eight_percent_capture(self):
        self.assertFalse(quality_pass([(100, 98)] * 128))

    def test_exact_ninety_nine_percent_with_one_lost_per_query_passes(self):
        self.assertTrue(quality_pass([(100, 99)] * 128))

    def test_holdout_requires_strict_byte_and_get_gates(self):
        metrics = {"quality_pass": True, "baseline_bytes": 1000,
                   "candidate_bytes": 900, "baseline_gets": 100,
                   "candidate_gets": 120}
        self.assertTrue(holdout_pass((9, 10), metrics))
        self.assertFalse(holdout_pass((1, 1), metrics))
        self.assertFalse(holdout_pass((9, 10), {**metrics, "candidate_bytes": 901}))
        self.assertFalse(holdout_pass((9, 10), {**metrics, "candidate_gets": 121}))

    def test_failed_fit_does_not_open_holdout_proxy_labels(self):
        baseline = {"intervals": [[0, 0]], "bytes": 24_960, "gets": 1}
        candidate = {"intervals": [[1, 1]], "bytes": 24_960, "gets": 1}
        plans = [{"query_ordinal": ordinal, "source_id": ordinal,
                  "baseline": baseline,
                  "fractions": {f"{n}/{d}": candidate for n, d in FRACTIONS}}
                 for ordinal in range(256, 512)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "plan-seal.json").write_text("sealed\n")
            with (root / "proxy-labels.jsonl").open("w") as labels:
                for ordinal in range(256, 384):
                    labels.write(json.dumps({"query_ordinal": ordinal,
                                             "source_id": ordinal,
                                             "actual_by_unit": [[0, 1]]}) + "\n")
                labels.write("holdout must remain unread\n")
            summary = _summary(root, [], plans)
            checked_labels = _read_labels_for_check(root, selected_fraction=None)
        self.assertIsNone(summary["selected_fraction"])
        self.assertIsNone(summary["holdout"])
        self.assertEqual(summary["decision"], "killed")
        self.assertEqual(len(checked_labels), 128)

    def test_frozen_holdout_passes_only_after_fit_selection(self):
        baseline = {"intervals": [[0, 1]], "bytes": 49_920, "gets": 1}
        candidate = {"intervals": [[0, 0]], "bytes": 24_960, "gets": 1}
        plans = [{"query_ordinal": ordinal, "source_id": ordinal,
                  "baseline": baseline,
                  "fractions": {f"{n}/{d}": candidate for n, d in FRACTIONS}}
                 for ordinal in range(256, 512)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "plan-seal.json").write_text("sealed\n")
            with (root / "proxy-labels.jsonl").open("w") as labels:
                for ordinal in range(256, 512):
                    labels.write(json.dumps({"query_ordinal": ordinal,
                                             "source_id": ordinal,
                                             "actual_by_unit": [[0, 100], [1, 0]]})
                                 + "\n")
            summary = _summary(root, [], plans)
        self.assertEqual(summary["selected_fraction"], "4/5")
        self.assertEqual(summary["decision"], "advance-to-100k")
        self.assertEqual(summary["holdout"]["candidate_capture"], 12_800)

    def test_one_over_one_selection_kills_without_scoring_holdout(self):
        baseline = {"intervals": [[0, 0]], "bytes": 24_960, "gets": 1}
        lost = {"intervals": [[1, 1]], "bytes": 24_960, "gets": 1}
        plans = [{"query_ordinal": ordinal, "source_id": ordinal,
                  "baseline": baseline,
                  "fractions": {f"{n}/{d}": baseline if (n, d) == (1, 1)
                                else lost for n, d in FRACTIONS}}
                 for ordinal in range(256, 512)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "plan-seal.json").write_text("sealed\n")
            with (root / "proxy-labels.jsonl").open("w") as labels:
                for ordinal in range(256, 384):
                    labels.write(json.dumps({"query_ordinal": ordinal,
                                             "source_id": ordinal,
                                             "actual_by_unit": [[0, 100], [1, 0]]})
                                 + "\n")
                labels.write("holdout must remain unread\n")
            summary = _summary(root, [], plans)
            checked_labels = _read_labels_for_check(root, selected_fraction="1/1")
        self.assertEqual(summary["selected_fraction"], "1/1")
        self.assertIsNone(summary["holdout"])
        self.assertEqual(summary["decision"], "killed")
        self.assertEqual(len(checked_labels), 128)


if __name__ == "__main__":
    unittest.main()
