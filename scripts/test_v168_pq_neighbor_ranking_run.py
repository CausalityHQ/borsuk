"""Frozen V168 adjacent-only ranking and decision checks."""

import unittest
import argparse
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.v168_pq_neighbor_ranking_run import (
    check, paired_bootstrap_interval, rank_adjacent_units, screen_decision,
)


class NeighborRankingTests(unittest.TestCase):
    def test_direct_and_neighbor_control_use_distinct_scores_and_stable_ties(self):
        features = [[9, 2.0, 3], [4, 1.0, 8], [7, 1.0, 2]]
        self.assertEqual(rank_adjacent_units(features, "direct", 2), [4, 7])
        self.assertEqual(rank_adjacent_units(features, "neighbor", 2), [7, 9])

    def test_bootstrap_of_constant_paired_gain_has_exact_interval(self):
        self.assertEqual(paired_bootstrap_interval([1] * 128), (128, 128))

    def test_decision_requires_gain_interval_and_sensitivity(self):
        rows = [{"direct": {16: 2, 32: 2, 64: 2},
                 "neighbor": {16: 1, 32: 1, 64: 1},
                 "positive_rows": 2} for _ in range(128)]
        summary = screen_decision(rows)
        self.assertEqual(summary["decision"], "advance-to-planner")
        self.assertEqual(summary["gain_32"], 128)
        for row in rows:
            row["direct"][16] = 0
        self.assertEqual(screen_decision(rows)["decision"], "killed")

    def test_check_replay_creates_fresh_child_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "sealed"
            output.mkdir()
            for name in ("features.jsonl", "proxy-labels.jsonl",
                         "prepare-seal.json"):
                (output / name).write_text(name)
            (output / "summary.json").write_text(json.dumps({"decision": "killed"}))
            feature = {"nominees": [0], "candidate_units": 1,
                       "candidate_rows": 32,
                       "adjacent_features": []}
            args = argparse.Namespace(output=output, old_layout=None,
                                      order=None, v164_terminal=None)

            def replay_prepare(replay_args):
                self.assertFalse(replay_args.output.exists())
                replay_args.output.mkdir()
                for name in ("features.jsonl", "proxy-labels.jsonl",
                             "prepare-seal.json"):
                    (replay_args.output / name).write_text(name)

            with (patch("scripts.v168_pq_neighbor_ranking_run._verify_prepare",
                        return_value=[feature]),
                  patch("scripts.v168_pq_neighbor_ranking_run._verify_plans",
                        return_value=[{}]),
                  patch("scripts.v168_pq_neighbor_ranking_run._summary",
                        return_value={"decision": "killed"}),
                  patch("scripts.v168_pq_neighbor_ranking_run.load_orders",
                        return_value=(np.arange(32), np.arange(32))),
                  patch("scripts.v168_pq_neighbor_ranking_run.records",
                        return_value=[{"actual_by_unit": []}]),
                  patch("scripts.v168_pq_neighbor_ranking_run.candidate_units",
                        return_value=(0,)),
                  patch("scripts.v168_pq_neighbor_ranking_run.prepare",
                        side_effect=replay_prepare)):
                self.assertEqual(check(args)["status"], "pass")


if __name__ == "__main__":
    unittest.main()
