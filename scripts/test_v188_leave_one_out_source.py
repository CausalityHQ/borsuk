"""Paired 512-row nominee replacement and source decision."""

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.v188_leave_one_out_source import (
    ARMS, COUNT, FIRST, SCHEMA, canonical, decide, evaluate, paired_rosters,
    sha256,
)


class LeaveOneOutSourceTests(unittest.TestCase):
    def test_replaces_self_with_next_ranked_row(self):
        nominees = np.arange(513, dtype=np.int64)
        rosters, own_rank = paired_rosters(nominees, 7)
        self.assertEqual(own_rank, 8)
        self.assertEqual(len(rosters["self_included"]), 512)
        self.assertEqual(len(rosters["leave_one_out"]), 512)
        self.assertIn(7, rosters["self_included"])
        self.assertNotIn(7, rosters["leave_one_out"])
        self.assertEqual(rosters["leave_one_out"][-1], 512)
        same, own_rank = paired_rosters(nominees, 512)
        self.assertEqual(own_rank, 513)
        self.assertEqual(same["self_included"], same["leave_one_out"])

    def test_decision_uses_leave_one_out_only(self):
        totals = {"self_included": {"446": 12800, "672": 12800},
                  "leave_one_out": {"446": 12744, "672": 12746}}
        p05 = {"self_included": {"446": 100, "672": 100},
               "leave_one_out": {"446": 98, "672": 98}}
        self.assertEqual(decide(totals, p05), "advance-elastic-only")
        totals["leave_one_out"]["446"] = 12745
        self.assertEqual(decide(totals, p05), "advance-446-physical-gate")
        p05["leave_one_out"]["446"] = 97
        totals["leave_one_out"]["672"] = 12744
        self.assertEqual(decide(totals, p05), "revise-candidate-generator")

    def test_sealed_paired_candidate_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            ids = list(range(FIRST, FIRST + COUNT))
            with (output / "features.jsonl").open("x") as stream:
                for index, identifier in enumerate(ids):
                    stream.write(canonical({
                        "ordinal": FIRST + index, "source_id": identifier,
                        "source_row": index, "own_nominee_rank": 1,
                        "ranked_units": {"self_included": [0, 1],
                                         "leave_one_out": [0]},
                    }))
            (output / "prepare-seal.json").write_text(canonical({
                "schema": SCHEMA + "-prepare-seal",
                "source_truth_opened": False,
                "features_sha256": sha256(output / "features.jsonl"),
                "candidate_width": 32, "allowances": [446, 672],
                "arms": ARMS, "threshold": 12745,
                "metric": "pq_reconstructed_cosine",
                "nominee_shortlist": 512, "replacement_shortlist": 513,
                "pseudo_ids": ids,
            }))
            args = Namespace(output=output,
                             prepare_sha256=sha256(output / "prepare-seal.json"))
            source_ids = np.zeros(1_000_000, dtype=np.int64)
            source_ids[:COUNT] = ids
            inverse_new = np.zeros(1_000_000, dtype=np.int64)
            inverse_new[:100] = np.arange(100, dtype=np.int64) * 32
            with (patch("scripts.v188_leave_one_out_source._inputs",
                        return_value=(None, inverse_new, source_ids,
                                      None, None, None)),
                  patch("scripts.v188_leave_one_out_source._panel",
                        return_value=tuple(ids)),
                  patch("scripts.v188_leave_one_out_source._normalized",
                        return_value=None),
                  patch("scripts.v188_leave_one_out_source._truth",
                        return_value=np.arange(100, dtype=np.int64))):
                evaluate(args)
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(summary["candidate_hits"]["holdout"],
                             {"self_included": COUNT, "leave_one_out": COUNT // 2})
            self.assertEqual(summary["paired"]["holdout"]["446"]["losses"],
                             COUNT // 2)
            self.assertEqual(summary["decision"], "revise-candidate-generator")


if __name__ == "__main__":
    unittest.main()
