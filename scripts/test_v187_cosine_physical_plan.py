"""GT-blind physical arm geometry and source decision."""

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import numpy as np

from scripts.v187_cosine_physical_plan import (
    ARMS, COUNT, FIRST, SCHEMA, canonical, decide, evaluate, plan, plan_arm,
    score_intervals, sha256,
)


class CosinePhysicalPlanTests(unittest.TestCase):
    def test_arm_reports_mandatory_floor_without_silent_cap_raise(self):
        result = plan_arm((0, 100), (0, 100), max_gets=1, unit_cap=100)
        self.assertEqual(result["mandatory_floor_units"], 101)
        self.assertIs(result["feasible"], False)
        self.assertEqual(result["intervals"], [])
        elastic = plan_arm((0, 100), (0, 100), max_gets=1,
                           unit_cap=100, floor_elastic=True)
        self.assertIs(elastic["feasible"], True)
        self.assertEqual(elastic["unit_cap"], 101)
        self.assertEqual(elastic["base_units"], 100)

    def test_arm_covers_mandatory_and_charges_bridges(self):
        result = plan_arm((0, 3), (3, 1, 2, 0), max_gets=1, unit_cap=4)
        self.assertIs(result["feasible"], True)
        self.assertEqual(result["intervals"], [[0, 3]])
        self.assertEqual(result["units"], 4)
        self.assertEqual(result["bytes"], 4 * 24960)
        self.assertEqual(score_intervals({0: 1, 1: 2, 2: 3, 3: 1},
                                         result["intervals"]), 7)

    def test_decision_requires_quality_and_v155_scaled_resources(self):
        good = {"hits": 12745, "p05": 98, "infeasible": 0,
                "bytes": 1425152900, "gets": 2832}
        results = {"v155_mean_floor_elastic": good,
                   "get32_units446": dict(good, gets=3000),
                   "elastic_16m": dict(good, bytes=2000000000, gets=3500)}
        self.assertEqual(decide(results), "advance-to-replication")
        results["v155_mean_floor_elastic"] = dict(good, bytes=1425152902)
        self.assertEqual(decide(results), "elastic-physical-screen-pass")
        results["v155_mean_floor_elastic"] = dict(good, infeasible=1)
        self.assertEqual(decide(results), "elastic-physical-screen-pass")
        results["elastic_16m"] = dict(good, hits=12744)
        self.assertEqual(decide(results), "revise-utility-allocation-or-layout")

    def test_sealed_plans_score_full_physical_truth(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            ids = list(range(FIRST, FIRST + COUNT))
            with (output / "features.jsonl").open("x") as stream:
                for index, identifier in enumerate(ids):
                    stream.write(canonical({
                        "ordinal": FIRST + index, "source_id": identifier,
                        "source_row": index, "mandatory_units": [0],
                        "own_nominated": index % 2 == 0,
                        "ranked_units": [0, 1],
                    }))
            (output / "prepare-seal.json").write_text(canonical({
                "schema": SCHEMA + "-prepare-seal",
                "source_truth_opened": False,
                "features_sha256": sha256(output / "features.jsonl"),
                "candidate_width": 32,
                "arms": ARMS,
                "metric": "cosine",
                "pseudo_ids": ids,
            }))
            args = Namespace(output=output,
                             prepare_sha256=sha256(output / "prepare-seal.json"))
            plan(args)
            self.assertFalse((output / "source-labels.jsonl").exists())
            args.plan_sha256 = sha256(output / "plan-seal.json")
            source_ids = np.zeros(1_000_000, dtype=np.int64)
            source_ids[:COUNT] = ids
            inverse_new = np.zeros(1_000_000, dtype=np.int64)
            inverse_new[:100] = np.arange(100, dtype=np.int64) * 32
            with (patch("scripts.v187_cosine_physical_plan._inputs",
                        return_value=(None, inverse_new, source_ids,
                                      None, None, None)),
                  patch("scripts.v187_cosine_physical_plan._panel",
                        return_value=tuple(ids)),
                  patch("scripts.v187_cosine_physical_plan._normalized",
                        return_value=None),
                  patch("scripts.v187_cosine_physical_plan._truth",
                        return_value=np.arange(100, dtype=np.int64))):
                evaluate(args)
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(summary["own_nominated"],
                             {"fit": COUNT // 4, "holdout": COUNT // 4})
            # Two truth-bearing units are fetched by each feasible arm.
            self.assertEqual(summary["results"]["holdout"]["v155_mean_floor_elastic"]["hits"],
                             COUNT)
            self.assertEqual(summary["results"]["holdout"]["v155_mean_floor_elastic"]["p05"], 2)
            self.assertEqual(summary["decision"],
                             "revise-utility-allocation-or-layout")
            with (output / "plans.jsonl").open("a") as stream:
                stream.write("{}\n")
            with self.assertRaises(ValueError):
                evaluate(args)


if __name__ == "__main__":
    unittest.main()
