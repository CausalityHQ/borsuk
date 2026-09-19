import subprocess
import sys
import unittest
from pathlib import Path

import numpy as np

from scripts.v86_coarse_to_fine_screen import build_coarse_to_fine_artifact
from scripts.v88_planner_objective_screen import (
    evaluate_planner_objective_pair,
    validate_result_budgets,
)


class V88PlannerObjectiveTests(unittest.TestCase):
    def test_pair_changes_only_wave_one_objective_and_binds_budgets(self) -> None:
        base = np.asarray(
            [
                [0.0, 0.0, 0.0, 0.0],
                [0.1, 0.0, 0.0, 0.0],
                [0.2, 0.0, 0.0, 0.0],
                [0.3, 0.0, 0.0, 0.0],
                [10.0, 0.0, 0.0, 0.0],
                [10.1, 0.0, 0.0, 0.0],
                [10.2, 0.0, 0.0, 0.0],
                [10.3, 0.0, 0.0, 0.0],
            ],
            dtype=np.float32,
        )
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )

        pair = evaluate_planner_objective_pair(
            base,
            np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            artifact=artifact,
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[104, 105]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            page_rows=4,
            neighbors=2,
            subspaces=2,
            clusters=4,
            wave1_rank_pages=2,
            wave1_max_span_pages=2,
            wave1_max_ranges=1,
            wave2_top_rows=2,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=8,
            data_page_bytes=32,
        )

        self.assertEqual(
            pair["schema"], "borsuk-v88-planner-objective-screen-v1"
        )
        self.assertEqual(pair["changed_parameter"], "wave1_planner_objective")
        self.assertEqual(pair["arms"]["control"]["objective"], "reciprocal-rank")
        self.assertEqual(pair["arms"]["challenger"]["objective"], "coverage-first")
        self.assertEqual(pair["actual_s3_requests"], 0)
        for arm in pair["arms"].values():
            self.assertEqual(arm["artifact_sha256"], artifact.digest())
            self.assertEqual(
                arm["result"]["artifact_sha256"], artifact.digest()
            )
            self.assertEqual(arm["budgets"]["max_wave1_pages"], 2)
            self.assertEqual(arm["budgets"]["max_wave1_ranges"], 1)
            self.assertEqual(
                arm["rank_cut_base_truth_hits"],
                {"100": 2, "1024": 2, "256": 2, "340": 2, "512": 2},
            )

    def test_budget_validator_reconstructs_ranges_and_rejects_drift(self) -> None:
        result = {
            "samples": [
                {
                    "wave1_bytes": 24,
                    "wave1_gets": 1,
                    "wave1_pages": [2, 3, 4],
                    "wave1_ranges": [[2, 4]],
                    "wave2_bytes": 64,
                    "wave2_gets": 1,
                    "wave2_pages": [7, 8],
                    "wave2_ranges": [[7, 8]],
                }
            ]
        }

        self.assertEqual(
            validate_result_budgets(
                result,
                wave1_page_bytes=8,
                wave1_max_pages=3,
                wave1_max_ranges=1,
                wave2_page_bytes=32,
                wave2_max_pages=2,
                wave2_max_ranges=1,
            ),
            {
                "max_wave1_bytes": 24,
                "max_wave1_pages": 3,
                "max_wave1_ranges": 1,
                "max_wave2_bytes": 64,
                "max_wave2_pages": 2,
                "max_wave2_ranges": 1,
            },
        )
        result["samples"][0]["wave1_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "V88 physical budget differs"):
            validate_result_budgets(
                result,
                wave1_page_bytes=8,
                wave1_max_pages=3,
                wave1_max_ranges=1,
                wave2_page_bytes=32,
                wave2_max_pages=2,
                wave2_max_ranges=1,
            )

    def test_cli_and_runner_freeze_one_burned_dev_spot_cell(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "scripts.v88_planner_objective_screen", "--help"],
            check=True,
            capture_output=True,
            text=True,
        )
        for flag in (
            "--source",
            "--queries",
            "--ground-truth",
            "--layout-order",
            "--output",
        ):
            self.assertIn(flag, completed.stdout)
        self.assertNotIn("--objective", completed.stdout)
        self.assertNotIn("--confirmation", completed.stdout)

        runner = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v88_planner_objective_run_remote.sh")),
                "--describe",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            runner.stdout,
            '{"max_wall_seconds":1200,"queries_per_arm":32,'
            '"spot_only":true,"total_arms":2}\n',
        )


if __name__ == "__main__":
    unittest.main()
