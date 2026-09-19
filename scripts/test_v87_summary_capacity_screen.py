import subprocess
import sys
import unittest
from pathlib import Path

import numpy as np

from scripts.v86_coarse_to_fine_screen import build_coarse_to_fine_artifact
from scripts.v87_summary_capacity_screen import (
    choose_summary_candidate,
    derive_summary_artifact,
    evaluate_summary_capacity_pair,
    select_development_rows,
    summarize_development_arm,
    validate_control_reproduction,
)


class V87SummaryCapacityTests(unittest.TestCase):
    def test_control_must_reproduce_registered_v86_development_evidence(self) -> None:
        gate = {
            "base_recall_ppm": 985_412,
            "hits": 3_158,
            "passed": False,
            "query_15_hits": 88,
            "recall_ppm": 986_875,
            "worst_query_hits": 88,
        }

        validate_control_reproduction(
            "649c739a81fcb930afdbd15b3692357a45a3106cdb7385aa37e8ed48badd1c56",
            gate,
        )
        gate["hits"] += 1
        with self.assertRaisesRegex(ValueError, "V87 control reproduction differs"):
            validate_control_reproduction(
                "649c739a81fcb930afdbd15b3692357a45a3106cdb7385aa37e8ed48badd1c56",
                gate,
            )

    def test_pair_shares_pq_routing_and_changes_only_page_summaries(self) -> None:
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
        control_artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )
        challenger_artifact = derive_summary_artifact(
            base, control_artifact, summaries_per_page=4
        )

        pair = evaluate_summary_capacity_pair(
            base,
            np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            control_artifact=control_artifact,
            challenger_artifact=challenger_artifact,
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
            pair["schema"],
            "borsuk-v87-fixed-block-summary-capacity-screen-v1",
        )
        self.assertEqual(
            pair["changed_parameter"], "fixed_block_summaries_per_page"
        )
        self.assertEqual(pair["delta_representation"], "production-sq8")
        self.assertEqual(pair["actual_s3_requests"], 0)
        self.assertEqual(pair["routing_sha256"], control_artifact.routing_digest())
        self.assertEqual(
            control_artifact.routing_digest(), challenger_artifact.routing_digest()
        )
        self.assertNotEqual(control_artifact.digest(), challenger_artifact.digest())
        self.assertEqual(pair["arms"]["control"]["summaries_per_page"], 2)
        self.assertEqual(pair["arms"]["challenger"]["summaries_per_page"], 4)
        for arm in pair["arms"].values():
            self.assertEqual(
                arm["artifact_sha256"], arm["result"]["artifact_sha256"]
            )
            self.assertEqual(
                arm["wave1_attribution"],
                {
                    "base_truth_hits": 2,
                    "gap_only_base_truth_hits": 0,
                    "planner_missed_base_truth_hits": 0,
                    "rank_visible_base_truth_hits": 2,
                    "rank_visible_selected_base_truth_hits": 2,
                    "wave1_base_truth_hits": 2,
                },
            )
        self.assertNotEqual(
            pair["arms"]["control"]["result"]["artifact_sha256"],
            pair["arms"]["challenger"]["result"]["artifact_sha256"],
        )

    def test_gate_recomputes_samples_and_rejects_unnecessary_or_failing_summary(
        self,
    ) -> None:
        def arm(*, hits: int, query_15_hits: int, base_hits: int) -> dict:
            samples = []
            for query in range(32):
                samples.append(
                    {
                        "base_truth_hits": 90,
                        "page_sq8_base_hits": base_hits,
                        "page_sq8_hits": query_15_hits if query == 15 else hits,
                        "query": query,
                    }
                )
            return {"samples": samples}

        passing = arm(hits=100, query_15_hits=90, base_hits=90)
        failing = arm(hits=99, query_15_hits=89, base_hits=89)
        self.assertTrue(summarize_development_arm(passing, neighbors=100)["passed"])
        self.assertFalse(summarize_development_arm(failing, neighbors=100)["passed"])
        self.assertEqual(
            choose_summary_candidate(passing, passing, neighbors=100),
            {"accepted": None, "reason": "control-already-passes"},
        )
        self.assertEqual(
            choose_summary_candidate(failing, failing, neighbors=100),
            {"accepted": None, "reason": "challenger-failed-gate"},
        )
        self.assertEqual(
            choose_summary_candidate(failing, passing, neighbors=100),
            {"accepted": "challenger", "reason": "challenger-only-passes"},
        )

    def test_development_selection_uses_prefix_of_authenticated_full_inputs(self) -> None:
        queries = np.arange(328 * 2, dtype=np.float32).reshape(328, 2)
        truth = np.arange(328 * 3, dtype=np.int64).reshape(328, 3)

        selected_queries, selected_truth, ordinals = select_development_rows(
            queries, truth, development_queries=32
        )

        self.assertTrue(np.array_equal(selected_queries, queries[:32]))
        self.assertTrue(np.array_equal(selected_truth, truth[:32]))
        self.assertEqual(ordinals.tolist(), list(range(32)))

    def test_cli_and_runner_are_fixed_to_one_burned_dev_spot_cell(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "scripts.v87_summary_capacity_screen", "--help"],
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
        self.assertNotIn("--summaries-per-page", completed.stdout)
        self.assertNotIn("--confirmation", completed.stdout)

        runner = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v87_summary_capacity_run_remote.sh")),
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
        accepted = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v87_summary_capacity_run_remote.sh")),
                "--classify-lifecycle",
                "spot",
            ],
            capture_output=True,
            text=True,
        )
        rejected = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v87_summary_capacity_run_remote.sh")),
                "--classify-lifecycle",
                "on-demand",
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(accepted.returncode, 0)
        self.assertEqual(accepted.stdout, "spot\n")
        self.assertNotEqual(rejected.returncode, 0)


if __name__ == "__main__":
    unittest.main()
