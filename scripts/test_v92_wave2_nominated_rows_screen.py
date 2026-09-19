import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

import scripts.v92_wave2_nominated_rows_screen as v92
from scripts.v86_coarse_to_fine_screen import build_coarse_to_fine_artifact
from scripts.v90_residual_row_sketch_screen import build_residual_row_sketch
from scripts.v92_wave2_nominated_rows_screen import (
    build_run_metadata,
    evaluate_wave2_nominated_pair,
    nominate_unselected_rows,
    nomination_reachability,
)


class V92WaveTwoNominatedRowsTests(unittest.TestCase):
    @staticmethod
    def _pair(minimum_reachable_missed_truth: int) -> dict[str, object]:
        base = np.asarray(
            [
                [0.0, 0.0],
                [0.1, 0.0],
                [10.0, 0.0],
                [10.1, 0.0],
                [20.0, 0.0],
                [20.1, 0.0],
                [30.0, 0.0],
                [30.1, 0.0],
            ],
            dtype=np.float32,
        )
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=8,
            sample_rows=8,
            seed=86,
            iterations=1,
        )
        sketch = build_residual_row_sketch(
            base,
            control,
            subspaces=1,
            clusters=8,
            sample_rows=8,
            seed=90,
            iterations=1,
        )
        return evaluate_wave2_nominated_pair(
            base,
            np.asarray([[40.0, 0.0]], dtype=np.float32),
            np.asarray([[10.0, 0.0]], dtype=np.float32),
            control_artifact=control,
            sketch_artifact=sketch,
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[102, 103]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            page_rows=2,
            neighbors=2,
            control_subspaces=1,
            control_clusters=8,
            wave1_rank_pages=4,
            wave1_max_span_pages=1,
            wave1_max_ranges=1,
            wave2_top_rows=1,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            nomination_rows=2,
            minimum_reachable_missed_truth=minimum_reachable_missed_truth,
            code_page_bytes=2,
            data_page_bytes=16,
        )

    def test_nominations_rank_only_valid_rows_from_unselected_fence_pages(self) -> None:
        candidates = np.asarray([2, 0, 1], dtype=np.int64)
        estimates = np.asarray(
            [[0.4, 0.1], [0.0, 0.2], [0.3, np.inf]], dtype=np.float32
        )
        valid = np.isfinite(estimates)

        nominations = nominate_unselected_rows(
            candidates,
            estimates,
            valid,
            selected_pages=np.asarray([0], dtype=np.int64),
            page_rows=2,
            rows=5,
            take=3,
        )

        self.assertEqual(nominations.tolist(), [2, 4])

    def test_reachability_counts_only_truth_missing_from_wave_one(self) -> None:
        diagnostics = nomination_reachability(
            nominations=np.asarray([4, 5, 7, 9], dtype=np.int64),
            truth_positions=np.asarray([0, 4, 5, 8, 9], dtype=np.int64),
            selected_pages=np.asarray([0, 4], dtype=np.int64),
            page_rows=2,
        )

        self.assertEqual(diagnostics["missed_truth_rows"], 2)
        self.assertEqual(diagnostics["nominated_missed_truth_rows"], 2)
        self.assertEqual(diagnostics["missed_truth_nomination_ranks"], [0, 1])

    def test_pair_changes_only_wave_two_nominations_after_frozen_wave_one(self) -> None:
        pair = self._pair(0)

        self.assertEqual(pair["schema"], "borsuk-v92-wave2-nominated-rows-screen-v1")
        self.assertEqual(pair["changed_parameter"], "wave2_page_evidence")
        self.assertEqual(len(pair["wave2_nomination_sha256"]), 64)
        self.assertEqual(pair["decision"]["reason"], "reachability-floor-passed")
        control = pair["arms"]["control"]["result"]
        challenger = pair["arms"]["challenger"]["result"]
        self.assertEqual(
            control["samples"][0]["wave1_pages"],
            challenger["samples"][0]["wave1_pages"],
        )
        self.assertEqual(
            challenger["wave2_nomination_sha256"],
            pair["wave2_nomination_sha256"],
        )
        nominations = challenger["samples"][0]["wave2_nominated_rows"]
        selected = set(control["samples"][0]["wave1_pages"])
        self.assertEqual(len(nominations), 2)
        self.assertTrue(all(position // 2 not in selected for position in nominations))

    def test_pair_stops_before_challenger_when_reachability_floor_fails(self) -> None:
        with mock.patch.object(
            v92,
            "evaluate_coarse_to_fine",
            side_effect=AssertionError("science evaluator must remain fenced"),
        ):
            pair = self._pair(99)

        self.assertEqual(pair["arms"], {})
        self.assertEqual(pair["decision"]["reason"], "reachability-floor-failed")
        self.assertFalse(pair["reachability"]["passed"])

    def test_metadata_and_cli_freeze_one_serial_fail_fast_cell(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(metadata["configuration"]["nomination_rows"], 512)
        self.assertEqual(
            metadata["configuration"]["minimum_reachable_missed_truth"], 8
        )
        self.assertEqual(metadata["configuration"]["query_parallelism"], 1)
        self.assertFalse(metadata["configuration"]["rayon_work_stealing"])
        self.assertEqual(metadata["projection_100m"]["added_resident_bytes"], 0)
        self.assertFalse(metadata["projection_100m"]["serving_cpu_qualified"])

        completed = subprocess.run(
            [sys.executable, "-m", "scripts.v92_wave2_nominated_rows_screen", "--help"],
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
        self.assertNotIn("--nomination-rows", completed.stdout)
        self.assertNotIn("--reachability-floor", completed.stdout)

        runner = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v92_wave2_nominated_rows_run_remote.sh")),
                "--describe",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            runner.stdout,
            '{"blas_threads":16,"max_wall_seconds":1200,'
            '"minimum_reachable_missed_truth":8,"nomination_rows":512,'
            '"queries_per_arm":32,"query_parallelism":1,'
            '"rayon_work_stealing":false,"sketch_bytes_per_base_row":16,'
            '"spot_only":true,"worker_model":"fixed-16-thread-blas"}\n',
        )


if __name__ == "__main__":
    unittest.main()
