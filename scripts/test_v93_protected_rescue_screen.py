import subprocess
import sys
import unittest
from pathlib import Path

import numpy as np

from scripts.v86_coarse_to_fine_screen import build_coarse_to_fine_artifact
from scripts.v93_protected_rescue_screen import (
    _record_rescue_code_budgets,
    build_run_metadata,
    code_rescore_rescue_pages,
    rank_rescue_pages,
)


class V93ProtectedRescueTests(unittest.TestCase):
    def test_code_budget_charges_every_nomination_even_when_data_page_is_rejected(
        self,
    ) -> None:
        samples = [
            {
                "wave1_bytes": 100,
                "wave1_gets": 2,
                "wave1_pages": [0, 1],
                "wave2_bytes": 200,
                "wave2_gets": 3,
            }
        ]

        maxima = _record_rescue_code_budgets(
            samples,
            [np.asarray([8, 4], dtype=np.int64)],
            code_page_bytes=10,
        )

        self.assertEqual(samples[0]["rescue_code_pages"], [4, 8])
        self.assertEqual(samples[0]["rescue_code_ranges"], [[4, 4], [8, 8]])
        self.assertEqual(samples[0]["rescue_code_bytes"], 20)
        self.assertEqual(samples[0]["rescue_code_gets"], 2)
        self.assertEqual(maxima["max_total_bytes"], 320)
        self.assertEqual(maxima["max_total_requests"], 7)

    def test_rank_rescue_pages_aggregates_support_and_protects_baseline(self) -> None:
        pages = rank_rescue_pages(
            np.asarray([8, 9, 4, 10, 5, 6], dtype=np.int64),
            baseline_pages=np.asarray([2], dtype=np.int64),
            page_rows=2,
            page_count=8,
            take_rows=6,
            max_pages=2,
        )

        self.assertEqual(pages.tolist(), [4, 5])

    def test_code_rescore_uses_pq192_before_selecting_rescue_data_pages(self) -> None:
        base = np.asarray(
            [[0.0], [0.1], [10.0], [10.1], [20.0], [20.1]],
            dtype=np.float32,
        )
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=6,
            sample_rows=6,
            seed=86,
            iterations=1,
        )

        pages = code_rescore_rescue_pages(
            np.asarray([20.0], dtype=np.float32),
            artifact,
            wave1_pages=np.asarray([0], dtype=np.int64),
            baseline_wave2_pages=np.asarray([0], dtype=np.int64),
            nominated_rescue_pages=np.asarray([1, 2], dtype=np.int64),
            rows=6,
            page_rows=2,
            top_rows=2,
            max_pages=1,
        )

        self.assertEqual(pages.tolist(), [2])

    def test_metadata_and_cli_freeze_single_shared_construction_cell(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(metadata["configuration"]["nomination_rows"], 256)
        self.assertEqual(metadata["configuration"]["rescue_page_cap"], 24)
        self.assertEqual(metadata["configuration"]["wave2_max_pages"], 105)
        self.assertEqual(metadata["configuration"]["wave2_max_ranges"], 56)
        self.assertEqual(metadata["configuration"]["whole_query_max_requests"], 112)
        self.assertEqual(metadata["configuration"]["whole_query_max_bytes"], 39_532_864)
        self.assertEqual(metadata["configuration"]["query_parallelism"], 1)
        self.assertFalse(metadata["configuration"]["rayon_work_stealing"])

        completed = subprocess.run(
            [sys.executable, "-m", "scripts.v93_protected_rescue_screen", "--help"],
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
        self.assertNotIn("--rescue-page-cap", completed.stdout)
        self.assertNotIn("--nomination-rows", completed.stdout)

        runner = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v93_protected_rescue_run_remote.sh")),
                "--describe",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            runner.stdout,
            '{"blas_threads":16,"max_wall_seconds":1200,'
            '"nomination_rows":256,"queries":32,"query_parallelism":1,'
            '"rayon_work_stealing":false,"rescue_page_cap":24,'
            '"spot_only":true,"total_arms":4,'
            '"worker_model":"fixed-16-thread-blas"}\n',
        )


if __name__ == "__main__":
    unittest.main()
