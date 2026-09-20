import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

import scripts.v93_protected_rescue_screen as v93
from scripts.v86_coarse_to_fine_screen import build_coarse_to_fine_artifact
from scripts.v90_residual_row_sketch_screen import build_residual_row_sketch
from scripts.v93_protected_rescue_screen import (
    _record_rescue_code_budgets,
    build_run_metadata,
    code_rescore_rescue_pages,
    evaluate_protected_rescue_arms,
    finalize_decision,
    rank_rescue_pages,
)


class V93ProtectedRescueTests(unittest.TestCase):
    @staticmethod
    def _hit_samples(total: int, base_total: int, truth_total: int) -> list[dict]:
        def distribute(
            value: int, *, floor: int = 0, query_15: int | None = None
        ) -> list[int]:
            values = [floor] * 32
            value -= floor * 32
            if query_15 is not None:
                value += floor
                values[15] = query_15
                value -= query_15
            for index in range(32):
                if index == 15 and query_15 is not None:
                    continue
                take = min(100 - floor, value)
                values[index] += take
                value -= take
            if value:
                raise AssertionError("invalid synthetic hit total")
            return values

        hits = distribute(total, floor=90, query_15=90)
        base_hits = distribute(base_total)
        truth_hits = distribute(truth_total)
        return [
            {
                "delta_truth_hits": 0,
                "page_sq8_base_hits": base_hits[index],
                "page_sq8_hits": hits[index],
                "query": index,
                "wave2_base_truth_page_hits": truth_hits[index],
            }
            for index in range(32)
        ]

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

    def test_four_arm_orchestration_protects_control_and_binds_rescue_evidence(
        self,
    ) -> None:
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
        with mock.patch.multiple(
            v93,
            _CLUSTERS=8,
            _CONTROL_SUBSPACES=1,
            _EXPANDED_WAVE2_MAX_PAGES=3,
            _EXPANDED_WAVE2_MAX_RANGES=3,
            _NEIGHBORS=2,
            _NOMINATION_ROWS=2,
            _PAGE_ROWS=2,
            _RESCUE_PAGE_CAP=2,
            _WAVE1_MAX_RANGES=2,
            _WAVE1_MAX_SPAN_PAGES=2,
            _WAVE1_RANK_PAGES=4,
            _WAVE2_MAX_RANGES=1,
            _WAVE2_MAX_SPAN_PAGES=1,
            _WAVE2_TOP_ROWS=3,
        ):
            result = evaluate_protected_rescue_arms(
                base,
                np.asarray([[40.0, 0.0]], dtype=np.float32),
                np.asarray([[10.0, 0.0]], dtype=np.float32),
                control_artifact=control,
                sketch_artifact=sketch,
                base_ids=np.arange(100, 108, dtype=np.int64),
                delta_ids=np.asarray([999], dtype=np.int64),
                truth_ids=np.asarray([[102, 103]], dtype=np.int64),
                query_ordinals=np.asarray([0], dtype=np.int64),
            )

        self.assertEqual(
            set(result["arms"]),
            {"budget_only", "code_rescue", "control", "direct_rescue"},
        )
        control_pages = set(
            result["arms"]["control"]["result"]["samples"][0]["wave2_pages"]
        )
        for name in ("budget_only", "direct_rescue", "code_rescue"):
            sample = result["arms"][name]["result"]["samples"][0]
            self.assertTrue(control_pages.issubset(sample["wave2_pages"]))
            self.assertLessEqual(len(sample["wave2_ranges"]), 3)
            self.assertEqual(len(result["rescue_sha256"][name]), 64)
        direct_pages = set(
            result["arms"]["direct_rescue"]["result"]["samples"][0][
                "wave2_rescue_pages"
            ]
        )
        code_sample = result["arms"]["code_rescue"]["result"]["samples"][0]
        self.assertTrue(set(code_sample["wave2_rescue_pages"]).issubset(direct_pages))
        self.assertEqual(set(code_sample["rescue_code_pages"]), direct_pages)

    def test_metadata_and_cli_freeze_single_shared_construction_cell(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(metadata["configuration"]["nomination_rows"], 256)
        self.assertEqual(metadata["configuration"]["rescue_page_cap"], 24)
        self.assertEqual(metadata["configuration"]["wave2_max_pages"], 105)
        self.assertEqual(metadata["configuration"]["wave2_max_ranges"], 56)
        self.assertEqual(metadata["configuration"]["whole_query_max_requests"], 112)
        self.assertEqual(metadata["configuration"]["whole_query_max_bytes"], 39_532_864)
        self.assertEqual(
            metadata["planned_max_requests_per_query"],
            {
                "budget_only": 88,
                "code_rescue": 112,
                "control": 64,
                "direct_rescue": 88,
            },
        )
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

    def test_finalize_attributes_pq16_only_beyond_budget_control(self) -> None:
        result = {
            "arms": {
                "control": {"result": {"samples": self._hit_samples(3_167, 2_846, 3_167)}},
                "budget_only": {"result": {"samples": self._hit_samples(3_175, 2_854, 3_180)}},
                "direct_rescue": {"result": {"samples": self._hit_samples(3_180, 2_858, 3_183)}},
                "code_rescue": {"result": {"samples": self._hit_samples(3_181, 2_859, 3_183)}},
            }
        }
        with mock.patch.object(v93, "summarize_development_arm", return_value={}):
            finalize_decision(result)

        self.assertEqual(result["decision"]["accepted"], "direct_rescue")
        self.assertEqual(result["decision"]["total_hit_delta_vs_budget_only"], 5)
        self.assertEqual(result["registered_gate"]["pq16_attribution_margin"], 4)

    def test_finalize_preserves_counterfactual_mismatch_instead_of_aborting(self) -> None:
        result = {
            "arms": {
                "control": {"result": {"samples": self._hit_samples(3_167, 2_846, 3_167)}},
                "budget_only": {"result": {"samples": self._hit_samples(3_175, 2_854, 3_180)}},
                "direct_rescue": {"result": {"samples": self._hit_samples(3_180, 2_858, 3_182)}},
                "code_rescue": {"result": {"samples": self._hit_samples(3_181, 2_859, 3_182)}},
            }
        }
        with mock.patch.object(v93, "summarize_development_arm", return_value={}):
            finalize_decision(result)

        self.assertIsNone(result["decision"]["accepted"])
        self.assertEqual(
            result["decision"]["reason"], "registered-counterfactual-differs"
        )
        self.assertEqual(result["registered_gate"]["observed_direct_candidate_truth_hits"], 3_182)


if __name__ == "__main__":
    unittest.main()
