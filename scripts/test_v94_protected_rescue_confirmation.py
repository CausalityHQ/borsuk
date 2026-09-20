import ast
import subprocess
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

import scripts.v93_protected_rescue_screen as v93
import scripts.v94_protected_rescue_confirmation as v94
from scripts.v86_coarse_to_fine_screen import build_coarse_to_fine_artifact
from scripts.v90_residual_row_sketch_screen import build_residual_row_sketch
from scripts.v94_protected_rescue_confirmation import (
    build_run_metadata,
    evaluate_confirmation_arms,
    finalize_confirmation,
    select_confirmation_rows,
    summarize_confirmation_arm,
)


class V94ProtectedRescueConfirmationTests(unittest.TestCase):
    @staticmethod
    def _confirmation_result(control_hits: int, direct_hits: int) -> dict:
        def samples(total: int) -> list[dict]:
            values = [99] * 128
            remaining = total - sum(values)
            for index in range(remaining):
                values[index] += 1
            return [
                {
                    "base_truth_hits": 100,
                    "page_sq8_base_hits": 100,
                    "page_sq8_hits": value,
                    "query": 328 + index,
                }
                for index, value in enumerate(values)
            ]

        return {
            "arms": {
                "control": {"result": {"samples": samples(control_hits)}},
                "direct_rescue": {"result": {"samples": samples(direct_hits)}},
            }
        }

    def test_registered_python39_runtime_does_not_use_zip_strict(self) -> None:
        tree = ast.parse(Path(v94.__file__).read_text(encoding="utf-8"))
        unsupported_calls = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "zip"
            and any(keyword.arg == "strict" for keyword in node.keywords)
        ]

        self.assertEqual(unsupported_calls, [])

    def test_confirmation_requires_four_hit_benefit_beyond_control(self) -> None:
        too_small = self._confirmation_result(12_685, 12_688)
        finalize_confirmation(too_small)
        self.assertFalse(too_small["decision"]["passed"])
        self.assertEqual(too_small["decision"]["total_hit_delta_vs_control"], 3)

        sufficient = self._confirmation_result(12_685, 12_689)
        finalize_confirmation(sufficient)
        self.assertTrue(sufficient["decision"]["passed"])
        self.assertEqual(sufficient["decision"]["total_hit_delta_vs_control"], 4)

    def test_runner_freezes_two_arm_confirmation_without_work_stealing(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(
            metadata["confirmation"],
            {
                "end_exclusive": 456,
                "queries": 128,
                "start": 328,
                "tuning_allowed": False,
            },
        )
        self.assertEqual(
            metadata["planned_max_requests_per_query"],
            {"control": 64, "direct_rescue": 88},
        )
        runner = subprocess.run(
            [
                "bash",
                str(
                    Path(__file__).with_name(
                        "v94_protected_rescue_confirmation_run_remote.sh"
                    )
                ),
                "--describe",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            runner.stdout,
            '{"blas_threads":16,"confirmation_end_exclusive":456,'
            '"confirmation_start":328,"instance_type":"c7i.8xlarge",'
            '"max_wall_seconds":1800,'
            '"queries":128,"query_parallelism":1,"rayon_work_stealing":false,'
            '"spot_only":true,"total_arms":2,'
            '"worker_model":"fixed-16-thread-blas"}\n',
        )

    def test_selection_uses_only_the_frozen_confirmation_ordinals(self) -> None:
        queries = np.arange(500 * 2, dtype=np.float32).reshape(500, 2)
        truth = np.arange(500 * 3, dtype=np.int64).reshape(500, 3)

        selected_queries, selected_truth, ordinals = select_confirmation_rows(
            queries,
            truth,
            confirmation_start=328,
            confirmation_queries=128,
        )

        self.assertEqual(ordinals.tolist(), list(range(328, 456)))
        np.testing.assert_array_equal(selected_queries, queries[328:456])
        np.testing.assert_array_equal(selected_truth, truth[328:456])

    def test_summary_applies_the_frozen_confirmation_gate(self) -> None:
        samples = [
            {
                "base_truth_hits": 100,
                "page_sq8_base_hits": 100 if index < 117 else 90,
                "page_sq8_hits": 100 if index < 13 else 99,
                "query": 328 + index,
            }
            for index in range(128)
        ]

        summary = summarize_confirmation_arm({"samples": samples}, neighbors=100)

        self.assertEqual(summary["hits"], 12_685)
        self.assertEqual(summary["recall_ppm"], 991_016)
        self.assertEqual(summary["worst_query_hits"], 99)
        self.assertGreaterEqual(summary["base_recall_ppm"], 991_000)
        self.assertTrue(summary["passed"])

    def test_confirmation_evaluates_only_control_and_frozen_direct_rescue(self) -> None:
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
            result = evaluate_confirmation_arms(
                base,
                np.asarray([[40.0, 0.0]], dtype=np.float32),
                np.asarray([[10.0, 0.0]], dtype=np.float32),
                control_artifact=control,
                sketch_artifact=sketch,
                base_ids=np.arange(100, 108, dtype=np.int64),
                delta_ids=np.asarray([999], dtype=np.int64),
                truth_ids=np.asarray([[102, 103]], dtype=np.int64),
                query_ordinals=np.asarray([328], dtype=np.int64),
            )

        self.assertEqual(set(result["arms"]), {"control", "direct_rescue"})
        self.assertEqual(
            result["schema"], "borsuk-v94-protected-rescue-confirmation-v1"
        )


if __name__ == "__main__":
    unittest.main()
