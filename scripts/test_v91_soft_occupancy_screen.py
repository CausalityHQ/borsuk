import dataclasses
import hashlib
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

import scripts.v91_soft_occupancy_screen as v91
from scripts.v86_coarse_to_fine_screen import (
    _select_optimal_weighted_pages,
    build_coarse_to_fine_artifact,
    evaluate_coarse_to_fine,
)
from scripts.v90_residual_row_sketch_screen import ResidualRowSketchArtifact
from scripts.v91_soft_occupancy_screen import (
    _REGISTERED_RESIDUAL_SKETCH_SHA256,
    _SOFT_MASS_SCALE,
    _compound_soft_page_weights,
    _validate_soft_evidence,
    build_run_metadata,
    evaluate_soft_occupancy_pair,
    residual_soft_page_weights,
    soft_top100_page_weights,
)


class V91SoftOccupancyTests(unittest.TestCase):
    def test_soft_mass_uses_registered_rank_as_strictly_lower_order_fallback(
        self,
    ) -> None:
        scores = np.asarray([3.0, 0.0, 2.0, 1.0, 4.0], dtype=np.float32)
        mass = np.asarray([1, 0, 0, 0, 0], dtype=np.uint64)

        combined = _compound_soft_page_weights(
            mass,
            scores,
            rank_limit=4,
            max_span_pages=2,
        )

        self.assertGreater(int(combined[1]), int(combined[2]))
        self.assertGreater(int(combined[2]), 0)
        self.assertEqual(int(combined[4]), 0)
        self.assertGreater(int(combined[0]), int(combined[[1, 2]].sum()))
        reciprocal = _compound_soft_page_weights(
            np.zeros(2, dtype=np.uint64),
            np.asarray([0.0, 1.0], dtype=np.float32),
            rank_limit=2,
            max_span_pages=2,
        )
        self.assertEqual(reciprocal.tolist(), [1_000_000_000, 500_000_000])
        with mock.patch.object(v91, "_SOFT_PRIMARY_MULTIPLIER", 16):
            with self.assertRaisesRegex(ValueError, "V91 soft evidence differs"):
                _compound_soft_page_weights(
                    np.zeros(2, dtype=np.uint64),
                    np.asarray([0.0, 1.0], dtype=np.float32),
                    rank_limit=2,
                    max_span_pages=2,
                )

    def test_soft_evidence_rejects_mass_loss_and_positive_weight_outside_fence(
        self,
    ) -> None:
        scores = np.asarray([[0.0, 1.0, 2.0]], dtype=np.float32)
        valid_mass = np.asarray(
            [[100 * _SOFT_MASS_SCALE, 0, 0]], dtype=np.uint64
        )

        _validate_soft_evidence(scores, valid_mass, rank_limit=2)
        with self.assertRaisesRegex(ValueError, "V91 soft evidence differs"):
            _validate_soft_evidence(
                scores,
                np.asarray([[1, 0, 1]], dtype=np.uint64),
                rank_limit=2,
            )
        with self.assertRaisesRegex(ValueError, "V91 soft evidence differs"):
            _validate_soft_evidence(
                scores,
                np.asarray([[100 * _SOFT_MASS_SCALE - 4, 0, 0]], dtype=np.uint64),
                rank_limit=2,
            )

    def test_one_soft_mass_quantum_dominates_maximum_340_page_fallback(self) -> None:
        scores = np.arange(1_024, dtype=np.float32)
        mass = np.zeros(1_024, dtype=np.uint64)
        mass[-1] = 1

        combined = _compound_soft_page_weights(
            mass,
            scores,
            rank_limit=1_024,
            max_span_pages=340,
        )
        pages, _ = _select_optimal_weighted_pages(
            combined,
            max_span_pages=340,
            max_ranges=2,
        )

        self.assertIn(1_023, pages.tolist())

    def test_registered_sketch_authority_is_exact(self) -> None:
        self.assertEqual(
            _REGISTERED_RESIDUAL_SKETCH_SHA256,
            "21aae14867749ac8dfed26043f8cb225024d4928bb81db32d488dd0c23a5a3bd",
        )

    def test_soft_occupancy_preserves_multiplicity_instead_of_only_best_row(self) -> None:
        estimates = np.full(256, np.float32(100.0), dtype=np.float32)
        estimates[0] = np.float32(0.0)
        estimates[128:228] = np.float32(0.01)
        pages = np.repeat(np.asarray([0, 1], dtype=np.int64), 128)

        weights = soft_top100_page_weights(
            estimates,
            pages,
            np.ones(256, dtype=bool),
            page_count=2,
        )

        self.assertGreater(int(weights[1]), int(weights[0]))
        self.assertGreaterEqual(int(weights.sum()), 100 * _SOFT_MASS_SCALE - 2)
        self.assertLessEqual(int(weights.sum()), 100 * _SOFT_MASS_SCALE)

    def test_soft_occupancy_is_deterministic_for_equal_distances(self) -> None:
        estimates = np.ones(200, dtype=np.float32)
        pages = np.repeat(np.asarray([0, 1], dtype=np.int64), 100)

        first = soft_top100_page_weights(
            estimates,
            pages,
            np.ones(200, dtype=bool),
            page_count=2,
        )
        second = soft_top100_page_weights(
            estimates.copy(),
            pages.copy(),
            np.ones(200, dtype=bool),
            page_count=2,
        )

        self.assertTrue(np.array_equal(first, second))
        self.assertEqual(first.tolist(), [50 * _SOFT_MASS_SCALE, 50 * _SOFT_MASS_SCALE])

    def test_soft_occupancy_excludes_padded_tail_rows_and_pages_outside_fence(self) -> None:
        estimates = np.concatenate(
            (np.arange(200, dtype=np.float32), np.asarray([-1_000.0], dtype=np.float32))
        )
        pages = np.concatenate(
            (np.repeat(np.asarray([1, 3], dtype=np.int64), 100), np.asarray([4]))
        )
        valid = np.ones(201, dtype=bool)
        valid[-1] = False

        weights = soft_top100_page_weights(
            estimates, pages, valid, page_count=6
        )

        self.assertEqual(int(weights[0]), 0)
        self.assertEqual(int(weights[2]), 0)
        self.assertEqual(int(weights[4]), 0)
        self.assertEqual(int(weights[5]), 0)
        self.assertGreater(int(weights[1]), 0)

    def test_soft_occupancy_rejects_nonfinite_valid_estimates(self) -> None:
        estimates = np.arange(200, dtype=np.float32)
        estimates[17] = np.float32(np.nan)

        with self.assertRaisesRegex(ValueError, "V91 soft occupancy authority differs"):
            soft_top100_page_weights(
                estimates,
                np.zeros(200, dtype=np.int64),
                np.ones(200, dtype=bool),
                page_count=1,
            )

    def test_residual_soft_weights_keep_only_control_fence_and_row_multiplicity(
        self,
    ) -> None:
        base = np.zeros((600, 2), dtype=np.float32)
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=200,
            subspaces=1,
            clusters=4,
            sample_rows=600,
            seed=86,
            iterations=1,
        )
        sketch = ResidualRowSketchArtifact(
            base_vectors_sha256=hashlib.sha256(base.tobytes()).hexdigest(),
            books=(
                np.asarray(
                    [[0.0, 0.0], [0.1, 0.0], [10.0, 0.0], [20.0, 0.0]],
                    dtype=np.float32,
                ),
            ),
            control_artifact_sha256=control.digest(),
            page_rows=200,
            row_codes=np.concatenate(
                (
                    np.full((200, 1), 2, dtype=np.uint8),
                    np.full((200, 1), 1, dtype=np.uint8),
                    np.zeros((200, 1), dtype=np.uint8),
                )
            ),
            training_rows=600,
        )

        weights = residual_soft_page_weights(
            np.zeros(2, dtype=np.float32),
            np.asarray([0.0, 1.0, 2.0], dtype=np.float32),
            control,
            sketch,
            rank_limit=2,
            validated_control_sha256=control.digest(),
        )

        self.assertEqual(int(weights[2]), 0)
        self.assertGreater(int(weights[1]), int(weights[0]))

    def test_pair_uses_soft_weights_as_the_only_changed_wave_one_evidence(self) -> None:
        base = np.zeros((400, 2), dtype=np.float32)
        base[200:, 0] = np.float32(10.0)
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=200,
            subspaces=1,
            clusters=4,
            sample_rows=400,
            seed=86,
            iterations=1,
        )
        control = dataclasses.replace(
            control,
            summary_codes=np.zeros_like(control.summary_codes),
        )
        sketch = ResidualRowSketchArtifact(
            base_vectors_sha256=hashlib.sha256(base.tobytes()).hexdigest(),
            books=(
                np.asarray(
                    [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0], [30.0, 0.0]],
                    dtype=np.float32,
                ),
            ),
            control_artifact_sha256=control.digest(),
            page_rows=200,
            row_codes=np.concatenate(
                (
                    np.full((200, 1), 3, dtype=np.uint8),
                    np.zeros((200, 1), dtype=np.uint8),
                )
            ),
            training_rows=400,
        )

        pair_args = {
            "control_artifact": control,
            "sketch_artifact": sketch,
            "base_ids": np.arange(100, 500, dtype=np.int64),
            "delta_ids": np.asarray([999], dtype=np.int64),
            "truth_ids": np.asarray([[300]], dtype=np.int64),
            "query_ordinals": np.asarray([0], dtype=np.int64),
            "page_rows": 200,
            "neighbors": 1,
            "control_subspaces": 1,
            "control_clusters": 4,
            "wave1_rank_pages": 2,
            "wave1_max_span_pages": 1,
            "wave1_max_ranges": 1,
            "wave2_top_rows": 1,
            "wave2_max_span_pages": 1,
            "wave2_max_ranges": 1,
            "code_page_bytes": 200,
            "data_page_bytes": 1_600,
        }
        pair = evaluate_soft_occupancy_pair(
            base,
            np.asarray([[40.0, 0.0]], dtype=np.float32),
            np.asarray([[10.0, 0.0]], dtype=np.float32),
            **pair_args,
        )

        self.assertEqual(pair["schema"], "borsuk-v91-soft-occupancy-screen-v1")
        self.assertEqual(len(pair["candidate_fence_sha256"]), 64)
        self.assertEqual(
            pair["arms"]["challenger"]["page_evidence"],
            "residual-pq16-soft-top100-reciprocal-rank-fallback",
        )
        self.assertEqual(pair["arms"]["challenger"]["result"]["samples"][0]["wave1_pages"], [1])
        self.assertEqual(pair["arms"]["challenger"]["result"]["page_sq8_recall_ppm"], 1_000_000)
        self.assertEqual(len(pair["row_min_scores_sha256"]), 64)
        self.assertEqual(len(pair["soft_page_mass_sha256"]), 64)
        self.assertEqual(len(pair["soft_page_weights_sha256"]), 64)
        self.assertEqual(len(pair["soft_reducer_diagnostics"]), 1)
        with mock.patch.object(
            v91,
            "_validate_soft_evidence",
            side_effect=ValueError("V91 soft evidence sentinel"),
        ):
            with self.assertRaisesRegex(ValueError, "V91 soft evidence sentinel"):
                evaluate_soft_occupancy_pair(
                    base,
                    np.asarray([[40.0, 0.0]], dtype=np.float32),
                    np.asarray([[10.0, 0.0]], dtype=np.float32),
                    **pair_args,
                )

    def test_metadata_and_runner_freeze_one_serial_burned_dev_cell(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(metadata["configuration"]["soft_target_rows"], 100)
        self.assertEqual(metadata["configuration"]["soft_temperature_rows"], 200)
        self.assertEqual(metadata["configuration"]["soft_bisection_iterations"], 48)
        self.assertEqual(
            metadata["configuration"]["soft_fallback_objective"],
            "reciprocal-rank",
        )
        self.assertEqual(metadata["configuration"]["soft_mass_scale"], 1 << 12)
        self.assertEqual(metadata["configuration"]["soft_primary_multiplier"], 1 << 33)
        self.assertEqual(metadata["configuration"]["query_parallelism"], 1)
        self.assertFalse(metadata["configuration"]["rayon_work_stealing"])
        self.assertEqual(metadata["projection_100m"]["resident_bytes"], 2_057_023_104)
        self.assertEqual(metadata["projection_100m"]["added_resident_bytes"], 0)
        self.assertFalse(metadata["projection_100m"]["serving_cpu_qualified"])

        completed = subprocess.run(
            [sys.executable, "-m", "scripts.v91_soft_occupancy_screen", "--help"],
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
        self.assertNotIn("--temperature", completed.stdout)
        self.assertNotIn("--target-mass", completed.stdout)

        runner = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v91_soft_occupancy_run_remote.sh")),
                "--describe",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            runner.stdout,
            '{"blas_threads":16,"max_wall_seconds":1200,"queries_per_arm":32,'
            '"query_parallelism":1,"rayon_work_stealing":false,'
            '"sketch_bytes_per_base_row":16,"spot_only":true,'
            '"total_arms":2,"worker_model":"fixed-16-thread-blas"}\n',
        )

    def test_evaluator_delivers_direct_page_weights_without_rank_conversion(self) -> None:
        base = np.asarray(
            [[0.0], [0.1], [10.0], [10.1], [20.0], [20.1]], dtype=np.float32
        )
        delta = np.asarray([[30.0]], dtype=np.float32)
        query = np.asarray([[10.0]], dtype=np.float32)
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=6,
            sample_rows=6,
            seed=86,
            iterations=1,
        )
        direct_weights = np.asarray([[1, 100, 0]], dtype=np.uint64)

        result = evaluate_coarse_to_fine(
            base,
            delta,
            query,
            base_ids=np.arange(100, 106, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[102]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            artifact=artifact,
            page_rows=2,
            neighbors=1,
            subspaces=1,
            clusters=6,
            wave1_rank_pages=3,
            wave1_max_span_pages=1,
            wave1_max_ranges=1,
            wave2_top_rows=1,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=2,
            data_page_bytes=8,
            wave1_page_weights_by_query=direct_weights,
            wave1_page_evidence_sha256="1" * 64,
        )

        self.assertEqual(result["samples"][0]["wave1_pages"], [1])
        self.assertEqual(result["page_sq8_recall_ppm"], 1_000_000)

    def test_evaluator_keeps_fence_ranks_separate_from_direct_page_utility(self) -> None:
        base = np.arange(8, dtype=np.float32).reshape(8, 1)
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=8,
            sample_rows=8,
            seed=86,
            iterations=1,
        )

        result = evaluate_coarse_to_fine(
            base,
            np.asarray([[20.0]], dtype=np.float32),
            np.asarray([[0.0]], dtype=np.float32),
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[104]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            artifact=artifact,
            page_rows=2,
            neighbors=1,
            subspaces=1,
            clusters=8,
            wave1_rank_pages=2,
            wave1_max_span_pages=1,
            wave1_max_ranges=1,
            wave2_top_rows=1,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=2,
            data_page_bytes=8,
            wave1_page_scores_by_query=np.asarray(
                [[2.0, 3.0, 0.0, 1.0]], dtype=np.float32
            ),
            wave1_page_weights_by_query=np.asarray(
                [[0, 0, 0, 100]], dtype=np.uint64
            ),
            wave1_page_evidence_sha256="2" * 64,
        )

        sample = result["samples"][0]
        self.assertEqual(sample["wave1_pages"], [3])
        self.assertEqual(sample["truth_page_ranks"], [0])
        self.assertEqual(sample["wave1_rank_visible_base_truth_hits"], 1)

    def test_direct_additive_mass_can_choose_a_different_physical_span_than_rank_utility(
        self,
    ) -> None:
        base = np.arange(8, dtype=np.float32).reshape(8, 1)
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=8,
            sample_rows=8,
            seed=86,
            iterations=1,
        )

        result = evaluate_coarse_to_fine(
            base,
            np.asarray([[20.0]], dtype=np.float32),
            np.asarray([[0.0]], dtype=np.float32),
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[100]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            artifact=artifact,
            page_rows=2,
            neighbors=1,
            subspaces=1,
            clusters=8,
            wave1_rank_pages=4,
            wave1_max_span_pages=2,
            wave1_max_ranges=1,
            wave2_top_rows=1,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=2,
            data_page_bytes=8,
            wave1_page_scores_by_query=np.asarray(
                [[1.0, 2.0, 3.0, 0.0]], dtype=np.float32
            ),
            wave1_page_weights_by_query=np.asarray(
                [[60, 60, 0, 100]], dtype=np.uint64
            ),
            wave1_page_evidence_sha256="3" * 64,
        )

        self.assertEqual(result["samples"][0]["wave1_pages"], [0, 1])
        self.assertEqual(result["wave1_objective"], "direct-page-weight")


if __name__ == "__main__":
    unittest.main()
