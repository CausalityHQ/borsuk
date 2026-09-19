import dataclasses
import subprocess
import sys
import unittest
from pathlib import Path

import numpy as np

from scripts.v86_coarse_to_fine_screen import (
    build_coarse_to_fine_artifact,
    plan_ranked_pages,
)
from scripts.v89_calibrated_planner_screen import (
    _exact_neighbor_positions,
    _squared_l2_scores,
    build_run_metadata,
    evaluate_calibrated_pair,
    fit_monotone_rank_curve,
    fit_page_rank_calibration,
    rank_bins,
    select_calibration_positions,
)


class V89CalibratedPlannerTests(unittest.TestCase):
    def test_exact_calibration_truth_breaks_boundary_ties_by_row_id(self) -> None:
        scores = np.asarray([0.0, 1.0, 1.0, 2.0], dtype=np.float32)
        ids = np.asarray([50, 99, 1, 7], dtype=np.int64)

        positions = _exact_neighbor_positions(scores, ids, neighbors=2)

        self.assertEqual(ids[positions].tolist(), [50, 1])

    def test_chunked_calibration_distance_matches_direct_float32_l2(self) -> None:
        vectors = np.arange(60, dtype=np.float32).reshape(15, 4) / np.float32(7.0)
        query = np.asarray([1.0, 2.0, 3.0, 4.0], dtype=np.float32)
        expected = np.einsum("ij,ij->i", vectors - query[None, :], vectors - query[None, :])

        actual = _squared_l2_scores(vectors, query, chunk_rows=4)

        self.assertTrue(np.array_equal(actual, expected))

    def test_metadata_discloses_burned_scope_and_unqualified_scale_cost(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(metadata["configuration"]["calibration_rows"], 128)
        self.assertEqual(
            metadata["configuration"]["calibration_source"], "resident-delta"
        )
        self.assertEqual(metadata["configuration"]["queries"], 32)
        self.assertFalse(metadata["configuration"]["uses_query_or_truth_labels"])
        self.assertFalse(metadata["projection_100m"]["calibration_cpu_qualified"])
        self.assertFalse(metadata["projection_100m"]["serving_memory_qualified"])
        self.assertEqual(
            metadata["scope"], "fixed-1m-burned-development-calibrated-planner-falsifier"
        )

    def test_hash_selected_calibration_rows_bind_ids_not_physical_order(self) -> None:
        ids = np.asarray([101, 7, 55, 2, 900, 44, 3, 88], dtype=np.int64)
        first = select_calibration_positions(ids, count=4, seed=89)
        permutation = np.asarray([4, 1, 7, 0, 3, 6, 2, 5], dtype=np.int64)
        second = select_calibration_positions(ids[permutation], count=4, seed=89)

        self.assertEqual(
            sorted(ids[first].tolist()), sorted(ids[permutation][second].tolist())
        )
        self.assertEqual(first.size, 4)
        self.assertEqual(np.unique(first).size, 4)

    def test_rank_bins_are_fixed_log2_intervals(self) -> None:
        ranks = np.asarray(
            [0, 1, 2, 3, 4, 7, 8, 15, 16, 511, 512, 1023, 1024, 2048, 4095],
            dtype=np.int64,
        )

        self.assertEqual(
            rank_bins(ranks).tolist(),
            [0, 1, 2, 3, 4, 7, 8, 15, 16, 67, 68, 83, 84, 84, 84],
        )

    def test_weighted_isotonic_curve_is_monotone_and_integer_authoritative(
        self,
    ) -> None:
        curve = fit_monotone_rank_curve(
            np.asarray([10, 2, 6, 0], dtype=np.int64),
            np.asarray([10, 10, 10, 10], dtype=np.int64),
            scale=1_000_000_000,
        )

        self.assertEqual(curve.tolist(), [1_000_000_000, 400_000_000, 400_000_000, 0])
        self.assertTrue(np.all(curve[:-1] >= curve[1:]))
        with self.assertRaisesRegex(ValueError, "rank calibration differs"):
            fit_monotone_rank_curve(
                np.asarray([-1, 2], dtype=np.int64),
                np.asarray([1, 1], dtype=np.int64),
                scale=1_000_000_000,
            )

    def test_calibrated_planner_uses_fixed_rank_weights_at_same_budget(self) -> None:
        scores = np.asarray([0.0, 9.0, 9.0, 1.0, 2.0], dtype=np.float32)

        pages, ranges = plan_ranked_pages(
            scores,
            rank_limit=5,
            max_span_pages=2,
            max_ranges=1,
            objective="calibrated",
            calibrated_rank_weights=np.asarray([10, 9, 8, 0, 0], dtype=np.uint64),
        )

        self.assertEqual(pages.tolist(), [3, 4])
        self.assertEqual(ranges, [(3, 4)])
        with self.assertRaisesRegex(ValueError, "ranked page objective differs"):
            plan_ranked_pages(
                scores,
                rank_limit=5,
                max_span_pages=2,
                max_ranges=1,
                objective="calibrated",
                calibrated_rank_weights=np.asarray([10, 9], dtype=np.uint64),
            )

    def test_corpus_only_calibration_binds_artifact_ids_and_monotone_curve(
        self,
    ) -> None:
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
        ids = np.arange(100, 108, dtype=np.int64)
        calibration_vectors = base + np.float32(0.05)
        calibration_ids = np.arange(200, 208, dtype=np.int64)
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )

        calibration = fit_page_rank_calibration(
            base,
            ids,
            calibration_vectors,
            calibration_ids,
            artifact=artifact,
            count=4,
            neighbors=2,
            rank_limit=2,
            seed=89,
        )
        repeated = fit_page_rank_calibration(
            base.copy(),
            ids.copy(),
            calibration_vectors.copy(),
            calibration_ids.copy(),
            artifact=artifact,
            count=4,
            neighbors=2,
            rank_limit=2,
            seed=89,
        )

        self.assertEqual(calibration.digest(), repeated.digest())
        self.assertEqual(calibration.routing_digest, artifact.routing_digest())
        self.assertEqual(calibration.calibration_rows, 4)
        self.assertEqual(calibration.neighbors, 2)
        self.assertEqual(calibration.rank_limit, 2)
        weights = calibration.rank_weights()
        self.assertEqual(weights.shape, (2,))
        self.assertTrue(np.all(weights[:-1] >= weights[1:]))
        self.assertGreater(int(weights[0]), 0)

    def test_calibration_uses_external_delta_rows_without_self_page_exclusion(
        self,
    ) -> None:
        base = np.arange(64, dtype=np.float32).reshape(16, 4) / np.float32(10.0)
        ids = np.arange(100, 116, dtype=np.int64)
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=16,
            seed=86,
            iterations=3,
        )
        calibration_vectors = np.asarray(
            [[0.05, 0.05, 0.05, 0.05], [2.05, 2.05, 2.05, 2.05]],
            dtype=np.float32,
        )
        calibration_ids = np.asarray([900, 901], dtype=np.int64)
        mutated = calibration_vectors.copy()
        mutated[1] += np.float32(10_000.0)

        first = fit_page_rank_calibration(
            base,
            ids,
            calibration_vectors,
            calibration_ids,
            artifact=artifact,
            count=1,
            neighbors=2,
            rank_limit=3,
            seed=89,
        )
        second = fit_page_rank_calibration(
            base,
            ids,
            mutated,
            calibration_ids,
            artifact=artifact,
            count=1,
            neighbors=2,
            rank_limit=3,
            seed=89,
        )

        self.assertEqual(first.bin_hits[0], 2)
        self.assertTrue(set(first.calibration_ids).issubset({900, 901}))
        self.assertEqual(first.bin_seen, second.bin_seen)
        self.assertNotEqual(
            first.calibration_vectors_sha256,
            second.calibration_vectors_sha256,
        )

    def test_pair_changes_only_wave_one_weights_and_rejects_binding_drift(self) -> None:
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
        ids = np.arange(100, 108, dtype=np.int64)
        delta = np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32)
        delta_ids = np.asarray([999], dtype=np.int64)
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )
        calibration = fit_page_rank_calibration(
            base,
            ids,
            delta,
            delta_ids,
            artifact=artifact,
            count=1,
            neighbors=2,
            rank_limit=2,
            seed=89,
        )

        pair = evaluate_calibrated_pair(
            base,
            delta,
            np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            artifact=artifact,
            calibration=calibration,
            base_ids=ids,
            delta_ids=delta_ids,
            truth_ids=np.asarray([[104, 105]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            page_rows=2,
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

        self.assertEqual(pair["schema"], "borsuk-v89-calibrated-planner-screen-v1")
        self.assertEqual(pair["arms"]["control"]["objective"], "reciprocal-rank")
        self.assertEqual(pair["arms"]["challenger"]["objective"], "calibrated")
        self.assertEqual(pair["calibration"]["sha256"], calibration.digest())
        self.assertEqual(pair["actual_s3_requests"], 0)
        with self.assertRaisesRegex(ValueError, "rank calibration binding differs"):
            evaluate_calibrated_pair(
                base,
                delta,
                np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                artifact=artifact,
                calibration=calibration,
                base_ids=ids + 1,
                delta_ids=delta_ids,
                truth_ids=np.asarray([[104, 105]], dtype=np.int64),
                query_ordinals=np.asarray([0], dtype=np.int64),
                page_rows=2,
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
        mutated_artifact = dataclasses.replace(
            artifact, summary_codes=artifact.summary_codes[::-1].copy()
        )
        with self.assertRaisesRegex(ValueError, "rank calibration binding differs"):
            evaluate_calibrated_pair(
                base,
                delta,
                np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                artifact=mutated_artifact,
                calibration=calibration,
                base_ids=ids,
                delta_ids=delta_ids,
                truth_ids=np.asarray([[104, 105]], dtype=np.int64),
                query_ordinals=np.asarray([0], dtype=np.int64),
                page_rows=2,
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

    def test_cli_and_runner_freeze_one_burned_dev_spot_cell(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "scripts.v89_calibrated_planner_screen", "--help"],
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
        self.assertNotIn("--calibration-rows", completed.stdout)
        self.assertNotIn("--rank-bins", completed.stdout)

        runner = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v89_calibrated_planner_run_remote.sh")),
                "--describe",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            runner.stdout,
            '{"calibration_rows":128,"max_wall_seconds":1200,'
            '"queries_per_arm":32,"spot_only":true,"total_arms":2}\n',
        )
        runner_source = Path(__file__).with_name(
            "v89_calibrated_planner_run_remote.sh"
        ).read_text()
        self.assertNotIn('"$0" --classify-lifecycle', runner_source)


if __name__ == "__main__":
    unittest.main()
