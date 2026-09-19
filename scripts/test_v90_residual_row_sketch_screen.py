import dataclasses
import hashlib
import subprocess
import sys
import unittest
from pathlib import Path

import numpy as np

from scripts.v86_coarse_to_fine_screen import build_coarse_to_fine_artifact
from scripts.v90_residual_row_sketch_screen import (
    ResidualRowSketchArtifact,
    _candidate_fenced_truth_oracle,
    _decode_candidate_page_means,
    build_residual_row_sketch,
    build_run_metadata,
    decode_page_means,
    evaluate_residual_sketch_pair,
    residual_page_rank_scores,
)


class V90ResidualRowSketchTests(unittest.TestCase):
    @staticmethod
    def _base() -> np.ndarray:
        return np.asarray(
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

    def test_page_means_are_decoded_from_registered_control_summaries(self) -> None:
        base = self._base()
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )

        means = decode_page_means(control, dimensions=4, rows=8)
        decoded = np.concatenate(
            [
                book[control.summary_codes[:, index]]
                for index, book in enumerate(control.books)
            ],
            axis=1,
        )
        expected = decoded.reshape(2, 2, 4).mean(axis=1, dtype=np.float32)

        self.assertTrue(np.array_equal(means, expected))

        candidates = np.asarray([1, 0], dtype=np.int64)
        candidate_means = _decode_candidate_page_means(
            control, candidates, dimensions=4, rows=8
        )
        self.assertTrue(np.array_equal(candidate_means, means[candidates]))

    def test_sketch_build_is_deterministic_and_binds_full_base_and_control(self) -> None:
        base = self._base()
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )

        first = build_residual_row_sketch(
            base,
            control,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=90,
            iterations=3,
        )
        second = build_residual_row_sketch(
            base.copy(),
            control,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=90,
            iterations=3,
        )
        mutated = base.copy()
        mutated[0, 0] += np.float32(0.25)
        third = build_residual_row_sketch(
            mutated,
            control,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=90,
            iterations=3,
        )

        self.assertEqual(first.digest(), second.digest())
        self.assertNotEqual(first.digest(), third.digest())
        self.assertEqual(first.control_artifact_sha256, control.digest())
        self.assertEqual(first.row_codes.shape, (8, 2))
        self.assertEqual(first.row_codes.dtype, np.uint8)

    def test_residual_scoring_is_fenced_by_control_top_pages_and_breaks_ties(self) -> None:
        control = build_coarse_to_fine_artifact(
            np.zeros((6, 2), dtype=np.float32),
            page_rows=2,
            subspaces=1,
            clusters=4,
            sample_rows=6,
            seed=86,
            iterations=1,
        )
        sketch = ResidualRowSketchArtifact(
            base_vectors_sha256="1" * 64,
            books=(
                np.asarray(
                    [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0], [30.0, 0.0]],
                    dtype=np.float32,
                ),
            ),
            control_artifact_sha256=control.digest(),
            page_rows=2,
            row_codes=np.asarray([[1], [1], [0], [0], [0], [0]], dtype=np.uint8),
            training_rows=6,
        )

        scores = residual_page_rank_scores(
            np.zeros(2, dtype=np.float32),
            np.asarray([0.0, 1.0, 2.0], dtype=np.float32),
            control,
            sketch,
            rank_limit=2,
        )

        self.assertEqual(np.argsort(scores, kind="stable").tolist(), [1, 0, 2])
        self.assertEqual(scores.tolist(), [1.0, 0.0, 2.0])

        tied = dataclasses.replace(
            sketch,
            row_codes=np.zeros((6, 1), dtype=np.uint8),
        )
        tied_scores = residual_page_rank_scores(
            np.zeros(2, dtype=np.float32),
            np.asarray([0.0, 0.0, 2.0], dtype=np.float32),
            control,
            tied,
            rank_limit=2,
        )
        self.assertEqual(tied_scores.tolist(), [0.0, 1.0, 2.0])

    def test_residual_scoring_uses_query_page_means_and_second_minimum(self) -> None:
        control = build_coarse_to_fine_artifact(
            np.asarray(
                [[0.0, 0.0], [0.0, 0.0], [10.0, 0.0], [10.0, 0.0]],
                dtype=np.float32,
            ),
            page_rows=2,
            subspaces=1,
            clusters=4,
            sample_rows=4,
            seed=86,
            iterations=1,
        )
        mean_sensitive = ResidualRowSketchArtifact(
            base_vectors_sha256="1" * 64,
            books=(
                np.asarray(
                    [[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0]],
                    dtype=np.float32,
                ),
            ),
            control_artifact_sha256=control.digest(),
            page_rows=2,
            row_codes=np.zeros((4, 1), dtype=np.uint8),
            training_rows=4,
        )
        scores = residual_page_rank_scores(
            np.asarray([9.0, 0.0], dtype=np.float32),
            np.asarray([0.0, 1.0], dtype=np.float32),
            control,
            mean_sensitive,
            rank_limit=2,
        )
        self.assertEqual(scores.tolist(), [1.0, 0.0])

        second_sensitive = dataclasses.replace(
            mean_sensitive,
            row_codes=np.asarray([[0], [3], [0], [1]], dtype=np.uint8),
        )
        tied_control = dataclasses.replace(
            control,
            summary_codes=np.zeros_like(control.summary_codes),
        )
        second_sensitive = dataclasses.replace(
            second_sensitive,
            control_artifact_sha256=tied_control.digest(),
        )
        second_scores = residual_page_rank_scores(
            np.zeros(2, dtype=np.float32),
            np.asarray([0.0, 0.0], dtype=np.float32),
            tied_control,
            second_sensitive,
            rank_limit=2,
        )
        self.assertEqual(second_scores.tolist(), [1.0, 0.0])

    def test_residual_scoring_masks_the_partial_tail_page(self) -> None:
        base = np.asarray(
            [[0.0], [0.0], [10.0], [10.0], [20.0]], dtype=np.float32
        )
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=5,
            sample_rows=5,
            seed=86,
            iterations=1,
        )
        sketch = ResidualRowSketchArtifact(
            base_vectors_sha256=hashlib.sha256(base.tobytes()).hexdigest(),
            books=(np.arange(5, dtype=np.float32).reshape(5, 1),),
            control_artifact_sha256=control.digest(),
            page_rows=2,
            row_codes=np.asarray([[0], [0], [0], [0], [0]], dtype=np.uint8),
            training_rows=5,
        )

        scores = residual_page_rank_scores(
            np.asarray([20.0], dtype=np.float32),
            np.asarray([2.0, 1.0, 0.0], dtype=np.float32),
            control,
            sketch,
            rank_limit=3,
        )

        self.assertTrue(np.isfinite(scores).all())
        self.assertEqual(int(np.argmin(scores)), 2)

    def test_candidate_fenced_oracle_separates_fence_from_evidence_failure(
        self,
    ) -> None:
        samples = []
        for query in range(32):
            samples.append(
                {
                    "base_truth_hits": 1,
                    "delta_truth_hits": 0,
                    "query": query,
                    "truth_base_pages": [query],
                    "truth_page_ranks": [1_024 if query == 15 else 0],
                }
            )
        oracle = _candidate_fenced_truth_oracle(
            {"samples": samples},
            neighbors=1,
            page_count=64,
            page_rows=2,
            rank_limit=1_024,
            max_pages=32,
            max_ranges=32,
        )

        self.assertEqual(oracle["hits"], 31)
        self.assertEqual(oracle["query_15_hits"], 0)
        self.assertFalse(oracle["can_pass_registered_gate"])

    def test_pair_consumes_challenger_evidence_to_change_selected_page(self) -> None:
        base = np.asarray(
            [[0.0, 0.0], [0.0, 0.0], [10.0, 0.0], [10.0, 0.0]],
            dtype=np.float32,
        )
        book = np.asarray([[0.0, 0.0], [10.0, 0.0]], dtype=np.float32)
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=2,
            sample_rows=4,
            seed=86,
            iterations=1,
        )
        control = dataclasses.replace(
            control,
            books=(book,),
            row_codes=np.asarray([[0], [0], [1], [1]], dtype=np.uint8),
            summary_codes=np.asarray([[0], [0]], dtype=np.uint8),
        )
        sketch = ResidualRowSketchArtifact(
            base_vectors_sha256=hashlib.sha256(base.tobytes()).hexdigest(),
            books=(book,),
            control_artifact_sha256=control.digest(),
            page_rows=2,
            row_codes=np.asarray([[0], [0], [1], [1]], dtype=np.uint8),
            training_rows=4,
        )

        pair = evaluate_residual_sketch_pair(
            base,
            np.asarray([[30.0, 0.0]], dtype=np.float32),
            np.asarray([[10.0, 0.0]], dtype=np.float32),
            control_artifact=control,
            sketch_artifact=sketch,
            base_ids=np.asarray([100, 101, 102, 103], dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[102]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            page_rows=2,
            neighbors=1,
            control_subspaces=1,
            control_clusters=2,
            wave1_rank_pages=2,
            wave1_max_span_pages=1,
            wave1_max_ranges=1,
            wave2_top_rows=1,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=2,
            data_page_bytes=8,
        )

        self.assertEqual(pair["arms"]["control"]["result"]["samples"][0]["wave1_pages"], [0])
        self.assertEqual(pair["arms"]["challenger"]["result"]["samples"][0]["wave1_pages"], [1])
        self.assertEqual(pair["arms"]["control"]["result"]["page_sq8_recall_ppm"], 0)
        self.assertEqual(pair["arms"]["challenger"]["result"]["page_sq8_recall_ppm"], 1_000_000)
        self.assertEqual(
            pair["arms"]["challenger"]["result"]["wave1_page_evidence_sha256"],
            sketch.digest(),
        )

    def test_pair_changes_only_wave_one_page_evidence_at_identical_budgets(self) -> None:
        base = self._base()
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )
        sketch = build_residual_row_sketch(
            base,
            control,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=90,
            iterations=3,
        )

        pair = evaluate_residual_sketch_pair(
            base,
            np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            control_artifact=control,
            sketch_artifact=sketch,
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[104, 105]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            page_rows=4,
            neighbors=2,
            control_subspaces=2,
            control_clusters=4,
            wave1_rank_pages=2,
            wave1_max_span_pages=2,
            wave1_max_ranges=1,
            wave2_top_rows=2,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=8,
            data_page_bytes=32,
        )

        self.assertEqual(pair["schema"], "borsuk-v90-residual-row-sketch-screen-v1")
        self.assertEqual(pair["changed_parameter"], "wave1_page_evidence")
        self.assertEqual(pair["actual_s3_requests"], 0)
        self.assertEqual(pair["sketch_sha256"], sketch.digest())
        self.assertEqual(
            pair["arms"]["control"]["budgets"],
            pair["arms"]["challenger"]["budgets"],
        )
        for arm in pair["arms"].values():
            self.assertEqual(arm["result"]["artifact_sha256"], control.digest())
            self.assertEqual(arm["budgets"]["max_wave1_pages"], 2)
            self.assertEqual(arm["budgets"]["max_wave2_pages"], 1)

    def test_binding_rejects_control_or_base_drift(self) -> None:
        base = self._base()
        control = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )
        sketch = build_residual_row_sketch(
            base,
            control,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=90,
            iterations=3,
        )
        broken = dataclasses.replace(sketch, control_artifact_sha256="0" * 64)

        with self.assertRaisesRegex(ValueError, "V90 sketch binding differs"):
            evaluate_residual_sketch_pair(
                base,
                np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                control_artifact=control,
                sketch_artifact=broken,
                base_ids=np.arange(100, 108, dtype=np.int64),
                delta_ids=np.asarray([999], dtype=np.int64),
                truth_ids=np.asarray([[104, 105]], dtype=np.int64),
                query_ordinals=np.asarray([0], dtype=np.int64),
                page_rows=4,
                neighbors=2,
                control_subspaces=2,
                control_clusters=4,
                wave1_rank_pages=2,
                wave1_max_span_pages=2,
                wave1_max_ranges=1,
                wave2_top_rows=2,
                wave2_max_span_pages=1,
                wave2_max_ranges=1,
                code_page_bytes=8,
                data_page_bytes=32,
            )

    def test_metadata_projects_bounded_resident_memory_but_not_100m_cpu(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(metadata["configuration"]["sketch_bytes_per_base_row"], 16)
        self.assertEqual(metadata["configuration"]["queries"], 32)
        self.assertLess(metadata["projection_100m"]["resident_bytes"], 3 * 2**30)
        self.assertEqual(
            metadata["projection_100m"]["resident_bytes"], 2_057_023_104
        )
        self.assertEqual(
            metadata["projection_100m"]["candidate_page_mean_scratch_bytes"],
            1_024 * 768 * 4,
        )
        self.assertEqual(
            metadata["projection_100m"]["decoded_page_means_resident_bytes"], 0
        )
        self.assertEqual(
            metadata["projection_100m"]["dense_wave1_traceback_bytes_per_query"],
            8_791_406_250,
        )
        self.assertFalse(metadata["projection_100m"]["serving_memory_qualified"])
        self.assertFalse(metadata["projection_100m"]["serving_cpu_qualified"])
        self.assertFalse(metadata["projection_100m"]["serving_latency_qualified"])
        self.assertEqual(metadata["scope"], "fixed-1m-burned-development-row-sketch-falsifier")

    def test_cli_and_runner_freeze_one_burned_dev_spot_cell(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "scripts.v90_residual_row_sketch_screen", "--help"],
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
        self.assertNotIn("--sketch-width", completed.stdout)
        self.assertNotIn("--confirmation", completed.stdout)

        runner = subprocess.run(
            [
                "bash",
                str(Path(__file__).with_name("v90_residual_row_sketch_run_remote.sh")),
                "--describe",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            runner.stdout,
            '{"max_wall_seconds":1200,"queries_per_arm":32,'
            '"sketch_bytes_per_base_row":16,"spot_only":true,"total_arms":2}\n',
        )


if __name__ == "__main__":
    unittest.main()
