import dataclasses
import subprocess
import sys
import unittest

import numpy as np

from scripts.v86_coarse_to_fine_screen import (
    _means_per_page,
    _plan_candidate_rows,
    build_coarse_to_fine_artifact,
    build_run_metadata,
    evaluate_coarse_to_fine,
    plan_ranked_pages,
    select_evaluation_rows,
    summarize_gates,
    validate_layout_order,
    validate_truth_rows,
)


class V86CoarseToFineTests(unittest.TestCase):
    def test_wave2_nominations_add_independent_reciprocal_rank_mass(self) -> None:
        control_pages, _ = _plan_candidate_rows(
            np.asarray([0, 1], dtype=np.int64),
            np.asarray([0.0, 1.0], dtype=np.float32),
            page_count=3,
            page_rows=2,
            top_rows=1,
            max_span_pages=1,
            max_ranges=1,
        )
        nominated_pages, nominated_ranges = _plan_candidate_rows(
            np.asarray([0, 1], dtype=np.int64),
            np.asarray([0.0, 1.0], dtype=np.float32),
            page_count=3,
            page_rows=2,
            top_rows=1,
            max_span_pages=1,
            max_ranges=1,
            nominated_positions=np.asarray([4, 5], dtype=np.int64),
        )

        self.assertEqual(control_pages.tolist(), [0])
        self.assertEqual(nominated_pages.tolist(), [2])
        self.assertEqual(nominated_ranges, [(2, 2)])

    def test_page_summaries_keep_fixed_physical_blocks_on_partial_tail(self) -> None:
        rows = np.arange(6, dtype=np.float32).reshape(6, 1)

        summaries = _means_per_page(rows, page_rows=4, summaries_per_page=2)

        self.assertEqual(summaries[:, 0].tolist(), [0.5, 2.5, 4.5, 4.5])

    def test_evaluation_rows_use_burned_development_and_untouched_confirmation(
        self,
    ) -> None:
        queries = np.arange(328 * 2, dtype=np.float32).reshape(328, 2)
        truth = np.arange(328 * 3, dtype=np.int64).reshape(328, 3)

        selected_queries, selected_truth, ordinals = select_evaluation_rows(
            queries,
            truth,
            development_queries=32,
            confirmation_start=200,
            confirmation_queries=128,
        )

        expected_ordinals = np.concatenate(
            (np.arange(32, dtype=np.int64), np.arange(200, 328, dtype=np.int64))
        )
        self.assertTrue(np.array_equal(ordinals, expected_ordinals))
        self.assertTrue(np.array_equal(selected_queries, queries[expected_ordinals]))
        self.assertTrue(np.array_equal(selected_truth, truth[expected_ordinals]))

    def test_metadata_discloses_1m_scope_and_unqualified_100m_cpu(self) -> None:
        metadata = build_run_metadata()

        self.assertEqual(metadata["configuration"]["queries"], 160)
        self.assertEqual(metadata["configuration"]["confirmation_start"], 200)
        self.assertEqual(metadata["configuration"]["wave1_max_span_pages"], 340)
        self.assertLessEqual(
            metadata["configuration"]["wave1_max_bytes"], 16 * 1024 * 1024
        )
        self.assertFalse(metadata["projection_100m"]["serving_cpu_qualified"])
        self.assertFalse(metadata["projection_100m"]["serving_latency_qualified"])
        self.assertEqual(
            metadata["projection_100m"]["page_summary_adc_lookups_per_query"],
            150_000_000,
        )
        self.assertEqual(metadata["scope"], "fixed-1m-quality-falsifier")

    def test_cli_exposes_only_the_fixed_v86_artifact_inputs(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.v86_coarse_to_fine_screen",
                "--help",
            ],
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
        self.assertNotIn("--subspaces", completed.stdout)
        self.assertNotIn("--page-cap", completed.stdout)

    def test_injected_page_evidence_is_digest_bound(self) -> None:
        base = np.asarray([[0.0], [1.0], [10.0], [11.0]], dtype=np.float32)
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=2,
            subspaces=1,
            clusters=4,
            sample_rows=4,
            seed=86,
            iterations=1,
        )
        common = {
            "artifact": artifact,
            "base_ids": np.arange(4, dtype=np.int64),
            "delta_ids": np.asarray([4], dtype=np.int64),
            "truth_ids": np.asarray([[2]], dtype=np.int64),
            "query_ordinals": np.asarray([0], dtype=np.int64),
            "page_rows": 2,
            "neighbors": 1,
            "subspaces": 1,
            "clusters": 4,
            "wave1_rank_pages": 2,
            "wave1_max_span_pages": 1,
            "wave1_max_ranges": 1,
            "wave2_top_rows": 1,
            "wave2_max_span_pages": 1,
            "wave2_max_ranges": 1,
            "code_page_bytes": 2,
            "data_page_bytes": 8,
            "wave1_page_scores_by_query": np.asarray(
                [[1.0, 0.0]], dtype=np.float32
            ),
        }

        result = evaluate_coarse_to_fine(
            base,
            np.asarray([[20.0]], dtype=np.float32),
            np.asarray([[10.0]], dtype=np.float32),
            wave1_page_evidence_sha256="a" * 64,
            **common,
        )
        self.assertEqual(result["wave1_page_evidence_sha256"], "a" * 64)

        nominated = evaluate_coarse_to_fine(
            base,
            np.asarray([[20.0]], dtype=np.float32),
            np.asarray([[10.0]], dtype=np.float32),
            wave1_page_evidence_sha256="a" * 64,
            wave2_nominated_positions_by_query=np.asarray([[0]], dtype=np.int64),
            wave2_nomination_sha256="b" * 64,
            **common,
        )
        self.assertEqual(nominated["wave2_nomination_sha256"], "b" * 64)
        self.assertEqual(nominated["samples"][0]["wave2_nominated_rows"], [0])

        with self.assertRaisesRegex(ValueError, "wave-two nomination differs"):
            evaluate_coarse_to_fine(
                base,
                np.asarray([[20.0]], dtype=np.float32),
                np.asarray([[10.0]], dtype=np.float32),
                wave1_page_evidence_sha256="a" * 64,
                wave2_nominated_positions_by_query=np.asarray(
                    [[0]], dtype=np.int64
                ),
                **common,
            )

        with self.assertRaisesRegex(ValueError, "page evidence differs"):
            evaluate_coarse_to_fine(
                base,
                np.asarray([[20.0]], dtype=np.float32),
                np.asarray([[10.0]], dtype=np.float32),
                wave1_page_evidence_sha256="not-a-digest",
                **common,
            )

    def test_truth_authority_requires_exact_query_rank_order_and_unique_ids(
        self,
    ) -> None:
        ids = validate_truth_rows(
            np.asarray([0, 0, 1, 1], dtype=np.int64),
            np.asarray([0, 1, 0, 1], dtype=np.int64),
            np.asarray([10, 20, 30, 40], dtype=np.int64),
            queries=2,
            neighbors=2,
        )
        self.assertEqual(ids.tolist(), [[10, 20], [30, 40]])

        for ordinals, ranks, values in (
            ([0, 1, 0, 1], [0, 1, 0, 1], [10, 20, 30, 40]),
            ([0, 0, 1, 1], [0, 0, 0, 1], [10, 20, 30, 40]),
            ([0, 0, 1, 1], [0, 1, 0, 1], [10, 10, 30, 40]),
        ):
            with self.assertRaisesRegex(ValueError, "ground-truth order differs"):
                validate_truth_rows(
                    np.asarray(ordinals),
                    np.asarray(ranks),
                    np.asarray(values),
                    queries=2,
                    neighbors=2,
                )

    def test_layout_authority_rejects_negative_duplicate_and_delta_rows(self) -> None:
        self.assertEqual(
            validate_layout_order(
                np.asarray([2, 0, 3, 1], dtype=np.int64),
                rows=6,
                base_rows=4,
            ).tolist(),
            [2, 0, 3, 1],
        )
        for broken in (
            np.asarray([-1, 0, 1, 2], dtype=np.int64),
            np.asarray([0, 1, 1, 3], dtype=np.int64),
            np.asarray([0, 1, 2, 4], dtype=np.int64),
        ):
            with self.assertRaisesRegex(ValueError, "base layout order differs"):
                validate_layout_order(broken, rows=6, base_rows=4)

    def test_artifact_is_deterministic_and_accepts_only_base_training_data(self) -> None:
        base = np.asarray(
            [
                [0.0, 0.0, 0.0, 0.0],
                [0.1, 0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0, 0.0],
                [1.1, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 1.1, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
                [0.0, 0.0, 1.1, 0.0],
            ],
            dtype=np.float32,
        )

        first = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )
        second = build_coarse_to_fine_artifact(
            base.copy(),
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
        )

        self.assertEqual(first.digest(), second.digest())
        self.assertTrue(np.array_equal(first.row_codes, second.row_codes))
        self.assertTrue(np.array_equal(first.summary_codes, second.summary_codes))
        self.assertEqual(first.training_rows, 8)

        eight_summary = build_coarse_to_fine_artifact(
            base,
            page_rows=4,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
            summaries_per_page=4,
        )
        self.assertEqual(first.routing_digest(), eight_summary.routing_digest())
        self.assertNotEqual(first.digest(), eight_summary.digest())
        self.assertEqual(eight_summary.summary_codes.shape, (8, 2))

    def test_evaluator_uses_one_supplied_artifact_without_retraining(self) -> None:
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

        result = evaluate_coarse_to_fine(
            base,
            np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[104, 105]], dtype=np.int64),
            query_ordinals=np.asarray([0], dtype=np.int64),
            artifact=artifact,
            page_rows=4,
            neighbors=2,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=87,
            iterations=3,
            wave1_rank_pages=1,
            wave1_max_span_pages=1,
            wave1_max_ranges=1,
            wave2_top_rows=2,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=8,
            data_page_bytes=32,
        )

        self.assertEqual(result["artifact_sha256"], artifact.digest())
        self.assertEqual(result["page_sq8_recall_ppm"], 1_000_000)

        with self.assertRaisesRegex(
            ValueError, "coarse-to-fine artifact authority differs"
        ):
            evaluate_coarse_to_fine(
                base,
                np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
                base_ids=np.arange(100, 108, dtype=np.int64),
                delta_ids=np.asarray([999], dtype=np.int64),
                truth_ids=np.asarray([[104, 105]], dtype=np.int64),
                query_ordinals=np.asarray([0], dtype=np.int64),
                artifact=dataclasses.replace(artifact, page_rows=8),
                page_rows=4,
                neighbors=2,
                subspaces=2,
                wave1_rank_pages=1,
                wave1_max_span_pages=1,
                wave1_max_ranges=1,
                wave2_top_rows=2,
                wave2_max_span_pages=1,
                wave2_max_ranges=1,
                code_page_bytes=8,
                data_page_bytes=32,
            )

    def test_ranked_page_plan_charges_intervening_gap_pages(self) -> None:
        pages, ranges = plan_ranked_pages(
            np.asarray([0.01, 9.0, 0.02, 9.0, 0.03], dtype=np.float32),
            rank_limit=3,
            max_span_pages=3,
            max_ranges=1,
        )

        self.assertEqual(pages.tolist(), [0, 1, 2])
        self.assertEqual(ranges, [(0, 2)])
        self.assertEqual(pages.size, 3)

    def test_coverage_first_planner_prefers_more_ranked_pages_before_rank_mass(
        self,
    ) -> None:
        scores = np.asarray([0.0, 9.0, 9.0, 1.0, 2.0], dtype=np.float32)

        reciprocal_pages, reciprocal_ranges = plan_ranked_pages(
            scores,
            rank_limit=3,
            max_span_pages=2,
            max_ranges=1,
        )
        coverage_pages, coverage_ranges = plan_ranked_pages(
            scores,
            rank_limit=3,
            max_span_pages=2,
            max_ranges=1,
            objective="coverage-first",
        )

        self.assertEqual(reciprocal_pages.tolist(), [0])
        self.assertEqual(reciprocal_ranges, [(0, 0)])
        self.assertEqual(coverage_pages.tolist(), [3, 4])
        self.assertEqual(coverage_ranges, [(3, 4)])

        with self.assertRaisesRegex(ValueError, "ranked page objective differs"):
            plan_ranked_pages(
                scores,
                rank_limit=3,
                max_span_pages=2,
                max_ranges=1,
                objective="unknown",
            )

    def test_two_wave_screen_scores_only_fetched_codes_and_recomputes_accounting(
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
        delta = np.asarray([[30.0, 0.0, 0.0, 0.0]], dtype=np.float32)
        result = evaluate_coarse_to_fine(
            base,
            delta,
            np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[104, 105]], dtype=np.int64),
            query_ordinals=np.asarray([200], dtype=np.int64),
            page_rows=4,
            neighbors=2,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
            wave1_rank_pages=1,
            wave1_max_span_pages=1,
            wave1_max_ranges=1,
            wave2_top_rows=2,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=8,
            data_page_bytes=32,
        )

        self.assertEqual(result["schema"], "borsuk-v86-coarse-to-fine-screen-v1")
        self.assertFalse(result["claim_eligible"])
        self.assertEqual(result["page_sq8_recall_ppm"], 1_000_000)
        self.assertEqual(result["exact_recall_ppm"], 1_000_000)
        self.assertEqual(result["base_truth_hits"], 2)
        self.assertEqual(result["delta_truth_hits"], 0)
        self.assertEqual(result["wave1_objective"], "reciprocal-rank")
        self.assertEqual(
            result["samples"],
            [
                {
                    "base_truth_hits": 2,
                    "delta_truth_hits": 0,
                    "exact_base_hits": 2,
                    "exact_hits": 2,
                    "page_sq8_base_hits": 2,
                    "page_sq8_hits": 2,
                    "pq_shortlist_base_truth_hits": 2,
                    "query": 200,
                    "truth_base_pages": [1, 1],
                    "truth_page_ranks": [0, 0],
                    "wave1_base_truth_hits": 2,
                    "wave1_bytes": 8,
                    "wave1_gap_only_base_truth_hits": 0,
                    "wave1_gets": 1,
                    "wave1_pages": [1],
                    "wave1_planner_missed_base_truth_hits": 0,
                    "wave1_rank_visible_base_truth_hits": 2,
                    "wave1_rank_visible_selected_base_truth_hits": 2,
                    "wave1_ranked_pages_selected": 1,
                    "wave1_reciprocal_rank_utility": 1_000_000_000,
                    "wave1_ranges": [[1, 1]],
                    "wave2_base_truth_page_hits": 2,
                    "wave2_bytes": 32,
                    "wave2_gets": 1,
                    "wave2_pages": [1],
                    "wave2_ranges": [[1, 1]],
                }
            ],
        )

        poisoned = base.copy()
        poisoned[:4] = np.asarray([10.0, 0.0, 0.0, 0.0], dtype=np.float32)
        poisoned_result = evaluate_coarse_to_fine(
            poisoned,
            delta,
            np.asarray([[10.0, 0.0, 0.0, 0.0]], dtype=np.float32),
            base_ids=np.arange(100, 108, dtype=np.int64),
            delta_ids=np.asarray([999], dtype=np.int64),
            truth_ids=np.asarray([[104, 105]], dtype=np.int64),
            query_ordinals=np.asarray([200], dtype=np.int64),
            page_rows=4,
            neighbors=2,
            subspaces=2,
            clusters=4,
            sample_rows=8,
            seed=86,
            iterations=3,
            wave1_rank_pages=1,
            wave1_max_span_pages=1,
            wave1_max_ranges=1,
            wave2_top_rows=2,
            wave2_max_span_pages=1,
            wave2_max_ranges=1,
            code_page_bytes=8,
            data_page_bytes=32,
        )
        self.assertEqual(poisoned_result["samples"][0]["wave1_pages"], [0])
        self.assertEqual(poisoned_result["page_sq8_recall_ppm"], 0)

    def test_promotion_gates_recompute_dev_confirmation_and_base_only_hits(
        self,
    ) -> None:
        samples = []
        for query in range(160):
            query_ordinal = query if query < 32 else query + 168
            samples.append(
                {
                    "base_truth_hits": 90,
                    "exact_base_hits": 90,
                    "exact_hits": 100,
                    "page_sq8_base_hits": 90,
                    "page_sq8_hits": 100,
                    "pq_shortlist_base_truth_hits": 90,
                    "query": query_ordinal,
                    "wave1_base_truth_hits": 90,
                    "wave1_bytes": 16,
                    "wave1_gets": 1,
                    "wave1_pages": [0],
                    "wave1_ranges": [[0, 0]],
                    "wave2_base_truth_page_hits": 90,
                    "wave2_bytes": 32,
                    "wave2_gets": 1,
                    "wave2_pages": [0],
                    "wave2_ranges": [[0, 0]],
                }
            )
        result = {
            "samples": samples,
            # These deliberately false aggregate fields must not influence
            # promotion; samples are the canonical evidence.
            "page_sq8_recall_ppm": 0,
            "exact_recall_ppm": 0,
        }

        gates = summarize_gates(result, neighbors=100)

        self.assertEqual(
            gates,
            {
                "confirmation": {
                    "base_recall_ppm": 1_000_000,
                    "hits": 12_800,
                    "passed": True,
                    "recall_ppm": 1_000_000,
                    "worst_query_hits": 100,
                },
                "development": {
                    "base_recall_ppm": 1_000_000,
                    "hits": 3_200,
                    "passed": True,
                    "query_15_hits": 100,
                    "recall_ppm": 1_000_000,
                    "worst_query_hits": 100,
                },
                "passed": True,
            },
        )

        result["samples"][32]["page_sq8_hits"] = 79
        self.assertFalse(summarize_gates(result, neighbors=100)["passed"])

        for sample in result["samples"]:
            sample["page_sq8_hits"] = 100
            sample["page_sq8_base_hits"] = 89
        gates = summarize_gates(result, neighbors=100)
        self.assertEqual(gates["development"]["base_recall_ppm"], 988_889)
        self.assertFalse(gates["development"]["passed"])
        self.assertFalse(gates["confirmation"]["passed"])

        result["samples"][32]["query"] = 32
        with self.assertRaisesRegex(
            ValueError, "coarse-to-fine promotion ordinals differ"
        ):
            summarize_gates(result, neighbors=100)


if __name__ == "__main__":
    unittest.main()
