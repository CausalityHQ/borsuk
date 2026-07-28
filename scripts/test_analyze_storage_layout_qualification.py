#!/usr/bin/env python3
"""Tests for conservative storage-layout promotion decisions."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from scripts import analyze_storage_layout_qualification as analysis

ROOT = Path(__file__).resolve().parents[1]


class StorageLayoutQualificationAnalysisTest(unittest.TestCase):
    def test_checked_protocol_matches_executable_thresholds(self) -> None:
        protocol = json.loads(
            (
                ROOT / "docs/research/storage-layout-qualification-protocol.json"
            ).read_text()
        )
        self.assertEqual(
            protocol["correctness"]["maximum_mean_recall_at_10_loss"],
            analysis.MAX_RECALL_LOSS,
        )
        self.assertEqual(
            protocol["latency"]["maximum_p95_ratio"],
            analysis.TARGET_P95_RATIO,
        )
        self.assertEqual(
            protocol["latency"]["maximum_p95_ratio_bootstrap_familywise_upper"],
            analysis.MAX_FAMILYWISE_CONFIDENCE_UPPER,
        )
        self.assertEqual(
            protocol["latency"]["maximum_p99_ratio"],
            analysis.MAX_P99_RATIO,
        )
        self.assertEqual(protocol["minimum_backends"], len(analysis.REQUIRED_BACKENDS))
        self.assertEqual(
            protocol["minimum_repetitions"],
            analysis.MINIMUM_REPETITIONS,
        )
        self.assertEqual(set(protocol["backends"]), set(analysis.REQUIRED_BACKENDS))
        self.assertEqual(set(protocol["datasets"]), set(analysis.REQUIRED_DATASETS))
        self.assertEqual(set(protocol["candidate_arms"]), set(analysis.CANDIDATE_ARMS))
        self.assertEqual(
            protocol["latency"]["bootstrap_resamples"],
            analysis.BOOTSTRAP_SAMPLES,
        )
        self.assertEqual(
            protocol["latency"]["familywise_alpha"],
            analysis.FAMILYWISE_ALPHA,
        )
        self.assertEqual(
            protocol["latency"]["familywise_upper_quantile"],
            analysis.BOOTSTRAP_UPPER_QUANTILE,
        )
        guardrails = protocol["operational_guardrails"]
        self.assertEqual(
            guardrails["maximum_mean_request_ratio"],
            analysis.MAX_REQUEST_RATIO,
        )
        self.assertEqual(
            guardrails["maximum_mean_bytes_read_ratio"],
            analysis.MAX_BYTES_RATIO,
        )
        self.assertEqual(
            guardrails["maximum_mean_build_time_ratio"],
            analysis.MAX_BUILD_RATIO,
        )
        self.assertEqual(
            guardrails["maximum_mean_active_index_bytes_ratio"],
            analysis.MAX_INDEX_BYTES_RATIO,
        )
        self.assertEqual(
            guardrails["maximum_mean_segment_bytes_ratio"],
            analysis.MAX_SEGMENT_BYTES_RATIO,
        )
        self.assertEqual(
            guardrails["maximum_mean_peak_rss_ratio"],
            analysis.MAX_PEAK_RSS_RATIO,
        )
        self.assertEqual(
            guardrails["maximum_mean_cpu_core_time_ratio"],
            analysis.MAX_CPU_RATIO,
        )

    @staticmethod
    def rows(
        *,
        datasets: tuple[str, ...] = ("fashion-mnist-784", "glove-100"),
        backends: tuple[str, ...] = ("local_disk", "s3"),
        candidate_latency: float = 7.0,
        candidate_recall: float = 0.95,
        candidate_gets: float = 8.0,
        candidate_bytes: float = 900.0,
        candidate_build_ms: float = 90.0,
        candidate_segment_bytes: float = 900.0,
        candidate_index_bytes: float = 900.0,
        candidate_peak_rss: float = 90.0,
        candidate_cpu_core_ms: float = 90.0,
    ) -> list[dict[str, object]]:
        rows = []
        for backend in backends:
            for dataset in datasets:
                for repetition in ("r01", "r02", "r03", "r04", "r05"):
                    for query in range(40):
                        for arm, latency, recall in (
                            ("fixed-parquet", 10.0 + query / 100, 0.95),
                            (
                                "mixed-vortex-range",
                                candidate_latency + query / 100,
                                candidate_recall,
                            ),
                        ):
                            rows.append(
                                {
                                    "dataset": dataset,
                                    "backend": backend,
                                    "arm": arm,
                                    "repetition_id": repetition,
                                    "query_position": query,
                                    "query_source_index": query + 100,
                                    "latency_ms": latency,
                                    "recall_at_10": recall,
                                    "physical_requests": (
                                        10.0
                                        if arm == "fixed-parquet"
                                        else candidate_gets
                                    ),
                                    "bytes_read": (
                                        1_000.0
                                        if arm == "fixed-parquet"
                                        else candidate_bytes
                                    ),
                                    "build_ms": (
                                        100.0
                                        if arm == "fixed-parquet"
                                        else candidate_build_ms
                                    ),
                                    "segment_bytes": (
                                        1_000.0
                                        if arm == "fixed-parquet"
                                        else candidate_segment_bytes
                                    ),
                                    "total_active_index_bytes": (
                                        1_000.0
                                        if arm == "fixed-parquet"
                                        else candidate_index_bytes
                                    ),
                                    "peak_rss_bytes": (
                                        100.0
                                        if arm == "fixed-parquet"
                                        else candidate_peak_rss
                                    ),
                                    "cpu_core_ms": (
                                        100.0
                                        if arm == "fixed-parquet"
                                        else candidate_cpu_core_ms
                                    ),
                                    "status": "ok",
                                }
                            )
        return rows

    def test_promotes_only_a_correct_confident_win_on_two_datasets(self) -> None:
        decisions = analysis.analyze_rows(self.rows(), minimum_samples=30)

        overall = next(
            row
            for row in decisions
            if row["dataset"] == "all"
            and row["backend"] == "all"
            and row["arm"] == "mixed-vortex-range"
        )
        self.assertEqual(overall["decision"], "promote")
        self.assertLess(float(overall["worst_p95_ratio"]), 0.95)
        self.assertEqual(overall["backends_passed"], 2)
        self.assertLessEqual(float(overall["worst_bytes_ratio"]), 1.0)

    def test_emits_no_promotion_for_every_missing_preregistered_arm(self) -> None:
        decisions = analysis.analyze_rows(self.rows(), minimum_samples=30)
        global_rows = {
            row["arm"]: row
            for row in decisions
            if row["dataset"] == "all" and row["backend"] == "all"
        }
        self.assertEqual(set(global_rows), set(analysis.CANDIDATE_ARMS))
        for arm in set(analysis.CANDIDATE_ARMS) - {"mixed-vortex-range"}:
            self.assertEqual(global_rows[arm]["decision"], "no-promotion")

    def test_withholds_global_promotion_when_a_required_backend_is_missing(
        self,
    ) -> None:
        decisions = analysis.analyze_rows(
            self.rows(backends=("s3",)), minimum_samples=30
        )

        overall = decisions[-1]
        self.assertEqual(overall["backend"], "all")
        self.assertEqual(overall["decision"], "no-promotion")
        self.assertIn("both required backends", overall["reason"])

    def test_rejects_unregistered_replacement_datasets(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown datasets"):
            analysis.analyze_rows(
                self.rows(datasets=("corpus-a", "corpus-b")),
                minimum_samples=30,
            )

    def test_withholds_promotion_for_one_dataset_or_latency_regression(self) -> None:
        one_dataset = analysis.analyze_rows(
            self.rows(datasets=("fashion-mnist-784",)), minimum_samples=30
        )
        self.assertEqual(one_dataset[-1]["decision"], "no-promotion")
        self.assertIn("two datasets", one_dataset[-1]["reason"])

        slower = analysis.analyze_rows(
            self.rows(candidate_latency=10.5), minimum_samples=30
        )
        self.assertEqual(slower[-1]["decision"], "no-promotion")
        self.assertIn("confidence", slower[-1]["reason"])

    def test_withholds_promotion_for_recall_loss_or_missing_samples(self) -> None:
        recall_loss = analysis.analyze_rows(
            self.rows(candidate_recall=0.90), minimum_samples=30
        )
        self.assertEqual(recall_loss[-1]["decision"], "no-promotion")
        self.assertIn("correctness", recall_loss[-1]["reason"])

        incomplete = self.rows()
        incomplete = [
            row
            for row in incomplete
            if not (
                row["arm"] == "mixed-vortex-range"
                and row["dataset"] == "glove-100"
                and row["repetition_id"] == "r03"
                and int(row["query_position"]) >= 20
            )
        ]
        missing = analysis.analyze_rows(incomplete, minimum_samples=30)
        self.assertEqual(missing[-1]["decision"], "no-promotion")
        self.assertIn("sample gate", missing[-1]["reason"])

    def test_withholds_promotion_for_one_sided_extra_query_identity(self) -> None:
        asymmetric = self.rows()
        extra = dict(
            next(
                row
                for row in asymmetric
                if row["backend"] == "local_disk"
                and row["dataset"] == "fashion-mnist-784"
                and row["arm"] == "fixed-parquet"
                and row["repetition_id"] == "r01"
            )
        )
        extra["query_position"] = 999
        extra["query_source_index"] = 999
        asymmetric.append(extra)

        decisions = analysis.analyze_rows(asymmetric, minimum_samples=30)

        local_fashion = next(
            row
            for row in decisions
            if row["dataset"] == "fashion-mnist-784" and row["backend"] == "local_disk"
        )
        self.assertEqual(local_fashion["decision"], "no-promotion")
        self.assertIn("sample gate", local_fashion["reason"])

    def test_withholds_promotion_for_any_operational_regression(self) -> None:
        regressions = {
            "candidate_gets": 11.0,
            "candidate_bytes": 1_100.0,
            "candidate_build_ms": 120.0,
            "candidate_segment_bytes": 1_100.0,
            "candidate_index_bytes": 1_100.0,
            "candidate_peak_rss": 1_100.0,
            "candidate_cpu_core_ms": 120.0,
        }
        for field, value in regressions.items():
            with self.subTest(field=field):
                decisions = analysis.analyze_rows(
                    self.rows(**{field: value}),
                    minimum_samples=30,
                )
                self.assertEqual(decisions[-1]["decision"], "no-promotion")
                self.assertIn("operational", decisions[-1]["reason"])

    def test_missing_operational_evidence_fails_the_sample_gate(self) -> None:
        rows = self.rows()
        for row in rows:
            row.pop("peak_rss_bytes")

        decisions = analysis.analyze_rows(rows, minimum_samples=30)

        self.assertEqual(decisions[-1]["decision"], "no-promotion")
        self.assertIn("sample gate", decisions[-1]["reason"])

    def test_pairs_by_query_source_identity_not_sample_position(self) -> None:
        rows = self.rows()
        for row in rows:
            if row["arm"] == "mixed-vortex-range" and row["query_position"] == 0:
                row["query_source_index"] = 999_999

        decisions = analysis.analyze_rows(rows, minimum_samples=40)

        self.assertEqual(decisions[-1]["decision"], "no-promotion")
        self.assertIn("sample gate", decisions[-1]["reason"])


if __name__ == "__main__":
    unittest.main()
