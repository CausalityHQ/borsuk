#!/usr/bin/env python3
"""Tests for assembling raw layout qualification artifacts."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts import assemble_storage_layout_qualification as assemble
from scripts.production_bench_schema import QUERY_STAGE_TIMING_FIELDS


class AssembleStorageLayoutQualificationTest(unittest.TestCase):
    def test_assembles_query_build_and_resource_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case = root / "r01/fashion/local/fixed-parquet/results"
            case.mkdir(parents=True)
            (root / "schedule.csv").write_text(
                "repetition_id,query_seed,dataset,backend,arm,arm_position,case_id\n"
                "r01,17,fashion,local,fixed-parquet,0,"
                "r01/fashion/local/fixed-parquet\n"
            )
            source_sha = "ab" * 32
            (root / "environment.txt").write_text(f"source_sha256={source_sha}\n")
            (root / "qualification-protocol.json").write_text(
                json.dumps(
                    {
                        "datasets": ["fashion"],
                        "backends": ["local"],
                        "repetitions": 1,
                        "queries_per_repetition": 1,
                        "query_seeds": [17],
                        "baseline_arm": "fixed-parquet",
                        "candidate_arms": [],
                    }
                )
            )
            case_root = case.parent
            (case_root / "CASE_COMPLETE").write_text("complete\n")
            (case_root / "protocol.txt").write_text(
                f"source_sha256={source_sha}\n"
                "repetition_id=r01\n"
                "query_seed=17\n"
                "dataset=fashion\n"
                "backend=local\n"
                "arm=fixed-parquet\n"
                "arm_position=0\n"
                "segment_parquet_objects=1\n"
                "segment_vortex_objects=0\n"
            )
            (case_root / "segment-layout.txt").write_text(
                "segment_parquet_objects=1\nsegment_vortex_objects=0\n"
            )
            (case / "bench_build.csv").write_text(
                "ingest_ms,compaction_ms,segment_bytes,total_active_index_bytes\n"
                "10,5,2048,4096\n"
            )
            (case / "resources.csv").write_text(
                "elapsed_ms,cpu_percent,rss_bytes\n0,0,100\n100,50,200\n300,25,150\n"
            )
            timing_header = ",".join(QUERY_STAGE_TIMING_FIELDS)
            timing_values = ",".join("0" for _ in QUERY_STAGE_TIMING_FIELDS)
            (case / "bench_query_samples.csv").write_text(
                "schema_version,scan_codec,cache_execution,phase,mode,nprobe,max_candidates,sample_index,latency_ms,"
                "query_source_index,recall_at_10,network_gets,backing_reads,bytes_read,backing_bytes_read,segments_searched,"
                "global_leaf_directory_reads,global_leaf_directory_bytes,global_leaf_code_pages_read,global_leaf_code_bytes,global_leaf_pages_read,global_leaf_page_bytes,global_leaf_waves,global_leaf_continuations,global_leaf_exact_scores,execution_engine,query_seed,repetition_id,global_leaf_code_requests,global_leaf_exact_requests,global_leaf_exact_cells,global_leaf_exact_cards,global_leaf_deepest_winning_card_rank,global_leaf_exact_groups,global_leaf_exact_selected_bytes,global_leaf_exact_speculative_bytes,"
                f"{timing_header}\n"
                "borsuk-production-bench-v18,srht-pq-scan,scan,uncached,srht-pq-scan,8,320,0,1.5,17,0.99,3,7,2048,1024,8,0,0,0,0,0,0,0,0,0,srht-pq-scan,17,r01,0,0,0,0,0,0,0,0,"
                f"{timing_values}\n"
                "borsuk-production-bench-v18,srht-pq-scan,scan,disk_cached,srht-pq-scan,8,320,0,0.5,17,0.99,0,0,128,0,8,0,0,0,0,0,0,0,0,0,srht-pq-scan,17,r01,0,0,0,0,0,0,0,0,"
                f"{timing_values}\n"
            )

            rows = assemble.assemble_rows(root, minimum_samples=1)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["build_ms"], "15.000000")
            self.assertEqual(rows[0]["segment_bytes"], 2048)
            self.assertEqual(rows[0]["total_active_index_bytes"], 4096)
            self.assertEqual(rows[0]["peak_rss_bytes"], 200)
            self.assertEqual(rows[0]["cpu_core_ms"], "100.000000")
            self.assertEqual(rows[0]["query_position"], "0")
            self.assertEqual(rows[0]["query_source_index"], "17")
            self.assertEqual(rows[0]["physical_requests"], "7")
            self.assertEqual(rows[0]["bytes_read"], "1024")

    def test_physical_request_metric_uses_network_gets_only_for_s3(self) -> None:
        row = {"network_gets": "3", "backing_reads": "7"}
        self.assertEqual(assemble._physical_requests(row, "local_disk"), "7")
        self.assertEqual(assemble._physical_requests(row, "s3"), "3")

    def test_rejects_a_global_pq_sample_disguised_as_segment_evidence(self) -> None:
        row = {
            "phase": "uncached",
            "mode": "srht-pq-scan",
            "nprobe": "8",
            "max_candidates": "320",
            "schema_version": "borsuk-production-bench-v18",
            "segments_searched": "8",
            "global_leaf_directory_reads": "1",
            "global_leaf_directory_bytes": "1",
            "global_leaf_code_pages_read": "1",
            "global_leaf_code_bytes": "1",
            "global_leaf_pages_read": "1",
            "global_leaf_page_bytes": "1",
            "global_leaf_waves": "1",
            "global_leaf_continuations": "0",
            "global_leaf_exact_scores": "1",
        }
        with self.assertRaisesRegex(ValueError, "global-leaf"):
            assemble._validate_segment_path(row, "case")

    def test_rejects_an_unversioned_production_benchmark_sample(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "unsupported production benchmark schema"
        ):
            assemble._validate_segment_path(
                {
                    "segments_searched": "8",
                    "global_leaf_directory_reads": "0",
                    "global_leaf_directory_bytes": "0",
                    "global_leaf_pages_read": "0",
                    "global_leaf_page_bytes": "0",
                    "global_leaf_waves": "0",
                    "global_leaf_continuations": "0",
                    "global_leaf_exact_scores": "0",
                },
                "case",
            )

    def test_rejects_off_cohort_or_mixed_schema_rows_before_selection(self) -> None:
        valid = {
            "schema_version": "borsuk-production-bench-v18",
            "global_leaf_directory_reads": "0",
            "global_leaf_directory_bytes": "0",
            "global_leaf_code_pages_read": "0",
            "global_leaf_code_bytes": "0",
            "global_leaf_pages_read": "0",
            "global_leaf_page_bytes": "0",
            "global_leaf_waves": "0",
            "global_leaf_continuations": "0",
            "global_leaf_exact_scores": "0",
            "backing_reads": "0",
            "backing_bytes_read": "0",
            "global_leaf_exact_cells": "0",
            "global_leaf_exact_cards": "0",
            "global_leaf_exact_groups": "0",
            "global_leaf_exact_selected_bytes": "0",
            "global_leaf_exact_speculative_bytes": "0",
            **{field: "0" for field in QUERY_STAGE_TIMING_FIELDS},
        }
        off_cohort_v9 = {
            **valid,
            "schema_version": "borsuk-production-bench-v9",
            "phase": "disk_cached",
        }
        with self.assertRaisesRegex(ValueError, "schema"):
            assemble._validate_query_sample_schema_rows([valid, off_cohort_v9], "case")

        missing_telemetry = valid.copy()
        missing_telemetry.pop("global_leaf_waves")
        with self.assertRaisesRegex(ValueError, "telemetry"):
            assemble._validate_query_sample_schema_rows([missing_telemetry], "case")

    def test_rejects_raw_sample_identity_that_disagrees_with_schedule(self) -> None:
        row = {
            "scan_codec": "srht-pq-scan",
            "cache_execution": "scan",
            "execution_engine": "srht-pq-scan",
            "query_seed": "999",
            "repetition_id": "r01",
        }
        case = {"query_seed": "17", "repetition_id": "r01"}
        with self.assertRaisesRegex(ValueError, "query_seed"):
            assemble._validate_sample_identity(row, case, "case")

    def test_rejects_non_finite_or_out_of_range_query_measurements(self) -> None:
        with self.assertRaisesRegex(ValueError, "latency_ms"):
            assemble._validate_sample_values(
                {"latency_ms": "nan", "recall_at_10": "0.99"}, "case"
            )
        with self.assertRaisesRegex(ValueError, "recall_at_10"):
            assemble._validate_sample_values(
                {"latency_ms": "1.5", "recall_at_10": "1.01"}, "case"
            )

    def test_rejects_case_without_complete_identity_and_layout_proof(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "environment.txt").write_text(f"source_sha256={'ab' * 32}\n")
            case = {
                "repetition_id": "r01",
                "query_seed": "17",
                "dataset": "fashion",
                "backend": "local_disk",
                "arm": "fixed-parquet",
                "arm_position": "0",
                "case_id": "r01/fashion/local_disk/fixed-parquet",
            }
            with self.assertRaisesRegex(ValueError, "CASE_COMPLETE"):
                assemble._validate_case_proof(root, case, "ab" * 32)

    def test_rejects_under_sampled_cases(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "schedule.csv").write_text(
                "repetition_id,query_seed,dataset,backend,arm,arm_position,case_id\n"
            )
            with self.assertRaisesRegex(ValueError, "schedule is empty"):
                assemble.assemble_rows(root, minimum_samples=1)

    def test_rejects_schedule_that_deviates_from_frozen_protocol(self) -> None:
        protocol = {
            "datasets": ["fashion-mnist-784", "glove-100"],
            "backends": ["local_disk", "s3"],
            "repetitions": 5,
            "queries_per_repetition": 100,
            "query_seeds": [20260727, 20260728, 20260729, 20260730, 20260731],
            "baseline_arm": "fixed-parquet",
            "candidate_arms": [
                "fixed-vortex-full",
                "fixed-vortex-range",
                "mixed-vortex-full",
                "mixed-vortex-range",
            ],
        }
        schedule = [
            {
                "repetition_id": "r01",
                "query_seed": "20260727",
                "dataset": "replacement-corpus",
                "backend": "local_disk",
                "arm": "fixed-parquet",
                "arm_position": "0",
                "case_id": "r01/replacement-corpus/local_disk/fixed-parquet",
            }
        ]

        with self.assertRaisesRegex(ValueError, "schedule does not exactly match"):
            assemble._validate_schedule_contract(schedule, protocol)


if __name__ == "__main__":
    unittest.main()
