import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts.production_bench_schema import QUERY_STAGE_AGGREGATE_FIELDS
from scripts.publication_v3_clones import (
    build_clone_receipt,
    clone_receipt_document_sha256,
)
from scripts.publication_v3_protocol import (
    build_schedule_document,
    canonical_json_bytes,
    validate_manifest,
)
from scripts.publication_v3_receipts import build_index_receipt, receipt_document_sha256
from scripts.publication_v3_results import validate_cell_result
from scripts.run_publication_v3_cell import (
    PRODUCTION_BUILD_FIELDS,
    authorize_publication_mutation_runtime,
    authorize_publication_runtime,
    build_execution_plan,
    build_lifecycle_publication_report,
    build_publication_report,
    build_receipt_metrics,
    build_smoke_report,
    claim_ineligible_lifecycle_diagnostic,
    concurrency_result_arm,
    disk_cached_cohort_authority,
    execute_plan,
    execute_plan_with_resources,
    execute_publication_phase,
    lifecycle_batch_records,
    plan_arms,
    read_build_artifact,
    reconcile_concurrency_storage,
    reconcile_lifecycle_storage_trace,
    reconcile_read_storage_trace,
    runtime_execution_contract,
    runtime_expected_cache_cohort_size,
    runtime_flow_control_authority,
    smoke_cache_cohort_authority,
    summarize_concurrency_artifacts,
    summarize_lifecycle_artifacts,
    summarize_query_samples,
    summarize_read_diagnostic_samples,
    summarize_runtime_write_trace,
    summarize_v21_feasibility_artifacts,
    validate_and_canonicalize_v21_summary,
    validate_publication_cell_authority,
    validate_query_cache_cohort,
)
from scripts.test_publication_v3_protocol import paid_v3_manifest
from scripts.test_publication_v3_receipts import (
    build_artifact,
    build_metrics,
    data_roster,
)
from scripts.test_publication_v3_results import runtime_attestation_for


def scheduled_cell(
    *, system: str = "borsuk", kind: str = "read-recall"
) -> dict[str, object]:
    manifest = validate_manifest(paid_v3_manifest())
    return next(
        cell
        for cell in build_schedule_document(manifest)["cells"]
        if cell["system"] == system and cell["workload"]["kind"] == kind
    )


def runtime_flow_control(profile: str = "recall") -> dict[str, int]:
    values = {
        "disk_cache_max_bytes": 0,
        "exact_read_max_physical_amplification": 2,
        "max_active_searches": 4,
        "max_waiting_searches": 16,
        "leaf_read_width": 32,
        "max_inflight_leaf_reads": 48,
        "max_parallel_decode_rank_tasks": 1,
        "cpu_threads": 3,
        "io_threads": 88,
        "s3_get_concurrency": 64,
        "ram_budget_bytes": 3 * 1024 * 1024 * 1024,
    }
    if profile == "concurrency":
        values.update(
            {
                "max_active_searches": 16,
                "max_waiting_searches": 64,
                "max_inflight_leaf_reads": 96,
                "io_threads": 160,
                "s3_get_concurrency": 128,
            }
        )
    return values


def v21_feasibility_fixture() -> tuple[
    list[dict[str, str]], list[dict[str, str]], dict[str, object]
]:
    arms: list[dict[str, str]] = []
    samples: list[dict[str, str]] = []
    summaries: list[dict[str, object]] = []
    arm_index = 0
    for bundle_row_limit in (128, 256):
        for selector_span in (32, 64):
            for hedge_delay_ms in (None, 20, 35):
                directory_bytes = 1_000 + arm_index
                transient_bytes = 59_392
                peak_bytes = 700_000_000 - 100_000_000 + directory_bytes + transient_bytes
                maximum_requests = 2 + int(hedge_delay_ms is not None)
                arm = {
                    "schema": "borsuk-v21-selector-feasibility-v1",
                    "arm_index": str(arm_index),
                    "bundle_row_limit": str(bundle_row_limit),
                    "selector_span": str(selector_span),
                    "hedge_delay_ms": "off" if hedge_delay_ms is None else str(hedge_delay_ms),
                    "bundle_count": "3",
                    "region_count": "5",
                    "projected_directory_bytes": str(directory_bytes),
                    "replaced_v20_root_bytes": "100000000",
                    "v20_root_checksum": "b" * 64,
                    "baseline_rss_bytes": "700000000",
                    "projected_query_transient_bytes": str(transient_bytes),
                    "projected_peak_rss_bytes": str(peak_bytes),
                    "gt_coverage": "0.50000000000000000",
                    "recall_at_10": "0.50000000000000000",
                    "maximum_actual_requests": str(maximum_requests),
                    "maximum_physical_bytes": "8192",
                    "selector_within_frozen_cap": "true",
                    "eligible": "false",
                    "rows": "100",
                }
                arms.append(arm)
                summaries.append(
                    {
                        "arm_index": arm_index,
                        "bundle_row_limit": bundle_row_limit,
                        "selector_span": selector_span,
                        "hedge_delay_ms": hedge_delay_ms,
                        "bundle_count": 3,
                        "region_count": 5,
                        "projected_directory_bytes": directory_bytes,
                        "replaced_v20_root_bytes": 100_000_000,
                        "selector_within_frozen_cap": True,
                        "rows": 100,
                        "gt_coverage": 0.5,
                        "recall_at_10": 0.5,
                        "maximum_actual_requests": maximum_requests,
                        "maximum_physical_bytes": 8_192,
                        "projected_query_transient_bytes": transient_bytes,
                        "projected_peak_rss_bytes": peak_bytes,
                        "eligible": False,
                    }
                )
                for query_index, hits in enumerate((10, 0)):
                    first_bundle_rejected = query_index == 1
                    samples.append(
                        {
                            "schema": "borsuk-v21-selector-feasibility-v1",
                            "arm_index": str(arm_index),
                            "query_index": str(query_index),
                            "query_source_index": str((7, 11)[query_index]),
                            "routed_cells": "4",
                            "selected_rows": "0" if first_bundle_rejected else "100",
                            "selected_bundles": "0" if first_bundle_rejected else "3",
                            "primary_requests": "0" if first_bundle_rejected else "2",
                            "maximum_actual_requests": (
                                "0" if first_bundle_rejected else str(maximum_requests)
                            ),
                            "selected_bytes": "0" if first_bundle_rejected else "4096",
                            "physical_bytes": "0" if first_bundle_rejected else "8192",
                            "gt_hits": str(hits),
                            "recall_hits": str(hits),
                            "limiting_bound": (
                                "first_bundle" if first_bundle_rejected else "exhausted"
                            ),
                        }
                    )
                arm_index += 1
    summary: dict[str, object] = {
        "schema": "borsuk-v21-selector-feasibility-v1",
        "claim_eligible": False,
        "dataset_name": "deep-image-10m",
        "dataset_id": "deep-image-96",
        "index_id": "index-abc",
        "source_archive_sha256": "a" * 64,
        "v20_root_checksum": "b" * 64,
        "dataset_rows": 100,
        "dimensions": 96,
        "query_seed": 23001,
        "query_source_indices": [7, 11],
        "arm_count": 12,
        "sample_count": 24,
        "baseline_rss_bytes": 700_000_000,
        "minimum_arm_gt_coverage": 0.5,
        "minimum_arm_recall_at_10": 0.5,
        "maximum_actual_requests": 3,
        "maximum_physical_bytes": 8_192,
        "eligible_arm_indexes": [],
        "arms": summaries,
    }
    return arms, samples, summary


def concurrency_artifact_fixture(
    *, cache_profile: str, disk_bytes: int, backing_bytes: int, decoded_bytes: int
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    summary = {
        "schema_version": "borsuk-production-bench-v20",
        "scan_codec": "fast-turboquant-scan",
        "execution_engine": "bounded-cell-card-v20",
        "nprobe": "32",
        "max_candidates": "512",
        "cache_profile": cache_profile,
        "target_cache_coverage_percent": "0",
        "workers": "1",
        "total_queries": "1",
        "qps": "10",
        "mean_ms": "5",
        "p50_ms": "5",
        "p95_ms": "5",
        "p99_ms": "5",
        "max_ms": "5",
    }
    sample = {
        "schema_version": "borsuk-production-bench-v20",
        "scan_codec": "fast-turboquant-scan",
        "execution_engine": "bounded-cell-card-v20",
        "nprobe": "32",
        "max_candidates": "512",
        "cache_profile": cache_profile,
        "target_cache_coverage_percent": "0",
        "workers": "1",
        "sample_index": "0",
        "query_source_index": "100",
        "cache_cohort_index": "0",
        "cache_cohort_size": "0",
        "cache_cohort_count": "0",
        "latency_ms": "5",
        "recall_at_10": "0.99",
        "network_gets": "1",
        "disk_cache_reads": "1" if disk_bytes else "0",
        "bytes_read": str(disk_bytes + backing_bytes + decoded_bytes),
        "decoded_cache_bytes_read": str(decoded_bytes),
        "disk_cache_bytes_read": str(disk_bytes),
        "backing_bytes_read": str(backing_bytes),
        "global_base_approximate_us": "1",
        "global_base_head_admission_us": "2",
        "global_base_head_fetch_us": "3",
        "global_base_head_read_attempts": "1" if backing_bytes else "0",
        "global_base_head_read_successes": "1" if backing_bytes else "0",
        "global_base_head_read_response_bytes": str(backing_bytes // 2),
        "global_base_head_read_us_max": "2" if backing_bytes else "0",
        "global_base_head_read_us_sum": "2" if backing_bytes else "0",
        "global_base_head_read_queue_us_max": "1" if backing_bytes else "0",
        "global_base_head_read_queue_us_sum": "1" if backing_bytes else "0",
        "global_base_head_reads_over_20ms": "0",
        "global_base_head_reads_over_30ms": "0",
        "global_base_head_reads_over_50ms": "0",
        "global_base_head_reads_over_100ms": "0",
        "global_base_head_decode_admission_us": "4",
        "global_base_head_decode_us": "5",
        "global_base_exact_admission_us": "6",
        "global_base_exact_fetch_us": "10",
        "global_base_exact_read_attempts": "1" if backing_bytes else "0",
        "global_base_exact_read_successes": "1" if backing_bytes else "0",
        "global_base_exact_read_response_bytes": str(backing_bytes - backing_bytes // 2),
        "global_base_exact_read_queue_us_max": "1" if backing_bytes else "0",
        "global_base_exact_read_queue_us_sum": "1" if backing_bytes else "0",
        "global_base_exact_read_us_max": "8" if backing_bytes else "0",
        "global_base_exact_read_us_sum": "8" if backing_bytes else "0",
        "global_base_exact_reads_over_20ms": "0",
        "global_base_exact_reads_over_30ms": "0",
        "global_base_exact_reads_over_50ms": "0",
        "global_base_exact_reads_over_100ms": "0",
        "global_base_exact_cpu_us": "7",
        "global_base_exact_rerank_us": "23",
    }
    return [summary], [sample]


def query_artifact_fixture(*, decoded_bytes: int) -> dict[str, str]:
    _, samples = concurrency_artifact_fixture(
        cache_profile="uncached",
        disk_bytes=0,
        backing_bytes=75,
        decoded_bytes=decoded_bytes,
    )
    return {
        **samples[0],
        "phase": "uncached",
        "mode": "srht-pq-scan",
        "scan_codec": "srht-pq-scan",
        "cache_cohort_index": "0",
        "cache_cohort_size": "0",
        "cache_cohort_count": "0",
        "global_leaf_code_pages_read": "7",
        "global_leaf_code_requests": "2",
        "global_leaf_code_bytes": "30",
        "global_leaf_pages_read": "4",
        "global_leaf_exact_requests": "1",
        "global_leaf_page_bytes": "45",
        "global_leaf_exact_scores": "512",
        "global_base_head_read_attempts": "2",
        "global_base_head_read_successes": "2",
        "global_base_head_read_response_bytes": "30",
        "global_base_exact_read_response_bytes": "45",
    }


class PublicationV3CellRunnerTests(unittest.TestCase):
    def test_v21_summary_is_validated_before_cross_language_canonicalization(
        self,
    ) -> None:
        arms, samples, summary = v21_feasibility_fixture()
        noncanonical = json.dumps(summary).encode("utf-8") + b"\n"
        self.assertNotEqual(noncanonical, canonical_json_bytes(summary) + b"\n")
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "bench_v21_feasibility_summary.json"
            path.write_bytes(noncanonical)
            report = validate_and_canonicalize_v21_summary(
                path,
                arms,
                samples,
                expected_source_archive_sha256="a" * 64,
                expected_index_id="index-abc",
                expected_dataset_id="deep-image-96",
                expected_queries=2,
                expected_dataset_rows=100,
                expected_query_seed=23001,
                expected_dimensions=96,
            )
            self.assertEqual(report["status"], "complete")
            self.assertEqual(path.read_bytes(), canonical_json_bytes(summary) + b"\n")

            drifted = copy.deepcopy(summary)
            drifted["query_seed"] = 23002
            drifted_bytes = json.dumps(drifted).encode("utf-8") + b"\n"
            path.write_bytes(drifted_bytes)
            with self.assertRaises(ValueError):
                validate_and_canonicalize_v21_summary(
                    path,
                    arms,
                    samples,
                    expected_source_archive_sha256="a" * 64,
                    expected_index_id="index-abc",
                    expected_dataset_id="deep-image-96",
                    expected_queries=2,
                    expected_dataset_rows=100,
                    expected_query_seed=23001,
                    expected_dimensions=96,
                )
            self.assertEqual(path.read_bytes(), drifted_bytes)

    def test_v21_feasibility_parser_is_exact_claim_ineligible_and_mutation_safe(
        self,
    ) -> None:
        arms, samples, summary = v21_feasibility_fixture()
        report = summarize_v21_feasibility_artifacts(
            arms,
            samples,
            summary,
            expected_source_archive_sha256="a" * 64,
            expected_index_id="index-abc",
            expected_dataset_id="deep-image-96",
            expected_queries=2,
            expected_dataset_rows=100,
            expected_query_seed=23001,
            expected_dimensions=96,
        )
        self.assertEqual(report["document_kind"], "publication-v3-v21-feasibility")
        self.assertEqual(report["claim_eligible"], False)
        self.assertEqual(report["publishable"], False)
        self.assertEqual(len(report["arms"]), 12)

        mutations: list[tuple[list[dict[str, str]], list[dict[str, str]], dict[str, object]]] = []
        missing_arm = copy.deepcopy(arms)
        missing_arm.pop()
        mutations.append((missing_arm, copy.deepcopy(samples), copy.deepcopy(summary)))
        duplicate_arm = copy.deepcopy(arms)
        duplicate_arm[-1] = copy.deepcopy(duplicate_arm[-2])
        mutations.append((duplicate_arm, copy.deepcopy(samples), copy.deepcopy(summary)))
        reordered_arms = copy.deepcopy(arms)
        reordered_arms[0], reordered_arms[1] = reordered_arms[1], reordered_arms[0]
        mutations.append((reordered_arms, copy.deepcopy(samples), copy.deepcopy(summary)))
        missing_sample = copy.deepcopy(samples)
        missing_sample.pop()
        mutations.append((copy.deepcopy(arms), missing_sample, copy.deepcopy(summary)))
        duplicate_sample = copy.deepcopy(samples)
        duplicate_sample[-1] = copy.deepcopy(duplicate_sample[-2])
        mutations.append((copy.deepcopy(arms), duplicate_sample, copy.deepcopy(summary)))
        reordered_samples = copy.deepcopy(samples)
        reordered_samples[0], reordered_samples[1] = reordered_samples[1], reordered_samples[0]
        mutations.append((copy.deepcopy(arms), reordered_samples, copy.deepcopy(summary)))
        drifted_hit = copy.deepcopy(samples)
        drifted_hit[0]["gt_hits"] = "9"
        mutations.append((copy.deepcopy(arms), drifted_hit, copy.deepcopy(summary)))
        drifted_recall = copy.deepcopy(samples)
        drifted_recall[0]["recall_hits"] = "9"
        mutations.append((copy.deepcopy(arms), drifted_recall, copy.deepcopy(summary)))
        drifted_request = copy.deepcopy(samples)
        drifted_request[0]["maximum_actual_requests"] = "4"
        mutations.append((copy.deepcopy(arms), drifted_request, copy.deepcopy(summary)))
        drifted_bytes = copy.deepcopy(samples)
        drifted_bytes[0]["physical_bytes"] = "8193"
        mutations.append((copy.deepcopy(arms), drifted_bytes, copy.deepcopy(summary)))
        drifted_capacity = copy.deepcopy(arms)
        drifted_capacity[0]["projected_directory_bytes"] = "40000001"
        mutations.append((drifted_capacity, copy.deepcopy(samples), copy.deepcopy(summary)))
        drifted_eligibility = copy.deepcopy(summary)
        drifted_eligibility["arms"][0]["eligible"] = True
        mutations.append((copy.deepcopy(arms), copy.deepcopy(samples), drifted_eligibility))
        drifted_rows = copy.deepcopy(summary)
        drifted_rows["dataset_rows"] = 101
        mutations.append((copy.deepcopy(arms), copy.deepcopy(samples), drifted_rows))
        drifted_seed = copy.deepcopy(summary)
        drifted_seed["query_seed"] = 23002
        mutations.append((copy.deepcopy(arms), copy.deepcopy(samples), drifted_seed))
        drifted_dimensions = copy.deepcopy(summary)
        drifted_dimensions["dimensions"] = 95
        mutations.append(
            (copy.deepcopy(arms), copy.deepcopy(samples), drifted_dimensions)
        )
        for mutated_arms, mutated_samples, mutated_summary in mutations:
            with self.assertRaises(ValueError):
                summarize_v21_feasibility_artifacts(
                    mutated_arms,
                    mutated_samples,
                    mutated_summary,
                    expected_source_archive_sha256="a" * 64,
                    expected_index_id="index-abc",
                    expected_dataset_id="deep-image-96",
                    expected_queries=2,
                    expected_dataset_rows=100,
                    expected_query_seed=23001,
                    expected_dimensions=96,
                )

    def test_runtime_cache_cohort_authority_binds_read_profiles_to_complete_query_set(
        self,
    ) -> None:
        arm = {"cache_state": "warm"}
        flow = {"disk_cache_max_bytes": 64 * 1024 * 1024 * 1024}
        self.assertEqual(
            runtime_expected_cache_cohort_size(
                arm,
                runtime_profile="recall",
                effective_flow_control=flow,
                effective_queries=1_000,
            ),
            1_000,
        )
        self.assertEqual(
            runtime_expected_cache_cohort_size(
                arm,
                runtime_profile="concurrency",
                effective_flow_control=flow,
                effective_queries=1_000,
            ),
            1_000,
        )
        with self.assertRaisesRegex(ValueError, "complete query set"):
            runtime_expected_cache_cohort_size(
                arm,
                runtime_profile="concurrency",
                effective_flow_control={"disk_cache_max_bytes": 1024**3},
                effective_queries=1_000,
            )

    def test_lifecycle_runtime_has_no_read_cache_cohort(self) -> None:
        arm = plan_arms(scheduled_cell(kind="write-update-delete-compact"))[11]
        self.assertNotIn("cache_state", arm)
        self.assertEqual(
            runtime_expected_cache_cohort_size(
                arm,
                runtime_profile="recall",
                effective_flow_control={"disk_cache_max_bytes": 0},
                effective_queries=1_000,
            ),
            0,
        )

    def test_lifecycle_batch_schedule_balances_only_to_exercise_writers(self) -> None:
        self.assertEqual(lifecycle_batch_records(19_859, 1_024, 16), [1_024] * 19 + [403])
        self.assertEqual(lifecycle_batch_records(1_986, 1_024, 16), [125, 125, *([124] * 14)])
        self.assertEqual(lifecycle_batch_records(17, 1_024, 16), [2, *([1] * 15)])

    def test_query_samples_account_decoded_ram_cache_bytes(self) -> None:
        cell = scheduled_cell()
        cell["queries_per_repetition"] = 1
        metrics = summarize_query_samples(
            [query_artifact_fixture(decoded_bytes=25)],
            cell=cell,
            arm={"k": 10, "leaf_page_budget": 32, "cache_state": "cold"},
            expected_queries=1,
            expected_cache_cohort_size=0,
        )

        self.assertEqual(metrics["storage_bytes_read"], 75)
        self.assertEqual(metrics["decoded_cache_bytes_read"], 25)

    def test_each_cold_query_sample_proves_backing_reads(self) -> None:
        cell = scheduled_cell()
        cell["queries_per_repetition"] = 1
        row = query_artifact_fixture(decoded_bytes=25)
        row.update(
            {
                "network_gets": "0",
                "bytes_read": "25",
                "backing_bytes_read": "0",
            }
        )

        with self.assertRaisesRegex(ValueError, "performed no backing reads"):
            summarize_query_samples(
                [row],
                cell=cell,
                arm={"k": 10, "leaf_page_budget": 32, "cache_state": "cold"},
                expected_queries=1,
                expected_cache_cohort_size=0,
            )

    def test_query_sample_reconciles_planned_ranges_with_physical_reads(self) -> None:
        cell = scheduled_cell()
        cell["queries_per_repetition"] = 1
        row = query_artifact_fixture(decoded_bytes=25)
        row["global_base_head_read_attempts"] = "1"
        row["global_base_head_read_successes"] = "1"

        with self.assertRaisesRegex(ValueError, "planner/read telemetry"):
            summarize_query_samples(
                [row],
                cell=cell,
                arm={"k": 10, "leaf_page_budget": 32, "cache_state": "cold"},
                expected_queries=1,
                expected_cache_cohort_size=0,
            )

    def test_uncached_concurrency_allows_in_wave_local_disk_hits(self) -> None:
        summaries, samples = concurrency_artifact_fixture(
            cache_profile="uncached",
            disk_bytes=40,
            backing_bytes=60,
            decoded_bytes=0,
        )

        metrics = summarize_concurrency_artifacts(
            summaries,
            samples,
            expected_workers=(1,),
            expected_queries=1,
            minimum_recall_ppm=980_000,
            expected_scan_codec="fast-turboquant-scan",
            expected_nprobe=32,
            expected_max_candidates=512,
            expected_cache_profile="uncached",
            expected_cache_coverage_percent=0,
            expected_cache_cohort_size=0,
        )

        self.assertEqual(metrics[0]["storage_bytes_read"], 60)
        self.assertEqual(metrics[0]["disk_cache_bytes_read"], 40)

    def test_each_uncached_concurrency_profile_proves_backing_reads(self) -> None:
        summaries, samples = concurrency_artifact_fixture(
            cache_profile="uncached",
            disk_bytes=40,
            backing_bytes=60,
            decoded_bytes=0,
        )
        second_summary = copy.deepcopy(summaries[0])
        second_summary["workers"] = "2"
        second_sample = copy.deepcopy(samples[0])
        second_sample.update(
            {
                "workers": "2",
                "network_gets": "0",
                "bytes_read": "100",
                "disk_cache_bytes_read": "100",
                "backing_bytes_read": "0",
            }
        )

        with self.assertRaisesRegex(ValueError, "performed no backing reads"):
            summarize_concurrency_artifacts(
                [*summaries, second_summary],
                [*samples, second_sample],
                expected_workers=(1, 2),
                expected_queries=1,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="uncached",
                expected_cache_coverage_percent=0,
                expected_cache_cohort_size=0,
            )

    def test_concurrency_accounts_decoded_ram_cache_bytes(self) -> None:
        summaries, samples = concurrency_artifact_fixture(
            cache_profile="uncached",
            disk_bytes=25,
            backing_bytes=50,
            decoded_bytes=25,
        )

        metrics = summarize_concurrency_artifacts(
            summaries,
            samples,
            expected_workers=(1,),
            expected_queries=1,
            minimum_recall_ppm=980_000,
            expected_scan_codec="fast-turboquant-scan",
            expected_nprobe=32,
            expected_max_candidates=512,
            expected_cache_profile="uncached",
            expected_cache_coverage_percent=0,
            expected_cache_cohort_size=0,
        )

        self.assertEqual(metrics[0]["storage_bytes_read"], 50)
        self.assertEqual(metrics[0]["disk_cache_bytes_read"], 25)
        self.assertEqual(metrics[0]["decoded_cache_bytes_read"], 25)

    def test_lifecycle_artifacts_require_exact_operations_and_visibility_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            (output / "bench_write_costs.csv").write_text(
                "op,configured_writers,configured_batch_records,ops,batches,wall_ms,ops_per_s,mean_batch_ms,stddev_batch_ms,p50_batch_ms,p95_batch_ms,p99_batch_ms,max_batch_ms,mean_amortized_ms,gets,puts,deletes,heads,lists,bytes_read,bytes_written\n"
                "insert,1,64,100,2,20,5000,10,1,8,12,14,15,0.2,1,10,0,2,0,100,2000\n"
                "flush,1,64,1,1,3,0.333,3,0,3,3,3,3,3,2,3,0,1,0,400,500\n"
                "consolidate,1,64,1,1,6,0.167,6,0,6,6,6,6,6,4,5,0,1,0,600,700\n"
                "upsert,1,64,100,2,10,10000,5,1,4,6,7,8,0.1,1,8,0,2,0,100,1000\n"
                "delete,1,64,100,2,8,12500,4,1,3,5,6,7,0.08,1,8,0,2,0,100,1000\n"
                "compact,1,64,1,1,9,0.111,9,0,9,9,9,9,9,1,2,0,1,0,1000,3000\n"
                "purge,1,64,1,1,4,0.25,4,0,4,4,4,4,4,1,2,3,1,0,0,0\n",
                encoding="utf-8",
            )
            (output / "bench_write_samples.csv").write_text(
                "op,writer_index,wave_index,batch_index,batch_records,batch_latency_ms,amortized_ms,gets,puts,deletes,heads,lists\n"
                "insert,0,0,0,64,8,0.125,0,1,0,0,0\n"
                "insert,0,1,1,36,15,0.417,1,9,0,2,0\n"
                "flush,0,0,0,100,3,0.03,2,3,0,1,0\n"
                "consolidate,0,0,0,100,6,0.06,4,5,0,1,0\n"
                "upsert,0,0,0,64,4,0.063,0,4,0,1,0\n"
                "upsert,0,1,1,36,8,0.222,1,4,0,1,0\n"
                "delete,0,0,0,64,3,0.047,0,4,0,1,0\n"
                "delete,0,1,1,36,7,0.194,1,4,0,1,0\n"
                "compact,0,0,0,100,9,0.09,1,2,0,1,0\n"
                "purge,0,0,0,100,4,0.04,1,2,3,1,0\n",
                encoding="utf-8",
            )
            (output / "bench_lifecycle.csv").write_text(
                "configured_writers,configured_batch_records,inserted_vectors,logical_vector_bytes,insert_wall_ms,insert_vectors_per_s,first_batch_publish_ms,searchability_refresh_ms,time_to_searchable_ms,searchable_samples,searchable_fraction,upsert_samples,upsert_correct_fraction,delete_samples,delete_absent_fraction,compact_delete_absent_fraction,purge_delete_absent_fraction,delta_flush_ms,time_to_fully_indexed_ms,wal_publish_bytes,indexed_delta_bytes,total_indexing_bytes,write_amplification,write_amplification_is_lower_bound,consolidation_ms,time_to_consolidated_ms,consolidated_global_bytes,consolidation_amplification\n"
                "1,64,100,200,20,5000,8,1,21,16,1,16,1,16,1,1,1,3,24,2000,1000,3000,15,true,6,30,4000,20\n",
                encoding="utf-8",
            )

            summary = summarize_lifecycle_artifacts(
                output, expected_batch_size=64, expected_writers=1
            )

            sample_path = output / "bench_write_samples.csv"
            canonical_samples = sample_path.read_text(encoding="utf-8")
            sample_path.write_text(
                canonical_samples.replace(
                    "insert,0,0,0,64,8", "insert,0,0,0,50,8"
                ).replace("insert,0,1,1,36,15", "insert,0,1,1,50,15"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "batch schedule"):
                summarize_lifecycle_artifacts(
                    output, expected_batch_size=64, expected_writers=1
                )
            sample_path.write_text(canonical_samples, encoding="utf-8")

            sample_path.write_text(
                canonical_samples.replace(
                    "insert,0,0,0,64,8,0.125,0,1,0,0,0",
                    "insert,0,0,0,64,8,0.125,9,1,0,0,0",
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "sample request totals"):
                summarize_lifecycle_artifacts(
                    output, expected_batch_size=64, expected_writers=1
                )
            sample_path.write_text(canonical_samples, encoding="utf-8")

            # Merely labeling a serial artifact as writers=4 must not satisfy
            # the concurrency arm. Batch 1 belongs to writer 1 in wave 0, but
            # every sample below still claims writer 0.
            costs_path = output / "bench_write_costs.csv"
            costs_path.write_text(
                costs_path.read_text(encoding="utf-8").replace(",1,64,", ",4,64,"),
                encoding="utf-8",
            )
            lifecycle_path = output / "bench_lifecycle.csv"
            serial_lifecycle = lifecycle_path.read_text(encoding="utf-8")
            lifecycle_path.write_text(
                serial_lifecycle.replace("1,64,100,200", "4,64,100,200"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "writer wave"):
                summarize_lifecycle_artifacts(
                    output, expected_batch_size=64, expected_writers=4
                )
            costs_path.write_text(
                costs_path.read_text(encoding="utf-8").replace(",4,64,", ",1,64,"),
                encoding="utf-8",
            )
            lifecycle_path.write_text(serial_lifecycle, encoding="utf-8")

            valid_lifecycle = lifecycle_path.read_text(encoding="utf-8")
            lifecycle_path.write_text(
                valid_lifecycle.replace("1,64,100,200", "1,64,99,200"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "operation totals"):
                summarize_lifecycle_artifacts(
                    output, expected_batch_size=64, expected_writers=1
                )

        self.assertEqual(summary["insert_ops"], 100)
        self.assertEqual(summary["flush_ops"], 1)
        self.assertEqual(summary["consolidate_ops"], 1)
        self.assertEqual(summary["upsert_ops"], 100)
        self.assertEqual(summary["delete_ops"], 100)
        self.assertEqual(summary["lifecycle_accuracy_ppm"], 1_000_000)
        self.assertEqual(summary["batch_latency_p50_us"], 7_000)
        self.assertEqual(summary["batch_latency_p95_us"], 15_000)
        self.assertEqual(summary["throughput_milli_per_second"], 7_894_737)
        self.assertEqual(summary["time_to_consolidated_us"], 30_000)
        self.assertEqual(summary["storage_gets"], 11)
        self.assertEqual(summary["storage_puts"], 38)
        self.assertEqual(summary["storage_bytes_read"], 2300)
        self.assertEqual(summary["storage_bytes_written"], 8200)

    def test_publication_cell_must_match_the_frozen_manifest_prefix_authority(
        self,
    ) -> None:
        cell = scheduled_cell()
        with tempfile.TemporaryDirectory() as root:
            manifest_path = Path(root) / "manifest.json"
            manifest_path.write_bytes(
                canonical_json_bytes(validate_manifest(paid_v3_manifest()))
            )
            self.assertEqual(
                validate_publication_cell_authority(cell, manifest_path), cell
            )
            index_root, index_name = cell["index_prefix"].rsplit("/", 1)
            retry = {
                **cell,
                "index_prefix": f"{index_root}/build-attempts/0002/{index_name}",
            }
            self.assertEqual(
                validate_publication_cell_authority(retry, manifest_path), retry
            )
            manifest_path.write_bytes(manifest_path.read_bytes() + b"\n")
            with self.assertRaisesRegex(ValueError, "frozen manifest is not canonical"):
                validate_publication_cell_authority(cell, manifest_path)
            manifest_path.write_bytes(
                canonical_json_bytes(validate_manifest(paid_v3_manifest()))
            )
            substituted = {
                **cell,
                "index_prefix": "s3://attacker-bucket/substituted/"
                + cell["index_prefix"].rsplit("/", 1)[1],
            }
            with self.assertRaisesRegex(ValueError, "frozen manifest"):
                validate_publication_cell_authority(substituted, manifest_path)

    def test_publication_runtime_requires_matching_immutable_build_receipt(
        self,
    ) -> None:
        cell = scheduled_cell()
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=plan_arms(cell)[0],
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="runtime",
                runtime_flow_control=runtime_flow_control(),
            )
        for phase in ("build", "runtime"):
            environment = plan[phase]["steps"][-1]["env"]
            self.assertEqual(environment["AWS_REGION"], "eu-central-1")
            self.assertEqual(environment["AWS_DEFAULT_REGION"], "eu-central-1")
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type=cell["environment_contract"]["build_workers"][
                "borsuk"
            ]["instance_type"],
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        runtime = authorize_publication_runtime(
            plan,
            receipt=receipt,
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
        )
        self.assertEqual(
            runtime["index_receipt_sha256"], receipt_document_sha256(receipt)
        )
        self.assertEqual(runtime["steps"], plan["runtime"]["steps"])
        self.assertNotIn("build", runtime)
        with self.assertRaises(ValueError):
            authorize_publication_runtime(
                plan,
                receipt={**receipt, "index_uri": "s3://attacker/substitute"},
                cell=cell,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
            )

    def test_lifecycle_runtime_requires_a_fresh_verified_clone_and_never_mutates_base(
        self,
    ) -> None:
        cell = scheduled_cell(kind="write-update-delete-compact")
        arm = next(arm for arm in plan_arms(cell) if arm["writers"] == 4)
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="runtime",
                runtime_flow_control=runtime_flow_control(),
            )
        base = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type=cell["environment_contract"]["build_workers"][
                "borsuk"
            ]["instance_type"],
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        with self.assertRaisesRegex(ValueError, "read-only"):
            authorize_publication_runtime(
                plan,
                receipt=base,
                cell=cell,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
            )
        roster = data_roster(cell)
        clone = build_clone_receipt(
            cell=cell,
            arm=arm,
            attempt_id="attempt-01",
            base_receipt=base,
            base_roster=roster,
            copy_inventory=[
                {
                    "path": item["path"],
                    "bytes": item["bytes"],
                    "source_etag": item["etag"],
                    "destination_etag": f'"{index:032x}"',
                }
                for index, item in enumerate(roster)
            ],
        )
        runtime = authorize_publication_mutation_runtime(
            plan,
            clone_receipt=clone,
            base_receipt=base,
            arm=arm,
            attempt_id="attempt-01",
            cell=cell,
        )
        environment = runtime["steps"][-1]["env"]
        self.assertEqual(environment["BORSUK_BENCH_URI"], clone["clone_index_uri"])
        self.assertNotEqual(environment["BORSUK_BENCH_URI"], base["index_uri"])
        self.assertEqual(environment["BORSUK_BENCH_READ_ONLY"], "0")
        self.assertEqual(environment["BORSUK_BENCH_LIFECYCLE_ONLY"], "1")
        self.assertEqual(environment["BORSUK_BENCH_SKIP_RECALL"], "1")
        self.assertEqual(environment["BORSUK_BENCH_WRITE_BATCH_SIZE"], "1")
        self.assertEqual(environment["BORSUK_BENCH_LIFECYCLE_WRITERS"], "4")

        attestation = runtime_attestation_for(cell)
        report = build_lifecycle_publication_report(
            cell=cell,
            arm=arm,
            protocol_bytes=canonical_json_bytes(cell) + b"\n",
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            attempt_id="attempt-01",
            instance_identity="local-test",
            lifecycle_metrics={
                "insert_ops": 1000,
                "flush_ops": 1,
                "consolidate_ops": 1,
                "upsert_ops": 100,
                "delete_ops": 100,
                "compact_ops": 1,
                "purge_ops": 1,
                "lifecycle_accuracy_ppm": 1_000_000,
                "batch_latency_p50_us": 1000,
                "batch_latency_p95_us": 2000,
                "batch_latency_p99_us": 3000,
                "throughput_milli_per_second": 100_000,
                "first_publish_us": 500,
                "time_to_searchable_us": 500,
                "time_to_fully_indexed_us": 4000,
                "time_to_consolidated_us": 9000,
                "write_amplification_ppm": 1_500_000,
            },
            resource_metrics={
                "cpu_ns": 1_000_000,
                "peak_rss_bytes": 10_000_000,
                "disk_read_bytes": 0,
                "disk_write_bytes": 0,
            },
            storage_metrics={
                "storage_gets": 10,
                "storage_puts": 20,
                "storage_bytes_read": 4096,
                "storage_bytes_written": 8192,
                "storage_distinct_data_objects": 20,
                "storage_max_data_object_bytes": 1024,
            },
            index_receipt=base,
            clone_receipt=clone,
            runtime_attestation=attestation,
        )
        self.assertTrue(report["publishable"])
        self.assertEqual(
            report["result"]["clone_receipt_sha256"],
            clone_receipt_document_sha256(clone),
        )

        with self.assertRaisesRegex(ValueError, "writers must be in"):
            authorize_publication_mutation_runtime(
                plan,
                clone_receipt=clone,
                base_receipt=base,
                arm={**arm, "writers": 0},
                attempt_id="attempt-01",
                cell=cell,
            )

        for flag in ("BORSUK_BENCH_LIFECYCLE_ONLY", "BORSUK_BENCH_SKIP_RECALL"):
            for invalid in (None, "0"):
                malformed = copy.deepcopy(plan)
                environment = malformed["runtime"]["steps"][-1]["env"]
                if invalid is None:
                    environment.pop(flag)
                else:
                    environment[flag] = invalid
                with (
                    self.subTest(flag=flag, invalid=invalid),
                    self.assertRaisesRegex(ValueError, "mutation runtime flags"),
                ):
                    authorize_publication_mutation_runtime(
                        malformed,
                        clone_receipt=clone,
                        base_receipt=base,
                        arm=arm,
                        attempt_id="attempt-01",
                        cell=cell,
                    )

    def test_build_identity_and_storage_are_read_from_the_real_benchmark_artifact(
        self,
    ) -> None:
        cell = scheduled_cell()
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            (output / "bench_build.csv").write_text(
                "storage_gets,storage_puts,storage_deletes,storage_heads,storage_lists,storage_bytes_read,storage_bytes_written\n"
                "7,11,0,3,2,654321,123456\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "header differs"):
                read_build_artifact(output, cell=cell)
            row = {field: "0" for field in PRODUCTION_BUILD_FIELDS}
            row.update(
                {
                    "logical_cell_catalog_checksum": "3" * 64,
                    "logical_cells": str(cell["index_profile"]["logical_cells"]),
                    "scan_codec": str(cell["index_profile"]["global_scan_codec"]),
                    "records": str(cell["dataset"]["scale"]["rows"]),
                    "total_active_index_bytes": str(123 * 1024 * 1024),
                    "ingest_ms": "1234.500",
                    "compaction_ms": "67.250",
                    "compaction_bytes_read": "7654321",
                    "compaction_bytes_written": "2345678",
                    "gc_ms": "12.125",
                    "gc_objects_scanned": "2879",
                    "gc_objects_deleted": "275",
                    "gc_transaction_states_remaining": "0",
                    "gc_bytes_read": "345678",
                    "gc_bytes_reclaimed": "456789",
                    "storage_gets": "7",
                    "storage_puts": "11",
                    "storage_deletes": "0",
                    "storage_heads": "3",
                    "storage_lists": "2",
                    "storage_bytes_read": "654321",
                    "storage_bytes_written": "123456",
                    "configured_build_writers": "8",
                    "ingest_batches": "61",
                    "ingest_waves": "8",
                    "ingest_vectors_per_s": "1234.500",
                }
            )
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS)
                + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS)
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "phase timing"):
                read_build_artifact(output, cell=cell)
            phase_names = (
                "logical_cell_routing",
                "positioned_wal_encode",
                "positioned_id_directory_encode",
                "positioned_payload_assembly",
                "positioned_stamp_reduce",
                "positioned_route_facts",
                "positioned_route_plan_build",
                "positioned_route_plan_encode",
                "positioned_transaction_metadata",
                "positioned_append_prepare",
                "record_preparation",
                "id_validation",
                "id_claim_coordination",
                "claim_authorization",
                "positioned_immutable_commit",
                "positioned_install",
                "auto_flush",
                "flush_wal_materialization",
                "quantizer_refresh",
                "segment_centroid_radius",
                "segment_routing_codes",
                "segment_pq_bounds",
                "segment_pq_encode",
                "graph_build",
                "vector_sidecar",
                "filter_index",
                "segment_table",
                "object_puts",
                "voronoi_chunks",
                "compaction_source_read",
                "locality_sort",
            )
            (output / "bench_build_phases.csv").write_text(
                "schema_version,group,phase,nanos,calls\n"
                + "".join(
                    f"2,{group},{phase},{index + 1},{index + 2}\n"
                    for group in ("ingest", "compaction")
                    for index, phase in enumerate(phase_names)
                ),
                encoding="utf-8",
            )
            row["ingest_waves"] = "7"
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS)
                + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS)
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "ingest schedule differs"):
                read_build_artifact(output, cell=cell)
            row["ingest_waves"] = "8"
            row["scan_codec"] = "pq-scan"
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS)
                + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS)
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "codec identity differs"):
                read_build_artifact(output, cell=cell)
            row["scan_codec"] = cell["index_profile"]["global_scan_codec"]
            row["gc_transaction_states_remaining"] = "1"
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS)
                + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS)
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "transaction states remain"):
                read_build_artifact(output, cell=cell)
            row["gc_transaction_states_remaining"] = "0"
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS)
                + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS)
                + "\n",
                encoding="utf-8",
            )
            artifact = read_build_artifact(output, cell=cell)

            turboquant_cell = {
                **cell,
                "index_profile": {
                    **cell["index_profile"],
                    "global_scan_codec": "fast-turboquant-scan",
                    "turboquant_bits": 3,
                    "turboquant_qjl_bits": 0,
                    "turboquant_shards": 1,
                },
            }
            turboquant_cell["index_profile"].pop("code_bytes")
            row.update(
                {
                    "scan_codec": "fast-turboquant-scan",
                    "turboquant_bits": "2",
                    "turboquant_qjl_bits": "0",
                    "turboquant_shards": "1",
                }
            )
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS)
                + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS)
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "codec identity differs"):
                read_build_artifact(output, cell=turboquant_cell)
            row["turboquant_bits"] = "3"
            (output / "bench_build.csv").write_text(
                ",".join(PRODUCTION_BUILD_FIELDS)
                + "\n"
                + ",".join(row[field] for field in PRODUCTION_BUILD_FIELDS)
                + "\n",
                encoding="utf-8",
            )
            read_build_artifact(output, cell=turboquant_cell)
        metrics = artifact["storage_metrics"]
        self.assertEqual(
            artifact["index_stats"]["records"], cell["dataset"]["scale"]["rows"]
        )
        self.assertEqual(
            artifact["build_timings"],
            {
                "ingest_ns": 1_234_500_000,
                "configured_build_writers": 8,
                "ingest_batches": 61,
                "ingest_waves": 8,
                "ingest_vectors_per_s_micros": 1_234_500_000,
                "compaction_ns": 67_250_000,
                "compaction_bytes_read": 7_654_321,
                "compaction_bytes_written": 2_345_678,
                "gc_ns": 12_125_000,
                "gc_objects_scanned": 2_879,
                "gc_objects_deleted": 275,
                "gc_transaction_states_remaining": 0,
                "gc_bytes_read": 345_678,
                "gc_bytes_reclaimed": 456_789,
            },
        )
        self.assertEqual(metrics["storage_puts"], 11)
        self.assertEqual(metrics["storage_bytes_read"], 654321)
        self.assertEqual(metrics["storage_bytes_written"], 123456)
        self.assertEqual(artifact["phase_timings"]["rows"], 62)
        self.assertEqual(len(artifact["phase_timings"]["sha256"]), 64)
        resource = build_receipt_metrics(
            {
                "cpu_ns": 10,
                "peak_rss_bytes": 20,
                "disk_read_bytes": 30,
                "disk_write_bytes": 40,
            },
            metrics,
            elapsed_ns=90,
        )
        self.assertEqual(
            frozenset(resource),
            frozenset(
                {
                    "cpu_ns",
                    "peak_rss_bytes",
                    "disk_read_bytes",
                    "disk_write_bytes",
                    "storage_gets",
                    "storage_puts",
                    "storage_bytes_read",
                    "storage_bytes_written",
                    "storage_deletes",
                    "storage_heads",
                    "storage_lists",
                    "build_elapsed_ns",
                }
            ),
        )

    def test_execution_records_child_cpu_rss_disk_and_elapsed_resources(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            workspace = Path(root)
            output = workspace / "output"
            command = (
                "mkdir -p output; "
                "printf 'schema_version\\nreal\\n' > output/bench_query_samples.csv"
            )
            samples, resources, elapsed_ns = execute_plan_with_resources(
                {
                    "workspace": str(workspace),
                    "output_dir": str(output),
                    "steps": [{"argv": ["/bin/sh", "-c", command], "env": {}}],
                }
            )
        self.assertEqual(samples.name, "bench_query_samples.csv")
        self.assertGreater(elapsed_ns, 0)
        self.assertGreater(resources["cpu_ns"], 0)
        self.assertGreater(resources["peak_rss_bytes"], 0)
        self.assertGreaterEqual(resources["disk_read_bytes"], 0)
        self.assertGreaterEqual(resources["disk_write_bytes"], 0)

    def test_publication_build_and_runtime_execute_as_separate_phases(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            workspace = Path(root)
            plan = {
                "mode": "publication",
                "workspace": str(workspace),
                "build": {
                    "output_dir": str(workspace / "build-output"),
                    "steps": [{"argv": ["/bin/true"], "env": {}}],
                },
                "runtime": {
                    "output_dir": str(workspace / "runtime-output"),
                    "steps": [{"argv": ["/bin/true"], "env": {}}],
                },
            }
            build_output, build_resources, _ = execute_publication_phase(plan, "build")
            runtime_output, runtime_resources, _ = execute_publication_phase(
                plan, "runtime"
            )
        self.assertNotEqual(build_output, runtime_output)
        self.assertGreater(build_resources["cpu_ns"], 0)
        self.assertGreater(runtime_resources["cpu_ns"], 0)

    def test_read_storage_trace_separates_timed_and_setup_backing_io(self) -> None:
        measured = {
            "storage_gets": 2,
            "storage_bytes_read": 600,
            "decoded_cache_bytes_read": 100,
            "disk_cache_bytes_read": 300,
        }
        trace = {
            "storage_gets": 5,
            "storage_puts": 0,
            "storage_bytes_read": 1_600,
            "storage_bytes_written": 0,
            "storage_distinct_data_objects": 0,
            "storage_max_data_object_bytes": 0,
        }
        self.assertEqual(
            reconcile_read_storage_trace(measured, trace),
            {
                "excluded_setup_storage_gets": 3,
                "excluded_setup_storage_bytes_read": 1_000,
            },
        )
        for drift in (
            {**trace, "storage_gets": 1},
            {**trace, "storage_bytes_read": 599},
        ):
            with self.subTest(drift=drift):
                with self.assertRaisesRegex(ValueError, "smaller than measured"):
                    reconcile_read_storage_trace(measured, drift)
        for invalid in (
            {**measured, "storage_gets": True},
            {**trace, "storage_bytes_read": -1},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "storage accounting is invalid"):
                    if "disk_cache_bytes_read" in invalid:
                        reconcile_read_storage_trace(invalid, trace)
                    else:
                        reconcile_read_storage_trace(measured, invalid)

    def test_publication_report_is_a_complete_admissible_result(self) -> None:
        cell = scheduled_cell()
        protocol = canonical_json_bytes(cell) + b"\n"
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type=cell["environment_contract"]["build_workers"][
                "borsuk"
            ]["instance_type"],
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        attestation = runtime_attestation_for(cell, instance_id="i-0123456789abcdef0")
        report = build_publication_report(
            cell=cell,
            arm={
                "k": 10,
                "leaf_page_budget": 32,
                "cache_state": "cold",
            },
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            attempt_id="attempt-01",
            instance_identity="i-0123456789abcdef0",
            elapsed_ns=2_000_000_000,
            query_metrics={
                "queries": 1_000,
                "correctness_ppm": 960_000,
                "latency_p50_us": 1_000,
                "latency_p95_us": 2_000,
                "latency_p99_us": 3_000,
                "storage_gets": 10,
                "storage_bytes_read": 4096,
                "decoded_cache_bytes_read": 0,
                "disk_cache_bytes_read": 0,
                "global_leaf_code_requests": 4,
                "global_leaf_exact_requests": 6,
                "query_elapsed_ns": 1_000_000_000,
                **{field: 0 for field in QUERY_STAGE_AGGREGATE_FIELDS},
            },
            resource_metrics={
                "cpu_ns": 1_500_000_000,
                "peak_rss_bytes": 256 * 1024 * 1024,
                "disk_read_bytes": 8192,
                "disk_write_bytes": 16384,
            },
            runtime_storage_trace={
                "storage_gets": 12,
                "storage_puts": 0,
                "storage_bytes_read": 5_096,
                "storage_bytes_written": 0,
                "storage_distinct_data_objects": 0,
                "storage_max_data_object_bytes": 0,
            },
            index_receipt=receipt,
            runtime_attestation=attestation,
            runtime_profile="concurrency",
        )
        self.assertTrue(report["publishable"])
        self.assertEqual(report["runtime_profile"], "concurrency")
        self.assertEqual(report["result"]["schema_version"], 4)
        self.assertEqual(report["result"]["metrics"]["storage_gets"], 10)
        self.assertEqual(report["result"]["metrics"]["storage_bytes_read"], 4096)
        self.assertEqual(
            report["result"]["metrics"]["decoded_cache_bytes_read"], 0
        )
        self.assertEqual(report["result"]["metrics"]["disk_cache_bytes_read"], 0)
        self.assertEqual(
            report["result"]["metrics"]["excluded_setup_storage_gets"], 2
        )
        self.assertEqual(
            report["result"]["metrics"]["excluded_setup_storage_bytes_read"], 1_000
        )
        self.assertEqual(report["result"]["metrics"]["global_leaf_code_requests"], 4)
        self.assertEqual(report["result"]["metrics"]["global_leaf_exact_requests"], 6)
        admitted = validate_cell_result(
            report["result"],
            cell=cell,
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            index_receipt=receipt,
            runtime_attestation=attestation,
        )
        self.assertEqual(admitted, report["result"])

        with tempfile.TemporaryDirectory() as root:
            trace = Path(root) / "storage-access.csv"
            trace.write_text(
                "operation,object_role,path,physical_format,object_bytes,request_count,bytes_fetched,logical_projection,row_selection,logical_rows_requested,logical_rows_decoded,decode_cpu_ns,cache_state,status\n"
                "write,catalog,collection/CURRENT,json,4096,1,4096,,,,,,write,ok\n"
                "write,normal_segment,segments/a.parquet,parquet,8192,2,8192,,,,,,write,ok\n"
                "write,normal_segment,segments/a.parquet,parquet,8192,1,8192,,,,,,write,conflict\n"
                "write,normal_segment,segments/failed.parquet,parquet,16384,1,16384,,,,,,write,error\n"
                "write,catalog,collection/CURRENT,json,4096,1,4096,,,,,,write,ok\n"
                "write,normal_segment,segments/b.parquet,parquet,2048,1,2048,,,,,,write,ok\n"
                "read,catalog,collection/CURRENT,json,4096,2,1024,,,,,,backing,ok\n",
                encoding="utf-8",
            )
            observed = summarize_runtime_write_trace(trace)
        self.assertEqual(
            observed,
            {
                "storage_gets": 2,
                "storage_puts": 7,
                "storage_bytes_read": 1024,
                "storage_bytes_written": 43_008,
                "storage_distinct_data_objects": 2,
                "storage_max_data_object_bytes": 8192,
            },
        )
        self.assertEqual(
            reconcile_lifecycle_storage_trace(
                {
                    "storage_gets": 2,
                    "storage_puts": 7,
                    "storage_bytes_read": 1024,
                    "storage_bytes_written": 43_008,
                },
                observed,
            ),
            {
                "storage_gets": 2,
                "storage_bytes_read": 1024,
                "storage_distinct_data_objects": 2,
                "storage_max_data_object_bytes": 8192,
            },
        )
        with self.assertRaisesRegex(
            ValueError, "differs from the complete trace"
        ) as mismatch:
            reconcile_lifecycle_storage_trace(
                {
                    "storage_gets": 1,
                    "storage_puts": 4,
                    "storage_bytes_read": 512,
                    "storage_bytes_written": 4096,
                },
                observed,
            )
        self.assertIn("lifecycle_puts=4", str(mismatch.exception))
        self.assertIn("trace_puts=7", str(mismatch.exception))
        self.assertIn("lifecycle_bytes=4096", str(mismatch.exception))
        self.assertIn("trace_bytes=43008", str(mismatch.exception))
        with self.assertRaisesRegex(ValueError, "differs from the complete trace"):
            reconcile_lifecycle_storage_trace(
                {
                    "storage_gets": 2,
                    "storage_puts": 8,
                    "storage_bytes_read": 1024,
                    "storage_bytes_written": 43_008,
                },
                observed,
            )
        with self.assertRaisesRegex(ValueError, "cannot write"):
            validate_cell_result(
                {
                    **report["result"],
                    "metrics": {
                        **report["result"]["metrics"],
                        "storage_puts": observed["storage_puts"],
                        "storage_bytes_written": observed["storage_bytes_written"],
                    },
                },
                cell=cell,
                protocol_bytes=protocol,
                source_archive_sha256="a" * 64,
                dataset_materialization_sha256="d" * 64,
                index_receipt=receipt,
                runtime_attestation=attestation,
            )

    def test_borsuk_read_smoke_plan_invokes_real_generator_and_production_bench(
        self,
    ) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))[
                "cells"
            ]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        with tempfile.TemporaryDirectory() as root:
            arm = plan_arms(cell)[0]
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/opt/borsuk/generate_synthetic_dataset"),
                borsuk_bench=Path("/opt/borsuk/production_bench"),
                mode="smoke",
            )
        self.assertFalse(plan["publishable"])
        self.assertEqual(
            [step["argv"] for step in plan["steps"]],
            [
                ["/opt/borsuk/generate_synthetic_dataset"],
                ["/opt/borsuk/production_bench"],
            ],
        )
        generator_env = plan["steps"][0]["env"]
        benchmark_env = plan["steps"][1]["env"]
        self.assertEqual(int(generator_env["BORSUK_SYNTHETIC_TRAIN"]), 32_800)
        self.assertEqual(
            int(generator_env["BORSUK_SYNTHETIC_DIMENSIONS"]),
            cell["dataset"]["dimensions"],
        )
        self.assertEqual(
            generator_env["BORSUK_SYNTHETIC_SEED"],
            str(cell["dataset"]["source"]["seed"]),
        )
        self.assertEqual(
            benchmark_env["BORSUK_BENCH_QUERY_SEED"], str(cell["query_seed"])
        )
        self.assertEqual(benchmark_env["BORSUK_BENCH_QUERIES"], "10")
        self.assertEqual(
            benchmark_env["BORSUK_BENCH_NPROBES"], str(arm["leaf_page_budget"])
        )
        self.assertEqual(benchmark_env["BORSUK_BENCH_CANDIDATES"], "512")
        self.assertEqual(benchmark_env["BORSUK_BENCH_SKIP_EXACT_RECALL"], "1")
        self.assertEqual(benchmark_env["BORSUK_BENCH_LOGICAL_CELLS"], "128")
        self.assertEqual(
            benchmark_env["BORSUK_BENCH_LOGICAL_CELL_TRAINING_ROWS"], "4096"
        )
        self.assertEqual(benchmark_env["BORSUK_BENCH_LOGICAL_CELL_ITERATIONS"], "8")
        self.assertEqual(benchmark_env["BORSUK_BENCH_GLOBAL_PQ_CODE_BYTES"], "128")
        self.assertEqual(
            benchmark_env["BORSUK_BENCH_RAM_BUDGET_BYTES"], str(3 * 1024**3)
        )
        self.assertEqual(benchmark_env["BORSUK_BENCH_DISK_CACHE_MAX_BYTES"], "0")
        self.assertEqual(benchmark_env["BORSUK_BENCH_CACHE_COVERAGE_PERCENT"], "0")
        warm_arm = next(
            candidate
            for candidate in plan_arms(cell)
            if candidate["cache_state"] == "warm"
        )
        with tempfile.TemporaryDirectory() as root:
            warm_plan = build_execution_plan(
                cell,
                arm=warm_arm,
                workspace=Path(root),
                generator=Path("/opt/borsuk/generate_synthetic_dataset"),
                borsuk_bench=Path("/opt/borsuk/production_bench"),
                mode="smoke",
            )
        self.assertEqual(
            warm_plan["steps"][1]["env"]["BORSUK_BENCH_DISK_CACHE_MAX_BYTES"],
            str(64 * 1024**3),
        )
        self.assertEqual(
            warm_plan["steps"][1]["env"]["BORSUK_BENCH_CACHE_COVERAGE_PERCENT"],
            "100",
        )
        self.assertEqual(
            smoke_cache_cohort_authority(warm_plan, warm_arm),
            10,
            "smoke must authenticate the complete warm query cohort",
        )
        self.assertEqual(smoke_cache_cohort_authority(plan, arm), 0)
        self.assertEqual(plan["runtime_client"]["instance_type"], "c7g.xlarge")
        self.assertEqual(plan["runtime_storage"]["volume_size_gib"], 96)

    def test_every_frozen_dense_generator_has_an_exact_executable_plan(self) -> None:
        manifest = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "docs/research/publication-v3-manifest.json"
            ).read_text()
        )
        for dataset in manifest["datasets"]:
            source = dataset["source"]
            if source["state"] == "staged-generated":
                dataset["source"] = {
                    "state": "generated",
                    "generator": source["generator"],
                    "seed": source["seed"],
                }
        schedule = build_schedule_document(validate_manifest(manifest))
        expected = {
            "synthetic-clustered-v1",
            "synthetic-uniform-v1",
            "synthetic-duplicate-v1",
            "synthetic-adversarial-v1",
        }
        observed: set[str] = set()
        for cell in schedule["cells"]:
            source = cell["dataset"]["source"]
            generator_id = source.get("generator")
            if (
                cell["system"] != "borsuk"
                or cell["workload"]["kind"] != "read-recall"
                or generator_id not in expected
            ):
                continue
            if generator_id in observed:
                continue
            observed.add(generator_id)
            with tempfile.TemporaryDirectory() as root:
                plan = build_execution_plan(
                    cell,
                    arm=plan_arms(cell)[0],
                    workspace=Path(root),
                    generator=Path("/opt/borsuk/generate_synthetic_dataset"),
                    borsuk_bench=Path("/opt/borsuk/production_bench"),
                    mode="smoke",
                )
            self.assertEqual(
                plan["steps"][0]["env"]["BORSUK_SYNTHETIC_GENERATOR"],
                generator_id,
            )
            self.assertEqual(
                plan["steps"][0]["env"]["BORSUK_SYNTHETIC_DATASET_ID"],
                cell["dataset"]["id"],
            )
        self.assertEqual(observed, expected)

    def test_borsuk_turboquant_plan_omits_incompatible_pq_width(self) -> None:
        manifest = paid_v3_manifest()
        profile = manifest["index_profiles"]["borsuk"]
        profile["global_scan_codec"] = "fast-turboquant-scan"
        profile.pop("code_bytes")
        profile.update(
            {
                "turboquant_bits": 3,
                "turboquant_qjl_bits": 0,
                "turboquant_shards": 1,
            }
        )
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(manifest))["cells"]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=plan_arms(cell)[0],
                workspace=Path(root),
                generator=Path("/opt/borsuk/generate_synthetic_dataset"),
                borsuk_bench=Path("/opt/borsuk/production_bench"),
                mode="smoke",
            )

        environment = plan["steps"][-1]["env"]
        self.assertEqual(
            environment["BORSUK_BENCH_GLOBAL_SCAN_CODEC"],
            "fast-turboquant-scan",
        )
        self.assertEqual(environment["BORSUK_BENCH_TURBOQUANT_BITS"], "3")
        self.assertEqual(environment["BORSUK_BENCH_TURBOQUANT_QJL_BITS"], "0")
        self.assertEqual(environment["BORSUK_BENCH_TURBOQUANT_SHARDS"], "1")
        self.assertEqual(
            environment["BORSUK_BENCH_RECALL_LEAF_MODE"],
            "fast-turboquant-scan",
        )
        self.assertEqual(
            environment["BORSUK_BENCH_SERVING_LEAF_MODE"],
            "fast-turboquant-scan",
        )
        self.assertNotIn("BORSUK_BENCH_GLOBAL_PQ_CODE_BYTES", environment)

        malformed = {
            **cell,
            "index_profile": {
                **cell["index_profile"],
            },
        }
        malformed["index_profile"].pop("turboquant_bits")
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, "index profile is not executable"):
                build_execution_plan(
                    malformed,
                    arm=plan_arms(malformed)[0],
                    workspace=Path(root),
                    generator=Path("/opt/borsuk/generate_synthetic_dataset"),
                    borsuk_bench=Path("/opt/borsuk/production_bench"),
                    mode="smoke",
                )

    def test_smoke_plan_is_scaled_and_cannot_be_published(self) -> None:
        self.assertEqual(
            PRODUCTION_BUILD_FIELDS[-4:],
            (
                "configured_build_writers",
                "ingest_batches",
                "ingest_waves",
                "ingest_vectors_per_s",
            ),
        )
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))[
                "cells"
            ]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        with tempfile.TemporaryDirectory() as root:
            arm = plan_arms(cell)[0]
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="smoke",
            )
        self.assertFalse(plan["publishable"])
        self.assertEqual(plan["effective_rows"], 32_800)
        self.assertEqual(plan["effective_queries"], 10)
        self.assertEqual(plan["steps"][0]["env"]["BORSUK_SYNTHETIC_TRAIN"], "32800")
        self.assertEqual(plan["steps"][1]["env"]["BORSUK_BENCH_QUERIES"], "10")
        cell["source"] = {"state": "frozen"}
        publication = build_execution_plan(
            cell,
            arm=arm,
            workspace=Path(root),
            generator=Path("/bin/true"),
            borsuk_bench=Path("/bin/true"),
            mode="build",
        )
        self.assertTrue(publication["publishable"])
        self.assertEqual(
            publication["effective_rows"], cell["dataset"]["scale"]["rows"]
        )
        self.assertNotIn("steps", publication)
        self.assertEqual(publication["build"]["worker"]["instance_type"], "r7g.8xlarge")
        self.assertEqual(
            publication["runtime"]["client"]["instance_type"], "c7g.xlarge"
        )
        build_env = publication["build"]["steps"][-1]["env"]
        runtime_env = publication["runtime"]["steps"][-1]["env"]
        runtime_ram_budget_bytes = (
            cell["environment_contract"]["runtime_clients"]["borsuk"][
                "resident_limit_mib"
            ]
            * 1024**2
        )
        self.assertEqual(build_env["BORSUK_BENCH_BUILD_INDEX"], "1")
        self.assertEqual(build_env["BORSUK_BENCH_BUILD_ONLY"], "1")
        self.assertEqual(build_env["BORSUK_BUILD_TIMING"], "1")
        self.assertEqual(
            build_env["BORSUK_BUILD_TIMING_OUTPUT"],
            str(Path(publication["build"]["output_dir"]) / "bench_build_phases.csv"),
        )
        self.assertEqual(build_env["BORSUK_CPU_THREADS"], "32")
        self.assertEqual(build_env["BORSUK_BENCH_BUILD_WRITERS"], "8")
        self.assertEqual(
            build_env["BORSUK_BENCH_RAM_BUDGET_BYTES"],
            str(runtime_ram_budget_bytes),
        )
        self.assertEqual(build_env["BORSUK_BENCH_NPROBES"], "4")
        self.assertEqual(build_env["BORSUK_BENCH_CANDIDATES"], "512")
        self.assertNotIn("BORSUK_BENCH_READ_ONLY", build_env)
        self.assertEqual(runtime_env["BORSUK_CPU_THREADS"], "3")
        self.assertEqual(runtime_env["BORSUK_IO_THREADS"], "88")
        self.assertEqual(runtime_env["BORSUK_BACKING_GET_CONCURRENCY"], "64")
        self.assertEqual(
            runtime_env["BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS"], "1"
        )
        self.assertEqual(runtime_env["BORSUK_BENCH_RECALL_ONLY"], "1")
        self.assertEqual(runtime_env["BORSUK_BENCH_READ_ONLY"], "1")
        self.assertEqual(runtime_env["BORSUK_BENCH_BUILD_INDEX"], "0")
        self.assertEqual(
            runtime_env["BORSUK_BENCH_RAM_BUDGET_BYTES"],
            build_env["BORSUK_BENCH_RAM_BUDGET_BYTES"],
        )
        self.assertEqual(
            runtime_env["BORSUK_BENCH_NPROBES"],
            str(arm["leaf_page_budget"]),
        )
        self.assertEqual(
            runtime_env["BORSUK_BENCH_CANDIDATES"],
            "512",
        )
        self.assertEqual(runtime_env["BORSUK_BENCH_URI"], cell["index_prefix"])
        self.assertNotEqual(
            runtime_env["BORSUK_BENCH_DATASET"], build_env["BORSUK_BENCH_DATASET"]
        )
        self.assertEqual(
            build_env["BORSUK_BENCH_LOGICAL_CELLS"],
            str(cell["index_profile"]["logical_cells"]),
        )

    def test_publication_concurrency_profile_reuses_frozen_index_without_recall_warmup(
        self,
    ) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))[
                "cells"
            ]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        cell["source"] = {"state": "frozen"}
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=plan_arms(cell)[0],
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="runtime",
                runtime_profile="concurrency",
                runtime_flow_control=runtime_flow_control("concurrency"),
            )
        runtime_env = plan["runtime"]["steps"][-1]["env"]
        self.assertEqual(runtime_env["BORSUK_BENCH_RECALL_ONLY"], "0")
        self.assertEqual(runtime_env["BORSUK_BENCH_SKIP_RECALL"], "1")
        self.assertEqual(runtime_env["BORSUK_BENCH_CONCURRENCY"], "1,2,4,8,16")
        self.assertEqual(
            runtime_env["BORSUK_BENCH_SERVING_NPROBE"],
            str(plan_arms(cell)[0]["leaf_page_budget"]),
        )
        self.assertEqual(runtime_env["BORSUK_BENCH_SERVING_CANDIDATES"], "512")
        self.assertEqual(runtime_env["BORSUK_BENCH_MAX_ACTIVE_SEARCHES"], "16")
        self.assertEqual(runtime_env["BORSUK_BENCH_MAX_WAITING_SEARCHES"], "64")
        self.assertEqual(runtime_env["BORSUK_BENCH_LEAF_READ_WIDTH"], "32")
        self.assertEqual(runtime_env["BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS"], "96")
        self.assertEqual(
            runtime_env["BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS"], "1"
        )
        self.assertEqual(
            runtime_env["BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION"], "2"
        )
        self.assertEqual(runtime_env["BORSUK_CPU_THREADS"], "3")
        self.assertEqual(runtime_env["BORSUK_IO_THREADS"], "160")
        self.assertEqual(runtime_env["BORSUK_BACKING_GET_CONCURRENCY"], "128")
        self.assertEqual(runtime_env["BORSUK_BENCH_READ_ONLY"], "1")
        self.assertEqual(runtime_env["BORSUK_BENCH_BUILD_INDEX"], "0")

        arm = plan_arms(cell)[0]
        self.assertEqual(concurrency_result_arm(arm), arm)
        self.assertNotIn("arm_id", concurrency_result_arm(arm))

        effective = {
            "schema_version": 4,
            "disk_cache_max_bytes": 0,
            "ram_budget_bytes": 3 * 1024 * 1024 * 1024,
            "max_active_searches": 16,
            "max_waiting_searches": 64,
            "leaf_read_width": 32,
            "max_inflight_leaf_reads": 96,
            "max_parallel_decode_rank_tasks": 1,
            "exact_read_max_physical_amplification": 2,
            "cpu_threads": 3,
            "io_threads": 160,
            "s3_get_concurrency": 128,
        }
        contract = runtime_execution_contract(plan, "concurrency", effective)
        self.assertEqual(
            contract,
            {
                "schema_version": 5,
                "runtime_profile": "concurrency",
                "disk_cache_max_bytes": 0,
                "ram_budget_bytes": 3 * 1024 * 1024 * 1024,
                "max_active_searches": 16,
                "max_waiting_searches": 64,
                "leaf_read_width": 32,
                "max_inflight_leaf_reads": 96,
                "max_parallel_decode_rank_tasks": 1,
                "exact_read_max_physical_amplification": 2,
                "cpu_threads": 3,
                "io_threads": 160,
                "s3_get_concurrency": 128,
            },
        )
        runtime_env["BORSUK_BENCH_RAM_BUDGET_BYTES"] = "1073741824"
        self.assertEqual(
            runtime_execution_contract(
                plan,
                "concurrency",
                {**effective, "ram_budget_bytes": 1024 * 1024 * 1024},
            )["ram_budget_bytes"],
            1024 * 1024 * 1024,
        )
        with self.assertRaisesRegex(ValueError, "effective runtime flow control"):
            runtime_execution_contract(
                plan,
                "concurrency",
                {**effective, "max_active_searches": 3},
            )
        runtime_env["BORSUK_BENCH_RAM_BUDGET_BYTES"] = str(3 * 1024 * 1024 * 1024)
        runtime_env["BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION"] = "6"
        with self.assertRaisesRegex(ValueError, "physical amplification"):
            runtime_execution_contract(
                plan,
                "concurrency",
                {**effective, "exact_read_max_physical_amplification": 6},
            )

    def test_runtime_flow_control_is_mandatory_bounded_and_protocol_bound(self) -> None:
        fields = {
            "disk_cache_max_bytes": 0,
            "exact_read_max_physical_amplification": 2,
            "max_active_searches": 16,
            "max_waiting_searches": 64,
            "leaf_read_width": 32,
            "max_inflight_leaf_reads": 96,
            "max_parallel_decode_rank_tasks": 1,
            "cpu_threads": 3,
            "io_threads": 160,
            "s3_get_concurrency": 128,
            "ram_budget_bytes": 3 * 1024 * 1024 * 1024,
        }
        with self.assertRaisesRegex(ValueError, "required"):
            runtime_flow_control_authority("runtime", {key: None for key in fields})
        with self.assertRaisesRegex(ValueError, "atomically"):
            runtime_flow_control_authority("runtime", {**fields, "cpu_threads": None})
        with self.assertRaisesRegex(ValueError, "runtime mode"):
            runtime_flow_control_authority("smoke", fields)
        self.assertEqual(runtime_flow_control_authority("runtime", fields), fields)
        self.assertIsNone(
            runtime_flow_control_authority("build", {key: None for key in fields})
        )

        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))[
                "cells"
            ]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        cell["source"] = {"state": "frozen"}
        arm = plan_arms(cell)[0]
        for field, value in (
            ("max_active_searches", 0),
            ("max_active_searches", 17),
            ("max_active_searches", 15),
            ("max_waiting_searches", 65),
            ("max_parallel_decode_rank_tasks", 4),
            ("s3_get_concurrency", 129),
            ("io_threads", 127),
            ("leaf_read_width", 1025),
            ("max_inflight_leaf_reads", 1025),
            ("cpu_threads", 5),
            ("ram_budget_bytes", 3 * 1024 * 1024 * 1024 + 1),
            ("exact_read_max_physical_amplification", 6),
            ("disk_cache_max_bytes", 1),
            ("cpu_threads", True),
        ):
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValueError, "flow-control authority"),
                tempfile.TemporaryDirectory() as root,
            ):
                build_execution_plan(
                    cell,
                    arm=arm,
                    workspace=Path(root),
                    generator=Path("/bin/true"),
                    borsuk_bench=Path("/bin/true"),
                    mode="runtime",
                    runtime_profile="concurrency",
                    runtime_flow_control={**fields, field: value},
                )

        with tempfile.TemporaryDirectory() as authority_root:
            with self.assertRaisesRegex(ValueError, "runtime flow-control authority"):
                build_execution_plan(
                    cell,
                    arm=arm,
                    workspace=Path(authority_root),
                    generator=Path("/bin/true"),
                    borsuk_bench=Path("/bin/true"),
                    mode="runtime",
                    runtime_profile="recall",
                )
            recall_fields = {
                **runtime_flow_control(),
                "max_waiting_searches": 17,
            }
            authority_plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(authority_root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="runtime",
                runtime_profile="recall",
                runtime_flow_control=recall_fields,
            )
        flow_names = (
            "BORSUK_BENCH_DISK_CACHE_MAX_BYTES",
            "BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION",
            "BORSUK_BENCH_MAX_ACTIVE_SEARCHES",
            "BORSUK_BENCH_MAX_WAITING_SEARCHES",
            "BORSUK_BENCH_LEAF_READ_WIDTH",
            "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS",
            "BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS",
            "BORSUK_CPU_THREADS",
            "BORSUK_IO_THREADS",
            "BORSUK_BACKING_GET_CONCURRENCY",
            "BORSUK_BENCH_RAM_BUDGET_BYTES",
        )
        authority_env = authority_plan["runtime"]["steps"][0]["env"]
        field_by_environment = {
            "BORSUK_BENCH_DISK_CACHE_MAX_BYTES": "disk_cache_max_bytes",
            "BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION": (
                "exact_read_max_physical_amplification"
            ),
            "BORSUK_BENCH_MAX_ACTIVE_SEARCHES": "max_active_searches",
            "BORSUK_BENCH_MAX_WAITING_SEARCHES": "max_waiting_searches",
            "BORSUK_BENCH_LEAF_READ_WIDTH": "leaf_read_width",
            "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS": "max_inflight_leaf_reads",
            "BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS": (
                "max_parallel_decode_rank_tasks"
            ),
            "BORSUK_CPU_THREADS": "cpu_threads",
            "BORSUK_IO_THREADS": "io_threads",
            "BORSUK_BACKING_GET_CONCURRENCY": "s3_get_concurrency",
            "BORSUK_BENCH_RAM_BUDGET_BYTES": "ram_budget_bytes",
        }
        self.assertEqual(tuple(field_by_environment), flow_names)
        self.assertEqual(
            {name: authority_env[name] for name in flow_names},
            {
                name: str(recall_fields[field])
                for name, field in field_by_environment.items()
            },
        )

    def test_concurrency_artifacts_require_complete_workers_queries_and_recall(
        self,
    ) -> None:
        summaries = [
            {
                "schema_version": "borsuk-production-bench-v20",
                "scan_codec": "fast-turboquant-scan",
                "execution_engine": "bounded-cell-card-v20",
                "nprobe": "32",
                "max_candidates": "512",
                "cache_profile": "disk_cached",
                "target_cache_coverage_percent": "100",
                "workers": str(workers),
                "total_queries": "2",
                "qps": str(10 * workers),
                "mean_ms": "5",
                "p50_ms": "4",
                "p95_ms": "7",
                "p99_ms": "8",
                "max_ms": "9",
            }
            for workers in (1, 2, 4)
        ]
        samples = [
            {
                "schema_version": "borsuk-production-bench-v20",
                "scan_codec": "fast-turboquant-scan",
                "execution_engine": "bounded-cell-card-v20",
                "nprobe": "32",
                "max_candidates": "512",
                "cache_profile": "disk_cached",
                "target_cache_coverage_percent": "100",
                "workers": str(workers),
                "sample_index": str(sample),
                "query_source_index": str(100 + sample),
                "cache_cohort_index": "0",
                "cache_cohort_size": "2",
                "cache_cohort_count": "1",
                "latency_ms": "5",
                "recall_at_10": "0.99",
                "network_gets": "0",
                "disk_cache_reads": "1",
                "bytes_read": "100",
                "decoded_cache_bytes_read": "0",
                "disk_cache_bytes_read": "100",
                "backing_bytes_read": "0",
                "global_base_approximate_us": "1",
                "global_base_head_admission_us": "2",
                "global_base_head_fetch_us": "3",
                "global_base_head_read_attempts": "0",
                "global_base_head_read_successes": "0",
                "global_base_head_read_response_bytes": "0",
                "global_base_head_read_us_max": "0",
                "global_base_head_read_us_sum": "0",
                "global_base_head_read_queue_us_max": "0",
                "global_base_head_read_queue_us_sum": "0",
                "global_base_head_reads_over_20ms": "0",
                "global_base_head_reads_over_30ms": "0",
                "global_base_head_reads_over_50ms": "0",
                "global_base_head_reads_over_100ms": "0",
                "global_base_head_decode_admission_us": "4",
                "global_base_head_decode_us": "5",
                "global_base_exact_admission_us": "6",
                "global_base_exact_fetch_us": "10",
                "global_base_exact_read_attempts": "0",
                "global_base_exact_read_successes": "0",
                "global_base_exact_read_response_bytes": "0",
                "global_base_exact_read_queue_us_max": "0",
                "global_base_exact_read_queue_us_sum": "0",
                "global_base_exact_read_us_max": "0",
                "global_base_exact_read_us_sum": "0",
                "global_base_exact_reads_over_20ms": "0",
                "global_base_exact_reads_over_30ms": "0",
                "global_base_exact_reads_over_50ms": "0",
                "global_base_exact_reads_over_100ms": "0",
                "global_base_exact_cpu_us": "7",
                "global_base_exact_rerank_us": "23",
            }
            for workers in (1, 2, 4)
            for sample in range(2)
        ]
        metrics = summarize_concurrency_artifacts(
            summaries,
            samples,
            expected_workers=(1, 2, 4),
            expected_queries=2,
            minimum_recall_ppm=980_000,
            expected_scan_codec="fast-turboquant-scan",
            expected_nprobe=32,
            expected_max_candidates=512,
            expected_cache_profile="disk_cached",
            expected_cache_coverage_percent=100,
            expected_cache_cohort_size=2,
        )
        self.assertEqual([row["workers"] for row in metrics], [1, 2, 4])
        self.assertEqual(metrics[-1]["qps_milli"], 40_000)
        self.assertEqual(metrics[-1]["storage_gets"], 0)
        self.assertEqual(metrics[-1]["storage_bytes_read"], 0)
        self.assertEqual(metrics[-1]["disk_cache_bytes_read"], 200)
        wrong_wave = json.loads(json.dumps(samples))
        next(row for row in wrong_wave if row["workers"] == "2")[
            "cache_cohort_size"
        ] = "1"
        with self.assertRaisesRegex(ValueError, "cache cohort"):
            summarize_concurrency_artifacts(
                summaries,
                wrong_wave,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        self.assertEqual(
            reconcile_concurrency_storage(
                metrics,
                {
                    "storage_gets": 3,
                    "storage_puts": 0,
                    "storage_bytes_read": 1_200,
                    "storage_bytes_written": 0,
                    "storage_distinct_data_objects": 0,
                    "storage_max_data_object_bytes": 0,
                },
            ),
            {
                "storage_gets": 0,
                "storage_puts": 0,
                "storage_bytes_read": 0,
                "storage_bytes_written": 0,
                "decoded_cache_bytes_read": 0,
                "disk_cache_bytes_read": 600,
                "excluded_setup_storage_gets": 3,
                "excluded_setup_storage_bytes_read": 1_200,
            },
        )
        shifted_samples = json.loads(json.dumps(samples))
        for row in shifted_samples:
            if row["workers"] == "4":
                row["sample_index"] = str(int(row["sample_index"]) + 2)
        with self.assertRaisesRegex(ValueError, "canonical"):
            summarize_concurrency_artifacts(
                summaries,
                shifted_samples,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        mismatched_source = json.loads(json.dumps(samples))
        next(
            row
            for row in mismatched_source
            if row["workers"] == "4" and row["sample_index"] == "1"
        )["query_source_index"] = "999"
        with self.assertRaisesRegex(ValueError, "query source mapping"):
            summarize_concurrency_artifacts(
                summaries,
                mismatched_source,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        self.assertEqual(metrics[-1]["global_base_exact_fetch_us_total"], 20)
        self.assertEqual(metrics[-1]["global_base_exact_read_us_sum_total"], 0)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            summarize_concurrency_artifacts(
                summaries,
                samples[:-1],
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        wrong_codec = json.loads(json.dumps(samples))
        wrong_codec[0]["scan_codec"] = "srht-pq-scan"
        with self.assertRaisesRegex(ValueError, "concurrency sample differs"):
            summarize_concurrency_artifacts(
                summaries,
                wrong_codec,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        missing_timing = json.loads(json.dumps(samples))
        del missing_timing[0]["global_base_exact_fetch_us"]
        with self.assertRaisesRegex(ValueError, "timing telemetry is missing"):
            summarize_concurrency_artifacts(
                summaries,
                missing_timing,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        inconsistent_physical_read = json.loads(json.dumps(samples))
        inconsistent_physical_read[0]["global_base_head_read_attempts"] = "1"
        with self.assertRaisesRegex(ValueError, "timing telemetry is inconsistent"):
            summarize_concurrency_artifacts(
                summaries,
                inconsistent_physical_read,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        fallback = json.loads(json.dumps(samples))
        fallback[0]["execution_engine"] = "fast-turboquant-scan"
        with self.assertRaisesRegex(ValueError, "concurrency sample differs"):
            summarize_concurrency_artifacts(
                summaries,
                fallback,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )
        wrong_budget = json.loads(json.dumps(summaries))
        wrong_budget[0]["nprobe"] = "64"
        with self.assertRaisesRegex(ValueError, "concurrency summary differs"):
            summarize_concurrency_artifacts(
                wrong_budget,
                samples,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )

        network = json.loads(json.dumps(samples))
        network[0]["network_gets"] = "1"
        with self.assertRaisesRegex(ValueError, "disk-cached concurrency"):
            summarize_concurrency_artifacts(
                summaries,
                network,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )

        memory_only = json.loads(json.dumps(samples))
        memory_only[0]["disk_cache_reads"] = "0"
        with self.assertRaisesRegex(ValueError, "disk-cached concurrency"):
            summarize_concurrency_artifacts(
                summaries,
                memory_only,
                expected_workers=(1, 2, 4),
                expected_queries=2,
                minimum_recall_ppm=980_000,
                expected_scan_codec="fast-turboquant-scan",
                expected_nprobe=32,
                expected_max_candidates=512,
                expected_cache_profile="disk_cached",
                expected_cache_coverage_percent=100,
                expected_cache_cohort_size=2,
            )

    def test_disk_cached_cohort_authority_is_budgeted_and_row_bound(self) -> None:
        self.assertEqual(
            disk_cached_cohort_authority(64 * 1024 * 1024 * 1024, 1_000),
            (1_000, 1),
        )
        with self.assertRaisesRegex(ValueError, "complete query set"):
            disk_cached_cohort_authority(1024 * 1024 * 1024, 1_000)
        for disk_bytes, queries in ((True, 1_000), (1024**3, True), (0, 1_000)):
            with (
                self.subTest(disk_bytes=disk_bytes, queries=queries),
                self.assertRaisesRegex(ValueError, "cohort authority"),
            ):
                disk_cached_cohort_authority(disk_bytes, queries)
        row = {
            "cache_cohort_index": "0",
            "cache_cohort_size": "1000",
            "cache_cohort_count": "1",
        }
        validate_query_cache_cohort(
            row,
            sample_index=20,
            expected_queries=1_000,
            expected_cohort_size=1_000,
        )
        for field, value in (
            ("cache_cohort_index", "1"),
            ("cache_cohort_size", "999"),
            ("cache_cohort_count", "2"),
        ):
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValueError, "cache cohort"),
            ):
                validate_query_cache_cohort(
                    {**row, field: value},
                    sample_index=20,
                    expected_queries=1_000,
                    expected_cohort_size=1_000,
                )
        validate_query_cache_cohort(
            {
                "cache_cohort_index": "0",
                "cache_cohort_size": "0",
                "cache_cohort_count": "0",
            },
            sample_index=20,
            expected_queries=1_000,
            expected_cohort_size=0,
        )

    def test_unavailable_local_system_is_rejected_not_simulated(self) -> None:
        cell = scheduled_cell(system="amazon-s3-vectors")
        with (
            tempfile.TemporaryDirectory() as root,
            self.assertRaisesRegex(ValueError, "not available in local execution"),
        ):
            build_execution_plan(
                cell,
                arm={},
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="smoke",
            )

    def test_execution_rejects_successful_processes_without_real_query_artifacts(
        self,
    ) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))[
                "cells"
            ]
            if cell["system"] == "borsuk"
            and cell["workload"]["kind"] == "read-recall"
            and cell["dataset"]["source"].get("generator") == "synthetic-clustered-v1"
        )
        with tempfile.TemporaryDirectory() as root:
            arm = plan_arms(cell)[0]
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/true"),
                borsuk_bench=Path("/bin/true"),
                mode="smoke",
            )
            with self.assertRaisesRegex(ValueError, "query sample artifact"):
                execute_plan(plan)

    def test_query_summary_uses_every_real_sample_and_quality_floor(self) -> None:
        cell = scheduled_cell()
        cell["queries_per_repetition"] = 3
        rows = []
        for index, (latency, recall) in enumerate(
            ((1.0, 0.96), (2.0, 0.95), (4.0, 0.99))
        ):
            rows.append(
                {
                    "schema_version": "borsuk-production-bench-v20",
                    "sample_index": str(index),
                    "query_source_index": str(100 + index),
                    "latency_ms": str(latency),
                    "recall_at_10": str(recall),
                    "network_gets": str(index + 1),
                    "bytes_read": str((index + 1) * 100),
                    "disk_cache_reads": "0",
                    "decoded_cache_bytes_read": "0",
                    "disk_cache_bytes_read": "0",
                    "backing_bytes_read": str((index + 1) * 100),
                    "global_leaf_code_pages_read": str(100 + index),
                    "global_leaf_code_bytes": str((index + 1) * 40),
                    "global_leaf_code_requests": str(index + 2),
                    "global_leaf_pages_read": str(30 + index),
                    "global_leaf_exact_requests": str(index + 3),
                    "global_leaf_page_bytes": str((index + 1) * 60),
                    "global_leaf_exact_scores": str(960 + index * 32),
                    "global_base_approximate_us": str(10 + index),
                    "global_base_head_admission_us": "1",
                    "global_base_head_fetch_us": "2",
                    "global_base_head_read_attempts": str(index + 2),
                    "global_base_head_read_successes": str(index + 2),
                    "global_base_head_read_response_bytes": str((index + 1) * 40),
                    "global_base_head_read_us_max": "1",
                    "global_base_head_read_us_sum": str(index + 2),
                    "global_base_head_read_queue_us_max": "1",
                    "global_base_head_read_queue_us_sum": str(index + 2),
                    "global_base_head_reads_over_20ms": "0",
                    "global_base_head_reads_over_30ms": "0",
                    "global_base_head_reads_over_50ms": "0",
                    "global_base_head_reads_over_100ms": "0",
                    "global_base_head_decode_admission_us": "3",
                    "global_base_head_decode_us": "4",
                    "global_base_exact_admission_us": "5",
                    "global_base_exact_fetch_us": str(20 + index),
                    "global_base_exact_read_attempts": str(index + 3),
                    "global_base_exact_read_successes": str(index + 3),
                    "global_base_exact_read_response_bytes": str((index + 1) * 60),
                    "global_base_exact_read_queue_us_max": "1",
                    "global_base_exact_read_queue_us_sum": str(index + 3),
                    "global_base_exact_read_us_max": "10",
                    "global_base_exact_read_us_sum": "30",
                    "global_base_exact_reads_over_20ms": "2",
                    "global_base_exact_reads_over_30ms": "1",
                    "global_base_exact_reads_over_50ms": "0",
                    "global_base_exact_reads_over_100ms": "0",
                    "global_base_exact_cpu_us": "6",
                    "global_base_exact_rerank_us": "31",
                }
            )
        arm = {
            "k": 10,
            "leaf_page_budget": 32,
            "cache_state": "cold",
        }
        for row in rows:
            row.update(
                {
                    "phase": "uncached",
                    "mode": "srht-pq-scan",
                    "scan_codec": "srht-pq-scan",
                    "execution_engine": "bounded-cell-card-v20",
                    "nprobe": "32",
                    "max_candidates": "512",
                    "cache_cohort_index": "0",
                    "cache_cohort_size": "0",
                    "cache_cohort_count": "0",
                }
            )
        summary = summarize_query_samples(
            rows,
            cell=cell,
            arm=arm,
            expected_queries=3,
            expected_cache_cohort_size=0,
        )
        self.assertEqual(summary["queries"], 3)
        self.assertEqual(summary["correctness_ppm"], 966667)
        self.assertEqual(summary["latency_p50_us"], 2000)
        self.assertEqual(summary["latency_p95_us"], 4000)
        self.assertEqual(summary["latency_p99_us"], 4000)
        self.assertEqual(summary["storage_gets"], 6)
        self.assertEqual(summary["storage_bytes_read"], 600)
        self.assertEqual(summary["decoded_cache_bytes_read"], 0)
        self.assertEqual(summary["disk_cache_bytes_read"], 0)
        self.assertEqual(summary["global_leaf_code_requests"], 9)
        self.assertEqual(summary["global_leaf_exact_requests"], 12)
        self.assertEqual(summary["global_base_exact_fetch_us_total"], 63)
        self.assertEqual(summary["global_base_exact_read_us_sum_total"], 90)
        self.assertEqual(summary["global_base_exact_reads_over_20ms_total"], 6)
        self.assertEqual(summary["query_elapsed_ns"], 7_000_000)
        shifted_rows = json.loads(json.dumps(rows))
        for row in shifted_rows:
            row["sample_index"] = str(int(row["sample_index"]) + 3)
        with self.assertRaisesRegex(ValueError, "canonical"):
            summarize_query_samples(
                shifted_rows,
                cell=cell,
                arm=arm,
                expected_queries=3,
            )
        duplicate_source = json.loads(json.dumps(rows))
        duplicate_source[1]["query_source_index"] = duplicate_source[0][
            "query_source_index"
        ]
        with self.assertRaisesRegex(ValueError, "query source"):
            summarize_query_samples(
                duplicate_source,
                cell=cell,
                arm=arm,
                expected_queries=3,
            )

        warm_arm = {**arm, "cache_state": "warm"}
        warm_rows = json.loads(json.dumps(rows))
        for row in warm_rows:
            row["phase"] = "disk_cached"
            row["network_gets"] = "0"
            row["disk_cache_reads"] = "1"
            row["disk_cache_bytes_read"] = row["bytes_read"]
            row["backing_bytes_read"] = "0"
            row["global_leaf_code_bytes"] = "0"
            row["cache_cohort_index"] = "0"
            row["cache_cohort_size"] = "3"
            row["cache_cohort_count"] = "1"
        with self.assertRaisesRegex(ValueError, "cache cohort authority"):
            summarize_query_samples(
                warm_rows,
                cell=cell,
                arm=warm_arm,
                expected_queries=3,
            )
        warm_summary = summarize_query_samples(
            warm_rows,
            cell=cell,
            arm=warm_arm,
            expected_queries=3,
            expected_cache_cohort_size=3,
        )
        self.assertEqual(warm_summary["storage_gets"], 0)
        self.assertEqual(warm_summary["storage_bytes_read"], 0)
        self.assertEqual(warm_summary["disk_cache_bytes_read"], 600)
        wrong_cohort = json.loads(json.dumps(warm_rows))
        wrong_cohort[2]["cache_cohort_index"] = "1"
        with self.assertRaisesRegex(ValueError, "cache cohort"):
            summarize_query_samples(
                wrong_cohort,
                cell=cell,
                arm=warm_arm,
                expected_queries=3,
                expected_cache_cohort_size=3,
            )
        warm_network = json.loads(json.dumps(warm_rows))
        warm_network[0]["network_gets"] = "1"
        with self.assertRaisesRegex(ValueError, "disk-cached query sample"):
            summarize_query_samples(
                warm_network,
                cell=cell,
                arm=warm_arm,
                expected_queries=3,
                expected_cache_cohort_size=3,
            )
        warm_memory_only = json.loads(json.dumps(warm_rows))
        warm_memory_only[0]["disk_cache_reads"] = "0"
        with self.assertRaisesRegex(ValueError, "disk-cached query sample"):
            summarize_query_samples(
                warm_memory_only,
                cell=cell,
                arm=warm_arm,
                expected_queries=3,
                expected_cache_cohort_size=3,
            )
        warm_head_reads = json.loads(json.dumps(warm_rows))
        warm_head_reads[0]["global_leaf_code_bytes"] = "1"
        with self.assertRaisesRegex(ValueError, "prepared code planes"):
            summarize_query_samples(
                warm_head_reads,
                cell=cell,
                arm=warm_arm,
                expected_queries=3,
                expected_cache_cohort_size=3,
            )

        protocol = canonical_json_bytes(cell) + b"\n"
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            build_attempt_id="build-attempt-01",
            builder_instance_identity="i-builder-01",
            builder_instance_type=cell["environment_contract"]["build_workers"][
                "borsuk"
            ]["instance_type"],
            build_artifact=build_artifact(cell),
            object_roster=data_roster(cell),
            build_metrics=build_metrics(),
        )
        attestation = runtime_attestation_for(cell, instance_id="i-0123456789abcdef0")
        report = build_publication_report(
            cell=cell,
            arm=arm,
            protocol_bytes=protocol,
            source_archive_sha256="a" * 64,
            dataset_materialization_sha256="d" * 64,
            attempt_id="attempt-01",
            instance_identity="i-0123456789abcdef0",
            elapsed_ns=7_000_000,
            query_metrics=summary,
            resource_metrics={
                "cpu_ns": 1_000_000,
                "peak_rss_bytes": 1024,
                "disk_read_bytes": 0,
                "disk_write_bytes": 0,
            },
            runtime_storage_trace={
                "storage_gets": 6,
                "storage_puts": 0,
                "storage_bytes_read": 600,
                "storage_bytes_written": 0,
                "storage_distinct_data_objects": 0,
                "storage_max_data_object_bytes": 0,
            },
            index_receipt=receipt,
            runtime_attestation=attestation,
        )
        self.assertTrue(report["publishable"])
        self.assertEqual(report["result"]["schema_version"], 4)

        missing_timing = json.loads(json.dumps(rows))
        del missing_timing[0]["global_base_exact_fetch_us"]
        with self.assertRaisesRegex(ValueError, "timing telemetry is missing"):
            summarize_query_samples(
                missing_timing, cell=cell, arm=arm, expected_queries=3
            )

        bad = json.loads(json.dumps(rows))
        for row in bad:
            row["recall_at_10"] = "0.94"
        with self.assertRaisesRegex(
            ValueError,
            "observed 940000 ppm is below required 950000 ppm; "
            "exact_scores=960..1024 code_pages=100..102 exact_blocks=30..32 "
            "gets=6 bytes=600",
        ):
            summarize_query_samples(bad, cell=cell, arm=arm, expected_queries=3)
        smoke = summarize_query_samples(
            bad,
            cell=cell,
            arm=arm,
            expected_queries=3,
            enforce_quality=False,
        )
        self.assertEqual(smoke["correctness_ppm"], 940000)

        turboquant_cell = json.loads(json.dumps(cell))
        turboquant_cell["index_profile"].update(
            {
                "global_scan_codec": "fast-turboquant-scan",
                "turboquant_bits": 4,
                "turboquant_qjl_bits": 0,
                "turboquant_shards": 1,
            }
        )
        turboquant_cell["index_profile"].pop("code_bytes")
        turboquant_rows = json.loads(json.dumps(rows))
        for row in turboquant_rows:
            row["mode"] = "fast-turboquant-scan"
            row["scan_codec"] = "fast-turboquant-scan"
        turboquant = summarize_query_samples(
            turboquant_rows,
            cell=turboquant_cell,
            arm=arm,
            expected_queries=3,
        )
        self.assertEqual(turboquant["correctness_ppm"], 966667)
        wrong_codec_rows = json.loads(json.dumps(turboquant_rows))
        wrong_codec_rows[0]["scan_codec"] = "srht-pq-scan"
        with self.assertRaisesRegex(ValueError, "different factor arm"):
            summarize_query_samples(
                wrong_codec_rows,
                cell=turboquant_cell,
                arm=arm,
                expected_queries=3,
            )
        fallback_rows = json.loads(json.dumps(turboquant_rows))
        fallback_rows[0]["execution_engine"] = "fast-turboquant-scan"
        with self.assertRaisesRegex(ValueError, "different factor arm"):
            summarize_query_samples(
                fallback_rows,
                cell=turboquant_cell,
                arm=arm,
                expected_queries=3,
            )

    def test_read_arms_expand_one_declared_axis_without_cross_product_aliasing(
        self,
    ) -> None:
        cell = scheduled_cell()
        cell["workload"]["factors"] = {
            "k": [10],
            "leaf_page_budgets": [4, 32, 64],
            "cache_states": ["cold", "warm"],
            "minimum_recall_ppm": 950000,
        }
        self.assertEqual(
            plan_arms(cell),
            [
                {"k": 10, "leaf_page_budget": 4, "cache_state": "cold"},
                {"k": 10, "leaf_page_budget": 4, "cache_state": "warm"},
                {"k": 10, "leaf_page_budget": 32, "cache_state": "cold"},
                {"k": 10, "leaf_page_budget": 32, "cache_state": "warm"},
                {"k": 10, "leaf_page_budget": 64, "cache_state": "cold"},
                {"k": 10, "leaf_page_budget": 64, "cache_state": "warm"},
            ],
        )

        cell["workload"]["factors"]["k"] = [10, 100]
        with self.assertRaisesRegex(ValueError, "k=100"):
            plan_arms(cell)

    def test_lifecycle_arms_expand_every_frozen_mutation_factor(self) -> None:
        cell = scheduled_cell(kind="write-update-delete-compact")
        arms = plan_arms(cell)
        self.assertEqual(len(arms), 18)
        self.assertEqual(
            arms[0],
            {
                "writers": 1,
                "batch_size": 1,
                "insert_mode": "general-upsert",
                "update_percent": 10,
                "delete_percent": 10,
            },
        )
        self.assertEqual(arms[9]["insert_mode"], "claim-free-put")
        self.assertEqual(arms[-1]["writers"], 16)
        self.assertEqual(arms[-1]["batch_size"], 1024)

    def test_lifecycle_diagnostic_write_count_is_explicit_and_runtime_only(
        self,
    ) -> None:
        cell = scheduled_cell(kind="write-update-delete-compact")
        cell["source"] = {
            "state": "frozen",
            "git_commit": "1" * 40,
            "archive_sha256": "2" * 64,
            "cargo_lock_sha256": "3" * 64,
            "python_lock_sha256": "4" * 64,
            "node_lock_sha256": "5" * 64,
        }
        arm = plan_arms(cell)[13]
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/false"),
                borsuk_bench=Path("/bin/true"),
                mode="runtime",
                runtime_profile="lifecycle",
                runtime_flow_control=runtime_flow_control("lifecycle"),
                diagnostic_write_ops=2_560,
            )
            environment = plan["runtime"]["steps"][0]["env"]
            self.assertEqual(environment["BORSUK_BENCH_WRITE_OPS"], "2560")
            self.assertEqual(environment["BORSUK_OPEN_PROGRESS"], "1")
            self.assertEqual(environment["BORSUK_LIFECYCLE_PROGRESS"], "1")
            self.assertEqual(environment["BORSUK_V20_PROGRESS"], "1")

            with self.assertRaisesRegex(ValueError, "diagnostic write count"):
                build_execution_plan(
                    cell,
                    arm=arm,
                    workspace=Path(root),
                    generator=Path("/bin/false"),
                    borsuk_bench=Path("/bin/true"),
                    mode="build",
                    diagnostic_write_ops=2_560,
                )

    def test_lifecycle_diagnostic_cannot_be_mistaken_for_publishable_evidence(
        self,
    ) -> None:
        diagnostic = claim_ineligible_lifecycle_diagnostic(
            {"publishable": True, "result": {"status": "complete"}},
            write_ops=2_560,
        )

        self.assertEqual(
            diagnostic,
            {
                "publishable": False,
                "claim_eligible": False,
                "diagnostic_write_ops": 2_560,
                "result": {"status": "complete"},
            },
        )
        with self.assertRaisesRegex(ValueError, "write count"):
            claim_ineligible_lifecycle_diagnostic(
                {"publishable": True, "result": {}}, write_ops=0
            )

    def test_read_diagnostic_matrix_is_complete_below_floor_and_claim_ineligible(
        self,
    ) -> None:
        cell = scheduled_cell()
        cell["queries_per_repetition"] = 2
        arm = {"k": 10, "leaf_page_budget": 32, "cache_state": "cold"}
        rows = []
        for nprobe in (32, 64):
            for candidates in (512, 1024, 2048, 4096):
                for sample_index in range(2):
                    rows.append(
                        {
                            "schema_version": "borsuk-production-bench-v20",
                            "phase": "uncached",
                            "mode": "srht-pq-scan",
                            "scan_codec": "srht-pq-scan",
                            "execution_engine": "bounded-cell-card-v20",
                            "nprobe": str(nprobe),
                            "max_candidates": str(candidates),
                            "sample_index": str(sample_index),
                            "query_source_index": str(100 + sample_index),
                            "latency_ms": str(10 + sample_index),
                            "recall_at_10": "0.80",
                            "network_gets": "3",
                            "bytes_read": "100",
                            "disk_cache_reads": "0",
                            "decoded_cache_bytes_read": "0",
                            "disk_cache_bytes_read": "0",
                            "backing_bytes_read": "100",
                            "global_leaf_code_pages_read": "7",
                            "global_leaf_code_requests": "2",
                            "global_leaf_code_bytes": "40",
                            "global_leaf_pages_read": "4",
                            "global_leaf_exact_requests": "1",
                            "global_leaf_page_bytes": "60",
                            "global_leaf_exact_scores": str(candidates),
                            "global_base_approximate_us": "10",
                            "global_base_head_admission_us": "1",
                            "global_base_head_fetch_us": "2",
                            "global_base_head_read_attempts": "2",
                            "global_base_head_read_successes": "2",
                            "global_base_head_read_response_bytes": "40",
                            "global_base_head_read_us_max": "1",
                            "global_base_head_read_us_sum": "2",
                            "global_base_head_read_queue_us_max": "1",
                            "global_base_head_read_queue_us_sum": "2",
                            "global_base_head_reads_over_20ms": "0",
                            "global_base_head_reads_over_30ms": "0",
                            "global_base_head_reads_over_50ms": "0",
                            "global_base_head_reads_over_100ms": "0",
                            "global_base_head_decode_admission_us": "3",
                            "global_base_head_decode_us": "4",
                            "global_base_exact_admission_us": "5",
                            "global_base_exact_fetch_us": "20",
                            "global_base_exact_read_attempts": "1",
                            "global_base_exact_read_successes": "1",
                            "global_base_exact_read_response_bytes": "60",
                            "global_base_exact_read_queue_us_max": "1",
                            "global_base_exact_read_queue_us_sum": "1",
                            "global_base_exact_read_us_max": "10",
                            "global_base_exact_read_us_sum": "30",
                            "global_base_exact_reads_over_20ms": "1",
                            "global_base_exact_reads_over_30ms": "1",
                            "global_base_exact_reads_over_50ms": "0",
                            "global_base_exact_reads_over_100ms": "0",
                            "global_base_exact_cpu_us": "6",
                            "global_base_exact_rerank_us": "31",
                        }
                    )

        summaries = [
            {
                "schema_version": "borsuk-production-bench-v20",
                "scan_codec": "srht-pq-scan",
                "execution_engine": "bounded-cell-card-v20",
                "phase": "uncached",
                "mode": "srht-pq-scan",
                "nprobe": str(nprobe),
                "max_candidates": str(candidates),
                "recall_at_10": "0.800",
                "samples": "2",
            }
            for nprobe in (32, 64)
            for candidates in (512, 1024, 2048, 4096)
        ]

        report = summarize_read_diagnostic_samples(
            rows,
            summary_rows=summaries,
            cell=cell,
            arm=arm,
            expected_queries=2,
            nprobes=(32, 64),
            candidates=(512, 1024, 2048, 4096),
        )

        self.assertEqual(report["document_kind"], "publication-v3-read-diagnostic")
        self.assertFalse(report["publishable"])
        self.assertFalse(report["claim_eligible"])
        self.assertEqual(report["nprobes"], [32, 64])
        self.assertEqual(report["candidates"], [512, 1024, 2048, 4096])
        self.assertEqual(len(report["metrics"]), 8)
        self.assertEqual(
            [(item["nprobe"], item["max_candidates"]) for item in report["metrics"]],
            [
                (32, 512),
                (32, 1024),
                (32, 2048),
                (32, 4096),
                (64, 512),
                (64, 1024),
                (64, 2048),
                (64, 4096),
            ],
        )
        self.assertTrue(
            all(item["correctness_ppm"] == 800000 for item in report["metrics"])
        )

        with self.assertRaisesRegex(ValueError, "diagnostic matrix is incomplete"):
            summarize_read_diagnostic_samples(
                rows[:-2],
                summary_rows=summaries,
                cell=cell,
                arm=arm,
                expected_queries=2,
                nprobes=(32, 64),
                candidates=(512, 1024, 2048, 4096),
            )
        with self.assertRaisesRegex(ValueError, "summary matrix is incomplete"):
            summarize_read_diagnostic_samples(
                rows,
                summary_rows=summaries[:-1],
                cell=cell,
                arm=arm,
                expected_queries=2,
                nprobes=(32, 64),
                candidates=(512, 1024, 2048, 4096),
            )
        mismatched_source = [dict(row) for row in rows]
        mismatched_source[-1]["query_source_index"] = "999"
        with self.assertRaisesRegex(ValueError, "source indices differ"):
            summarize_read_diagnostic_samples(
                mismatched_source,
                summary_rows=summaries,
                cell=cell,
                arm=arm,
                expected_queries=2,
                nprobes=(32, 64),
                candidates=(512, 1024, 2048, 4096),
            )
        disjoint_samples = [dict(row) for row in rows]
        disjoint_samples[-2]["sample_index"] = "2"
        disjoint_samples[-1]["sample_index"] = "3"
        with self.assertRaisesRegex(ValueError, "sample indices are not canonical"):
            summarize_read_diagnostic_samples(
                disjoint_samples,
                summary_rows=summaries,
                cell=cell,
                arm=arm,
                expected_queries=2,
                nprobes=(32, 64),
                candidates=(512, 1024, 2048, 4096),
            )
        with self.assertRaisesRegex(ValueError, "below required 950000 ppm"):
            summarize_query_samples(rows[:2], cell=cell, arm=arm, expected_queries=2)

    def test_read_diagnostic_plan_sets_only_the_bounded_cross_product(self) -> None:
        cell = scheduled_cell()
        arm = plan_arms(cell)[0]
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/false"),
                borsuk_bench=Path("/bin/true"),
                mode="runtime",
                runtime_profile="recall",
                runtime_flow_control=runtime_flow_control(),
                diagnostic_read_nprobes=(32, 64),
                diagnostic_read_candidates=(512, 1024, 2048, 4096),
            )
            environment = plan["runtime"]["steps"][0]["env"]
            self.assertEqual(environment["BORSUK_BENCH_NPROBES"], "32,64")
            self.assertEqual(
                environment["BORSUK_BENCH_CANDIDATES"], "512,1024,2048,4096"
            )

            with self.assertRaisesRegex(ValueError, "read diagnostic authority"):
                build_execution_plan(
                    cell,
                    arm=arm,
                    workspace=Path(root),
                    generator=Path("/bin/false"),
                    borsuk_bench=Path("/bin/true"),
                    mode="runtime",
                    runtime_profile="recall",
                    runtime_flow_control=runtime_flow_control(),
                    diagnostic_read_nprobes=(32, 64),
                )

    def test_v21_feasibility_plan_is_an_exclusive_claim_ineligible_runtime(self) -> None:
        cell = scheduled_cell()
        cell["workload"]["id"] = "standard-ann-read"
        cell["dataset"]["id"] = "deep-image-96"
        cell["dataset"]["dimensions"] = 96
        cell["source"]["archive_sha256"] = "a" * 64
        arm = plan_arms(cell)[0]
        with tempfile.TemporaryDirectory() as root:
            plan = build_execution_plan(
                cell,
                arm=arm,
                workspace=Path(root),
                generator=Path("/bin/false"),
                borsuk_bench=Path("/bin/true"),
                mode="runtime",
                runtime_profile="recall",
                runtime_flow_control=runtime_flow_control(),
                v21_feasibility=True,
            )
        self.assertIs(plan["publishable"], False)
        environment = plan["runtime"]["steps"][0]["env"]
        self.assertEqual(environment["BORSUK_BENCH_V21_FEASIBILITY"], "1")
        self.assertEqual(
            environment["BORSUK_BENCH_V21_SOURCE_ARCHIVE_SHA256"], "a" * 64
        )
        self.assertEqual(
            environment["BORSUK_BENCH_V21_INDEX_ID"],
            str(cell["index_prefix"]).rstrip("/").rsplit("/", 1)[-1],
        )
        self.assertEqual(
            environment["BORSUK_BENCH_V21_DATASET_ID"], "deep-image-96"
        )
        for forbidden in (
            "BORSUK_BENCH_BUILD_INDEX",
            "BORSUK_BENCH_READ_ONLY",
            "BORSUK_BENCH_RECALL_ONLY",
            "BORSUK_BENCH_SKIP_RECALL",
            "BORSUK_BENCH_NPROBES",
            "BORSUK_BENCH_CANDIDATES",
            "BORSUK_BENCH_CONCURRENCY",
            "BORSUK_BENCH_LIMIT",
            "BORSUK_STORAGE_TRACE",
        ):
            self.assertNotIn(forbidden, environment)

    def test_smoke_report_is_distinct_from_a_publishable_cell_result(self) -> None:
        cell = scheduled_cell()
        arm = {
            "k": 10,
            "leaf_page_budget": 32,
            "cache_state": "cold",
        }
        report = build_smoke_report(
            cell=cell,
            arm=arm,
            effective_rows=1_000,
            effective_queries=10,
            metrics={"queries": 10, "correctness_ppm": 920000},
            protocol_sha256="a" * 64,
        )
        self.assertEqual(report["document_kind"], "publication-v3-smoke")
        self.assertFalse(report["publishable"])
        self.assertNotIn("object_roster", report)


if __name__ == "__main__":
    unittest.main()
