#!/usr/bin/env python3
"""Fail a benchmark run when a required CSV is missing, ragged, or mislabeled."""

from __future__ import annotations

import argparse
import csv
import math
from collections import Counter
from pathlib import Path

try:
    from .production_bench_schema import (
        CURRENT_QUERY_TELEMETRY_FIELDS,
        QUERY_STAGE_TIMING_FIELDS,
        validate_production_bench_schema_rows,
        validate_current_query_sample_rows,
        validate_query_stage_timings,
    )
except ImportError:
    from production_bench_schema import (  # type: ignore[no-redef]
        CURRENT_QUERY_TELEMETRY_FIELDS,
        QUERY_STAGE_TIMING_FIELDS,
        validate_production_bench_schema_rows,
        validate_current_query_sample_rows,
        validate_query_stage_timings,
    )

DEFAULT_REQUIRED = (
    "bench_build.csv",
    "bench_recall_latency.csv",
    "bench_query_samples.csv",
    "bench_startup.csv",
    "bench_cache_states.csv",
    "bench_concurrency.csv",
    "bench_concurrency_samples.csv",
)

REQUIRED_COLUMNS = {
    "bench_build.csv": {
        "vector_element_type",
        "scan_codec",
        "records",
        "segment_bytes",
        "vector_sidecar_bytes",
        "global_scan_bytes",
        "total_active_index_bytes",
        "bytes_per_vector",
        "resident_bytes_estimate",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
        "ingest_ms",
    },
    "bench_recall_latency.csv": {
        "scan_codec",
        "cache_execution",
        "phase",
        "mode",
        "nprobe",
        "max_candidates",
        "recall_at_10",
        "samples",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
    },
    "bench_query_samples.csv": {
        "schema_version",
        "scan_codec",
        "cache_execution",
        "phase",
        "mode",
        "nprobe",
        "max_candidates",
        "sample_index",
        "latency_ms",
        "recall_at_10",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
        *CURRENT_QUERY_TELEMETRY_FIELDS,
    },
    "bench_concurrency.csv": {
        "schema_version",
        "scan_codec",
        "cache_execution",
        "execution_engine",
        "nprobe",
        "max_candidates",
        "workers",
        "total_queries",
        "qps",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
    },
    "bench_concurrency_samples.csv": {
        "schema_version",
        "scan_codec",
        "cache_execution",
        "execution_engine",
        "nprobe",
        "max_candidates",
        "workers",
        "sample_index",
        "latency_ms",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
        *QUERY_STAGE_TIMING_FIELDS,
    },
    "bench_write_costs.csv": {
        "op",
        "configured_batch_records",
        "ops",
        "batches",
        "wall_ms",
        "ops_per_s",
        "mean_batch_ms",
        "stddev_batch_ms",
        "p50_batch_ms",
        "p95_batch_ms",
        "p99_batch_ms",
        "max_batch_ms",
        "mean_amortized_ms",
        "gets",
        "puts",
    },
    "bench_write_samples.csv": {
        "op",
        "batch_index",
        "batch_records",
        "batch_latency_ms",
        "amortized_ms",
        "gets",
        "puts",
    },
    "bench_lifecycle.csv": {
        "configured_batch_records",
        "inserted_vectors",
        "logical_vector_bytes",
        "insert_vectors_per_s",
        "time_to_searchable_ms",
        "searchable_fraction",
        "time_to_fully_indexed_ms",
        "indexed_delta_bytes",
        "write_amplification",
        "write_amplification_is_lower_bound",
        "consolidation_ms",
    },
    "bench_mutation_queries.csv": {
        "stage",
        "queries",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
        "avg_bytes_read",
        "avg_network_gets",
    },
    "bench_mutation_query_samples.csv": {
        "stage",
        "sample_index",
        "latency_ms",
        "execution_engine",
        "bytes_read",
        "network_gets",
    },
    "hybrid_build.csv": {
        "dataset",
        "documents",
        "scan_codec",
        "ingest_ms",
        "finish_ms",
        "total_ms",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "hybrid_queries.csv": {
        "dataset",
        "scan_codec",
        "mode",
        "latency_ms",
        "ndcg_at_10",
        "recall_at_10",
        "mrr_at_10",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "filter_build.csv": {
        "dataset",
        "records",
        "dimensions",
        "tenants",
        "records_per_tenant",
        "vector_element_type",
        "elapsed_ms",
        "vectors_per_s",
        "total_active_bytes",
        "bytes_per_vector",
        "resident_bytes_estimate",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "filter_samples.csv": {
        "dataset",
        "cache_profile",
        "target_cache_coverage_percent",
        "client_concurrency",
        "selectivity",
        "sample_index",
        "latency_ms",
        "recall_at_10",
        "fallback_exact",
        "segments_searched",
        "segments_pruned",
        "rows_evaluated",
        "rows_passed",
        "bytes_read",
        "disk_reads",
        "backing_reads",
        "disk_bytes",
        "backing_bytes",
        "network_gets",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "filter_summary.csv": {
        "dataset",
        "cache_profile",
        "target_cache_coverage_percent",
        "client_concurrency",
        "selectivity",
        "samples",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
        "recall_at_10",
        "fallback_exact_ratio",
        "avg_rows_evaluated",
        "avg_rows_passed",
        "avg_bytes_read",
        "avg_network_gets",
    },
    "namespace_build.csv": {
        "dataset",
        "namespace",
        "records",
        "dimensions",
        "vector_element_type",
        "elapsed_ms",
        "vectors_per_s",
        "total_active_bytes",
        "bytes_per_vector",
        "resident_bytes_estimate",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "namespace_samples.csv": {
        "dataset",
        "cache_profile",
        "target_cache_coverage_percent",
        "client_concurrency",
        "phase",
        "namespace",
        "namespace_rows",
        "sample_index",
        "latency_ms",
        "recall_at_10",
        "bytes_read",
        "disk_reads",
        "backing_reads",
        "network_gets",
        "auth_failures",
        "auth_overhead_ms",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "namespace_summary.csv": {
        "dataset",
        "cache_profile",
        "target_cache_coverage_percent",
        "client_concurrency",
        "phase",
        "namespace",
        "namespace_rows",
        "samples",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
        "recall_at_10",
        "avg_bytes_read",
        "avg_network_gets",
        "noisy_neighbor_slowdown",
        "auth_failures",
        "auth_overhead_ms",
    },
    "late_interaction_build.csv": {
        "dataset",
        "documents",
        "token_dimensions",
        "vector_element_type",
        "elapsed_ms",
        "documents_per_s",
        "total_active_bytes",
        "bytes_per_document",
        "resident_bytes_estimate",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "late_interaction_samples.csv": {
        "dataset",
        "cache_profile",
        "target_cache_coverage_percent",
        "client_concurrency",
        "frontier",
        "sample_index",
        "query_id",
        "latency_ms",
        "mrr_at_10",
        "recall_at_50",
        "token_search_ms",
        "rerank_ms",
        "query_tokens",
        "token_hits_considered",
        "candidate_entities",
        "bytes_read",
        "disk_bytes",
        "backing_bytes",
        "network_gets",
        "ram_budget_bytes",
        "collection_resident_bytes",
        "retained_bytes",
        "retained_capacity_bytes",
        "retained_peak_bytes",
        "transient_bytes",
        "transient_capacity_bytes",
        "transient_peak_bytes",
    },
    "late_interaction_summary.csv": {
        "dataset",
        "cache_profile",
        "target_cache_coverage_percent",
        "client_concurrency",
        "frontier",
        "samples",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
        "mrr_at_10",
        "recall_at_50",
        "avg_token_search_ms",
        "avg_rerank_ms",
        "avg_candidate_entities",
        "avg_bytes_read",
        "avg_network_gets",
    },
    "resources.csv": {
        "elapsed_ms",
        "cpu_percent",
        "rss_bytes",
        "vms_bytes",
        "process_read_bytes",
        "process_write_bytes",
        "cache_disk_bytes",
        "scratch_disk_bytes",
        "network_receive_bytes",
        "network_transmit_bytes",
    },
}

NONNEGATIVE_COLUMNS = {
    "samples",
    "mean_ms",
    "stddev_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "latency_ms",
    "workers",
    "total_queries",
    "qps",
    "elapsed_ms",
    "cpu_percent",
    "rss_bytes",
    "vms_bytes",
    "process_read_bytes",
    "process_write_bytes",
    "cache_disk_bytes",
    "scratch_disk_bytes",
    "network_receive_bytes",
    "network_transmit_bytes",
    "ops",
    "batches",
    "wall_ms",
    "ops_per_s",
    "mean_batch_ms",
    "stddev_batch_ms",
    "p50_batch_ms",
    "p95_batch_ms",
    "p99_batch_ms",
    "max_batch_ms",
    "mean_amortized_ms",
    "batch_index",
    "batch_records",
    "batch_latency_ms",
    "amortized_ms",
    "gets",
    "puts",
    "deletes",
    "heads",
    "lists",
    "queries",
    "avg_bytes_read",
    "avg_network_gets",
    "bytes_read",
    "network_gets",
    "records",
    "segment_bytes",
    "vector_sidecar_bytes",
    "global_scan_bytes",
    "total_active_index_bytes",
    "bytes_per_vector",
    "resident_bytes_estimate",
    "ram_budget_bytes",
    "collection_resident_bytes",
    "retained_bytes",
    "retained_capacity_bytes",
    "retained_peak_bytes",
    "transient_bytes",
    "transient_capacity_bytes",
    "transient_peak_bytes",
    "ingest_ms",
    "inserted_vectors",
    "logical_vector_bytes",
    "insert_vectors_per_s",
    "time_to_searchable_ms",
    "searchable_fraction",
    "time_to_fully_indexed_ms",
    "indexed_delta_bytes",
    "write_amplification",
    "consolidation_ms",
    "target_cache_coverage_percent",
    "client_concurrency",
    "selectivity",
    "sample_index",
    "fallback_exact_ratio",
    "segments_searched",
    "segments_pruned",
    "rows_evaluated",
    "rows_passed",
    "disk_reads",
    "backing_reads",
    "disk_bytes",
    "backing_bytes",
    "total_active_bytes",
    "vectors_per_s",
    "namespace",
    "namespace_rows",
    "auth_failures",
    "auth_overhead_ms",
    "noisy_neighbor_slowdown",
    "documents",
    "token_dimensions",
    "documents_per_s",
    "bytes_per_document",
    "frontier",
    "token_search_ms",
    "rerank_ms",
    "query_tokens",
    "token_hits_considered",
    "candidate_entities",
    "avg_token_search_ms",
    "avg_rerank_ms",
    "avg_candidate_entities",
    "avg_disk_reads",
    "avg_backing_reads",
    "avg_disk_bytes",
    "avg_backing_bytes",
}

UNIT_INTERVAL_COLUMNS = {
    "recall_at_10",
    "recall_at_50",
    "mrr_at_10",
    "fallback_exact_ratio",
    "searchable_fraction",
}


def _finite_nonnegative(path: Path, line: int, column: str, value: str) -> float:
    try:
        number = float(value)
    except ValueError as error:
        raise ValueError(f"{path}:{line} {column} is not numeric: {value!r}") from error
    if not math.isfinite(number):
        raise ValueError(f"{path}:{line} {column} must be finite")
    if number < 0:
        raise ValueError(f"{path}:{line} {column} must be nonnegative")
    return number


def _validate_distribution_rows(path: Path, rows: list[dict[str, str]]) -> None:
    for line, row in enumerate(rows, start=2):
        for column in NONNEGATIVE_COLUMNS.intersection(row):
            _finite_nonnegative(path, line, column, row[column])
        for column in UNIT_INTERVAL_COLUMNS.intersection(row):
            if row[column] == "":
                continue
            value = _finite_nonnegative(path, line, column, row[column])
            if value > 1:
                raise ValueError(f"{path}:{line} {column} must be at most 1")
        if (
            "target_cache_coverage_percent" in row
            and row["target_cache_coverage_percent"] != ""
        ):
            coverage = _finite_nonnegative(
                path,
                line,
                "target_cache_coverage_percent",
                row["target_cache_coverage_percent"],
            )
            if coverage > 100:
                raise ValueError(
                    f"{path}:{line} target_cache_coverage_percent must be at most 100"
                )
        if {"p50_ms", "p95_ms", "p99_ms", "max_ms"}.issubset(row):
            percentiles = [
                float(row["p50_ms"]),
                float(row["p95_ms"]),
                float(row["p99_ms"]),
                float(row["max_ms"]),
            ]
            if percentiles != sorted(percentiles):
                raise ValueError(f"{path}:{line} latency percentiles are not ordered")
        if {
            "p50_batch_ms",
            "p95_batch_ms",
            "p99_batch_ms",
            "max_batch_ms",
        }.issubset(row):
            percentiles = [
                float(row["p50_batch_ms"]),
                float(row["p95_batch_ms"]),
                float(row["p99_batch_ms"]),
                float(row["max_batch_ms"]),
            ]
            if percentiles != sorted(percentiles):
                raise ValueError(
                    f"{path}:{line} batch latency percentiles are not ordered"
                )
        memory_columns = {
            "ram_budget_bytes",
            "collection_resident_bytes",
            "retained_bytes",
            "retained_capacity_bytes",
            "retained_peak_bytes",
            "transient_bytes",
            "transient_capacity_bytes",
            "transient_peak_bytes",
        }
        if memory_columns.issubset(row):
            memory = {
                column: _finite_nonnegative(path, line, column, row[column])
                for column in memory_columns
            }
            if memory["retained_bytes"] > memory["retained_capacity_bytes"]:
                raise ValueError(f"{path}:{line} retained bytes exceed capacity")
            if memory["retained_peak_bytes"] > memory["retained_capacity_bytes"]:
                raise ValueError(f"{path}:{line} retained peak exceeds capacity")
            if memory["transient_bytes"] > memory["transient_capacity_bytes"]:
                raise ValueError(f"{path}:{line} transient bytes exceed capacity")
            if memory["transient_peak_bytes"] > memory["transient_capacity_bytes"]:
                raise ValueError(f"{path}:{line} transient peak exceeds capacity")
            governed = (
                memory["collection_resident_bytes"]
                + memory["retained_capacity_bytes"]
                + memory["transient_capacity_bytes"]
            )
            if memory["ram_budget_bytes"] > 0 and governed > memory["ram_budget_bytes"]:
                raise ValueError(f"{path}:{line} governed memory exceeds RAM budget")
        if {
            "resident_bytes_estimate",
            "collection_resident_bytes",
        }.issubset(row) and float(row["resident_bytes_estimate"]) != float(
            row["collection_resident_bytes"]
        ):
            raise ValueError(f"{path}:{line} resident byte aliases do not match")


def _validate_sample_reconciliation(
    directory: Path,
    summary_name: str,
    raw_name: str,
    group_columns: tuple[str, ...],
    count_column: str,
) -> None:
    summary_path = directory / summary_name
    raw_path = directory / raw_name
    if not summary_path.is_file():
        return
    if not raw_path.is_file():
        raise ValueError(f"missing raw distribution artifact: {raw_path}")
    with summary_path.open(newline="") as handle:
        summaries = list(csv.DictReader(handle))
    with raw_path.open(newline="") as handle:
        samples = list(csv.DictReader(handle))
    observed = Counter(
        tuple(row[column] for column in group_columns) for row in samples
    )
    for line, row in enumerate(summaries, start=2):
        key = tuple(row[column] for column in group_columns)
        try:
            expected = int(row[count_column])
        except ValueError as error:
            raise ValueError(
                f"{summary_path}:{line} {count_column} must be an integer"
            ) from error
        if observed[key] != expected:
            raise ValueError(
                f"{summary_path}:{line} sample count mismatch for {key}: "
                f"summary={expected}, raw={observed[key]}"
            )


def validate_directory(
    directory: Path, expected_codec: str | None, required: tuple[str, ...]
) -> None:
    parsed: dict[str, list[dict[str, str]]] = {}
    for name in required:
        path = directory / name
        if not path.is_file():
            raise ValueError(f"missing benchmark artifact: {path}")
        with path.open(newline="") as handle:
            rows = list(csv.reader(handle))
        if not rows or not rows[0] or any(not column for column in rows[0]):
            raise ValueError(f"invalid header in {path}")
        if len(rows) < 2:
            raise ValueError(f"benchmark artifact has no data rows: {path}")
        width = len(rows[0])
        for line, row in enumerate(rows[1:], start=2):
            if len(row) != width:
                raise ValueError(
                    f"{path}:{line} has {len(row)} columns; expected {width}"
                )
        if expected_codec and "scan_codec" in rows[0]:
            codec_column = rows[0].index("scan_codec")
            mismatches = [
                row[codec_column]
                for row in rows[1:]
                if row[codec_column] != expected_codec
            ]
            if mismatches:
                raise ValueError(
                    f"{path} codec mismatch: expected {expected_codec}, observed {sorted(set(mismatches))}"
                )
        required_columns = REQUIRED_COLUMNS.get(name, set())
        missing = sorted(required_columns.difference(rows[0]))
        if missing:
            raise ValueError(f"{path} missing required columns: {', '.join(missing)}")
        with path.open(newline="") as handle:
            parsed[name] = list(csv.DictReader(handle))
        if name == "bench_query_samples.csv":
            validate_current_query_sample_rows(parsed[name], path)
        elif name in {"bench_concurrency.csv", "bench_concurrency_samples.csv"}:
            validate_production_bench_schema_rows(parsed[name], path)
            if name == "bench_concurrency_samples.csv":
                for line, row in enumerate(parsed[name], start=2):
                    validate_query_stage_timings(row, role=f"{path}:{line}")
        _validate_distribution_rows(path, parsed[name])

    _validate_sample_reconciliation(
        directory,
        "bench_recall_latency.csv",
        "bench_query_samples.csv",
        ("scan_codec", "cache_execution", "phase", "mode", "nprobe", "max_candidates"),
        "samples",
    )
    _validate_sample_reconciliation(
        directory,
        "bench_concurrency.csv",
        "bench_concurrency_samples.csv",
        (
            "scan_codec",
            "cache_execution",
            "nprobe",
            "max_candidates",
            "workers",
        ),
        "total_queries",
    )
    _validate_sample_reconciliation(
        directory,
        "bench_write_costs.csv",
        "bench_write_samples.csv",
        ("op",),
        "batches",
    )
    _validate_sample_reconciliation(
        directory,
        "bench_mutation_queries.csv",
        "bench_mutation_query_samples.csv",
        ("stage",),
        "queries",
    )
    _validate_sample_reconciliation(
        directory,
        "filter_summary.csv",
        "filter_samples.csv",
        (
            "dataset",
            "cache_profile",
            "target_cache_coverage_percent",
            "client_concurrency",
            "selectivity",
        ),
        "samples",
    )
    _validate_sample_reconciliation(
        directory,
        "namespace_summary.csv",
        "namespace_samples.csv",
        (
            "dataset",
            "cache_profile",
            "target_cache_coverage_percent",
            "client_concurrency",
            "phase",
            "namespace",
            "namespace_rows",
        ),
        "samples",
    )
    _validate_sample_reconciliation(
        directory,
        "late_interaction_summary.csv",
        "late_interaction_samples.csv",
        (
            "dataset",
            "cache_profile",
            "target_cache_coverage_percent",
            "client_concurrency",
            "frontier",
        ),
        "samples",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--expected-codec")
    parser.add_argument("--required", default=",".join(DEFAULT_REQUIRED))
    args = parser.parse_args()
    required = tuple(value for value in args.required.split(",") if value)
    validate_directory(args.directory, args.expected_codec, required)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
