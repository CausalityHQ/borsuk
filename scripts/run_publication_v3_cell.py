#!/usr/bin/env python3
"""Fail-closed execution adapter for one canonical Publication V3 cell."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import subprocess
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

try:
    from scripts.production_bench_schema import (
        PRODUCTION_BENCH_SCHEMA_VERSION,
        QUERY_STAGE_AGGREGATE_FIELD_BY_SAMPLE,
        QUERY_STAGE_AGGREGATE_FIELDS,
        QUERY_STAGE_MAX_FIELDS,
        validate_query_planner_read_telemetry,
        validate_query_stage_timings,
    )
    from scripts.publication_v3_attestation import (
        collect_runtime_attestation,
        runtime_attestation_sha256,
        validate_runtime_attestation,
    )
    from scripts.publication_v3_clones import (
        clone_receipt_document_sha256,
        require_verified_clone_inventory,
        validate_clone_receipt,
    )
    from scripts.publication_v3_protocol import (
        BORSUK_GLOBAL_SCAN_CODECS,
        BORSUK_TURBOQUANT_GLOBAL_SCAN_CODECS,
        DISK_CACHED_QUERY_CACHE_AUTHORITY_MIB,
        build_schedule_document,
        canonical_json_bytes,
        read_protocol,
        validate_manifest,
    )
    from scripts.publication_v3_receipts import (
        build_index_receipt,
        receipt_document_sha256,
        reconcile_index_inventory,
        require_verified_index,
        require_verified_object_roster,
        validate_index_receipt,
    )
    from scripts.publication_v3_results import validate_cell_result
except ModuleNotFoundError:
    from production_bench_schema import (  # type: ignore[no-redef]
        PRODUCTION_BENCH_SCHEMA_VERSION,
        QUERY_STAGE_AGGREGATE_FIELD_BY_SAMPLE,
        QUERY_STAGE_AGGREGATE_FIELDS,
        QUERY_STAGE_MAX_FIELDS,
        validate_query_planner_read_telemetry,
        validate_query_stage_timings,
    )
    from publication_v3_attestation import (
        collect_runtime_attestation,
        runtime_attestation_sha256,
        validate_runtime_attestation,
    )
    from publication_v3_clones import (
        clone_receipt_document_sha256,
        require_verified_clone_inventory,
        validate_clone_receipt,
    )
    from publication_v3_protocol import (
        BORSUK_GLOBAL_SCAN_CODECS,
        BORSUK_TURBOQUANT_GLOBAL_SCAN_CODECS,
        DISK_CACHED_QUERY_CACHE_AUTHORITY_MIB,
        build_schedule_document,
        canonical_json_bytes,
        read_protocol,
        validate_manifest,
    )
    from publication_v3_receipts import (
        build_index_receipt,
        receipt_document_sha256,
        reconcile_index_inventory,
        require_verified_index,
        require_verified_object_roster,
        validate_index_receipt,
    )
    from publication_v3_results import validate_cell_result


SUPPORTED_LOCAL_KINDS = frozenset({"read-recall", "write-update-delete-compact"})
V20_COMPATIBILITY_CANDIDATES = 512
V20_EXECUTION_ENGINE = "bounded-cell-card-v20"
CONCURRENCY_SWEEP = (1, 2, 4, 8, 16)
RUNTIME_FLOW_CONTROL_FIELDS = frozenset(
    {
        "disk_cache_max_bytes",
        "exact_read_max_physical_amplification",
        "max_active_searches",
        "max_waiting_searches",
        "leaf_read_width",
        "max_inflight_leaf_reads",
        "max_parallel_decode_rank_tasks",
        "cpu_threads",
        "io_threads",
        "s3_get_concurrency",
        "ram_budget_bytes",
    }
)
PRODUCTION_BUILD_FIELDS = tuple(
    "logical_cell_catalog_checksum,logical_cells,logical_cell_dimensions,logical_cell_catalog_bytes,vector_element_type,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,build_layout,leaf_capability,segment_max_vectors,records,segment_bytes,vector_sidecar_bytes,graph_bytes,global_scan_bytes,total_active_index_bytes,bytes_per_vector,resident_bytes_estimate,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes,ingest_ms,compaction_ms,compaction_bytes_read,compaction_bytes_written,gc_ms,gc_objects_scanned,gc_objects_deleted,gc_transaction_states_remaining,gc_bytes_read,gc_bytes_reclaimed,storage_gets,storage_puts,storage_deletes,storage_heads,storage_lists,storage_bytes_read,storage_bytes_written,configured_build_writers,ingest_batches,ingest_waves,ingest_vectors_per_s".split(
        ","
    )
)
BUILD_PHASE_FIELDS = ("schema_version", "group", "phase", "nanos", "calls")
BUILD_PHASE_NAMES = (
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
WRITE_COST_FIELDS = tuple(
    "op,configured_writers,configured_batch_records,ops,batches,wall_ms,ops_per_s,mean_batch_ms,stddev_batch_ms,p50_batch_ms,p95_batch_ms,p99_batch_ms,max_batch_ms,mean_amortized_ms,gets,puts,deletes,heads,lists,bytes_read,bytes_written".split(
        ","
    )
)
WRITE_SAMPLE_FIELDS = tuple(
    "op,writer_index,wave_index,batch_index,batch_records,batch_latency_ms,amortized_ms,gets,puts,deletes,heads,lists".split(
        ","
    )
)
LIFECYCLE_FIELDS = tuple(
    "configured_writers,configured_batch_records,inserted_vectors,logical_vector_bytes,insert_wall_ms,insert_vectors_per_s,first_batch_publish_ms,searchability_refresh_ms,time_to_searchable_ms,searchable_samples,searchable_fraction,upsert_samples,upsert_correct_fraction,delete_samples,delete_absent_fraction,compact_delete_absent_fraction,purge_delete_absent_fraction,delta_flush_ms,time_to_fully_indexed_ms,wal_publish_bytes,indexed_delta_bytes,total_indexing_bytes,write_amplification,write_amplification_is_lower_bound,consolidation_ms,time_to_consolidated_ms,consolidated_global_bytes,consolidation_amplification".split(
        ","
    )
)
LIFECYCLE_OPERATIONS = (
    "insert",
    "flush",
    "consolidate",
    "upsert",
    "delete",
    "compact",
    "purge",
)


def validate_publication_cell_authority(
    cell: dict[str, object], manifest_path: Path
) -> dict[str, object]:
    payload = manifest_path.read_bytes()
    try:
        manifest_value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("frozen manifest is not valid JSON") from error
    manifest = validate_manifest(manifest_value)
    if payload != canonical_json_bytes(manifest):
        raise ValueError("frozen manifest is not canonical")
    expected = next(
        (
            candidate
            for candidate in build_schedule_document(manifest)["cells"]
            if candidate["cell_id"] == cell.get("cell_id")
        ),
        None,
    )
    if expected is None:
        raise ValueError("publication cell differs from its frozen manifest authority")
    expected_index = str(expected["index_prefix"])
    index_root, index_name = expected_index.rsplit("/", 1)
    candidate_index = str(cell.get("index_prefix"))
    attempt_prefix = f"{index_root}/build-attempts/"
    attempt_tail = candidate_index.removeprefix(attempt_prefix)
    attempt_parts = attempt_tail.split("/")
    retry_index = (
        candidate_index.startswith(attempt_prefix)
        and len(attempt_parts) == 2
        and len(attempt_parts[0]) == 4
        and attempt_parts[0].isdigit()
        and 0 < int(attempt_parts[0]) <= 9_999
        and attempt_parts[1] == index_name
    )
    normalized = copy.deepcopy(cell)
    normalized["index_prefix"] = expected_index
    if canonical_json_bytes(normalized) != canonical_json_bytes(expected) or (
        candidate_index != expected_index and not retry_index
    ):
        raise ValueError("publication cell differs from its frozen manifest authority")
    return copy.deepcopy(cell)


def dataset_training_seed(dataset: dict[str, object]) -> int:
    source = dataset.get("source")
    if isinstance(source, dict):
        seed = source.get("seed")
        if isinstance(seed, int) and not isinstance(seed, bool) and seed >= 0:
            return seed
    dataset_id = dataset.get("id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise ValueError("dataset has no stable training identity")
    return int.from_bytes(hashlib.sha256(dataset_id.encode()).digest()[:8], "big")


def plan_arms(cell: dict[str, object]) -> list[dict[str, object]]:
    workload = cell.get("workload")
    if not isinstance(workload, dict):
        raise ValueError("cell workload is invalid")
    factors = workload.get("factors")
    if not isinstance(factors, dict):
        raise ValueError("workload factors are invalid")
    if workload.get("kind") == "write-update-delete-compact":
        axes = {
            "writers": factors.get("writers"),
            "batch_size": factors.get("batch_sizes"),
            "insert_mode": factors.get("insert_modes"),
            "update_percent": factors.get("update_percent"),
            "delete_percent": factors.get("delete_percent"),
        }
        if any(not isinstance(values, list) or not values for values in axes.values()):
            raise ValueError("lifecycle arm factors are incomplete")
        return [
            {
                "writers": writers,
                "batch_size": batch_size,
                "insert_mode": insert_mode,
                "update_percent": update_percent,
                "delete_percent": delete_percent,
            }
            for insert_mode in axes["insert_mode"]
            for writers in axes["writers"]
            for batch_size in axes["batch_size"]
            for update_percent in axes["update_percent"]
            for delete_percent in axes["delete_percent"]
        ]
    if workload.get("kind") != "read-recall":
        raise ValueError(f"workload kind {workload.get('kind')!r} is not executable")
    k_values = factors.get("k")
    if k_values != [10]:
        unsupported = next(
            (value for value in k_values or [] if value != 10), "missing"
        )
        raise ValueError(f"k={unsupported} is not executable by the current benchmark")
    leaf_page_budgets = factors.get("leaf_page_budgets")
    cache_states = factors.get("cache_states")
    if (
        not isinstance(leaf_page_budgets, list)
        or not leaf_page_budgets
        or not isinstance(cache_states, list)
        or not cache_states
        or any(budget not in {4, 8, 16, 32, 64} for budget in leaf_page_budgets)
    ):
        raise ValueError("read-recall arm factors are incomplete")
    if any(state not in {"cold", "warm"} for state in cache_states):
        raise ValueError("read-recall cache state is unsupported")
    return [
        {
            "k": 10,
            "leaf_page_budget": leaf_page_budget,
            "cache_state": state,
        }
        for leaf_page_budget in leaf_page_budgets
        for state in cache_states
    ]


def concurrency_result_arm(arm: dict[str, object]) -> dict[str, object]:
    if frozenset(arm) != frozenset({"k", "leaf_page_budget", "cache_state"}):
        raise ValueError("concurrency result arm fields differ")
    return copy.deepcopy(arm)


def runtime_flow_control_authority(
    mode: str, values: dict[str, int | None]
) -> dict[str, int] | None:
    if frozenset(values) != RUNTIME_FLOW_CONTROL_FIELDS:
        raise ValueError("runtime flow-control authority fields differ")
    supplied = sum(value is not None for value in values.values())
    if mode == "runtime" and supplied == 0:
        raise ValueError("runtime flow-control authority is required")
    if supplied not in {0, len(values)}:
        raise ValueError("runtime flow-control authority must be supplied atomically")
    if supplied != 0 and mode != "runtime":
        raise ValueError("runtime flow-control authority requires runtime mode")
    if supplied == 0:
        return None
    return {key: int(value) for key, value in values.items() if value is not None}


def build_execution_plan(
    cell: dict[str, object],
    *,
    arm: dict[str, object],
    workspace: Path,
    generator: Path,
    borsuk_bench: Path,
    mode: str,
    runtime_profile: str = "recall",
    runtime_flow_control: dict[str, int] | None = None,
    diagnostic_write_ops: int | None = None,
    diagnostic_read_nprobes: tuple[int, ...] | None = None,
    diagnostic_read_candidates: tuple[int, ...] | None = None,
    v21_feasibility: bool = False,
) -> dict[str, object]:
    if mode not in {"build", "runtime", "smoke"}:
        raise ValueError("execution mode must be build, runtime, or smoke")
    if runtime_profile not in {"recall", "concurrency", "lifecycle"}:
        raise ValueError("runtime profile must be recall, concurrency, or lifecycle")
    if mode != "runtime" and runtime_profile != "recall":
        raise ValueError("non-default runtime profile requires runtime mode")
    if mode == "runtime" and runtime_flow_control is None:
        raise ValueError("runtime flow-control authority is required")
    if mode != "runtime" and runtime_flow_control is not None:
        raise ValueError("runtime flow-control authority requires runtime mode")
    if diagnostic_write_ops is not None and (
        mode != "runtime"
        or runtime_profile != "lifecycle"
        or isinstance(diagnostic_write_ops, bool)
        or not 1 <= diagnostic_write_ops <= 50_000
    ):
        raise ValueError("lifecycle diagnostic write count is invalid")
    read_diagnostic_values = (
        diagnostic_read_nprobes,
        diagnostic_read_candidates,
    )
    if (
        type(v21_feasibility) is not bool
        or v21_feasibility
        and (
            mode != "runtime"
            or runtime_profile != "recall"
            or diagnostic_write_ops is not None
            or any(value is not None for value in read_diagnostic_values)
        )
    ):
        raise ValueError("V21 feasibility mode must be an exclusive recall runtime")
    if any(value is not None for value in read_diagnostic_values):
        if (
            any(value is None for value in read_diagnostic_values)
            or mode != "runtime"
            or runtime_profile != "recall"
            or diagnostic_write_ops is not None
        ):
            raise ValueError("read diagnostic authority must be supplied atomically")
        assert diagnostic_read_nprobes is not None
        assert diagnostic_read_candidates is not None
        if (
            not diagnostic_read_nprobes
            or not diagnostic_read_candidates
            or tuple(sorted(set(diagnostic_read_nprobes))) != diagnostic_read_nprobes
            or tuple(sorted(set(diagnostic_read_candidates)))
            != diagnostic_read_candidates
            or any(
                isinstance(value, bool) or not 1 <= value <= 256
                for value in diagnostic_read_nprobes
            )
            or any(
                isinstance(value, bool) or not 1 <= value <= 16_384
                for value in diagnostic_read_candidates
            )
            or len(diagnostic_read_nprobes) * len(diagnostic_read_candidates) > 32
        ):
            raise ValueError("read diagnostic authority is invalid")
    if runtime_flow_control is not None and (
        frozenset(runtime_flow_control) != RUNTIME_FLOW_CONTROL_FIELDS
        or any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in runtime_flow_control.values()
        )
        or runtime_flow_control["disk_cache_max_bytes"] < 0
        or any(
            runtime_flow_control[field] <= 0
            for field in RUNTIME_FLOW_CONTROL_FIELDS - {"disk_cache_max_bytes"}
        )
    ):
        raise ValueError("runtime flow-control authority is invalid")
    if cell.get("system") != "borsuk":
        raise ValueError(
            f"system {cell.get('system')!r} is not available in local execution"
        )
    workload = cell.get("workload")
    dataset = cell.get("dataset")
    source = cell.get("source")
    if (
        not isinstance(workload, dict)
        or workload.get("kind") not in SUPPORTED_LOCAL_KINDS
    ):
        raise ValueError("workload is not supported by the local read runner")
    if not isinstance(dataset, dict) or not isinstance(dataset.get("source"), dict):
        raise ValueError("cell dataset is invalid")
    publication = mode != "smoke"
    if publication and (
        not isinstance(source, dict) or source.get("state") != "frozen"
    ):
        raise ValueError("publication execution requires a frozen source archive")
    environment = cell.get("environment_contract")
    if not isinstance(environment, dict):
        raise ValueError("cell environment contract is invalid")
    region = environment.get("region")
    if not isinstance(region, str) or not region:
        raise ValueError("cell environment region is invalid")
    runtime_clients = environment.get("runtime_clients")
    runtime_storage = environment.get("runtime_storage")
    if not isinstance(runtime_clients, dict) or not isinstance(runtime_storage, dict):
        raise ValueError("cell has no bounded runtime-client contract")
    runtime_client = runtime_clients.get("borsuk")
    if not isinstance(runtime_client, dict):
        raise ValueError("cell has no BORSUK runtime-client contract")
    resident_limit_mib = runtime_client.get("resident_limit_mib")
    disk_cache_limit_mib = runtime_client.get("disk_cache_limit_mib")
    runtime_vcpus = runtime_client.get("vcpus")
    if (
        isinstance(resident_limit_mib, bool)
        or not isinstance(resident_limit_mib, int)
        or resident_limit_mib <= 0
        or isinstance(disk_cache_limit_mib, bool)
        or not isinstance(disk_cache_limit_mib, int)
        or disk_cache_limit_mib <= 0
        or isinstance(runtime_vcpus, bool)
        or not isinstance(runtime_vcpus, int)
        or runtime_vcpus <= 0
    ):
        raise ValueError("BORSUK runtime-client limits are invalid")
    build_workers = environment.get("build_workers")
    build_storage = environment.get("build_storage")
    build_worker = (
        build_workers.get("borsuk") if isinstance(build_workers, dict) else None
    )
    if not isinstance(build_worker, dict) or not isinstance(build_storage, dict):
        raise ValueError("cell has no BORSUK offline-build contract")

    factors = workload.get("factors")
    scale = dataset.get("scale")
    index_profile = cell.get("index_profile")
    if (
        not isinstance(factors, dict)
        or not isinstance(scale, dict)
        or not isinstance(index_profile, dict)
    ):
        raise ValueError("cell workload factors or dataset scale are invalid")
    scheduled_rows = scale.get("rows")
    dimensions = dataset.get("dimensions")
    if (
        isinstance(scheduled_rows, bool)
        or not isinstance(scheduled_rows, int)
        or scheduled_rows <= 0
        or isinstance(dimensions, bool)
        or not isinstance(dimensions, int)
        or dimensions <= 0
    ):
        raise ValueError("cell rows and dimensions must be positive integers")
    queries_per_repetition = cell.get("queries_per_repetition")
    if isinstance(queries_per_repetition, bool) or not isinstance(
        queries_per_repetition, int
    ):
        raise ValueError("cell source query count is invalid")

    profile_cells = index_profile.get("logical_cells")
    minimum_rows_per_cell = index_profile.get("minimum_rows_per_logical_cell")
    if (
        isinstance(profile_cells, bool)
        or not isinstance(profile_cells, int)
        or profile_cells <= 0
        or isinstance(minimum_rows_per_cell, bool)
        or not isinstance(minimum_rows_per_cell, int)
        or minimum_rows_per_cell <= 0
    ):
        raise ValueError("BORSUK logical-cell profile is not executable")
    smoke_cells = min(profile_cells, 128)
    smoke_rows = max(1_000, smoke_cells * minimum_rows_per_cell)
    dense_generators = {
        "synthetic-clustered-v1",
        "synthetic-uniform-v1",
        "synthetic-duplicate-v1",
        "synthetic-adversarial-v1",
    }
    if mode == "smoke" and dataset["source"].get("generator") in dense_generators:
        smoke_rows = ((smoke_rows + 99) // 100) * 100
    effective_rows = scheduled_rows if publication else min(scheduled_rows, smoke_rows)
    effective_queries = (
        queries_per_repetition if publication else min(queries_per_repetition, 10)
    )
    dataset_dir = workspace / "dataset"
    output_dir = workspace / "output"
    index_dir = workspace / "index"
    cache_dir = workspace / "cache"
    if arm not in plan_arms(cell):
        raise ValueError("execution arm is not authorized by the scheduled cell")
    if runtime_flow_control is not None:
        expected_disk_cache_bytes = (
            0
            if arm.get("cache_state", "cold") == "cold"
            else disk_cache_limit_mib * 1024 * 1024
        )
        if (
            runtime_flow_control["ram_budget_bytes"] != resident_limit_mib * 1024 * 1024
            or runtime_flow_control["disk_cache_max_bytes"] != expected_disk_cache_bytes
            or runtime_flow_control["cpu_threads"] > runtime_vcpus
            or runtime_flow_control["cpu_threads"] > 64
            or runtime_flow_control["max_active_searches"] > runtime_vcpus * 4
            or runtime_flow_control["max_waiting_searches"] > runtime_vcpus * 16
            or runtime_flow_control["max_parallel_decode_rank_tasks"]
            > runtime_flow_control["cpu_threads"]
            or runtime_flow_control["s3_get_concurrency"] > 128
            or not (
                runtime_flow_control["s3_get_concurrency"]
                <= runtime_flow_control["io_threads"]
                <= 256
            )
            or runtime_flow_control["leaf_read_width"] > 1_024
            or runtime_flow_control["max_inflight_leaf_reads"] > 1_024
            or not 1
            <= runtime_flow_control["exact_read_max_physical_amplification"]
            <= 5
            or (
                runtime_profile == "concurrency"
                and runtime_flow_control["max_active_searches"] < max(CONCURRENCY_SWEEP)
            )
        ):
            raise ValueError("runtime flow-control authority violates runtime bounds")

    steps: list[dict[str, object]] = []
    dataset_source = dataset["source"]
    if dataset_source.get("state") == "generated":
        generator_id = dataset_source.get("generator")
        if generator_id not in dense_generators:
            raise ValueError("scheduled synthetic generator is not implemented")
        if dataset.get("metric") != "cosine":
            raise ValueError(
                "the deterministic dense generator supports cosine cells only"
            )
        steps.append(
            {
                "argv": [str(generator)],
                "env": {
                    "BORSUK_SYNTHETIC_OUTPUT": str(dataset_dir),
                    "BORSUK_SYNTHETIC_GENERATOR": str(generator_id),
                    "BORSUK_SYNTHETIC_DATASET_ID": str(dataset["id"]),
                    "BORSUK_SYNTHETIC_TRAIN": str(effective_rows),
                    "BORSUK_SYNTHETIC_DIMENSIONS": str(dimensions),
                    "BORSUK_SYNTHETIC_QUERIES": str(effective_queries),
                    "BORSUK_SYNTHETIC_GROUP_SIZE": "100",
                    "BORSUK_SYNTHETIC_SEED": str(dataset_source.get("seed")),
                },
            }
        )
    elif dataset_source.get("state") not in {"staged", "staged-generated"}:
        raise ValueError("dataset must be generated or staged before execution")

    workload_kind = workload.get("kind")
    if workload_kind == "read-recall":
        routing_budget = arm["leaf_page_budget"]
    else:
        routing_budget = 32
    benchmark_env = {
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
        "BORSUK_BENCH_DATASET": str(dataset_dir),
        "BORSUK_BENCH_URI": str(index_dir),
        "BORSUK_BENCH_CACHE": str(cache_dir),
        "BORSUK_BENCH_OUTPUT_DIR": str(output_dir),
        "BORSUK_BENCH_QUERIES": str(effective_queries),
        "BORSUK_BENCH_QUERY_SEED": str(cell.get("query_seed")),
        "BORSUK_BENCH_REPETITION_ID": str(cell.get("repetition_id")),
        "BORSUK_BENCH_NPROBES": str(routing_budget),
        "BORSUK_BENCH_CANDIDATES": str(V20_COMPATIBILITY_CANDIDATES),
        "BORSUK_BENCH_READ_ONLY": "1",
        "BORSUK_BENCH_CONCURRENCY": "1",
        "BORSUK_BENCH_SKIP_EXACT_RECALL": "1",
        "BORSUK_BENCH_CACHE_PROFILE": (
            "uncached" if arm.get("cache_state", "cold") == "cold" else "disk_cached"
        ),
        "BORSUK_BENCH_CACHE_COVERAGE_PERCENT": (
            "0" if arm.get("cache_state", "cold") == "cold" else "100"
        ),
        "BORSUK_BENCH_RAM_BUDGET_BYTES": str(resident_limit_mib * 1024 * 1024),
        "BORSUK_BENCH_MAX_ACTIVE_SEARCHES": str(runtime_vcpus),
        "BORSUK_BENCH_MAX_WAITING_SEARCHES": "16",
        "BORSUK_BENCH_LEAF_READ_WIDTH": "32",
        "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS": "48",
        "BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS": "1",
        "BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION": "2",
        "BORSUK_CPU_THREADS": str(max(1, min(runtime_vcpus - 1, 4))),
        "BORSUK_IO_THREADS": "88",
        "BORSUK_BACKING_GET_CONCURRENCY": "64",
        "BORSUK_BENCH_DISK_CACHE_MAX_BYTES": str(
            0
            if arm.get("cache_state", "cold") == "cold"
            else disk_cache_limit_mib * 1024 * 1024
        ),
    }
    global_scan_codec = index_profile.get("global_scan_codec")
    turboquant = global_scan_codec in BORSUK_TURBOQUANT_GLOBAL_SCAN_CODECS
    code_bytes = index_profile.get("code_bytes")
    turboquant_values = tuple(
        index_profile.get(field)
        for field in (
            "turboquant_bits",
            "turboquant_qjl_bits",
            "turboquant_shards",
        )
    )
    training_rows_per_cell = index_profile.get("training_rows_per_cell")
    training_iterations = index_profile.get("training_iterations")
    if (
        (
            not turboquant
            and (
                isinstance(code_bytes, bool)
                or not isinstance(code_bytes, int)
                or code_bytes <= 0
            )
        )
        or (
            turboquant
            and any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in turboquant_values
            )
        )
        or isinstance(training_rows_per_cell, bool)
        or not isinstance(training_rows_per_cell, int)
        or training_rows_per_cell <= 0
        or isinstance(training_iterations, bool)
        or not isinstance(training_iterations, int)
        or training_iterations <= 0
    ):
        raise ValueError("BORSUK index profile is not executable")
    effective_cells = (
        profile_cells
        if publication
        else min(profile_cells, max(1, effective_rows // minimum_rows_per_cell))
    )
    training_rows = min(effective_rows, effective_cells * training_rows_per_cell)
    if training_rows < effective_cells:
        raise ValueError(
            "BORSUK index profile has fewer training rows than logical cells"
        )
    benchmark_env.update(
        {
            "BORSUK_BENCH_LOGICAL_CELLS": str(effective_cells),
            "BORSUK_BENCH_LOGICAL_CELL_TRAINING_ROWS": str(training_rows),
            "BORSUK_BENCH_LOGICAL_CELL_SEED": str(dataset_training_seed(dataset)),
            "BORSUK_BENCH_LOGICAL_CELL_ITERATIONS": str(training_iterations),
            "BORSUK_BENCH_GLOBAL_SCAN_CODEC": str(global_scan_codec),
        }
    )
    if turboquant:
        benchmark_env.update(
            {
                "BORSUK_BENCH_TURBOQUANT_BITS": str(index_profile["turboquant_bits"]),
                "BORSUK_BENCH_TURBOQUANT_QJL_BITS": str(
                    index_profile["turboquant_qjl_bits"]
                ),
                "BORSUK_BENCH_TURBOQUANT_SHARDS": str(
                    index_profile["turboquant_shards"]
                ),
                "BORSUK_BENCH_RECALL_LEAF_MODE": str(global_scan_codec),
                "BORSUK_BENCH_SERVING_LEAF_MODE": str(global_scan_codec),
            }
        )
    else:
        benchmark_env["BORSUK_BENCH_GLOBAL_PQ_CODE_BYTES"] = str(code_bytes)
    if publication:
        index_uri = str(cell.get("index_prefix"))
        runtime_dataset_dir = workspace / "runtime-dataset"
        build_output_dir = workspace / "build-output"
        runtime_output_dir = workspace / "runtime-output"
        build_env = {
            **benchmark_env,
            "BORSUK_BENCH_DATASET": str(dataset_dir),
            "BORSUK_BENCH_URI": index_uri,
            "BORSUK_BENCH_OUTPUT_DIR": str(build_output_dir),
            "BORSUK_BENCH_BUILD_INDEX": "1",
            "BORSUK_BENCH_BUILD_ONLY": "1",
            "BORSUK_BUILD_TIMING": "1",
            "BORSUK_BUILD_TIMING_OUTPUT": str(
                build_output_dir / "bench_build_phases.csv"
            ),
            "BORSUK_CPU_THREADS": "32",
            "BORSUK_BENCH_BUILD_WRITERS": "8",
        }
        for field in (
            "BORSUK_BENCH_READ_ONLY",
            "BORSUK_BENCH_RECALL_ONLY",
            "BORSUK_BENCH_CACHE_PROFILE",
            "BORSUK_BENCH_RAM_BUDGET_BYTES",
            "BORSUK_BENCH_DISK_CACHE_MAX_BYTES",
        ):
            build_env.pop(field, None)
        # Persist the same resident-memory contract the small runtime must
        # honor. The larger worker supplies CPU and process headroom, but must
        # not produce an index that the scheduled runtime cannot open.
        build_env["BORSUK_BENCH_RAM_BUDGET_BYTES"] = str(
            resident_limit_mib * 1024 * 1024
        )
        # Index construction does not execute recall queries, but the benchmark
        # validates all query knobs at startup. Keep this build-only phase on a
        # valid V20 leaf-page/candidate compatibility pair; runtime owns the
        # scheduled recall sweep and its result labels.
        build_env["BORSUK_BENCH_NPROBES"] = "4"
        build_env["BORSUK_BENCH_CANDIDATES"] = str(V20_COMPATIBILITY_CANDIDATES)
        runtime_env = {
            **benchmark_env,
            "BORSUK_BENCH_DATASET": str(runtime_dataset_dir),
            "BORSUK_BENCH_URI": index_uri,
            "BORSUK_BENCH_OUTPUT_DIR": str(runtime_output_dir),
            "BORSUK_BENCH_RECALL_ONLY": "1",
            "BORSUK_BENCH_READ_ONLY": "1",
            "BORSUK_BENCH_BUILD_INDEX": "0",
            "BORSUK_STORAGE_TRACE": str(runtime_output_dir / "storage-access.csv"),
        }
        if diagnostic_read_nprobes is not None:
            assert diagnostic_read_candidates is not None
            runtime_env.update(
                {
                    "BORSUK_BENCH_NPROBES": ",".join(
                        str(value) for value in diagnostic_read_nprobes
                    ),
                    "BORSUK_BENCH_CANDIDATES": ",".join(
                        str(value) for value in diagnostic_read_candidates
                    ),
                }
            )
        if runtime_profile == "concurrency":
            runtime_env.update(
                {
                    "BORSUK_BENCH_RECALL_ONLY": "0",
                    "BORSUK_BENCH_SKIP_RECALL": "1",
                    "BORSUK_BENCH_CONCURRENCY": ",".join(
                        str(value) for value in CONCURRENCY_SWEEP
                    ),
                    "BORSUK_BENCH_SERVING_NPROBE": str(routing_budget),
                    "BORSUK_BENCH_SERVING_CANDIDATES": str(
                        V20_COMPATIBILITY_CANDIDATES
                    ),
                }
            )
        if runtime_flow_control is not None:
            runtime_env.update(
                {
                    "BORSUK_BENCH_MAX_ACTIVE_SEARCHES": str(
                        runtime_flow_control["max_active_searches"]
                    ),
                    "BORSUK_BENCH_MAX_WAITING_SEARCHES": str(
                        runtime_flow_control["max_waiting_searches"]
                    ),
                    "BORSUK_BENCH_LEAF_READ_WIDTH": str(
                        runtime_flow_control["leaf_read_width"]
                    ),
                    "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS": str(
                        runtime_flow_control["max_inflight_leaf_reads"]
                    ),
                    "BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS": str(
                        runtime_flow_control["max_parallel_decode_rank_tasks"]
                    ),
                    "BORSUK_CPU_THREADS": str(runtime_flow_control["cpu_threads"]),
                    "BORSUK_IO_THREADS": str(runtime_flow_control["io_threads"]),
                    "BORSUK_BACKING_GET_CONCURRENCY": str(
                        runtime_flow_control["s3_get_concurrency"]
                    ),
                    "BORSUK_BENCH_RAM_BUDGET_BYTES": str(
                        runtime_flow_control["ram_budget_bytes"]
                    ),
                    "BORSUK_BENCH_DISK_CACHE_MAX_BYTES": str(
                        runtime_flow_control["disk_cache_max_bytes"]
                    ),
                    "BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION": str(
                        runtime_flow_control["exact_read_max_physical_amplification"]
                    ),
                }
            )
        if workload_kind == "write-update-delete-compact":
            runtime_env.update(
                {
                    "BORSUK_BENCH_RECALL_ONLY": "0",
                    "BORSUK_BENCH_READ_ONLY": "0",
                    "BORSUK_BENCH_SKIP_RECALL": "1",
                    "BORSUK_BENCH_LIFECYCLE_ONLY": "1",
                    "BORSUK_BENCH_WRITE_BATCH_SIZE": str(arm["batch_size"]),
                    "BORSUK_BENCH_LIFECYCLE_WRITERS": str(arm["writers"]),
                    "BORSUK_BENCH_LIFECYCLE_INSERT_MODE": str(arm["insert_mode"]),
                    "BORSUK_BENCH_UPDATE_PERCENT": str(arm["update_percent"]),
                    "BORSUK_BENCH_DELETE_PERCENT": str(arm["delete_percent"]),
                }
            )
            if diagnostic_write_ops is not None:
                runtime_env["BORSUK_BENCH_WRITE_OPS"] = str(diagnostic_write_ops)
                runtime_env["BORSUK_OPEN_PROGRESS"] = "1"
                runtime_env["BORSUK_LIFECYCLE_PROGRESS"] = "1"
                runtime_env["BORSUK_V20_PROGRESS"] = "1"
        if v21_feasibility:
            if (
                workload.get("id") != "standard-ann-read"
                or dataset.get("id") != "deep-image-96"
                or cell.get("repetition_id") != "r01"
                or arm != plan_arms(cell)[0]
                or not isinstance(source, dict)
                or not isinstance(source.get("archive_sha256"), str)
                or not isinstance(cell.get("index_prefix"), str)
            ):
                raise ValueError(
                    "V21 feasibility requires the canonical Deep Image build authority"
                )
            for field in (
                "BORSUK_BENCH_BUILD_INDEX",
                "BORSUK_BENCH_READ_ONLY",
                "BORSUK_BENCH_RECALL_ONLY",
                "BORSUK_BENCH_SKIP_RECALL",
                "BORSUK_BENCH_SKIP_EXACT_RECALL",
                "BORSUK_BENCH_NPROBES",
                "BORSUK_BENCH_CANDIDATES",
                "BORSUK_BENCH_CONCURRENCY",
                "BORSUK_BENCH_SERVING_NPROBE",
                "BORSUK_BENCH_SERVING_CANDIDATES",
                "BORSUK_STORAGE_TRACE",
            ):
                runtime_env.pop(field, None)
            runtime_env.update(
                {
                    "BORSUK_BENCH_V21_FEASIBILITY": "1",
                    "BORSUK_BENCH_V21_SOURCE_ARCHIVE_SHA256": str(
                        source["archive_sha256"]
                    ),
                    "BORSUK_BENCH_V21_INDEX_ID": str(cell["index_prefix"])
                    .rstrip("/")
                    .rsplit("/", 1)[-1],
                    "BORSUK_BENCH_V21_DATASET_ID": str(dataset["id"]),
                }
            )
        return {
            "schema_version": 1,
            "cell_id": cell.get("cell_id"),
            "mode": "publication",
            "publishable": not v21_feasibility,
            "v21_feasibility": v21_feasibility,
            "effective_rows": effective_rows,
            "effective_queries": effective_queries,
            "workspace": str(workspace),
            "build": {
                "worker": build_worker,
                "storage": build_storage,
                "output_dir": str(build_output_dir),
                "steps": [*steps, {"argv": [str(borsuk_bench)], "env": build_env}],
            },
            "runtime": {
                "client": runtime_client,
                "storage": runtime_storage,
                "dataset_dir": str(runtime_dataset_dir),
                "output_dir": str(runtime_output_dir),
                "steps": [{"argv": [str(borsuk_bench)], "env": runtime_env}],
            },
        }
    benchmark_env["BORSUK_BENCH_LIMIT"] = str(effective_rows)
    steps.append({"argv": [str(borsuk_bench)], "env": benchmark_env})
    return {
        "schema_version": 1,
        "cell_id": cell.get("cell_id"),
        "mode": "smoke",
        "publishable": False,
        "effective_rows": effective_rows,
        "effective_queries": effective_queries,
        "runtime_client": runtime_client,
        "runtime_storage": runtime_storage,
        "workspace": str(workspace),
        "output_dir": str(output_dir),
        "steps": steps,
    }


def authorize_publication_runtime(
    plan: dict[str, object],
    *,
    receipt: dict[str, object],
    cell: dict[str, object],
    source_archive_sha256: str,
    dataset_materialization_sha256: str,
) -> dict[str, object]:
    workload = cell.get("workload")
    if not isinstance(workload, dict) or workload.get("kind") != "read-recall":
        raise ValueError("immutable index runtime authorization is read-only")
    v21_feasibility = plan.get("v21_feasibility") is True
    if plan.get("mode") != "publication" or (
        plan.get("publishable") is not True
        and not (v21_feasibility and plan.get("publishable") is False)
    ):
        raise ValueError("only a publication plan has an authorized runtime phase")
    if plan.get("cell_id") != cell.get("cell_id"):
        raise ValueError("runtime plan cell differs from its protocol")
    validated_receipt = validate_index_receipt(
        receipt,
        cell=cell,
        source_archive_sha256=source_archive_sha256,
        dataset_materialization_sha256=dataset_materialization_sha256,
    )
    runtime = plan.get("runtime")
    if not isinstance(runtime, dict) or not isinstance(runtime.get("steps"), list):
        raise ValueError("publication plan has no runtime phase")
    authorized = copy.deepcopy(runtime)
    for step in authorized["steps"]:
        if not isinstance(step, dict) or not isinstance(step.get("env"), dict):
            raise ValueError("publication runtime step is invalid")
        environment = step["env"]
        if environment.get("BORSUK_BENCH_URI") != validated_receipt["index_uri"]:
            raise ValueError(
                "runtime index URI differs from the immutable build receipt"
            )
        if environment.get("BORSUK_BENCH_BUILD_INDEX") != "0" and not (
            v21_feasibility
            and "BORSUK_BENCH_BUILD_INDEX" not in environment
            and environment.get("BORSUK_BENCH_V21_FEASIBILITY") == "1"
        ):
            raise ValueError("publication runtime must disable index construction")
        for forbidden in (
            "BORSUK_BENCH_BUILD_ONLY",
            "BORSUK_BENCH_INSERT_ONLY",
            "BORSUK_BENCH_RECLUSTER_BUILD",
            "BORSUK_BENCH_PRELOAD_SERVING",
            "BORSUK_BENCH_LIFECYCLE_ONLY",
        ):
            if forbidden in environment:
                raise ValueError("publication runtime step contains a build-only flag")
    authorized["index_receipt_sha256"] = receipt_document_sha256(validated_receipt)
    return authorized


def authorize_publication_mutation_runtime(
    plan: dict[str, object],
    *,
    clone_receipt: dict[str, object],
    base_receipt: dict[str, object],
    arm: dict[str, object],
    attempt_id: str,
    cell: dict[str, object],
) -> dict[str, object]:
    workload = cell.get("workload")
    if (
        not isinstance(workload, dict)
        or workload.get("kind") != "write-update-delete-compact"
    ):
        raise ValueError("mutation runtime requires a lifecycle cell")
    writers = arm.get("writers")
    if (
        isinstance(writers, bool)
        or not isinstance(writers, int)
        or not 1 <= writers <= 64
    ):
        raise ValueError("publication lifecycle writers must be in 1..=64")
    clone = validate_clone_receipt(
        clone_receipt,
        cell=cell,
        arm=arm,
        attempt_id=attempt_id,
        base_receipt=base_receipt,
    )
    if clone["clone_index_uri"] == base_receipt.get("index_uri"):
        raise ValueError("mutation runtime must never target the immutable base")
    runtime = plan.get("runtime")
    if not isinstance(runtime, dict) or not isinstance(runtime.get("steps"), list):
        raise ValueError("publication plan has no mutation runtime")
    authorized = copy.deepcopy(runtime)
    for step in authorized["steps"]:
        if not isinstance(step, dict) or not isinstance(step.get("env"), dict):
            raise ValueError("publication mutation step is invalid")
        environment = step["env"]
        if environment.get("BORSUK_BENCH_URI") != base_receipt.get("index_uri"):
            raise ValueError("mutation plan does not originate from its immutable base")
        environment["BORSUK_BENCH_URI"] = clone["clone_index_uri"]
        if environment.get("BORSUK_BENCH_LIFECYCLE_WRITERS") != str(writers):
            raise ValueError(
                "mutation plan writer count differs from its lifecycle arm"
            )
        if environment.get("BORSUK_BENCH_LIFECYCLE_INSERT_MODE") != arm.get(
            "insert_mode"
        ):
            raise ValueError("mutation plan insert mode differs from its lifecycle arm")
        if (
            environment.get("BORSUK_BENCH_BUILD_INDEX") != "0"
            or environment.get("BORSUK_BENCH_READ_ONLY") != "0"
            or environment.get("BORSUK_BENCH_RECALL_ONLY") != "0"
            or environment.get("BORSUK_BENCH_SKIP_RECALL") != "1"
            or environment.get("BORSUK_BENCH_LIFECYCLE_ONLY") != "1"
        ):
            raise ValueError("mutation runtime flags are invalid")
    authorized["base_index_receipt_sha256"] = receipt_document_sha256(base_receipt)
    authorized["clone_receipt_sha256"] = clone["receipt_sha256"]
    return authorized


def execute_plan_with_resources(
    plan: dict[str, object],
) -> tuple[Path, dict[str, int], int]:
    output_dir, resources, elapsed_ns = _execute_steps_with_resources(
        workspace=Path(str(plan.get("workspace"))),
        output_dir=Path(str(plan.get("output_dir"))),
        steps=plan.get("steps"),
    )
    samples = output_dir / "bench_query_samples.csv"
    if not samples.is_file() or samples.stat().st_size == 0:
        raise ValueError("execution completed without a real query sample artifact")
    return samples, resources, elapsed_ns


def _execute_steps_with_resources(
    *, workspace: Path, output_dir: Path, steps: object
) -> tuple[Path, dict[str, int], int]:
    if not isinstance(steps, list) or not steps:
        raise ValueError("execution plan has no steps")
    workspace.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    cpu_ns = 0
    peak_rss_bytes = 0
    disk_read_bytes = 0
    disk_write_bytes = 0
    started_ns = time.monotonic_ns()
    for index, step in enumerate(steps):
        if not isinstance(step, dict) or not isinstance(step.get("argv"), list):
            raise ValueError("execution step is invalid")
        argv = [str(value) for value in step["argv"]]
        if not argv or any(not value for value in argv):
            raise ValueError("execution argv is invalid")
        environment = step.get("env")
        if not isinstance(environment, dict):
            raise ValueError("execution environment is invalid")
        child_environment = {
            "HOME": os.environ.get("HOME", "/tmp"),
            "LANG": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
        }
        child_environment.update(
            {str(key): str(value) for key, value in environment.items()}
        )
        log_path = workspace / f"step-{index:02d}.log"
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                argv,
                cwd=workspace,
                env=child_environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, argv)
        cpu_ns += round((usage.ru_utime + usage.ru_stime) * 1_000_000_000)
        peak_rss_bytes = max(peak_rss_bytes, int(usage.ru_maxrss) * 1024)
        disk_read_bytes += int(usage.ru_inblock) * 512
        disk_write_bytes += int(usage.ru_oublock) * 512
    elapsed_ns = time.monotonic_ns() - started_ns
    return (
        output_dir,
        {
            "cpu_ns": cpu_ns,
            "peak_rss_bytes": peak_rss_bytes,
            "disk_read_bytes": disk_read_bytes,
            "disk_write_bytes": disk_write_bytes,
        },
        elapsed_ns,
    )


def execute_publication_phase(
    plan: dict[str, object], phase: str
) -> tuple[Path, dict[str, int], int]:
    if plan.get("mode") != "publication" or phase not in {"build", "runtime"}:
        raise ValueError("publication execution phase is invalid")
    selected = plan.get(phase)
    if not isinstance(selected, dict):
        raise ValueError("publication execution phase is missing")
    return _execute_steps_with_resources(
        workspace=Path(str(plan.get("workspace"))) / phase,
        output_dir=Path(str(selected.get("output_dir"))),
        steps=selected.get("steps"),
    )


def execute_plan(plan: dict[str, object]) -> Path:
    samples, _, _ = execute_plan_with_resources(plan)
    return samples


def _nearest_rank(values: list[int], quantile: float) -> int:
    return sorted(values)[max(0, math.ceil(quantile * len(values)) - 1)]


def _validated_query_stage_timings(row: dict[str, str], *, role: str) -> dict[str, int]:
    return validate_query_stage_timings(row, role=role)


def _accumulate_query_stage_timings(
    totals: dict[str, int], sample: dict[str, int]
) -> None:
    for field, value in sample.items():
        aggregate_field = QUERY_STAGE_AGGREGATE_FIELD_BY_SAMPLE[field]
        if field in QUERY_STAGE_MAX_FIELDS:
            totals[aggregate_field] = max(totals[aggregate_field], value)
        else:
            totals[aggregate_field] += value


def disk_cached_cohort_authority(
    disk_cache_max_bytes: int, expected_queries: int
) -> tuple[int, int]:
    if (
        type(disk_cache_max_bytes) is not int
        or type(expected_queries) is not int
        or disk_cache_max_bytes <= 0
        or expected_queries <= 0
    ):
        raise ValueError("disk cache cohort authority is invalid")
    cohort_bytes = disk_cache_max_bytes * 3 // 4
    cache_safe_queries = cohort_bytes // (
        DISK_CACHED_QUERY_CACHE_AUTHORITY_MIB * 1024 * 1024
    )
    if cache_safe_queries < expected_queries:
        raise ValueError("disk cache cannot fund the complete query set")
    return expected_queries, 1


def runtime_expected_cache_cohort_size(
    arm: dict[str, object],
    *,
    runtime_profile: str,
    effective_flow_control: dict[str, object],
    effective_queries: int,
) -> int:
    if arm.get("cache_state") != "warm":
        return 0
    try:
        disk_cache_max_bytes = effective_flow_control["disk_cache_max_bytes"]
    except KeyError as error:
        raise ValueError("runtime cache cohort authority is invalid") from error
    if type(disk_cache_max_bytes) is not int or disk_cache_max_bytes <= 0:
        raise ValueError("runtime cache cohort authority is invalid")
    if runtime_profile in {"recall", "concurrency"}:
        return disk_cached_cohort_authority(disk_cache_max_bytes, effective_queries)[0]
    raise ValueError("runtime cache cohort authority is invalid")


def smoke_cache_cohort_authority(
    plan: dict[str, object], arm: dict[str, object]
) -> int:
    if arm.get("cache_state") == "cold":
        return 0
    steps = plan.get("steps")
    expected_queries = plan.get("effective_queries")
    if (
        plan.get("mode") != "smoke"
        or arm.get("cache_state") != "warm"
        or type(expected_queries) is not int
        or expected_queries <= 0
        or not isinstance(steps, list)
        or not steps
        or not isinstance(steps[-1], dict)
        or not isinstance(steps[-1].get("env"), dict)
    ):
        raise ValueError("smoke cache cohort authority is invalid")
    try:
        disk_cache_max_bytes = int(
            steps[-1]["env"]["BORSUK_BENCH_DISK_CACHE_MAX_BYTES"]
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("smoke cache cohort authority is invalid") from error
    return disk_cached_cohort_authority(disk_cache_max_bytes, expected_queries)[0]


def summarize_v21_feasibility_artifacts(
    arm_rows: list[dict[str, str]],
    sample_rows: list[dict[str, str]],
    summary: dict[str, object],
    *,
    expected_source_archive_sha256: str,
    expected_index_id: str,
    expected_dataset_id: str,
    expected_queries: int,
    expected_dataset_rows: int,
    expected_query_seed: int,
    expected_dimensions: int,
) -> dict[str, object]:
    schema = "borsuk-v21-selector-feasibility-v1"
    arm_fields = {
        "schema",
        "arm_index",
        "bundle_row_limit",
        "selector_span",
        "hedge_delay_ms",
        "bundle_count",
        "region_count",
        "projected_directory_bytes",
        "replaced_v20_root_bytes",
        "v20_root_checksum",
        "baseline_rss_bytes",
        "projected_query_transient_bytes",
        "projected_peak_rss_bytes",
        "gt_coverage",
        "recall_at_10",
        "maximum_actual_requests",
        "maximum_physical_bytes",
        "selector_within_frozen_cap",
        "eligible",
        "rows",
    }
    sample_fields = {
        "schema",
        "arm_index",
        "query_index",
        "query_source_index",
        "routed_cells",
        "selected_rows",
        "selected_bundles",
        "primary_requests",
        "maximum_actual_requests",
        "selected_bytes",
        "physical_bytes",
        "gt_hits",
        "recall_hits",
        "limiting_bound",
    }
    summary_fields = {
        "schema",
        "claim_eligible",
        "dataset_name",
        "dataset_id",
        "index_id",
        "source_archive_sha256",
        "v20_root_checksum",
        "dataset_rows",
        "dimensions",
        "query_seed",
        "query_source_indices",
        "arm_count",
        "sample_count",
        "baseline_rss_bytes",
        "minimum_arm_gt_coverage",
        "minimum_arm_recall_at_10",
        "maximum_actual_requests",
        "maximum_physical_bytes",
        "eligible_arm_indexes",
        "arms",
    }
    summary_arm_fields = {
        "arm_index",
        "bundle_row_limit",
        "selector_span",
        "hedge_delay_ms",
        "bundle_count",
        "region_count",
        "projected_directory_bytes",
        "replaced_v20_root_bytes",
        "selector_within_frozen_cap",
        "rows",
        "gt_coverage",
        "recall_at_10",
        "maximum_actual_requests",
        "maximum_physical_bytes",
        "projected_query_transient_bytes",
        "projected_peak_rss_bytes",
        "eligible",
    }

    def parse_int(value: object, role: str, *, minimum: int = 0) -> int:
        if type(value) is int:
            parsed = value
        elif isinstance(value, str) and value and value.isascii():
            try:
                parsed = int(value)
            except ValueError as error:
                raise ValueError(f"V21 {role} is invalid") from error
            if str(parsed) != value:
                raise ValueError(f"V21 {role} is not canonical")
        else:
            raise ValueError(f"V21 {role} is invalid")
        if parsed < minimum:
            raise ValueError(f"V21 {role} is out of range")
        return parsed

    def parse_float(value: object, role: str) -> float:
        if type(value) not in {str, float}:
            raise ValueError(f"V21 {role} is invalid")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"V21 {role} is invalid") from error
        if not math.isfinite(parsed):
            raise ValueError(f"V21 {role} is invalid")
        return parsed

    def parse_bool(value: object, role: str) -> bool:
        if type(value) is bool:
            return value
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError(f"V21 {role} is invalid")

    def require_digest(value: object, role: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError(f"V21 {role} is invalid")
        return value

    def require_identifier(value: object, role: str) -> str:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 128
            or any(
                not (character.isascii() and character.isalnum())
                and character not in "._-"
                for character in value
            )
        ):
            raise ValueError(f"V21 {role} is invalid")
        return value

    if (
        type(expected_queries) is not int
        or expected_queries <= 0
        or type(expected_dataset_rows) is not int
        or expected_dataset_rows <= 0
        or type(expected_query_seed) is not int
        or expected_query_seed < 0
        or type(expected_dimensions) is not int
        or expected_dimensions <= 0
        or len(arm_rows) != 12
        or len(sample_rows) != 12 * expected_queries
        or set(summary) != summary_fields
        or summary.get("schema") != schema
        or summary.get("claim_eligible") is not False
        or summary.get("source_archive_sha256") != expected_source_archive_sha256
        or summary.get("index_id") != expected_index_id
        or summary.get("dataset_id") != expected_dataset_id
        or parse_int(summary.get("arm_count"), "arm count") != 12
        or parse_int(summary.get("sample_count"), "sample count") != len(sample_rows)
    ):
        raise ValueError("V21 feasibility authority differs")
    require_digest(expected_source_archive_sha256, "source digest")
    require_identifier(expected_index_id, "index identity")
    require_identifier(expected_dataset_id, "dataset identity")
    if not isinstance(summary.get("dataset_name"), str) or not summary["dataset_name"]:
        raise ValueError("V21 dataset name is invalid")
    root_checksum = require_digest(summary.get("v20_root_checksum"), "root digest")
    dataset_rows = parse_int(summary.get("dataset_rows"), "dataset rows", minimum=1)
    dimensions = parse_int(summary.get("dimensions"), "dimensions", minimum=1)
    query_seed = parse_int(summary.get("query_seed"), "query seed")
    if (
        dataset_rows != expected_dataset_rows
        or query_seed != expected_query_seed
        or dimensions != expected_dimensions
    ):
        raise ValueError("V21 dataset or query authority differs")
    baseline_rss = parse_int(
        summary.get("baseline_rss_bytes"), "baseline RSS", minimum=1
    )
    source_indices = summary.get("query_source_indices")
    if (
        not isinstance(source_indices, list)
        or len(source_indices) != expected_queries
        or any(type(value) is not int or value < 0 for value in source_indices)
        or len(set(source_indices)) != expected_queries
    ):
        raise ValueError("V21 query source authority is invalid")
    expected_arms = [
        (bundle, span, hedge)
        for bundle in (128, 256)
        for span in (32, 64)
        for hedge in (None, 20, 35)
    ]
    summary_arms = summary.get("arms")
    if not isinstance(summary_arms, list) or len(summary_arms) != 12:
        raise ValueError("V21 summary arm matrix is invalid")
    parsed_arms: list[dict[str, object]] = []
    all_gt: list[float] = []
    all_recall: list[float] = []
    all_requests: list[int] = []
    all_physical: list[int] = []
    eligible_indexes: list[int] = []
    row_bytes = dimensions * 4 + 128
    for arm_index, (arm, summary_arm, factors) in enumerate(
        zip(arm_rows, summary_arms, expected_arms, strict=True)
    ):
        if (
            set(arm) != arm_fields
            or arm.get("schema") != schema
            or not isinstance(summary_arm, dict)
            or set(summary_arm) != summary_arm_fields
        ):
            raise ValueError("V21 arm schema differs")
        bundle, span, hedge = factors
        hedge_text = "off" if hedge is None else str(hedge)
        if (
            parse_int(arm["arm_index"], "arm index") != arm_index
            or parse_int(arm["bundle_row_limit"], "bundle row limit") != bundle
            or parse_int(arm["selector_span"], "selector span") != span
            or arm["hedge_delay_ms"] != hedge_text
            or summary_arm.get("arm_index") != arm_index
            or summary_arm.get("bundle_row_limit") != bundle
            or summary_arm.get("selector_span") != span
            or summary_arm.get("hedge_delay_ms") != hedge
        ):
            raise ValueError("V21 arm order or factors differ")
        if require_digest(arm["v20_root_checksum"], "root digest") != root_checksum:
            raise ValueError("V21 root digest differs")
        bundle_count = parse_int(arm["bundle_count"], "bundle count", minimum=1)
        region_count = parse_int(arm["region_count"], "region count", minimum=1)
        report_rows = parse_int(arm["rows"], "rows", minimum=1)
        directory = parse_int(
            arm["projected_directory_bytes"], "directory bytes", minimum=1
        )
        replaced = parse_int(
            arm["replaced_v20_root_bytes"], "replaced root bytes", minimum=1
        )
        if report_rows != dataset_rows:
            raise ValueError("V21 arm rows differ from dataset authority")
        samples = sample_rows[
            arm_index * expected_queries : (arm_index + 1) * expected_queries
        ]
        gt_hits = 0
        recall_hits = 0
        maximum_requests = 0
        maximum_physical = 0
        maximum_transient = 0
        for query_index, sample in enumerate(samples):
            if set(sample) != sample_fields or sample.get("schema") != schema:
                raise ValueError("V21 sample schema differs")
            if (
                parse_int(sample["arm_index"], "sample arm") != arm_index
                or parse_int(sample["query_index"], "query index") != query_index
                or parse_int(sample["query_source_index"], "query source")
                != source_indices[query_index]
            ):
                raise ValueError("V21 sample identity or order differs")
            rows = parse_int(sample["selected_rows"], "selected rows")
            requests = parse_int(sample["maximum_actual_requests"], "requests")
            primary_requests = parse_int(sample["primary_requests"], "primary requests")
            routed_cells = parse_int(sample["routed_cells"], "routed cells", minimum=1)
            selected_bundles = parse_int(sample["selected_bundles"], "selected bundles")
            physical = parse_int(sample["physical_bytes"], "physical bytes")
            selected = parse_int(sample["selected_bytes"], "selected bytes")
            sample_gt_hits = parse_int(sample["gt_hits"], "GT hits")
            sample_recall_hits = parse_int(sample["recall_hits"], "recall hits")
            if (
                routed_cells <= 0
                or rows > report_rows
                or selected_bundles > bundle_count
                or primary_requests > requests
                or sample_gt_hits > 10
                or sample_recall_hits > 10
                or selected > physical
                or sample["limiting_bound"]
                not in {
                    "exhausted",
                    "requests",
                    "bytes",
                    "amplification",
                    "first_bundle",
                }
            ):
                raise ValueError("V21 sample evidence is invalid")
            gt_hits += sample_gt_hits
            recall_hits += sample_recall_hits
            maximum_requests = max(maximum_requests, requests)
            maximum_physical = max(maximum_physical, physical)
            maximum_transient = max(maximum_transient, rows * row_bytes + physical)
        gt_coverage = gt_hits / (expected_queries * 10)
        recall = recall_hits / (expected_queries * 10)
        projected_peak = baseline_rss - replaced + directory + maximum_transient
        selector_within_cap = directory <= 40_000_000
        eligible = (
            selector_within_cap
            and projected_peak <= 768 * 1024 * 1024
            and gt_coverage >= 0.990
            and recall >= 0.975
            and maximum_requests <= 4
            and maximum_physical <= 1024 * 1024
        )
        expected_values: dict[str, object] = {
            "arm_index": arm_index,
            "bundle_row_limit": bundle,
            "selector_span": span,
            "hedge_delay_ms": hedge,
            "bundle_count": bundle_count,
            "region_count": region_count,
            "projected_directory_bytes": directory,
            "replaced_v20_root_bytes": replaced,
            "selector_within_frozen_cap": selector_within_cap,
            "rows": report_rows,
            "gt_coverage": gt_coverage,
            "recall_at_10": recall,
            "maximum_actual_requests": maximum_requests,
            "maximum_physical_bytes": maximum_physical,
            "projected_query_transient_bytes": maximum_transient,
            "projected_peak_rss_bytes": projected_peak,
            "eligible": eligible,
        }
        for field, value in expected_values.items():
            observed = summary_arm.get(field)
            if type(observed) is not type(value) or observed != value:
                raise ValueError(f"V21 summary arm {field} differs")
        if (
            parse_int(arm["baseline_rss_bytes"], "baseline RSS") != baseline_rss
            or parse_int(arm["projected_query_transient_bytes"], "transient bytes")
            != maximum_transient
            or parse_int(arm["projected_peak_rss_bytes"], "peak RSS") != projected_peak
            or parse_float(arm["gt_coverage"], "GT coverage") != gt_coverage
            or parse_float(arm["recall_at_10"], "recall") != recall
            or parse_int(arm["maximum_actual_requests"], "maximum requests")
            != maximum_requests
            or parse_int(arm["maximum_physical_bytes"], "maximum physical")
            != maximum_physical
            or parse_bool(arm["selector_within_frozen_cap"], "selector cap")
            != selector_within_cap
            or parse_bool(arm["eligible"], "eligibility") != eligible
        ):
            raise ValueError("V21 arm evidence differs from samples")
        parsed_arms.append(expected_values)
        all_gt.append(gt_coverage)
        all_recall.append(recall)
        all_requests.append(maximum_requests)
        all_physical.append(maximum_physical)
        if eligible:
            eligible_indexes.append(arm_index)
    if (
        summary.get("eligible_arm_indexes") != eligible_indexes
        or parse_float(summary.get("minimum_arm_gt_coverage"), "minimum GT")
        != min(all_gt)
        or parse_float(summary.get("minimum_arm_recall_at_10"), "minimum recall")
        != min(all_recall)
        or parse_int(summary.get("maximum_actual_requests"), "maximum requests")
        != max(all_requests)
        or parse_int(summary.get("maximum_physical_bytes"), "maximum physical")
        != max(all_physical)
    ):
        raise ValueError("V21 aggregate summary differs")
    return {
        "schema_version": 1,
        "document_kind": "publication-v3-v21-feasibility",
        "status": "complete",
        "publishable": False,
        "claim_eligible": False,
        "dataset_id": expected_dataset_id,
        "index_id": expected_index_id,
        "source_archive_sha256": expected_source_archive_sha256,
        "v20_root_checksum": root_checksum,
        "dataset_rows": dataset_rows,
        "dimensions": dimensions,
        "query_seed": query_seed,
        "query_source_indices": source_indices,
        "eligible_arm_indexes": eligible_indexes,
        "arms": parsed_arms,
    }


def validate_query_cache_cohort(
    row: dict[str, str],
    *,
    sample_index: int,
    expected_queries: int,
    expected_cohort_size: int,
) -> None:
    try:
        cohort_index = int(row["cache_cohort_index"])
        cohort_size = int(row["cache_cohort_size"])
        cohort_count = int(row["cache_cohort_count"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("cache cohort metadata is invalid") from error
    if expected_cohort_size == 0:
        if (cohort_index, cohort_size, cohort_count) != (0, 0, 0):
            raise ValueError("cache cohort metadata differs from its authority")
        return
    if (
        expected_queries <= 0
        or not 0 <= sample_index < expected_queries
        or cohort_size != expected_cohort_size
        or cohort_count
        != (expected_queries + expected_cohort_size - 1) // expected_cohort_size
        or cohort_index != sample_index // expected_cohort_size
    ):
        raise ValueError("cache cohort metadata differs from its authority")


def summarize_query_samples(
    rows: list[dict[str, str]],
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    expected_queries: int,
    enforce_quality: bool = True,
    expected_candidates: int = V20_COMPATIBILITY_CANDIDATES,
    expected_cache_cohort_size: int | None = None,
) -> dict[str, int]:
    if len(rows) != expected_queries:
        raise ValueError("query sample artifact is incomplete for its arm")
    if arm.get("cache_state") == "warm" and expected_cache_cohort_size is None:
        raise ValueError("disk-cached query cache cohort authority is missing")
    index_profile = cell.get("index_profile")
    expected_mode = (
        index_profile.get("global_scan_codec")
        if isinstance(index_profile, dict)
        else None
    )
    if expected_mode not in BORSUK_GLOBAL_SCAN_CODECS:
        raise ValueError("query sample has no scheduled leaf-codec authority")
    latencies_us: list[int] = []
    recalls_ppm: list[int] = []
    sample_indices: set[int] = set()
    query_source_indices: set[int] = set()
    storage_gets = 0
    storage_bytes_read = 0
    decoded_cache_bytes_read = 0
    disk_cache_bytes_read = 0
    timing_totals = {field: 0 for field in QUERY_STAGE_AGGREGATE_FIELDS}
    diagnostic_values: dict[str, list[int]] = {
        "global_leaf_exact_scores": [],
        "global_leaf_code_pages_read": [],
        "global_leaf_code_requests": [],
        "global_leaf_pages_read": [],
        "global_leaf_exact_requests": [],
    }
    for row in rows:
        if row.get("schema_version") != PRODUCTION_BENCH_SCHEMA_VERSION:
            raise ValueError("query sample schema differs")
        expected_phase = "uncached" if arm["cache_state"] == "cold" else "disk_cached"
        if (
            row.get("phase") != expected_phase
            or row.get("mode") != expected_mode
            or row.get("scan_codec") != expected_mode
            or row.get("execution_engine") != V20_EXECUTION_ENGINE
            or int(row.get("nprobe", "-1")) != arm["leaf_page_budget"]
            or int(row.get("max_candidates", "-1")) != expected_candidates
        ):
            raise ValueError("query sample belongs to a different factor arm")
        sample_index = int(row["sample_index"])
        if sample_index < 0 or sample_index in sample_indices:
            raise ValueError("query sample indices must be unique and nonnegative")
        sample_indices.add(sample_index)
        query_source_index = int(row.get("query_source_index", "-1"))
        if query_source_index < 0 or query_source_index in query_source_indices:
            raise ValueError("query source indices must be unique and nonnegative")
        query_source_indices.add(query_source_index)
        if expected_cache_cohort_size is not None:
            validate_query_cache_cohort(
                row,
                sample_index=sample_index,
                expected_queries=expected_queries,
                expected_cohort_size=expected_cache_cohort_size,
            )
        latency = float(row["latency_ms"])
        recall = float(row["recall_at_10"])
        if (
            not math.isfinite(latency)
            or latency < 0
            or not math.isfinite(recall)
            or not 0 <= recall <= 1
        ):
            raise ValueError("query sample latency or recall is invalid")
        latencies_us.append(round(latency * 1_000))
        recalls_ppm.append(round(recall * 1_000_000))
        network_gets = int(row["network_gets"])
        bytes_read = int(row["bytes_read"])
        decoded_bytes = int(row.get("decoded_cache_bytes_read", "-1"))
        disk_cache_reads = int(row["disk_cache_reads"])
        disk_bytes = int(row["disk_cache_bytes_read"])
        backing_bytes = int(row["backing_bytes_read"])
        code_bytes = int(
            row.get(
                "global_leaf_code_bytes",
                "-1" if arm["cache_state"] == "warm" else "0",
            )
        )
        if (
            network_gets < 0
            or bytes_read < 0
            or decoded_bytes < 0
            or disk_cache_reads < 0
            or disk_bytes < 0
            or backing_bytes < 0
            or code_bytes < 0
            or bytes_read != decoded_bytes + disk_bytes + backing_bytes
        ):
            raise ValueError("query sample storage telemetry is invalid")
        if arm["cache_state"] == "warm":
            if code_bytes != 0:
                raise ValueError(
                    "disk-cached query sample did not use prepared code planes"
                )
            if (
                network_gets != 0
                or backing_bytes != 0
                or disk_cache_reads <= 0
                or disk_bytes <= 0
            ):
                raise ValueError(
                    "disk-cached query sample was not served from local disk"
                )
        else:
            if network_gets <= 0 or backing_bytes <= 0:
                raise ValueError("uncached query sample performed no backing reads")
            if disk_cache_reads != 0 or disk_bytes != 0:
                raise ValueError("uncached query sample was served from local disk")
        storage_gets += network_gets
        storage_bytes_read += backing_bytes
        decoded_cache_bytes_read += decoded_bytes
        disk_cache_bytes_read += disk_bytes
        _accumulate_query_stage_timings(
            timing_totals,
            _validated_query_stage_timings(row, role="query sample"),
        )
        validate_query_planner_read_telemetry(row, role="query sample")
        for field, values in diagnostic_values.items():
            value = row.get(field)
            if value is None:
                raise ValueError("query sample planner telemetry is missing")
            parsed = int(value)
            if parsed < 0:
                raise ValueError("query sample planner telemetry is invalid")
            values.append(parsed)
    if sample_indices != set(range(expected_queries)):
        raise ValueError("query sample indices are not canonical")
    correctness_ppm = round(sum(recalls_ppm) / len(recalls_ppm))
    factors = cell.get("workload", {}).get("factors", {})
    floor = factors.get("minimum_recall_ppm")
    if not isinstance(floor, int):
        raise ValueError("query sample quality floor is invalid")
    if enforce_quality and correctness_ppm < floor:
        diagnostic = ""
        if all(len(values) == len(rows) for values in diagnostic_values.values()):
            exact_scores = diagnostic_values["global_leaf_exact_scores"]
            code_pages = diagnostic_values["global_leaf_code_pages_read"]
            exact_blocks = diagnostic_values["global_leaf_pages_read"]
            diagnostic = (
                f"; exact_scores={min(exact_scores)}..{max(exact_scores)}"
                f" code_pages={min(code_pages)}..{max(code_pages)}"
                f" exact_blocks={min(exact_blocks)}..{max(exact_blocks)}"
                f" gets={storage_gets} bytes={storage_bytes_read}"
            )
        raise ValueError(
            f"query sample recall observed {correctness_ppm} ppm is below required {floor} ppm"
            f"{diagnostic}"
        )
    return {
        "queries": len(rows),
        "correctness_ppm": correctness_ppm,
        "latency_p50_us": _nearest_rank(latencies_us, 0.50),
        "latency_p95_us": _nearest_rank(latencies_us, 0.95),
        "latency_p99_us": _nearest_rank(latencies_us, 0.99),
        "storage_gets": storage_gets,
        "storage_bytes_read": storage_bytes_read,
        "decoded_cache_bytes_read": decoded_cache_bytes_read,
        "disk_cache_bytes_read": disk_cache_bytes_read,
        "global_leaf_code_requests": sum(
            diagnostic_values["global_leaf_code_requests"]
        ),
        "global_leaf_exact_requests": sum(
            diagnostic_values["global_leaf_exact_requests"]
        ),
        **timing_totals,
        "query_elapsed_ns": sum(latencies_us) * 1_000,
    }


def summarize_read_diagnostic_samples(
    rows: list[dict[str, str]],
    *,
    summary_rows: list[dict[str, str]],
    cell: dict[str, object],
    arm: dict[str, object],
    expected_queries: int,
    nprobes: tuple[int, ...],
    candidates: tuple[int, ...],
    expected_cache_cohort_size: int | None = None,
) -> dict[str, object]:
    """Fold one bounded read-width matrix without creating release evidence."""

    if (
        not nprobes
        or not candidates
        or tuple(sorted(set(nprobes))) != nprobes
        or tuple(sorted(set(candidates))) != candidates
        or expected_queries <= 0
    ):
        raise ValueError("read diagnostic matrix authority is invalid")
    grouped: dict[tuple[int, int], list[dict[str, str]]] = {}
    for row in rows:
        try:
            key = (int(row["nprobe"]), int(row["max_candidates"]))
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("read diagnostic row identity is invalid") from error
        grouped.setdefault(key, []).append(row)
    expected = [(probe, width) for probe in nprobes for width in candidates]
    if set(grouped) != set(expected) or any(
        len(grouped[key]) != expected_queries for key in expected
    ):
        raise ValueError("read diagnostic matrix is incomplete")
    canonical_sample_indices = set(range(expected_queries))
    for key in expected:
        try:
            sample_indices = {int(row["sample_index"]) for row in grouped[key]}
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("read diagnostic sample index is invalid") from error
        if sample_indices != canonical_sample_indices:
            raise ValueError("read diagnostic sample indices are not canonical")
    source_indices: dict[int, int] = {}
    for key in expected:
        for row in grouped[key]:
            try:
                sample_index = int(row["sample_index"])
                source_index = int(row["query_source_index"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError("read diagnostic source index is invalid") from error
            if sample_index < 0 or source_index < 0:
                raise ValueError("read diagnostic source index is invalid")
            prior = source_indices.setdefault(sample_index, source_index)
            if prior != source_index:
                raise ValueError(
                    "read diagnostic source indices differ across matrix cells"
                )
    metrics = []
    for probe, width in expected:
        diagnostic_arm = {**arm, "leaf_page_budget": probe}
        metrics.append(
            {
                "nprobe": probe,
                "max_candidates": width,
                **summarize_query_samples(
                    grouped[(probe, width)],
                    cell=cell,
                    arm=diagnostic_arm,
                    expected_queries=expected_queries,
                    enforce_quality=False,
                    expected_candidates=width,
                    expected_cache_cohort_size=expected_cache_cohort_size,
                ),
            }
        )
    expected_mode = cell.get("index_profile", {}).get("global_scan_codec")
    expected_phase = "uncached" if arm["cache_state"] == "cold" else "disk_cached"
    summaries: dict[tuple[int, int], dict[str, str]] = {}
    for row in summary_rows:
        try:
            key = (int(row["nprobe"]), int(row["max_candidates"]))
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("read diagnostic summary identity is invalid") from error
        if key in summaries:
            raise ValueError("read diagnostic summary matrix contains duplicates")
        summaries[key] = row
    if set(summaries) != set(expected):
        raise ValueError("read diagnostic summary matrix is incomplete")
    metrics_by_key = {
        (int(metric["nprobe"]), int(metric["max_candidates"])): metric
        for metric in metrics
    }
    for key in expected:
        row = summaries[key]
        try:
            samples = int(row["samples"])
            recall = float(row["recall_at_10"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("read diagnostic summary values are invalid") from error
        if (
            row.get("schema_version") != PRODUCTION_BENCH_SCHEMA_VERSION
            or row.get("phase") != expected_phase
            or row.get("mode") != expected_mode
            or row.get("scan_codec") != expected_mode
            or row.get("execution_engine") != V20_EXECUTION_ENGINE
            or samples != expected_queries
            or not math.isfinite(recall)
            or not 0 <= recall <= 1
            or abs(
                round(recall * 1_000_000) - int(metrics_by_key[key]["correctness_ppm"])
            )
            > 501
        ):
            raise ValueError("read diagnostic summary differs from query samples")
    return {
        "schema_version": 1,
        "document_kind": "publication-v3-read-diagnostic",
        "publishable": False,
        "claim_eligible": False,
        "nprobes": list(nprobes),
        "candidates": list(candidates),
        "metrics": metrics,
    }


def summarize_concurrency_artifacts(
    summaries: list[dict[str, str]],
    samples: list[dict[str, str]],
    *,
    expected_workers: tuple[int, ...],
    expected_queries: int,
    minimum_recall_ppm: int,
    expected_scan_codec: str,
    expected_nprobe: int,
    expected_max_candidates: int,
    expected_cache_profile: str,
    expected_cache_coverage_percent: int,
    expected_cache_cohort_size: int | None = None,
) -> list[dict[str, int]]:
    if expected_queries <= 0 or not expected_workers:
        raise ValueError("concurrency authority is empty")
    if expected_cache_profile == "disk_cached" and expected_cache_cohort_size is None:
        raise ValueError("disk-cached concurrency cache cohort authority is missing")
    if (
        expected_scan_codec not in BORSUK_GLOBAL_SCAN_CODECS
        or expected_nprobe <= 0
        or expected_max_candidates <= 0
    ):
        raise ValueError("concurrency scan-codec authority is invalid")
    expected = set(expected_workers)
    if len(expected) != len(expected_workers) or any(
        worker <= 0 for worker in expected
    ):
        raise ValueError("concurrency worker authority is invalid")
    if expected_cache_profile == "disk_cached" and (
        type(expected_cache_cohort_size) is not int
        or expected_cache_cohort_size <= 0
        or expected_cache_cohort_size != expected_queries
    ):
        raise ValueError("disk-cached concurrency cache cohort authority is invalid")
    # Every worker profile measures the same fully primed query set as one
    # steady pipeline; worker count must not redefine cache residency.
    by_worker: dict[int, dict[str, str]] = {}
    for row in summaries:
        worker = int(row.get("workers", "-1"))
        if (
            row.get("schema_version") != PRODUCTION_BENCH_SCHEMA_VERSION
            or row.get("scan_codec") != expected_scan_codec
            or row.get("execution_engine") != V20_EXECUTION_ENGINE
            or int(row.get("nprobe", "-1")) != expected_nprobe
            or int(row.get("max_candidates", "-1")) != expected_max_candidates
            or row.get("cache_profile") != expected_cache_profile
            or int(row.get("target_cache_coverage_percent", "-1"))
            != expected_cache_coverage_percent
            or worker not in expected
            or worker in by_worker
            or int(row.get("total_queries", "-1")) != expected_queries
        ):
            raise ValueError("concurrency summary differs from its authority")
        for field in ("qps", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"):
            value = float(row.get(field, "nan"))
            if not math.isfinite(value) or value < 0 or (field == "qps" and value == 0):
                raise ValueError("concurrency summary metric is invalid")
        by_worker[worker] = row
    if set(by_worker) != expected:
        raise ValueError("concurrency summary is incomplete")

    sample_indices = {worker: set() for worker in expected_workers}
    query_source_by_sample: dict[int, int] = {}
    recalls = {worker: [] for worker in expected_workers}
    storage_totals = {
        worker: {
            "storage_gets": 0,
            "storage_bytes_read": 0,
            "decoded_cache_bytes_read": 0,
            "disk_cache_bytes_read": 0,
        }
        for worker in expected_workers
    }
    timing_totals = {
        worker: {field: 0 for field in QUERY_STAGE_AGGREGATE_FIELDS}
        for worker in expected_workers
    }
    for row in samples:
        worker = int(row.get("workers", "-1"))
        if (
            row.get("schema_version") != PRODUCTION_BENCH_SCHEMA_VERSION
            or row.get("scan_codec") != expected_scan_codec
            or row.get("execution_engine") != V20_EXECUTION_ENGINE
            or int(row.get("nprobe", "-1")) != expected_nprobe
            or int(row.get("max_candidates", "-1")) != expected_max_candidates
            or row.get("cache_profile") != expected_cache_profile
            or int(row.get("target_cache_coverage_percent", "-1"))
            != expected_cache_coverage_percent
            or worker not in expected
        ):
            raise ValueError("concurrency sample differs from its authority")
        sample_index = int(row.get("sample_index", "-1"))
        if not 0 <= sample_index < expected_queries:
            raise ValueError("concurrency sample indices are not canonical")
        if sample_index in sample_indices[worker]:
            raise ValueError("concurrency sample index is duplicated")
        query_source_index = int(row.get("query_source_index", "-1"))
        if query_source_index < 0:
            raise ValueError("concurrency query source mapping is invalid")
        prior_source = query_source_by_sample.setdefault(
            sample_index, query_source_index
        )
        if prior_source != query_source_index:
            raise ValueError("concurrency query source mapping differs across workers")
        if expected_cache_cohort_size is not None:
            validate_query_cache_cohort(
                row,
                sample_index=sample_index,
                expected_queries=expected_queries,
                expected_cohort_size=expected_cache_cohort_size,
            )
        latency_ms = float(row.get("latency_ms", "nan"))
        recall = float(row.get("recall_at_10", "nan"))
        if (
            not math.isfinite(latency_ms)
            or latency_ms < 0
            or not math.isfinite(recall)
            or not 0 <= recall <= 1
        ):
            raise ValueError("concurrency sample latency or recall is invalid")
        network_gets = int(row.get("network_gets", "-1"))
        disk_cache_reads = int(row.get("disk_cache_reads", "-1"))
        bytes_read = int(row.get("bytes_read", "-1"))
        decoded_bytes = int(row.get("decoded_cache_bytes_read", "-1"))
        disk_bytes = int(row.get("disk_cache_bytes_read", "-1"))
        backing_bytes = int(row.get("backing_bytes_read", "-1"))
        if (
            network_gets < 0
            or disk_cache_reads < 0
            or bytes_read < 0
            or decoded_bytes < 0
            or disk_bytes < 0
            or backing_bytes < 0
            or bytes_read != decoded_bytes + disk_bytes + backing_bytes
        ):
            raise ValueError("concurrency sample cache counters are invalid")
        if expected_cache_profile == "disk_cached" and (
            network_gets != 0
            or backing_bytes != 0
            or disk_cache_reads == 0
            or disk_bytes == 0
        ):
            raise ValueError(
                "disk-cached concurrency sample was not served from local disk"
            )
        sample_indices[worker].add(sample_index)
        recalls[worker].append(round(recall * 1_000_000))
        storage_totals[worker]["storage_gets"] += network_gets
        storage_totals[worker]["storage_bytes_read"] += backing_bytes
        storage_totals[worker]["decoded_cache_bytes_read"] += decoded_bytes
        storage_totals[worker]["disk_cache_bytes_read"] += disk_bytes
        _accumulate_query_stage_timings(
            timing_totals[worker],
            _validated_query_stage_timings(row, role="concurrency sample"),
        )

    result = []
    if len(set(query_source_by_sample.values())) != expected_queries:
        raise ValueError("concurrency query source mapping is not one-to-one")
    if expected_cache_profile == "uncached" and any(
        row["storage_gets"] == 0 or row["storage_bytes_read"] == 0
        for row in storage_totals.values()
    ):
        raise ValueError("uncached concurrency wave performed no backing reads")
    for worker in expected_workers:
        if len(sample_indices[worker]) != expected_queries:
            raise ValueError("concurrency samples are incomplete")
        recall_ppm = round(sum(recalls[worker]) / expected_queries)
        if recall_ppm < minimum_recall_ppm:
            raise ValueError("concurrency recall is below its frozen floor")
        row = by_worker[worker]
        result.append(
            {
                "workers": worker,
                "queries": expected_queries,
                "qps_milli": round(float(row["qps"]) * 1_000),
                "p50_us": round(float(row["p50_ms"]) * 1_000),
                "p95_us": round(float(row["p95_ms"]) * 1_000),
                "p99_us": round(float(row["p99_ms"]) * 1_000),
                "recall_ppm": recall_ppm,
                **storage_totals[worker],
                **timing_totals[worker],
            }
        )
    return result


def summarize_runtime_write_trace(path: Path) -> dict[str, int]:
    if not path.is_file():
        raise ValueError("publication runtime storage trace is missing")
    if (path.parent / "bench_build.csv").exists():
        raise ValueError("publication runtime unexpectedly emitted a build artifact")
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    storage_puts = 0
    storage_gets = 0
    storage_bytes_read = 0
    storage_bytes_written = 0
    data_paths: set[str] = set()
    storage_max_data_object_bytes = 0
    control_roles = {
        "catalog",
        "lane_head",
        "writer_directory",
        "commit_marker",
        "id_directory_control",
        "positioned_head",
    }
    for row in rows:
        operation = row.get("operation")
        if operation not in {"read", "write"}:
            continue
        try:
            requests = int(row["request_count"])
            byte_count = int(row["object_bytes"])
            bytes_fetched = int(row["bytes_fetched"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication runtime storage trace is invalid") from error
        status = row.get("status")
        if requests < 0 or byte_count < 0 or bytes_fetched < 0:
            raise ValueError("publication runtime storage trace is invalid")
        if operation == "read":
            if status != "ok":
                raise ValueError("publication runtime storage trace is invalid")
            storage_gets += requests
            storage_bytes_read += bytes_fetched
            continue
        # The trace is an attempt ledger. A higher layer may recover a failed
        # immutable write, so all terminal attempts remain billable even when
        # only successful paths contribute to the live object topology.
        if status not in {"ok", "conflict", "error"} or requests <= 0:
            raise ValueError("publication runtime storage trace is invalid")
        storage_puts += requests
        storage_bytes_written += byte_count
        if status == "ok" and row.get("object_role") not in control_roles:
            path_value = row.get("path")
            if not isinstance(path_value, str) or not path_value:
                raise ValueError("publication runtime storage trace is invalid")
            data_paths.add(path_value)
            storage_max_data_object_bytes = max(
                storage_max_data_object_bytes, byte_count
            )
    return {
        "storage_gets": storage_gets,
        "storage_puts": storage_puts,
        "storage_bytes_read": storage_bytes_read,
        "storage_bytes_written": storage_bytes_written,
        "storage_distinct_data_objects": len(data_paths),
        "storage_max_data_object_bytes": storage_max_data_object_bytes,
    }


def reconcile_lifecycle_storage_trace(
    lifecycle: dict[str, int], trace: dict[str, int]
) -> dict[str, int]:
    lifecycle_bytes = lifecycle.get("storage_bytes_written")
    trace_bytes = trace.get("storage_bytes_written")
    lifecycle_puts = lifecycle.get("storage_puts")
    trace_puts = trace.get("storage_puts")
    if lifecycle_bytes != trace_bytes or lifecycle_puts != trace_puts:
        raise ValueError(
            "publication lifecycle storage accounting differs from the complete trace: "
            f"lifecycle_puts={lifecycle_puts} trace_puts={trace_puts} "
            f"lifecycle_bytes={lifecycle_bytes} trace_bytes={trace_bytes}"
        )
    return {
        # Reads also happen in verification and query stages, outside the
        # mutation rows. The complete storage trace is their authority.
        "storage_gets": trace["storage_gets"],
        "storage_bytes_read": trace["storage_bytes_read"],
        "storage_distinct_data_objects": trace["storage_distinct_data_objects"],
        "storage_max_data_object_bytes": trace["storage_max_data_object_bytes"],
    }


def reconcile_read_storage_trace(
    measured: dict[str, int], trace: dict[str, int]
) -> dict[str, int]:
    def counter(values: dict[str, int], field: str) -> int:
        value = values.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("read storage accounting is invalid")
        return value

    measured_gets = counter(measured, "storage_gets")
    measured_bytes = counter(measured, "storage_bytes_read")
    counter(measured, "disk_cache_bytes_read")
    counter(measured, "decoded_cache_bytes_read")
    trace_gets = counter(trace, "storage_gets")
    trace_bytes = counter(trace, "storage_bytes_read")
    for field in (
        "storage_puts",
        "storage_bytes_written",
        "storage_distinct_data_objects",
        "storage_max_data_object_bytes",
    ):
        counter(trace, field)
    if trace_gets < measured_gets or trace_bytes < measured_bytes:
        raise ValueError(
            "complete read storage trace is smaller than measured query I/O"
        )
    return {
        "excluded_setup_storage_gets": trace_gets - measured_gets,
        "excluded_setup_storage_bytes_read": trace_bytes - measured_bytes,
    }


def reconcile_concurrency_storage(
    metrics: list[dict[str, int]], trace: dict[str, int]
) -> dict[str, int]:
    if not metrics:
        raise ValueError("concurrency storage accounting is empty")
    measured = {
        "storage_gets": 0,
        "storage_bytes_read": 0,
        "decoded_cache_bytes_read": 0,
        "disk_cache_bytes_read": 0,
    }
    for row in metrics:
        for field in measured:
            value = row.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("concurrency storage accounting is invalid")
            measured[field] += value
    setup = reconcile_read_storage_trace(measured, trace)
    return {
        "storage_gets": measured["storage_gets"],
        "storage_puts": trace["storage_puts"],
        "storage_bytes_read": measured["storage_bytes_read"],
        "storage_bytes_written": trace["storage_bytes_written"],
        "decoded_cache_bytes_read": measured["decoded_cache_bytes_read"],
        "disk_cache_bytes_read": measured["disk_cache_bytes_read"],
        **setup,
    }


def _read_exact_csv(path: Path, fields: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.is_file():
        raise ValueError(f"publication artifact {path.name} is missing")
    with path.open(newline="") as source:
        reader = csv.DictReader(source)
        if tuple(reader.fieldnames or ()) != fields:
            raise ValueError(f"publication artifact {path.name} header differs")
        rows = list(reader)
    if not rows:
        raise ValueError(f"publication artifact {path.name} is empty")
    return rows


def _finite_nonnegative_float(value: object, role: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{role} is invalid") from error
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{role} is invalid")
    return result


def lifecycle_batch_records(
    operations: int, configured_batch_size: int, writers: int
) -> list[int]:
    if (
        isinstance(operations, bool)
        or operations <= 0
        or isinstance(configured_batch_size, bool)
        or configured_batch_size <= 0
        or isinstance(writers, bool)
        or not 1 <= writers <= 64
    ):
        raise ValueError("publication lifecycle batch schedule is invalid")
    natural_batches = (operations + configured_batch_size - 1) // configured_batch_size
    participating_writers = min(writers, operations)
    if natural_batches >= participating_writers:
        return [
            min(configured_batch_size, operations - index * configured_batch_size)
            for index in range(natural_batches)
        ]
    base, larger_batches = divmod(operations, participating_writers)
    return [
        base + int(index < larger_batches) for index in range(participating_writers)
    ]


def summarize_lifecycle_artifacts(
    output_dir: Path, *, expected_batch_size: int, expected_writers: int
) -> dict[str, int]:
    if isinstance(expected_batch_size, bool) or expected_batch_size <= 0:
        raise ValueError("publication lifecycle batch size is invalid")
    if isinstance(expected_writers, bool) or not 1 <= expected_writers <= 64:
        raise ValueError("publication lifecycle writer count is invalid")
    costs = _read_exact_csv(output_dir / "bench_write_costs.csv", WRITE_COST_FIELDS)
    samples = _read_exact_csv(
        output_dir / "bench_write_samples.csv", WRITE_SAMPLE_FIELDS
    )
    lifecycle_rows = _read_exact_csv(
        output_dir / "bench_lifecycle.csv", LIFECYCLE_FIELDS
    )
    if len(costs) != len(LIFECYCLE_OPERATIONS) or len(lifecycle_rows) != 1:
        raise ValueError(
            "publication lifecycle artifacts have incomplete operation rows"
        )
    by_operation: dict[str, dict[str, str]] = {}
    for row in costs:
        operation = row.get("op", "")
        if operation not in LIFECYCLE_OPERATIONS or operation in by_operation:
            raise ValueError("publication lifecycle operations differ")
        by_operation[operation] = row
        try:
            configured_writers = int(row["configured_writers"])
            configured_batch = int(row["configured_batch_records"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "publication lifecycle configuration is invalid"
            ) from error
        if (
            configured_writers != expected_writers
            or configured_batch != expected_batch_size
        ):
            raise ValueError(
                "publication lifecycle artifact belongs to another batch arm"
            )
    if tuple(sorted(by_operation)) != tuple(sorted(LIFECYCLE_OPERATIONS)):
        raise ValueError("publication lifecycle operations differ")

    latencies_us: list[int] = []
    sample_indices: dict[str, set[int]] = {
        operation: set() for operation in LIFECYCLE_OPERATIONS
    }
    sample_batch_records: dict[str, dict[int, int]] = {
        operation: {} for operation in LIFECYCLE_OPERATIONS
    }
    sample_records = {operation: 0 for operation in LIFECYCLE_OPERATIONS}
    sample_request_totals = {
        operation: {field: 0 for field in ("gets", "puts", "deletes", "heads", "lists")}
        for operation in LIFECYCLE_OPERATIONS
    }
    for row in samples:
        operation = row.get("op", "")
        if operation not in sample_indices:
            raise ValueError("publication lifecycle sample operation differs")
        try:
            writer_index = int(row["writer_index"])
            wave_index = int(row["wave_index"])
            index = int(row["batch_index"])
            batch_records = int(row["batch_records"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication lifecycle sample is invalid") from error
        if (
            not 0 <= writer_index < expected_writers
            or wave_index < 0
            or index < 0
            or index in sample_indices[operation]
            or batch_records < 0
        ):
            raise ValueError("publication lifecycle sample identity is invalid")
        if operation in {"insert", "upsert", "delete"} and (
            writer_index != index % expected_writers
            or wave_index != index // expected_writers
        ):
            raise ValueError("publication lifecycle writer wave is not canonical")
        if operation in {"flush", "consolidate", "compact", "purge"} and (
            writer_index != 0 or wave_index != 0 or index != 0
        ):
            raise ValueError("publication lifecycle maintenance writer wave is invalid")
        sample_indices[operation].add(index)
        sample_batch_records[operation][index] = batch_records
        sample_records[operation] += batch_records
        for field in sample_request_totals[operation]:
            try:
                observed = int(row[field])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    "publication lifecycle sample request is invalid"
                ) from error
            if observed < 0:
                raise ValueError("publication lifecycle sample request is invalid")
            sample_request_totals[operation][field] += observed
        latency_ms = _finite_nonnegative_float(
            row.get("batch_latency_ms"), "publication lifecycle sample latency"
        )
        if operation in {"insert", "upsert", "delete"}:
            latencies_us.append(round(latency_ms * 1_000))
    for operation, row in by_operation.items():
        try:
            expected_batches = int(row["batches"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication lifecycle operation is invalid") from error
        if expected_batches <= 0 or sample_indices[operation] != set(
            range(expected_batches)
        ):
            raise ValueError("publication lifecycle samples are incomplete")

    lifecycle = lifecycle_rows[0]
    try:
        lifecycle_writers = int(lifecycle["configured_writers"])
        lifecycle_batch = int(lifecycle["configured_batch_records"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("publication lifecycle configuration is invalid") from error
    if lifecycle_writers != expected_writers or lifecycle_batch != expected_batch_size:
        raise ValueError("publication lifecycle artifact belongs to another batch arm")
    fractions = []
    for sample_field, fraction_field in (
        ("searchable_samples", "searchable_fraction"),
        ("upsert_samples", "upsert_correct_fraction"),
        ("delete_samples", "delete_absent_fraction"),
        ("delete_samples", "compact_delete_absent_fraction"),
        ("delete_samples", "purge_delete_absent_fraction"),
    ):
        try:
            sample_count = int(lifecycle[sample_field])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "publication lifecycle verification evidence is invalid"
            ) from error
        fraction = _finite_nonnegative_float(
            lifecycle.get(fraction_field), "publication lifecycle verification fraction"
        )
        if sample_count <= 0 or fraction > 1:
            raise ValueError("publication lifecycle verification evidence is invalid")
        fractions.append(fraction)

    operation_counts: dict[str, int] = {}
    wall_ms = 0.0
    storage_totals = {
        "storage_gets": 0,
        "storage_puts": 0,
        "storage_bytes_read": 0,
        "storage_bytes_written": 0,
    }
    for operation, row in by_operation.items():
        try:
            operations = int(row["ops"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "publication lifecycle operation count is invalid"
            ) from error
        if operations <= 0:
            raise ValueError("publication lifecycle operation count is invalid")
        operation_counts[operation] = operations
        if operation in {"insert", "upsert", "delete"}:
            expected_schedule = lifecycle_batch_records(
                operations, expected_batch_size, expected_writers
            )
            try:
                declared_batches = int(row["batches"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    "publication lifecycle batch schedule is invalid"
                ) from error
            expected_records = dict(enumerate(expected_schedule))
            if (
                declared_batches != len(expected_schedule)
                or sample_batch_records[operation] != expected_records
            ):
                raise ValueError("publication lifecycle batch schedule differs")
        try:
            declared_requests = {
                field: int(row[field]) for field in sample_request_totals[operation]
            }
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "publication lifecycle operation request is invalid"
            ) from error
        if (
            any(value < 0 for value in declared_requests.values())
            or declared_requests != sample_request_totals[operation]
        ):
            raise ValueError("publication lifecycle sample request totals differ")
        wall_ms += _finite_nonnegative_float(
            row.get("wall_ms"), "publication lifecycle operation wall time"
        )
        for source, destination in (
            ("gets", "storage_gets"),
            ("puts", "storage_puts"),
            ("bytes_read", "storage_bytes_read"),
            ("bytes_written", "storage_bytes_written"),
        ):
            try:
                observed = int(row[source])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    "publication lifecycle storage telemetry is invalid"
                ) from error
            if observed < 0:
                raise ValueError("publication lifecycle storage telemetry is invalid")
            storage_totals[destination] += observed
    for operation in ("insert", "upsert", "delete"):
        if sample_records[operation] != operation_counts[operation]:
            raise ValueError("publication lifecycle sample operation totals differ")
    if wall_ms <= 0:
        raise ValueError("publication lifecycle wall time is invalid")

    try:
        inserted_vectors = int(lifecycle["inserted_vectors"])
        logical_vector_bytes = int(lifecycle["logical_vector_bytes"])
        wal_publish_bytes = int(lifecycle["wal_publish_bytes"])
        indexed_delta_bytes = int(lifecycle["indexed_delta_bytes"])
        total_indexing_bytes = int(lifecycle["total_indexing_bytes"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            "publication lifecycle operation totals are invalid"
        ) from error
    if (
        inserted_vectors != operation_counts["insert"]
        or logical_vector_bytes <= 0
        or wal_publish_bytes < 0
        or indexed_delta_bytes < 0
        or total_indexing_bytes <= 0
        or total_indexing_bytes != wal_publish_bytes + indexed_delta_bytes
    ):
        raise ValueError("publication lifecycle operation totals differ")

    first_publish_us = round(
        _finite_nonnegative_float(
            lifecycle.get("first_batch_publish_ms"), "first publish"
        )
        * 1_000
    )
    searchable_us = round(
        _finite_nonnegative_float(
            lifecycle.get("time_to_searchable_ms"), "searchable time"
        )
        * 1_000
    )
    fully_indexed_us = round(
        _finite_nonnegative_float(
            lifecycle.get("time_to_fully_indexed_ms"), "indexed time"
        )
        * 1_000
    )
    consolidated_us = round(
        _finite_nonnegative_float(
            lifecycle.get("time_to_consolidated_ms"), "consolidated time"
        )
        * 1_000
    )
    if not 0 < first_publish_us <= searchable_us <= fully_indexed_us <= consolidated_us:
        raise ValueError("publication lifecycle milestone times are not ordered")
    write_amplification = _finite_nonnegative_float(
        lifecycle.get("write_amplification"), "write amplification"
    )
    expected_amplification = total_indexing_bytes / logical_vector_bytes
    if (
        write_amplification <= 0
        or abs(write_amplification - expected_amplification) > 0.000_001
        or lifecycle.get("write_amplification_is_lower_bound") != "true"
    ):
        raise ValueError("publication lifecycle write amplification is invalid")
    insert_wall_ms = _finite_nonnegative_float(
        lifecycle.get("insert_wall_ms"), "insert wall time"
    )
    searchability_refresh_ms = _finite_nonnegative_float(
        lifecycle.get("searchability_refresh_ms"), "searchability refresh time"
    )
    delta_flush_ms = _finite_nonnegative_float(
        lifecycle.get("delta_flush_ms"), "delta flush time"
    )
    consolidation_ms = _finite_nonnegative_float(
        lifecycle.get("consolidation_ms"), "consolidation time"
    )
    cost_insert_wall_ms = _finite_nonnegative_float(
        by_operation["insert"].get("wall_ms"), "insert operation wall time"
    )
    if (
        abs(insert_wall_ms - cost_insert_wall_ms) > 0.001
        or abs(searchable_us / 1_000 - (insert_wall_ms + searchability_refresh_ms))
        > 0.002
        or abs(fully_indexed_us / 1_000 - (searchable_us / 1_000 + delta_flush_ms))
        > 0.002
        or abs(consolidated_us / 1_000 - (fully_indexed_us / 1_000 + consolidation_ms))
        > 0.002
    ):
        raise ValueError("publication lifecycle milestone totals differ")
    mutation_operations = sum(
        operation_counts[operation] for operation in ("insert", "upsert", "delete")
    )
    mutation_wall_ms = sum(
        _finite_nonnegative_float(
            by_operation[operation].get("wall_ms"),
            "publication lifecycle mutation wall time",
        )
        for operation in ("insert", "upsert", "delete")
    )
    return {
        "insert_ops": operation_counts["insert"],
        "flush_ops": operation_counts["flush"],
        "consolidate_ops": operation_counts["consolidate"],
        "upsert_ops": operation_counts["upsert"],
        "delete_ops": operation_counts["delete"],
        "compact_ops": operation_counts["compact"],
        "purge_ops": operation_counts["purge"],
        "lifecycle_accuracy_ppm": round(min(fractions) * 1_000_000),
        "batch_latency_p50_us": _nearest_rank(latencies_us, 0.50),
        "batch_latency_p95_us": _nearest_rank(latencies_us, 0.95),
        "batch_latency_p99_us": _nearest_rank(latencies_us, 0.99),
        "throughput_milli_per_second": max(
            1, round(mutation_operations * 1_000_000 / mutation_wall_ms)
        ),
        "first_publish_us": first_publish_us,
        "time_to_searchable_us": searchable_us,
        "time_to_fully_indexed_us": fully_indexed_us,
        "time_to_consolidated_us": consolidated_us,
        "write_amplification_ppm": round(write_amplification * 1_000_000),
        **storage_totals,
    }


def _read_build_phase_artifact(output_dir: Path) -> dict[str, object]:
    path = output_dir / "bench_build_phases.csv"
    if not path.is_file():
        raise ValueError("publication build phase timing artifact is missing")
    payload = path.read_bytes()
    if not payload or len(payload) > 64 * 1024:
        raise ValueError("publication build phase timing artifact exceeds its bound")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(
            "publication build phase timing artifact is not UTF-8"
        ) from error
    reader = csv.DictReader(text.splitlines())
    if tuple(reader.fieldnames or ()) != BUILD_PHASE_FIELDS:
        raise ValueError("publication build phase timing header differs")
    rows = list(reader)
    expected = {
        (group, phase)
        for group in ("ingest", "compaction")
        for phase in BUILD_PHASE_NAMES
    }
    observed: dict[tuple[str, str], dict[str, int | str]] = {}
    for row in rows:
        if row.get("schema_version") != "2":
            raise ValueError("publication build phase timing schema differs")
        key = (str(row.get("group", "")), str(row.get("phase", "")))
        if key in observed:
            raise ValueError("publication build phase timing rows are duplicated")
        try:
            nanos = int(row["nanos"])
            calls = int(row["calls"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "publication build phase timing value is invalid"
            ) from error
        if nanos < 0 or calls < 0:
            raise ValueError("publication build phase timing value is negative")
        observed[key] = {
            "group": key[0],
            "phase": key[1],
            "nanos": nanos,
            "calls": calls,
        }
    if set(observed) != expected:
        raise ValueError("publication build phase timing coverage differs")
    return {
        "path": path.name,
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "rows": len(rows),
        "timings": [observed[key] for key in sorted(observed)],
    }


def read_build_artifact(
    output_dir: Path, *, cell: dict[str, object]
) -> dict[str, object]:
    path = output_dir / "bench_build.csv"
    if not path.is_file():
        raise ValueError("publication build storage artifact is missing")
    with path.open(newline="") as source:
        reader = csv.DictReader(source)
        if tuple(reader.fieldnames or ()) != PRODUCTION_BUILD_FIELDS:
            raise ValueError(
                "publication build artifact header differs from production"
            )
        rows = list(reader)
    if len(rows) != 1:
        raise ValueError("publication build storage artifact must contain one row")
    row = rows[0]
    integer_fields = (
        "logical_cells",
        "records",
        "total_active_index_bytes",
        "compaction_bytes_read",
        "compaction_bytes_written",
        "gc_objects_scanned",
        "gc_objects_deleted",
        "gc_transaction_states_remaining",
        "gc_bytes_read",
        "gc_bytes_reclaimed",
        "storage_gets",
        "storage_puts",
        "storage_deletes",
        "storage_heads",
        "storage_lists",
        "storage_bytes_read",
        "storage_bytes_written",
        "configured_build_writers",
        "ingest_batches",
        "ingest_waves",
    )
    parsed: dict[str, int] = {}
    for field in integer_fields:
        try:
            value = int(row[field])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"publication build storage field {field} is invalid"
            ) from error
        if value < 0:
            raise ValueError(f"publication build storage field {field} is negative")
        parsed[field] = value
    dataset = cell.get("dataset")
    profile = cell.get("index_profile")
    expected_rows = (
        dataset.get("scale", {}).get("rows") if isinstance(dataset, dict) else None
    )
    expected_cells = profile.get("logical_cells") if isinstance(profile, dict) else None
    if parsed["records"] != expected_rows or parsed["logical_cells"] != expected_cells:
        raise ValueError("publication build identity differs from its scheduled index")
    expected_codec = (
        profile.get("global_scan_codec") if isinstance(profile, dict) else None
    )
    if row.get("scan_codec") != expected_codec:
        raise ValueError(
            "publication build codec identity differs from its scheduled index"
        )
    if expected_codec in BORSUK_TURBOQUANT_GLOBAL_SCAN_CODECS:
        for artifact_field, profile_field in (
            ("turboquant_bits", "turboquant_bits"),
            ("turboquant_qjl_bits", "turboquant_qjl_bits"),
            ("turboquant_shards", "turboquant_shards"),
        ):
            if row.get(artifact_field) != str(profile.get(profile_field)):
                raise ValueError(
                    "publication build codec identity differs from its scheduled index"
                )
    checksum = row.get("logical_cell_catalog_checksum", "")
    if len(checksum) != 64 or any(
        character not in "0123456789abcdef" for character in checksum
    ):
        raise ValueError("publication build catalog checksum is invalid")
    if parsed["total_active_index_bytes"] <= 0:
        raise ValueError("publication build active index bytes must be positive")
    if (
        parsed["configured_build_writers"] != 8
        or parsed["ingest_batches"] <= 0
        or parsed["ingest_waves"]
        != math.ceil(parsed["ingest_batches"] / parsed["configured_build_writers"])
    ):
        raise ValueError("publication build ingest schedule differs")
    if parsed["gc_transaction_states_remaining"] != 0:
        raise ValueError(
            "publication build transaction states remain after finalization"
        )
    build_timings: dict[str, int] = {}
    for field in ("ingest_ms", "compaction_ms", "gc_ms"):
        try:
            nanos = Decimal(row[field]) * 1_000_000
        except (KeyError, InvalidOperation) as error:
            raise ValueError(
                f"publication build timing field {field} is invalid"
            ) from error
        if nanos < 0 or nanos != nanos.to_integral_value():
            raise ValueError(f"publication build timing field {field} is invalid")
        build_timings[field.removesuffix("_ms") + "_ns"] = int(nanos)
    try:
        ingest_vectors_per_s = Decimal(row["ingest_vectors_per_s"])
    except (KeyError, InvalidOperation) as error:
        raise ValueError("publication build ingest throughput is invalid") from error
    if not ingest_vectors_per_s.is_finite() or ingest_vectors_per_s <= 0:
        raise ValueError("publication build ingest throughput is invalid")
    build_timings.update(
        {
            "configured_build_writers": parsed["configured_build_writers"],
            "ingest_batches": parsed["ingest_batches"],
            "ingest_waves": parsed["ingest_waves"],
            "ingest_vectors_per_s_micros": int(ingest_vectors_per_s * 1_000_000),
            "compaction_bytes_read": parsed["compaction_bytes_read"],
            "compaction_bytes_written": parsed["compaction_bytes_written"],
            "gc_objects_scanned": parsed["gc_objects_scanned"],
            "gc_objects_deleted": parsed["gc_objects_deleted"],
            "gc_transaction_states_remaining": parsed[
                "gc_transaction_states_remaining"
            ],
            "gc_bytes_read": parsed["gc_bytes_read"],
            "gc_bytes_reclaimed": parsed["gc_bytes_reclaimed"],
        }
    )
    return {
        "index_stats": {
            "logical_cells": parsed["logical_cells"],
            "records": parsed["records"],
            "total_active_index_bytes": parsed["total_active_index_bytes"],
            "logical_cell_catalog_checksum": checksum,
        },
        "storage_metrics": {
            field: parsed[field]
            for field in integer_fields
            if field.startswith("storage_")
        },
        "build_timings": build_timings,
        "phase_timings": _read_build_phase_artifact(output_dir),
    }


def build_receipt_metrics(
    process_resources: dict[str, int],
    storage_metrics: dict[str, int],
    *,
    elapsed_ns: int,
) -> dict[str, int]:
    process_fields = frozenset(
        {"cpu_ns", "peak_rss_bytes", "disk_read_bytes", "disk_write_bytes"}
    )
    required_storage = frozenset(
        {
            "storage_gets",
            "storage_puts",
            "storage_deletes",
            "storage_heads",
            "storage_lists",
            "storage_bytes_read",
            "storage_bytes_written",
        }
    )
    if frozenset(process_resources) != process_fields or not required_storage.issubset(
        storage_metrics
    ):
        raise ValueError("publication resource inputs differ")
    if (
        isinstance(elapsed_ns, bool)
        or not isinstance(elapsed_ns, int)
        or elapsed_ns <= 0
    ):
        raise ValueError("publication build elapsed time is invalid")
    return {
        **process_resources,
        **{field: storage_metrics[field] for field in sorted(required_storage)},
        "build_elapsed_ns": elapsed_ns,
    }


def build_smoke_report(
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    effective_rows: int,
    effective_queries: int,
    metrics: dict[str, int],
    protocol_sha256: str,
) -> dict[str, object]:
    if len(protocol_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in protocol_sha256
    ):
        raise ValueError("smoke protocol checksum is invalid")
    return {
        "schema_version": 1,
        "document_kind": "publication-v3-smoke",
        "publishable": False,
        "cell_id": cell["cell_id"],
        "protocol_sha256": protocol_sha256,
        "arm": arm,
        "effective_rows": effective_rows,
        "effective_queries": effective_queries,
        "metrics": metrics,
    }


def build_publication_report(
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    protocol_bytes: bytes,
    source_archive_sha256: str,
    dataset_materialization_sha256: str,
    attempt_id: str,
    instance_identity: str,
    elapsed_ns: int,
    query_metrics: dict[str, int],
    resource_metrics: dict[str, int],
    runtime_storage_trace: dict[str, int],
    index_receipt: dict[str, object],
    runtime_attestation: dict[str, object],
    runtime_profile: str = "recall",
) -> dict[str, object]:
    if runtime_profile not in {"recall", "concurrency"}:
        raise ValueError("publication runtime profile is invalid")
    if (
        isinstance(elapsed_ns, bool)
        or not isinstance(elapsed_ns, int)
        or elapsed_ns <= 0
    ):
        raise ValueError("publication elapsed time must be a positive integer")
    queries = query_metrics.get("queries")
    if isinstance(queries, bool) or not isinstance(queries, int) or queries <= 0:
        raise ValueError("publication query count must be a positive integer")
    expected_query_fields = frozenset(
        {
            "queries",
            "correctness_ppm",
            "latency_p50_us",
            "latency_p95_us",
            "latency_p99_us",
            "storage_gets",
            "storage_bytes_read",
            "decoded_cache_bytes_read",
            "disk_cache_bytes_read",
            "global_leaf_code_requests",
            "global_leaf_exact_requests",
            "query_elapsed_ns",
        }
    ) | frozenset(QUERY_STAGE_AGGREGATE_FIELDS)
    expected_resource_fields = frozenset(
        {
            "cpu_ns",
            "peak_rss_bytes",
            "disk_read_bytes",
            "disk_write_bytes",
        }
    )
    if frozenset(query_metrics) != expected_query_fields:
        raise ValueError("publication query metric fields differ")
    if frozenset(resource_metrics) != expected_resource_fields:
        raise ValueError("publication resource metric fields differ")
    expected_storage_trace_fields = frozenset(
        {
            "storage_gets",
            "storage_puts",
            "storage_bytes_read",
            "storage_bytes_written",
            "storage_distinct_data_objects",
            "storage_max_data_object_bytes",
        }
    )
    if frozenset(runtime_storage_trace) != expected_storage_trace_fields:
        raise ValueError("publication runtime storage trace fields differ")
    setup_storage = reconcile_read_storage_trace(query_metrics, runtime_storage_trace)
    result = {
        "schema_version": 4,
        "status": "complete",
        "cell_id": cell.get("cell_id"),
        "manifest_sha256": cell.get("manifest_sha256"),
        "protocol_sha256": hashlib.sha256(protocol_bytes).hexdigest(),
        "source_archive_sha256": source_archive_sha256,
        "attempt_id": attempt_id,
        "instance_identity": instance_identity,
        "arm": arm,
        "metrics": {
            **{
                key: value
                for key, value in query_metrics.items()
                if key != "query_elapsed_ns"
            },
            **resource_metrics,
            "storage_gets": query_metrics["storage_gets"],
            "storage_puts": runtime_storage_trace["storage_puts"],
            "storage_bytes_read": query_metrics["storage_bytes_read"],
            "storage_bytes_written": runtime_storage_trace["storage_bytes_written"],
            **setup_storage,
            "throughput_milli_per_second": max(
                1,
                round(queries * 1_000_000_000_000 / query_metrics["query_elapsed_ns"]),
            ),
        },
        "index_receipt_sha256": receipt_document_sha256(index_receipt),
        "clone_receipt_sha256": None,
        "runtime_attestation_sha256": runtime_attestation_sha256(runtime_attestation),
    }
    validated = validate_cell_result(
        result,
        cell=cell,
        protocol_bytes=protocol_bytes,
        source_archive_sha256=source_archive_sha256,
        dataset_materialization_sha256=dataset_materialization_sha256,
        index_receipt=index_receipt,
        runtime_attestation=runtime_attestation,
    )
    return {
        "publishable": True,
        "runtime_profile": runtime_profile,
        "result": validated,
    }


def build_lifecycle_publication_report(
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    protocol_bytes: bytes,
    source_archive_sha256: str,
    dataset_materialization_sha256: str,
    attempt_id: str,
    instance_identity: str,
    lifecycle_metrics: dict[str, int],
    resource_metrics: dict[str, int],
    storage_metrics: dict[str, int],
    index_receipt: dict[str, object],
    clone_receipt: dict[str, object],
    runtime_attestation: dict[str, object],
) -> dict[str, object]:
    expected_lifecycle_fields = frozenset(
        {
            "insert_ops",
            "flush_ops",
            "consolidate_ops",
            "upsert_ops",
            "delete_ops",
            "compact_ops",
            "purge_ops",
            "lifecycle_accuracy_ppm",
            "batch_latency_p50_us",
            "batch_latency_p95_us",
            "batch_latency_p99_us",
            "throughput_milli_per_second",
            "first_publish_us",
            "time_to_searchable_us",
            "time_to_fully_indexed_us",
            "time_to_consolidated_us",
            "write_amplification_ppm",
        }
    )
    expected_resource_fields = frozenset(
        {"cpu_ns", "peak_rss_bytes", "disk_read_bytes", "disk_write_bytes"}
    )
    expected_storage_fields = frozenset(
        {
            "storage_gets",
            "storage_puts",
            "storage_bytes_read",
            "storage_bytes_written",
            "storage_distinct_data_objects",
            "storage_max_data_object_bytes",
        }
    )
    if frozenset(lifecycle_metrics) != expected_lifecycle_fields:
        raise ValueError("publication lifecycle metric fields differ")
    if frozenset(resource_metrics) != expected_resource_fields:
        raise ValueError("publication lifecycle resource fields differ")
    if frozenset(storage_metrics) != expected_storage_fields:
        raise ValueError("publication lifecycle storage fields differ")
    result = {
        "schema_version": 1,
        "status": "complete",
        "cell_id": cell.get("cell_id"),
        "manifest_sha256": cell.get("manifest_sha256"),
        "protocol_sha256": hashlib.sha256(protocol_bytes).hexdigest(),
        "source_archive_sha256": source_archive_sha256,
        "attempt_id": attempt_id,
        "instance_identity": instance_identity,
        "arm": arm,
        "metrics": {**lifecycle_metrics, **resource_metrics, **storage_metrics},
        "index_receipt_sha256": receipt_document_sha256(index_receipt),
        "clone_receipt_sha256": clone_receipt_document_sha256(clone_receipt),
        "runtime_attestation_sha256": runtime_attestation_sha256(runtime_attestation),
    }
    validated = validate_cell_result(
        result,
        cell=cell,
        protocol_bytes=protocol_bytes,
        source_archive_sha256=source_archive_sha256,
        dataset_materialization_sha256=dataset_materialization_sha256,
        index_receipt=index_receipt,
        runtime_attestation=runtime_attestation,
        clone_receipt=clone_receipt,
    )
    return {"publishable": True, "result": validated}


def claim_ineligible_lifecycle_diagnostic(
    report: dict[str, object], *, write_ops: int
) -> dict[str, object]:
    """Label a bounded lifecycle diagnosis so it can never become release evidence."""

    if (
        isinstance(write_ops, bool)
        or not 1 <= write_ops <= 50_000
        or report.get("publishable") is not True
        or not isinstance(report.get("result"), dict)
    ):
        raise ValueError("lifecycle diagnostic write count or report is invalid")
    return {
        **report,
        "publishable": False,
        "claim_eligible": False,
        "diagnostic_write_ops": write_ops,
    }


def _read_canonical_value(path: Path, maximum_bytes: int) -> object:
    payload = path.read_bytes()
    if not payload or len(payload) > maximum_bytes or not payload.endswith(b"\n"):
        raise ValueError(f"{path} is missing or exceeds its canonical bound")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path} is not valid UTF-8 JSON") from error
    if canonical_json_bytes(value) + b"\n" != payload:
        raise ValueError(f"{path} is not canonical JSON")
    return value


def runtime_execution_contract(
    plan: dict[str, object],
    runtime_profile: str,
    effective_flow_control: dict[str, object],
) -> dict[str, object]:
    if runtime_profile not in {"recall", "concurrency", "lifecycle"}:
        raise ValueError("runtime execution contract profile is invalid")
    runtime = plan.get("runtime")
    steps = runtime.get("steps") if isinstance(runtime, dict) else None
    if not isinstance(steps, list) or len(steps) != 1:
        raise ValueError("runtime execution contract requires one benchmark step")
    step = steps[0]
    environment = step.get("env") if isinstance(step, dict) else None
    if not isinstance(environment, dict):
        raise ValueError("runtime execution contract has no benchmark environment")

    def positive_environment_integer(name: str) -> int:
        value = environment.get(name)
        if not isinstance(value, str) or not value.isascii() or not value.isdigit():
            raise ValueError(f"runtime execution contract {name} is invalid")
        parsed = int(value)
        if parsed <= 0:
            raise ValueError(f"runtime execution contract {name} is invalid")
        return parsed

    def nonnegative_environment_integer(name: str) -> int:
        value = environment.get(name)
        if not isinstance(value, str) or not value.isascii() or not value.isdigit():
            raise ValueError(f"runtime execution contract {name} is invalid")
        return int(value)

    requested = {
        "disk_cache_max_bytes": nonnegative_environment_integer(
            "BORSUK_BENCH_DISK_CACHE_MAX_BYTES"
        ),
        "ram_budget_bytes": positive_environment_integer(
            "BORSUK_BENCH_RAM_BUDGET_BYTES"
        ),
        "max_active_searches": positive_environment_integer(
            "BORSUK_BENCH_MAX_ACTIVE_SEARCHES"
        ),
        "max_waiting_searches": positive_environment_integer(
            "BORSUK_BENCH_MAX_WAITING_SEARCHES"
        ),
        "leaf_read_width": positive_environment_integer("BORSUK_BENCH_LEAF_READ_WIDTH"),
        "max_inflight_leaf_reads": positive_environment_integer(
            "BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS"
        ),
        "max_parallel_decode_rank_tasks": positive_environment_integer(
            "BORSUK_BENCH_MAX_PARALLEL_DECODE_RANK_TASKS"
        ),
        "exact_read_max_physical_amplification": positive_environment_integer(
            "BORSUK_BENCH_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION"
        ),
        "cpu_threads": positive_environment_integer("BORSUK_CPU_THREADS"),
        "io_threads": positive_environment_integer("BORSUK_IO_THREADS"),
        "s3_get_concurrency": positive_environment_integer(
            "BORSUK_BACKING_GET_CONCURRENCY"
        ),
    }
    if not 1 <= requested["exact_read_max_physical_amplification"] <= 5:
        raise ValueError("exact-read physical amplification must be in 1..=5")
    if (
        not isinstance(effective_flow_control, dict)
        or frozenset(effective_flow_control) != {"schema_version", *requested}
        or effective_flow_control.get("schema_version") != 4
        or any(
            effective_flow_control.get(key) != value for key, value in requested.items()
        )
    ):
        raise ValueError(
            "effective runtime flow control differs from its frozen request"
        )
    return {
        "schema_version": 5,
        "runtime_profile": runtime_profile,
        **requested,
    }


def _positive_integer_tuple(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(item) for item in value.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("expected positive comma-separated integers")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("protocol", type=Path)
    parser.add_argument("workspace", type=Path)
    parser.add_argument("--generator", type=Path)
    parser.add_argument("--borsuk-bench", type=Path)
    parser.add_argument("--arm-index", type=int, default=0)
    parser.add_argument(
        "--mode", choices=("smoke", "build", "seal", "runtime"), default="smoke"
    )
    parser.add_argument(
        "--runtime-profile",
        choices=("recall", "concurrency", "lifecycle"),
        default="recall",
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--source-archive-sha256")
    parser.add_argument("--dataset-materialization-sha256")
    parser.add_argument("--attempt-id")
    parser.add_argument("--instance-identity")
    parser.add_argument("--purchase-option", choices=("spot", "on-demand"))
    parser.add_argument("--object-roster", type=Path)
    parser.add_argument("--index-receipt", type=Path)
    parser.add_argument("--index-inventory", type=Path)
    parser.add_argument("--clone-receipt", type=Path)
    parser.add_argument("--clone-inventory", type=Path)
    parser.add_argument("--build-complete", type=Path)
    parser.add_argument("--max-active-searches", type=int)
    parser.add_argument("--max-waiting-searches", type=int)
    parser.add_argument("--leaf-read-width", type=int)
    parser.add_argument("--max-inflight-leaf-reads", type=int)
    parser.add_argument("--max-parallel-decode-rank-tasks", type=int)
    parser.add_argument("--cpu-threads", type=int)
    parser.add_argument("--io-threads", type=int)
    parser.add_argument("--s3-get-concurrency", type=int)
    parser.add_argument("--ram-budget-bytes", type=int)
    parser.add_argument("--disk-cache-max-bytes", type=int)
    parser.add_argument("--exact-read-max-physical-amplification", type=int)
    parser.add_argument("--diagnostic-write-ops", type=int)
    parser.add_argument("--diagnostic-read-nprobes", type=_positive_integer_tuple)
    parser.add_argument("--diagnostic-read-candidates", type=_positive_integer_tuple)
    parser.add_argument("--v21-feasibility", action="store_true")
    parser.add_argument("--v21-diagnostic-protocol", type=Path)
    parser.add_argument("--v21-diagnostic-manifest", type=Path)
    args = parser.parse_args()

    runtime_flow_control = runtime_flow_control_authority(
        args.mode,
        {
            "disk_cache_max_bytes": args.disk_cache_max_bytes,
            "exact_read_max_physical_amplification": (
                args.exact_read_max_physical_amplification
            ),
            "max_active_searches": args.max_active_searches,
            "max_waiting_searches": args.max_waiting_searches,
            "leaf_read_width": args.leaf_read_width,
            "max_inflight_leaf_reads": args.max_inflight_leaf_reads,
            "max_parallel_decode_rank_tasks": args.max_parallel_decode_rank_tasks,
            "cpu_threads": args.cpu_threads,
            "io_threads": args.io_threads,
            "s3_get_concurrency": args.s3_get_concurrency,
            "ram_budget_bytes": args.ram_budget_bytes,
        },
    )

    cell = read_protocol(args.protocol)
    diagnostic_cell = None
    if args.v21_feasibility:
        if (
            args.v21_diagnostic_protocol is None
            or args.v21_diagnostic_manifest is None
        ):
            raise ValueError("V21 feasibility requires diagnostic source authority")
        diagnostic_cell = read_protocol(args.v21_diagnostic_protocol)
        validate_publication_cell_authority(
            diagnostic_cell, args.v21_diagnostic_manifest
        )
    elif (
        args.v21_diagnostic_protocol is not None
        or args.v21_diagnostic_manifest is not None
    ):
        raise ValueError("diagnostic source authority is V21-only")
    protocol_bytes = args.protocol.read_bytes()
    arms = plan_arms(cell)
    if args.arm_index < 0 or args.arm_index >= len(arms):
        raise ValueError("arm index is outside the scheduled factor matrix")
    arm = arms[args.arm_index]
    publication = args.mode in {"build", "seal", "runtime"}
    if publication:
        if args.manifest is None:
            raise ValueError("publication execution requires its frozen manifest")
        validate_publication_cell_authority(cell, args.manifest)
        for role, value in (
            ("source archive checksum", args.source_archive_sha256),
            ("dataset materialization checksum", args.dataset_materialization_sha256),
            ("attempt identity", args.attempt_id),
            ("instance identity", args.instance_identity),
        ):
            if not value:
                raise ValueError(f"publication execution requires {role}")
    if args.mode == "seal":
        if args.build_complete is None or args.object_roster is None:
            raise ValueError(
                "publication seal requires build completion and object roster"
            )
        completion = _read_canonical_value(args.build_complete, 256 * 1024)
        if not isinstance(completion, dict) or frozenset(completion) != frozenset(
            {
                "schema_version",
                "document_kind",
                "status",
                "cell_id",
                "builder_instance_identity",
                "builder_instance_type",
                "build_artifact",
                "process_resources",
                "elapsed_ns",
            }
        ):
            raise ValueError("publication build completion fields differ")
        if (
            completion["schema_version"] != 1
            or completion["document_kind"] != "publication-v3-build-complete"
            or completion["status"] != "complete"
            or completion["cell_id"] != cell.get("cell_id")
            or completion["builder_instance_identity"] != args.instance_identity
        ):
            raise ValueError("publication build completion authority differs")
        roster = _read_canonical_value(args.object_roster, 32 * 1024 * 1024)
        if not isinstance(roster, list):
            raise ValueError("publication object roster must be a JSON list")
        receipt = build_index_receipt(
            cell=cell,
            source_archive_sha256=str(args.source_archive_sha256),
            dataset_materialization_sha256=str(args.dataset_materialization_sha256),
            build_attempt_id=str(args.attempt_id),
            builder_instance_identity=str(completion["builder_instance_identity"]),
            builder_instance_type=str(completion["builder_instance_type"]),
            build_artifact=completion["build_artifact"],
            object_roster=roster,
            build_metrics=build_receipt_metrics(
                completion["process_resources"],
                completion["build_artifact"]["storage_metrics"],
                elapsed_ns=completion["elapsed_ns"],
            ),
        )
        destination = args.workspace / "INDEX_COMPLETE.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        (args.workspace / "INDEX_OBJECTS.json").write_bytes(
            canonical_json_bytes(roster) + b"\n"
        )
        destination.write_bytes(canonical_json_bytes(receipt) + b"\n")
        print(json.dumps(receipt, sort_keys=True))
        return 0
    if args.borsuk_bench is None or (
        args.mode in {"smoke", "build"} and args.generator is None
    ):
        raise ValueError("selected execution mode requires its benchmark binaries")
    plan = build_execution_plan(
        cell,
        arm=arm,
        workspace=args.workspace,
        generator=args.generator or Path("/bin/false"),
        borsuk_bench=args.borsuk_bench,
        mode=args.mode,
        runtime_profile=args.runtime_profile,
        runtime_flow_control=runtime_flow_control,
        diagnostic_write_ops=args.diagnostic_write_ops,
        diagnostic_read_nprobes=args.diagnostic_read_nprobes,
        diagnostic_read_candidates=args.diagnostic_read_candidates,
        v21_feasibility=args.v21_feasibility,
    )
    if args.mode == "build":
        output, resources, elapsed_ns = execute_publication_phase(plan, "build")
        artifact = read_build_artifact(output, cell=cell)
        completion = {
            "schema_version": 1,
            "document_kind": "publication-v3-build-complete",
            "status": "complete",
            "cell_id": cell["cell_id"],
            "builder_instance_identity": str(args.instance_identity),
            "builder_instance_type": str(plan["build"]["worker"]["instance_type"]),
            "build_artifact": artifact,
            "process_resources": resources,
            "elapsed_ns": elapsed_ns,
        }
        destination = args.workspace / "BUILD_COMPLETE.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(canonical_json_bytes(completion) + b"\n")
        print(json.dumps(completion, sort_keys=True))
        return 0
    if args.mode == "runtime":
        if args.purchase_option is None:
            raise ValueError("publication runtime requires its purchase option")
        if (
            args.index_receipt is None
            or args.object_roster is None
            or args.index_inventory is None
        ):
            raise ValueError(
                "publication runtime requires receipt, roster, and inventory authority"
            )
        receipt_payload = args.index_receipt.read_bytes()
        receipt, _ = require_verified_index(
            receipt_payload,
            cell=cell,
            source_archive_sha256=str(args.source_archive_sha256),
            dataset_materialization_sha256=str(args.dataset_materialization_sha256),
        )
        inventory = _read_canonical_value(args.index_inventory, 32 * 1024 * 1024)
        if not isinstance(inventory, list):
            raise ValueError("publication index inventory must be a JSON list")
        roster = require_verified_object_roster(
            receipt, args.object_roster.read_bytes(), cell=cell
        )
        reconcile_index_inventory(roster, inventory)
        workload = cell.get("workload")
        workload_kind = workload.get("kind") if isinstance(workload, dict) else None
        clone_receipt = None
        if workload_kind == "read-recall":
            if args.clone_receipt is not None or args.clone_inventory is not None:
                raise ValueError(
                    "read-only publication runtime cannot use a mutable clone"
                )
            authorized_runtime = authorize_publication_runtime(
                plan,
                receipt=receipt,
                cell=cell,
                source_archive_sha256=str(args.source_archive_sha256),
                dataset_materialization_sha256=str(args.dataset_materialization_sha256),
            )
        elif workload_kind == "write-update-delete-compact":
            if args.clone_receipt is None or args.clone_inventory is None:
                raise ValueError(
                    "lifecycle publication runtime requires clone authority"
                )
            clone_value = _read_canonical_value(args.clone_receipt, 256 * 1024)
            if not isinstance(clone_value, dict):
                raise ValueError("publication clone receipt must be a JSON object")
            clone_receipt = clone_value
            require_verified_clone_inventory(
                clone_receipt,
                args.clone_inventory.read_bytes(),
                base_roster=roster,
            )
            authorized_runtime = authorize_publication_mutation_runtime(
                plan,
                clone_receipt=clone_receipt,
                base_receipt=receipt,
                arm=arm,
                attempt_id=str(args.attempt_id),
                cell=cell,
            )
        else:
            raise ValueError("publication runtime workload is not implemented")
        authorized_plan = {**plan, "runtime": authorized_runtime}
        source_root = Path(__file__).resolve().parent.parent
        attestation_cell = diagnostic_cell or cell
        attestation_resource_role = (
            "diagnostic" if args.v21_feasibility else "runtime"
        )
        attestation_memory_max = 32 * 1024**3 if args.v21_feasibility else None
        preflight = validate_runtime_attestation(
            collect_runtime_attestation(
                cell=attestation_cell,
                attempt_id=str(args.attempt_id),
                runtime=authorized_runtime,
                source_root=source_root,
                purchase_option=args.purchase_option,
            ),
            cell=attestation_cell,
            attempt_id=str(args.attempt_id),
            resource_role=attestation_resource_role,
            expected_memory_max_bytes=attestation_memory_max,
        )
        if preflight["instance_id"] != args.instance_identity:
            raise ValueError("runtime EC2 identity differs from its scheduled instance")
        output, resources, elapsed_ns = execute_publication_phase(
            authorized_plan, "runtime"
        )
        if args.v21_feasibility:
            if runtime_flow_control is None:
                raise ValueError("V21 feasibility requires runtime flow authority")
            execution_contract = runtime_execution_contract(
                authorized_plan,
                args.runtime_profile,
                {"schema_version": 4, **runtime_flow_control},
            )
            (args.workspace / "RUNTIME_EXECUTION_CONTRACT.json").write_bytes(
                canonical_json_bytes(execution_contract) + b"\n"
            )
            runtime_attestation = validate_runtime_attestation(
                collect_runtime_attestation(
                    cell=attestation_cell,
                    attempt_id=str(args.attempt_id),
                    runtime=authorized_runtime,
                    source_root=source_root,
                    purchase_option=args.purchase_option,
                ),
                cell=attestation_cell,
                attempt_id=str(args.attempt_id),
                resource_role=attestation_resource_role,
                expected_memory_max_bytes=attestation_memory_max,
            )
            (args.workspace / "RUNTIME_ATTESTATION.json").write_bytes(
                canonical_json_bytes(runtime_attestation) + b"\n"
            )
            arms_path = output / "bench_v21_feasibility_arms.csv"
            samples_path = output / "bench_v21_feasibility_samples.csv"
            summary_path = output / "bench_v21_feasibility_summary.json"
            if any(
                not path.is_file() or path.stat().st_size == 0
                for path in (arms_path, samples_path, summary_path)
            ):
                raise ValueError("V21 feasibility runtime emitted incomplete artifacts")
            with arms_path.open(newline="") as source_file:
                arm_rows = list(csv.DictReader(source_file))
            with samples_path.open(newline="") as source_file:
                sample_rows = list(csv.DictReader(source_file))
            summary_value = _read_canonical_value(summary_path, 2 * 1024 * 1024)
            if not isinstance(summary_value, dict):
                raise ValueError("V21 feasibility summary must be a JSON object")
            report = summarize_v21_feasibility_artifacts(
                arm_rows,
                sample_rows,
                summary_value,
                expected_source_archive_sha256=str(args.source_archive_sha256),
                expected_index_id=str(cell["index_prefix"])
                .rstrip("/")
                .rsplit("/", 1)[-1],
                expected_dataset_id=str(cell["dataset"]["id"]),
                expected_queries=int(plan["effective_queries"]),
                expected_dataset_rows=int(plan["effective_rows"]),
                expected_query_seed=int(cell["query_seed"]),
                expected_dimensions=int(cell["dataset"]["dimensions"]),
            )
            report.update(
                {
                    "cell_id": cell["cell_id"],
                    "attempt_id": args.attempt_id,
                    "instance_identity": args.instance_identity,
                    "dataset_materialization_sha256": (
                        args.dataset_materialization_sha256
                    ),
                    "elapsed_ns": elapsed_ns,
                    "resources": resources,
                    "runtime_attestation": runtime_attestation,
                }
            )
            if args.v21_feasibility:
                report["diagnostic_cell_id"] = attestation_cell["cell_id"]
            destination = args.workspace / "RESULT_COMPLETE.json"
            destination.write_bytes(canonical_json_bytes(report) + b"\n")
            print(json.dumps(report, sort_keys=True))
            return 0
        effective_flow_control = _read_canonical_value(
            output / "bench_runtime_flow_control.json", 64 * 1024
        )
        if not isinstance(effective_flow_control, dict):
            raise ValueError("benchmark emitted no effective runtime flow control")
        execution_contract = runtime_execution_contract(
            authorized_plan, args.runtime_profile, effective_flow_control
        )
        (args.workspace / "RUNTIME_EXECUTION_CONTRACT.json").write_bytes(
            canonical_json_bytes(execution_contract) + b"\n"
        )
        runtime_attestation = validate_runtime_attestation(
            collect_runtime_attestation(
                cell=cell,
                attempt_id=str(args.attempt_id),
                runtime=authorized_runtime,
                source_root=source_root,
                purchase_option=args.purchase_option,
            ),
            cell=cell,
            attempt_id=str(args.attempt_id),
        )
        (args.workspace / "RUNTIME_ATTESTATION.json").write_bytes(
            canonical_json_bytes(runtime_attestation) + b"\n"
        )
        effective_queries = int(plan["effective_queries"])
        expected_cache_cohort_size = runtime_expected_cache_cohort_size(
            arm,
            runtime_profile=args.runtime_profile,
            effective_flow_control=effective_flow_control,
            effective_queries=effective_queries,
        )
        if workload_kind == "read-recall" and args.runtime_profile == "concurrency":
            summary_path = output / "bench_concurrency.csv"
            samples_path = output / "bench_concurrency_samples.csv"
            if not summary_path.is_file() or not samples_path.is_file():
                raise ValueError("concurrency runtime emitted no concurrency artifacts")
            with summary_path.open(newline="") as source:
                summary_rows = list(csv.DictReader(source))
            with samples_path.open(newline="") as source:
                sample_rows = list(csv.DictReader(source))
            runtime_step = authorized_runtime["steps"][0]
            runtime_environment = runtime_step["env"]
            workers = tuple(
                int(value)
                for value in runtime_environment["BORSUK_BENCH_CONCURRENCY"].split(",")
            )
            factors = cell["workload"]["factors"]
            concurrency_metrics = summarize_concurrency_artifacts(
                summary_rows,
                sample_rows,
                expected_workers=workers,
                expected_queries=effective_queries,
                minimum_recall_ppm=int(factors["minimum_recall_ppm"]),
                expected_scan_codec=str(cell["index_profile"]["global_scan_codec"]),
                expected_nprobe=int(arm["leaf_page_budget"]),
                expected_max_candidates=V20_COMPATIBILITY_CANDIDATES,
                expected_cache_profile=(
                    "uncached" if arm["cache_state"] == "cold" else "disk_cached"
                ),
                expected_cache_coverage_percent=(
                    0 if arm["cache_state"] == "cold" else 100
                ),
                expected_cache_cohort_size=expected_cache_cohort_size,
            )
            trace_writes = summarize_runtime_write_trace(output / "storage-access.csv")
            runtime_storage = reconcile_concurrency_storage(
                concurrency_metrics, trace_writes
            )
            report = {
                "publishable": True,
                "runtime_profile": "concurrency",
                "result": {
                    "schema_version": 1,
                    "cell_id": cell["cell_id"],
                    "arm": concurrency_result_arm(arm),
                    "attempt_id": args.attempt_id,
                    "instance_identity": args.instance_identity,
                    "source_archive_sha256": args.source_archive_sha256,
                    "dataset_materialization_sha256": args.dataset_materialization_sha256,
                    "elapsed_ns": elapsed_ns,
                    "metrics": concurrency_metrics,
                    "resources": resources,
                    "runtime_storage": runtime_storage,
                    "runtime_attestation": runtime_attestation,
                },
            }
        elif workload_kind == "read-recall":
            samples = output / "bench_query_samples.csv"
            if not samples.is_file() or samples.stat().st_size == 0:
                raise ValueError("publication runtime emitted no query samples")
            with samples.open(newline="") as source:
                rows = list(csv.DictReader(source))
            if args.diagnostic_read_nprobes is not None:
                assert args.diagnostic_read_candidates is not None
                summary_path = output / "bench_recall_latency.csv"
                if not summary_path.is_file() or summary_path.stat().st_size == 0:
                    raise ValueError("read diagnostic emitted no summary artifact")
                with summary_path.open(newline="") as source:
                    summary_rows = list(csv.DictReader(source))
                report = summarize_read_diagnostic_samples(
                    rows,
                    summary_rows=summary_rows,
                    cell=cell,
                    arm=arm,
                    expected_queries=effective_queries,
                    nprobes=args.diagnostic_read_nprobes,
                    candidates=args.diagnostic_read_candidates,
                    expected_cache_cohort_size=expected_cache_cohort_size,
                )
                report.update(
                    {
                        "cell_id": cell["cell_id"],
                        "attempt_id": args.attempt_id,
                        "instance_identity": args.instance_identity,
                        "source_archive_sha256": args.source_archive_sha256,
                        "dataset_materialization_sha256": (
                            args.dataset_materialization_sha256
                        ),
                        "elapsed_ns": elapsed_ns,
                        "resources": resources,
                        "runtime_attestation": runtime_attestation,
                    }
                )
                destination = args.workspace / "RESULT_COMPLETE.json"
                destination.write_bytes(canonical_json_bytes(report) + b"\n")
                print(json.dumps(report, sort_keys=True))
                return 0
            metrics = summarize_query_samples(
                rows,
                cell=cell,
                arm=arm,
                expected_queries=effective_queries,
                expected_cache_cohort_size=expected_cache_cohort_size,
            )
            trace_writes = summarize_runtime_write_trace(output / "storage-access.csv")
            report = build_publication_report(
                cell=cell,
                arm=arm,
                protocol_bytes=protocol_bytes,
                source_archive_sha256=str(args.source_archive_sha256),
                dataset_materialization_sha256=str(args.dataset_materialization_sha256),
                attempt_id=str(args.attempt_id),
                instance_identity=str(args.instance_identity),
                elapsed_ns=elapsed_ns,
                query_metrics=metrics,
                resource_metrics=resources,
                runtime_storage_trace=trace_writes,
                index_receipt=receipt,
                runtime_attestation=runtime_attestation,
                runtime_profile=args.runtime_profile,
            )
        else:
            lifecycle = summarize_lifecycle_artifacts(
                output,
                expected_batch_size=int(arm["batch_size"]),
                expected_writers=int(arm["writers"]),
            )
            lifecycle.update(
                reconcile_lifecycle_storage_trace(
                    lifecycle,
                    summarize_runtime_write_trace(output / "storage-access.csv"),
                )
            )
            storage_metrics = {
                field: lifecycle.pop(field)
                for field in (
                    "storage_gets",
                    "storage_puts",
                    "storage_bytes_read",
                    "storage_bytes_written",
                    "storage_distinct_data_objects",
                    "storage_max_data_object_bytes",
                )
            }
            if clone_receipt is None:
                raise ValueError("lifecycle publication clone authority is missing")
            report = build_lifecycle_publication_report(
                cell=cell,
                arm=arm,
                protocol_bytes=protocol_bytes,
                source_archive_sha256=str(args.source_archive_sha256),
                dataset_materialization_sha256=str(args.dataset_materialization_sha256),
                attempt_id=str(args.attempt_id),
                instance_identity=str(args.instance_identity),
                lifecycle_metrics=lifecycle,
                resource_metrics=resources,
                storage_metrics=storage_metrics,
                index_receipt=receipt,
                clone_receipt=clone_receipt,
                runtime_attestation=runtime_attestation,
            )
            if args.diagnostic_write_ops is not None:
                report = claim_ineligible_lifecycle_diagnostic(
                    report, write_ops=args.diagnostic_write_ops
                )
        destination = args.workspace / "RESULT_COMPLETE.json"
        destination.write_bytes(canonical_json_bytes(report) + b"\n")
        print(json.dumps(report, sort_keys=True))
        return 0
    samples = execute_plan(plan)
    with samples.open(newline="") as source:
        rows = list(csv.DictReader(source))
    metrics = summarize_query_samples(
        rows,
        cell=cell,
        arm=arm,
        expected_queries=int(plan["effective_queries"]),
        enforce_quality=False,
        expected_cache_cohort_size=smoke_cache_cohort_authority(plan, arm),
    )
    report = build_smoke_report(
        cell=cell,
        arm=arm,
        effective_rows=int(plan["effective_rows"]),
        effective_queries=int(plan["effective_queries"]),
        metrics=metrics,
        protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),
    )
    destination = args.workspace / "SMOKE_COMPLETE.json"
    destination.write_bytes(canonical_json_bytes(report) + b"\n")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
