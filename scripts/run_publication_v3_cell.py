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
V12_COMPATIBILITY_CANDIDATES = 4096
PRODUCTION_BUILD_FIELDS = tuple(
    "logical_cell_catalog_checksum,logical_cells,logical_cell_dimensions,logical_cell_catalog_bytes,vector_element_type,scan_codec,turboquant_bits,turboquant_qjl_bits,turboquant_shards,build_layout,leaf_capability,segment_max_vectors,records,segment_bytes,vector_sidecar_bytes,graph_bytes,global_scan_bytes,total_active_index_bytes,bytes_per_vector,resident_bytes_estimate,ram_budget_bytes,collection_resident_bytes,retained_bytes,retained_capacity_bytes,retained_peak_bytes,transient_bytes,transient_capacity_bytes,transient_peak_bytes,ingest_ms,compaction_ms,compaction_bytes_read,compaction_bytes_written,storage_gets,storage_puts,storage_deletes,storage_heads,storage_lists,storage_bytes_read,storage_bytes_written".split(",")
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
    "op,configured_batch_records,ops,batches,wall_ms,ops_per_s,mean_batch_ms,stddev_batch_ms,p50_batch_ms,p95_batch_ms,p99_batch_ms,max_batch_ms,mean_amortized_ms,gets,puts,deletes,heads,lists,bytes_read,bytes_written".split(",")
)
WRITE_SAMPLE_FIELDS = tuple(
    "op,batch_index,batch_records,batch_latency_ms,amortized_ms,gets,puts,deletes,heads,lists".split(",")
)
LIFECYCLE_FIELDS = tuple(
    "configured_batch_records,inserted_vectors,logical_vector_bytes,insert_wall_ms,insert_vectors_per_s,first_batch_publish_ms,time_to_searchable_ms,searchable_samples,searchable_fraction,upsert_samples,upsert_correct_fraction,delete_samples,delete_absent_fraction,compact_delete_absent_fraction,purge_delete_absent_fraction,delta_flush_ms,time_to_fully_indexed_ms,wal_publish_bytes,indexed_delta_bytes,total_indexing_bytes,write_amplification,write_amplification_is_lower_bound,consolidation_ms,time_to_consolidated_ms,consolidated_global_bytes,consolidation_amplification".split(",")
)
LIFECYCLE_OPERATIONS = ("insert", "upsert", "delete", "compact", "purge")


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
    if (
        canonical_json_bytes(normalized) != canonical_json_bytes(expected)
        or (candidate_index != expected_index and not retry_index)
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
            "update_percent": factors.get("update_percent"),
            "delete_percent": factors.get("delete_percent"),
        }
        if any(not isinstance(values, list) or not values for values in axes.values()):
            raise ValueError("lifecycle arm factors are incomplete")
        return [
            {
                "writers": writers,
                "batch_size": batch_size,
                "update_percent": update_percent,
                "delete_percent": delete_percent,
            }
            for writers in axes["writers"]
            for batch_size in axes["batch_size"]
            for update_percent in axes["update_percent"]
            for delete_percent in axes["delete_percent"]
        ]
    if workload.get("kind") != "read-recall":
        raise ValueError(f"workload kind {workload.get('kind')!r} is not executable")
    k_values = factors.get("k")
    if k_values != [10]:
        unsupported = next((value for value in k_values or [] if value != 10), "missing")
        raise ValueError(f"k={unsupported} is not executable by the current benchmark")
    leaf_page_budgets = factors.get("leaf_page_budgets")
    cache_states = factors.get("cache_states")
    if (
        not isinstance(leaf_page_budgets, list)
        or not leaf_page_budgets
        or not isinstance(cache_states, list)
        or not cache_states
        or any(budget not in {4, 8, 16, 32} for budget in leaf_page_budgets)
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


def build_execution_plan(
    cell: dict[str, object],
    *,
    arm: dict[str, object],
    workspace: Path,
    generator: Path,
    borsuk_bench: Path,
    mode: str,
) -> dict[str, object]:
    if mode not in {"publication", "smoke"}:
        raise ValueError("execution mode must be publication or smoke")
    if cell.get("system") != "borsuk":
        raise ValueError(f"system {cell.get('system')!r} is not available in local execution")
    workload = cell.get("workload")
    dataset = cell.get("dataset")
    source = cell.get("source")
    if not isinstance(workload, dict) or workload.get("kind") not in SUPPORTED_LOCAL_KINDS:
        raise ValueError("workload is not supported by the local read runner")
    if not isinstance(dataset, dict) or not isinstance(dataset.get("source"), dict):
        raise ValueError("cell dataset is invalid")
    if mode == "publication" and (not isinstance(source, dict) or source.get("state") != "frozen"):
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
    if (
        isinstance(resident_limit_mib, bool)
        or not isinstance(resident_limit_mib, int)
        or resident_limit_mib <= 0
        or isinstance(disk_cache_limit_mib, bool)
        or not isinstance(disk_cache_limit_mib, int)
        or disk_cache_limit_mib <= 0
    ):
        raise ValueError("BORSUK runtime-client limits are invalid")
    build_workers = environment.get("build_workers")
    build_storage = environment.get("build_storage")
    build_worker = build_workers.get("borsuk") if isinstance(build_workers, dict) else None
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
    if isinstance(queries_per_repetition, bool) or not isinstance(queries_per_repetition, int):
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
    if mode == "smoke" and dataset["source"].get("generator") == "synthetic-clustered-v1":
        smoke_rows = ((smoke_rows + 99) // 100) * 100
    effective_rows = scheduled_rows if mode == "publication" else min(scheduled_rows, smoke_rows)
    effective_queries = queries_per_repetition if mode == "publication" else min(queries_per_repetition, 10)
    dataset_dir = workspace / "dataset"
    output_dir = workspace / "output"
    index_dir = workspace / "index"
    cache_dir = workspace / "cache"
    if arm not in plan_arms(cell):
        raise ValueError("execution arm is not authorized by the scheduled cell")

    steps: list[dict[str, object]] = []
    dataset_source = dataset["source"]
    if dataset_source.get("state") == "generated":
        if dataset_source.get("generator") != "synthetic-clustered-v1":
            raise ValueError("scheduled synthetic generator is not implemented")
        if dataset.get("metric") != "cosine":
            raise ValueError("the deterministic dense generator supports cosine cells only")
        steps.append(
            {
                "argv": [str(generator)],
                "env": {
                    "BORSUK_SYNTHETIC_OUTPUT": str(dataset_dir),
                    "BORSUK_SYNTHETIC_TRAIN": str(effective_rows),
                    "BORSUK_SYNTHETIC_DIMENSIONS": str(dimensions),
                    "BORSUK_SYNTHETIC_QUERIES": str(effective_queries),
                    "BORSUK_SYNTHETIC_GROUP_SIZE": "100",
                    "BORSUK_SYNTHETIC_SEED": str(dataset_source.get("seed")),
                },
            }
        )
    elif dataset_source.get("state") != "staged":
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
        "BORSUK_BENCH_CANDIDATES": str(V12_COMPATIBILITY_CANDIDATES),
        "BORSUK_BENCH_READ_ONLY": "1",
        "BORSUK_BENCH_CONCURRENCY": "1",
        "BORSUK_BENCH_SKIP_EXACT_RECALL": "1",
        "BORSUK_BENCH_CACHE_PROFILE": (
            "uncached" if arm.get("cache_state", "cold") == "cold" else "disk_cached"
        ),
        "BORSUK_BENCH_RAM_BUDGET_BYTES": str(resident_limit_mib * 1024 * 1024),
        "BORSUK_BENCH_DISK_CACHE_MAX_BYTES": str(
            disk_cache_limit_mib * 1024 * 1024
        ),
    }
    code_bytes = index_profile.get("code_bytes")
    training_rows_per_cell = index_profile.get("training_rows_per_cell")
    training_iterations = index_profile.get("training_iterations")
    if (
        isinstance(code_bytes, bool)
        or not isinstance(code_bytes, int)
        or code_bytes <= 0
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
        if mode == "publication"
        else min(profile_cells, max(1, effective_rows // minimum_rows_per_cell))
    )
    training_rows = min(effective_rows, effective_cells * training_rows_per_cell)
    if training_rows < effective_cells:
        raise ValueError("BORSUK index profile has fewer training rows than logical cells")
    benchmark_env.update(
        {
            "BORSUK_BENCH_LOGICAL_CELLS": str(effective_cells),
            "BORSUK_BENCH_LOGICAL_CELL_TRAINING_ROWS": str(training_rows),
            "BORSUK_BENCH_LOGICAL_CELL_SEED": str(dataset_training_seed(dataset)),
            "BORSUK_BENCH_LOGICAL_CELL_ITERATIONS": str(training_iterations),
            "BORSUK_BENCH_GLOBAL_SCAN_CODEC": str(index_profile.get("leaf_codec")),
            "BORSUK_BENCH_GLOBAL_PQ_CODE_BYTES": str(code_bytes),
        }
    )
    if mode == "publication":
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
        }
        for field in (
            "BORSUK_BENCH_READ_ONLY",
            "BORSUK_BENCH_RECALL_ONLY",
            "BORSUK_BENCH_CACHE_PROFILE",
            "BORSUK_BENCH_RAM_BUDGET_BYTES",
            "BORSUK_BENCH_DISK_CACHE_MAX_BYTES",
        ):
            build_env.pop(field, None)
        # Index construction does not execute recall queries, but the benchmark
        # validates all query knobs at startup. Keep this build-only phase on a
        # valid V12 leaf-page/candidate compatibility pair; runtime owns the
        # scheduled recall sweep and its result labels.
        build_env["BORSUK_BENCH_NPROBES"] = "4"
        build_env["BORSUK_BENCH_CANDIDATES"] = "4096"
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
        if workload_kind == "write-update-delete-compact":
            runtime_env.update(
                {
                    "BORSUK_BENCH_RECALL_ONLY": "0",
                    "BORSUK_BENCH_READ_ONLY": "0",
                    "BORSUK_BENCH_WRITE_BATCH_SIZE": str(arm["batch_size"]),
                    "BORSUK_BENCH_LIFECYCLE_WRITERS": str(arm["writers"]),
                    "BORSUK_BENCH_UPDATE_PERCENT": str(arm["update_percent"]),
                    "BORSUK_BENCH_DELETE_PERCENT": str(arm["delete_percent"]),
                }
            )
        return {
            "schema_version": 1,
            "cell_id": cell.get("cell_id"),
            "mode": mode,
            "publishable": True,
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
        "mode": mode,
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
    if plan.get("mode") != "publication" or plan.get("publishable") is not True:
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
            raise ValueError("runtime index URI differs from the immutable build receipt")
        if environment.get("BORSUK_BENCH_BUILD_INDEX") != "0":
            raise ValueError("publication runtime must disable index construction")
        for forbidden in (
            "BORSUK_BENCH_BUILD_ONLY",
            "BORSUK_BENCH_INSERT_ONLY",
            "BORSUK_BENCH_RECLUSTER_BUILD",
            "BORSUK_BENCH_PRELOAD_SERVING",
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
    if not isinstance(workload, dict) or workload.get("kind") != "write-update-delete-compact":
        raise ValueError("mutation runtime requires a lifecycle cell")
    if arm.get("writers") != 1:
        raise ValueError("Publication V3 MVP supports lifecycle writers=1 only")
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
        if (
            environment.get("BORSUK_BENCH_BUILD_INDEX") != "0"
            or environment.get("BORSUK_BENCH_READ_ONLY") != "0"
            or environment.get("BORSUK_BENCH_RECALL_ONLY") != "0"
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
        child_environment.update({str(key): str(value) for key, value in environment.items()})
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


def summarize_query_samples(
    rows: list[dict[str, str]],
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    expected_queries: int,
    enforce_quality: bool = True,
) -> dict[str, int]:
    if len(rows) != expected_queries:
        raise ValueError("query sample artifact is incomplete for its arm")
    latencies_us: list[int] = []
    recalls_ppm: list[int] = []
    sample_indices: set[int] = set()
    storage_gets = 0
    storage_bytes_read = 0
    for row in rows:
        if row.get("schema_version") != "borsuk-production-bench-v11":
            raise ValueError("query sample schema differs")
        expected_phase = "uncached" if arm["cache_state"] == "cold" else "disk_cached"
        if (
            row.get("phase") != expected_phase
            or row.get("mode") != "srht-pq-scan"
            or int(row.get("nprobe", "-1")) != arm["leaf_page_budget"]
            or int(row.get("max_candidates", "-1"))
            != V12_COMPATIBILITY_CANDIDATES
        ):
            raise ValueError("query sample belongs to a different factor arm")
        sample_index = int(row["sample_index"])
        if sample_index < 0 or sample_index in sample_indices:
            raise ValueError("query sample indices must be unique and nonnegative")
        sample_indices.add(sample_index)
        latency = float(row["latency_ms"])
        recall = float(row["recall_at_10"])
        if not math.isfinite(latency) or latency < 0 or not math.isfinite(recall) or not 0 <= recall <= 1:
            raise ValueError("query sample latency or recall is invalid")
        latencies_us.append(round(latency * 1_000))
        recalls_ppm.append(round(recall * 1_000_000))
        network_gets = int(row["network_gets"])
        bytes_read = int(row["bytes_read"])
        if network_gets < 0 or bytes_read < 0:
            raise ValueError("query sample storage telemetry is invalid")
        storage_gets += network_gets
        storage_bytes_read += bytes_read
    correctness_ppm = round(sum(recalls_ppm) / len(recalls_ppm))
    factors = cell.get("workload", {}).get("factors", {})
    floor = factors.get("minimum_recall_ppm")
    if not isinstance(floor, int):
        raise ValueError("query sample quality floor is invalid")
    if enforce_quality and correctness_ppm < floor:
        raise ValueError(
            f"query sample recall observed {correctness_ppm} ppm is below required {floor} ppm"
        )
    return {
        "queries": len(rows),
        "correctness_ppm": correctness_ppm,
        "latency_p50_us": _nearest_rank(latencies_us, 0.50),
        "latency_p95_us": _nearest_rank(latencies_us, 0.95),
        "latency_p99_us": _nearest_rank(latencies_us, 0.99),
        "storage_gets": storage_gets,
        "storage_bytes_read": storage_bytes_read,
        "query_elapsed_ns": sum(latencies_us) * 1_000,
    }


def summarize_runtime_write_trace(path: Path) -> dict[str, int]:
    if not path.is_file():
        raise ValueError("publication runtime storage trace is missing")
    if (path.parent / "bench_build.csv").exists():
        raise ValueError("publication runtime unexpectedly emitted a build artifact")
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    storage_puts = 0
    storage_bytes_written = 0
    for row in rows:
        if row.get("operation") != "write":
            continue
        try:
            requests = int(row["request_count"])
            byte_count = int(row["object_bytes"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication runtime write trace is invalid") from error
        if requests < 0 or byte_count < 0:
            raise ValueError("publication runtime write trace is negative")
        storage_puts += requests
        storage_bytes_written += byte_count
    return {
        "storage_puts": storage_puts,
        "storage_bytes_written": storage_bytes_written,
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


def summarize_lifecycle_artifacts(
    output_dir: Path, *, expected_batch_size: int
) -> dict[str, int]:
    if isinstance(expected_batch_size, bool) or expected_batch_size <= 0:
        raise ValueError("publication lifecycle batch size is invalid")
    costs = _read_exact_csv(output_dir / "bench_write_costs.csv", WRITE_COST_FIELDS)
    samples = _read_exact_csv(output_dir / "bench_write_samples.csv", WRITE_SAMPLE_FIELDS)
    lifecycle_rows = _read_exact_csv(output_dir / "bench_lifecycle.csv", LIFECYCLE_FIELDS)
    if len(costs) != len(LIFECYCLE_OPERATIONS) or len(lifecycle_rows) != 1:
        raise ValueError("publication lifecycle artifacts have incomplete operation rows")
    by_operation: dict[str, dict[str, str]] = {}
    for row in costs:
        operation = row.get("op", "")
        if operation not in LIFECYCLE_OPERATIONS or operation in by_operation:
            raise ValueError("publication lifecycle operations differ")
        by_operation[operation] = row
        try:
            configured_batch = int(row["configured_batch_records"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication lifecycle batch size is invalid") from error
        if configured_batch != expected_batch_size:
            raise ValueError("publication lifecycle artifact belongs to another batch arm")
    if tuple(sorted(by_operation)) != tuple(sorted(LIFECYCLE_OPERATIONS)):
        raise ValueError("publication lifecycle operations differ")

    latencies_us: list[int] = []
    sample_indices: dict[str, set[int]] = {operation: set() for operation in LIFECYCLE_OPERATIONS}
    sample_records = {operation: 0 for operation in LIFECYCLE_OPERATIONS}
    for row in samples:
        operation = row.get("op", "")
        if operation not in sample_indices:
            raise ValueError("publication lifecycle sample operation differs")
        try:
            index = int(row["batch_index"])
            batch_records = int(row["batch_records"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication lifecycle sample is invalid") from error
        if index < 0 or index in sample_indices[operation] or batch_records < 0:
            raise ValueError("publication lifecycle sample identity is invalid")
        sample_indices[operation].add(index)
        sample_records[operation] += batch_records
        latency_ms = _finite_nonnegative_float(
            row.get("batch_latency_ms"), "publication lifecycle sample latency"
        )
        latencies_us.append(round(latency_ms * 1_000))
    for operation, row in by_operation.items():
        try:
            expected_batches = int(row["batches"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication lifecycle operation is invalid") from error
        if expected_batches <= 0 or sample_indices[operation] != set(range(expected_batches)):
            raise ValueError("publication lifecycle samples are incomplete")

    lifecycle = lifecycle_rows[0]
    try:
        lifecycle_batch = int(lifecycle["configured_batch_records"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("publication lifecycle batch size is invalid") from error
    if lifecycle_batch != expected_batch_size:
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
            raise ValueError("publication lifecycle verification evidence is invalid") from error
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
            raise ValueError("publication lifecycle operation count is invalid") from error
        if operations <= 0:
            raise ValueError("publication lifecycle operation count is invalid")
        operation_counts[operation] = operations
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
                raise ValueError("publication lifecycle storage telemetry is invalid") from error
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
        raise ValueError("publication lifecycle operation totals are invalid") from error
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
        _finite_nonnegative_float(lifecycle.get("first_batch_publish_ms"), "first publish")
        * 1_000
    )
    searchable_us = round(
        _finite_nonnegative_float(lifecycle.get("time_to_searchable_ms"), "searchable time")
        * 1_000
    )
    fully_indexed_us = round(
        _finite_nonnegative_float(lifecycle.get("time_to_fully_indexed_ms"), "indexed time")
        * 1_000
    )
    consolidated_us = round(
        _finite_nonnegative_float(lifecycle.get("time_to_consolidated_ms"), "consolidated time")
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
        or abs(fully_indexed_us / 1_000 - (insert_wall_ms + delta_flush_ms)) > 0.002
        or abs(consolidated_us / 1_000 - (fully_indexed_us / 1_000 + consolidation_ms))
        > 0.002
    ):
        raise ValueError("publication lifecycle milestone totals differ")
    total_operations = sum(operation_counts.values())
    return {
        "insert_ops": operation_counts["insert"],
        "upsert_ops": operation_counts["upsert"],
        "delete_ops": operation_counts["delete"],
        "compact_ops": operation_counts["compact"],
        "purge_ops": operation_counts["purge"],
        "lifecycle_accuracy_ppm": round(min(fractions) * 1_000_000),
        "batch_latency_p50_us": _nearest_rank(latencies_us, 0.50),
        "batch_latency_p95_us": _nearest_rank(latencies_us, 0.95),
        "batch_latency_p99_us": _nearest_rank(latencies_us, 0.99),
        "throughput_milli_per_second": max(1, round(total_operations * 1_000_000 / wall_ms)),
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
        raise ValueError("publication build phase timing artifact is not UTF-8") from error
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
        if row.get("schema_version") != "1":
            raise ValueError("publication build phase timing schema differs")
        key = (str(row.get("group", "")), str(row.get("phase", "")))
        if key in observed:
            raise ValueError("publication build phase timing rows are duplicated")
        try:
            nanos = int(row["nanos"])
            calls = int(row["calls"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("publication build phase timing value is invalid") from error
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
            raise ValueError("publication build artifact header differs from production")
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
        "storage_gets",
        "storage_puts",
        "storage_deletes",
        "storage_heads",
        "storage_lists",
        "storage_bytes_read",
        "storage_bytes_written",
    )
    parsed: dict[str, int] = {}
    for field in integer_fields:
        try:
            value = int(row[field])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"publication build storage field {field} is invalid") from error
        if value < 0:
            raise ValueError(f"publication build storage field {field} is negative")
        parsed[field] = value
    dataset = cell.get("dataset")
    profile = cell.get("index_profile")
    expected_rows = dataset.get("scale", {}).get("rows") if isinstance(dataset, dict) else None
    expected_cells = profile.get("logical_cells") if isinstance(profile, dict) else None
    if parsed["records"] != expected_rows or parsed["logical_cells"] != expected_cells:
        raise ValueError("publication build identity differs from its scheduled index")
    checksum = row.get("logical_cell_catalog_checksum", "")
    if len(checksum) != 64 or any(character not in "0123456789abcdef" for character in checksum):
        raise ValueError("publication build catalog checksum is invalid")
    if parsed["total_active_index_bytes"] <= 0:
        raise ValueError("publication build active index bytes must be positive")
    build_timings: dict[str, int] = {}
    for field in ("ingest_ms", "compaction_ms"):
        try:
            nanos = Decimal(row[field]) * 1_000_000
        except (KeyError, InvalidOperation) as error:
            raise ValueError(f"publication build timing field {field} is invalid") from error
        if nanos < 0 or nanos != nanos.to_integral_value():
            raise ValueError(f"publication build timing field {field} is invalid")
        build_timings[field.removesuffix("_ms") + "_ns"] = int(nanos)
    build_timings.update(
        {
            "compaction_bytes_read": parsed["compaction_bytes_read"],
            "compaction_bytes_written": parsed["compaction_bytes_written"],
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
    if isinstance(elapsed_ns, bool) or not isinstance(elapsed_ns, int) or elapsed_ns <= 0:
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
    runtime_write_metrics: dict[str, int],
    index_receipt: dict[str, object],
    runtime_attestation: dict[str, object],
) -> dict[str, object]:
    if isinstance(elapsed_ns, bool) or not isinstance(elapsed_ns, int) or elapsed_ns <= 0:
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
            "query_elapsed_ns",
        }
    )
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
    if frozenset(runtime_write_metrics) != frozenset(
        {"storage_puts", "storage_bytes_written"}
    ):
        raise ValueError("publication runtime write metric fields differ")
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
        "metrics": {
            **{
                key: value
                for key, value in query_metrics.items()
                if key != "query_elapsed_ns"
            },
            **resource_metrics,
            "storage_gets": query_metrics["storage_gets"],
            "storage_puts": runtime_write_metrics["storage_puts"],
            "storage_bytes_read": query_metrics["storage_bytes_read"],
            "storage_bytes_written": runtime_write_metrics["storage_bytes_written"],
            "throughput_milli_per_second": max(
                1,
                round(
                    queries
                    * 1_000_000_000_000
                    / query_metrics["query_elapsed_ns"]
                ),
            ),
        },
        "index_receipt_sha256": receipt_document_sha256(index_receipt),
        "clone_receipt_sha256": None,
        "runtime_attestation_sha256": runtime_attestation_sha256(
            runtime_attestation
        ),
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
    return {"publishable": True, "result": validated}


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
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--source-archive-sha256")
    parser.add_argument("--dataset-materialization-sha256")
    parser.add_argument("--attempt-id")
    parser.add_argument("--instance-identity")
    parser.add_argument("--object-roster", type=Path)
    parser.add_argument("--index-receipt", type=Path)
    parser.add_argument("--index-inventory", type=Path)
    parser.add_argument("--clone-receipt", type=Path)
    parser.add_argument("--clone-inventory", type=Path)
    parser.add_argument("--build-complete", type=Path)
    args = parser.parse_args()

    cell = read_protocol(args.protocol)
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
            raise ValueError("publication seal requires build completion and object roster")
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
        mode="publication" if publication else "smoke",
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
                raise ValueError("read-only publication runtime cannot use a mutable clone")
            authorized_runtime = authorize_publication_runtime(
                plan,
                receipt=receipt,
                cell=cell,
                source_archive_sha256=str(args.source_archive_sha256),
                dataset_materialization_sha256=str(args.dataset_materialization_sha256),
            )
        elif workload_kind == "write-update-delete-compact":
            if args.clone_receipt is None or args.clone_inventory is None:
                raise ValueError("lifecycle publication runtime requires clone authority")
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
        preflight = validate_runtime_attestation(
            collect_runtime_attestation(
                cell=cell,
                attempt_id=str(args.attempt_id),
                runtime=authorized_runtime,
                source_root=source_root,
            ),
            cell=cell,
            attempt_id=str(args.attempt_id),
        )
        if preflight["instance_id"] != args.instance_identity:
            raise ValueError("runtime EC2 identity differs from its scheduled instance")
        output, resources, elapsed_ns = execute_publication_phase(
            authorized_plan, "runtime"
        )
        runtime_attestation = validate_runtime_attestation(
            collect_runtime_attestation(
                cell=cell,
                attempt_id=str(args.attempt_id),
                runtime=authorized_runtime,
                source_root=source_root,
            ),
            cell=cell,
            attempt_id=str(args.attempt_id),
        )
        (args.workspace / "RUNTIME_ATTESTATION.json").write_bytes(
            canonical_json_bytes(runtime_attestation) + b"\n"
        )
        if workload_kind == "read-recall":
            samples = output / "bench_query_samples.csv"
            if not samples.is_file() or samples.stat().st_size == 0:
                raise ValueError("publication runtime emitted no query samples")
            with samples.open(newline="") as source:
                rows = list(csv.DictReader(source))
            metrics = summarize_query_samples(
                rows,
                cell=cell,
                arm=arm,
                expected_queries=int(plan["effective_queries"]),
            )
            runtime_writes = summarize_runtime_write_trace(output / "storage-access.csv")
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
                runtime_write_metrics=runtime_writes,
                index_receipt=receipt,
                runtime_attestation=runtime_attestation,
            )
        else:
            lifecycle = summarize_lifecycle_artifacts(
                output, expected_batch_size=int(arm["batch_size"])
            )
            storage_metrics = {
                field: lifecycle.pop(field)
                for field in (
                    "storage_gets",
                    "storage_puts",
                    "storage_bytes_read",
                    "storage_bytes_written",
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
