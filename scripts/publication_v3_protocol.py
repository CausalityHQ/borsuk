#!/usr/bin/env python3
"""Canonical identity and deterministic scheduling for Publication V3."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "campaign_id",
        "campaign_kind",
        "master_seed",
        "repetitions",
        "queries_per_repetition",
        "publish_p99",
        "source",
        "environment_contract",
        "prefixes",
        "index_profiles",
        "budget_contract",
        "datasets",
        "workloads",
        "systems",
    }
)
SCHEDULE_FIELDS = frozenset(
    {"schema_version", "campaign_id", "manifest_sha256", "cells"}
)
SCHEDULE_CELL_FIELDS = frozenset(
    {
        "schema_version",
        "campaign_id",
        "manifest_sha256",
        "cell_id",
        "repetition_id",
        "system",
        "index_profile",
        "query_seed",
        "queries_per_repetition",
        "dataset",
        "workload",
        "source",
        "environment_contract",
        "result_prefix",
        "index_prefix",
        "dataset_prefix",
        "cache_key",
    }
)
DATASET_FIELDS = frozenset(
    {"id", "kind", "scale", "dimensions", "metric", "source"}
)
WORKLOAD_FIELDS = frozenset({"id", "kind", "dataset_ids", "systems", "factors"})
ENVIRONMENT_FIELDS = frozenset(
    {
        "aws_account",
        "region",
        "architecture",
        "spot_default",
        "build_workers",
        "runtime_clients",
        "build_storage",
        "runtime_storage",
        "runtime_data_contract",
        "interruption_contract",
    }
)
PREFIX_FIELDS = frozenset({"result", "index", "dataset", "cache"})
S3_PREFIX = re.compile(r"s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/[^\s/](?:[^\s]*[^\s/])?")
SYSTEMS = ("borsuk", "amazon-s3-vectors", "faiss")
METRICS = frozenset({"cosine", "l2", "dot", "hamming", "jaccard"})
BORSUK_PQ_GLOBAL_SCAN_CODECS = frozenset({"pq-scan", "srht-pq-scan"})
BORSUK_TURBOQUANT_GLOBAL_SCAN_CODECS = frozenset(
    {"fast-turboquant-mse-scan", "fast-turboquant-scan"}
)
BORSUK_GLOBAL_SCAN_CODECS = BORSUK_PQ_GLOBAL_SCAN_CODECS | BORSUK_TURBOQUANT_GLOBAL_SCAN_CODECS
IDENTIFIER = re.compile(r"[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?")
HEX_40 = re.compile(r"[0-9a-f]{40}")
HEX_64 = re.compile(r"[0-9a-f]{64}")
AWS_ACCOUNT = re.compile(r"[0-9]{12}")
WORKLOAD_FACTOR_FIELDS = {
    "read-recall": frozenset(
        {
            "k",
            "leaf_page_budgets",
            "cache_states",
            "minimum_recall_ppm",
        }
    ),
    "write-update-delete-compact": frozenset(
        {
            "writers",
            "batch_sizes",
            "update_percent",
            "delete_percent",
            "minimum_lifecycle_accuracy_ppm",
        }
    ),
    "sparse-text-dense-hybrid": frozenset(
        {"modes", "candidate_depths", "fusion", "rrf_k", "minimum_ndcg_ppm"}
    ),
    "filtered-namespace-read": frozenset(
        {"selectivity_ppm", "namespace_counts", "k", "minimum_recall_ppm"}
    ),
    "late-interaction-read": frozenset(
        {
            "query_token_counts",
            "document_token_counts",
            "candidate_depths",
            "k",
            "minimum_ndcg_ppm",
        }
    ),
    "datatype-simd-matrix": frozenset(
        {"datatypes", "dimensions", "kernels", "k", "minimum_recall_ppm"}
    ),
    "storage-layout-audit": frozenset(
        {
            "bundle_target_mib",
            "row_group_target_mib",
            "max_active_levels",
            "require_multiple_data_bundles",
        }
    ),
}


def canonical_json_bytes(value: object) -> bytes:
    """Return the one canonical JSON representation used by V3 identities."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _exact_fields(
    value: dict[str, object], expected: frozenset[str], role: str
) -> None:
    actual = frozenset(value)
    if actual != expected:
        raise ValueError(
            f"{role} fields differ: "
            f"missing={sorted(expected - actual)} unknown={sorted(actual - expected)}"
        )


def _dict(value: object, role: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{role} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ValueError(f"{role} keys must be strings")
    return value


def _identifier(value: object, role: str) -> str:
    result = str(value)
    if not IDENTIFIER.fullmatch(result):
        raise ValueError(f"{role} is not a canonical identifier")
    return result


def _positive_int(value: object, role: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{role} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{role} must be a positive integer") from error
    if result <= 0 or result != value:
        raise ValueError(f"{role} must be a positive integer")
    return result


def _nonempty_string(value: object, role: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{role} must be a nonempty canonical string")
    if any(character in value for character in "\r\n\0"):
        raise ValueError(f"{role} contains a forbidden control character")
    return value


def _unique_strings(value: object, role: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{role} must be a nonempty list")
    result = [_identifier(item, f"{role} item") for item in value]
    if len(result) != len(set(result)):
        raise ValueError(f"{role} must not contain duplicates")
    return result


def _positive_int_list(value: object, role: str) -> list[int]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{role} must be a nonempty list")
    result = [_positive_int(item, f"{role} item") for item in value]
    if len(result) != len(set(result)):
        raise ValueError(f"{role} must not contain duplicates")
    return result


def _bounded_int_list(
    value: object, role: str, *, minimum: int, maximum: int
) -> list[int]:
    result = _positive_int_list(value, role)
    if any(item < minimum or item > maximum for item in result):
        raise ValueError(f"{role} items must be between {minimum} and {maximum}")
    return result


def _quality_ppm(value: object, role: str) -> int:
    result = _positive_int(value, role)
    if result > 1_000_000:
        raise ValueError(f"{role} must not exceed 1,000,000")
    return result


def _leaf_page_budgets(value: object) -> list[int]:
    result = _positive_int_list(value, "leaf page budgets")
    if any(item not in {4, 8, 16, 32, 64} for item in result):
        raise ValueError("leaf page budget must be 4, 8, 16, 32, or 64")
    return result


def _validate_workload_factors(kind: str, value: object) -> dict[str, object]:
    factors = _dict(value, f"{kind} factors")
    expected = WORKLOAD_FACTOR_FIELDS.get(kind)
    if expected is None:
        raise ValueError(f"unsupported workload kind {kind!r}")
    _exact_fields(factors, expected, f"{kind} factors")
    if kind == "read-recall":
        return {
            "k": _positive_int_list(factors["k"], "read k"),
            "leaf_page_budgets": _leaf_page_budgets(factors["leaf_page_budgets"]),
            "cache_states": _unique_strings(
                factors["cache_states"], "cache states"
            ),
            "minimum_recall_ppm": _quality_ppm(
                factors["minimum_recall_ppm"], "minimum recall ppm"
            ),
        }
    if kind == "write-update-delete-compact":
        return {
            "writers": _positive_int_list(factors["writers"], "writers"),
            "batch_sizes": _positive_int_list(
                factors["batch_sizes"], "batch sizes"
            ),
            "update_percent": _bounded_int_list(
                factors["update_percent"], "update percent", minimum=1, maximum=100
            ),
            "delete_percent": _bounded_int_list(
                factors["delete_percent"], "delete percent", minimum=1, maximum=100
            ),
            "minimum_lifecycle_accuracy_ppm": _quality_ppm(
                factors["minimum_lifecycle_accuracy_ppm"],
                "minimum lifecycle accuracy ppm",
            ),
        }
    if kind == "sparse-text-dense-hybrid":
        fusion = _identifier(factors["fusion"], "hybrid fusion")
        return {
            "modes": _unique_strings(factors["modes"], "hybrid modes"),
            "candidate_depths": _positive_int_list(
                factors["candidate_depths"], "hybrid candidate depths"
            ),
            "fusion": fusion,
            "rrf_k": _positive_int(factors["rrf_k"], "hybrid rrf_k"),
            "minimum_ndcg_ppm": _quality_ppm(
                factors["minimum_ndcg_ppm"], "minimum hybrid nDCG ppm"
            ),
        }
    if kind == "filtered-namespace-read":
        return {
            "selectivity_ppm": _bounded_int_list(
                factors["selectivity_ppm"],
                "filter selectivity ppm",
                minimum=1,
                maximum=1_000_000,
            ),
            "namespace_counts": _positive_int_list(
                factors["namespace_counts"], "namespace counts"
            ),
            "k": _positive_int_list(factors["k"], "filtered k"),
            "minimum_recall_ppm": _quality_ppm(
                factors["minimum_recall_ppm"], "minimum filtered recall ppm"
            ),
        }
    if kind == "late-interaction-read":
        return {
            "query_token_counts": _positive_int_list(
                factors["query_token_counts"], "query token counts"
            ),
            "document_token_counts": _positive_int_list(
                factors["document_token_counts"], "document token counts"
            ),
            "candidate_depths": _positive_int_list(
                factors["candidate_depths"], "late candidate depths"
            ),
            "k": _positive_int_list(factors["k"], "late k"),
            "minimum_ndcg_ppm": _quality_ppm(
                factors["minimum_ndcg_ppm"], "minimum late nDCG ppm"
            ),
        }
    if kind == "datatype-simd-matrix":
        return {
            "datatypes": _unique_strings(factors["datatypes"], "datatypes"),
            "dimensions": _positive_int_list(factors["dimensions"], "dimensions"),
            "kernels": _unique_strings(factors["kernels"], "kernels"),
            "k": _positive_int_list(factors["k"], "datatype k"),
            "minimum_recall_ppm": _quality_ppm(
                factors["minimum_recall_ppm"], "minimum datatype recall ppm"
            ),
        }
    if kind == "storage-layout-audit":
        multiple = factors["require_multiple_data_bundles"]
        if not isinstance(multiple, bool):
            raise ValueError("require_multiple_data_bundles must be a boolean")
        return {
            "bundle_target_mib": _positive_int(
                factors["bundle_target_mib"], "bundle target MiB"
            ),
            "row_group_target_mib": _positive_int(
                factors["row_group_target_mib"], "row group target MiB"
            ),
            "max_active_levels": _positive_int(
                factors["max_active_levels"], "max active levels"
            ),
            "require_multiple_data_bundles": multiple,
        }
    raise AssertionError("validated workload kind has no factor decoder")


def _validate_index_profile(system: str, value: object) -> dict[str, object]:
    profile = _dict(value, f"{system} index profile")
    engine = profile.get("engine")
    if system == "borsuk":
        common_fields = frozenset(
            {
                "engine",
                "logical_cells",
                "minimum_rows_per_logical_cell",
                "training_rows_per_cell",
                "training_iterations",
                "routing",
                "hnsw_m",
                "hnsw_ef_construction",
                "global_scan_codec",
                "bundle_target_mib",
                "row_group_target_mib",
                "max_active_levels",
            }
        )
        global_scan_codec = _identifier(
            profile.get("global_scan_codec"), "borsuk global scan codec"
        )
        if global_scan_codec not in BORSUK_GLOBAL_SCAN_CODECS:
            raise ValueError("borsuk global scan codec is unsupported")
        turboquant = global_scan_codec in BORSUK_TURBOQUANT_GLOBAL_SCAN_CODECS
        expected = common_fields | (
            frozenset(
                {
                    "turboquant_bits",
                    "turboquant_qjl_bits",
                    "turboquant_shards",
                }
            )
            if turboquant
            else frozenset({"code_bytes"})
        )
        _exact_fields(profile, expected, "borsuk index profile")
        if engine != "borsuk-v20":
            raise ValueError("borsuk index engine must be borsuk-v20")
        codec_parameters: dict[str, int]
        if turboquant:
            bits = _positive_int(profile["turboquant_bits"], "borsuk TurboQuant bits")
            if bits > 8 or (global_scan_codec == "fast-turboquant-scan" and bits < 2):
                raise ValueError("borsuk TurboQuant bits are invalid for the selected codec")
            qjl_bits = profile["turboquant_qjl_bits"]
            if (
                isinstance(qjl_bits, bool)
                or not isinstance(qjl_bits, int)
                or qjl_bits < 0
                or qjl_bits > 2**32 - 1
            ):
                raise ValueError("borsuk TurboQuant QJL bits must fit u32")
            shards = _positive_int(
                profile["turboquant_shards"], "borsuk TurboQuant shards"
            )
            if qjl_bits != 0:
                raise ValueError("borsuk TurboQuant codec does not accept a QJL override")
            if global_scan_codec == "fast-turboquant-scan" and shards != 1:
                raise ValueError("borsuk production TurboQuant codec requires one shard")
            codec_parameters = {
                "turboquant_bits": bits,
                "turboquant_qjl_bits": qjl_bits,
                "turboquant_shards": shards,
            }
        else:
            code_bytes = _positive_int(profile["code_bytes"], "borsuk code bytes")
            if code_bytes & (code_bytes - 1):
                raise ValueError("borsuk code bytes must be a power of two")
            codec_parameters = {"code_bytes": code_bytes}
        validated = {
            "engine": "borsuk-v20",
            "logical_cells": _positive_int(profile["logical_cells"], "logical cells"),
            "minimum_rows_per_logical_cell": _positive_int(
                profile["minimum_rows_per_logical_cell"],
                "minimum rows per logical cell",
            ),
            "training_rows_per_cell": _positive_int(
                profile["training_rows_per_cell"], "training rows per cell"
            ),
            "training_iterations": _positive_int(
                profile["training_iterations"], "training iterations"
            ),
            "routing": _identifier(profile["routing"], "borsuk routing"),
            "hnsw_m": _positive_int(profile["hnsw_m"], "borsuk hnsw_m"),
            "hnsw_ef_construction": _positive_int(
                profile["hnsw_ef_construction"], "borsuk hnsw ef construction"
            ),
            "global_scan_codec": global_scan_codec,
            "bundle_target_mib": _positive_int(
                profile["bundle_target_mib"], "borsuk bundle target MiB"
            ),
            "row_group_target_mib": _positive_int(
                profile["row_group_target_mib"], "borsuk row group target MiB"
            ),
            "max_active_levels": _positive_int(
                profile["max_active_levels"], "borsuk max active levels"
            ),
        }
        validated.update(codec_parameters)
        return validated
    if system == "faiss":
        _exact_fields(
            profile,
            frozenset(
                {
                    "engine",
                    "nlist",
                    "training_rows",
                    "minimum_training_rows_per_centroid",
                }
            ),
            "faiss index profile",
        )
        if engine != "faiss-ivf-flat":
            raise ValueError("faiss index engine must be faiss-ivf-flat")
        return {
            "engine": "faiss-ivf-flat",
            "nlist": _positive_int(profile["nlist"], "faiss nlist"),
            "training_rows": _positive_int(
                profile["training_rows"], "faiss training rows"
            ),
            "minimum_training_rows_per_centroid": _positive_int(
                profile["minimum_training_rows_per_centroid"],
                "minimum Faiss training rows per centroid",
            ),
        }
    if system == "amazon-s3-vectors":
        _exact_fields(profile, frozenset({"engine"}), "S3 Vectors index profile")
        if engine != "amazon-s3-vectors-managed":
            raise ValueError(
                "amazon-s3-vectors index engine must be amazon-s3-vectors-managed"
            )
        return {"engine": "amazon-s3-vectors-managed"}
    raise ValueError(f"unsupported index-profile system {system!r}")


def _validate_index_profiles(value: object) -> dict[str, object]:
    profiles = _dict(value, "index_profiles")
    _exact_fields(profiles, frozenset(SYSTEMS), "index_profiles")
    return {
        system: _validate_index_profile(system, profiles[system])
        for system in SYSTEMS
    }


def _largest_power_of_two_not_exceeding(value: int) -> int:
    if value < 1:
        raise ValueError("index profile has no feasible partition count")
    return 1 << (value.bit_length() - 1)


def _effective_index_profile(
    profile: dict[str, object],
    dataset: dict[str, object],
    workload: dict[str, object],
) -> dict[str, object]:
    scale = _dict(dataset["scale"], "dataset scale")
    if scale.get("state") != "exact":
        raise ValueError(
            f"dataset {dataset['id']} needs an exact row count for index sizing"
        )
    rows = int(scale["rows"])
    result = copy.deepcopy(profile)
    if profile["engine"] == "borsuk-v20":
        feasible = rows // int(profile["minimum_rows_per_logical_cell"])
        result["logical_cells"] = _largest_power_of_two_not_exceeding(
            min(int(profile["logical_cells"]), feasible)
        )
        if workload["kind"] == "storage-layout-audit":
            factors = _dict(workload["factors"], "storage-layout factors")
            for field in (
                "bundle_target_mib",
                "row_group_target_mib",
                "max_active_levels",
            ):
                result[field] = factors[field]
    elif profile["engine"] == "faiss-ivf-flat":
        training_rows = min(int(profile["training_rows"]), rows)
        feasible = training_rows // int(
            profile["minimum_training_rows_per_centroid"]
        )
        result["training_rows"] = training_rows
        result["nlist"] = _largest_power_of_two_not_exceeding(
            min(int(profile["nlist"]), feasible)
        )
    return result


def _validate_budget(value: object) -> dict[str, object]:
    budget = _dict(value, "budget_contract")
    _exact_fields(
        budget,
        frozenset(
            {
                "max_campaign_usd_micros",
                "max_cell_seconds",
                "max_index_build_seconds",
                "terminate_on_terminal_marker",
            }
        ),
        "budget_contract",
    )
    if budget["terminate_on_terminal_marker"] is not True:
        raise ValueError("budget contract must terminate compute on terminal markers")
    return {
        "max_campaign_usd_micros": _positive_int(
            budget["max_campaign_usd_micros"], "campaign budget micros"
        ),
        "max_cell_seconds": _positive_int(
            budget["max_cell_seconds"], "maximum cell seconds"
        ),
        "max_index_build_seconds": _positive_int(
            budget["max_index_build_seconds"], "maximum index-build seconds"
        ),
        "terminate_on_terminal_marker": True,
    }


def _validate_campaign_source(value: object) -> dict[str, object]:
    source = _dict(value, "source")
    state = source.get("state")
    if state == "unfrozen":
        _exact_fields(source, frozenset({"state"}), "unfrozen source")
    elif state == "frozen":
        _exact_fields(
            source,
            frozenset(
                {
                    "state",
                    "git_commit",
                    "archive_sha256",
                    "cargo_lock_sha256",
                    "python_lock_sha256",
                    "node_lock_sha256",
                }
            ),
            "frozen source",
        )
        if not HEX_40.fullmatch(str(source["git_commit"])):
            raise ValueError("frozen source git_commit must be lowercase SHA-1")
        for field in (
            "archive_sha256",
            "cargo_lock_sha256",
            "python_lock_sha256",
            "node_lock_sha256",
        ):
            if not HEX_64.fullmatch(str(source[field])):
                raise ValueError(f"frozen source {field} must be lowercase SHA-256")
    else:
        raise ValueError("source state must be unfrozen or frozen")
    return copy.deepcopy(source)


def _validate_dataset_source(value: object) -> dict[str, object]:
    source = _dict(value, "dataset source")
    state = source.get("state")
    if state == "unstaged":
        _exact_fields(
            source,
            frozenset({"state", "expected_source", "license"}),
            "unstaged dataset source",
        )
        _nonempty_string(source["expected_source"], "dataset expected_source")
        _nonempty_string(source["license"], "dataset license")
    elif state == "staged":
        _exact_fields(
            source,
            frozenset({"state", "url", "sha256", "license"}),
            "staged dataset source",
        )
        _nonempty_string(source["url"], "dataset url")
        _nonempty_string(source["license"], "dataset license")
        if not HEX_64.fullmatch(str(source["sha256"])):
            raise ValueError("staged dataset sha256 must be lowercase SHA-256")
    elif state == "generated":
        _exact_fields(
            source,
            frozenset({"state", "generator", "seed"}),
            "generated dataset source",
        )
        generator = _identifier(source["generator"], "dataset generator")
        seed = _positive_int(source["seed"], "dataset seed")
    else:
        raise ValueError("dataset source state is invalid")
    if state == "generated":
        return {"state": "generated", "generator": generator, "seed": seed}
    return copy.deepcopy(source)


def _validate_scale(value: object) -> dict[str, object]:
    scale = _dict(value, "dataset scale")
    _exact_fields(scale, frozenset({"state", "rows"}), "exact scale")
    if scale["state"] != "exact":
        raise ValueError("publication dataset scale must be exact")
    return {
        "state": "exact",
        "rows": _positive_int(scale["rows"], "dataset rows"),
    }


def _validate_dataset(value: object) -> dict[str, object]:
    dataset = _dict(value, "dataset")
    _exact_fields(dataset, DATASET_FIELDS, "dataset")
    dataset_id = _identifier(dataset["id"], "dataset id")
    kind = _identifier(dataset["kind"], "dataset kind")
    dimensions = _positive_int(dataset["dimensions"], "dataset dimensions")
    metric = str(dataset["metric"])
    if metric not in METRICS:
        raise ValueError(f"dataset {dataset_id} has unsupported metric {metric!r}")
    return {
        "id": dataset_id,
        "kind": kind,
        "scale": _validate_scale(dataset["scale"]),
        "dimensions": dimensions,
        "metric": metric,
        "source": _validate_dataset_source(dataset["source"]),
    }


def _validate_workload(value: object) -> dict[str, object]:
    workload = _dict(value, "workload")
    _exact_fields(workload, WORKLOAD_FIELDS, "workload")
    kind = _identifier(workload["kind"], "workload kind")
    return {
        "id": _identifier(workload["id"], "workload id"),
        "kind": kind,
        "dataset_ids": _unique_strings(workload["dataset_ids"], "dataset_ids"),
        "systems": _unique_strings(workload["systems"], "workload systems"),
        "factors": _validate_workload_factors(kind, workload["factors"]),
    }


def _validate_environment(value: object) -> dict[str, object]:
    environment = _dict(value, "environment_contract")
    _exact_fields(environment, ENVIRONMENT_FIELDS, "environment_contract")
    account = str(environment["aws_account"])
    if not AWS_ACCOUNT.fullmatch(account):
        raise ValueError("environment aws_account must contain 12 digits")
    region = _nonempty_string(environment["region"], "environment region")
    architecture = str(environment["architecture"])
    if architecture not in {"aarch64", "x86_64"}:
        raise ValueError("environment architecture is unsupported")
    if not isinstance(environment["spot_default"], bool):
        raise ValueError("environment spot_default must be a boolean")
    resource_fields = frozenset({"instance_type", "vcpus", "memory_mib"})
    query_resource_fields = resource_fields | frozenset(
        {"resident_limit_mib", "disk_cache_limit_mib"}
    )

    def normalize_resources(raw: object, role: str, *, query: bool) -> dict[str, object]:
        resources = _dict(raw, role)
        _exact_fields(resources, query_resource_fields if query else resource_fields, role)
        result = {
            "instance_type": _identifier(resources["instance_type"], f"{role} instance type"),
            "vcpus": _positive_int(resources["vcpus"], f"{role} vCPUs"),
            "memory_mib": _positive_int(resources["memory_mib"], f"{role} memory MiB"),
        }
        if query:
            result["resident_limit_mib"] = _positive_int(
                resources["resident_limit_mib"], f"{role} resident limit MiB"
            )
            result["disk_cache_limit_mib"] = _positive_int(
                resources["disk_cache_limit_mib"], f"{role} disk cache limit MiB"
            )
        return result

    workers = _dict(environment["build_workers"], "build_workers")
    clients = _dict(environment["runtime_clients"], "runtime_clients")
    _exact_fields(workers, frozenset(SYSTEMS), "build_workers")
    _exact_fields(clients, frozenset(SYSTEMS), "runtime_clients")
    normalized_workers = {
        system: normalize_resources(workers[system], f"{system} build worker", query=False)
        for system in SYSTEMS
    }
    normalized_clients = {
        system: normalize_resources(clients[system], f"{system} query client", query=True)
        for system in SYSTEMS
    }
    query_caps = {
        "borsuk": (8_192, 2_048),
        "amazon-s3-vectors": (8_192, 2_048),
        "faiss": (16_384, 12_288),
    }
    for system, (memory_cap, resident_cap) in query_caps.items():
        client = normalized_clients[system]
        if client["memory_mib"] > memory_cap:
            raise ValueError(f"{system} runtime client memory exceeds its small-client cap")
        if client["resident_limit_mib"] > resident_cap or client["resident_limit_mib"] >= client["memory_mib"]:
            raise ValueError(f"{system} runtime resident limit exceeds its cap")
        if client["disk_cache_limit_mib"] > 1_024:
            raise ValueError(f"{system} runtime cache exceeds its 1 GiB cap")

    def normalize_storage(raw: object, role: str) -> dict[str, object]:
        storage = _dict(raw, role)
        _exact_fields(
            storage,
            frozenset({"volume_type", "volume_size_gib", "iops", "throughput_mib_s"}),
            role,
        )
        return {
            "volume_type": _identifier(storage["volume_type"], f"{role} volume type"),
            "volume_size_gib": _positive_int(storage["volume_size_gib"], f"{role} size GiB"),
            "iops": _positive_int(storage["iops"], f"{role} IOPS"),
            "throughput_mib_s": _positive_int(storage["throughput_mib_s"], f"{role} throughput MiB/s"),
        }

    normalized_build_storage = normalize_storage(environment["build_storage"], "build storage")
    normalized_runtime_storage = normalize_storage(environment["runtime_storage"], "runtime storage")
    if (
        normalized_runtime_storage["volume_size_gib"] > 32
        or normalized_runtime_storage["iops"] > 3_000
        or normalized_runtime_storage["throughput_mib_s"] > 125
    ):
        raise ValueError("runtime storage exceeds the small-client cap")
    runtime_data_contract = _dict(environment["runtime_data_contract"], "runtime_data_contract")
    expected_runtime_data_contract = {
        "index_location": "s3",
        "dataset_location": "s3",
        "allow_local_corpus": False,
        "allow_local_index": False,
    }
    if runtime_data_contract != expected_runtime_data_contract:
        raise ValueError("runtime data contract must be S3-only")
    interruption = _dict(
        environment["interruption_contract"], "interruption_contract"
    )
    expected_interruption = {
        "measurement_unit": "factor-arm",
        "sync_terminal_repetitions": True,
        "discard_interrupted_measurement_cell": True,
    }
    if interruption != expected_interruption:
        raise ValueError(
            "interruption_contract must checkpoint factor arms, sync terminal "
            "repetitions, and discard interrupted measurement cells"
        )
    return {
        "aws_account": account,
        "region": region,
        "architecture": architecture,
        "spot_default": environment["spot_default"],
        "build_workers": normalized_workers,
        "runtime_clients": normalized_clients,
        "build_storage": normalized_build_storage,
        "runtime_storage": normalized_runtime_storage,
        "runtime_data_contract": expected_runtime_data_contract,
        "interruption_contract": expected_interruption,
    }


def _validate_prefixes(value: object) -> dict[str, str]:
    prefixes = _dict(value, "prefixes")
    _exact_fields(prefixes, PREFIX_FIELDS, "prefixes")
    result = {
        field: _nonempty_string(prefixes[field], f"{field} prefix")
        for field in PREFIX_FIELDS
    }
    if len(set(result.values())) != len(result):
        raise ValueError("publication prefixes must be distinct")
    for field in ("result", "index", "dataset"):
        if S3_PREFIX.fullmatch(result[field]) is None:
            raise ValueError(f"{field} prefix must be an S3 URI without a trailing slash")
    for left_name, left in result.items():
        for right_name, right in result.items():
            if left_name != right_name and (
                left.rstrip("/") + "/"
            ).startswith(right.rstrip("/") + "/"):
                raise ValueError("publication prefixes must not contain one another")
    return result


def validate_manifest(value: dict[str, object]) -> dict[str, object]:
    manifest = _dict(value, "manifest")
    _exact_fields(manifest, MANIFEST_FIELDS, "manifest")
    if manifest["schema_version"] != 3:
        raise ValueError("publication manifest schema_version must be 3")
    campaign_id = _identifier(manifest["campaign_id"], "campaign_id")
    if not campaign_id.startswith("publication-v3-"):
        raise ValueError("campaign_id must start with publication-v3-")
    if manifest["campaign_kind"] != "publication":
        raise ValueError("campaign_kind must be publication")
    master_seed = _positive_int(manifest["master_seed"], "master_seed")
    repetitions = _positive_int(manifest["repetitions"], "repetitions")
    if repetitions != 5:
        raise ValueError("Publication V3 requires exactly five repetitions")
    queries = _positive_int(
        manifest["queries_per_repetition"], "queries_per_repetition"
    )
    if queries != 1000:
        raise ValueError("Publication V3 requires exactly 1,000 queries")
    if manifest["publish_p99"] is not True:
        raise ValueError("Publication V3 must publish p99")
    systems = _unique_strings(manifest["systems"], "systems")
    if systems != list(SYSTEMS):
        raise ValueError("systems must be borsuk, amazon-s3-vectors, and faiss")
    datasets_value = manifest["datasets"]
    workloads_value = manifest["workloads"]
    if not isinstance(datasets_value, list) or not datasets_value:
        raise ValueError("datasets must be a nonempty list")
    if not isinstance(workloads_value, list) or not workloads_value:
        raise ValueError("workloads must be a nonempty list")
    datasets = [_validate_dataset(item) for item in datasets_value]
    workloads = [_validate_workload(item) for item in workloads_value]
    dataset_ids = [item["id"] for item in datasets]
    workload_ids = [item["id"] for item in workloads]
    if len(dataset_ids) != len(set(dataset_ids)):
        raise ValueError("dataset identifiers must be unique")
    if len(workload_ids) != len(set(workload_ids)):
        raise ValueError("workload identifiers must be unique")
    known_datasets = set(dataset_ids)
    datasets_by_id = {dataset["id"]: dataset for dataset in datasets}
    known_systems = set(systems)
    for workload in workloads:
        missing_datasets = set(workload["dataset_ids"]) - known_datasets
        missing_systems = set(workload["systems"]) - known_systems
        if missing_datasets:
            raise ValueError(
                f"workload {workload['id']} names unknown datasets {sorted(missing_datasets)}"
            )
        if missing_systems:
            raise ValueError(
                f"workload {workload['id']} names unknown systems {sorted(missing_systems)}"
            )
        if workload["kind"] == "datatype-simd-matrix":
            datatypes = set(workload["factors"]["datatypes"])
            selected_dimensions = {
                datasets_by_id[dataset_id]["dimensions"]
                for dataset_id in workload["dataset_ids"]
            }
            missing_dimensions = (
                set(workload["factors"]["dimensions"]) - selected_dimensions
            )
            if missing_dimensions:
                raise ValueError(
                    f"workload {workload['id']} has dimensions without datasets: "
                    f"{sorted(missing_dimensions)}"
                )
            for dataset_id in workload["dataset_ids"]:
                binary_dataset = datasets_by_id[dataset_id]["metric"] in {
                    "hamming",
                    "jaccard",
                }
                compatible = "binary" in datatypes if binary_dataset else bool(
                    datatypes - {"binary"}
                )
                if not compatible:
                    raise ValueError(
                        f"workload {workload['id']} has no compatible datatype "
                        f"for dataset {dataset_id}"
                    )
    return {
        "schema_version": 3,
        "campaign_id": campaign_id,
        "campaign_kind": "publication",
        "master_seed": master_seed,
        "repetitions": repetitions,
        "queries_per_repetition": queries,
        "publish_p99": True,
        "source": _validate_campaign_source(manifest["source"]),
        "environment_contract": _validate_environment(
            manifest["environment_contract"]
        ),
        "prefixes": _validate_prefixes(manifest["prefixes"]),
        "index_profiles": _validate_index_profiles(manifest["index_profiles"]),
        "budget_contract": _validate_budget(manifest["budget_contract"]),
        "datasets": datasets,
        "workloads": workloads,
        "systems": systems,
    }


def _cell_base(
    *,
    manifest: dict[str, object],
    manifest_sha256: str,
    repetition_id: str,
    query_seed: int,
    dataset: dict[str, object],
    workload: dict[str, object],
    system: str,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "campaign_id": manifest["campaign_id"],
        "manifest_sha256": manifest_sha256,
        "repetition_id": repetition_id,
        "system": system,
        "index_profile": _effective_index_profile(
            manifest["index_profiles"][system], dataset, workload
        ),
        "query_seed": query_seed,
        "queries_per_repetition": manifest["queries_per_repetition"],
        "dataset": dataset,
        "workload": workload,
        "source": manifest["source"],
        "environment_contract": manifest["environment_contract"],
    }


def _project_workload_for_dataset(
    workload: dict[str, object], dataset: dict[str, object]
) -> dict[str, object]:
    projected = copy.deepcopy(workload)
    projected["dataset_ids"] = [dataset["id"]]
    if workload["kind"] == "datatype-simd-matrix":
        projected["factors"]["dimensions"] = [dataset["dimensions"]]
        if dataset["metric"] in {"hamming", "jaccard"}:
            projected["factors"]["datatypes"] = [
                datatype
                for datatype in projected["factors"]["datatypes"]
                if datatype == "binary"
            ]
        else:
            projected["factors"]["datatypes"] = [
                datatype
                for datatype in projected["factors"]["datatypes"]
                if datatype != "binary"
            ]
    return projected


def index_id(base: dict[str, object]) -> str:
    workload = _dict(base["workload"], "index workload")
    index_base = {
        "schema_version": base["schema_version"],
        "campaign_id": base["campaign_id"],
        "system": base["system"],
        "index_profile": base["index_profile"],
        "dataset": base["dataset"],
        "source": base["source"],
        "workload_kind": workload["kind"],
    }
    digest = hashlib.sha256(canonical_json_bytes(index_base)).hexdigest()
    return f"index-{digest[:24]}"


def _cell_id(base: dict[str, object]) -> str:
    identity_base = {
        key: value for key, value in base.items() if key != "manifest_sha256"
    }
    digest = hashlib.sha256(canonical_json_bytes(identity_base)).hexdigest()
    return f"{base['repetition_id']}-{digest[:24]}"


def _finish_cell(base: dict[str, object], prefixes: dict[str, str]) -> dict[str, object]:
    cell_id = _cell_id(base)
    dataset = _dict(base["dataset"], "cell dataset")
    return {
        **base,
        "cell_id": cell_id,
        "result_prefix": f"{prefixes['result'].rstrip('/')}/{cell_id}",
        "index_prefix": f"{prefixes['index'].rstrip('/')}/{index_id(base)}",
        "dataset_prefix": (
            f"{prefixes['dataset'].rstrip('/')}/{dataset['id']}"
        ),
        "cache_key": f"{prefixes['cache']}-{cell_id}",
    }


def build_schedule_document(manifest: dict[str, object]) -> dict[str, object]:
    manifest = validate_manifest(manifest)
    manifest_sha256 = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    datasets = {item["id"]: item for item in manifest["datasets"]}
    cells: list[dict[str, object]] = []
    for repetition in range(1, int(manifest["repetitions"]) + 1):
        repetition_id = f"r{repetition:02d}"
        query_seed = int(manifest["master_seed"]) + repetition
        for workload in manifest["workloads"]:
            for dataset_id in workload["dataset_ids"]:
                for system in workload["systems"]:
                    base = _cell_base(
                        manifest=manifest,
                        manifest_sha256=manifest_sha256,
                        repetition_id=repetition_id,
                        query_seed=query_seed,
                        dataset=datasets[dataset_id],
                        workload=_project_workload_for_dataset(
                            workload, datasets[dataset_id]
                        ),
                        system=system,
                    )
                    cells.append(_finish_cell(base, manifest["prefixes"]))
    cells.sort(key=lambda cell: str(cell["cell_id"]))
    return {
        "schema_version": 1,
        "campaign_id": manifest["campaign_id"],
        "manifest_sha256": manifest_sha256,
        "cells": cells,
    }


def validate_schedule_cell(value: dict[str, object]) -> dict[str, object]:
    cell = _dict(value, "schedule cell")
    _exact_fields(cell, SCHEDULE_CELL_FIELDS, "schedule cell")
    if cell["schema_version"] != 1:
        raise ValueError("schedule cell schema_version must be 1")
    campaign_id = _identifier(cell["campaign_id"], "cell campaign_id")
    if not campaign_id.startswith("publication-v3-"):
        raise ValueError("cell campaign_id must start with publication-v3-")
    manifest_sha256 = str(cell["manifest_sha256"])
    if not HEX_64.fullmatch(manifest_sha256):
        raise ValueError("cell manifest_sha256 must be lowercase SHA-256")
    repetition_id = str(cell["repetition_id"])
    if not re.fullmatch(r"r0[1-5]", repetition_id):
        raise ValueError("cell repetition_id must be r01 through r05")
    system = str(cell["system"])
    if system not in SYSTEMS:
        raise ValueError("cell system is unsupported")
    index_profile = _validate_index_profile(system, cell["index_profile"])
    query_seed = _positive_int(cell["query_seed"], "cell query_seed")
    queries_per_repetition = _positive_int(
        cell["queries_per_repetition"], "cell queries_per_repetition"
    )
    dataset = _validate_dataset(cell["dataset"])
    workload = _validate_workload(cell["workload"])
    if index_profile != _effective_index_profile(index_profile, dataset, workload):
        raise ValueError("schedule cell index profile is infeasible for dataset scale")
    if dataset["id"] not in workload["dataset_ids"] or system not in workload["systems"]:
        raise ValueError("cell dataset/system is not authorized by workload")
    if workload["dataset_ids"] != [dataset["id"]]:
        raise ValueError("schedule cell workload must select exactly its dataset")
    if workload["kind"] == "datatype-simd-matrix":
        if workload["factors"]["dimensions"] != [dataset["dimensions"]]:
            raise ValueError("datatype schedule cell dimension differs from dataset")
        expected_binary = dataset["metric"] in {"hamming", "jaccard"}
        if (workload["factors"]["datatypes"] == ["binary"]) != expected_binary:
            raise ValueError("datatype schedule cell has incompatible datatype arms")
    source = _validate_campaign_source(cell["source"])
    environment = _validate_environment(cell["environment_contract"])
    for field in ("result_prefix", "index_prefix", "dataset_prefix", "cache_key"):
        _nonempty_string(cell[field], f"cell {field}")
    for field in ("result_prefix", "index_prefix", "dataset_prefix"):
        if S3_PREFIX.fullmatch(str(cell[field])) is None:
            raise ValueError(f"cell {field} must be an S3 URI without a trailing slash")
    if len(
        {
            str(cell["result_prefix"]),
            str(cell["index_prefix"]),
            str(cell["dataset_prefix"]),
        }
    ) != 3:
        raise ValueError("cell object prefixes must be distinct")
    base = {
        "schema_version": 1,
        "campaign_id": campaign_id,
        "manifest_sha256": manifest_sha256,
        "repetition_id": repetition_id,
        "system": system,
        "index_profile": index_profile,
        "query_seed": query_seed,
        "queries_per_repetition": queries_per_repetition,
        "dataset": dataset,
        "workload": workload,
        "source": source,
        "environment_contract": environment,
    }
    expected_cell_id = _cell_id(base)
    if cell["cell_id"] != expected_cell_id:
        raise ValueError("schedule cell identity or derived locations differ")
    expected_suffixes = {
        "result_prefix": f"/{expected_cell_id}",
        "index_prefix": f"/{index_id(base)}",
        "dataset_prefix": f"/{dataset['id']}",
        "cache_key": f"-{expected_cell_id}",
    }
    if any(
        not str(cell[field]).endswith(suffix)
        for field, suffix in expected_suffixes.items()
    ):
        raise ValueError("schedule cell identity or derived locations differ")
    return copy.deepcopy(cell)


def validate_schedule_document(value: object) -> dict[str, object]:
    schedule = _dict(value, "schedule")
    _exact_fields(schedule, SCHEDULE_FIELDS, "schedule")
    if schedule["schema_version"] != 1:
        raise ValueError("schedule schema_version must be 1")
    campaign_id = _identifier(schedule["campaign_id"], "schedule campaign_id")
    manifest_sha256 = str(schedule["manifest_sha256"])
    if not HEX_64.fullmatch(manifest_sha256):
        raise ValueError("schedule manifest_sha256 must be lowercase SHA-256")
    cells_value = schedule["cells"]
    if not isinstance(cells_value, list) or not cells_value:
        raise ValueError("schedule cells must be a nonempty list")
    cells = [validate_schedule_cell(item) for item in cells_value]
    cell_ids = [str(cell["cell_id"]) for cell in cells]
    if cell_ids != sorted(cell_ids) or len(cell_ids) != len(set(cell_ids)):
        raise ValueError("schedule cells must have sorted unique identifiers")
    if any(cell["campaign_id"] != campaign_id for cell in cells):
        raise ValueError("schedule cells contain mixed campaign identities")
    if any(cell["manifest_sha256"] != manifest_sha256 for cell in cells):
        raise ValueError("schedule cells contain mixed manifest identities")
    return {
        "schema_version": 1,
        "campaign_id": campaign_id,
        "manifest_sha256": manifest_sha256,
        "cells": cells,
    }


def validate_schedule_for_manifest(
    schedule: object, manifest: dict[str, object]
) -> dict[str, object]:
    """Require the schedule to be the exact canonical expansion of the manifest."""

    try:
        validated = validate_schedule_document(schedule)
    except ValueError as error:
        raise ValueError("schedule differs from the canonical schedule for manifest") from error
    expected = build_schedule_document(manifest)
    if canonical_json_bytes(validated) != canonical_json_bytes(expected):
        raise ValueError("schedule differs from the canonical schedule for manifest")
    return validated


def _read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return _dict(value, str(path))


def _write_canonical(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def _read_canonical_json(path: Path, maximum_bytes: int) -> dict[str, object]:
    payload = path.read_bytes()
    if not payload or len(payload) > maximum_bytes:
        raise ValueError(f"{path} is empty or exceeds its byte bound")
    try:
        value = _dict(json.loads(payload.decode("utf-8")), str(path))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path} is not canonical UTF-8 JSON") from error
    if payload != canonical_json_bytes(value) + b"\n":
        raise ValueError(f"{path} is not canonical JSON")
    return value


def write_protocol(path: Path, cell: dict[str, object]) -> None:
    _write_canonical(path, validate_schedule_cell(cell))


def read_protocol(path: Path) -> dict[str, object]:
    return validate_schedule_cell(_read_canonical_json(path, 256 * 1024))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_structural_replay(
    manifest: dict[str, object], source_archive: Path, output: Path
) -> None:
    if output.exists():
        raise ValueError("replay output must not already exist")
    source_sha256 = _sha256_file(source_archive)
    if source_sha256 != manifest["source"].get("archive_sha256"):
        raise ValueError("source archive checksum differs from manifest")
    schedule = build_schedule_document(manifest)
    output.mkdir(parents=True)
    _write_canonical(output / "manifest.json", manifest)
    _write_canonical(output / "schedule.json", schedule)
    _write_canonical(output / "environment.json", manifest["environment_contract"])
    shutil.copyfile(source_archive, output / "source-archive.tar.gz")
    for cell in schedule["cells"]:
        cell_root = output / "cells" / str(cell["cell_id"])
        protocol_path = cell_root / "protocol.json"
        write_protocol(protocol_path, cell)
        _write_canonical(
            cell_root / "CELL_COMPLETE",
            {
                "schema_version": 1,
                "status": "complete",
                "cell_id": cell["cell_id"],
                "manifest_sha256": schedule["manifest_sha256"],
                "protocol_sha256": _sha256_file(protocol_path),
            },
        )
    _write_canonical(
        output / "PUBLICATION_V3_COMPLETE",
        {
            "schema_version": 1,
            "status": "complete",
            "manifest_sha256": schedule["manifest_sha256"],
            "schedule_sha256": _sha256_file(output / "schedule.json"),
            "complete_cells": len(schedule["cells"]),
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("manifest", type=Path)
    schedule_parser = subparsers.add_parser("schedule")
    schedule_parser.add_argument("manifest", type=Path)
    schedule_parser.add_argument("--output", required=True, type=Path)
    verify_parser = subparsers.add_parser("verify-schedule")
    verify_parser.add_argument("manifest", type=Path)
    verify_parser.add_argument("schedule", type=Path)
    protocol_parser = subparsers.add_parser("protocol")
    protocol_parser.add_argument("--schedule", required=True, type=Path)
    protocol_parser.add_argument("--cell-id", required=True)
    protocol_parser.add_argument("--output", required=True, type=Path)
    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("manifest", type=Path)
    replay_parser.add_argument("--source-archive", required=True, type=Path)
    replay_parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "protocol":
        schedule = validate_schedule_document(_read_json(args.schedule))
        cells = {
            str(cell["cell_id"]): cell for cell in schedule["cells"]
        }
        if args.cell_id not in cells:
            raise ValueError("requested cell_id is absent from schedule")
        write_protocol(args.output, cells[args.cell_id])
        print(
            json.dumps(
                {"cell_id": args.cell_id, "output": str(args.output)},
                sort_keys=True,
            )
        )
        return 0
    manifest = validate_manifest(_read_json(args.manifest))
    unstaged = sum(
        dataset["source"]["state"] == "unstaged" for dataset in manifest["datasets"]
    )
    paid_ready = manifest["source"]["state"] == "frozen" and unstaged == 0
    if args.command == "replay":
        if not paid_ready:
            raise ValueError("replay requires paid-ready source and datasets")
        _write_structural_replay(manifest, args.source_archive, args.output)
        validator = Path(__file__).with_name("validate_publication_v3_results.py")
        completed = subprocess.run(
            [
                sys.executable,
                str(validator),
                str(args.output),
                "--structural-only",
                "--expected-source-sha256",
                _sha256_file(args.source_archive),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        print(completed.stdout.strip())
        return 0
    if args.command == "validate":
        print(
            json.dumps(
                {
                    "campaign_id": manifest["campaign_id"],
                    "datasets": len(manifest["datasets"]),
                    "paid_ready": paid_ready,
                    "schema_version": 3,
                    "unstaged_datasets": unstaged,
                    "workloads": len(manifest["workloads"]),
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "verify-schedule":
        if not paid_ready:
            raise ValueError("schedule verification requires paid-ready source and datasets")
        schedule = validate_schedule_for_manifest(_read_json(args.schedule), manifest)
        print(
            json.dumps(
                {
                    "campaign_id": manifest["campaign_id"],
                    "cells": len(schedule["cells"]),
                    "manifest_sha256": schedule["manifest_sha256"],
                    "paid_ready": True,
                    "schema_version": schedule["schema_version"],
                },
                sort_keys=True,
            )
        )
        return 0
    if not paid_ready:
        raise ValueError("schedule requires a frozen source and staged datasets")
    schedule = build_schedule_document(manifest)
    _write_canonical(args.output, schedule)
    print(
        json.dumps(
            {
                "campaign_id": manifest["campaign_id"],
                "cells": len(schedule["cells"]),
                "output": str(args.output),
                "paid_ready": paid_ready,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
