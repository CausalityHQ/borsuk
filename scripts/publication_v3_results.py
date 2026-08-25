#!/usr/bin/env python3
"""Shared Publication V3 measurement and physical-object admission."""

from __future__ import annotations

import copy
import hashlib
import re

try:
    from scripts.production_bench_schema import QUERY_STAGE_AGGREGATE_FIELDS
    from scripts.publication_v3_protocol import canonical_json_bytes
except ModuleNotFoundError:
    from production_bench_schema import QUERY_STAGE_AGGREGATE_FIELDS
    from publication_v3_protocol import canonical_json_bytes

OBJECT_FIELDS = frozenset({"role", "path", "format", "bytes", "rows", "checksum", "etag"})
OBJECT_ETAG = re.compile(r'"[0-9a-f]{32}(?:-[1-9][0-9]*)?"')
OBJECT_ROLES = frozenset({"data-bundle", "query-page", "directory", "control"})
FORMATS = {
    "data-bundle": frozenset({"parquet", "arrow-ipc"}),
    "query-page": frozenset({"arrow-ipc", "parquet", "packed"}),
    "directory": frozenset({"arrow-ipc", "parquet"}),
    "control": frozenset({"json", "packed"}),
}
MAX_DATA_OBJECT_BYTES = 128 * 1024 * 1024
MAX_LIFECYCLE_DATA_OBJECT_BYTES = 128 * 1024 * 1024
MAX_CONTROL_OBJECT_BYTES = 256 * 1024
MAX_CONTROL_OBJECTS = 512
MAX_DATA_OBJECTS = 8192
RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "cell_id",
        "manifest_sha256",
        "protocol_sha256",
        "source_archive_sha256",
        "attempt_id",
        "instance_identity",
        "arm",
        "metrics",
        "index_receipt_sha256",
        "clone_receipt_sha256",
        "runtime_attestation_sha256",
    }
)
READ_METRIC_FIELDS_V1 = frozenset(
    {
        "queries",
        "correctness_ppm",
        "latency_p50_us",
        "latency_p95_us",
        "latency_p99_us",
        "throughput_milli_per_second",
        "cpu_ns",
        "peak_rss_bytes",
        "disk_read_bytes",
        "disk_write_bytes",
        "storage_gets",
        "storage_puts",
        "storage_bytes_read",
        "storage_bytes_written",
    }
)
READ_METRIC_FIELDS_V2 = READ_METRIC_FIELDS_V1 | frozenset(
    {"global_leaf_code_requests", "global_leaf_exact_requests"}
)
READ_METRIC_FIELDS_V3 = READ_METRIC_FIELDS_V2 | frozenset(
    QUERY_STAGE_AGGREGATE_FIELDS
)
READ_METRIC_FIELDS = READ_METRIC_FIELDS_V3 | frozenset(
    {
        "decoded_cache_bytes_read",
        "disk_cache_bytes_read",
        "excluded_setup_storage_gets",
        "excluded_setup_storage_bytes_read",
    }
)
LIFECYCLE_METRIC_FIELDS = frozenset(
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
        "cpu_ns",
        "peak_rss_bytes",
        "disk_read_bytes",
        "disk_write_bytes",
        "storage_gets",
        "storage_puts",
        "storage_bytes_read",
        "storage_bytes_written",
        "storage_distinct_data_objects",
        "storage_max_data_object_bytes",
    }
)


def _positive_integer(value: object, role: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{role} must be a positive integer")
    return value


def _checksum(value: object) -> str:
    digest = str(value)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("object checksum must be lowercase SHA-256")
    return digest


def _nonnegative_integer(value: object, role: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{role} must be a nonnegative integer")
    return value


def _bounded_identity(value: object, role: str) -> str:
    identity = str(value)
    if not identity or len(identity.encode("utf-8")) > 256:
        raise ValueError(f"{role} must be a nonempty bounded string")
    return identity


def _correctness_floor(cell: dict[str, object]) -> int:
    workload = cell.get("workload")
    if not isinstance(workload, dict) or not isinstance(workload.get("factors"), dict):
        raise ValueError("cell workload factors are invalid")
    factors = workload["factors"]
    floors = [
        factors[key]
        for key in (
            "minimum_recall_ppm",
            "minimum_ndcg_ppm",
            "minimum_lifecycle_accuracy_ppm",
        )
        if key in factors
    ]
    if len(floors) > 1:
        raise ValueError("cell declares multiple correctness floors")
    if not floors:
        return 1_000_000
    floor = _positive_integer(floors[0], "correctness floor")
    if floor > 1_000_000:
        raise ValueError("correctness floor exceeds one million ppm")
    return floor


def _validate_result_arm(value: object, cell: dict[str, object]) -> dict[str, object]:
    arm = value if isinstance(value, dict) else None
    workload = cell.get("workload")
    if not isinstance(arm, dict) or not isinstance(workload, dict):
        raise ValueError("cell result arm is invalid")
    factors = workload.get("factors")
    kind = workload.get("kind")
    if not isinstance(factors, dict):
        raise ValueError("cell result arm kind is not implemented")
    if kind == "read-recall":
        expected = frozenset(
            {"k", "leaf_page_budget", "cache_state"}
        )
        if frozenset(arm) != expected:
            raise ValueError("cell result arm fields differ")
        for field in ("k", "leaf_page_budget"):
            _positive_integer(arm[field], f"cell result arm {field}")
        _bounded_identity(arm["cache_state"], "cell result arm cache state")
        if (
            arm["k"] not in factors.get("k", [])
            or arm["leaf_page_budget"] not in factors.get("leaf_page_budgets", [])
            or arm["cache_state"] not in factors.get("cache_states", [])
        ):
            raise ValueError("cell result arm is not scheduled")
        return copy.deepcopy(arm)
    if kind != "write-update-delete-compact":
        raise ValueError("cell result arm kind is not implemented")
    expected = frozenset(
        {
            "writers",
            "batch_size",
            "insert_mode",
            "update_percent",
            "delete_percent",
        }
    )
    if frozenset(arm) != expected:
        raise ValueError("cell result arm fields differ")
    for field in (
        "writers",
        "batch_size",
        "update_percent",
        "delete_percent",
    ):
        _positive_integer(arm[field], f"cell result arm {field}")
    _bounded_identity(arm["insert_mode"], "cell result arm insert mode")
    if (
        arm["writers"] not in factors.get("writers", [])
        or arm["batch_size"] not in factors.get("batch_sizes", [])
        or arm["insert_mode"] not in factors.get("insert_modes", [])
        or arm["update_percent"] not in factors.get("update_percent", [])
        or arm["delete_percent"] not in factors.get("delete_percent", [])
    ):
        raise ValueError("cell result arm is not scheduled")
    return copy.deepcopy(arm)


def validate_object_roster(
    value: list[dict[str, object]], *, logical_rows: int, logical_cells: int | None = None
) -> dict[str, int]:
    logical_rows = _positive_integer(logical_rows, "logical rows")
    if logical_cells is not None:
        logical_cells = _positive_integer(logical_cells, "logical cells")
    if not isinstance(value, list) or not value:
        raise ValueError("object roster must be a nonempty list")
    paths: set[str] = set()
    represented_rows = 0
    data_bundles = 0
    data_objects = 0
    control_objects = 0
    total_bytes = 0
    maximum_bytes = 0
    for item in value:
        if not isinstance(item, dict) or frozenset(item) != OBJECT_FIELDS:
            raise ValueError("object roster fields differ")
        role = str(item["role"])
        path = str(item["path"])
        if role not in OBJECT_ROLES or str(item["format"]) not in FORMATS[role]:
            raise ValueError("object roster role or format is invalid")
        if not path or path.startswith("/") or ".." in path.split("/") or path in paths:
            raise ValueError("object roster paths must be relative and unique")
        paths.add(path)
        byte_count = _positive_integer(item["bytes"], "object bytes")
        rows = item["rows"]
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise ValueError("object rows must be a nonnegative integer")
        _checksum(item["checksum"])
        if OBJECT_ETAG.fullmatch(str(item["etag"])) is None:
            raise ValueError("object ETag is invalid")
        cap = MAX_CONTROL_OBJECT_BYTES if role == "control" else MAX_DATA_OBJECT_BYTES
        if byte_count > cap:
            raise ValueError("object exceeds its format byte cap")
        if role == "control":
            control_objects += 1
            if rows != 0:
                raise ValueError("control objects cannot represent data rows")
        else:
            data_objects += 1
        if role == "data-bundle":
            if rows <= 0:
                raise ValueError("data bundles must represent rows")
            data_bundles += 1
            represented_rows += rows
        total_bytes += byte_count
        maximum_bytes = max(maximum_bytes, byte_count)
    if represented_rows != logical_rows:
        raise ValueError("object roster logical row total differs from dataset")
    if logical_rows >= 10_000_000 and data_bundles < 2:
        raise ValueError("large-scale results require multiple data bundles")
    if (
        (data_bundles > 1 and data_bundles * 1024 > logical_rows)
        or data_objects > MAX_DATA_OBJECTS
        or (
            logical_cells is not None
            and logical_cells > MAX_DATA_OBJECTS
            and data_objects >= logical_cells
        )
    ):
        raise ValueError("object-count amplification is proportional to rows or cells")
    if control_objects > MAX_CONTROL_OBJECTS:
        raise ValueError("control-object amplification exceeds its fixed cap")
    return {
        "objects": len(value),
        "data_objects": data_objects,
        "data_bundles": data_bundles,
        "control_objects": control_objects,
        "represented_rows": represented_rows,
        "total_object_bytes": total_bytes,
        "maximum_object_bytes": maximum_bytes,
    }


def validate_cell_result(
    value: dict[str, object],
    *,
    cell: dict[str, object],
    protocol_bytes: bytes,
    source_archive_sha256: str,
    dataset_materialization_sha256: str,
    index_receipt: dict[str, object],
    runtime_attestation: dict[str, object],
    clone_receipt: dict[str, object] | None = None,
) -> dict[str, object]:
    if not isinstance(value, dict) or frozenset(value) != RESULT_FIELDS:
        raise ValueError("cell result fields differ")
    if value["schema_version"] not in (1, 2, 3, 4) or value["status"] != "complete":
        raise ValueError("cell result schema or status is invalid")
    if value["cell_id"] != cell.get("cell_id"):
        raise ValueError("cell result identity differs from its protocol")
    if value["manifest_sha256"] != cell.get("manifest_sha256"):
        raise ValueError("cell result manifest differs from its protocol")
    expected_protocol = canonical_json_bytes(cell) + b"\n"
    if protocol_bytes != expected_protocol:
        raise ValueError("protocol bytes are not the canonical scheduled cell")
    protocol_sha256 = hashlib.sha256(protocol_bytes).hexdigest()
    if value["protocol_sha256"] != protocol_sha256:
        raise ValueError("cell result protocol checksum differs")
    source_archive_sha256 = _checksum(source_archive_sha256)
    if value["source_archive_sha256"] != source_archive_sha256:
        raise ValueError("cell result source archive checksum differs")
    _bounded_identity(value["attempt_id"], "attempt identity")
    _bounded_identity(value["instance_identity"], "instance identity")
    arm = _validate_result_arm(value["arm"], cell)
    workload = cell.get("workload")
    workload_kind = workload.get("kind") if isinstance(workload, dict) else None
    try:
        from scripts.publication_v3_receipts import (
            receipt_document_sha256,
            validate_index_receipt,
        )
    except ModuleNotFoundError:
        from publication_v3_receipts import (
            receipt_document_sha256,
            validate_index_receipt,
        )

    receipt = validate_index_receipt(
        index_receipt,
        cell=cell,
        source_archive_sha256=source_archive_sha256,
        dataset_materialization_sha256=dataset_materialization_sha256,
    )
    if value["index_receipt_sha256"] != receipt_document_sha256(receipt):
        raise ValueError("cell result index receipt differs from its authorized build")
    if workload_kind == "read-recall":
        if clone_receipt is not None or value["clone_receipt_sha256"] is not None:
            raise ValueError("read-only cell result cannot use a mutable clone")
    elif workload_kind == "write-update-delete-compact":
        if clone_receipt is None:
            raise ValueError("lifecycle cell result requires a mutable clone receipt")
        try:
            from scripts.publication_v3_clones import (
                clone_receipt_document_sha256,
                validate_clone_receipt,
            )
        except ModuleNotFoundError:
            from publication_v3_clones import (
                clone_receipt_document_sha256,
                validate_clone_receipt,
            )

        clone = validate_clone_receipt(
            clone_receipt,
            cell=cell,
            arm=arm,
            attempt_id=str(value["attempt_id"]),
            base_receipt=receipt,
        )
        if value["clone_receipt_sha256"] != clone_receipt_document_sha256(clone):
            raise ValueError("cell result clone receipt differs from its mutable index")
    else:
        raise ValueError("cell result workload kind is not implemented")
    try:
        from scripts.publication_v3_attestation import (
            runtime_attestation_sha256,
            validate_runtime_attestation,
        )
    except ModuleNotFoundError:
        from publication_v3_attestation import (
            runtime_attestation_sha256,
            validate_runtime_attestation,
        )

    attestation = validate_runtime_attestation(
        runtime_attestation, cell=cell, attempt_id=str(value["attempt_id"])
    )
    if attestation["instance_id"] != value["instance_identity"]:
        raise ValueError("cell result runtime instance differs from its attestation")
    if value["runtime_attestation_sha256"] != runtime_attestation_sha256(attestation):
        raise ValueError("cell result runtime attestation differs from its measured host")

    metrics = value["metrics"]
    result_schema_version = value["schema_version"]
    if workload_kind == "read-recall":
        if result_schema_version == 1:
            expected_metric_fields = READ_METRIC_FIELDS_V1
        elif result_schema_version == 2:
            expected_metric_fields = READ_METRIC_FIELDS_V2
        elif result_schema_version == 3:
            expected_metric_fields = READ_METRIC_FIELDS_V3
        elif result_schema_version == 4:
            expected_metric_fields = READ_METRIC_FIELDS
        else:
            raise ValueError("cell result schema version is unsupported")
    else:
        if result_schema_version != 1:
            raise ValueError("cell result schema version is unsupported")
        expected_metric_fields = LIFECYCLE_METRIC_FIELDS
    if not isinstance(metrics, dict) or frozenset(metrics) != expected_metric_fields:
        raise ValueError("cell result metric fields differ")
    correctness_field = (
        "correctness_ppm"
        if workload_kind == "read-recall"
        else "lifecycle_accuracy_ppm"
    )
    correctness = _nonnegative_integer(metrics[correctness_field], "correctness ppm")
    if correctness > 1_000_000 or correctness < _correctness_floor(cell):
        raise ValueError("cell result correctness is below its protocol floor")
    if workload_kind == "read-recall":
        queries = _positive_integer(metrics["queries"], "query count")
        if queries != cell.get("queries_per_repetition"):
            raise ValueError("cell result query count differs from its protocol")
        latency_prefix = "latency"
    else:
        for field in (
            "insert_ops",
            "upsert_ops",
            "delete_ops",
        ):
            _positive_integer(metrics[field], field.replace("_", " "))
        expected_upserts = (
            metrics["insert_ops"] * arm["update_percent"] + 99
        ) // 100
        expected_deletes = (
            metrics["insert_ops"] * arm["delete_percent"] + 99
        ) // 100
        if (
            metrics["upsert_ops"] != expected_upserts
            or metrics["delete_ops"] != expected_deletes
        ):
            raise ValueError("lifecycle result differs from its scheduled mutation mix")
        if metrics["compact_ops"] != 1 or metrics["purge_ops"] != 1:
            raise ValueError("lifecycle result must compact and purge exactly once")
        if any(
            isinstance(metrics[field], bool) or metrics[field] != 1
            for field in ("flush_ops", "consolidate_ops")
        ):
            raise ValueError("lifecycle result must flush and consolidate exactly once")
        lifecycle_times = [
            _positive_integer(metrics[field], field.replace("_", " "))
            for field in (
                "first_publish_us",
                "time_to_searchable_us",
                "time_to_fully_indexed_us",
                "time_to_consolidated_us",
            )
        ]
        if lifecycle_times != sorted(lifecycle_times):
            raise ValueError("lifecycle result milestone times are not ordered")
        _positive_integer(metrics["write_amplification_ppm"], "write amplification")
        latency_prefix = "batch_latency"
    p50 = _nonnegative_integer(metrics[f"{latency_prefix}_p50_us"], "p50 latency")
    p95 = _nonnegative_integer(metrics[f"{latency_prefix}_p95_us"], "p95 latency")
    p99 = _nonnegative_integer(metrics[f"{latency_prefix}_p99_us"], "p99 latency")
    if not p50 <= p95 <= p99:
        raise ValueError("cell result latency quantiles are not ordered")
    _positive_integer(metrics["throughput_milli_per_second"], "throughput")
    _positive_integer(metrics["cpu_ns"], "CPU time")
    peak_rss_bytes = _positive_integer(metrics["peak_rss_bytes"], "peak RSS")
    environment = cell.get("environment_contract")
    system = cell.get("system")
    if not isinstance(environment, dict) or not isinstance(system, str):
        raise ValueError("cell runtime-client environment is invalid")
    clients = environment.get("runtime_clients")
    client = clients.get(system) if isinstance(clients, dict) else None
    memory_mib = client.get("memory_mib") if isinstance(client, dict) else None
    if isinstance(memory_mib, bool) or not isinstance(memory_mib, int) or memory_mib <= 0:
        raise ValueError("cell runtime-client memory is invalid")
    if peak_rss_bytes > memory_mib * 1024 * 1024:
        raise ValueError("cell result peak RSS exceeds its runtime client")
    for field in (
        "disk_read_bytes",
        "disk_write_bytes",
        "storage_gets",
        "storage_puts",
        "storage_bytes_read",
        "storage_bytes_written",
    ):
        _nonnegative_integer(metrics[field], field.replace("_", " "))
    if workload_kind != "read-recall":
        write_objects = _nonnegative_integer(
            metrics["storage_distinct_data_objects"], "storage distinct data objects"
        )
        max_object_bytes = _nonnegative_integer(
            metrics["storage_max_data_object_bytes"], "storage max data object bytes"
        )
        if (
            write_objects == 0
            or max_object_bytes == 0
            or write_objects > metrics["storage_puts"]
            or max_object_bytes > metrics["storage_bytes_written"]
        ):
            raise ValueError("lifecycle storage topology is inconsistent")
        if write_objects < 2 or max_object_bytes > MAX_LIFECYCLE_DATA_OBJECT_BYTES:
            raise ValueError(
                "lifecycle writes must use a bounded multi-object storage topology"
            )
    if workload_kind == "read-recall" and result_schema_version >= 2:
        for field in ("global_leaf_code_requests", "global_leaf_exact_requests"):
            _nonnegative_integer(metrics[field], field.replace("_", " "))
    if workload_kind == "read-recall" and result_schema_version >= 3:
        for field in QUERY_STAGE_AGGREGATE_FIELDS:
            _nonnegative_integer(metrics[field], field.replace("_", " "))
        if (
            metrics["global_base_exact_read_us_max_across_queries"]
            > metrics["global_base_exact_read_us_sum_total"]
            or metrics["global_base_exact_read_us_max_across_queries"]
            > metrics["global_base_exact_fetch_us_total"]
            or metrics["global_base_exact_fetch_us_total"]
            > metrics["global_base_exact_rerank_us_total"]
        ):
            raise ValueError("cell result timing aggregates are inconsistent")
        timing_tails = [
            metrics["global_base_exact_reads_over_20ms_total"],
            metrics["global_base_exact_reads_over_30ms_total"],
            metrics["global_base_exact_reads_over_50ms_total"],
            metrics["global_base_exact_reads_over_100ms_total"],
        ]
        if timing_tails != sorted(timing_tails, reverse=True):
            raise ValueError("cell result timing aggregates are inconsistent")
    if workload_kind == "read-recall" and result_schema_version >= 4:
        for field in (
            "decoded_cache_bytes_read",
            "disk_cache_bytes_read",
            "excluded_setup_storage_gets",
            "excluded_setup_storage_bytes_read",
        ):
            _nonnegative_integer(metrics[field], field.replace("_", " "))
    if workload_kind == "read-recall" and (
        metrics["storage_puts"] != 0 or metrics["storage_bytes_written"] != 0
    ):
        raise ValueError("read-only publication results cannot write index objects")
    return copy.deepcopy(value)
