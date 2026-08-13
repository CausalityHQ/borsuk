#!/usr/bin/env python3
"""Canonical immutable Publication V3 index-completion receipts."""

from __future__ import annotations

import copy
import hashlib
import json
import re

try:
    from scripts.publication_v3_protocol import canonical_json_bytes, index_id
    from scripts.publication_v3_results import validate_object_roster
except ModuleNotFoundError:
    from publication_v3_protocol import canonical_json_bytes, index_id
    from publication_v3_results import validate_object_roster

RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "document_kind",
        "status",
        "campaign_id",
        "manifest_sha256",
        "index_id",
        "index_uri",
        "system",
        "source_archive_sha256",
        "index_profile_sha256",
        "dataset_id",
        "dataset_sha256",
        "dataset_rows",
        "build_attempt_id",
        "builder_instance_identity",
        "builder_instance_type",
        "index_stats",
        "build_metrics",
        "object_roster_ref",
        "receipt_sha256",
    }
)
ROSTER_REF_FIELDS = frozenset({"path", "bytes", "checksum", "objects", "total_bytes"})
INDEX_STATS_FIELDS = frozenset(
    {
        "logical_cells",
        "records",
        "total_active_index_bytes",
        "logical_cell_catalog_checksum",
    }
)
BUILD_METRIC_FIELDS = frozenset(
    {
        "cpu_ns",
        "peak_rss_bytes",
        "disk_read_bytes",
        "disk_write_bytes",
        "storage_gets",
        "storage_puts",
        "storage_deletes",
        "storage_heads",
        "storage_lists",
        "storage_bytes_read",
        "storage_bytes_written",
        "build_elapsed_ns",
    }
)
S3_URI = re.compile(r"s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/[^\s/](?:[^\s]*[^\s/])?")
MAX_RECEIPT_BYTES = 256 * 1024


def _checksum(value: object, role: str) -> str:
    digest = str(value)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"{role} must be lowercase SHA-256")
    return digest


def _identity(value: object, role: str) -> str:
    identity = str(value)
    if not identity or len(identity.encode("utf-8")) > 256:
        raise ValueError(f"{role} must be a bounded nonempty string")
    return identity


def _nonnegative(value: object, role: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{role} must be a nonnegative integer")
    return value


def _validate_dataset_materialization(
    cell: dict[str, object], dataset_materialization_sha256: str
) -> str:
    dataset = cell.get("dataset")
    if not isinstance(dataset, dict):
        raise ValueError("scheduled dataset is invalid")
    source = dataset.get("source")
    if not isinstance(source, dict):
        raise ValueError("scheduled dataset source is invalid")
    materialization = _checksum(
        dataset_materialization_sha256, "dataset materialization checksum"
    )
    if source.get("state") == "staged" and materialization != _checksum(
        source.get("sha256"), "staged dataset checksum"
    ):
        raise ValueError("staged dataset materialization differs from its schedule")
    return materialization


def _receipt_sha256(receipt: dict[str, object]) -> str:
    unsigned = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    return hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()


def receipt_document_sha256(receipt: dict[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(receipt) + b"\n").hexdigest()


def require_verified_index(
    payload: bytes,
    *,
    cell: dict[str, object],
    source_archive_sha256: str,
    dataset_materialization_sha256: str,
) -> tuple[dict[str, object], str]:
    if not payload or len(payload) > MAX_RECEIPT_BYTES or not payload.endswith(b"\n"):
        raise ValueError("index receipt bytes are missing or exceed their canonical bound")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("index receipt is not valid UTF-8 JSON") from error
    if canonical_json_bytes(value) + b"\n" != payload:
        raise ValueError("index receipt is not canonical JSON")
    receipt = validate_index_receipt(
        value,
        cell=cell,
        source_archive_sha256=source_archive_sha256,
        dataset_materialization_sha256=dataset_materialization_sha256,
    )
    return receipt, hashlib.sha256(payload).hexdigest()


def require_verified_object_roster(
    receipt: dict[str, object], payload: bytes, *, cell: dict[str, object]
) -> list[dict[str, object]]:
    reference = receipt.get("object_roster_ref")
    if not isinstance(reference, dict):
        raise ValueError("index receipt has no object roster reference")
    if len(payload) != reference.get("bytes") or hashlib.sha256(payload).hexdigest() != reference.get("checksum"):
        raise ValueError("index object roster differs from its receipt")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("index object roster is not valid JSON") from error
    if canonical_json_bytes(value) + b"\n" != payload or not isinstance(value, list):
        raise ValueError("index object roster is not canonical")
    dataset = cell.get("dataset")
    profile = cell.get("index_profile")
    summary = validate_object_roster(
        value,
        logical_rows=dataset.get("scale", {}).get("rows") if isinstance(dataset, dict) else None,
        logical_cells=profile.get("logical_cells") if isinstance(profile, dict) else None,
    )
    if summary["objects"] != reference.get("objects") or summary["total_object_bytes"] != reference.get("total_bytes"):
        raise ValueError("index object roster summary differs from its receipt")
    if not any(item.get("role") == "directory" for item in value):
        raise ValueError("index object roster must include directory authority")
    return copy.deepcopy(value)


def reconcile_index_inventory(
    object_roster: list[dict[str, object]], inventory: list[dict[str, object]]
) -> None:
    expected = {
        item["path"]: (item["bytes"], item["checksum"], item["etag"])
        for item in object_roster
        if isinstance(item, dict)
    }
    observed: dict[str, tuple[object, object]] = {}
    for item in inventory:
        if not isinstance(item, dict) or frozenset(item) != frozenset(
            {"path", "bytes", "checksum", "etag"}
        ):
            raise ValueError("index inventory fields differ")
        path = str(item["path"])
        if path in observed:
            raise ValueError("index inventory paths must be unique")
        observed[path] = (item["bytes"], item["checksum"], item["etag"])
    if observed != expected:
        raise ValueError("index inventory differs from its immutable receipt")


def build_index_receipt(
    *,
    cell: dict[str, object],
    source_archive_sha256: str,
    dataset_materialization_sha256: str,
    build_attempt_id: str,
    builder_instance_identity: str,
    builder_instance_type: str,
    build_artifact: dict[str, dict[str, object]],
    object_roster: list[dict[str, object]],
    build_metrics: dict[str, int],
) -> dict[str, object]:
    dataset = cell.get("dataset")
    profile = cell.get("index_profile")
    rows = dataset.get("scale", {}).get("rows") if isinstance(dataset, dict) else None
    logical_cells = profile.get("logical_cells") if isinstance(profile, dict) else None
    roster_summary = validate_object_roster(
        object_roster, logical_rows=rows, logical_cells=logical_cells
    )
    roster_payload = canonical_json_bytes(object_roster) + b"\n"
    receipt: dict[str, object] = {
        "schema_version": 1,
        "document_kind": "publication-v3-index-complete",
        "status": "complete",
        "campaign_id": cell.get("campaign_id"),
        "manifest_sha256": cell.get("manifest_sha256"),
        "index_id": index_id(cell),
        "index_uri": cell.get("index_prefix"),
        "system": cell.get("system"),
        "source_archive_sha256": source_archive_sha256,
        "index_profile_sha256": hashlib.sha256(canonical_json_bytes(profile)).hexdigest(),
        "dataset_id": dataset.get("id") if isinstance(dataset, dict) else None,
        "dataset_sha256": _validate_dataset_materialization(
            cell, dataset_materialization_sha256
        ),
        "dataset_rows": dataset.get("scale", {}).get("rows") if isinstance(dataset, dict) else None,
        "build_attempt_id": build_attempt_id,
        "builder_instance_identity": builder_instance_identity,
        "builder_instance_type": builder_instance_type,
        "index_stats": copy.deepcopy(build_artifact.get("index_stats")),
        "build_metrics": copy.deepcopy(build_metrics),
        "object_roster_ref": {
            "path": "INDEX_OBJECTS.json",
            "bytes": len(roster_payload),
            "checksum": hashlib.sha256(roster_payload).hexdigest(),
            "objects": roster_summary["objects"],
            "total_bytes": roster_summary["total_object_bytes"],
        },
    }
    storage_metrics = build_artifact.get("storage_metrics")
    if not isinstance(storage_metrics, dict) or any(
        receipt["build_metrics"].get(field) != value
        for field, value in storage_metrics.items()
    ):
        raise ValueError("index receipt build metrics differ from bench_build.csv")
    receipt["receipt_sha256"] = _receipt_sha256(receipt)
    return validate_index_receipt(
        receipt,
        cell=cell,
        source_archive_sha256=source_archive_sha256,
        dataset_materialization_sha256=dataset_materialization_sha256,
    )


def validate_index_receipt(
    value: object,
    *,
    cell: dict[str, object],
    source_archive_sha256: str,
    dataset_materialization_sha256: str,
) -> dict[str, object]:
    if not isinstance(value, dict) or frozenset(value) != RECEIPT_FIELDS:
        raise ValueError("index receipt fields differ")
    if value["schema_version"] != 1 or value["document_kind"] != "publication-v3-index-complete" or value["status"] != "complete":
        raise ValueError("index receipt schema, kind, or status is invalid")
    expected_index_id = index_id(cell)
    expected_uri = cell.get("index_prefix")
    if value["index_id"] != expected_index_id:
        raise ValueError("index receipt identity differs from the scheduled index")
    if value["index_uri"] != expected_uri or S3_URI.fullmatch(str(value["index_uri"])) is None or not str(value["index_uri"]).endswith(f"/{expected_index_id}"):
        raise ValueError("index receipt URI differs from its S3 authority")
    for field in ("campaign_id", "manifest_sha256", "system"):
        if value[field] != cell.get(field):
            raise ValueError(f"index receipt {field} differs from its protocol")
    if value["source_archive_sha256"] != _checksum(source_archive_sha256, "source archive checksum"):
        raise ValueError("index receipt source archive differs")
    profile = cell.get("index_profile")
    if value["index_profile_sha256"] != hashlib.sha256(canonical_json_bytes(profile)).hexdigest():
        raise ValueError("index receipt profile differs")
    dataset = cell.get("dataset")
    if not isinstance(dataset, dict) or value["dataset_id"] != dataset.get("id") or value["dataset_sha256"] != _validate_dataset_materialization(cell, dataset_materialization_sha256) or value["dataset_rows"] != dataset.get("scale", {}).get("rows"):
        raise ValueError("index receipt dataset differs")
    _identity(value["build_attempt_id"], "build attempt id")
    _identity(value["builder_instance_identity"], "builder instance identity")
    environment = cell.get("environment_contract")
    workers = environment.get("build_workers") if isinstance(environment, dict) else None
    worker = workers.get(cell.get("system")) if isinstance(workers, dict) else None
    if value["builder_instance_type"] != (worker.get("instance_type") if isinstance(worker, dict) else None):
        raise ValueError("index receipt builder instance type differs from its contract")
    stats = value["index_stats"]
    if not isinstance(stats, dict) or frozenset(stats) != INDEX_STATS_FIELDS:
        raise ValueError("index receipt stats fields differ")
    profile_cells = profile.get("logical_cells") if isinstance(profile, dict) else None
    if stats["logical_cells"] != profile_cells or stats["records"] != value["dataset_rows"]:
        raise ValueError("index receipt stats differ from the scheduled index")
    _checksum(stats["logical_cell_catalog_checksum"], "logical cell catalog checksum")
    if _nonnegative(stats["total_active_index_bytes"], "active index bytes") <= 0:
        raise ValueError("active index bytes must be positive")
    metrics = value["build_metrics"]
    if not isinstance(metrics, dict) or frozenset(metrics) != BUILD_METRIC_FIELDS:
        raise ValueError("index receipt build metric fields differ")
    for field, metric in metrics.items():
        _nonnegative(metric, f"build metric {field}")
    if metrics["cpu_ns"] <= 0 or metrics["peak_rss_bytes"] <= 0 or metrics["build_elapsed_ns"] <= 0:
        raise ValueError("index receipt CPU, RSS, and elapsed metrics must be positive")
    roster_ref = value["object_roster_ref"]
    if not isinstance(roster_ref, dict) or frozenset(roster_ref) != ROSTER_REF_FIELDS:
        raise ValueError("index receipt roster reference fields differ")
    if roster_ref["path"] != "INDEX_OBJECTS.json":
        raise ValueError("index receipt roster reference path differs")
    _checksum(roster_ref["checksum"], "object roster checksum")
    for field in ("bytes", "objects", "total_bytes"):
        if _nonnegative(roster_ref[field], f"object roster {field}") <= 0:
            raise ValueError(f"object roster {field} must be positive")
    if value["receipt_sha256"] != _receipt_sha256(value):
        raise ValueError("index receipt checksum differs from its canonical payload")
    return copy.deepcopy(value)
