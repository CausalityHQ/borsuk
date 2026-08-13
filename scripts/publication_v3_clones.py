#!/usr/bin/env python3
"""Fail-closed mutable-arm clones of immutable Publication V3 indexes."""

from __future__ import annotations

import copy
import hashlib
import json
import re

try:
    from scripts.publication_v3_protocol import canonical_json_bytes
    from scripts.publication_v3_receipts import receipt_document_sha256
except ModuleNotFoundError:
    from publication_v3_protocol import canonical_json_bytes
    from publication_v3_receipts import receipt_document_sha256

S3_URI = re.compile(r"s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/[^\s/](?:[^\s]*[^\s/])?")
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}")
ETAG = re.compile(r'"[0-9a-f]{32}(?:-[1-9][0-9]*)?"')
MAX_CLONE_RECEIPT_BYTES = 256 * 1024
MAX_SINGLE_COPY_BYTES = 5 * 1024**3
FIELDS = frozenset(
    {
        "schema_version",
        "document_kind",
        "status",
        "cell_id",
        "arm_sha256",
        "attempt_id",
        "base_receipt_sha256",
        "base_index_id",
        "base_index_uri",
        "clone_index_uri",
        "verification_tier",
        "copy_inventory_ref",
        "expires_after_days",
        "receipt_sha256",
    }
)
INVENTORY_REF_FIELDS = frozenset(
    {"path", "bytes", "checksum", "objects", "total_bytes"}
)
INVENTORY_FIELDS = frozenset(
    {"path", "bytes", "source_etag", "destination_etag"}
)


def _sha256(value: object, role: str) -> str:
    digest = str(value)
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError(f"{role} must be lowercase SHA-256")
    return digest


def _identity(value: object, role: str) -> str:
    identity = str(value)
    if IDENTITY.fullmatch(identity) is None:
        raise ValueError(f"{role} is invalid")
    return identity


def _arm_sha256(arm: dict[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(arm)).hexdigest()


def clone_index_uri(
    cell: dict[str, object], *, arm: dict[str, object], attempt_id: str
) -> str:
    base = str(cell.get("index_prefix"))
    if S3_URI.fullmatch(base) is None or "/" not in base[5:]:
        raise ValueError("clone base index URI is invalid")
    attempt = _identity(attempt_id, "clone attempt identity")
    cell_id = _identity(cell.get("cell_id"), "clone cell identity")
    index_id = base.rsplit("/", 1)[-1]
    parent = base.rsplit("/", 1)[0]
    return (
        f"{parent}/mutable-arms/{cell_id}/{_arm_sha256(arm)[:24]}/"
        f"{attempt}/{index_id}"
    )


def _receipt_sha256(value: dict[str, object]) -> str:
    unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
    return hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()


def clone_receipt_document_sha256(receipt: dict[str, object]) -> str:
    """Bind a result to the exact canonical clone receipt document."""
    return hashlib.sha256(canonical_json_bytes(receipt) + b"\n").hexdigest()


def _inventory_summary(
    value: object, *, base_roster: list[dict[str, object]]
) -> dict[str, int]:
    if not isinstance(value, list) or not value:
        raise ValueError("clone copy evidence is empty")
    expected = {
        item.get("path"): (item.get("bytes"), item.get("etag"))
        for item in base_roster
        if isinstance(item, dict)
    }
    observed: dict[object, object] = {}
    total_bytes = 0
    for item in value:
        if not isinstance(item, dict) or frozenset(item) != INVENTORY_FIELDS:
            raise ValueError("clone copy evidence fields differ")
        path = item["path"]
        byte_count = item["bytes"]
        if path in observed:
            raise ValueError("clone copy evidence paths must be unique")
        if (
            isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count <= 0
            or byte_count > MAX_SINGLE_COPY_BYTES
            or ETAG.fullmatch(str(item["source_etag"])) is None
            or ETAG.fullmatch(str(item["destination_etag"])) is None
        ):
            raise ValueError("clone copy evidence is invalid")
        observed[path] = (byte_count, item["source_etag"])
        total_bytes += byte_count
    if observed != expected:
        raise ValueError("clone copy evidence differs from the immutable roster")
    return {"objects": len(value), "total_bytes": total_bytes}


def build_clone_receipt(
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    attempt_id: str,
    base_receipt: dict[str, object],
    base_roster: list[dict[str, object]],
    copy_inventory: list[dict[str, object]],
) -> dict[str, object]:
    summary = _inventory_summary(copy_inventory, base_roster=base_roster)
    payload = canonical_json_bytes(copy_inventory) + b"\n"
    receipt: dict[str, object] = {
        "schema_version": 1,
        "document_kind": "publication-v3-clone-complete",
        "status": "complete",
        "cell_id": cell.get("cell_id"),
        "arm_sha256": _arm_sha256(arm),
        "attempt_id": attempt_id,
        "base_receipt_sha256": receipt_document_sha256(base_receipt),
        "base_index_id": base_receipt.get("index_id"),
        "base_index_uri": base_receipt.get("index_uri"),
        "clone_index_uri": clone_index_uri(cell, arm=arm, attempt_id=attempt_id),
        "verification_tier": "copy-source-if-match-etag-v1",
        "copy_inventory_ref": {
            "path": "CLONE_OBJECTS.json",
            "bytes": len(payload),
            "checksum": hashlib.sha256(payload).hexdigest(),
            **summary,
        },
        "expires_after_days": 7,
    }
    receipt["receipt_sha256"] = _receipt_sha256(receipt)
    return validate_clone_receipt(
        receipt,
        cell=cell,
        arm=arm,
        attempt_id=attempt_id,
        base_receipt=base_receipt,
    )


def validate_clone_receipt(
    value: object,
    *,
    cell: dict[str, object],
    arm: dict[str, object],
    attempt_id: str,
    base_receipt: dict[str, object],
) -> dict[str, object]:
    if not isinstance(value, dict) or frozenset(value) != FIELDS:
        raise ValueError("clone receipt fields differ")
    if (
        value["schema_version"] != 1
        or value["document_kind"] != "publication-v3-clone-complete"
        or value["status"] != "complete"
    ):
        raise ValueError("clone receipt schema, kind, or status differs")
    if (
        value["cell_id"] != cell.get("cell_id")
        or value["arm_sha256"] != _arm_sha256(arm)
        or value["attempt_id"] != _identity(attempt_id, "clone attempt identity")
    ):
        raise ValueError("clone receipt execution identity differs")
    if (
        value["base_receipt_sha256"] != receipt_document_sha256(base_receipt)
        or value["base_index_id"] != base_receipt.get("index_id")
        or value["base_index_uri"] != base_receipt.get("index_uri")
    ):
        raise ValueError("clone receipt base authority differs")
    expected_uri = clone_index_uri(cell, arm=arm, attempt_id=attempt_id)
    if value["clone_index_uri"] != expected_uri or value["clone_index_uri"] == value["base_index_uri"]:
        raise ValueError("clone receipt mutable URI differs")
    reference = value["copy_inventory_ref"]
    if not isinstance(reference, dict) or frozenset(reference) != INVENTORY_REF_FIELDS:
        raise ValueError("clone receipt inventory reference differs")
    for field in ("bytes", "objects", "total_bytes"):
        if isinstance(reference[field], bool) or not isinstance(reference[field], int) or reference[field] <= 0:
            raise ValueError("clone receipt inventory reference is invalid")
    _sha256(reference["checksum"], "clone inventory checksum")
    if (
        reference["path"] != "CLONE_OBJECTS.json"
        or value["verification_tier"] != "copy-source-if-match-etag-v1"
        or value["expires_after_days"] != 7
        or value["receipt_sha256"] != _receipt_sha256(value)
        or len(canonical_json_bytes(value) + b"\n") > MAX_CLONE_RECEIPT_BYTES
    ):
        raise ValueError("clone receipt authority is invalid")
    return copy.deepcopy(value)


def require_verified_clone_inventory(
    receipt: dict[str, object], payload: bytes, *, base_roster: list[dict[str, object]]
) -> list[dict[str, object]]:
    reference = receipt.get("copy_inventory_ref")
    if not isinstance(reference, dict):
        raise ValueError("clone receipt has no copy inventory")
    if len(payload) != reference.get("bytes") or hashlib.sha256(payload).hexdigest() != reference.get("checksum"):
        raise ValueError("clone copy evidence differs from its receipt")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("clone copy evidence is not valid JSON") from error
    if canonical_json_bytes(value) + b"\n" != payload:
        raise ValueError("clone copy evidence is not canonical")
    summary = _inventory_summary(value, base_roster=base_roster)
    if summary["objects"] != reference.get("objects") or summary["total_bytes"] != reference.get("total_bytes"):
        raise ValueError("clone copy evidence summary differs from its receipt")
    return copy.deepcopy(value)
