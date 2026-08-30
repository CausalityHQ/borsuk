#!/usr/bin/env python3
"""Fail-closed structural admission for Publication V3 result trees."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

if __package__:
    from scripts.publication_v3_protocol import (
        _read_canonical_json,
        _validate_environment,
        canonical_json_bytes,
        read_protocol,
        validate_manifest,
        validate_schedule_for_manifest,
    )
else:
    from publication_v3_protocol import (  # type: ignore[no-redef]
        _read_canonical_json,
        _validate_environment,
        canonical_json_bytes,
        read_protocol,
        validate_manifest,
        validate_schedule_for_manifest,
    )


ROOT_FILES = frozenset(
    {
        "manifest.json",
        "schedule.json",
        "environment.json",
        "source-archive.tar.gz",
        "cells",
        "PUBLICATION_V3_COMPLETE",
    }
)
CELL_FILES = frozenset({"protocol.json", "CELL_COMPLETE"})
CELL_MARKER_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "cell_id",
        "manifest_sha256",
        "protocol_sha256",
    }
)
ROOT_MARKER_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "manifest_sha256",
        "schedule_sha256",
        "complete_cells",
    }
)


def _require_exact_fields(
    value: dict[str, object], expected: frozenset[str], role: str
) -> None:
    actual = frozenset(value)
    if actual != expected:
        raise ValueError(
            f"{role} fields differ: missing={sorted(expected - actual)} "
            f"unknown={sorted(actual - expected)}"
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _exact_children(path: Path, expected: frozenset[str], role: str) -> None:
    if not path.is_dir() or path.is_symlink():
        raise ValueError(f"{role} is not a plain directory")
    actual = frozenset(child.name for child in path.iterdir())
    if actual != expected:
        raise ValueError(
            f"{role} entries differ: missing={sorted(expected - actual)} "
            f"unknown={sorted(actual - expected)}"
        )


def validate_structural_tree(
    root: Path, expected_source_sha256: str | None = None
) -> dict[str, object]:
    root = root.resolve()
    _exact_children(root, ROOT_FILES, "result root")
    manifest = validate_manifest(
        _read_canonical_json(root / "manifest.json", 256 * 1024)
    )
    schedule_value = _read_canonical_json(root / "schedule.json", 64 * 1024 * 1024)
    schedule = validate_schedule_for_manifest(schedule_value, manifest)
    environment = _validate_environment(
        _read_canonical_json(root / "environment.json", 256 * 1024)
    )
    if environment != manifest["environment_contract"]:
        raise ValueError("environment.json differs from manifest")

    source_sha256 = _sha256_file(root / "source-archive.tar.gz")
    if source_sha256 != manifest["source"].get("archive_sha256"):
        raise ValueError("source archive checksum differs from manifest")
    if expected_source_sha256 is not None and source_sha256 != expected_source_sha256:
        raise ValueError("source archive checksum differs from expected checksum")

    expected = {str(cell["cell_id"]): cell for cell in schedule["cells"]}
    cells_root = root / "cells"
    _exact_children(cells_root, frozenset(expected), "cell directory")
    for cell_id, cell in expected.items():
        cell_root = cells_root / cell_id
        _exact_children(cell_root, CELL_FILES, f"cell {cell_id}")
        protocol_path = cell_root / "protocol.json"
        protocol = read_protocol(protocol_path)
        if canonical_json_bytes(protocol) != canonical_json_bytes(cell):
            raise ValueError(f"cell {cell_id} protocol differs from schedule")
        marker = _read_canonical_json(cell_root / "CELL_COMPLETE", 16 * 1024)
        _require_exact_fields(marker, CELL_MARKER_FIELDS, f"cell {cell_id} marker")
        expected_marker = {
            "schema_version": 1,
            "status": "complete",
            "cell_id": cell_id,
            "manifest_sha256": schedule["manifest_sha256"],
            "protocol_sha256": _sha256_file(protocol_path),
        }
        if marker != expected_marker:
            raise ValueError(f"cell {cell_id} marker differs from protocol")

    root_marker = _read_canonical_json(root / "PUBLICATION_V3_COMPLETE", 16 * 1024)
    _require_exact_fields(root_marker, ROOT_MARKER_FIELDS, "root marker")
    expected_root_marker = {
        "schema_version": 1,
        "status": "complete",
        "manifest_sha256": schedule["manifest_sha256"],
        "schedule_sha256": _sha256_file(root / "schedule.json"),
        "complete_cells": len(expected),
    }
    if root_marker != expected_root_marker:
        raise ValueError("root marker differs from schedule")
    return {
        "status": "structurally-valid",
        "scheduled_cells": len(expected),
        "complete_cells": len(expected),
        "manifest_sha256": schedule["manifest_sha256"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--structural-only", action="store_true", required=True)
    parser.add_argument("--expected-source-sha256")
    args = parser.parse_args()
    summary = validate_structural_tree(args.root, args.expected_source_sha256)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
