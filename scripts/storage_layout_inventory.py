#!/usr/bin/env python3
"""Validate BORSUK's checked persisted-object inventory."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

FIELDS = (
    "object_role",
    "current_format",
    "path_family",
    "writer",
    "reader",
    "access_patterns",
    "conditional_write",
    "checksum",
    "range_read",
    "format_candidates",
    "qualification_status",
)
REQUIRED_ROLES = {
    "catalog",
    "wal_run",
    "lane_head",
    "commit_marker",
    "routing_page",
    "graph_index",
    "normal_segment",
    "product_codes",
    "exact_vectors",
    "filter_index",
    "lexical_block",
    "late_interaction",
    "tombstone",
    "id_directory",
}
BOOLEAN_FIELDS = {"conditional_write", "checksum", "range_read"}


def load_and_validate(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != FIELDS:
            raise ValueError(f"inventory fields must be {','.join(FIELDS)}")
        rows = list(reader)
    roles = [row["object_role"] for row in rows]
    if len(roles) != len(set(roles)):
        raise ValueError("inventory contains duplicate object roles")
    if set(roles) != REQUIRED_ROLES:
        missing = sorted(REQUIRED_ROLES - set(roles))
        extra = sorted(set(roles) - REQUIRED_ROLES)
        raise ValueError(f"object-role mismatch: missing={missing}, extra={extra}")
    for row in rows:
        for field in FIELDS:
            if not row[field].strip():
                raise ValueError(f"{row['object_role']}: {field} must not be empty")
        for field in BOOLEAN_FIELDS:
            if row[field] not in {"yes", "no"}:
                raise ValueError(f"{row['object_role']}: {field} must be yes or no")
        if row["qualification_status"] not in {
            "current",
            "experimental",
            "not-implemented",
        }:
            raise ValueError(f"{row['object_role']}: invalid qualification_status")
    return sorted(rows, key=lambda row: row["object_role"])


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("inventory", type=Path)
    validate.add_argument("--output", type=Path)
    args = parser.parse_args()
    rows = load_and_validate(args.inventory)
    if args.output:
        args.output.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    print(f"valid storage object inventory: {len(rows)} roles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
