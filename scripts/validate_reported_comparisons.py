#!/usr/bin/env python3
"""Validate vendor/paper reported context without promoting it to direct evidence."""

from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

REQUIRED = (
    "evidence_id",
    "system",
    "evidence_class",
    "primary_source_url",
    "access_date",
    "dataset",
    "metric",
    "k",
    "cache_state",
    "latency_scope",
    "reported_value",
    "unit",
    "logical_cpus",
    "ram_bytes",
    "accelerator",
    "storage_class",
    "mismatch_reasons",
    "permitted_wording",
)
UNKNOWN = {"unknown", "not-reported", "not-applicable"}


def validate_rows(rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ValueError("reported comparison registry is empty")
    identifiers = set()
    for position, row in enumerate(rows, start=2):
        missing = [field for field in REQUIRED if not str(row.get(field, "")).strip()]
        if missing:
            raise ValueError(f"row {position} has empty required fields: {missing}")
        if row["evidence_id"] in identifiers:
            raise ValueError(f"duplicate evidence_id: {row['evidence_id']}")
        identifiers.add(row["evidence_id"])
        if row["evidence_class"] not in {"vendor-reported", "paper-reported"}:
            raise ValueError("reported registry cannot contain direct-controlled rows")
        parsed = urlparse(row["primary_source_url"])
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("primary_source_url must be a direct HTTPS source")
        date.fromisoformat(row["access_date"])
        float(row["reported_value"])
        comparison_fields = (
            row["dataset"],
            row["k"],
            row["latency_scope"],
            row["logical_cpus"],
            row["ram_bytes"],
            row["accelerator"],
            row["storage_class"],
        )
        has_unknown = any(value.lower() in UNKNOWN for value in comparison_fields)
        superiority = row["permitted_wording"] != "context-only"
        if has_unknown and superiority:
            raise ValueError("unknown comparability fields forbid superiority wording")
        if superiority:
            raise ValueError(
                "reported evidence is context-only; direct evidence owns claims"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "registry",
        nargs="?",
        type=Path,
        default=Path("docs/research/reported-comparisons.csv"),
    )
    args = parser.parse_args()
    with args.registry.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    validate_rows(rows)
    print(f"valid reported comparison registry: {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
