"""Exact fail-closed schema contract for current production query samples."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

V10_PRODUCTION_BENCH_SCHEMA_VERSION = "borsuk-production-bench-v10"
PRODUCTION_BENCH_SCHEMA_VERSION = "borsuk-production-bench-v14"
V10_QUERY_TELEMETRY_FIELDS = (
    "global_leaf_directory_reads",
    "global_leaf_directory_bytes",
    "global_leaf_pages_read",
    "global_leaf_page_bytes",
    "global_leaf_waves",
    "global_leaf_continuations",
    "global_leaf_exact_scores",
    "backing_reads",
    "backing_bytes_read",
)
V11_QUERY_TELEMETRY_FIELDS = (
    "global_leaf_directory_reads",
    "global_leaf_directory_bytes",
    "global_leaf_code_pages_read",
    "global_leaf_code_bytes",
    "global_leaf_pages_read",
    "global_leaf_page_bytes",
    "global_leaf_exact_cells",
    "global_leaf_exact_cards",
    "global_leaf_exact_groups",
    "global_leaf_exact_selected_bytes",
    "global_leaf_exact_speculative_bytes",
    "global_leaf_waves",
    "global_leaf_continuations",
    "global_leaf_exact_scores",
    "backing_reads",
    "backing_bytes_read",
)


def validate_v10_query_sample_rows(
    rows: Sequence[Mapping[str, object]], path: str | Path
) -> None:
    """Reject every query-sample row outside the exact current V10 contract."""

    for line, row in enumerate(rows, start=2):
        version = row.get("schema_version")
        if version != V10_PRODUCTION_BENCH_SCHEMA_VERSION:
            raise ValueError(
                f"{path}:{line} production benchmark schema {version!r}; "
                f"expected {V10_PRODUCTION_BENCH_SCHEMA_VERSION!r}"
            )
        missing = [
            field
            for field in V10_QUERY_TELEMETRY_FIELDS
            if field not in row or row[field] in (None, "")
        ]
        if missing:
            raise ValueError(
                f"{path}:{line} missing V10 telemetry: {', '.join(missing)}"
            )


def validate_v11_query_sample_rows(
    rows: Sequence[Mapping[str, object]], path: str | Path
) -> None:
    """Reject every query-sample row outside the current V11 contract."""

    for line, row in enumerate(rows, start=2):
        version = row.get("schema_version")
        if version != PRODUCTION_BENCH_SCHEMA_VERSION:
            raise ValueError(
                f"{path}:{line} production benchmark schema {version!r}; "
                f"expected {PRODUCTION_BENCH_SCHEMA_VERSION!r}"
            )
        missing = [
            field
            for field in V11_QUERY_TELEMETRY_FIELDS
            if field not in row or row[field] in (None, "")
        ]
        if missing:
            raise ValueError(
                f"{path}:{line} missing V11 telemetry: {', '.join(missing)}"
            )
