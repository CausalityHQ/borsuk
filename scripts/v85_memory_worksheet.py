#!/usr/bin/env python3
"""Executable 100M resident-memory worksheet for the V85 serving design."""

from __future__ import annotations

import argparse
import json


def project_v85_resident_memory(
    *, resident_exact_delta_row_bytes: int = 0
) -> dict[str, int | bool | str]:
    """Project the complete bounded resident footprint at 100M rows.

    Base and delta PQ16 codes are page ordered, so row ordinal determines the
    S3 page and no ID-to-page dictionary is resident.  The mutation directory
    is a sorted structure-of-arrays capped at 32 bytes per changed ID.  Exact
    vectors and page bodies remain in S3.
    """

    if (
        type(resident_exact_delta_row_bytes) is not int
        or resident_exact_delta_row_bytes < 0
    ):
        raise ValueError("resident exact delta row bytes differ")

    collection_rows = 100_000_000
    maximum_delta_rows = 8_000_000
    base_rows = collection_rows - maximum_delta_rows
    page_rows = 256
    pq_bytes_per_row = 16
    mutation_entry_bytes = 32
    range_concurrency = 16
    sparse_planner_bytes_per_query = 16 * 1024**2
    metadata_reserve_bytes = 32 * 1024**2
    runtime_reserve_bytes = 512 * 1024**2
    ram_budget_bytes = 3 * 1024**3

    router_codes_bytes = collection_rows * pq_bytes_per_row
    mutation_directory_bytes = maximum_delta_rows * mutation_entry_bytes
    base_pages = (base_rows + page_rows - 1) // page_rows
    delta_pages = (maximum_delta_rows + page_rows - 1) // page_rows
    page_directories_bytes = (base_pages + delta_pages + 2) * 8
    pq16_codebooks_bytes = 16 * 256 * (768 // 16) * 4
    concurrent_sparse_planner_bytes = range_concurrency * sparse_planner_bytes_per_query
    resident_exact_delta_bytes = maximum_delta_rows * resident_exact_delta_row_bytes
    projected_resident_bytes = sum(
        (
            router_codes_bytes,
            mutation_directory_bytes,
            page_directories_bytes,
            pq16_codebooks_bytes,
            metadata_reserve_bytes,
            concurrent_sparse_planner_bytes,
            runtime_reserve_bytes,
            resident_exact_delta_bytes,
        )
    )

    maximum_ranges = 32
    maximum_span_pages = 83
    dense_traceback_bytes = (
        2 * (base_pages + delta_pages) * (maximum_ranges + 1) * (maximum_span_pages + 1)
    )
    dense_planner_projected_bytes = (
        projected_resident_bytes
        - concurrent_sparse_planner_bytes
        + dense_traceback_bytes
    )

    return {
        "base_id_to_page_bytes": 0,
        "base_rows": base_rows,
        "collection_rows": collection_rows,
        "concurrent_sparse_planner_bytes": concurrent_sparse_planner_bytes,
        "dense_planner_feasible": dense_planner_projected_bytes <= ram_budget_bytes,
        "dense_planner_projected_bytes": dense_planner_projected_bytes,
        "dense_traceback_bytes_per_query": dense_traceback_bytes,
        "delta_page_bodies": "s3-only",
        "feasible": projected_resident_bytes <= ram_budget_bytes,
        "headroom_bytes": ram_budget_bytes - projected_resident_bytes,
        "maximum_delta_rows": maximum_delta_rows,
        "metadata_reserve_bytes": metadata_reserve_bytes,
        "mutation_directory_bytes": mutation_directory_bytes,
        "mutation_directory_entry_bytes": mutation_entry_bytes,
        "page_directories_bytes": page_directories_bytes,
        "page_rows": page_rows,
        "pq16_codebooks_bytes": pq16_codebooks_bytes,
        "projected_resident_bytes": projected_resident_bytes,
        "qualification_state": "feasible-only-sparse-planner-not-implemented",
        "ram_budget_bytes": ram_budget_bytes,
        "range_concurrency": range_concurrency,
        "resident_exact_delta_bytes": resident_exact_delta_bytes,
        "router_codes_bytes": router_codes_bytes,
        "runtime_reserve_bytes": runtime_reserve_bytes,
        "schema": "borsuk-v85-memory-worksheet-v1",
        "workspace_boundary": "sparse-touched-page-planner-only",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true")
    args = parser.parse_args()
    if not args.print:
        parser.error("--print is required")
    print(
        json.dumps(project_v85_resident_memory(), separators=(",", ":"), sort_keys=True)
    )


if __name__ == "__main__":
    main()
