#!/usr/bin/env python3
"""Exact truth-aware upper bound for 32 SQ8 ranges and 16 MiB on V63 pages.

This is an offline physical feasibility diagnostic. It may inspect GT100 and
must never be used to route a serving query. Each full page is 256 rows and
the final short page is 64 rows, so one 64-row unit costs exactly 49,920
bytes. The byte cap therefore admits at most 336 units without rounding.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np

ROWS = 1_000_000
PAGE_ROWS = 256
PAGE_COUNT = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS
SQ8_ROW_BYTES = 780
MAX_GETS = 32
MAX_BYTES = 16_777_216
UNIT_ROWS = 64
UNIT_BYTES = UNIT_ROWS * SQ8_ROW_BYTES
MAX_UNITS = MAX_BYTES // UNIT_BYTES
FULL_PAGE_UNITS = PAGE_ROWS // UNIT_ROWS
LAST_PAGE_UNITS = (ROWS - (PAGE_COUNT - 1) * PAGE_ROWS) // UNIT_ROWS
SOURCE_SHA256 = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
TRUTH_SHA256 = "fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11"
ORDER_SHA256 = "32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def optimal_truth_hits(
    page_weights: dict[int, int], *, page_count: int, max_gets: int,
    max_units: int, full_page_units: int, last_page_units: int,
) -> int:
    """Maximize covered truth hits over all disjoint physical page intervals.

    A state records completed GET count, paid page units, and whether an
    interval is open after the previous truth page. At each truth page, a
    feasible plan may skip it, extend the open interval through the gap, or
    start a new interval. These exhaust possible useful interval endpoints.
    """
    if (page_count <= 0 or max_gets <= 0 or max_units < 0
        or full_page_units <= 0 or last_page_units <= 0
        or any(page < 0 or page >= page_count or weight <= 0
               for page, weight in page_weights.items())
        or sum(page_weights.values()) >= 2**30):
        raise ValueError("oracle geometry or truth weights differ")
    negative = -2**30
    shape = (max_gets + 1, max_units + 1)
    closed = np.full(shape, negative, dtype=np.int32)
    opened = np.full(shape, negative, dtype=np.int32)
    closed[0, 0] = 0
    previous_page = None
    for page, weight in sorted(page_weights.items()):
        page_units = last_page_units if page == page_count - 1 else full_page_units
        gap_units = 0 if previous_page is None else (page - previous_page - 1) * full_page_units
        base = np.maximum(closed, opened)
        next_open = np.full(shape, negative, dtype=np.int32)
        if page_units <= max_units:
            next_open[1:, page_units:] = np.maximum(
                next_open[1:, page_units:],
                base[:-1, :max_units + 1 - page_units] + weight,
            )
        continue_units = gap_units + page_units
        if continue_units <= max_units:
            next_open[:, continue_units:] = np.maximum(
                next_open[:, continue_units:],
                opened[:, :max_units + 1 - continue_units] + weight,
            )
        closed, opened = base, next_open
        previous_page = page
    return int(max(closed.max(), opened.max()))


def truth_pages(source: Path, truth: Path, layout: Path) -> np.ndarray:
    import pyarrow.parquet as pq

    source_ids = pq.read_table(source, columns=["feature_row_id"])["feature_row_id"].combine_chunks().to_numpy()
    ground = pq.read_table(truth, columns=["query_ordinal", "rank", "feature_row_id"])
    truth_ids = ground["feature_row_id"].combine_chunks().to_numpy()
    query_ordinals = ground["query_ordinal"].combine_chunks().to_numpy()
    ranks = ground["rank"].combine_chunks().to_numpy()
    order = np.load(layout)
    if (len(source_ids) != ROWS or len(truth_ids) != 100_000
        or order.shape != (ROWS,) or not np.array_equal(np.sort(order), np.arange(ROWS))
        or not np.array_equal(query_ordinals, np.repeat(np.arange(1000), 100))
        or not np.array_equal(ranks, np.tile(np.arange(100), 1000))):
        raise ValueError("source, layout, or GT100 roster differs")
    physical_ids = source_ids[order]
    sorted_rows = np.argsort(physical_ids, kind="stable")
    sorted_ids = physical_ids[sorted_rows]
    if np.any(sorted_ids[1:] == sorted_ids[:-1]):
        raise ValueError("source feature IDs are not unique")
    positions = np.searchsorted(sorted_ids, truth_ids)
    if np.any(positions >= ROWS) or not np.array_equal(sorted_ids[positions], truth_ids):
        raise ValueError("ground truth references unknown source IDs")
    return (sorted_rows[positions] // PAGE_ROWS).reshape(1000, 100).astype(np.int32)


def run(source: Path, truth: Path, layout: Path, output: Path) -> None:
    for path, expected in ((source, SOURCE_SHA256), (truth, TRUTH_SHA256),
                           (layout, ORDER_SHA256)):
        if sha256_file(path) != expected:
            raise ValueError(f"input SHA-256 differs: {path.name}")
    pages = truth_pages(source, truth, layout)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("w") as handle:
        header = {"schema": "borsuk-v110-physical-oracle-v1", "query_count": 1000,
                  "source_sha256": SOURCE_SHA256, "truth_sha256": TRUTH_SHA256,
                  "layout_sha256": ORDER_SHA256, "max_gets": MAX_GETS,
                  "max_bytes": MAX_BYTES, "unit_bytes": UNIT_BYTES,
                  "max_units": MAX_UNITS, "oracle_kind": "truth-aware-upper-bound"}
        handle.write(json.dumps(header, sort_keys=True, separators=(",", ":")) + "\n")
        for ordinal, row in enumerate(pages):
            unique, counts = np.unique(row, return_counts=True)
            weights = dict(zip(map(int, unique), map(int, counts)))
            best = optimal_truth_hits(
                weights, page_count=PAGE_COUNT, max_gets=MAX_GETS,
                max_units=MAX_UNITS, full_page_units=FULL_PAGE_UNITS,
                last_page_units=LAST_PAGE_UNITS,
            )
            if not 0 <= best <= 100:
                raise AssertionError("oracle score outside GT100")
            handle.write(json.dumps({"query_ordinal": ordinal, "best_hits": best,
                                     "truth_page_weights": weights},
                                    sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--truth", type=Path, required=True)
    parser.add_argument("--layout", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(args.source, args.truth, args.layout, args.output)


if __name__ == "__main__":
    main()
