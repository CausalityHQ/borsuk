#!/usr/bin/env python3
"""GT-free approximate centroid page-admission screen on frozen SQ8 layouts."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np

from v138_unit_bound_feasibility import load_cohort, percentile

UNIT_ROWS = 32
PAGE_ROWS = 256
MAX_GETS = 32
MAX_BYTES = 16_777_216
D96_MEAN_BYTE_SCREEN = 4_714_063


def cover_pages(pages: np.ndarray, rows: int, row_bytes: int) -> tuple[list[list[int]], int]:
    """Minimum-byte cover of selected pages with at most MAX_GETS ranges."""
    if pages.size == 0:
        raise ValueError("primary pages must be admitted")
    selected = [int(page) for page in pages]
    runs: list[list[int]] = [[selected[0], selected[0] + 1]]
    for page in selected[1:]:
        if page == runs[-1][1]:
            runs[-1][1] += 1
        else:
            runs.append([page, page + 1])
    bridges = max(0, len(runs) - MAX_GETS)
    gaps = sorted((runs[i + 1][0] - runs[i][1], i) for i in range(len(runs) - 1))
    joined = {index for _, index in gaps[:bridges]}
    merged: list[list[int]] = [runs[0].copy()]
    for index, run in enumerate(runs[1:]):
        if index in joined:
            merged[-1][1] = run[1]
        else:
            merged.append(run.copy())
    ranges = [[first * PAGE_ROWS * row_bytes,
               min(last * PAGE_ROWS, rows) * row_bytes] for first, last in merged]
    if len(ranges) > MAX_GETS or any(first >= last for first, last in ranges):
        raise AssertionError("page cover construction differs")
    return ranges, sum(last - first for first, last in ranges)


def run(args: argparse.Namespace) -> None:
    rows, dimensions = args.rows, args.dimensions
    row_bytes = dimensions + 12
    unit_count = math.ceil(rows / UNIT_ROWS)
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (dimensions,))])
    if args.sq8.stat().st_size != rows * row_bytes:
        raise ValueError("SQ8 geometry differs")
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(rows,))
    low = np.fromfile(args.low, dtype="<f4")
    step = np.fromfile(args.step, dtype="<f4")
    if (low.size != dimensions or step.size != dimensions
            or not np.all(np.isfinite(low)) or not np.all(np.isfinite(step))
            or np.any(step <= 0)):
        raise ValueError("SQ8 coefficients differ")
    queries, primary = load_cohort(args.input, args.cohort)
    if (queries.shape != (1000, dimensions) or np.any(~np.isfinite(queries))
            or np.any(primary < 0) or np.any(primary >= rows)):
        raise ValueError("query or primary geometry differs")
    epsilon = float(np.linalg.norm(step.astype(np.float64) / 2))
    centers = np.empty((unit_count, dimensions), dtype=np.float16)
    for unit in range(unit_count):
        start = unit * UNIT_ROWS
        stop = min(rows, start + UNIT_ROWS)
        restored = low + sq8["code"][start:stop].astype(np.float32) * step
        centers[unit] = restored.mean(axis=0, dtype=np.float64).astype(np.float16)
    centers32 = centers.astype(np.float32)
    center_norms = np.einsum("ij,ij->i", centers32, centers32)
    records = []
    with args.raw.open("x") as output:
        for start in range(0, queries.shape[0], 32):
            batch = queries[start:start + 32]
            query_norms = np.einsum("ij,ij->i", batch, batch)
            distances = np.sqrt(np.maximum(
                0.0, query_norms[:, None] + center_norms[None, :]
                - 2.0 * (batch @ centers32.T)))
            for offset, query in enumerate(batch):
                ordinal = start + offset
                witnesses = low + sq8["code"][primary[ordinal]].astype(np.float32) * step
                threshold = float(np.linalg.norm(
                    witnesses.astype(np.float64) - query.astype(np.float64), axis=1).max()
                    + epsilon + 0.001)
                admitted_units = np.flatnonzero(distances[offset] <= threshold)
                admitted_pages = np.unique(np.concatenate((
                    admitted_units // (PAGE_ROWS // UNIT_ROWS),
                    primary[ordinal] // PAGE_ROWS)))
                ranges, charged = cover_pages(admitted_pages, rows, row_bytes)
                record = {
                    "query_ordinal": ordinal,
                    "witness_threshold": threshold,
                    "admitted_units": int(admitted_units.size),
                    "admitted_pages": int(admitted_pages.size),
                    "primary_pages": int(np.unique(primary[ordinal] // PAGE_ROWS).size),
                    "ranges": ranges,
                    "minimum_gets": len(ranges),
                    "minimum_bytes_at_32_gets": charged,
                    "fits_get_byte_cap": charged <= MAX_BYTES,
                }
                records.append(record)
                output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    mean_bytes = sum(row["minimum_bytes_at_32_gets"] for row in records) / len(records)
    fits = sum(row["fits_get_byte_cap"] for row in records)
    summary = {
        "schema": "borsuk-v139-centroid-page-screen-v1",
        "cohort": args.cohort, "rows": rows, "dimensions": dimensions,
        "query_count": len(records), "unit_count": unit_count,
        "unit_rows": UNIT_ROWS, "page_rows": PAGE_ROWS,
        "quantization_error_envelope": epsilon,
        "f16_centroid_payload_bytes": unit_count * 2 * dimensions,
        "fits_count": fits, "mean_minimum_bytes": mean_bytes,
        "p50_admitted_units": percentile([r["admitted_units"] for r in records], 50),
        "p95_admitted_units": percentile([r["admitted_units"] for r in records], 95),
        "p50_admitted_pages": percentile([r["admitted_pages"] for r in records], 50),
        "p95_admitted_pages": percentile([r["admitted_pages"] for r in records], 95),
        "p50_minimum_bytes": percentile([r["minimum_bytes_at_32_gets"] for r in records], 50),
        "p95_minimum_bytes": percentile([r["minimum_bytes_at_32_gets"] for r in records], 95),
        "max_minimum_bytes": max(r["minimum_bytes_at_32_gets"] for r in records),
        "stop_centroid_policy": (fits < 950 or (
            args.cohort == "deep-image-96-angular-random100k"
            and mean_bytes > D96_MEAN_BYTE_SCREEN)),
    }
    args.summary.write_text(json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", choices=("deep-image-96-angular-random100k", "ReLAION-1M"), required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--sq8", type=Path, required=True)
    parser.add_argument("--low", type=Path, required=True)
    parser.add_argument("--step", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    if ((args.cohort == "deep-image-96-angular-random100k"
         and (args.rows, args.dimensions) != (100_000, 96))
        or (args.cohort == "ReLAION-1M"
            and (args.rows, args.dimensions) != (1_000_000, 768))):
        parser.error("frozen cohort geometry differs")
    run(args)


if __name__ == "__main__":
    main()
