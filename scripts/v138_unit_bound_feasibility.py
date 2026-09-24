#!/usr/bin/env python3
"""Source-only SQ8 unit-bound feasibility screen; no GT or returned IDs."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np

UNIT_ROWS = 32
PAGE_ROWS = 256
MAX_GETS = 32
MAX_BYTES = 16_777_216
ROUNDING_MARGIN = 0.001
D96_MEAN_BYTE_SCREEN = 4_714_063


def percentile(values: list[int | float], percent: int) -> int | float:
    return sorted(values)[math.ceil(len(values) * percent / 100) - 1]


def minimum_cover_bytes(pages: np.ndarray, rows: int, row_bytes: int) -> tuple[int, int, int]:
    if pages.size == 0:
        return 0, 0, 0
    last_page = (rows - 1) // PAGE_ROWS
    full_bytes = PAGE_ROWS * row_bytes
    selected_bytes = int(pages.size) * full_bytes
    if pages[-1] == last_page:
        selected_bytes -= PAGE_ROWS * (row_bytes) - (rows - last_page * PAGE_ROWS) * row_bytes
    gaps = [int(right - left - 1) for left, right in zip(pages[:-1], pages[1:])
            if right > left + 1]
    runs = len(gaps) + 1
    bridges = max(0, runs - MAX_GETS)
    return selected_bytes + sum(sorted(gaps)[:bridges]) * full_bytes, min(runs, MAX_GETS), runs


def load_cohort(root: Path, name: str) -> tuple[np.ndarray, np.ndarray]:
    if name == "deep-image-96-angular-random100k":
        requests = [json.loads(line) for line in (root / "queries.jsonl").open()]
        sealed = [json.loads(line) for line in (root / "evidence.jsonl").open()]
    else:
        requests = [json.loads(line) for line in (root / "requests.jsonl").open()]
        sealed = [json.loads(line) for line in (root / "rust-replay.jsonl").open()]
    if len(requests) != 1000 or len(sealed) != 1000:
        raise ValueError("sealed cohort count differs")
    queries = []
    primary = []
    for ordinal, (request, record) in enumerate(zip(requests, sealed)):
        if request["query_ordinal"] != ordinal or record["query_ordinal"] != ordinal:
            raise ValueError("sealed query ordinal differs")
        if name == "deep-image-96-angular-random100k" and (
            request["source_query_ordinal"] != 9000 + ordinal
            or record["source_query_ordinal"] != 9000 + ordinal
        ):
            raise ValueError("deep-image source query ordinal differs")
        if len(record["primary"]) != 100 or len(set(record["primary"])) != 100:
            raise ValueError("sealed primary inventory differs")
        queries.append(request["query"])
        primary.append(record["primary"])
    return np.asarray(queries, dtype=np.float32), np.asarray(primary, dtype=np.int64)


def run(args: argparse.Namespace) -> None:
    rows = args.rows
    dimensions = args.dimensions
    row_bytes = dimensions + 12
    unit_count = math.ceil(rows / UNIT_ROWS)
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (dimensions,))])
    if args.sq8.stat().st_size != rows * row_bytes:
        raise ValueError("SQ8 object geometry differs")
    sq8 = np.memmap(args.sq8, dtype=dtype, mode="r", shape=(rows,))
    low = np.fromfile(args.low, dtype="<f4")
    step = np.fromfile(args.step, dtype="<f4")
    if low.size != dimensions or step.size != dimensions or not np.all(np.isfinite(low)) or not np.all(np.isfinite(step)) or np.any(step <= 0):
        raise ValueError("SQ8 coefficients differ")
    queries, primary = load_cohort(args.input, args.cohort)
    if queries.shape != (1000, dimensions) or np.any(~np.isfinite(queries)) or np.any(primary < 0) or np.any(primary >= rows):
        raise ValueError("query or primary geometry differs")
    epsilon = float(np.linalg.norm(step.astype(np.float64) / 2))
    centers = np.empty((unit_count, dimensions), dtype=np.float16)
    radii = np.empty(unit_count, dtype=np.float32)
    for unit in range(unit_count):
        start = unit * UNIT_ROWS
        stop = min(rows, start + UNIT_ROWS)
        restored = low + sq8["code"][start:stop].astype(np.float32) * step
        center = restored.mean(axis=0, dtype=np.float64).astype(np.float16)
        center32 = center.astype(np.float32)
        radius = float(np.linalg.norm(restored.astype(np.float64) - center32, axis=1).max())
        centers[unit] = center
        radii[unit] = radius + epsilon + ROUNDING_MARGIN
    centers32 = centers.astype(np.float32)
    center_norms = np.einsum("ij,ij->i", centers32, centers32)
    records = []
    with args.raw.open("x") as output:
        for start in range(0, queries.shape[0], 32):
            batch = queries[start:start + 32]
            query_norms = np.einsum("ij,ij->i", batch, batch)
            distances = np.sqrt(np.maximum(0.0, query_norms[:, None] + center_norms[None, :]
                                               - 2.0 * (batch @ centers32.T)))
            for offset, query in enumerate(batch):
                ordinal = start + offset
                witnesses = low + sq8["code"][primary[ordinal]].astype(np.float32) * step
                threshold = float(np.linalg.norm(witnesses.astype(np.float64)
                                                 - query.astype(np.float64), axis=1).max()
                                  + epsilon + ROUNDING_MARGIN)
                promising = distances[offset] - radii <= threshold
                unit_ids = np.flatnonzero(promising)
                page_ids = np.unique(unit_ids // (PAGE_ROWS // UNIT_ROWS))
                minimum_bytes, min_gets, unbridged_gets = minimum_cover_bytes(
                    page_ids, rows, row_bytes)
                record = {
                    "query_ordinal": ordinal,
                    "witness_threshold": threshold,
                    "promising_units": int(unit_ids.size),
                    "promising_pages": int(page_ids.size),
                    "unbridged_gets": unbridged_gets,
                    "minimum_gets": min_gets,
                    "minimum_bytes_at_32_gets": minimum_bytes,
                    "fits_get_byte_cap": minimum_bytes <= MAX_BYTES and min_gets <= MAX_GETS,
                }
                records.append(record)
                output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    fits_count = sum(row["fits_get_byte_cap"] for row in records)
    mean_minimum_bytes = sum(row["minimum_bytes_at_32_gets"] for row in records) / len(records)
    summary = {
        "schema": "borsuk-v138-unit-bound-feasibility-v1",
        "cohort": args.cohort, "rows": rows, "dimensions": dimensions,
        "unit_rows": UNIT_ROWS, "page_rows": PAGE_ROWS,
        "unit_count": unit_count, "query_count": len(records),
        "quantization_error_envelope": epsilon,
        "rounding_margin": ROUNDING_MARGIN,
        "f16_centroid_f32_radius_payload_bytes": unit_count * (2 * dimensions + 4),
        "fits_count": fits_count,
        "mean_minimum_bytes": mean_minimum_bytes,
        "p50_promising_units": percentile([row["promising_units"] for row in records], 50),
        "p95_promising_units": percentile([row["promising_units"] for row in records], 95),
        "p50_promising_pages": percentile([row["promising_pages"] for row in records], 50),
        "p95_promising_pages": percentile([row["promising_pages"] for row in records], 95),
        "p50_minimum_bytes": percentile([row["minimum_bytes_at_32_gets"] for row in records], 50),
        "p95_minimum_bytes": percentile([row["minimum_bytes_at_32_gets"] for row in records], 95),
        "max_minimum_bytes": max(row["minimum_bytes_at_32_gets"] for row in records),
        "stop_primary_exact_policy": (
            fits_count < 950
            or (args.cohort == "deep-image-96-angular-random100k"
                and mean_minimum_bytes > D96_MEAN_BYTE_SCREEN)
        ),
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
    if (args.cohort == "deep-image-96-angular-random100k" and (args.rows, args.dimensions) != (100_000, 96)) or (args.cohort == "ReLAION-1M" and (args.rows, args.dimensions) != (1_000_000, 768)):
        parser.error("frozen cohort geometry differs")
    run(args)


if __name__ == "__main__":
    main()
