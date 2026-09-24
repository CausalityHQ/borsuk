#!/usr/bin/env python3
"""GT-free primary-first centroid-page ranking with a hard S3 range budget."""

from __future__ import annotations

import argparse
import json
import math
from contextlib import ExitStack
from pathlib import Path

import numpy as np

from v138_unit_bound_feasibility import load_cohort, percentile
from v139_centroid_page_screen import cover_pages

UNIT_ROWS = 32
PAGE_ROWS = 256
PAGE_UNITS = PAGE_ROWS // UNIT_ROWS
MAX_BYTES = 16_777_216
BETAS = (1, 2, 4, 8)


def choose_pages(
    page_scores: np.ndarray, primary: np.ndarray, rows: int, row_bytes: int
) -> dict:
    page_count = len(page_scores)
    first_primary_rank: dict[int, int] = {}
    for rank, ordinal in enumerate(primary):
        first_primary_rank.setdefault(int(ordinal) // PAGE_ROWS, rank)
    primary_pages = set(first_primary_rank)
    primary_order = sorted(first_primary_rank, key=lambda page: (first_primary_rank[page], page))
    score_order = np.lexsort((np.arange(page_count), page_scores))
    score_ranks = np.empty(page_count, dtype=np.int32)
    score_ranks[score_order] = np.arange(1, page_count + 1, dtype=np.int32)
    targets = {beta: min(page_count, beta * len(primary_pages)) for beta in BETAS}
    selected: set[int] = set()
    snapshots: dict[str, dict] = {}
    skipped_over_cap = 0

    def snapshot(target: int) -> dict:
        pages = np.asarray(sorted(selected), dtype=np.int64)
        ranges, charged = cover_pages(pages, rows, row_bytes)
        return {
            "target_pages": target,
            "selected_pages": pages.tolist(),
            "selected_page_count": len(pages),
            "target_shortfall": max(0, target - len(pages)),
            "primary_pages_retained": len(selected & primary_pages),
            "ranges": ranges,
            "gets": len(ranges),
            "planned_bytes": charged,
            "fits_cap": charged <= MAX_BYTES and len(ranges) <= 32,
        }

    def ordered_pages():
        yield from primary_order
        for ordinal in score_order:
            page = int(ordinal)
            if page not in primary_pages:
                yield page

    for page in ordered_pages():
        if page in selected:
            continue
        proposed = np.asarray(sorted(selected | {page}), dtype=np.int64)
        _, charged = cover_pages(proposed, rows, row_bytes)
        if charged > MAX_BYTES:
            skipped_over_cap += 1
            continue
        selected.add(page)
        for beta, target in targets.items():
            key = str(beta)
            if key not in snapshots and len(selected) >= target:
                snapshots[key] = snapshot(target)
        if len(selected) >= targets[BETAS[-1]]:
            break
    for beta, target in targets.items():
        if str(beta) not in snapshots:
            snapshots[str(beta)] = snapshot(target)
    return {
        "primary_page_count": len(primary_pages),
        "worst_primary_page_score_rank": max(int(score_ranks[page]) for page in primary_pages),
        "pages_rejected_over_cap": skipped_over_cap,
        "variants": snapshots,
    }


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
    centers = np.empty((unit_count, dimensions), dtype=np.float16)
    for unit in range(unit_count):
        start, stop = unit * UNIT_ROWS, min(rows, (unit + 1) * UNIT_ROWS)
        restored = low + sq8["code"][start:stop].astype(np.float32) * step
        centers[unit] = restored.mean(axis=0, dtype=np.float64).astype(np.float16)
    centers32 = centers.astype(np.float32)
    center_norms = np.einsum("ij,ij->i", centers32, centers32)
    records = []
    with ExitStack() as files:
        output = files.enter_context(args.raw.open("x"))
        scores_output = (files.enter_context(args.scores_output.open("xb"))
                         if args.scores_output else None)
        for start in range(0, 1000, 32):
            batch = queries[start:start + 32]
            query_norms = np.einsum("ij,ij->i", batch, batch)
            distances = np.sqrt(np.maximum(
                0.0, query_norms[:, None] + center_norms[None, :]
                - 2.0 * (batch @ centers32.T)))
            for offset in range(len(batch)):
                ordinal = start + offset
                page_scores = np.minimum.reduceat(
                    distances[offset], np.arange(0, unit_count, PAGE_UNITS))
                if not np.all(np.isfinite(page_scores)):
                    raise ValueError("centroid page score is not finite")
                if scores_output is not None:
                    scores_output.write(page_scores.astype("<f4", copy=False).tobytes())
                result = choose_pages(page_scores, primary[ordinal], rows, row_bytes)
                result["query_ordinal"] = ordinal
                records.append(result)
                output.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    summaries = {}
    primary_total = sum(row["primary_page_count"] for row in records)
    for beta in BETAS:
        key = str(beta)
        variants = [row["variants"][key] for row in records]
        byte_values = [row["planned_bytes"] for row in variants]
        get_values = [row["gets"] for row in variants]
        retention = [variant["primary_pages_retained"] / row["primary_page_count"]
                     for row, variant in zip(records, variants)]
        summaries[key] = {
            "mean_planned_bytes": sum(byte_values) / 1000,
            "p50_planned_bytes": percentile(byte_values, 50),
            "p95_planned_bytes": percentile(byte_values, 95),
            "p99_planned_bytes": percentile(byte_values, 99),
            "mean_gets": sum(get_values) / 1000,
            "p50_gets": percentile(get_values, 50),
            "p95_gets": percentile(get_values, 95),
            "p99_gets": percentile(get_values, 99),
            "p50_selected_pages": percentile([v["selected_page_count"] for v in variants], 50),
            "p95_selected_pages": percentile([v["selected_page_count"] for v in variants], 95),
            "p05_primary_retention": sorted(retention)[49],
            "primary_pages_retained": sum(v["primary_pages_retained"] for v in variants),
            "primary_pages_total": primary_total,
            "queries_all_primary_retained": sum(
                v["primary_pages_retained"] == row["primary_page_count"]
                for row, v in zip(records, variants)),
            "queries_with_target_shortfall": sum(v["target_shortfall"] > 0 for v in variants),
            "max_target_shortfall": max(v["target_shortfall"] for v in variants),
            "cap_violations": sum(not v["fits_cap"] for v in variants),
        }
    summary = {
        "schema": "borsuk-v140-budgeted-centroid-page-rank-v1",
        "cohort": args.cohort, "rows": rows, "dimensions": dimensions,
        "query_count": 1000, "unit_rows": UNIT_ROWS, "page_rows": PAGE_ROWS,
        "unit_count": unit_count, "page_count": math.ceil(rows / PAGE_ROWS),
        "f16_centroid_payload_bytes": unit_count * 2 * dimensions,
        "p50_worst_primary_score_rank": percentile(
            [row["worst_primary_page_score_rank"] for row in records], 50),
        "p95_worst_primary_score_rank": percentile(
            [row["worst_primary_page_score_rank"] for row in records], 95),
        "variants": summaries,
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
    parser.add_argument("--scores-output", type=Path)
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
