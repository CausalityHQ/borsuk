#!/usr/bin/env python3
"""S3 request profile and rerank necessity for the V65/V66 two-stage design.

Everything through V66 counted bytes and pages. Neither is what an object store
charges for: it charges per request, and a request is a contiguous range. This
measures the two numbers that actually decide whether the design is servable.

1. How many *coalesced ranges* the router's selected pages form, swept over a
   gap-merge threshold, for both stages. Pages are contiguous in the k-means
   layout, so adjacent selections merge into one GET at the cost of reading the
   gap between them.
2. Whether stage two is needed at all. V65 showed SQ8 and PQ192 rank a shortlist
   as well as exact f32 does. If the top-100 taken straight from code scores
   already clears the bar, the exact round trip disappears and the design
   becomes one round trip.

Recall reported for the no-rerank path is true returned Recall@100 against the
exact ground truth, not containment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

ROWS = 1_000_000
QUERIES = 1_000
NEIGHBORS = 100
DIMENSIONS = 768

PAGE_ROWS = 256
ROUTER_BLOCKS_PER_PAGE = 2
ROUTER_PAGE_BUDGETS = (128, 256, 512)
SHORTLIST = 512
GAP_PAGES = (0, 1, 2, 4, 8, 16, 32)
CHUNK_ROWS = 16_384
QUERY_CHUNK = 50

CODE_ROW_BYTES = {"pq192x8": 208, "sq8": 784}
EXACT_ROW_BYTES = 16 + DIMENSIONS * 4


def fixed_list(table, name, width, rows):
    values = table[name].combine_chunks().values.to_numpy(zero_copy_only=False)
    result = np.array(values, dtype=np.float32, copy=True).reshape(rows, width)
    if not np.isfinite(result).all():
        raise ValueError(f"{name} differs")
    return result


def scalar(table, name, dtype):
    return np.asarray(
        table[name].combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype
    )


def load_truth_rows(source_path: Path, truth_path: Path) -> np.ndarray:
    source = pq.read_table(source_path, columns=["feature_row_id"])
    truth = pq.read_table(truth_path, columns=["query_ordinal", "rank", "feature_row_id"])
    if source.num_rows != ROWS or truth.num_rows != QUERIES * NEIGHBORS:
        raise ValueError("row count differs")
    feature_ids = scalar(source, "feature_row_id", np.uint64)
    if not np.array_equal(
        scalar(truth, "query_ordinal", np.int64), np.repeat(np.arange(QUERIES), NEIGHBORS)
    ) or not np.array_equal(
        scalar(truth, "rank", np.int64), np.tile(np.arange(NEIGHBORS), QUERIES)
    ):
        raise ValueError("ground-truth order differs")
    truth_ids = scalar(truth, "feature_row_id", np.uint64).reshape(QUERIES, NEIGHBORS)
    ordering = np.argsort(feature_ids, kind="stable")
    positions = np.searchsorted(feature_ids[ordering], truth_ids)
    if np.any(positions == ROWS) or not np.array_equal(
        feature_ids[ordering][positions], truth_ids
    ):
        raise ValueError("ground truth references an unknown feature ID")
    return ordering[positions].astype(np.int32, copy=False)


def lloyd(data, clusters, iterations, seed):
    clusters = min(clusters, data.shape[0])
    generator = np.random.default_rng(seed)
    centroids = data[generator.choice(data.shape[0], clusters, replace=False)].copy()
    for _ in range(iterations):
        norms = np.einsum("ij,ij->i", centroids, centroids)
        assignment = np.argmin(norms[None, :] - 2.0 * (data @ centroids.T), axis=1)
        counts = np.bincount(assignment, minlength=clusters)
        order = np.argsort(assignment, kind="stable")
        starts = np.concatenate(([0], np.cumsum(counts)[:-1]))
        occupied = counts > 0
        centroids[occupied] = (
            np.add.reduceat(data[order], starts[occupied], axis=0)
            / counts[occupied][:, None]
        )
    return centroids.astype(np.float32, copy=False)


def pq_reconstruct(data, subspaces, seed, sample_rows=100_000):
    width = DIMENSIONS // subspaces
    generator = np.random.default_rng(seed)
    take = min(data.shape[0], sample_rows)
    sample = data[generator.choice(data.shape[0], take, replace=False)]
    out = np.empty_like(data)
    for index in range(subspaces):
        lo, hi = index * width, (index + 1) * width
        book = lloyd(np.ascontiguousarray(sample[:, lo:hi]), 256, 10, seed + index)
        norms = np.einsum("ij,ij->i", book, book)
        for start in range(0, data.shape[0], CHUNK_ROWS):
            stop = min(start + CHUNK_ROWS, data.shape[0])
            block = data[start:stop, lo:hi]
            out[start:stop, lo:hi] = book[
                np.argmin(norms[None, :] - 2.0 * (block @ book.T), axis=1)
            ]
    return out


def sq8_reconstruct(data):
    low, high = data.min(axis=0), data.max(axis=0)
    span = np.maximum(high - low, 1e-12).astype(np.float32)
    codes = np.clip(np.rint((data - low) / span * 255.0), 0, 255).astype(np.uint8)
    return (codes.astype(np.float32) / 255.0) * span + low


def block_means(ordered, block_rows):
    rows = ordered.shape[0]
    starts = np.arange(0, rows, block_rows)
    counts = np.diff(np.append(starts, rows)).astype(np.float32)
    return (np.add.reduceat(ordered, starts, axis=0) / counts[:, None]).astype(np.float32)


def coalesce(sorted_units: np.ndarray, gap: int) -> tuple[int, int]:
    """Ranges and units covered when runs separated by <= `gap` are merged."""
    if sorted_units.size == 0:
        return 0, 0
    breaks = np.flatnonzero(np.diff(sorted_units) > gap + 1)
    starts = np.concatenate(([sorted_units[0]], sorted_units[breaks + 1]))
    ends = np.concatenate((sorted_units[breaks], [sorted_units[-1]]))
    return starts.size, int((ends - starts + 1).sum())


def nearest_rank(ordered, numerator, denominator):
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return int(ordered[index])


def stats(values: np.ndarray) -> dict:
    ordered = np.sort(values)
    return {
        "p50": nearest_rank(ordered, 50, 100),
        "p95": nearest_rank(ordered, 95, 100),
        "p99": nearest_rank(ordered, 99, 100),
        "maximum": int(ordered[-1]),
        "mean_x100": int(round(float(values.mean()) * 100)),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--development-query", type=Path)
    parser.add_argument("--ground-truth", type=Path)
    parser.add_argument("--layout-order", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    for name in ("source", "development_query", "ground_truth", "layout_order", "output"):
        if getattr(args, name) is None:
            parser.error(f"missing required argument: {name}")

    started = time.perf_counter()
    truth_rows = load_truth_rows(args.source, args.ground_truth)
    order = np.asarray(np.load(args.layout_order), dtype=np.int32)
    if order.shape != (ROWS,) or not np.array_equal(
        np.sort(order), np.arange(ROWS, dtype=np.int32)
    ):
        raise ValueError("layout order is not a row permutation")
    position = np.empty(ROWS, dtype=np.int32)
    position[order] = np.arange(ROWS, dtype=np.int32)
    truth_positions = position[truth_rows]
    pages = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS

    queries = fixed_list(
        pq.read_table(args.development_query), "embedding", DIMENSIONS, QUERIES
    )
    ordered = np.ascontiguousarray(
        fixed_list(
            pq.read_table(args.source, columns=["embedding"]), "embedding", DIMENSIONS, ROWS
        )[order]
    )

    # Router: PQ192 summaries, two per page - the V66 operating point.
    summaries = pq_reconstruct(
        block_means(ordered, PAGE_ROWS // ROUTER_BLOCKS_PER_PAGE), 192, 6701, 60_000
    )
    summary_norms = np.einsum("ij,ij->i", summaries, summaries).astype(np.float32)
    scores = summary_norms[None, :] - 2.0 * (queries @ summaries.T)
    needed = pages * ROUTER_BLOCKS_PER_PAGE
    if scores.shape[1] < needed:
        scores = np.concatenate(
            [scores, np.full((QUERIES, needed - scores.shape[1]), np.inf, np.float32)],
            axis=1,
        )
    page_scores = scores[:, :needed].reshape(QUERIES, pages, ROUTER_BLOCKS_PER_PAGE).min(axis=2)
    page_ranking = np.argsort(page_scores, axis=1, kind="stable").astype(np.int32)
    print(json.dumps({"phase": "router built"}), flush=True)

    cells = []
    # Phase 1: stage-one request profile. Independent of codec.
    for budget in ROUTER_PAGE_BUDGETS:
        selected = np.sort(page_ranking[:, :budget], axis=1)
        for gap in GAP_PAGES:
            ranges = np.empty(QUERIES, dtype=np.int32)
            covered = np.empty(QUERIES, dtype=np.int32)
            for query in range(QUERIES):
                ranges[query], covered[query] = coalesce(selected[query], gap)
            for codec, row_bytes in CODE_ROW_BYTES.items():
                cells.append(
                    {
                        "phase": "stage_one_requests",
                        "codec": codec,
                        "router_pages": budget,
                        "gap_pages": gap,
                        "requests": stats(ranges),
                        "pages_read": stats(covered),
                        "bytes_p95": nearest_rank(np.sort(covered), 95, 100)
                        * PAGE_ROWS
                        * row_bytes,
                        "gap_waste_x1000": int(
                            round(float(covered.mean() / budget) * 1000)
                        ),
                    }
                )
        print(json.dumps({"phase": "stage one", "budget": budget}), flush=True)

    # Phase 2: is stage two needed? Rank by code score alone and return top-100.
    for codec in ("pq192x8", "sq8"):
        approximate = (
            pq_reconstruct(ordered, 192, 6702)
            if codec == "pq192x8"
            else sq8_reconstruct(ordered)
        )
        code_norms = np.einsum("ij,ij->i", approximate, approximate).astype(np.float32)
        no_rerank = np.zeros(QUERIES, dtype=np.int32)
        with_rerank = np.zeros(QUERIES, dtype=np.int32)
        stage_two_ranges = {gap: np.zeros(QUERIES, dtype=np.int32) for gap in GAP_PAGES}
        budget = 256
        for start in range(0, QUERIES, QUERY_CHUNK):
            stop = min(start + QUERY_CHUNK, QUERIES)
            block = code_norms[None, :] - 2.0 * (queries[start:stop] @ approximate.T)
            for local, query in enumerate(range(start, stop)):
                selected = page_ranking[query, :budget].astype(np.int64)
                candidate = (
                    selected[:, None] * PAGE_ROWS
                    + np.arange(PAGE_ROWS, dtype=np.int64)[None, :]
                ).reshape(-1)
                candidate = candidate[candidate < ROWS]
                values = block[local][candidate]
                head = np.argpartition(values, NEIGHBORS - 1)[:NEIGHBORS]
                returned = candidate[head]
                no_rerank[query] = np.isin(truth_positions[query], returned).sum()
                deep = np.argpartition(values, SHORTLIST - 1)[:SHORTLIST]
                shortlist = np.sort(candidate[deep])
                with_rerank[query] = np.isin(truth_positions[query], shortlist).sum()
                for gap in GAP_PAGES:
                    stage_two_ranges[gap][query] = coalesce(
                        shortlist, gap * PAGE_ROWS
                    )[0]
        cells.append(
            {
                "phase": "rerank_necessity",
                "codec": codec,
                "router_pages": budget,
                "no_rerank_aggregate_ppm": int(
                    round(float(no_rerank.sum()) * 1_000_000 / (QUERIES * NEIGHBORS))
                ),
                "no_rerank_worst_ppm": int(no_rerank.min()) * 10_000,
                "with_rerank_aggregate_ppm": int(
                    round(float(with_rerank.sum()) * 1_000_000 / (QUERIES * NEIGHBORS))
                ),
                "with_rerank_worst_ppm": int(with_rerank.min()) * 10_000,
                "stage_two_requests": {
                    str(gap): stats(values) for gap, values in stage_two_ranges.items()
                },
            }
        )
        print(json.dumps(cells[-1], default=int)[:400], flush=True)
        del approximate

    result = {
        "schema": "borsuk-v67-algorithm-first-request-profile-result-v1",
        "claim_eligible": False,
        "evidence_kind": "object-store-request-profile-and-rerank-necessity-not-latency",
        "source_rows": ROWS,
        "queries": QUERIES,
        "neighbors": NEIGHBORS,
        "page_rows": PAGE_ROWS,
        "router": f"pq192x8_block_mean_{ROUTER_BLOCKS_PER_PAGE}",
        "shortlist": SHORTLIST,
        "gap_pages_ladder": list(GAP_PAGES),
        "code_row_bytes": CODE_ROW_BYTES,
        "exact_row_bytes": EXACT_ROW_BYTES,
        "cells": cells,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "validation_opened": False,
    }
    payload = (
        json.dumps(result, sort_keys=True, separators=(",", ":"), default=int) + "\n"
    ).encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest()}), flush=True)


def self_test() -> None:
    units = np.array([3, 4, 5, 9, 10, 40], dtype=np.int64)
    if coalesce(units, 0) != (3, 6):
        raise AssertionError("adjacent-only coalescing differs")
    if coalesce(units, 3) != (2, 9):
        raise AssertionError("gap-3 coalescing differs")
    if coalesce(units, 100) != (1, 38):
        raise AssertionError("full coalescing differs")
    if coalesce(np.array([], dtype=np.int64), 4) != (0, 0):
        raise AssertionError("empty coalescing differs")
    if coalesce(np.array([7], dtype=np.int64), 0) != (1, 1):
        raise AssertionError("single-unit coalescing differs")
    # Coalescing must be monotone: a wider gap never costs more requests and
    # never reads fewer units.
    generator = np.random.default_rng(67)
    for _ in range(200):
        sample = np.unique(generator.integers(0, 500, generator.integers(1, 60)))
        previous = None
        for gap in (0, 1, 2, 4, 8, 16, 32, 64):
            requests, covered = coalesce(sample, gap)
            if covered < sample.size:
                raise AssertionError("coalescing read fewer units than were selected")
            if previous is not None and (requests > previous[0] or covered < previous[1]):
                raise AssertionError("coalescing is not monotone in the gap")
            previous = (requests, covered)
    print(json.dumps({"self_test": "passed"}), flush=True)


if __name__ == "__main__":
    main()
