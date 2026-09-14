#!/usr/bin/env python3
"""Two-stage read: compact codes over a candidate region, then exact rows.

V64 showed a single resident router over the V63 layout plateaus near 97.7%
Recall@100 at 64 pages, with 64% on the worst query, and that sharpening the
per-page summary converges on brute force because resident cost is
`4 * dimensions / block_rows` bytes per row.

This measures the alternative every blob-native system actually uses: spend one
round trip reading compact per-row codes across a wide candidate region, rank
with those, then spend a second round trip on exact rows. Reported for each
codec and budget: containment of the true top-100 in the shortlist - which is
Recall@100 once the shortlist is exactly rescored - together with the bytes and
distinct pages each stage costs.

Codebooks and codes are built from the corpus alone. Queries and ground truth
are used only to score.
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
ROUTER_BLOCKS_PER_PAGE = 8
ROUTER_PAGE_BUDGETS = (64, 128, 256, 512)
SHORTLIST_SIZES = (256, 512, 1024, 2048)
QUERY_CHUNK = 50
CHUNK_ROWS = 16_384

SQ8_ROW_BYTES = 16 + DIMENSIONS
PAGE_HEADER_BYTES = 64
EXACT_ROW_BYTES = 16 + DIMENSIONS * 4


def fixed_list(table, name: str, width: int, rows: int) -> np.ndarray:
    column = table[name].combine_chunks()
    values = column.values.to_numpy(zero_copy_only=False)
    result = np.array(values, dtype=np.float32, copy=True).reshape(rows, width)
    if result.shape != (rows, width) or not np.isfinite(result).all():
        raise ValueError(f"{name} differs")
    return result


def scalar(table, name: str, dtype) -> np.ndarray:
    return np.asarray(
        table[name].combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype
    )


def load_truth_rows(source_path: Path, truth_path: Path) -> np.ndarray:
    source = pq.read_table(source_path, columns=["feature_row_id"])
    truth = pq.read_table(
        truth_path, columns=["query_ordinal", "rank", "feature_row_id"]
    )
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
    sorted_ids = feature_ids[ordering]
    positions = np.searchsorted(sorted_ids, truth_ids)
    if np.any(positions == ROWS) or not np.array_equal(sorted_ids[positions], truth_ids):
        raise ValueError("ground truth references an unknown feature ID")
    return ordering[positions].astype(np.int32, copy=False)


def lloyd(data: np.ndarray, clusters: int, iterations: int, seed: int) -> np.ndarray:
    """Small-dimension Lloyd's algorithm for one product-quantiser subspace."""
    generator = np.random.default_rng(seed)
    centroids = data[generator.choice(data.shape[0], clusters, replace=False)].copy()
    for _ in range(iterations):
        norms = np.einsum("ij,ij->i", centroids, centroids)
        assignment = np.argmin(norms[None, :] - 2.0 * (data @ centroids.T), axis=1)
        counts = np.bincount(assignment, minlength=clusters)
        order = np.argsort(assignment, kind="stable")
        starts = np.concatenate(([0], np.cumsum(counts)[:-1]))
        occupied = counts > 0
        sums = np.add.reduceat(data[order], starts[occupied], axis=0)
        centroids[occupied] = sums / counts[occupied][:, None]
    return centroids.astype(np.float32, copy=False)


class Codec:
    """A corpus-only row codec that reconstructs an approximate vector.

    Ranking with a reconstruction is equivalent to asymmetric scoring against
    the same codes, and it lets the whole corpus be scored with one sgemm.
    """

    def __init__(self, name: str, row_bytes: int, reconstruction: np.ndarray) -> None:
        self.name = name
        self.row_bytes = row_bytes
        self.reconstruction = reconstruction


def build_sq8(ordered: np.ndarray) -> Codec:
    low = ordered.min(axis=0)
    high = ordered.max(axis=0)
    span = np.maximum(high - low, 1e-12).astype(np.float32)
    codes = np.clip(np.rint((ordered - low) / span * 255.0), 0, 255).astype(np.uint8)
    return Codec("sq8", 16 + DIMENSIONS, (codes.astype(np.float32) / 255.0) * span + low)


def build_pq(ordered: np.ndarray, subspaces: int, seed: int) -> Codec:
    width = DIMENSIONS // subspaces
    generator = np.random.default_rng(seed)
    sample = ordered[generator.choice(ROWS, min(ROWS, 100_000), replace=False)]
    reconstruction = np.empty_like(ordered)
    for index in range(subspaces):
        lo, hi = index * width, (index + 1) * width
        book = lloyd(np.ascontiguousarray(sample[:, lo:hi]), 256, 10, seed + index)
        norms = np.einsum("ij,ij->i", book, book)
        for start in range(0, ROWS, CHUNK_ROWS):
            stop = min(start + CHUNK_ROWS, ROWS)
            block = ordered[start:stop, lo:hi]
            assignment = np.argmin(norms[None, :] - 2.0 * (block @ book.T), axis=1)
            reconstruction[start:stop, lo:hi] = book[assignment]
    return Codec(f"pq{subspaces}x8", 16 + subspaces, reconstruction)


def build_binary(ordered: np.ndarray, seed: int) -> Codec:
    """Sign codes in a random rotation, reconstructed as +/-1 scaled by radius."""
    generator = np.random.default_rng(seed)
    rotation = np.linalg.qr(
        generator.standard_normal((DIMENSIONS, DIMENSIONS))
    )[0].astype(np.float32)
    centre = ordered.mean(axis=0)
    reconstruction = np.empty_like(ordered)
    for start in range(0, ROWS, CHUNK_ROWS):
        stop = min(start + CHUNK_ROWS, ROWS)
        residual = (ordered[start:stop] - centre) @ rotation
        scale = np.linalg.norm(residual, axis=1, keepdims=True) / np.sqrt(DIMENSIONS)
        reconstruction[start:stop] = (
            np.sign(residual).astype(np.float32) * scale
        ) @ rotation.T + centre
    return Codec("binary768", 16 + DIMENSIONS // 8, reconstruction)


def block_summaries(ordered: np.ndarray, block_rows: int) -> np.ndarray:
    rows = ordered.shape[0]
    starts = np.arange(0, rows, block_rows)
    counts = np.diff(np.append(starts, rows)).astype(np.float32)
    return (np.add.reduceat(ordered, starts, axis=0) / counts[:, None]).astype(np.float32)


def plain(value):
    """Convert NumPy scalars for JSON, refusing anything genuinely unknown.

    A run that survives every measurement and then dies serialising one
    np.int64 has thrown the whole machine away for nothing.
    """
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    raise TypeError(f"unserialisable value of type {type(value).__name__}")


def nearest_rank(ordered: np.ndarray, numerator: int, denominator: int) -> int:
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return int(ordered[index])


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
    missing = [
        name
        for name in ("source", "development_query", "ground_truth", "layout_order", "output")
        if getattr(args, name) is None
    ]
    if missing:
        parser.error(f"missing required arguments: {', '.join(missing)}")

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

    queries = fixed_list(
        pq.read_table(args.development_query), "embedding", DIMENSIONS, QUERIES
    )
    ordered = np.ascontiguousarray(
        fixed_list(pq.read_table(args.source, columns=["embedding"]), "embedding", DIMENSIONS, ROWS)[order]
    )

    pages = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS
    page_bytes = PAGE_HEADER_BYTES + PAGE_ROWS * SQ8_ROW_BYTES
    router_centres = block_summaries(ordered, PAGE_ROWS // ROUTER_BLOCKS_PER_PAGE)
    centre_norms = np.einsum("ij,ij->i", router_centres, router_centres).astype(np.float32)
    block_scores = centre_norms[None, :] - 2.0 * (queries @ router_centres.T)
    needed = pages * ROUTER_BLOCKS_PER_PAGE
    if block_scores.shape[1] < needed:
        block_scores = np.concatenate(
            [
                block_scores,
                np.full((QUERIES, needed - block_scores.shape[1]), np.inf, np.float32),
            ],
            axis=1,
        )
    page_scores = block_scores[:, :needed].reshape(QUERIES, pages, ROUTER_BLOCKS_PER_PAGE).min(axis=2)
    page_ranking = np.argsort(page_scores, axis=1, kind="stable").astype(np.int32)

    # Built one at a time: each reconstruction is a full corpus-sized array,
    # so holding all five at once would need five times the corpus in RAM.
    builders = [
        lambda: Codec("exact_f32", EXACT_ROW_BYTES, ordered),
        lambda: build_sq8(ordered),
        lambda: build_pq(ordered, 192, 6501),
        lambda: build_pq(ordered, 64, 6502),
        lambda: build_binary(ordered, 6503),
    ]

    cells = []
    largest = max(SHORTLIST_SIZES)
    for builder in builders:
        codec = builder()
        code_norms = np.einsum("ij,ij->i", codec.reconstruction, codec.reconstruction).astype(np.float32)
        counts = {
            (budget, shortlist): np.zeros(QUERIES, dtype=np.int32)
            for budget in ROUTER_PAGE_BUDGETS
            for shortlist in SHORTLIST_SIZES
        }
        global_counts = {size: np.zeros(QUERIES, dtype=np.int32) for size in SHORTLIST_SIZES}
        stage_two_pages = {
            (budget, shortlist): np.zeros(QUERIES, dtype=np.int32)
            for budget in ROUTER_PAGE_BUDGETS
            for shortlist in SHORTLIST_SIZES
        }
        for start in range(0, QUERIES, QUERY_CHUNK):
            stop = min(start + QUERY_CHUNK, QUERIES)
            approximate = code_norms[None, :] - 2.0 * (
                queries[start:stop] @ codec.reconstruction.T
            )
            for local, query in enumerate(range(start, stop)):
                row_scores = approximate[local]
                truth = truth_positions[query]
                top = np.argpartition(row_scores, largest - 1)[:largest]
                top = top[np.argsort(row_scores[top], kind="stable")]
                for size in SHORTLIST_SIZES:
                    global_counts[size][query] = np.isin(truth, top[:size]).sum()
                for budget in ROUTER_PAGE_BUDGETS:
                    selected = page_ranking[query, :budget]
                    candidate = (
                        selected[:, None].astype(np.int64) * PAGE_ROWS
                        + np.arange(PAGE_ROWS, dtype=np.int64)[None, :]
                    ).reshape(-1)
                    candidate = candidate[candidate < ROWS]
                    values = row_scores[candidate]
                    if candidate.size <= largest:
                        ranked = candidate[np.argsort(values, kind="stable")]
                    else:
                        head = np.argpartition(values, largest - 1)[:largest]
                        ranked = candidate[head[np.argsort(values[head], kind="stable")]]
                    for size in SHORTLIST_SIZES:
                        shortlist = ranked[:size]
                        counts[(budget, size)][query] = np.isin(truth, shortlist).sum()
                        stage_two_pages[(budget, size)][query] = np.unique(
                            shortlist // PAGE_ROWS
                        ).size

        for budget in ROUTER_PAGE_BUDGETS:
            for size in SHORTLIST_SIZES:
                hits = counts[(budget, size)]
                touched = stage_two_pages[(budget, size)]
                cells.append(
                    {
                        "mode": "staged",
                        "codec": codec.name,
                        "code_row_bytes": codec.row_bytes,
                        "router_pages": budget,
                        "shortlist": size,
                        "aggregate_recall_ppm": int(
                            round(float(hits.sum()) * 1_000_000 / (QUERIES * NEIGHBORS))
                        ),
                        "worst_query_recall_ppm": int(hits.min()) * 10_000,
                        "p05_query_recall_ppm": nearest_rank(np.sort(hits), 5, 100) * 10_000,
                        "stage_one_bytes": budget * PAGE_ROWS * codec.row_bytes,
                        "stage_one_gets": budget,
                        "stage_two_p95_pages": nearest_rank(np.sort(touched), 95, 100),
                        "stage_two_p95_bytes": nearest_rank(np.sort(touched), 95, 100)
                        * page_bytes,
                    }
                )
        for size in SHORTLIST_SIZES:
            hits = global_counts[size]
            cells.append(
                {
                    "mode": "global",
                    "codec": codec.name,
                    "code_row_bytes": codec.row_bytes,
                    "router_pages": 0,
                    "shortlist": size,
                    "aggregate_recall_ppm": int(
                        round(float(hits.sum()) * 1_000_000 / (QUERIES * NEIGHBORS))
                    ),
                    "worst_query_recall_ppm": int(hits.min()) * 10_000,
                    "p05_query_recall_ppm": nearest_rank(np.sort(hits), 5, 100) * 10_000,
                    "stage_one_bytes": ROWS * codec.row_bytes,
                    "stage_one_gets": 0,
                    "stage_two_p95_pages": 0,
                    "stage_two_p95_bytes": 0,
                }
            )
        print(
            json.dumps(
                {
                    "codec": codec.name,
                    "row_bytes": codec.row_bytes,
                    "global_2048_ppm": int(global_counts[2048].sum()) * 10_000 // QUERIES,
                },
                sort_keys=True,
                default=plain,
            ),
            flush=True,
        )
        del codec

    result = {
        "schema": "borsuk-v65-algorithm-first-two-stage-result-v1",
        "claim_eligible": False,
        "evidence_kind": "shortlist-containment-equals-exact-rerank-recall-not-latency",
        "codebooks_built_without_queries_or_ground_truth": True,
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "queries": QUERIES,
        "neighbors": NEIGHBORS,
        "page_rows": PAGE_ROWS,
        "router": f"block_mean_{ROUTER_BLOCKS_PER_PAGE}",
        "router_page_budgets": list(ROUTER_PAGE_BUDGETS),
        "shortlist_sizes": list(SHORTLIST_SIZES),
        "cells": cells,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "validation_opened": False,
    }
    payload = (
        json.dumps(result, sort_keys=True, separators=(",", ":"), default=plain) + "\n"
    ).encode()
    args.output.write_bytes(payload)
    print(
        json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest()}, sort_keys=True),
        flush=True,
    )


def self_test() -> None:
    global ROWS, QUERIES, NEIGHBORS, DIMENSIONS, PAGE_ROWS, CHUNK_ROWS
    global ROUTER_PAGE_BUDGETS, SHORTLIST_SIZES
    ROWS, QUERIES, NEIGHBORS, DIMENSIONS = 4_096, 16, 10, 32
    PAGE_ROWS, CHUNK_ROWS = 32, 512
    ROUTER_PAGE_BUDGETS, SHORTLIST_SIZES = (4, 16), (32, 128)

    rng = np.random.default_rng(65)
    anchors = rng.standard_normal((10, DIMENSIONS), dtype=np.float32) * 3.0
    ordered = (
        anchors[rng.integers(0, 10, ROWS)]
        + rng.standard_normal((ROWS, DIMENSIONS), dtype=np.float32)
    ).astype(np.float32)

    for codec in (
        build_sq8(ordered),
        build_pq(ordered, 8, 1),
        build_binary(ordered, 2),
    ):
        error = np.linalg.norm(codec.reconstruction - ordered, axis=1)
        spread = np.linalg.norm(ordered - ordered.mean(axis=0), axis=1)
        if codec.reconstruction.shape != ordered.shape:
            raise AssertionError(f"{codec.name} reconstruction shape differs")
        if not np.isfinite(codec.reconstruction).all():
            raise AssertionError(f"{codec.name} produced a non-finite reconstruction")
        # A codec must be closer to the corpus than the corpus mean is, or it
        # carries no usable ranking signal at all.
        if error.mean() >= spread.mean():
            raise AssertionError(f"{codec.name} is no better than the corpus mean")
        print(
            json.dumps(
                {
                    "codec": codec.name,
                    "row_bytes": codec.row_bytes,
                    "mean_relative_error_x1000": int(
                        round(float(error.mean() / spread.mean()) * 1000)
                    ),
                },
                sort_keys=True,
            ),
            flush=True,
        )

    exact = Codec("exact_f32", EXACT_ROW_BYTES, ordered)
    if not np.array_equal(exact.reconstruction, ordered):
        raise AssertionError("the exact control is not exact")
    print(json.dumps({"self_test": "passed"}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
