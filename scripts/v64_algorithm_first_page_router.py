#!/usr/bin/env python3
"""Page-granular routing against V63's exact page-containment ceiling.

V63 established that a corpus-only k-means order cut into 256-row pages holds
100% of every query's top-100 inside 64 pages at 1.00x storage, and that a
posting-list router reaches only 96.71% aggregate / 48% worst query there. The
open question is whether a *page*-granular router closes that gap.

Every scorer here reads only resident per-page summaries built from the corpus
alone - never a query, never the ground truth, never a page body. Because all
fetched rows are exactly rescored, containment equals Recall@100 for an
exact-rerank serving path, and the same oracle is recomputed in-run so the
router is measured against its own ceiling rather than a remembered number.
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

PAGE_ROWS = (128, 256)
PAGE_BUDGET_LADDER = (8, 16, 24, 32, 48, 64, 96, 128, 192, 256)
SUBPAGE_SPLITS = (1, 4, 8)
CHUNK_ROWS = 16_384

SQ8_ROW_BYTES = 16 + DIMENSIONS
PAGE_HEADER_BYTES = 64


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
    if len(np.unique(feature_ids)) != ROWS:
        raise ValueError("source feature IDs are not unique")
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


def block_summaries(
    ordered: np.ndarray, block_rows: int
) -> tuple[np.ndarray, np.ndarray]:
    """Mean and squared radius of every contiguous block of `block_rows` rows."""
    rows = ordered.shape[0]
    starts = np.arange(0, rows, block_rows)
    counts = np.diff(np.append(starts, rows)).astype(np.float32)
    sums = np.add.reduceat(ordered, starts, axis=0)
    centres = (sums / counts[:, None]).astype(np.float32)
    centre_norms = np.einsum("ij,ij->i", centres, centres)
    squared = np.empty(rows, dtype=np.float32)
    for start in range(0, rows, CHUNK_ROWS):
        stop = min(start + CHUNK_ROWS, rows)
        block = ordered[start:stop]
        owner = np.arange(start, stop) // block_rows
        squared[start:stop] = np.maximum(
            np.einsum("ij,ij->i", block, block)
            + centre_norms[owner]
            - 2.0 * np.einsum("ij,ij->i", block, centres[owner]),
            0.0,
        )
    radii = np.maximum.reduceat(squared, starts)
    return centres, radii


def score_pages(
    queries: np.ndarray,
    centres: np.ndarray,
    radii: np.ndarray,
    blocks_per_page: int,
    use_radius: bool,
    pages: int,
) -> np.ndarray:
    """Per-page score: the best of its blocks, optionally radius-discounted."""
    centre_norms = np.einsum("ij,ij->i", centres, centres).astype(np.float32)
    query_norms = np.einsum("ij,ij->i", queries, queries).astype(np.float32)
    squared = (
        query_norms[:, None] + centre_norms[None, :] - 2.0 * (queries @ centres.T)
    )
    np.maximum(squared, 0.0, out=squared)
    if use_radius:
        # Distance from the query to the nearest point the block could hold.
        squared = np.maximum(np.sqrt(squared) - np.sqrt(radii)[None, :], 0.0) ** 2
    if blocks_per_page > 1:
        # The last page may own fewer blocks than the rest; pad rather than
        # truncate, or its rows become unreachable and recall is overstated.
        needed = pages * blocks_per_page
        if squared.shape[1] < needed:
            squared = np.concatenate(
                [
                    squared,
                    np.full(
                        (squared.shape[0], needed - squared.shape[1]),
                        np.inf,
                        dtype=np.float32,
                    ),
                ],
                axis=1,
            )
        squared = squared[:, :needed].reshape(
            squared.shape[0], pages, blocks_per_page
        ).min(axis=2)
    if squared.shape[1] != pages:
        raise ValueError("page score width differs from the page count")
    return squared


def nearest_rank(ordered: np.ndarray, numerator: int, denominator: int) -> int:
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return int(ordered[index])


def curve_from_hits(hits: np.ndarray, page_rows: int, page_bytes: int) -> list[dict]:
    curve = []
    for index, budget in enumerate(PAGE_BUDGET_LADDER):
        row = hits[index]
        curve.append(
            {
                "pages": budget,
                "rows_scanned": budget * page_rows,
                "aggregate_recall_ppm": int(
                    round(float(row.sum()) * 1_000_000 / (QUERIES * NEIGHBORS))
                ),
                "worst_query_recall_ppm": int(row.min()) * 10_000,
                "p05_query_recall_ppm": nearest_rank(np.sort(row), 5, 100) * 10_000,
                "sq8_bytes": budget * page_bytes,
            }
        )
    return curve


def evaluate_selection(
    selection_rank: np.ndarray, truth_pages: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """Recall at each budget, plus the budget each query needs for 100%."""
    ranks = selection_rank[np.arange(QUERIES)[:, None], truth_pages]
    hits = np.zeros((len(PAGE_BUDGET_LADDER), QUERIES), dtype=np.int32)
    for index, budget in enumerate(PAGE_BUDGET_LADDER):
        hits[index] = (ranks < budget).sum(axis=1)
    required = ranks.max(axis=1) + 1
    return hits, required


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
    row_to_position = np.empty(ROWS, dtype=np.int32)
    row_to_position[order] = np.arange(ROWS, dtype=np.int32)

    query_table = pq.read_table(args.development_query)
    if query_table.num_rows != QUERIES:
        raise ValueError("development query row count differs")
    queries = fixed_list(query_table, "embedding", DIMENSIONS, QUERIES)

    source_table = pq.read_table(args.source, columns=["embedding"])
    ordered = np.ascontiguousarray(
        fixed_list(source_table, "embedding", DIMENSIONS, ROWS)[order]
    )
    del source_table

    cells: list[dict] = []
    for page_rows in PAGE_ROWS:
        page_bytes = PAGE_HEADER_BYTES + page_rows * SQ8_ROW_BYTES
        pages = (ROWS + page_rows - 1) // page_rows
        truth_pages = row_to_position[truth_rows] // page_rows

        # Exact best-K-pages ceiling for this layout, recomputed in-run.
        oracle_hits = np.zeros((len(PAGE_BUDGET_LADDER), QUERIES), dtype=np.int32)
        oracle_required = np.zeros(QUERIES, dtype=np.int32)
        for query in range(QUERIES):
            counts = np.sort(np.unique(truth_pages[query], return_counts=True)[1])[::-1]
            cumulative = np.cumsum(counts)
            oracle_required[query] = counts.size
            for index, budget in enumerate(PAGE_BUDGET_LADDER):
                oracle_hits[index, query] = cumulative[min(budget, counts.size) - 1]
        cells.append(
            {
                "scorer": "oracle_best_pages",
                "page_rows": page_rows,
                "pages": pages,
                "resident_bytes_per_row_x100": 0,
                "curve": curve_from_hits(oracle_hits, page_rows, page_bytes),
                "pages_for_full_recall": {
                    "p50": nearest_rank(np.sort(oracle_required), 50, 100),
                    "p95": nearest_rank(np.sort(oracle_required), 95, 100),
                    "maximum": int(oracle_required.max()),
                },
            }
        )

        for splits in SUBPAGE_SPLITS:
            block_rows = page_rows // splits
            if block_rows < 8:
                continue
            centres, radii = block_summaries(ordered, block_rows)
            resident = splits * DIMENSIONS * 4 / page_rows
            for use_radius in (False, True):
                scores = score_pages(
                    queries, centres, radii, splits, use_radius, pages
                )
                ranking = np.argsort(scores, axis=1, kind="stable")
                selection_rank = np.empty_like(ranking)
                np.put_along_axis(
                    selection_rank,
                    ranking,
                    np.broadcast_to(
                        np.arange(ranking.shape[1], dtype=np.int32), ranking.shape
                    ),
                    axis=1,
                )
                hits, required = evaluate_selection(selection_rank, truth_pages)
                cells.append(
                    {
                        "scorer": (
                            f"block_mean_{splits}"
                            + ("_lower_bound" if use_radius else "")
                        ),
                        "page_rows": page_rows,
                        "pages": pages,
                        "blocks_per_page": splits,
                        "resident_bytes_per_row_x100": int(round(resident * 100)),
                        "curve": curve_from_hits(hits, page_rows, page_bytes),
                        "pages_for_full_recall": {
                            "p50": nearest_rank(np.sort(required), 50, 100),
                            "p95": nearest_rank(np.sort(required), 95, 100),
                            "maximum": int(required.max()),
                        },
                    }
                )
                print(
                    json.dumps(
                        {
                            "scorer": cells[-1]["scorer"],
                            "page_rows": page_rows,
                            "at_64": cells[-1]["curve"][5],
                            "p95_pages_for_full_recall": cells[-1][
                                "pages_for_full_recall"
                            ]["p95"],
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

    result = {
        "schema": "borsuk-v64-algorithm-first-page-router-result-v1",
        "claim_eligible": False,
        "evidence_kind": "page-router-containment-equals-exact-rerank-recall-not-latency",
        "summaries_built_without_queries_or_ground_truth": True,
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "queries": QUERIES,
        "neighbors": NEIGHBORS,
        "page_budget_ladder": list(PAGE_BUDGET_LADDER),
        "page_rows_ladder": list(PAGE_ROWS),
        "sq8_row_bytes": SQ8_ROW_BYTES,
        "cells": cells,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(
        json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest()}, sort_keys=True),
        flush=True,
    )


def self_test() -> None:
    global ROWS, QUERIES, NEIGHBORS, DIMENSIONS, PAGE_ROWS, PAGE_BUDGET_LADDER, CHUNK_ROWS
    ROWS, QUERIES, NEIGHBORS, DIMENSIONS = 4_090, 32, 10, 16
    PAGE_ROWS = (16, 32)
    PAGE_BUDGET_LADDER = (1, 2, 4, 8, 16, 32, 64, 128, 192, 256)
    CHUNK_ROWS = 512

    rng = np.random.default_rng(64)
    anchors = rng.standard_normal((12, DIMENSIONS), dtype=np.float32) * 3.0
    ordered = (
        anchors[np.repeat(np.arange(12), ROWS // 12 + 1)[:ROWS]]
        + rng.standard_normal((ROWS, DIMENSIONS), dtype=np.float32)
    ).astype(np.float32)
    queries = rng.standard_normal((QUERIES, DIMENSIONS), dtype=np.float32)

    for page_rows in PAGE_ROWS:
        pages = (ROWS + page_rows - 1) // page_rows
        for splits in SUBPAGE_SPLITS:
            block_rows = page_rows // splits
            if block_rows < 8:
                continue
            centres, radii = block_summaries(ordered, block_rows)
            for use_radius in (False, True):
                scored = score_pages(
                    queries, centres, radii, splits, use_radius, pages
                )
                if scored.shape != (QUERIES, pages):
                    raise AssertionError("page score shape differs from the page count")
                if not np.isfinite(scored).all():
                    raise AssertionError("a real page scored as unreachable")

    for block_rows in (16, 32):
        whole = ROWS // block_rows * block_rows
        block = ordered[:whole].reshape(-1, block_rows, DIMENSIONS)
        centres, radii = block_summaries(ordered[:whole], block_rows)
        if not np.allclose(centres, block.mean(axis=1), atol=1e-4):
            raise AssertionError("block means differ from a direct reshape mean")
        deltas = block - centres[:, None, :]
        direct = np.einsum("ijk,ijk->ij", deltas, deltas).max(axis=1)
        if not np.allclose(radii, direct, rtol=1e-3, atol=1e-3):
            raise AssertionError("block radii differ from direct computation")
        # A radius lower bound may never exceed the true distance to any member
        # of the block it summarises, or the router would discard pages that
        # actually hold neighbours.
        scores = score_pages(queries, centres, radii, 1, True, centres.shape[0])
        for query in range(QUERIES):
            offset = block - queries[query]
            true_best = np.einsum("ijk,ijk->ij", offset, offset).min(axis=1)
            if np.any(scores[query] > true_best + 1e-2):
                raise AssertionError("radius lower bound exceeds a true member distance")
    print(json.dumps({"self_test": "passed"}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
