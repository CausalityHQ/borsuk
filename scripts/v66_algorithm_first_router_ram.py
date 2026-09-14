#!/usr/bin/env python3
"""Router recall as a function of resident bytes per row.

V65 reached the quality bar with a two-stage read, and left one blocker: the
router's resident summaries are 96 bytes per row, which is 9.6 GB at 100M rows.
V65 also showed the per-row codec is free - SQ8 and PQ192 reproduce an exact
f32 control digit for digit - so the only thing that matters upstream is which
pages the router selects.

This sweeps how cheap the *router's own summaries* can be made before page
selection degrades: fewer summaries per page, and lossy encodings of each
summary. Recall here is containment of the true top-100 in the selected pages,
which V65 showed a PQ192 code layer and exact rescoring then recover intact.

Summaries and codebooks are built from the corpus alone.
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
BLOCKS_PER_PAGE = (1, 2, 4, 8)
PAGE_BUDGETS = (32, 64, 128, 256, 512, 1024)
PCA_RANK = 128
CHUNK_ROWS = 16_384


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
    sorted_ids = feature_ids[ordering]
    positions = np.searchsorted(sorted_ids, truth_ids)
    if np.any(positions == ROWS) or not np.array_equal(sorted_ids[positions], truth_ids):
        raise ValueError("ground truth references an unknown feature ID")
    return ordering[positions].astype(np.int32, copy=False)


def lloyd(data: np.ndarray, clusters: int, iterations: int, seed: int) -> np.ndarray:
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


def block_means(ordered: np.ndarray, block_rows: int) -> np.ndarray:
    rows = ordered.shape[0]
    starts = np.arange(0, rows, block_rows)
    counts = np.diff(np.append(starts, rows)).astype(np.float32)
    return (np.add.reduceat(ordered, starts, axis=0) / counts[:, None]).astype(np.float32)


def encode_f32(summaries, queries, seed):
    return summaries, queries, DIMENSIONS * 4


def encode_sq8(summaries, queries, seed):
    low, high = summaries.min(axis=0), summaries.max(axis=0)
    span = np.maximum(high - low, 1e-12).astype(np.float32)
    codes = np.clip(np.rint((summaries - low) / span * 255.0), 0, 255).astype(np.uint8)
    return (codes.astype(np.float32) / 255.0) * span + low, queries, DIMENSIONS


def encode_pq192(summaries, queries, seed):
    subspaces = min(192, DIMENSIONS)
    while DIMENSIONS % subspaces:
        subspaces -= 1
    width = DIMENSIONS // subspaces
    out = np.empty_like(summaries)
    for index in range(subspaces):
        lo, hi = index * width, (index + 1) * width
        book = lloyd(np.ascontiguousarray(summaries[:, lo:hi]), 256, 10, seed + index)
        norms = np.einsum("ij,ij->i", book, book)
        block = summaries[:, lo:hi]
        out[:, lo:hi] = book[np.argmin(norms[None, :] - 2.0 * (block @ book.T), axis=1)]
    return out, queries, subspaces


def encode_binary(summaries, queries, seed):
    generator = np.random.default_rng(seed)
    rotation = np.linalg.qr(
        generator.standard_normal((DIMENSIONS, DIMENSIONS))
    )[0].astype(np.float32)
    centre = summaries.mean(axis=0)
    residual = (summaries - centre) @ rotation
    scale = np.linalg.norm(residual, axis=1, keepdims=True) / np.sqrt(DIMENSIONS)
    return (
        (np.sign(residual).astype(np.float32) * scale) @ rotation.T + centre,
        queries,
        DIMENSIONS // 8,
    )


def _pca_basis(summaries: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    centre = summaries.mean(axis=0)
    centred = summaries - centre
    covariance = (centred.T @ centred) / max(centred.shape[0] - 1, 1)
    values, vectors = np.linalg.eigh(covariance.astype(np.float64))
    basis = vectors[:, ::-1][:, :PCA_RANK].astype(np.float32)
    return centre.astype(np.float32), basis


def encode_pca_f32(summaries, queries, seed):
    centre, basis = _pca_basis(summaries)
    return (summaries - centre) @ basis, (queries - centre) @ basis, PCA_RANK * 4


def encode_pca_sq8(summaries, queries, seed):
    centre, basis = _pca_basis(summaries)
    projected = (summaries - centre) @ basis
    low, high = projected.min(axis=0), projected.max(axis=0)
    span = np.maximum(high - low, 1e-12).astype(np.float32)
    codes = np.clip(np.rint((projected - low) / span * 255.0), 0, 255).astype(np.uint8)
    return (
        (codes.astype(np.float32) / 255.0) * span + low,
        (queries - centre) @ basis,
        PCA_RANK,
    )


ENCODERS = {
    "f32": encode_f32,
    "sq8": encode_sq8,
    "pq192x8": encode_pq192,
    "binary": encode_binary,
    f"pca{PCA_RANK}_f32": encode_pca_f32,
    f"pca{PCA_RANK}_sq8": encode_pca_sq8,
}


def nearest_rank(ordered: np.ndarray, numerator: int, denominator: int) -> int:
    index = max(0, min(len(ordered) - 1, (len(ordered) * numerator - 1) // denominator))
    return int(ordered[index])


def page_recall(
    summaries: np.ndarray,
    queries: np.ndarray,
    blocks_per_page: int,
    pages: int,
    truth_pages: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    norms = np.einsum("ij,ij->i", summaries, summaries).astype(np.float32)
    scores = norms[None, :] - 2.0 * (queries @ summaries.T)
    needed = pages * blocks_per_page
    if scores.shape[1] < needed:
        scores = np.concatenate(
            [scores, np.full((scores.shape[0], needed - scores.shape[1]), np.inf, np.float32)],
            axis=1,
        )
    scores = scores[:, :needed].reshape(-1, pages, blocks_per_page).min(axis=2)
    ranking = np.argsort(scores, axis=1, kind="stable")
    rank = np.empty_like(ranking)
    np.put_along_axis(
        rank, ranking, np.broadcast_to(np.arange(pages), ranking.shape), axis=1
    )
    truth_rank = rank[np.arange(QUERIES)[:, None], truth_pages]
    hits = np.zeros((len(PAGE_BUDGETS), QUERIES), dtype=np.int32)
    for index, budget in enumerate(PAGE_BUDGETS):
        hits[index] = (truth_rank < budget).sum(axis=1)
    return hits, truth_rank.max(axis=1) + 1


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
    pages = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS
    truth_pages = position[truth_rows] // PAGE_ROWS

    queries = fixed_list(
        pq.read_table(args.development_query), "embedding", DIMENSIONS, QUERIES
    )
    ordered = np.ascontiguousarray(
        fixed_list(
            pq.read_table(args.source, columns=["embedding"]), "embedding", DIMENSIONS, ROWS
        )[order]
    )

    cells = []
    for blocks in BLOCKS_PER_PAGE:
        summaries = block_means(ordered, PAGE_ROWS // blocks)
        for name, encoder in ENCODERS.items():
            encoded, projected_queries, summary_bytes = encoder(summaries, queries, 6600)
            hits, required = page_recall(
                encoded, projected_queries, blocks, pages, truth_pages
            )
            resident = blocks * summary_bytes / PAGE_ROWS
            cells.append(
                {
                    "encoding": name,
                    "blocks_per_page": blocks,
                    "summary_bytes": summary_bytes,
                    "resident_bytes_per_row_x100": int(round(resident * 100)),
                    "resident_gib_at_100m_x100": int(round(resident * 1e8 / 2**30 * 100)),
                    "curve": [
                        {
                            "pages": budget,
                            "aggregate_recall_ppm": int(
                                round(
                                    float(hits[index].sum()) * 1_000_000
                                    / (QUERIES * NEIGHBORS)
                                )
                            ),
                            "worst_query_recall_ppm": int(hits[index].min()) * 10_000,
                            "p05_query_recall_ppm": nearest_rank(
                                np.sort(hits[index]), 5, 100
                            )
                            * 10_000,
                        }
                        for index, budget in enumerate(PAGE_BUDGETS)
                    ],
                    "p95_pages_for_full_recall": nearest_rank(np.sort(required), 95, 100),
                }
            )
            print(
                json.dumps(
                    {
                        "encoding": name,
                        "blocks_per_page": blocks,
                        "resident_b_per_row": round(resident, 2),
                        "at_256_ppm": cells[-1]["curve"][3]["aggregate_recall_ppm"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

    result = {
        "schema": "borsuk-v66-algorithm-first-router-ram-result-v1",
        "claim_eligible": False,
        "evidence_kind": "router-page-containment-versus-resident-bytes-not-latency",
        "summaries_built_without_queries_or_ground_truth": True,
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "queries": QUERIES,
        "neighbors": NEIGHBORS,
        "page_rows": PAGE_ROWS,
        "page_budgets": list(PAGE_BUDGETS),
        "pca_rank": PCA_RANK,
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
    global ROWS, QUERIES, NEIGHBORS, DIMENSIONS, PAGE_ROWS, PAGE_BUDGETS, PCA_RANK
    ROWS, QUERIES, NEIGHBORS, DIMENSIONS = 4_096, 16, 10, 64
    # The top of the ladder must cover every page, or 'misses rows at full
    # budget' fires on a router that is behaving correctly.
    PAGE_ROWS, PAGE_BUDGETS, PCA_RANK = 32, (1, 2, 4, 8, 16, 128), 16

    rng = np.random.default_rng(66)
    anchors = rng.standard_normal((8, DIMENSIONS), dtype=np.float32) * 3.0
    ordered = (
        anchors[rng.integers(0, 8, ROWS)]
        + rng.standard_normal((ROWS, DIMENSIONS), dtype=np.float32)
    ).astype(np.float32)
    queries = rng.standard_normal((QUERIES, DIMENSIONS), dtype=np.float32)
    pages = ROWS // PAGE_ROWS
    truth_pages = rng.integers(0, pages, (QUERIES, NEIGHBORS))

    for blocks in (1, 4):
        summaries = block_means(ordered, PAGE_ROWS // blocks)
        if summaries.shape[0] != pages * blocks:
            raise AssertionError("block count differs from pages times blocks per page")
        baseline = None
        for name, encoder in ENCODERS.items():
            encoded, projected, summary_bytes = encoder(summaries, queries, 1)
            if encoded.shape[0] != summaries.shape[0]:
                raise AssertionError(f"{name} changed the summary count")
            if not np.isfinite(encoded).all() or not np.isfinite(projected).all():
                raise AssertionError(f"{name} produced a non-finite summary")
            hits, required = page_recall(encoded, projected, blocks, pages, truth_pages)
            aggregate = [entry.sum() for entry in hits]
            if aggregate != sorted(aggregate):
                raise AssertionError(f"{name} recall is not monotone in pages")
            if hits[-1].sum() != QUERIES * NEIGHBORS:
                raise AssertionError(f"{name} misses rows at full page budget")
            if required.max() > pages:
                raise AssertionError(f"{name} needs more pages than exist")
            if name == "f32":
                baseline = summary_bytes
        if baseline != DIMENSIONS * 4:
            raise AssertionError("the f32 control is not full width")
    print(json.dumps({"self_test": "passed"}), flush=True)


if __name__ == "__main__":
    main()
