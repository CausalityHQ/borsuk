#!/usr/bin/env python3
"""Fail-fast shared-PQ/base-page plus resident-delta quality screen."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

import numpy as np
import pyarrow.parquet as pq


def _lloyd(data: np.ndarray, clusters: int, seed: int) -> np.ndarray:
    count = min(clusters, data.shape[0])
    generator = np.random.default_rng(seed)
    centers = data[generator.choice(data.shape[0], count, replace=False)].copy()
    for _ in range(5):
        center_norms = np.einsum("ij,ij->i", centers, centers)
        assignment = np.argmin(
            center_norms[None, :] - np.float32(2.0) * (data @ centers.T), axis=1
        )
        populations = np.bincount(assignment, minlength=count)
        order = np.argsort(assignment, kind="stable")
        starts = np.concatenate(([0], np.cumsum(populations)[:-1]))
        occupied = populations > 0
        centers[occupied] = (
            np.add.reduceat(data[order], starts[occupied], axis=0)
            / populations[occupied, None]
        )
    return centers.astype(np.float32, copy=False)


def _train_pq(
    base: np.ndarray, subspaces: int, clusters: int, seed: int
) -> list[np.ndarray]:
    width = base.shape[1] // subspaces
    return [
        _lloyd(
            np.ascontiguousarray(base[:, index * width : (index + 1) * width]),
            clusters,
            seed + index,
        )
        for index in range(subspaces)
    ]


def _encode_pq(vectors: np.ndarray, books: list[np.ndarray]) -> np.ndarray:
    subspaces = len(books)
    width = vectors.shape[1] // subspaces
    codes = np.empty((vectors.shape[0], subspaces), dtype=np.uint8)
    for index, book in enumerate(books):
        block = vectors[:, index * width : (index + 1) * width]
        norms = np.einsum("ij,ij->i", book, book)
        codes[:, index] = np.argmin(
            norms[None, :] - np.float32(2.0) * (block @ book.T), axis=1
        )
    return codes


def _adc_scores(query: np.ndarray, codes: np.ndarray, books: list[np.ndarray]) -> np.ndarray:
    width = query.size // len(books)
    scores = np.zeros(codes.shape[0], dtype=np.float32)
    for index, book in enumerate(books):
        delta = book - query[index * width : (index + 1) * width]
        table = np.einsum("ij,ij->i", delta, delta)
        scores += table[codes[:, index]]
    return scores


def _sq8(vectors: np.ndarray) -> np.ndarray:
    low = np.min(vectors, axis=0).astype(np.float32)
    high = np.max(vectors, axis=0).astype(np.float32)
    step = ((high - low) / np.float32(255.0)).astype(np.float32)
    step[step == 0.0] = np.float32(1.0)
    code = np.clip(
        np.rint((vectors - low[None, :]) / step[None, :]), 0, 255
    ).astype(np.uint8)
    return low[None, :] + code.astype(np.float32) * step[None, :]


def _coalesce(pages: np.ndarray, gap: int = 2) -> list[tuple[int, int]]:
    if pages.size == 0:
        return []
    breaks = np.flatnonzero(np.diff(pages) > gap + 1)
    starts = np.concatenate(([pages[0]], pages[breaks + 1]))
    ends = np.concatenate((pages[breaks], [pages[-1]]))
    return list(zip(starts.tolist(), ends.tolist(), strict=True))


def _top_ids(
    query: np.ndarray, vectors: np.ndarray, ids: np.ndarray, neighbors: int
) -> list[int]:
    delta = vectors - query[None, :]
    scores = np.einsum("ij,ij->i", delta, delta)
    ordered = np.lexsort((ids, scores))[:neighbors]
    return [int(ids[index]) for index in ordered]


def evaluate_overlay(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    page_rows: int = 256,
    neighbors: int = 100,
    subspaces: int = 64,
    clusters: int = 256,
    shortlists: tuple[int, ...] = (256, 512, 1024),
    logical_run_counts: tuple[int, ...] = (1, 10, 100),
    seed: int = 85,
) -> dict[str, Any]:
    """Evaluate one shared base router with a fully resident delta tier."""

    if (
        base.ndim != 2
        or delta.ndim != 2
        or queries.ndim != 2
        or base.shape[1] != delta.shape[1]
        or base.shape[1] != queries.shape[1]
        or base.shape[0] == 0
        or delta.shape[0] == 0
        or queries.shape[0] == 0
        or base.shape[1] % subspaces != 0
        or page_rows <= 0
        or neighbors <= 0
    ):
        raise ValueError("overlay shape differs")
    if not all(np.isfinite(value).all() for value in (base, delta, queries)):
        raise ValueError("overlay vectors must be finite")

    base = np.ascontiguousarray(base, dtype=np.float32)
    delta = np.ascontiguousarray(delta, dtype=np.float32)
    queries = np.ascontiguousarray(queries, dtype=np.float32)
    books = _train_pq(base, subspaces, clusters, seed)
    base_codes = _encode_pq(base, books)
    base_sq8 = _sq8(base)
    delta_sq8 = _sq8(delta)
    base_ids = np.arange(base.shape[0], dtype=np.int64)
    delta_ids = np.arange(
        base.shape[0], base.shape[0] + delta.shape[0], dtype=np.int64
    )
    all_ids = np.concatenate((base_ids, delta_ids))
    exact_corpus = np.concatenate((base, delta), axis=0)
    truth = [_top_ids(query, exact_corpus, all_ids, neighbors) for query in queries]

    cells = []
    for shortlist in shortlists:
        samples = []
        exact_hits = 0
        sq8_hits = 0
        for query_index, query in enumerate(queries):
            scores = _adc_scores(query, base_codes, books)
            take = min(shortlist, scores.size)
            head = np.argpartition(scores, take - 1)[:take]
            pages = np.unique(head // page_rows)
            ranges = _coalesce(pages)
            base_candidates = np.concatenate(
                [
                    np.arange(
                        start * page_rows,
                        min((end + 1) * page_rows, base.shape[0]),
                        dtype=np.int64,
                    )
                    for start, end in ranges
                ]
            )
            candidate_ids = np.concatenate((base_ids[base_candidates], delta_ids))
            exact_vectors = np.concatenate((base[base_candidates], delta), axis=0)
            sq8_vectors = np.concatenate((base_sq8[base_candidates], delta_sq8), axis=0)
            exact_result = _top_ids(query, exact_vectors, candidate_ids, neighbors)
            sq8_result = _top_ids(query, sq8_vectors, candidate_ids, neighbors)
            expected = set(truth[query_index])
            exact_hits += len(expected.intersection(exact_result))
            sq8_hits += len(expected.intersection(sq8_result))
            samples.append(
                {
                    "base_bytes": int(base_candidates.size * (12 + base.shape[1])),
                    "base_gets": len(ranges),
                    "exact_result_ids": exact_result,
                    "query": query_index,
                    "result_ids": sq8_result,
                }
            )
        denominator = len(queries) * min(neighbors, exact_corpus.shape[0])
        first_result = samples[0]["result_ids"]
        cells.append(
            {
                "base_bytes_max": max(sample["base_bytes"] for sample in samples),
                "base_gets_max": max(sample["base_gets"] for sample in samples),
                "exact_recall_ppm": round(exact_hits * 1_000_000 / denominator),
                "logical_run_results": [first_result] * len(logical_run_counts),
                "result_ids": first_result,
                "shortlist_rows": shortlist,
                "sq8_recall_ppm": round(sq8_hits * 1_000_000 / denominator),
            }
        )
    return {
        "cells": cells,
        "claim_eligible": False,
        "cpu_parallelism": "sequential-per-query",
        "delta_resident_bytes": int(delta.nbytes),
        "delta_rows": int(delta.shape[0]),
        "logical_run_counts": list(logical_run_counts),
        "schema": "borsuk-v85-shared-overlay-screen-v1",
        "training_rows": int(base.shape[0]),
    }


def _fixed_list(path: pathlib.Path, column: str, rows: int) -> np.ndarray:
    values = pq.read_table(path, columns=[column])[column]
    matrix = np.asarray(values.combine_chunks().values.to_numpy(), dtype=np.float32)
    width = matrix.size // len(values)
    return np.ascontiguousarray(matrix.reshape(len(values), width)[:rows])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=pathlib.Path, required=True)
    parser.add_argument("--queries", type=pathlib.Path, required=True)
    parser.add_argument("--layout-order", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--base-rows", type=int, default=9_000)
    parser.add_argument("--query-count", type=int, default=32)
    args = parser.parse_args()
    if not 0 < args.base_rows < args.rows:
        parser.error("base rows must be inside the corpus")

    source = _fixed_list(args.source, "embedding", args.rows)
    queries = _fixed_list(args.queries, "embedding", args.query_count)
    order = np.asarray(np.load(args.layout_order), dtype=np.int64)
    base_order = order[order < args.base_rows]
    if base_order.size != args.base_rows or np.unique(base_order).size != args.base_rows:
        raise ValueError("base layout order differs")
    result = evaluate_overlay(
        source[base_order],
        source[args.base_rows : args.rows],
        queries,
    )
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
