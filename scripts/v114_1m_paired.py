"""Geometry-driven nomination for the paired V114 million-row gate.

The frozen ReLAION inputs are bound by the runner. This module keeps the
selection rule independent of their row count, dimension and page geometry.
"""

from __future__ import annotations

import numpy as np


def nominate_region_pq64(
    query: np.ndarray,
    summaries: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    *,
    page_rows: int,
    blocks_per_page: int,
    regions: int,
    shortlist: int,
) -> tuple[list[int], list[int], np.ndarray]:
    """Select PQ64 rows from nearest summary regions, with stable ties.

    Returns pages ranked by their best nominated row score, those pages in
    physical order, and nominated row ordinals in score order. The score
    arithmetic deliberately matches the frozen V111/V109 replay for D=768.
    """
    query = np.asarray(query)
    summaries = np.asarray(summaries)
    books = np.asarray(books)
    codes = np.asarray(codes)
    if query.ndim != 1 or query.dtype != np.float32 or query.size == 0:
        raise ValueError("query must be a nonempty float32 vector")
    if summaries.ndim != 2 or summaries.shape[1] != query.size or summaries.dtype != np.float32:
        raise ValueError("summary shape or dtype differs from query")
    if books.ndim != 3 or books.shape[:2] != (64, 256) or books.dtype != np.float32:
        raise ValueError("books must have shape (64, 256, width) and float32 dtype")
    if books.shape[2] * 64 < query.size or books.shape[2] * 64 - query.size >= 64:
        raise ValueError("PQ width does not cover the query exactly")
    if codes.ndim != 2 or codes.shape[1] != 64 or codes.dtype != np.uint8 or codes.shape[0] == 0:
        raise ValueError("codes must have shape (rows, 64) and uint8 dtype")
    if page_rows <= 0 or blocks_per_page <= 0:
        raise ValueError("page and summary block geometry must be positive")
    pages = (codes.shape[0] + page_rows - 1) // page_rows
    if summaries.shape[0] != pages * blocks_per_page:
        raise ValueError("summary count does not match page geometry")
    if not 1 <= regions <= pages or not 1 <= shortlist <= min(regions * page_rows, codes.shape[0]):
        raise ValueError("region or shortlist count is outside geometry")
    if not (np.isfinite(query).all() and np.isfinite(summaries).all() and np.isfinite(books).all()):
        raise ValueError("query, summaries and books must be finite")

    norms = np.einsum("ij,ij->i", summaries, summaries)
    page_scores = (norms - 2.0 * (summaries @ query)).reshape(pages, blocks_per_page).min(axis=1)
    chosen = np.lexsort((np.arange(pages), page_scores))[:regions]
    chosen.sort()
    rows = np.concatenate([
        np.arange(page * page_rows, min((page + 1) * page_rows, codes.shape[0]), dtype=np.int32)
        for page in chosen
    ])
    if shortlist > rows.size:
        raise ValueError("shortlist exceeds rows in selected regions")
    padded_query = np.zeros(books.shape[2] * 64, dtype=np.float32)
    padded_query[:query.size] = query
    delta = books - padded_query.reshape(64, 1, books.shape[2])
    table = np.einsum("ijk,ijk->ij", delta, delta)
    scores = np.zeros(rows.size, dtype=np.float32)
    for subspace in range(64):
        scores += table[subspace, codes[rows, subspace]]
    best = np.lexsort((rows, scores))[:shortlist]
    best_scores: dict[int, float] = {}
    for index in best:
        page = int(rows[index]) // page_rows
        best_scores[page] = min(best_scores.get(page, float("inf")), float(scores[index]))
    ranked = sorted(best_scores, key=lambda page: (best_scores[page], page))
    return ranked, sorted(best_scores), rows[best]
