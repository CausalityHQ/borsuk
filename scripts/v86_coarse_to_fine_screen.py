#!/usr/bin/env python3
"""Fixed two-wave PQ192 quality falsifier for the V85 physical layout."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
import struct
from typing import Any

import numpy as np

from scripts.v85_shared_overlay_screen import (
    _adc_scores,
    _encode_pq,
    _fixed_list,
    _page_payload_bytes,
    _page_sq8,
    _scalar,
    _select_optimal_weighted_pages,
    _sq8,
    _top_ids,
    _train_pq,
)

_ROWS = 1_000_000
_BASE_ROWS = 900_000
_QUERIES = 160
_DEVELOPMENT_QUERIES = 32
_CONFIRMATION_START = 200
_CONFIRMATION_QUERIES = 128
_LOADED_QUERIES = _CONFIRMATION_START + _CONFIRMATION_QUERIES
_NEIGHBORS = 100
_DIMENSIONS = 768
_PAGE_ROWS = 256
_PQ_SUBSPACES = 192
_CODE_PAGE_HEADER_BYTES = 64
_WAVE1_MAX_SPAN_PAGES = 340
_WAVE2_MAX_SPAN_PAGES = 81


def validate_layout_order(
    order: np.ndarray, *, rows: int, base_rows: int
) -> np.ndarray:
    """Return the exact base-only physical order or fail closed."""

    order = np.asarray(order, dtype=np.int64)
    if (
        rows <= 0
        or not 0 < base_rows < rows
        or order.shape != (base_rows,)
        or np.any(order < 0)
        or np.any(order >= base_rows)
        or not np.array_equal(np.sort(order), np.arange(base_rows))
    ):
        raise ValueError("base layout order differs")
    return order


def validate_truth_rows(
    query_ordinals: np.ndarray,
    ranks: np.ndarray,
    ids: np.ndarray,
    *,
    queries: int,
    neighbors: int,
) -> np.ndarray:
    query_ordinals = np.asarray(query_ordinals, dtype=np.int64)
    ranks = np.asarray(ranks, dtype=np.int64)
    ids = np.asarray(ids, dtype=np.int64)
    size = queries * neighbors
    if (
        queries <= 0
        or neighbors <= 0
        or query_ordinals.shape != (size,)
        or ranks.shape != (size,)
        or ids.shape != (size,)
        or not np.array_equal(query_ordinals, np.repeat(np.arange(queries), neighbors))
        or not np.array_equal(ranks, np.tile(np.arange(neighbors), queries))
        or any(np.unique(row).size != neighbors for row in ids.reshape(queries, neighbors))
    ):
        raise ValueError("ground-truth order differs")
    return ids.reshape(queries, neighbors)


def select_evaluation_rows(
    queries: np.ndarray,
    truth_ids: np.ndarray,
    *,
    development_queries: int,
    confirmation_start: int,
    confirmation_queries: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Select a burned development prefix and disjoint confirmation range."""

    queries = np.asarray(queries, dtype=np.float32)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    end = confirmation_start + confirmation_queries
    if (
        queries.ndim != 2
        or truth_ids.ndim != 2
        or queries.shape[0] != truth_ids.shape[0]
        or development_queries <= 0
        or confirmation_start < development_queries
        or confirmation_queries <= 0
        or end > queries.shape[0]
    ):
        raise ValueError("evaluation query selection differs")
    ordinals = np.concatenate(
        (
            np.arange(development_queries, dtype=np.int64),
            np.arange(confirmation_start, end, dtype=np.int64),
        )
    )
    return (
        np.ascontiguousarray(queries[ordinals]),
        np.ascontiguousarray(truth_ids[ordinals]),
        ordinals,
    )


def summarize_gates(result: dict[str, Any], *, neighbors: int) -> dict[str, Any]:
    """Recompute frozen dev/confirmation gates from canonical query samples."""

    samples = result.get("samples")
    if not isinstance(samples, list) or len(samples) != 160 or neighbors <= 0:
        raise ValueError("coarse-to-fine promotion samples differ")
    expected_ordinals = [*range(_DEVELOPMENT_QUERIES), *range(
        _CONFIRMATION_START, _CONFIRMATION_START + _CONFIRMATION_QUERIES
    )]
    if [sample.get("query") for sample in samples] != expected_ordinals:
        raise ValueError("coarse-to-fine promotion ordinals differ")

    def summary(part: list[dict[str, Any]], *, development: bool) -> dict[str, Any]:
        hits = [int(sample["page_sq8_hits"]) for sample in part]
        base_hits = sum(int(sample["page_sq8_base_hits"]) for sample in part)
        base_truth = sum(int(sample["base_truth_hits"]) for sample in part)
        if (
            any(not 0 <= value <= neighbors for value in hits)
            or base_truth <= 0
            or not 0 <= base_hits <= base_truth
        ):
            raise ValueError("coarse-to-fine promotion evidence differs")
        total = sum(hits)
        common = {
            "base_recall_ppm": round(base_hits * 1_000_000 / base_truth),
            "hits": total,
            "recall_ppm": round(total * 1_000_000 / (len(part) * neighbors)),
            "worst_query_hits": min(hits),
        }
        if development:
            common["query_15_hits"] = hits[15]
            common["passed"] = (
                total >= 3_176
                and hits[15] >= 90
                and common["base_recall_ppm"] >= 991_000
            )
        else:
            common["passed"] = (
                total >= 12_685
                and min(hits) >= 85
                and common["base_recall_ppm"] >= 991_000
            )
        return common

    development = summary(samples[:32], development=True)
    confirmation = summary(samples[32:], development=False)
    return {
        "confirmation": confirmation,
        "development": development,
        "passed": bool(development["passed"] and confirmation["passed"]),
    }


def build_run_metadata() -> dict[str, Any]:
    """Return frozen scope and representation-only scale projections."""

    code_page_bytes = _PAGE_ROWS * _PQ_SUBSPACES + _CODE_PAGE_HEADER_BYTES
    projected_pages = 390_625
    return {
        "configuration": {
            "base_rows": _BASE_ROWS,
            "code": "pq192x8",
            "code_page_bytes": code_page_bytes,
            "confirmation_queries": _CONFIRMATION_QUERIES,
            "confirmation_start": _CONFIRMATION_START,
            "development_queries": _DEVELOPMENT_QUERIES,
            "dimensions": _DIMENSIONS,
            "page_rows": _PAGE_ROWS,
            "queries": _QUERIES,
            "resident_page_summaries": 2,
            "rows": _ROWS,
            "training_queries_or_ground_truth": False,
            "wave1_max_bytes": _WAVE1_MAX_SPAN_PAGES * code_page_bytes,
            "wave1_max_ranges": 32,
            "wave1_max_span_pages": _WAVE1_MAX_SPAN_PAGES,
            "wave2_max_bytes": _WAVE2_MAX_SPAN_PAGES
            * _page_payload_bytes(dimensions=_DIMENSIONS, page_rows=_PAGE_ROWS),
            "wave2_max_ranges": 32,
            "wave2_max_span_pages": _WAVE2_MAX_SPAN_PAGES,
            "wave2_top_rows": 512,
        },
        "projection_100m": {
            "codebooks_resident_bytes": _PQ_SUBSPACES * 256 * 4 * 4,
            "page_summaries_resident_bytes": projected_pages
            * 2
            * _PQ_SUBSPACES,
            "page_summary_adc_lookups_per_query": projected_pages
            * 2
            * _PQ_SUBSPACES,
            "representation_bytes_only": True,
            "resident_delta_rows": 100_000,
            "row_codes_s3_bytes": 99_900_000 * _PQ_SUBSPACES,
            "serving_cpu_qualified": False,
            "serving_latency_qualified": False,
        },
        "scope": "fixed-1m-quality-falsifier",
    }


@dataclasses.dataclass(frozen=True)
class CoarseToFineArtifact:
    books: tuple[np.ndarray, ...]
    row_codes: np.ndarray
    summary_codes: np.ndarray
    page_rows: int
    training_rows: int

    def routing_digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(struct.pack("<QQ", self.page_rows, self.training_rows))
        for book in self.books:
            digest.update(np.ascontiguousarray(book).tobytes())
        digest.update(np.ascontiguousarray(self.row_codes).tobytes())
        return digest.hexdigest()

    def digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(struct.pack("<QQ", self.page_rows, self.training_rows))
        for book in self.books:
            digest.update(np.ascontiguousarray(book).tobytes())
        digest.update(np.ascontiguousarray(self.row_codes).tobytes())
        digest.update(np.ascontiguousarray(self.summary_codes).tobytes())
        return digest.hexdigest()


def _means_per_page(
    base: np.ndarray, page_rows: int, summaries_per_page: int
) -> np.ndarray:
    summaries = []
    for start in range(0, base.shape[0], page_rows):
        page = base[start : start + page_rows]
        for summary in range(summaries_per_page):
            first = summary * page_rows // summaries_per_page
            stop = (summary + 1) * page_rows // summaries_per_page
            block = page[first:stop]
            if block.size == 0:
                summaries.append(summaries[-1].copy())
            else:
                summaries.append(
                    np.mean(block, axis=0, dtype=np.float32)
                )
    return np.ascontiguousarray(np.asarray(summaries, dtype=np.float32))


def build_coarse_to_fine_artifact(
    base: np.ndarray,
    *,
    page_rows: int,
    subspaces: int,
    clusters: int,
    sample_rows: int,
    seed: int,
    iterations: int,
    summaries_per_page: int = 2,
) -> CoarseToFineArtifact:
    """Build query-independent PQ row codes and fixed block summaries."""

    base = np.asarray(base, dtype=np.float32)
    if (
        base.ndim != 2
        or base.shape[0] == 0
        or base.shape[1] == 0
        or not np.isfinite(base).all()
        or page_rows <= 1
        or not 0 < summaries_per_page <= page_rows
        or subspaces <= 0
        or base.shape[1] % subspaces
        or clusters <= 0
        or sample_rows <= 0
        or iterations <= 0
    ):
        raise ValueError("coarse-to-fine training authority differs")
    base = np.ascontiguousarray(base)
    books = tuple(
        _train_pq(base, subspaces, clusters, seed, sample_rows, iterations)
    )
    summaries = _means_per_page(base, page_rows, summaries_per_page)
    return CoarseToFineArtifact(
        books=books,
        row_codes=_encode_pq(base, list(books), max(page_rows, 1_024)),
        summary_codes=_encode_pq(summaries, list(books), max(page_rows, 1_024)),
        page_rows=page_rows,
        training_rows=base.shape[0],
    )


def _validate_coarse_to_fine_artifact(
    artifact: CoarseToFineArtifact,
    *,
    rows: int,
    dimensions: int,
    page_rows: int,
    subspaces: int,
    clusters: int,
) -> None:
    page_count = (rows + page_rows - 1) // page_rows
    width = dimensions // subspaces if subspaces > 0 else 0
    if (
        artifact.page_rows != page_rows
        or artifact.training_rows != rows
        or len(artifact.books) != subspaces
        or artifact.row_codes.dtype != np.uint8
        or artifact.row_codes.shape != (rows, subspaces)
        or artifact.summary_codes.dtype != np.uint8
        or artifact.summary_codes.ndim != 2
        or artifact.summary_codes.shape[0] < page_count
        or artifact.summary_codes.shape[0] % page_count
        or artifact.summary_codes.shape[0] // page_count > page_rows
        or artifact.summary_codes.shape[1] != subspaces
        or any(
            book.dtype != np.float32
            or book.shape != (clusters, width)
            or not np.isfinite(book).all()
            for book in artifact.books
        )
    ):
        raise ValueError("coarse-to-fine artifact authority differs")


def plan_ranked_pages(
    page_scores: np.ndarray,
    *,
    rank_limit: int,
    max_span_pages: int,
    max_ranges: int,
    objective: str = "reciprocal-rank",
    calibrated_rank_weights: np.ndarray | None = None,
) -> tuple[np.ndarray, list[tuple[int, int]]]:
    """Plan physical ranges from ranked page evidence, charging all gaps."""

    page_scores = np.asarray(page_scores, dtype=np.float32)
    if (
        page_scores.ndim != 1
        or page_scores.size == 0
        or not np.isfinite(page_scores).all()
        or rank_limit <= 0
        or max_span_pages <= 0
        or max_ranges <= 0
    ):
        raise ValueError("ranked page plan differs")
    if objective not in {"calibrated", "coverage-first", "reciprocal-rank"}:
        raise ValueError("ranked page objective differs")
    take = min(rank_limit, page_scores.size)
    order = np.lexsort((np.arange(page_scores.size), page_scores))[:take]
    weights = np.zeros(page_scores.size, dtype=np.uint64)
    rank_mass = np.asarray(
        [1_000_000_000 // (rank + 1) for rank in range(take)],
        dtype=np.uint64,
    )
    total_rank_mass = sum(int(value) for value in rank_mass)
    if objective == "calibrated":
        calibrated = np.asarray(calibrated_rank_weights)
        if (
            calibrated.ndim != 1
            or calibrated.size < take
            or calibrated.dtype.kind not in "iu"
            or np.any(calibrated < 0)
        ):
            raise ValueError("ranked page objective differs")
        weights[order] = calibrated[:take].astype(np.uint64, copy=False)
        maximum_objective = sum(int(value) for value in calibrated[:take])
    elif objective == "coverage-first":
        if calibrated_rank_weights is not None:
            raise ValueError("ranked page objective differs")
        dominance = total_rank_mass + 1
        weights[order] = np.uint64(dominance) + rank_mass
        maximum_objective = take * dominance + total_rank_mass
    else:
        if calibrated_rank_weights is not None:
            raise ValueError("ranked page objective differs")
        weights[order] = rank_mass
        maximum_objective = total_rank_mass
    if maximum_objective > 2**53:
        raise ValueError("ranked page objective differs")
    return _select_optimal_weighted_pages(
        weights,
        max_span_pages=min(max_span_pages, page_scores.size),
        max_ranges=max_ranges,
    )


def _page_rows(
    pages: np.ndarray, *, rows: int, page_rows: int
) -> np.ndarray:
    return np.concatenate(
        [
            np.arange(
                int(page) * page_rows,
                min((int(page) + 1) * page_rows, rows),
                dtype=np.int64,
            )
            for page in pages
        ]
    )


def _plan_candidate_rows(
    positions: np.ndarray,
    scores: np.ndarray,
    *,
    page_count: int,
    page_rows: int,
    top_rows: int,
    max_span_pages: int,
    max_ranges: int,
) -> tuple[np.ndarray, list[tuple[int, int]]]:
    take = min(top_rows, positions.size)
    order = np.lexsort((positions, scores))[:take]
    weights = np.zeros(page_count, dtype=np.uint64)
    ranked_pages = positions[order] // page_rows
    np.add.at(
        weights,
        ranked_pages,
        np.asarray(
            [1_000_000_000 // (rank + 1) for rank in range(take)],
            dtype=np.uint64,
        ),
    )
    return _select_optimal_weighted_pages(
        weights,
        max_span_pages=min(max_span_pages, page_count),
        max_ranges=max_ranges,
    )


def evaluate_coarse_to_fine(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    query_ordinals: np.ndarray,
    artifact: CoarseToFineArtifact | None = None,
    page_rows: int = _PAGE_ROWS,
    neighbors: int = 100,
    subspaces: int = _PQ_SUBSPACES,
    clusters: int = 256,
    sample_rows: int = 100_000,
    seed: int = 85,
    iterations: int = 10,
    wave1_rank_pages: int = 1_024,
    wave1_max_span_pages: int = _WAVE1_MAX_SPAN_PAGES,
    wave1_max_ranges: int = 32,
    wave1_objective: str = "reciprocal-rank",
    wave1_rank_weights: np.ndarray | None = None,
    wave2_top_rows: int = 512,
    wave2_max_span_pages: int = _WAVE2_MAX_SPAN_PAGES,
    wave2_max_ranges: int = 32,
    code_page_bytes: int | None = None,
    data_page_bytes: int | None = None,
    wave1_page_scores_by_query: np.ndarray | None = None,
    wave1_page_weights_by_query: np.ndarray | None = None,
    wave1_page_evidence_sha256: str | None = None,
) -> dict[str, Any]:
    """Evaluate one fixed two-wave routing design with a resident delta."""

    base = np.asarray(base, dtype=np.float32)
    delta = np.asarray(delta, dtype=np.float32)
    queries = np.asarray(queries, dtype=np.float32)
    base_ids = np.asarray(base_ids, dtype=np.int64)
    delta_ids = np.asarray(delta_ids, dtype=np.int64)
    truth_ids = np.asarray(truth_ids, dtype=np.int64)
    query_ordinals = np.asarray(query_ordinals, dtype=np.int64)
    if (
        base.ndim != 2
        or delta.ndim != 2
        or queries.ndim != 2
        or base.shape[0] == 0
        or delta.shape[0] == 0
        or queries.shape[0] == 0
        or base.shape[1] != delta.shape[1]
        or base.shape[1] != queries.shape[1]
        or base_ids.shape != (base.shape[0],)
        or delta_ids.shape != (delta.shape[0],)
        or truth_ids.shape != (queries.shape[0], neighbors)
        or query_ordinals.shape != (queries.shape[0],)
        or np.unique(query_ordinals).size != query_ordinals.size
        or np.any(query_ordinals < 0)
        or np.unique(np.concatenate((base_ids, delta_ids))).size
        != base_ids.size + delta_ids.size
        or any(np.unique(row).size != neighbors for row in truth_ids)
        or not all(np.isfinite(value).all() for value in (base, delta, queries))
    ):
        raise ValueError("coarse-to-fine evaluation authority differs")

    all_ids = np.concatenate((base_ids, delta_ids))
    if not np.all(np.isin(truth_ids, all_ids)):
        raise ValueError("coarse-to-fine truth authority differs")

    base = np.ascontiguousarray(base)
    delta = np.ascontiguousarray(delta)
    queries = np.ascontiguousarray(queries)
    if artifact is None:
        artifact = build_coarse_to_fine_artifact(
            base,
            page_rows=page_rows,
            subspaces=subspaces,
            clusters=clusters,
            sample_rows=sample_rows,
            seed=seed,
            iterations=iterations,
        )
    _validate_coarse_to_fine_artifact(
        artifact,
        rows=base.shape[0],
        dimensions=base.shape[1],
        page_rows=page_rows,
        subspaces=subspaces,
        clusters=clusters,
    )
    page_count = (base.shape[0] + page_rows - 1) // page_rows
    summaries_per_page = artifact.summary_codes.shape[0] // page_count
    if wave1_page_scores_by_query is not None:
        wave1_page_scores_by_query = np.asarray(
            wave1_page_scores_by_query, dtype=np.float32
        )
        if (
            wave1_page_scores_by_query.shape != (queries.shape[0], page_count)
            or not np.isfinite(wave1_page_scores_by_query).all()
        ):
            raise ValueError("coarse-to-fine page evidence differs")
    if wave1_page_weights_by_query is not None:
        raw_weights = np.asarray(wave1_page_weights_by_query)
        if (
            raw_weights.shape != (queries.shape[0], page_count)
            or raw_weights.dtype.kind not in "iu"
            or (raw_weights.dtype.kind == "i" and np.any(raw_weights < 0))
            or np.any(raw_weights.astype(np.float64).sum(axis=1) > 2**53)
        ):
            raise ValueError("coarse-to-fine page evidence differs")
        wave1_page_weights_by_query = raw_weights.astype(np.uint64, copy=False)
    has_injected_evidence = (
        wave1_page_scores_by_query is not None
        or wave1_page_weights_by_query is not None
    )
    if has_injected_evidence != (
        wave1_page_evidence_sha256 is not None
    ) or (
        wave1_page_evidence_sha256 is not None
        and (
            len(wave1_page_evidence_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in wave1_page_evidence_sha256
            )
        )
    ):
        raise ValueError("coarse-to-fine page evidence differs")
    base_page_sq8 = _page_sq8(base, page_rows)
    delta_sq8 = _sq8(delta)
    if code_page_bytes is None:
        code_page_bytes = page_rows * subspaces + _CODE_PAGE_HEADER_BYTES
    if data_page_bytes is None:
        data_page_bytes = _page_payload_bytes(
            dimensions=base.shape[1], page_rows=page_rows
        )
    if code_page_bytes <= 0 or data_page_bytes <= 0:
        raise ValueError("coarse-to-fine page bytes differ")

    base_id_to_position = {int(value): index for index, value in enumerate(base_ids)}
    delta_id_set = {int(value) for value in delta_ids}
    samples = []
    exact_hits = 0
    page_sq8_hits = 0
    base_truth_hits = 0
    delta_truth_hits = 0
    for query_index, query in enumerate(queries):
        if wave1_page_scores_by_query is None and wave1_page_weights_by_query is None:
            summary_scores = _adc_scores(
                query, artifact.summary_codes, list(artifact.books)
            )
            page_scores = summary_scores.reshape(
                page_count, summaries_per_page
            ).min(axis=1)
        else:
            if wave1_page_scores_by_query is not None:
                page_scores = wave1_page_scores_by_query[query_index]
            else:
                page_scores = np.zeros(page_count, dtype=np.float32)
        if wave1_page_weights_by_query is not None:
            page_weights = wave1_page_weights_by_query[query_index]
        if wave1_page_scores_by_query is not None:
            page_order = np.lexsort((np.arange(page_count), page_scores))
        elif wave1_page_weights_by_query is None:
            page_order = np.lexsort((np.arange(page_count), page_scores))
        else:
            page_order = np.lexsort(
                (
                    np.arange(page_count),
                    np.iinfo(np.uint64).max - page_weights,
                )
            )
        page_ranks = np.empty(page_count, dtype=np.int64)
        page_ranks[page_order] = np.arange(page_count, dtype=np.int64)
        if wave1_page_weights_by_query is None:
            wave1_pages, wave1_ranges = plan_ranked_pages(
                page_scores,
                rank_limit=wave1_rank_pages,
                max_span_pages=wave1_max_span_pages,
                max_ranges=wave1_max_ranges,
                objective=wave1_objective,
                calibrated_rank_weights=wave1_rank_weights,
            )
        else:
            if wave1_rank_weights is not None:
                raise ValueError("coarse-to-fine page evidence differs")
            wave1_pages, wave1_ranges = _select_optimal_weighted_pages(
                page_weights,
                max_span_pages=min(wave1_max_span_pages, page_count),
                max_ranges=wave1_max_ranges,
            )
        selected_page_ranks = page_ranks[wave1_pages]
        selected_ranked_page_ranks = selected_page_ranks[
            selected_page_ranks < wave1_rank_pages
        ]
        wave1_reciprocal_rank_utility = sum(
            1_000_000_000 // (int(rank) + 1)
            for rank in selected_ranked_page_ranks
        )
        wave1_positions = _page_rows(
            wave1_pages, rows=base.shape[0], page_rows=page_rows
        )
        wave1_scores = _adc_scores(
            query, artifact.row_codes[wave1_positions], list(artifact.books)
        )
        shortlist_take = min(wave2_top_rows, wave1_positions.size)
        shortlist_order = np.lexsort((wave1_positions, wave1_scores))[
            :shortlist_take
        ]
        pq_shortlist_positions = wave1_positions[shortlist_order]
        wave2_pages, wave2_ranges = _plan_candidate_rows(
            wave1_positions,
            wave1_scores,
            page_count=page_count,
            page_rows=page_rows,
            top_rows=wave2_top_rows,
            max_span_pages=wave2_max_span_pages,
            max_ranges=wave2_max_ranges,
        )
        wave2_positions = _page_rows(
            wave2_pages, rows=base.shape[0], page_rows=page_rows
        )
        candidate_ids = np.concatenate((base_ids[wave2_positions], delta_ids))
        exact_vectors = np.concatenate((base[wave2_positions], delta), axis=0)
        page_sq8_vectors = np.concatenate(
            (base_page_sq8[wave2_positions], delta_sq8), axis=0
        )
        exact_result = _top_ids(query, exact_vectors, candidate_ids, neighbors)
        page_sq8_result = _top_ids(
            query, page_sq8_vectors, candidate_ids, neighbors
        )

        expected = {int(value) for value in truth_ids[query_index]}
        expected_base_ids = expected.intersection(base_id_to_position)
        expected_base_positions = np.asarray(
            [
                base_id_to_position[int(value)]
                for value in truth_ids[query_index]
                if int(value) in base_id_to_position
            ],
            dtype=np.int64,
        )
        query_base_truth = expected_base_positions.size
        query_delta_truth = sum(value in delta_id_set for value in expected)
        query_wave1_truth = int(
            np.count_nonzero(np.isin(expected_base_positions, wave1_positions))
        )
        expected_base_page_ranks = page_ranks[
            expected_base_positions // page_rows
        ]
        rank_visible = expected_base_page_ranks < wave1_rank_pages
        selected = np.isin(expected_base_positions // page_rows, wave1_pages)
        query_rank_visible_truth = int(np.count_nonzero(rank_visible))
        query_rank_visible_selected_truth = int(
            np.count_nonzero(rank_visible & selected)
        )
        query_planner_missed_truth = (
            query_rank_visible_truth - query_rank_visible_selected_truth
        )
        query_gap_only_truth = query_wave1_truth - query_rank_visible_selected_truth
        query_pq_shortlist_truth = int(
            np.count_nonzero(
                np.isin(expected_base_positions, pq_shortlist_positions)
            )
        )
        query_wave2_page_truth = int(
            np.count_nonzero(
                np.isin(expected_base_positions // page_rows, wave2_pages)
            )
        )
        query_exact_hits = len(expected.intersection(exact_result))
        query_page_sq8_hits = len(expected.intersection(page_sq8_result))
        query_exact_base_hits = len(expected_base_ids.intersection(exact_result))
        query_page_sq8_base_hits = len(
            expected_base_ids.intersection(page_sq8_result)
        )
        base_truth_hits += query_base_truth
        delta_truth_hits += query_delta_truth
        exact_hits += query_exact_hits
        page_sq8_hits += query_page_sq8_hits
        samples.append(
            {
                "base_truth_hits": int(query_base_truth),
                "delta_truth_hits": int(query_delta_truth),
                "exact_base_hits": query_exact_base_hits,
                "exact_hits": query_exact_hits,
                "page_sq8_base_hits": query_page_sq8_base_hits,
                "page_sq8_hits": query_page_sq8_hits,
                "pq_shortlist_base_truth_hits": query_pq_shortlist_truth,
                "query": int(query_ordinals[query_index]),
                "truth_base_pages": [
                    int(position // page_rows)
                    for position in expected_base_positions
                ],
                "truth_page_ranks": [
                    int(page_ranks[position // page_rows])
                    for position in expected_base_positions
                ],
                "wave1_base_truth_hits": query_wave1_truth,
                "wave1_bytes": int(wave1_pages.size * code_page_bytes),
                "wave1_gap_only_base_truth_hits": query_gap_only_truth,
                "wave1_gets": len(wave1_ranges),
                "wave1_pages": [int(page) for page in wave1_pages],
                "wave1_planner_missed_base_truth_hits": query_planner_missed_truth,
                "wave1_rank_visible_base_truth_hits": query_rank_visible_truth,
                "wave1_rank_visible_selected_base_truth_hits": (
                    query_rank_visible_selected_truth
                ),
                "wave1_ranked_pages_selected": int(
                    selected_ranked_page_ranks.size
                ),
                "wave1_ranges": [list(pair) for pair in wave1_ranges],
                "wave1_reciprocal_rank_utility": int(
                    wave1_reciprocal_rank_utility
                ),
                "wave2_base_truth_page_hits": query_wave2_page_truth,
                "wave2_bytes": int(wave2_pages.size * data_page_bytes),
                "wave2_gets": len(wave2_ranges),
                "wave2_pages": [int(page) for page in wave2_pages],
                "wave2_ranges": [list(pair) for pair in wave2_ranges],
            }
        )

    denominator = queries.shape[0] * neighbors
    result = {
        "artifact_sha256": artifact.digest(),
        "base_truth_hits": base_truth_hits,
        "claim_eligible": False,
        "delta_truth_hits": delta_truth_hits,
        "exact_recall_ppm": round(exact_hits * 1_000_000 / denominator),
        "page_sq8_recall_ppm": round(page_sq8_hits * 1_000_000 / denominator),
        "samples": samples,
        "schema": "borsuk-v86-coarse-to-fine-screen-v1",
        "wave1_objective": (
            "direct-page-weight"
            if wave1_page_weights_by_query is not None
            else wave1_objective
        ),
    }
    if wave1_page_evidence_sha256 is not None:
        result["wave1_page_evidence_sha256"] = wave1_page_evidence_sha256
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed V86 two-wave PQ192 1M falsifier."
    )
    parser.add_argument("--source", type=pathlib.Path, required=True)
    parser.add_argument("--queries", type=pathlib.Path, required=True)
    parser.add_argument("--ground-truth", type=pathlib.Path, required=True)
    parser.add_argument("--layout-order", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()

    source_ids = _scalar(args.source, "feature_row_id", _ROWS)
    if np.unique(source_ids).size != _ROWS:
        raise ValueError("source identifiers differ")
    query_ordinals = _scalar(
        args.ground_truth, "query_ordinal", _LOADED_QUERIES * _NEIGHBORS
    )
    ranks = _scalar(
        args.ground_truth, "rank", _LOADED_QUERIES * _NEIGHBORS
    )
    loaded_truth_ids = validate_truth_rows(
        query_ordinals,
        ranks,
        _scalar(
            args.ground_truth,
            "feature_row_id",
            _LOADED_QUERIES * _NEIGHBORS,
        ),
        queries=_LOADED_QUERIES,
        neighbors=_NEIGHBORS,
    )
    full_order = np.asarray(np.load(args.layout_order), dtype=np.int64)
    if (
        full_order.shape != (_ROWS,)
        or np.any(full_order < 0)
        or np.any(full_order >= _ROWS)
        or np.unique(full_order).size != _ROWS
    ):
        raise ValueError("full layout order differs")
    base_order = validate_layout_order(
        full_order[full_order < _BASE_ROWS], rows=_ROWS, base_rows=_BASE_ROWS
    )

    source = _fixed_list(args.source, "embedding", _ROWS)
    if source.shape != (_ROWS, _DIMENSIONS):
        raise ValueError("source dimensions differ")
    loaded_queries = _fixed_list(args.queries, "embedding", _LOADED_QUERIES)
    if loaded_queries.shape != (_LOADED_QUERIES, _DIMENSIONS):
        raise ValueError("query dimensions differ")
    queries, truth_ids, evaluation_ordinals = select_evaluation_rows(
        loaded_queries,
        loaded_truth_ids,
        development_queries=_DEVELOPMENT_QUERIES,
        confirmation_start=_CONFIRMATION_START,
        confirmation_queries=_CONFIRMATION_QUERIES,
    )
    result = evaluate_coarse_to_fine(
        source[base_order],
        source[_BASE_ROWS:],
        queries,
        base_ids=source_ids[base_order],
        delta_ids=source_ids[_BASE_ROWS:],
        truth_ids=truth_ids,
        query_ordinals=evaluation_ordinals,
    )
    result["gates"] = summarize_gates(result, neighbors=_NEIGHBORS)
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
