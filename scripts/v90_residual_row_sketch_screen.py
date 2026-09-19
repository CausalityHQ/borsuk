#!/usr/bin/env python3
"""Burned-development falsifier for resident per-row residual PQ evidence."""

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
    _maximum_physical_oracle_hits,
    _page_payload_bytes,
    _scalar,
    _train_pq,
)
from scripts.v86_coarse_to_fine_screen import (
    CoarseToFineArtifact,
    _validate_coarse_to_fine_artifact,
    build_coarse_to_fine_artifact,
    evaluate_coarse_to_fine,
    validate_layout_order,
    validate_truth_rows,
)
from scripts.v87_summary_capacity_screen import (
    _REGISTERED_CONTROL_ARTIFACT_SHA256,
    choose_summary_candidate,
    select_development_rows,
    summarize_development_arm,
    summarize_wave1_attribution,
    validate_control_reproduction,
)
from scripts.v88_planner_objective_screen import (
    _rank_cut_attribution,
    _truth_oracle,
    validate_result_budgets,
)

_ROWS = 1_000_000
_BASE_ROWS = 900_000
_LOADED_QUERIES = 328
_DEVELOPMENT_QUERIES = 32
_NEIGHBORS = 100
_DIMENSIONS = 768
_PAGE_ROWS = 256
_CONTROL_SUBSPACES = 192
_SKETCH_SUBSPACES = 16
_CLUSTERS = 256
_TRAINING_SAMPLE_ROWS = 100_000
_TRAINING_ITERATIONS = 10
_CONTROL_SEED = 85
_SKETCH_SEED = 90
_CODE_PAGE_HEADER_BYTES = 64
_WAVE1_RANK_PAGES = 1_024
_WAVE1_MAX_SPAN_PAGES = 340
_WAVE1_MAX_RANGES = 32
_WAVE2_TOP_ROWS = 512
_WAVE2_MAX_SPAN_PAGES = 81
_WAVE2_MAX_RANGES = 32


def _array_sha256(values: np.ndarray, dtype: np.dtype[Any]) -> str:
    array = np.ascontiguousarray(values, dtype=dtype)
    return hashlib.sha256(memoryview(array).cast("B")).hexdigest()


def _valid_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


@dataclasses.dataclass(frozen=True)
class ResidualRowSketchArtifact:
    base_vectors_sha256: str
    books: tuple[np.ndarray, ...]
    control_artifact_sha256: str
    page_rows: int
    row_codes: np.ndarray
    training_rows: int

    def digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(
            struct.pack(
                "<QQQ", self.page_rows, self.training_rows, len(self.books)
            )
        )
        digest.update(self.base_vectors_sha256.encode("ascii"))
        digest.update(self.control_artifact_sha256.encode("ascii"))
        for book in self.books:
            digest.update(np.ascontiguousarray(book).tobytes())
        digest.update(np.ascontiguousarray(self.row_codes).tobytes())
        return digest.hexdigest()


def decode_page_means(
    artifact: CoarseToFineArtifact, *, dimensions: int, rows: int
) -> np.ndarray:
    """Decode and average the registered fixed-block summaries per page."""

    if not artifact.books:
        raise ValueError("V90 control artifact differs")
    _validate_coarse_to_fine_artifact(
        artifact,
        rows=rows,
        dimensions=dimensions,
        page_rows=artifact.page_rows,
        subspaces=len(artifact.books),
        clusters=artifact.books[0].shape[0],
    )
    page_count = (rows + artifact.page_rows - 1) // artifact.page_rows
    summaries_per_page = artifact.summary_codes.shape[0] // page_count
    decoded = np.concatenate(
        [
            book[artifact.summary_codes[:, index]]
            for index, book in enumerate(artifact.books)
        ],
        axis=1,
    )
    return np.ascontiguousarray(
        decoded.reshape(page_count, summaries_per_page, dimensions).mean(
            axis=1, dtype=np.float32
        ),
        dtype=np.float32,
    )


def _decode_candidate_page_means(
    artifact: CoarseToFineArtifact,
    candidates: np.ndarray,
    *,
    dimensions: int,
    rows: int,
) -> np.ndarray:
    """Decode means for only the candidate pages needed by one query."""

    candidates = np.asarray(candidates)
    page_count = (rows + artifact.page_rows - 1) // artifact.page_rows
    if (
        candidates.ndim != 1
        or candidates.dtype.kind not in "iu"
        or np.any(candidates < 0)
        or np.any(candidates >= page_count)
        or np.unique(candidates).size != candidates.size
    ):
        raise ValueError("V90 candidate pages differ")
    _validate_coarse_to_fine_artifact(
        artifact,
        rows=rows,
        dimensions=dimensions,
        page_rows=artifact.page_rows,
        subspaces=len(artifact.books),
        clusters=artifact.books[0].shape[0] if artifact.books else 0,
    )
    summaries_per_page = artifact.summary_codes.shape[0] // page_count
    summary_positions = (
        candidates[:, None] * summaries_per_page
        + np.arange(summaries_per_page, dtype=np.int64)[None, :]
    )
    decoded_blocks = []
    for subspace, book in enumerate(artifact.books):
        codes = artifact.summary_codes[summary_positions, subspace]
        decoded_blocks.append(book[codes].mean(axis=1, dtype=np.float32))
    return np.ascontiguousarray(np.concatenate(decoded_blocks, axis=1))


def _validate_sketch(
    artifact: ResidualRowSketchArtifact,
    *,
    rows: int,
    dimensions: int,
) -> None:
    width = dimensions // len(artifact.books) if artifact.books else 0
    clusters = artifact.books[0].shape[0] if artifact.books else 0
    if (
        not _valid_sha256(artifact.base_vectors_sha256)
        or not _valid_sha256(artifact.control_artifact_sha256)
        or artifact.page_rows <= 1
        or artifact.training_rows != rows
        or not artifact.books
        or dimensions % len(artifact.books)
        or clusters <= 0
        or clusters > 256
        or artifact.row_codes.dtype != np.uint8
        or artifact.row_codes.shape != (rows, len(artifact.books))
        or any(
            book.dtype != np.float32
            or book.shape != (clusters, width)
            or not np.isfinite(book).all()
            for book in artifact.books
        )
    ):
        raise ValueError("V90 sketch artifact differs")


def build_residual_row_sketch(
    base: np.ndarray,
    control_artifact: CoarseToFineArtifact,
    *,
    subspaces: int,
    clusters: int,
    sample_rows: int,
    seed: int,
    iterations: int,
) -> ResidualRowSketchArtifact:
    """Build query-independent per-row PQ codes around decoded page means."""

    base = np.asarray(base, dtype=np.float32)
    if (
        base.ndim != 2
        or base.shape[0] == 0
        or base.shape[1] == 0
        or not np.isfinite(base).all()
        or subspaces <= 0
        or base.shape[1] % subspaces
        or not 0 < clusters <= 256
        or not 0 < sample_rows <= base.shape[0]
        or seed < 0
        or iterations <= 0
    ):
        raise ValueError("V90 sketch training authority differs")
    means = decode_page_means(
        control_artifact, dimensions=base.shape[1], rows=base.shape[0]
    )
    residuals = np.ascontiguousarray(base.copy())
    for page, start in enumerate(range(0, base.shape[0], control_artifact.page_rows)):
        stop = min(start + control_artifact.page_rows, base.shape[0])
        residuals[start:stop] -= means[page]
    books = tuple(
        _train_pq(
            residuals,
            subspaces,
            clusters,
            seed,
            sample_rows,
            iterations,
        )
    )
    result = ResidualRowSketchArtifact(
        base_vectors_sha256=_array_sha256(base, np.dtype(np.float32)),
        books=books,
        control_artifact_sha256=control_artifact.digest(),
        page_rows=control_artifact.page_rows,
        row_codes=_encode_pq(
            residuals, list(books), max(control_artifact.page_rows, 1_024)
        ),
        training_rows=base.shape[0],
    )
    _validate_sketch(result, rows=base.shape[0], dimensions=base.shape[1])
    return result


def _residual_candidate_row_estimates(
    query: np.ndarray,
    control_page_scores: np.ndarray,
    control_artifact: CoarseToFineArtifact,
    artifact: ResidualRowSketchArtifact,
    *,
    rank_limit: int,
    validated_control_sha256: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Score every valid row inside the registered control-page fence."""

    query = np.asarray(query, dtype=np.float32)
    control_page_scores = np.asarray(control_page_scores, dtype=np.float32)
    page_count = (artifact.training_rows + artifact.page_rows - 1) // artifact.page_rows
    dimensions = sum(book.shape[1] for book in artifact.books)
    _validate_sketch(artifact, rows=artifact.training_rows, dimensions=dimensions)
    if (
        not _valid_sha256(validated_control_sha256)
        or artifact.control_artifact_sha256 != validated_control_sha256
        or artifact.page_rows != control_artifact.page_rows
        or query.shape != (dimensions,)
        or control_page_scores.shape != (page_count,)
        or not np.isfinite(query).all()
        or not np.isfinite(control_page_scores).all()
        or rank_limit <= 0
    ):
        raise ValueError("V90 sketch scoring authority differs")

    take = min(rank_limit, page_count)
    physical_pages = np.arange(page_count, dtype=np.int64)
    control_order = np.lexsort((physical_pages, control_page_scores))
    candidates = np.ascontiguousarray(control_order[:take], dtype=np.int64)
    page_means = _decode_candidate_page_means(
        control_artifact,
        candidates,
        dimensions=dimensions,
        rows=artifact.training_rows,
    )
    row_offsets = np.arange(artifact.page_rows, dtype=np.int64)
    positions = candidates[:, None] * artifact.page_rows + row_offsets[None, :]
    valid = positions < artifact.training_rows
    safe_positions = np.minimum(positions, artifact.training_rows - 1)
    estimates = np.zeros((take, artifact.page_rows), dtype=np.float32)
    width = dimensions // len(artifact.books)
    residual_queries = query[None, :] - page_means
    for subspace, book in enumerate(artifact.books):
        start = subspace * width
        stop = start + width
        query_block = residual_queries[:, start:stop]
        table = (
            np.einsum("ij,ij->i", query_block, query_block)[:, None]
            + np.einsum("ij,ij->i", book, book)[None, :]
            - np.float32(2.0) * (query_block @ book.T)
        )
        codes = artifact.row_codes[safe_positions, subspace]
        estimates += np.take_along_axis(table, codes, axis=1)
    estimates[~valid] = np.float32(np.inf)
    return candidates, estimates, valid


def _rank_scores_from_candidate_estimates(
    candidates: np.ndarray,
    estimates: np.ndarray,
    control_page_scores: np.ndarray,
    *,
    page_rows: int,
) -> np.ndarray:
    """Apply the registered V90 row-min/second-min page ordering."""

    candidates = np.asarray(candidates, dtype=np.int64)
    estimates = np.asarray(estimates, dtype=np.float32)
    control_page_scores = np.asarray(control_page_scores, dtype=np.float32)
    page_count = control_page_scores.size
    if (
        candidates.ndim != 1
        or estimates.shape != (candidates.size, page_rows)
        or np.any(candidates < 0)
        or np.any(candidates >= page_count)
        or np.unique(candidates).size != candidates.size
        or not np.isfinite(control_page_scores).all()
        or page_rows <= 0
    ):
        raise ValueError("V90 residual rank evidence differs")
    physical_pages = np.arange(page_count, dtype=np.int64)
    control_order = np.lexsort((physical_pages, control_page_scores))
    if page_rows == 1:
        minima = estimates[:, 0]
        seconds = minima
    else:
        first_two = np.partition(estimates, 1, axis=1)[:, :2]
        minima = np.min(first_two, axis=1)
        seconds = np.max(first_two, axis=1)
    candidate_order = np.lexsort((candidates, seconds, minima))
    ranked_candidates = candidates[candidate_order]
    selected = np.zeros(page_count, dtype=bool)
    selected[candidates] = True
    full_order = np.concatenate((ranked_candidates, control_order[~selected[control_order]]))
    result = np.empty(page_count, dtype=np.float32)
    result[full_order] = np.arange(page_count, dtype=np.float32)
    return result


def _residual_page_rank_scores_validated(
    query: np.ndarray,
    control_page_scores: np.ndarray,
    control_artifact: CoarseToFineArtifact,
    artifact: ResidualRowSketchArtifact,
    *,
    rank_limit: int,
    validated_control_sha256: str,
) -> np.ndarray:
    """Rerank only registered summary candidates by their best residual row."""

    candidates, estimates, _ = _residual_candidate_row_estimates(
        query,
        control_page_scores,
        control_artifact,
        artifact,
        rank_limit=rank_limit,
        validated_control_sha256=validated_control_sha256,
    )
    return _rank_scores_from_candidate_estimates(
        candidates,
        estimates,
        control_page_scores,
        page_rows=artifact.page_rows,
    )


def residual_page_rank_scores(
    query: np.ndarray,
    control_page_scores: np.ndarray,
    control_artifact: CoarseToFineArtifact,
    artifact: ResidualRowSketchArtifact,
    *,
    rank_limit: int,
) -> np.ndarray:
    """Authenticate, then rerank registered summary candidates by residual row."""

    return _residual_page_rank_scores_validated(
        query,
        control_page_scores,
        control_artifact,
        artifact,
        rank_limit=rank_limit,
        validated_control_sha256=control_artifact.digest(),
    )


def _control_page_scores(
    query: np.ndarray, artifact: CoarseToFineArtifact, page_count: int
) -> np.ndarray:
    summaries_per_page = artifact.summary_codes.shape[0] // page_count
    summary_scores = _adc_scores(query, artifact.summary_codes, list(artifact.books))
    return summary_scores.reshape(page_count, summaries_per_page).min(axis=1)


def _candidate_fenced_truth_oracle(
    result: dict[str, Any],
    *,
    neighbors: int,
    page_count: int,
    page_rows: int,
    rank_limit: int,
    max_pages: int,
    max_ranges: int,
) -> dict[str, Any]:
    """Compute the physical truth ceiling inside the control candidate fence."""

    samples = result.get("samples")
    if (
        not isinstance(samples, list)
        or not samples
        or neighbors <= 0
        or page_count <= 0
        or page_rows <= 0
        or rank_limit <= 0
    ):
        raise ValueError("V90 candidate-fenced oracle differs")
    query_hits: list[int] = []
    base_hits = 0
    base_truth = 0
    for query, sample in enumerate(samples):
        pages = np.asarray(sample.get("truth_base_pages"), dtype=np.int64)
        ranks = np.asarray(sample.get("truth_page_ranks"), dtype=np.int64)
        delta_hits = int(sample.get("delta_truth_hits", -1))
        query_base_truth = int(sample.get("base_truth_hits", -1))
        if (
            sample.get("query") != query
            or pages.ndim != 1
            or ranks.shape != pages.shape
            or pages.size != query_base_truth
            or delta_hits < 0
            or query_base_truth < 0
            or np.any(pages < 0)
            or np.any(pages >= page_count)
            or np.any(ranks < 0)
        ):
            raise ValueError("V90 candidate-fenced oracle differs")
        visible = ranks < rank_limit
        visible_pages, visible_counts = np.unique(
            pages[visible], return_counts=True
        )
        page_hits = {
            int(page): int(visible_counts[index])
            for index, page in enumerate(visible_pages)
        }
        query_base_hits = _maximum_physical_oracle_hits(
            page_hits,
            page_count=page_count,
            max_pages=max_pages,
            max_ranges=max_ranges,
        )
        base_hits += query_base_hits
        base_truth += query_base_truth
        query_hits.append(query_base_hits + delta_hits)
    if base_truth <= 0:
        raise ValueError("V90 candidate-fenced oracle differs")
    hits = sum(query_hits)
    base_recall_ppm = round(base_hits * 1_000_000 / base_truth)
    query_15_hits = query_hits[15] if len(query_hits) > 15 else None
    return {
        "base_hits": base_hits,
        "base_recall_ppm": base_recall_ppm,
        "can_pass_registered_gate": len(query_hits) == _DEVELOPMENT_QUERIES
        and hits >= 3_176
        and query_15_hits is not None
        and query_15_hits >= 90
        and base_recall_ppm >= 991_000,
        "hits": hits,
        "query_15_hits": query_15_hits,
        "worst_query_hits": min(query_hits),
    }


def evaluate_residual_sketch_pair(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    control_artifact: CoarseToFineArtifact,
    sketch_artifact: ResidualRowSketchArtifact,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    query_ordinals: np.ndarray,
    page_rows: int,
    neighbors: int,
    control_subspaces: int,
    control_clusters: int,
    wave1_rank_pages: int,
    wave1_max_span_pages: int,
    wave1_max_ranges: int,
    wave2_top_rows: int,
    wave2_max_span_pages: int,
    wave2_max_ranges: int,
    code_page_bytes: int | None = None,
    data_page_bytes: int | None = None,
) -> dict[str, Any]:
    """Evaluate the registered summary control and one residual-row challenger."""

    base = np.asarray(base, dtype=np.float32)
    queries = np.asarray(queries, dtype=np.float32)
    _validate_sketch(
        sketch_artifact, rows=base.shape[0], dimensions=base.shape[1]
    )
    control_sha256 = control_artifact.digest()
    if (
        sketch_artifact.base_vectors_sha256
        != _array_sha256(base, np.dtype(np.float32))
        or sketch_artifact.control_artifact_sha256 != control_sha256
        or sketch_artifact.page_rows != page_rows
    ):
        raise ValueError("V90 sketch binding differs")
    page_count = (base.shape[0] + page_rows - 1) // page_rows
    challenger_scores = np.vstack(
        [
            _residual_page_rank_scores_validated(
                query,
                _control_page_scores(query, control_artifact, page_count),
                control_artifact,
                sketch_artifact,
                rank_limit=wave1_rank_pages,
                validated_control_sha256=control_sha256,
            )
            for query in queries
        ]
    )
    if code_page_bytes is None:
        code_page_bytes = page_rows * control_subspaces + _CODE_PAGE_HEADER_BYTES
    if data_page_bytes is None:
        data_page_bytes = _page_payload_bytes(
            dimensions=base.shape[1], page_rows=page_rows
        )
    common = {
        "artifact": control_artifact,
        "base_ids": base_ids,
        "delta_ids": delta_ids,
        "truth_ids": truth_ids,
        "query_ordinals": query_ordinals,
        "page_rows": page_rows,
        "neighbors": neighbors,
        "subspaces": control_subspaces,
        "clusters": control_clusters,
        "wave1_rank_pages": wave1_rank_pages,
        "wave1_max_span_pages": wave1_max_span_pages,
        "wave1_max_ranges": wave1_max_ranges,
        "wave2_top_rows": wave2_top_rows,
        "wave2_max_span_pages": wave2_max_span_pages,
        "wave2_max_ranges": wave2_max_ranges,
        "code_page_bytes": code_page_bytes,
        "data_page_bytes": data_page_bytes,
    }
    arms: dict[str, dict[str, Any]] = {}
    for name, page_scores in (("control", None), ("challenger", challenger_scores)):
        result = evaluate_coarse_to_fine(
            base,
            delta,
            queries,
            wave1_page_scores_by_query=page_scores,
            wave1_page_evidence_sha256=(
                None if page_scores is None else sketch_artifact.digest()
            ),
            **common,
        )
        arms[name] = {
            "artifact_sha256": control_sha256,
            "budgets": validate_result_budgets(
                result,
                wave1_page_bytes=code_page_bytes,
                wave1_max_pages=wave1_max_span_pages,
                wave1_max_ranges=wave1_max_ranges,
                wave2_page_bytes=data_page_bytes,
                wave2_max_pages=wave2_max_span_pages,
                wave2_max_ranges=wave2_max_ranges,
            ),
            "page_evidence": "two-summary-pq192" if page_scores is None else "residual-pq16-row-min",
            "rank_cut_base_truth_hits": _rank_cut_attribution(result),
            "result": result,
            "wave1_attribution": summarize_wave1_attribution(result),
        }
    if (
        arms["control"]["rank_cut_base_truth_hits"]["1024"]
        != arms["challenger"]["rank_cut_base_truth_hits"]["1024"]
    ):
        raise ValueError("V90 candidate fence differs")
    candidate_fence_oracle = _candidate_fenced_truth_oracle(
        arms["control"]["result"],
        neighbors=neighbors,
        page_count=page_count,
        page_rows=page_rows,
        rank_limit=wave1_rank_pages,
        max_pages=wave1_max_span_pages,
        max_ranges=wave1_max_ranges,
    )
    return {
        "actual_s3_requests": 0,
        "arms": arms,
        "candidate_fence_oracle": candidate_fence_oracle,
        "changed_parameter": "wave1_page_evidence",
        "claim_eligible": False,
        "delta_representation": "production-sq8",
        "schema": "borsuk-v90-residual-row-sketch-screen-v1",
        "sketch_sha256": sketch_artifact.digest(),
    }


def build_run_metadata() -> dict[str, Any]:
    projected_base_rows = 99_900_000
    projected_pages = (projected_base_rows + _PAGE_ROWS - 1) // _PAGE_ROWS
    residual_codes = projected_base_rows * _SKETCH_SUBSPACES
    summaries = projected_pages * 2 * _CONTROL_SUBSPACES
    residual_books = _SKETCH_SUBSPACES * _CLUSTERS * (_DIMENSIONS // _SKETCH_SUBSPACES) * 4
    control_books = _CONTROL_SUBSPACES * _CLUSTERS * (_DIMENSIONS // _CONTROL_SUBSPACES) * 4
    resident_delta = 100_000 * _DIMENSIONS * 4
    resident_bytes = residual_codes + summaries + residual_books + control_books + resident_delta
    dense_traceback_bytes = (
        2 * 390_625 * (_WAVE1_MAX_RANGES + 1) * (_WAVE1_MAX_SPAN_PAGES + 1)
    )
    return {
        "configuration": {
            "base_rows": _BASE_ROWS,
            "dimensions": _DIMENSIONS,
            "page_rows": _PAGE_ROWS,
            "queries": _DEVELOPMENT_QUERIES,
            "rows": _ROWS,
            "sketch_bytes_per_base_row": _SKETCH_SUBSPACES,
            "sketch_clusters": _CLUSTERS,
            "sketch_iterations": _TRAINING_ITERATIONS,
            "sketch_sample_rows": _TRAINING_SAMPLE_ROWS,
            "sketch_seed": _SKETCH_SEED,
            "uses_query_or_truth_labels": False,
            "wave1_max_bytes": _WAVE1_MAX_SPAN_PAGES
            * (_PAGE_ROWS * _CONTROL_SUBSPACES + _CODE_PAGE_HEADER_BYTES),
            "wave1_max_pages": _WAVE1_MAX_SPAN_PAGES,
            "wave1_max_ranges": _WAVE1_MAX_RANGES,
            "wave1_rank_pages": _WAVE1_RANK_PAGES,
            "wave2_max_bytes": _WAVE2_MAX_SPAN_PAGES
            * _page_payload_bytes(dimensions=_DIMENSIONS, page_rows=_PAGE_ROWS),
            "wave2_max_pages": _WAVE2_MAX_SPAN_PAGES,
            "wave2_max_ranges": _WAVE2_MAX_RANGES,
        },
        "confirmation_required": {
            "queries": 128,
            "requires_new_authenticated_input": True,
            "start": 328,
        },
        "io_evidence": "planned-only-no-s3-query-requests",
        "planned_max_requests_per_query": {"challenger": 64, "control": 64},
        "projection_100m": {
            "candidate_page_mean_scratch_bytes": _WAVE1_RANK_PAGES
            * _DIMENSIONS
            * 4,
            "control_codebooks_resident_bytes": control_books,
            "decoded_page_means_resident_bytes": 0,
            "dense_wave1_traceback_bytes_per_query": dense_traceback_bytes,
            "page_summaries_resident_bytes": summaries,
            "resident_bytes": resident_bytes,
            "resident_bytes_scope": "representation-and-resident-delta-only",
            "resident_delta_float32_bytes": resident_delta,
            "residual_codebooks_resident_bytes": residual_books,
            "residual_row_codes_resident_bytes": residual_codes,
            "serving_cpu_qualified": False,
            "serving_latency_qualified": False,
            "serving_memory_qualified": False,
        },
        "scope": "fixed-1m-burned-development-row-sketch-falsifier",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed V90 resident residual-row sketch falsifier."
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
    size = _LOADED_QUERIES * _NEIGHBORS
    loaded_truth = validate_truth_rows(
        _scalar(args.ground_truth, "query_ordinal", size),
        _scalar(args.ground_truth, "rank", size),
        _scalar(args.ground_truth, "feature_row_id", size),
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
    loaded_queries = _fixed_list(args.queries, "embedding", _LOADED_QUERIES)
    if source.shape != (_ROWS, _DIMENSIONS) or loaded_queries.shape != (
        _LOADED_QUERIES,
        _DIMENSIONS,
    ):
        raise ValueError("V90 vector dimensions differ")
    queries, truth, query_ordinals = select_development_rows(
        loaded_queries, loaded_truth, development_queries=_DEVELOPMENT_QUERIES
    )
    base = np.ascontiguousarray(source[base_order])
    base_ids = np.ascontiguousarray(source_ids[base_order])
    control_artifact = build_coarse_to_fine_artifact(
        base,
        page_rows=_PAGE_ROWS,
        subspaces=_CONTROL_SUBSPACES,
        clusters=_CLUSTERS,
        sample_rows=_TRAINING_SAMPLE_ROWS,
        seed=_CONTROL_SEED,
        iterations=_TRAINING_ITERATIONS,
    )
    if control_artifact.digest() != _REGISTERED_CONTROL_ARTIFACT_SHA256:
        raise ValueError("V90 registered control artifact differs")
    sketch_artifact = build_residual_row_sketch(
        base,
        control_artifact,
        subspaces=_SKETCH_SUBSPACES,
        clusters=_CLUSTERS,
        sample_rows=_TRAINING_SAMPLE_ROWS,
        seed=_SKETCH_SEED,
        iterations=_TRAINING_ITERATIONS,
    )
    result = evaluate_residual_sketch_pair(
        base,
        np.ascontiguousarray(source[_BASE_ROWS:]),
        queries,
        control_artifact=control_artifact,
        sketch_artifact=sketch_artifact,
        base_ids=base_ids,
        delta_ids=source_ids[_BASE_ROWS:],
        truth_ids=truth,
        query_ordinals=query_ordinals,
        page_rows=_PAGE_ROWS,
        neighbors=_NEIGHBORS,
        control_subspaces=_CONTROL_SUBSPACES,
        control_clusters=_CLUSTERS,
        wave1_rank_pages=_WAVE1_RANK_PAGES,
        wave1_max_span_pages=_WAVE1_MAX_SPAN_PAGES,
        wave1_max_ranges=_WAVE1_MAX_RANGES,
        wave2_top_rows=_WAVE2_TOP_ROWS,
        wave2_max_span_pages=_WAVE2_MAX_SPAN_PAGES,
        wave2_max_ranges=_WAVE2_MAX_RANGES,
    )
    for arm in result["arms"].values():
        arm["gate"] = summarize_development_arm(
            arm["result"], neighbors=_NEIGHBORS
        )
        arm["wave1_truth_oracle"] = _truth_oracle(
            arm["result"],
            page_count=(base.shape[0] + _PAGE_ROWS - 1) // _PAGE_ROWS,
        )
    validate_control_reproduction(
        result["arms"]["control"]["artifact_sha256"],
        result["arms"]["control"]["gate"],
    )
    result["decision"] = choose_summary_candidate(
        result["arms"]["control"]["result"],
        result["arms"]["challenger"]["result"],
        neighbors=_NEIGHBORS,
    )
    if not result["candidate_fence_oracle"]["can_pass_registered_gate"]:
        result["decision"] = {
            "accepted": None,
            "reason": "candidate-fence-inconclusive",
        }
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
