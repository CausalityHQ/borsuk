#!/usr/bin/env python3
"""Burned-development falsifier for a self-calibrated page-rank objective."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
from typing import Any

import numpy as np

from scripts.v85_shared_overlay_screen import (
    _adc_scores,
    _fixed_list,
    _page_payload_bytes,
    _scalar,
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
_PQ_SUBSPACES = 192
_CODE_PAGE_HEADER_BYTES = 64
_WAVE1_RANK_PAGES = 1_024
_WAVE1_MAX_SPAN_PAGES = 340
_WAVE1_MAX_RANGES = 32
_WAVE2_TOP_ROWS = 512
_WAVE2_MAX_SPAN_PAGES = 81
_WAVE2_MAX_RANGES = 32
_CALIBRATION_ROWS = 128
_CALIBRATION_SEED = 89


def rank_bins(ranks: np.ndarray) -> np.ndarray:
    """Map ranks to fixed fine head bins and one overflow bucket."""

    ranks = np.asarray(ranks)
    if ranks.ndim != 1 or ranks.dtype.kind not in "iu" or np.any(ranks < 0):
        raise ValueError("rank calibration differs")
    result = np.empty(ranks.size, dtype=np.int64)
    head = ranks < 32
    middle = (ranks >= 32) & (ranks < 256)
    tail = (ranks >= 256) & (ranks < 1_024)
    result[head] = ranks[head]
    result[middle] = 32 + (ranks[middle] - 32) // 8
    result[tail] = 60 + (ranks[tail] - 256) // 32
    result[ranks >= 1_024] = 84
    return result


def select_calibration_positions(
    base_ids: np.ndarray, *, count: int, seed: int
) -> np.ndarray:
    """Select a stable corpus-only calibration set by hashed row identity."""

    base_ids = np.asarray(base_ids)
    if (
        base_ids.ndim != 1
        or base_ids.dtype.kind not in "iu"
        or base_ids.size == 0
        or np.any(base_ids < 0)
        or np.unique(base_ids).size != base_ids.size
        or not 0 < count <= base_ids.size
        or not 0 <= seed < 2**64
    ):
        raise ValueError("calibration row authority differs")
    values = base_ids.astype(np.uint64, copy=False) ^ np.uint64(seed)
    values = values + np.uint64(0x9E3779B97F4A7C15)
    values = (values ^ (values >> np.uint64(30))) * np.uint64(
        0xBF58476D1CE4E5B9
    )
    values = (values ^ (values >> np.uint64(27))) * np.uint64(
        0x94D049BB133111EB
    )
    hashes = values ^ (values >> np.uint64(31))
    order = np.lexsort((base_ids, hashes))
    return np.ascontiguousarray(order[:count], dtype=np.int64)


def fit_monotone_rank_curve(
    hits: np.ndarray, seen: np.ndarray, *, scale: int
) -> np.ndarray:
    """Fit an integer non-increasing rate curve with weighted PAVA."""

    hits = np.asarray(hits)
    seen = np.asarray(seen)
    if (
        hits.ndim != 1
        or seen.ndim != 1
        or hits.shape != seen.shape
        or hits.size == 0
        or hits.dtype.kind not in "iu"
        or seen.dtype.kind not in "iu"
        or np.any(hits < 0)
        or np.any(seen <= 0)
        or scale <= 0
    ):
        raise ValueError("rank calibration differs")
    blocks: list[list[int]] = []
    for index in range(hits.size):
        blocks.append([index, index + 1, int(hits[index]), int(seen[index])])
        while (
            len(blocks) >= 2
            and blocks[-2][2] * blocks[-1][3]
            < blocks[-1][2] * blocks[-2][3]
        ):
            right = blocks.pop()
            left = blocks.pop()
            blocks.append(
                [left[0], right[1], left[2] + right[2], left[3] + right[3]]
            )
    curve = np.empty(hits.size, dtype=np.uint64)
    for start, stop, block_hits, block_seen in blocks:
        weight = (block_hits * scale + block_seen // 2) // block_seen
        curve[start:stop] = np.uint64(weight)
    return curve


def _ids_digest(ids: np.ndarray) -> str:
    values = np.ascontiguousarray(ids, dtype=np.int64)
    return hashlib.sha256(values.tobytes()).hexdigest()


def _vectors_digest(vectors: np.ndarray) -> str:
    values = np.ascontiguousarray(vectors, dtype=np.float32)
    return hashlib.sha256(memoryview(values).cast("B")).hexdigest()


@dataclasses.dataclass(frozen=True)
class RankCalibration:
    artifact_sha256: str
    base_ids_sha256: str
    base_vectors_sha256: str
    bin_hits: tuple[int, ...]
    bin_seen: tuple[int, ...]
    bin_weights: tuple[int, ...]
    calibration_ids: tuple[int, ...]
    calibration_ids_sha256: str
    calibration_rows: int
    calibration_vectors_sha256: str
    neighbors: int
    rank_limit: int
    routing_digest: str
    seed: int

    def rank_weights(self) -> np.ndarray:
        bins = rank_bins(np.arange(self.rank_limit, dtype=np.int64))
        weights = np.asarray(self.bin_weights, dtype=np.uint64)
        if bins.size == 0 or int(np.max(bins)) >= weights.size:
            raise ValueError("rank calibration differs")
        return np.ascontiguousarray(weights[bins])

    def as_dict(self) -> dict[str, Any]:
        return {
            "artifact_sha256": self.artifact_sha256,
            "base_ids_sha256": self.base_ids_sha256,
            "base_vectors_sha256": self.base_vectors_sha256,
            "bin_hits": list(self.bin_hits),
            "bin_seen": list(self.bin_seen),
            "bin_weights": list(self.bin_weights),
            "calibration_ids": list(self.calibration_ids),
            "calibration_ids_sha256": self.calibration_ids_sha256,
            "calibration_rows": self.calibration_rows,
            "calibration_vectors_sha256": self.calibration_vectors_sha256,
            "neighbors": self.neighbors,
            "rank_limit": self.rank_limit,
            "routing_digest": self.routing_digest,
            "seed": self.seed,
        }

    def digest(self) -> str:
        body = json.dumps(self.as_dict(), separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(body.encode()).hexdigest()


def _exact_neighbor_positions(
    scores: np.ndarray, ids: np.ndarray, *, neighbors: int
) -> np.ndarray:
    scores = np.asarray(scores, dtype=np.float32)
    ids = np.asarray(ids, dtype=np.int64)
    if (
        scores.ndim != 1
        or ids.shape != scores.shape
        or np.unique(ids).size != ids.size
        or not 0 < neighbors < scores.size
        or np.count_nonzero(np.isfinite(scores)) < neighbors
    ):
        raise ValueError("rank calibration truth differs")
    threshold = np.partition(scores, neighbors - 1)[neighbors - 1]
    below = np.flatnonzero(scores < threshold)
    equal = np.flatnonzero(scores == threshold)
    equal = equal[np.argsort(ids[equal], kind="stable")][
        : neighbors - below.size
    ]
    candidates = np.concatenate((below, equal))
    order = np.lexsort((ids[candidates], scores[candidates]))
    return np.ascontiguousarray(candidates[order], dtype=np.int64)


def _squared_l2_scores(
    vectors: np.ndarray, query: np.ndarray, *, chunk_rows: int
) -> np.ndarray:
    """Compute float32 squared L2 exactly as the serving reranker, in chunks."""

    vectors = np.asarray(vectors, dtype=np.float32)
    query = np.asarray(query, dtype=np.float32)
    if (
        vectors.ndim != 2
        or query.shape != (vectors.shape[1],)
        or vectors.shape[0] == 0
        or chunk_rows <= 0
        or not np.isfinite(vectors).all()
        or not np.isfinite(query).all()
    ):
        raise ValueError("rank calibration truth differs")
    return _squared_l2_scores_prevalidated(
        vectors, query, chunk_rows=chunk_rows
    )


def _squared_l2_scores_prevalidated(
    vectors: np.ndarray, query: np.ndarray, *, chunk_rows: int
) -> np.ndarray:
    """Compute the same scores after the caller validates the shared inputs."""

    scores = np.empty(vectors.shape[0], dtype=np.float32)
    for start in range(0, vectors.shape[0], chunk_rows):
        stop = min(start + chunk_rows, vectors.shape[0])
        delta = vectors[start:stop] - query[None, :]
        scores[start:stop] = np.einsum("ij,ij->i", delta, delta)
    return scores


def fit_page_rank_calibration(
    base: np.ndarray,
    base_ids: np.ndarray,
    calibration_vectors: np.ndarray,
    calibration_ids: np.ndarray,
    *,
    artifact: CoarseToFineArtifact,
    count: int,
    neighbors: int,
    rank_limit: int,
    seed: int,
) -> RankCalibration:
    """Fit a corpus-only monotone expected-hit curve over summary-page ranks."""

    base = np.ascontiguousarray(base, dtype=np.float32)
    base_ids = np.asarray(base_ids, dtype=np.int64)
    calibration_vectors = np.ascontiguousarray(
        calibration_vectors, dtype=np.float32
    )
    calibration_ids = np.asarray(calibration_ids, dtype=np.int64)
    if (
        base.ndim != 2
        or base.shape[0] <= neighbors
        or base.shape[1] == 0
        or base_ids.shape != (base.shape[0],)
        or np.unique(base_ids).size != base_ids.size
        or not np.isfinite(base).all()
        or calibration_vectors.ndim != 2
        or calibration_vectors.shape[1] != base.shape[1]
        or calibration_ids.shape != (calibration_vectors.shape[0],)
        or np.unique(calibration_ids).size != calibration_ids.size
        or not np.isfinite(calibration_vectors).all()
        or np.intersect1d(base_ids, calibration_ids).size != 0
        or not 0 < count <= calibration_vectors.shape[0]
        or neighbors <= 0
        or rank_limit <= 0
    ):
        raise ValueError("rank calibration authority differs")
    clusters = artifact.books[0].shape[0] if artifact.books else 0
    _validate_coarse_to_fine_artifact(
        artifact,
        rows=base.shape[0],
        dimensions=base.shape[1],
        page_rows=artifact.page_rows,
        subspaces=len(artifact.books),
        clusters=clusters,
    )
    page_count = (base.shape[0] + artifact.page_rows - 1) // artifact.page_rows
    if rank_limit >= page_count:
        raise ValueError("rank calibration authority differs")
    positions = select_calibration_positions(calibration_ids, count=count, seed=seed)
    page_bins = rank_bins(np.arange(page_count, dtype=np.int64))
    bin_count = int(np.max(page_bins)) + 1
    bin_seen_per_query = np.bincount(page_bins, minlength=bin_count).astype(
        np.int64
    )
    bin_hits = np.zeros(bin_count, dtype=np.int64)
    summaries_per_page = artifact.summary_codes.shape[0] // page_count
    for position in positions:
        query = calibration_vectors[int(position)]
        distances = _squared_l2_scores_prevalidated(
            base, query, chunk_rows=65_536
        )
        truth_positions = _exact_neighbor_positions(
            distances, base_ids, neighbors=neighbors
        )
        summary_scores = _adc_scores(query, artifact.summary_codes, list(artifact.books))
        page_scores = summary_scores.reshape(page_count, summaries_per_page).min(axis=1)
        pages = np.arange(page_count, dtype=np.int64)
        order = pages[np.lexsort((pages, page_scores))]
        page_ranks = np.empty(page_count, dtype=np.int64)
        page_ranks[order] = np.arange(page_count, dtype=np.int64)
        truth_bins = rank_bins(page_ranks[truth_positions // artifact.page_rows])
        bin_hits += np.bincount(truth_bins, minlength=bin_count)
    bin_seen = bin_seen_per_query * count
    bin_weights = fit_monotone_rank_curve(
        bin_hits, bin_seen, scale=1_000_000_000
    )
    if not np.any(bin_weights[rank_bins(np.arange(rank_limit))] > 0):
        raise ValueError("rank calibration differs")
    return RankCalibration(
        artifact_sha256=artifact.digest(),
        base_ids_sha256=_ids_digest(base_ids),
        base_vectors_sha256=_vectors_digest(base),
        bin_hits=tuple(int(value) for value in bin_hits),
        bin_seen=tuple(int(value) for value in bin_seen),
        bin_weights=tuple(int(value) for value in bin_weights),
        calibration_ids=tuple(
            int(calibration_ids[position]) for position in positions
        ),
        calibration_ids_sha256=_ids_digest(calibration_ids),
        calibration_rows=count,
        calibration_vectors_sha256=_vectors_digest(calibration_vectors),
        neighbors=neighbors,
        rank_limit=rank_limit,
        routing_digest=artifact.routing_digest(),
        seed=seed,
    )


def evaluate_calibrated_pair(
    base: np.ndarray,
    delta: np.ndarray,
    queries: np.ndarray,
    *,
    artifact: CoarseToFineArtifact,
    calibration: RankCalibration,
    base_ids: np.ndarray,
    delta_ids: np.ndarray,
    truth_ids: np.ndarray,
    query_ordinals: np.ndarray,
    page_rows: int,
    neighbors: int,
    subspaces: int,
    clusters: int,
    wave1_rank_pages: int,
    wave1_max_span_pages: int,
    wave1_max_ranges: int,
    wave2_top_rows: int,
    wave2_max_span_pages: int,
    wave2_max_ranges: int,
    code_page_bytes: int | None = None,
    data_page_bytes: int | None = None,
) -> dict[str, Any]:
    """Evaluate reciprocal-rank and calibrated planning over one artifact."""

    base_ids = np.asarray(base_ids, dtype=np.int64)
    if (
        calibration.base_ids_sha256 != _ids_digest(base_ids)
        or calibration.base_vectors_sha256 != _vectors_digest(base)
        or calibration.calibration_ids_sha256 != _ids_digest(delta_ids)
        or calibration.calibration_vectors_sha256 != _vectors_digest(delta)
        or calibration.artifact_sha256 != artifact.digest()
        or calibration.routing_digest != artifact.routing_digest()
        or calibration.rank_limit != wave1_rank_pages
        or calibration.neighbors != neighbors
    ):
        raise ValueError("rank calibration binding differs")
    if code_page_bytes is None:
        code_page_bytes = page_rows * subspaces + 64
    if data_page_bytes is None:
        data_page_bytes = _page_payload_bytes(
            dimensions=np.asarray(base).shape[1], page_rows=page_rows
        )
    common = {
        "artifact": artifact,
        "base_ids": base_ids,
        "delta_ids": delta_ids,
        "truth_ids": truth_ids,
        "query_ordinals": query_ordinals,
        "page_rows": page_rows,
        "neighbors": neighbors,
        "subspaces": subspaces,
        "clusters": clusters,
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
    for name, objective in (
        ("control", "reciprocal-rank"),
        ("challenger", "calibrated"),
    ):
        result = evaluate_coarse_to_fine(
            base,
            delta,
            queries,
            wave1_objective=objective,
            wave1_rank_weights=(
                calibration.rank_weights() if objective == "calibrated" else None
            ),
            **common,
        )
        arms[name] = {
            "artifact_sha256": artifact.digest(),
            "budgets": validate_result_budgets(
                result,
                wave1_page_bytes=code_page_bytes,
                wave1_max_pages=wave1_max_span_pages,
                wave1_max_ranges=wave1_max_ranges,
                wave2_page_bytes=data_page_bytes,
                wave2_max_pages=wave2_max_span_pages,
                wave2_max_ranges=wave2_max_ranges,
            ),
            "objective": objective,
            "rank_cut_base_truth_hits": _rank_cut_attribution(result),
            "result": result,
            "wave1_attribution": summarize_wave1_attribution(result),
        }
    return {
        "actual_s3_requests": 0,
        "arms": arms,
        "calibration": {**calibration.as_dict(), "sha256": calibration.digest()},
        "changed_parameter": "wave1_rank_utility",
        "claim_eligible": False,
        "delta_representation": "production-sq8",
        "schema": "borsuk-v89-calibrated-planner-screen-v1",
    }


def build_run_metadata() -> dict[str, Any]:
    code_page_bytes = _PAGE_ROWS * _PQ_SUBSPACES + _CODE_PAGE_HEADER_BYTES
    data_page_bytes = _page_payload_bytes(
        dimensions=_DIMENSIONS, page_rows=_PAGE_ROWS
    )
    projected_pages = 390_625
    dense_traceback_bytes = (
        2
        * projected_pages
        * (_WAVE1_MAX_RANGES + 1)
        * (_WAVE1_MAX_SPAN_PAGES + 1)
    )
    return {
        "configuration": {
            "base_rows": _BASE_ROWS,
            "calibration_rows": _CALIBRATION_ROWS,
            "calibration_seed": _CALIBRATION_SEED,
            "calibration_source": "resident-delta",
            "dimensions": _DIMENSIONS,
            "page_rows": _PAGE_ROWS,
            "queries": _DEVELOPMENT_QUERIES,
            "rows": _ROWS,
            "uses_query_or_truth_labels": False,
            "wave1_max_bytes": _WAVE1_MAX_SPAN_PAGES * code_page_bytes,
            "wave1_max_pages": _WAVE1_MAX_SPAN_PAGES,
            "wave1_max_ranges": _WAVE1_MAX_RANGES,
            "wave1_rank_pages": _WAVE1_RANK_PAGES,
            "wave2_max_bytes": _WAVE2_MAX_SPAN_PAGES * data_page_bytes,
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
            "calibration_cpu_qualified": False,
            "dense_wave1_traceback_bytes_per_query": dense_traceback_bytes,
            "serving_memory_qualified": False,
            "summary_adc_lookups_per_query": projected_pages * 2 * _PQ_SUBSPACES,
            "summary_resident_bytes": projected_pages * 2 * _PQ_SUBSPACES,
        },
        "scope": "fixed-1m-burned-development-calibrated-planner-falsifier",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the fixed V89 self-calibrated planner falsifier."
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
        raise ValueError("V89 vector dimensions differ")
    queries, truth, query_ordinals = select_development_rows(
        loaded_queries,
        loaded_truth,
        development_queries=_DEVELOPMENT_QUERIES,
    )
    base = np.ascontiguousarray(source[base_order])
    base_ids = np.ascontiguousarray(source_ids[base_order])
    artifact = build_coarse_to_fine_artifact(
        base,
        page_rows=_PAGE_ROWS,
        subspaces=_PQ_SUBSPACES,
        clusters=256,
        sample_rows=100_000,
        seed=85,
        iterations=10,
    )
    calibration = fit_page_rank_calibration(
        base,
        base_ids,
        np.ascontiguousarray(source[_BASE_ROWS:]),
        np.ascontiguousarray(source_ids[_BASE_ROWS:]),
        artifact=artifact,
        count=_CALIBRATION_ROWS,
        neighbors=_NEIGHBORS,
        rank_limit=_WAVE1_RANK_PAGES,
        seed=_CALIBRATION_SEED,
    )
    result = evaluate_calibrated_pair(
        base,
        np.ascontiguousarray(source[_BASE_ROWS:]),
        queries,
        artifact=artifact,
        calibration=calibration,
        base_ids=base_ids,
        delta_ids=source_ids[_BASE_ROWS:],
        truth_ids=truth,
        query_ordinals=query_ordinals,
        page_rows=_PAGE_ROWS,
        neighbors=_NEIGHBORS,
        subspaces=_PQ_SUBSPACES,
        clusters=256,
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
            arm["result"], page_count=(base.shape[0] + _PAGE_ROWS - 1) // _PAGE_ROWS
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
    result.update(build_run_metadata())
    body = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    args.output.write_text(body)
    print(body, end="")


if __name__ == "__main__":
    main()
