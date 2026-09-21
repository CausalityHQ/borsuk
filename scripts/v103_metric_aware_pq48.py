#!/usr/bin/env python3
"""Metric-aware scoring primitives for the bounded V103 PQ48 spike."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal

import numpy as np

from scripts.v97_row_width_screen import (
    SUMMARY_ONLY_PQ16X8,
    ScreenAuthority,
    ScreenInputs,
    encode_pq,
    fit_pq,
)
from scripts.v98_hierarchical_row_router import (
    AggregateEvidence,
    ArrayIdentity,
    PairedIntervalEvidence,
    V98Projection,
    _array_identity,
    _producer_bootstrap_matrix,
    _producer_paired_interval,
    _retained_row_positions,
    build_hierarchy,
    project_v98_resident_bytes_100m,
    route_hierarchy,
    score_retained_rows,
)
from scripts.v99_ranked_gap_range_router import (
    RangeSample,
    RankedGapConfig,
    _aggregate_range_samples,
    _range_sample,
    _selection_from_rows,
)
from scripts.v102_two_wave_pq48_refinement import (
    PQ48X8,
    RefinementFetchEvidence,
    build_refinement_page_directory,
    plan_refinement_fetch,
)


@dataclass(frozen=True, slots=True)
class VectorNormEvidence:
    """Complete finite source-vector squared-norm bounds."""

    rows: int
    minimum_squared_norm: float
    maximum_squared_norm: float


@dataclass(frozen=True, slots=True)
class V103Projection:
    """Resident RAM and immutable S3 storage for PQ48 plus source norms."""

    summary_only_resident: V98Projection
    refinement_codebook_bytes: int
    refinement_row_bytes: int
    refinement_code_plane_bytes: int
    row_codes_resident_bytes: int
    total_resident_bytes: int
    budget_bytes: int
    resident_eligible: bool


@dataclass(frozen=True, slots=True)
class V103Result:
    """Same-code causal comparison of reconstructed- and source-norm ADC."""

    schema: str
    authority: ScreenAuthority
    config: RankedGapConfig
    query_count: int
    refinement_row_bytes: int
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    classification: Literal[
        "refinement-io-rejected",
        "metric-aware-pq48-rejected",
        "metric-aware-pq48-diagnostic-qualified",
    ]
    refinement_books_identity: ArrayIdentity
    refinement_codes_identity: ArrayIdentity
    source_norms_identity: ArrayIdentity
    source_norms: VectorNormEvidence
    query_norms: VectorNormEvidence
    refinement_fetches: tuple[RefinementFetchEvidence, ...]
    refinement_maximum_gets: int
    refinement_maximum_bytes: int
    refinement_resource_gate_passed: bool
    l2_control_aggregate: AggregateEvidence
    l2_control_samples: tuple[RangeSample, ...]
    metric_aware_aggregate: AggregateEvidence
    metric_aware_samples: tuple[RangeSample, ...]
    paired: PairedIntervalEvidence
    projection: V103Projection


def vector_norm_evidence(vectors: np.ndarray) -> VectorNormEvidence:
    """Reduce every source row to authenticated squared-norm bounds."""

    values = np.asarray(vectors)
    if (
        values.ndim != 2
        or values.dtype != np.float32
        or values.shape[0] == 0
        or values.shape[1] == 0
        or not np.isfinite(values).all()
    ):
        raise ValueError("V103 vector norm input differs")
    norms = np.einsum("ij,ij->i", values, values, dtype=np.float32)
    if not np.isfinite(norms).all() or np.any(norms <= 0):
        raise ValueError("V103 vector norm input differs")
    return VectorNormEvidence(
        rows=values.shape[0],
        minimum_squared_norm=float(norms.min()),
        maximum_squared_norm=float(norms.max()),
    )


def metric_aware_adc_scores(
    query: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    source_squared_norms: np.ndarray,
    *,
    subspaces: int,
    centroid_bits: int,
) -> np.ndarray:
    """Approximate squared L2 using exact source norms and PQ dot products."""

    vector = np.asarray(query)
    centroids = np.asarray(books)
    decoded = np.asarray(codes)
    source_norms = np.asarray(source_squared_norms)
    centroid_count = 1 << centroid_bits
    if (
        vector.ndim != 1
        or vector.dtype != np.float32
        or not np.isfinite(vector).all()
        or type(subspaces) is not int
        or subspaces <= 0
        or centroid_bits != 8
        or centroids.ndim != 3
        or centroids.dtype != np.float32
        or centroids.shape[0] != subspaces
        or centroids.shape[1] != centroid_count
        or centroids.shape[2] * subspaces != vector.size
        or not np.isfinite(centroids).all()
        or decoded.ndim != 2
        or decoded.dtype != np.uint8
        or decoded.shape != (source_norms.size, subspaces)
        or source_norms.ndim != 1
        or source_norms.dtype != np.float32
        or not np.isfinite(source_norms).all()
        or np.any(source_norms <= 0)
    ):
        raise ValueError("V103 metric-aware score input differs")
    scores = np.array(source_norms, dtype=np.float32, copy=True)
    width = centroids.shape[2]
    for subspace in range(subspaces):
        lo = subspace * width
        hi = lo + width
        table = np.float32(-2.0) * (centroids[subspace] @ vector[lo:hi])
        scores += table[decoded[:, subspace]]
    if not np.isfinite(scores).all():
        raise ValueError("V103 metric-aware score input differs")
    return scores


def score_retained_rows_metric_aware(
    query: np.ndarray,
    row_ids: np.ndarray,
    row_codes: np.ndarray,
    source_squared_norms: np.ndarray,
    books: np.ndarray,
    *,
    subspaces: int,
    centroid_bits: int,
    maximum_rows: int,
    shortlist_rows: int,
    block_rows: int = 8_192,
) -> tuple[int, ...]:
    """Bounded metric-aware scoring under the global ``(score,id)`` order."""

    ids = np.asarray(row_ids)
    codes = np.asarray(row_codes)
    norms = np.asarray(source_squared_norms)
    if (
        ids.ndim != 1
        or not np.issubdtype(ids.dtype, np.integer)
        or ids.size == 0
        or ids.size > maximum_rows
        or np.unique(ids).size != ids.size
        or codes.ndim != 2
        or codes.shape[0] != ids.size
        or norms.shape != (ids.size,)
        or type(shortlist_rows) is not int
        or not 0 < shortlist_rows <= ids.size
        or type(block_rows) is not int
        or block_rows <= 0
    ):
        raise ValueError("V103 retained row-score input differs")
    best_scores = np.empty(0, dtype=np.float32)
    best_ids = np.empty(0, dtype=ids.dtype)
    for start in range(0, ids.size, block_rows):
        stop = min(start + block_rows, ids.size)
        block_scores = metric_aware_adc_scores(
            query,
            books,
            np.ascontiguousarray(codes[start:stop]),
            np.ascontiguousarray(norms[start:stop]),
            subspaces=subspaces,
            centroid_bits=centroid_bits,
        )
        candidate_scores = np.concatenate((best_scores, block_scores))
        candidate_ids = np.concatenate((best_ids, ids[start:stop]))
        order = np.lexsort((candidate_ids, candidate_scores))[:shortlist_rows]
        best_scores = np.ascontiguousarray(candidate_scores[order])
        best_ids = np.ascontiguousarray(candidate_ids[order])
    return tuple(int(row_id) for row_id in best_ids)


def project_v103_resident_bytes_100m(config: RankedGapConfig) -> V103Projection:
    """Charge four source-norm bytes per S3 row and no resident row plane."""

    baseline = project_v98_resident_bytes_100m(SUMMARY_ONLY_PQ16X8, config)
    row_bytes = PQ48X8.row_bytes + np.dtype(np.float32).itemsize
    codebook_bytes = 256 * 768 * np.dtype(np.float32).itemsize
    total = baseline.total_bytes + codebook_bytes
    return V103Projection(
        summary_only_resident=baseline,
        refinement_codebook_bytes=codebook_bytes,
        refinement_row_bytes=row_bytes,
        refinement_code_plane_bytes=100_000_000 * row_bytes,
        row_codes_resident_bytes=0,
        total_resident_bytes=total,
        budget_bytes=baseline.budget_bytes,
        resident_eligible=total < baseline.budget_bytes,
    )


def _empty_aggregate() -> AggregateEvidence:
    return AggregateEvidence(0, 0, 0, 0, 0, False, False)


def evaluate_v103(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: RankedGapConfig,
) -> V103Result:
    """Evaluate one same-code source-norm correction over the V99 fence."""

    queries = np.asarray(inputs.queries)
    truth_ids = np.asarray(inputs.truth_ids)
    vectors = np.asarray(inputs.vectors)
    source_ids = np.asarray(inputs.source_ids)
    if (
        queries.ndim != 2
        or queries.dtype != np.float32
        or not np.isfinite(queries).all()
        or truth_ids.shape != (queries.shape[0], inputs.neighbors)
        or vectors.ndim != 2
        or vectors.dtype != np.float32
        or vectors.shape[1] % PQ48X8.subspaces
        or source_ids.shape != (vectors.shape[0],)
        or authority.dimensions != vectors.shape[1]
        or authority.seed != inputs.seed
        or inputs.max_gets != config.maximum_gets
        or inputs.max_bytes != config.maximum_bytes
    ):
        raise ValueError("V103 input authority differs")
    source_norm_values = np.einsum(
        "ij,ij->i", vectors, vectors, dtype=np.float32
    ).astype(np.float32, copy=False)
    source_norms = vector_norm_evidence(vectors)
    query_norms = vector_norm_evidence(queries)
    hierarchy = build_hierarchy(inputs, config)
    fences = tuple(route_hierarchy(query, hierarchy, config) for query in queries)
    projection = project_v103_resident_bytes_100m(config)
    directory = build_refinement_page_directory(
        inputs, row_bytes=projection.refinement_row_bytes
    )
    fetches: list[RefinementFetchEvidence] = []
    io_failed = False
    for ordinal, fence in enumerate(fences):
        try:
            selection = plan_refinement_fetch(
                fence.retained_pages,
                directory,
                max_gets=config.maximum_gets,
                max_bytes=config.maximum_bytes,
            )
        except ValueError:
            io_failed = True
            break
        fetches.append(
            RefinementFetchEvidence(
                query_ordinal=ordinal,
                required_pages=fence.retained_pages,
                selected_ranges=selection.ranges,
                selected_pages=selection.pages,
                gets=selection.gets,
                bytes=selection.bytes,
            )
        )
    base_positions = np.asarray(
        [
            position
            for position, row_id in enumerate(source_ids)
            if inputs.page_by_id[int(row_id)].object_role == "base"
        ],
        dtype=np.int64,
    )
    training = np.ascontiguousarray(vectors[base_positions])
    books = fit_pq(
        training,
        PQ48X8,
        seed=inputs.seed,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    codes = np.ascontiguousarray(encode_pq(vectors, books, PQ48X8))
    books_identity = _array_identity(books)
    codes_identity = _array_identity(codes)
    norms_identity = _array_identity(source_norm_values)
    empty_paired = PairedIntervalEvidence(
        "metric-aware-pq48", (0, 0), (0, 0), (0, 0)
    )
    if io_failed:
        return V103Result(
            schema="borsuk-v103-metric-aware-pq48-v1",
            authority=authority,
            config=config,
            query_count=queries.shape[0],
            refinement_row_bytes=projection.refinement_row_bytes,
            bootstrap_seed=authority.seed,
            bootstrap_resamples=10_000,
            bootstrap_matrix_sha256="0" * 64,
            classification="refinement-io-rejected",
            refinement_books_identity=books_identity,
            refinement_codes_identity=codes_identity,
            source_norms_identity=norms_identity,
            source_norms=source_norms,
            query_norms=query_norms,
            refinement_fetches=tuple(fetches),
            refinement_maximum_gets=max((item.gets for item in fetches), default=0),
            refinement_maximum_bytes=max((item.bytes for item in fetches), default=0),
            refinement_resource_gate_passed=False,
            l2_control_aggregate=_empty_aggregate(),
            l2_control_samples=(),
            metric_aware_aggregate=_empty_aggregate(),
            metric_aware_samples=(),
            paired=empty_paired,
            projection=projection,
        )
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    l2_samples: list[RangeSample] = []
    metric_samples: list[RangeSample] = []
    for ordinal, query in enumerate(queries):
        fence = fences[ordinal]
        row_ids, positions = _retained_row_positions(
            fence, inputs, position_by_id
        )
        shortlist = min(config.shortlist_rows, row_ids.size)
        retained_codes = np.ascontiguousarray(codes[positions])
        l2_rows = score_retained_rows(
            query,
            row_ids,
            retained_codes,
            books,
            PQ48X8,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=shortlist,
        )
        metric_rows = score_retained_rows_metric_aware(
            query,
            row_ids,
            retained_codes,
            np.ascontiguousarray(source_norm_values[positions]),
            books,
            subspaces=PQ48X8.subspaces,
            centroid_bits=PQ48X8.centroid_bits,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=shortlist,
        )
        l2_samples.append(
            _range_sample(
                ordinal,
                truth_ids[ordinal],
                _selection_from_rows(l2_rows, inputs, config),
                fence,
                inputs,
            )
        )
        metric_samples.append(
            _range_sample(
                ordinal,
                truth_ids[ordinal],
                _selection_from_rows(metric_rows, inputs, config),
                fence,
                inputs,
            )
        )
    l2_tuple = tuple(l2_samples)
    metric_tuple = tuple(metric_samples)
    l2_aggregate = _aggregate_range_samples(l2_tuple, config)
    metric_aggregate = _aggregate_range_samples(metric_tuple, config)
    matrix = _producer_bootstrap_matrix(
        queries.shape[0], seed=authority.seed, resamples=10_000
    )
    l2_10 = tuple(item.recall10_ppm for item in l2_tuple)
    l2_100 = tuple(item.recall100_ppm for item in l2_tuple)
    metric_10 = tuple(item.recall10_ppm for item in metric_tuple)
    metric_100 = tuple(item.recall100_ppm for item in metric_tuple)
    paired = PairedIntervalEvidence(
        name="metric-aware-pq48",
        average_recall10_ppm=_producer_paired_interval(
            metric_10, l2_10, matrix, statistic="mean"
        ),
        average_recall100_ppm=_producer_paired_interval(
            metric_100, l2_100, matrix, statistic="mean"
        ),
        p05_recall100_ppm=_producer_paired_interval(
            metric_100, l2_100, matrix, statistic="p05"
        ),
    )
    maximum_gets = max(item.gets for item in fetches)
    maximum_bytes = max(item.bytes for item in fetches)
    resource = (
        maximum_gets <= config.maximum_gets
        and maximum_bytes <= config.maximum_bytes
    )
    qualified = (
        resource
        and metric_aggregate.quality_gate_passed
        and metric_aggregate.resource_gate_passed
        and projection.resident_eligible
        and paired.average_recall100_ppm[0] >= 0
    )
    return V103Result(
        schema="borsuk-v103-metric-aware-pq48-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        refinement_row_bytes=projection.refinement_row_bytes,
        bootstrap_seed=authority.seed,
        bootstrap_resamples=10_000,
        bootstrap_matrix_sha256=hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        classification=(
            "metric-aware-pq48-diagnostic-qualified"
            if qualified
            else "metric-aware-pq48-rejected"
        ),
        refinement_books_identity=books_identity,
        refinement_codes_identity=codes_identity,
        source_norms_identity=norms_identity,
        source_norms=source_norms,
        query_norms=query_norms,
        refinement_fetches=tuple(fetches),
        refinement_maximum_gets=maximum_gets,
        refinement_maximum_bytes=maximum_bytes,
        refinement_resource_gate_passed=resource,
        l2_control_aggregate=l2_aggregate,
        l2_control_samples=l2_tuple,
        metric_aware_aggregate=metric_aggregate,
        metric_aware_samples=metric_tuple,
        paired=paired,
        projection=projection,
    )


def canonical_v103_result_bytes(result: V103Result) -> bytes:
    """Serialize complete typed V103 evidence as canonical newline JSON."""

    if (
        not isinstance(result, V103Result)
        or result.schema != "borsuk-v103-metric-aware-pq48-v1"
        or result.query_count <= 0
        or result.bootstrap_resamples != 10_000
    ):
        raise ValueError("V103 result differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
