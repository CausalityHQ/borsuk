#!/usr/bin/env python3
"""Score-aware noise shaping for the V106 PQ48 fail-fast spike."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal

import numpy as np

from scripts.v97_row_width_screen import (
    PqSpec,
    ScreenAuthority,
    ScreenInputs,
    encode_pq,
    fit_pq,
)
from scripts.v98_hierarchical_row_router import (
    AggregateEvidence,
    ArrayIdentity,
    PairedIntervalEvidence,
    _array_identity,
    _producer_bootstrap_matrix,
    _producer_paired_interval,
    _retained_row_positions,
    build_hierarchy,
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
    V102Projection,
    build_refinement_page_directory,
    plan_refinement_fetch,
    project_v102_resident_bytes_100m,
)


@dataclass(frozen=True, slots=True)
class V106Result:
    """Matched ordinary- versus anisotropic-PQ48 screen evidence."""

    schema: str
    authority: ScreenAuthority
    config: RankedGapConfig
    query_count: int
    threshold: float
    coordinate_passes: int
    refinement_row_bytes: int
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    classification: Literal[
        "refinement-io-rejected",
        "anisotropic-pq48-rejected",
        "anisotropic-pq48-qualified",
    ]
    books_identity: ArrayIdentity
    control_codes_identity: ArrayIdentity
    challenger_codes_identity: ArrayIdentity
    refinement_fetches: tuple[RefinementFetchEvidence, ...]
    refinement_maximum_gets: int
    refinement_maximum_bytes: int
    refinement_resource_gate_passed: bool
    control_aggregate: AggregateEvidence
    control_samples: tuple[RangeSample, ...]
    challenger_aggregate: AggregateEvidence
    challenger_samples: tuple[RangeSample, ...]
    paired: PairedIntervalEvidence
    projection: V102Projection


def anisotropy_eta(dimensions: int, norm: float, threshold: float) -> float:
    """Return the registered large-dimension AVQ parallel-error weight."""

    if (
        type(dimensions) is not int
        or dimensions <= 1
        or not np.isfinite(norm)
        or not np.isfinite(threshold)
        or norm <= threshold
        or threshold <= 0.0
    ):
        raise ValueError("anisotropic threshold differs")
    ratio = threshold / norm
    return (dimensions - 1) * ratio**2 / (1.0 - ratio**2) ** 2


def _validate(
    vectors: np.ndarray,
    books: np.ndarray,
    spec: PqSpec,
) -> tuple[np.ndarray, np.ndarray]:
    values = np.asarray(vectors)
    centroids = np.asarray(books)
    if (
        not isinstance(spec, PqSpec)
        or spec.centroid_bits != 8
        or spec.row_bytes != spec.subspaces
        or values.ndim != 2
        or values.dtype != np.float32
        or centroids.ndim != 3
        or centroids.dtype != np.float32
        or centroids.shape[0] != spec.subspaces
        or centroids.shape[1] != 256
        or centroids.shape[2] * spec.subspaces != values.shape[1]
        or not np.isfinite(values).all()
        or not np.isfinite(centroids).all()
    ):
        raise ValueError("anisotropic PQ authority differs")
    return values, centroids


def encode_anisotropic_pq(
    vectors: np.ndarray,
    books: np.ndarray,
    spec: PqSpec,
    *,
    threshold: float,
    passes: int,
    block_rows: int = 8_192,
) -> np.ndarray:
    """Noise-shape ordinary PQ codes with one fixed AVQ coordinate pass."""

    values, centroids = _validate(vectors, books, spec)
    if threshold != 0.2 or passes != 1 or type(block_rows) is not int or block_rows <= 0:
        raise ValueError("anisotropic encoding authority differs")
    source_norms = np.einsum("ij,ij->i", values, values, dtype=np.float32)
    if np.any(source_norms <= np.float32(threshold * threshold)):
        raise ValueError("anisotropic threshold differs")
    norms = np.sqrt(source_norms, dtype=np.float32)
    ratios = np.float32(threshold) / norms
    eta = np.float32(values.shape[1] - 1) * ratios * ratios / (
        np.float32(1.0) - ratios * ratios
    ) ** np.float32(2.0)
    if not np.isfinite(eta).all():
        raise ValueError("anisotropic threshold differs")

    codes = np.ascontiguousarray(encode_pq(values, centroids, spec))
    width = centroids.shape[2]
    total_error_norm = np.zeros(values.shape[0], dtype=np.float32)
    total_error_dot = np.zeros(values.shape[0], dtype=np.float32)
    for subspace in range(spec.subspaces):
        lo = subspace * width
        hi = lo + width
        residual = values[:, lo:hi] - centroids[subspace, codes[:, subspace]]
        total_error_norm += np.einsum(
            "ij,ij->i", residual, residual, dtype=np.float32
        )
        total_error_dot += np.einsum(
            "ij,ij->i", residual, values[:, lo:hi], dtype=np.float32
        )

    for subspace in range(spec.subspaces):
        lo = subspace * width
        hi = lo + width
        book = centroids[subspace]
        book_norms = np.einsum("ij,ij->i", book, book, dtype=np.float32)
        for start in range(0, values.shape[0], block_rows):
            stop = min(start + block_rows, values.shape[0])
            block = values[start:stop, lo:hi]
            current = book[codes[start:stop, subspace]]
            current_residual = block - current
            current_norm = np.einsum(
                "ij,ij->i", current_residual, current_residual, dtype=np.float32
            )
            current_dot = np.einsum(
                "ij,ij->i", current_residual, block, dtype=np.float32
            )
            other_norm = total_error_norm[start:stop] - current_norm
            other_dot = total_error_dot[start:stop] - current_dot
            block_norm = np.einsum("ij,ij->i", block, block, dtype=np.float32)
            products = block @ book.T
            candidate_norm = (
                block_norm[:, None]
                - np.float32(2.0) * products
                + book_norms[None, :]
            )
            candidate_dot = block_norm[:, None] - products
            full_dot = other_dot[:, None] + candidate_dot
            losses = (
                other_norm[:, None]
                + candidate_norm
                + (eta[start:stop, None] - np.float32(1.0))
                * full_dot
                * full_dot
                / source_norms[start:stop, None]
            )
            if not np.isfinite(losses).all():
                raise ValueError("anisotropic loss differs")
            chosen = np.argmin(losses, axis=1).astype(np.uint8, copy=False)
            row = np.arange(stop - start)
            total_error_norm[start:stop] = other_norm + candidate_norm[row, chosen]
            total_error_dot[start:stop] = other_dot + candidate_dot[row, chosen]
            codes[start:stop, subspace] = chosen
    return codes


def rank_anisotropic_rows(
    query: np.ndarray,
    row_ids: np.ndarray,
    row_codes: np.ndarray,
    books: np.ndarray,
    spec: PqSpec,
    *,
    maximum_rows: int,
    shortlist_rows: int,
    block_rows: int = 8_192,
) -> tuple[int, ...]:
    """Rank AVQ reconstructions by inner product with stable ID ties."""

    vector = np.asarray(query)
    ids = np.asarray(row_ids)
    codes = np.asarray(row_codes)
    _, centroids = _validate(vector[None, :], books, spec)
    if (
        ids.ndim != 1
        or not np.issubdtype(ids.dtype, np.integer)
        or ids.size == 0
        or ids.size > maximum_rows
        or np.unique(ids).size != ids.size
        or codes.shape != (ids.size, spec.subspaces)
        or codes.dtype != np.uint8
        or type(shortlist_rows) is not int
        or not 0 < shortlist_rows <= ids.size
        or type(block_rows) is not int
        or block_rows <= 0
    ):
        raise ValueError("anisotropic row-score input differs")
    width = centroids.shape[2]
    best_scores = np.empty(0, dtype=np.float32)
    best_ids = np.empty(0, dtype=ids.dtype)
    for start in range(0, ids.size, block_rows):
        stop = min(start + block_rows, ids.size)
        scores = np.zeros(stop - start, dtype=np.float32)
        for subspace in range(spec.subspaces):
            lo = subspace * width
            hi = lo + width
            table = -(centroids[subspace] @ vector[lo:hi])
            scores += table[codes[start:stop, subspace]]
        candidate_scores = np.concatenate((best_scores, scores))
        candidate_ids = np.concatenate((best_ids, ids[start:stop]))
        order = np.lexsort((candidate_ids, candidate_scores))[:shortlist_rows]
        best_scores = np.ascontiguousarray(candidate_scores[order])
        best_ids = np.ascontiguousarray(candidate_ids[order])
    return tuple(int(row_id) for row_id in best_ids)


def _empty_aggregate() -> AggregateEvidence:
    return AggregateEvidence(0, 0, 0, 0, 0, False, False)


def _classification(
    aggregate: AggregateEvidence,
    resource: bool,
    projection: V102Projection,
    paired: PairedIntervalEvidence,
) -> Literal["anisotropic-pq48-rejected", "anisotropic-pq48-qualified"]:
    intervals = (
        paired.average_recall10_ppm,
        paired.average_recall100_ppm,
        paired.p05_recall100_ppm,
    )
    loses = any(interval[1] < 0 for interval in intervals)
    return (
        "anisotropic-pq48-qualified"
        if aggregate.quality_gate_passed
        and aggregate.resource_gate_passed
        and resource
        and projection.resident_eligible
        and not loses
        else "anisotropic-pq48-rejected"
    )


def evaluate_v106(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: RankedGapConfig,
) -> V106Result:
    """Compare one theory-fixed AVQ encoding with V102's ordinary PQ48."""

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
        raise ValueError("V106 input authority differs")

    hierarchy = build_hierarchy(inputs, config)
    fences = tuple(route_hierarchy(query, hierarchy, config) for query in queries)
    directory = build_refinement_page_directory(inputs, row_bytes=PQ48X8.row_bytes)
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
    control_codes = np.ascontiguousarray(encode_pq(vectors, books, PQ48X8))
    challenger_codes = np.ascontiguousarray(
        encode_anisotropic_pq(
            vectors,
            books,
            PQ48X8,
            threshold=0.2,
            passes=1,
        )
    )
    projection = project_v102_resident_bytes_100m(config)
    books_identity = _array_identity(books)
    control_identity = _array_identity(control_codes)
    challenger_identity = _array_identity(challenger_codes)
    empty_paired = PairedIntervalEvidence(
        "anisotropic-pq48", (0, 0), (0, 0), (0, 0)
    )
    if io_failed:
        return V106Result(
            schema="borsuk-v106-avq-pq48-v1",
            authority=authority,
            config=config,
            query_count=queries.shape[0],
            threshold=0.2,
            coordinate_passes=1,
            refinement_row_bytes=PQ48X8.row_bytes,
            bootstrap_seed=authority.seed,
            bootstrap_resamples=10_000,
            bootstrap_matrix_sha256="0" * 64,
            classification="refinement-io-rejected",
            books_identity=books_identity,
            control_codes_identity=control_identity,
            challenger_codes_identity=challenger_identity,
            refinement_fetches=tuple(fetches),
            refinement_maximum_gets=max((item.gets for item in fetches), default=0),
            refinement_maximum_bytes=max((item.bytes for item in fetches), default=0),
            refinement_resource_gate_passed=False,
            control_aggregate=_empty_aggregate(),
            control_samples=(),
            challenger_aggregate=_empty_aggregate(),
            challenger_samples=(),
            paired=empty_paired,
            projection=projection,
        )

    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    control_samples: list[RangeSample] = []
    challenger_samples: list[RangeSample] = []
    for ordinal, query in enumerate(queries):
        fence = fences[ordinal]
        row_ids, positions = _retained_row_positions(fence, inputs, position_by_id)
        shortlist = min(config.shortlist_rows, row_ids.size)
        control_rows = score_retained_rows(
            query,
            row_ids,
            np.ascontiguousarray(control_codes[positions]),
            books,
            PQ48X8,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=shortlist,
        )
        challenger_rows = rank_anisotropic_rows(
            query,
            row_ids,
            np.ascontiguousarray(challenger_codes[positions]),
            books,
            PQ48X8,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=shortlist,
        )
        control_samples.append(
            _range_sample(
                ordinal,
                truth_ids[ordinal],
                _selection_from_rows(control_rows, inputs, config),
                fence,
                inputs,
            )
        )
        challenger_samples.append(
            _range_sample(
                ordinal,
                truth_ids[ordinal],
                _selection_from_rows(challenger_rows, inputs, config),
                fence,
                inputs,
            )
        )
    control_tuple = tuple(control_samples)
    challenger_tuple = tuple(challenger_samples)
    control_aggregate = _aggregate_range_samples(control_tuple, config)
    challenger_aggregate = _aggregate_range_samples(challenger_tuple, config)
    matrix = _producer_bootstrap_matrix(
        queries.shape[0], seed=authority.seed, resamples=10_000
    )
    paired = PairedIntervalEvidence(
        name="anisotropic-pq48",
        average_recall10_ppm=_producer_paired_interval(
            tuple(item.recall10_ppm for item in challenger_tuple),
            tuple(item.recall10_ppm for item in control_tuple),
            matrix,
            statistic="mean",
        ),
        average_recall100_ppm=_producer_paired_interval(
            tuple(item.recall100_ppm for item in challenger_tuple),
            tuple(item.recall100_ppm for item in control_tuple),
            matrix,
            statistic="mean",
        ),
        p05_recall100_ppm=_producer_paired_interval(
            tuple(item.recall100_ppm for item in challenger_tuple),
            tuple(item.recall100_ppm for item in control_tuple),
            matrix,
            statistic="p05",
        ),
    )
    maximum_gets = max(item.gets for item in fetches)
    maximum_bytes = max(item.bytes for item in fetches)
    resource = (
        maximum_gets <= config.maximum_gets
        and maximum_bytes <= config.maximum_bytes
    )
    return V106Result(
        schema="borsuk-v106-avq-pq48-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        threshold=0.2,
        coordinate_passes=1,
        refinement_row_bytes=PQ48X8.row_bytes,
        bootstrap_seed=authority.seed,
        bootstrap_resamples=10_000,
        bootstrap_matrix_sha256=_array_identity(matrix).sha256,
        classification=_classification(
            challenger_aggregate, resource, projection, paired
        ),
        books_identity=books_identity,
        control_codes_identity=control_identity,
        challenger_codes_identity=challenger_identity,
        refinement_fetches=tuple(fetches),
        refinement_maximum_gets=maximum_gets,
        refinement_maximum_bytes=maximum_bytes,
        refinement_resource_gate_passed=resource,
        control_aggregate=control_aggregate,
        control_samples=control_tuple,
        challenger_aggregate=challenger_aggregate,
        challenger_samples=challenger_tuple,
        paired=paired,
        projection=projection,
    )


def canonical_v106_result_bytes(result: V106Result) -> bytes:
    """Validate and serialize one canonical V106 result."""

    if (
        not isinstance(result, V106Result)
        or result.schema != "borsuk-v106-avq-pq48-v1"
        or result.threshold != 0.2
        or result.coordinate_passes != 1
        or result.refinement_row_bytes != 48
        or result.bootstrap_seed != result.authority.seed
        or result.bootstrap_resamples != 10_000
        or result.query_count <= 0
        or len(result.refinement_fetches) != result.query_count
        or len(result.control_samples) != result.query_count
        or len(result.challenger_samples) != result.query_count
        or _aggregate_range_samples(result.control_samples, result.config)
        != result.control_aggregate
        or _aggregate_range_samples(result.challenger_samples, result.config)
        != result.challenger_aggregate
        or result.classification
        != _classification(
            result.challenger_aggregate,
            result.refinement_resource_gate_passed,
            result.projection,
            result.paired,
        )
    ):
        raise ValueError("V106 result evidence differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
