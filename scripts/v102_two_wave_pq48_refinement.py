#!/usr/bin/env python3
"""Bounded two-wave PQ48 refinement over the immutable V99 hierarchy."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal, Mapping, Sequence

import numpy as np

from scripts.v97_row_width_screen import (
    PQ16X8,
    SUMMARY_ONLY_PQ16X8,
    PageKey,
    PqSpec,
    RoutedPage,
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
    PageRange,
    RangeSample,
    RangeSelection,
    RankedGapConfig,
    _aggregate_range_samples,
    _range_sample,
    _selection_from_rows,
    select_ranked_gap_ranges,
)

PQ48X8 = PqSpec("pq48x8-s3-refinement", 48, 8, 48)


@dataclass(frozen=True, slots=True)
class V102Projection:
    """Resident RAM and immutable S3 storage for the two-wave design."""

    summary_only_resident: V98Projection
    refinement_codebook_bytes: int
    refinement_code_plane_bytes: int
    row_codes_resident_bytes: int
    total_resident_bytes: int
    budget_bytes: int
    resident_eligible: bool


@dataclass(frozen=True, slots=True)
class RefinementFetchEvidence:
    """The complete required-page set and its charged code-plane ranges."""

    query_ordinal: int
    required_pages: tuple[PageKey, ...]
    selected_ranges: tuple[PageRange, ...]
    selected_pages: tuple[PageKey, ...]
    gets: int
    bytes: int


@dataclass(frozen=True, slots=True)
class V102Result:
    """Matched control and S3-resident PQ48 diagnostic evidence."""

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
        "two-wave-pq48-rejected",
        "two-wave-pq48-diagnostic-qualified",
    ]
    refinement_books_identity: ArrayIdentity
    refinement_codes_identity: ArrayIdentity
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


def build_refinement_page_directory(
    inputs: ScreenInputs, *, row_bytes: int
) -> dict[object, RoutedPage]:
    """Build exact same-role offsets for page-aligned immutable code planes."""

    if type(row_bytes) is not int or row_bytes <= 0:
        raise ValueError("V102 refinement row width differs")
    pages: dict[object, RoutedPage] = {}
    offsets = {"base": 0, "delta": 0}
    for key in sorted(inputs.row_order_by_page):
        rows = inputs.row_order_by_page[key]
        byte_count = len(rows) * row_bytes
        if not rows or byte_count <= 0 or key.object_role not in offsets:
            raise ValueError("V102 refinement page roster differs")
        pages[key] = RoutedPage(key, offsets[key.object_role], byte_count)
        offsets[key.object_role] += byte_count
    if set(pages) != set(inputs.pages):
        raise ValueError("V102 refinement page roster differs")
    return pages


def plan_refinement_fetch(
    required_pages: Sequence[object],
    directory: Mapping[object, RoutedPage],
    *,
    max_gets: int,
    max_bytes: int,
) -> RangeSelection:
    """Cover every retained code page, charging every merged interior gap."""

    required = tuple(dict.fromkeys(required_pages))
    if not required:
        raise ValueError("V102 refinement pages do not fit")
    try:
        selection = select_ranked_gap_ranges(
            required, directory, max_gets=max_gets, max_bytes=max_bytes
        )
    except ValueError as error:
        raise ValueError("V102 refinement pages do not fit") from error
    if not set(required).issubset(selection.pages):
        raise ValueError("V102 refinement pages do not fit")
    return selection


def project_v102_resident_bytes_100m(config: RankedGapConfig) -> V102Projection:
    """Keep the PQ48 plane in S3 and account only its codebook in RAM."""

    baseline = project_v98_resident_bytes_100m(SUMMARY_ONLY_PQ16X8, config)
    codebook_bytes = 256 * 768 * np.dtype(np.float32).itemsize
    total = baseline.total_bytes + codebook_bytes
    return V102Projection(
        summary_only_resident=baseline,
        refinement_codebook_bytes=codebook_bytes,
        refinement_code_plane_bytes=100_000_000 * PQ48X8.row_bytes,
        row_codes_resident_bytes=0,
        total_resident_bytes=total,
        budget_bytes=baseline.budget_bytes,
        resident_eligible=total < baseline.budget_bytes,
    )


def _empty_aggregate() -> AggregateEvidence:
    return AggregateEvidence(0, 0, 0, 0, 0, False, False)


def evaluate_v102(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: RankedGapConfig,
) -> V102Result:
    """Evaluate one fixed page-aligned S3 PQ48 refinement wave."""

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
        raise ValueError("V102 input authority differs")
    hierarchy = build_hierarchy(inputs, config)
    fences = tuple(route_hierarchy(query, hierarchy, config) for query in queries)
    refinement_directory = build_refinement_page_directory(
        inputs, row_bytes=PQ48X8.row_bytes
    )
    fetches: list[RefinementFetchEvidence] = []
    io_failed = False
    for ordinal, fence in enumerate(fences):
        try:
            selection = plan_refinement_fetch(
                fence.retained_pages,
                refinement_directory,
                max_gets=config.maximum_gets,
                max_bytes=config.maximum_bytes,
            )
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
        except ValueError:
            io_failed = True
            break

    base_positions = np.asarray(
        [
            position
            for position, row_id in enumerate(source_ids)
            if inputs.page_by_id[int(row_id)].object_role == "base"
        ],
        dtype=np.int64,
    )
    training = np.ascontiguousarray(vectors[base_positions])
    refinement_books = fit_pq(
        training,
        PQ48X8,
        seed=inputs.seed,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    refinement_codes = np.ascontiguousarray(
        encode_pq(vectors, refinement_books, PQ48X8)
    )
    projection = project_v102_resident_bytes_100m(config)
    empty_paired = PairedIntervalEvidence("pq48x8-s3-refinement", (0, 0), (0, 0), (0, 0))
    if io_failed:
        return V102Result(
            schema="borsuk-v102-two-wave-pq48-v1",
            authority=authority,
            config=config,
            query_count=queries.shape[0],
            refinement_row_bytes=PQ48X8.row_bytes,
            bootstrap_seed=authority.seed,
            bootstrap_resamples=10_000,
            bootstrap_matrix_sha256="0" * 64,
            classification="refinement-io-rejected",
            refinement_books_identity=_array_identity(refinement_books),
            refinement_codes_identity=_array_identity(refinement_codes),
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

    control_books = fit_pq(
        training,
        PQ16X8,
        seed=inputs.seed,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    control_codes = np.ascontiguousarray(encode_pq(vectors, control_books, PQ16X8))
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
            control_books,
            PQ16X8,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=shortlist,
        )
        challenger_rows = score_retained_rows(
            query,
            row_ids,
            np.ascontiguousarray(refinement_codes[positions]),
            refinement_books,
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
    control10 = tuple(item.recall10_ppm for item in control_tuple)
    control100 = tuple(item.recall100_ppm for item in control_tuple)
    challenger10 = tuple(item.recall10_ppm for item in challenger_tuple)
    challenger100 = tuple(item.recall100_ppm for item in challenger_tuple)
    paired = PairedIntervalEvidence(
        name="pq48x8-s3-refinement",
        average_recall10_ppm=_producer_paired_interval(
            challenger10, control10, matrix, statistic="mean"
        ),
        average_recall100_ppm=_producer_paired_interval(
            challenger100, control100, matrix, statistic="mean"
        ),
        p05_recall100_ppm=_producer_paired_interval(
            challenger100, control100, matrix, statistic="p05"
        ),
    )
    refinement_gets = max(item.gets for item in fetches)
    refinement_bytes = max(item.bytes for item in fetches)
    refinement_resource = (
        refinement_gets <= config.maximum_gets
        and refinement_bytes <= config.maximum_bytes
    )
    qualified = (
        refinement_resource
        and challenger_aggregate.quality_gate_passed
        and challenger_aggregate.resource_gate_passed
        and projection.resident_eligible
        and paired.average_recall100_ppm[0] >= 0
    )
    return V102Result(
        schema="borsuk-v102-two-wave-pq48-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        refinement_row_bytes=PQ48X8.row_bytes,
        bootstrap_seed=authority.seed,
        bootstrap_resamples=10_000,
        bootstrap_matrix_sha256=hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        classification=(
            "two-wave-pq48-diagnostic-qualified"
            if qualified
            else "two-wave-pq48-rejected"
        ),
        refinement_books_identity=_array_identity(refinement_books),
        refinement_codes_identity=_array_identity(refinement_codes),
        refinement_fetches=tuple(fetches),
        refinement_maximum_gets=refinement_gets,
        refinement_maximum_bytes=refinement_bytes,
        refinement_resource_gate_passed=refinement_resource,
        control_aggregate=control_aggregate,
        control_samples=control_tuple,
        challenger_aggregate=challenger_aggregate,
        challenger_samples=challenger_tuple,
        paired=paired,
        projection=projection,
    )


def canonical_v102_result_bytes(result: V102Result) -> bytes:
    """Serialize complete typed V102 evidence as canonical newline JSON."""

    if (
        not isinstance(result, V102Result)
        or result.schema != "borsuk-v102-two-wave-pq48-v1"
        or result.query_count <= 0
        or result.bootstrap_resamples != 10_000
    ):
        raise ValueError("V102 result differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
