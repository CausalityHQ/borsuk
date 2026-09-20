#!/usr/bin/env python3
"""Page-relative residual PQ evidence for the V99 adjacent-range planner."""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import asdict, dataclass
from typing import Literal, Mapping, Sequence

import numpy as np

from scripts.v97_row_width_screen import (
    PQ16X8,
    PageKey,
    RoutedPage,
    ScreenAuthority,
    ScreenInputs,
    encode_pq,
    fit_pq,
)
from scripts.v98_hierarchical_row_router import (
    AggregateEvidence,
    ArrayIdentity,
    HierarchyArtifact,
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
    RangeSelection,
    RankedGapConfig,
    _aggregate_range_samples,
    _range_sample,
    _selection_from_rows,
    select_ranked_gap_ranges,
)


@dataclass(frozen=True, slots=True)
class V100Projection:
    """Conservative 100M resident-memory worksheet for V100."""

    row_bytes: int
    v99_pq16: V98Projection
    decoded_page_means_resident_bytes: int
    decoded_page_mean_scratch_bytes: int
    total_bytes: int
    budget_bytes: int
    eligible: bool


@dataclass(frozen=True, slots=True)
class PageResidualArtifact:
    """Query-blind page-major PQ16 residual representation."""

    hierarchy_ipc_sha256: str
    page_keys: tuple[PageKey, ...]
    page_means: np.ndarray
    row_offsets: tuple[int, ...]
    row_ids_sha256: str
    books: np.ndarray
    row_codes: np.ndarray
    page_means_identity: ArrayIdentity
    books_identity: ArrayIdentity
    row_codes_identity: ArrayIdentity

    def digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(self.hierarchy_ipc_sha256.encode("ascii"))
        for key in self.page_keys:
            digest.update(key.object_role.encode("ascii"))
            digest.update(struct.pack("<Q", key.ordinal))
        digest.update(np.asarray(self.row_offsets, dtype=np.uint64).tobytes())
        digest.update(self.row_ids_sha256.encode("ascii"))
        digest.update(np.ascontiguousarray(self.page_means).tobytes())
        digest.update(np.ascontiguousarray(self.books).tobytes())
        digest.update(np.ascontiguousarray(self.row_codes).tobytes())
        return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class V100Result:
    """Typed matched evidence for V99 PQ16 versus V100 page residual PQ16."""

    schema: str
    authority: ScreenAuthority
    config: RankedGapConfig
    query_count: int
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    classification: Literal["page-residual-rejected", "page-residual-qualified"]
    artifact_sha256: str
    page_means_identity: ArrayIdentity
    residual_books_identity: ArrayIdentity
    residual_codes_identity: ArrayIdentity
    control_aggregate: AggregateEvidence
    control_samples: tuple[RangeSample, ...]
    residual_aggregate: AggregateEvidence
    residual_samples: tuple[RangeSample, ...]
    paired: PairedIntervalEvidence
    projection: V100Projection


def rank_page_residuals(
    query: np.ndarray,
    *,
    page_keys: Sequence[PageKey],
    page_means: np.ndarray,
    page_codes: np.ndarray,
    page_row_counts: np.ndarray,
    books: np.ndarray,
) -> tuple[PageKey, ...]:
    """Rank pages by `(best residual ADC, second best, PageKey)`."""

    vector = np.asarray(query)
    means = np.asarray(page_means)
    codes = np.asarray(page_codes)
    counts = np.asarray(page_row_counts)
    centroids = np.asarray(books)
    page_count = len(page_keys)
    if (
        page_count == 0
        or len(set(page_keys)) != page_count
        or vector.ndim != 1
        or vector.dtype != np.float32
        or means.shape != (page_count, vector.size)
        or means.dtype != np.float32
        or codes.ndim != 3
        or codes.shape[0] != page_count
        or codes.shape[2] != PQ16X8.subspaces
        or codes.dtype != np.uint8
        or counts.shape != (page_count,)
        or not np.issubdtype(counts.dtype, np.integer)
        or np.any(counts <= 0)
        or np.any(counts > codes.shape[1])
        or centroids.shape
        != (
            PQ16X8.subspaces,
            1 << PQ16X8.centroid_bits,
            vector.size // PQ16X8.subspaces,
        )
        or centroids.dtype != np.float32
        or vector.size % PQ16X8.subspaces
        or not np.isfinite(vector).all()
        or not np.isfinite(means).all()
        or not np.isfinite(centroids).all()
    ):
        raise ValueError("V100 residual page-score input differs")

    residual_queries = np.ascontiguousarray(vector[None, :] - means)
    scores = np.zeros(codes.shape[:2], dtype=np.float32)
    width = vector.size // PQ16X8.subspaces
    for subspace in range(PQ16X8.subspaces):
        start = subspace * width
        stop = start + width
        query_block = residual_queries[:, start:stop]
        book = centroids[subspace]
        table = (
            np.einsum("ij,ij->i", query_block, query_block)[:, None]
            + np.einsum("ij,ij->i", book, book)[None, :]
            - np.float32(2.0) * (query_block @ book.T)
        )
        scores += np.take_along_axis(table, codes[:, :, subspace], axis=1)

    evidence: list[tuple[float, float, PageKey]] = []
    for position, key in enumerate(page_keys):
        valid = scores[position, : int(counts[position])]
        if not np.isfinite(valid).all():
            raise ValueError("V100 residual page score differs")
        if valid.size == 1:
            best = second = float(valid[0])
        else:
            pair = np.partition(valid, 1)[:2]
            best, second = sorted((float(pair[0]), float(pair[1])))
        evidence.append((best, second, key))
    return tuple(key for _, _, key in sorted(evidence))


def _decoded_page_means(hierarchy: HierarchyArtifact) -> np.ndarray:
    page_count = len(hierarchy.page_keys)
    books = np.asarray(hierarchy.summary_books)
    codes = np.asarray(hierarchy.page_summary_codes)
    if (
        page_count == 0
        or books.ndim != 3
        or books.dtype != np.float32
        or codes.shape != (page_count * 2, books.shape[0])
        or codes.dtype != np.uint8
    ):
        raise ValueError("V100 hierarchy summaries differ")
    decoded = np.concatenate(
        [books[subspace][codes[:, subspace]] for subspace in range(books.shape[0])],
        axis=1,
    )
    return np.ascontiguousarray(
        decoded.reshape(page_count, 2, -1).mean(axis=1, dtype=np.float32)
    )


def build_page_residual_artifact(
    inputs: ScreenInputs, hierarchy: HierarchyArtifact
) -> PageResidualArtifact:
    """Build one query-blind PQ16 artifact in canonical page-major order."""

    source_ids = np.asarray(inputs.source_ids)
    vectors = np.asarray(inputs.vectors)
    if (
        hierarchy.page_keys != tuple(sorted(inputs.pages))
        or source_ids.ndim != 1
        or not np.issubdtype(source_ids.dtype, np.integer)
        or np.unique(source_ids).size != source_ids.size
        or vectors.ndim != 2
        or vectors.shape[0] != source_ids.size
        or vectors.dtype != np.float32
        or not np.isfinite(vectors).all()
    ):
        raise ValueError("V100 residual artifact input differs")
    means = _decoded_page_means(hierarchy)
    if means.shape != (len(hierarchy.page_keys), vectors.shape[1]):
        raise ValueError("V100 decoded page means differ")
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    residual_blocks: list[np.ndarray] = []
    row_ids: list[int] = []
    offsets = [0]
    base_blocks: list[np.ndarray] = []
    for page_position, key in enumerate(hierarchy.page_keys):
        registered = tuple(int(row_id) for row_id in inputs.row_order_by_page[key])
        try:
            positions = np.asarray(
                [position_by_id[row_id] for row_id in registered], dtype=np.int64
            )
        except KeyError as error:
            raise ValueError("V100 page row binding differs") from error
        block = np.ascontiguousarray(vectors[positions] - means[page_position])
        residual_blocks.append(block)
        row_ids.extend(registered)
        offsets.append(offsets[-1] + len(registered))
        if key.object_role == "base":
            base_blocks.append(block)
    if offsets[-1] != source_ids.size or len(set(row_ids)) != source_ids.size:
        raise ValueError("V100 page-major roster differs")
    residuals = np.ascontiguousarray(np.concatenate(residual_blocks, axis=0))
    base_residuals = np.ascontiguousarray(np.concatenate(base_blocks, axis=0))
    books = fit_pq(
        base_residuals,
        PQ16X8,
        seed=inputs.seed ^ 0x56313030,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    codes = np.ascontiguousarray(encode_pq(residuals, books, PQ16X8))
    row_id_bytes = np.asarray(row_ids, dtype=np.int64).tobytes(order="C")
    return PageResidualArtifact(
        hierarchy_ipc_sha256=hierarchy.ipc_sha256,
        page_keys=hierarchy.page_keys,
        page_means=means,
        row_offsets=tuple(offsets),
        row_ids_sha256=hashlib.sha256(row_id_bytes).hexdigest(),
        books=books,
        row_codes=codes,
        page_means_identity=_array_identity(means),
        books_identity=_array_identity(books),
        row_codes_identity=_array_identity(codes),
    )


def _rank_retained_pages(
    query: np.ndarray,
    retained_pages: Sequence[PageKey],
    artifact: PageResidualArtifact,
) -> tuple[PageKey, ...]:
    page_position = {key: position for position, key in enumerate(artifact.page_keys)}
    try:
        positions = np.asarray(
            [page_position[key] for key in retained_pages], dtype=np.int64
        )
    except KeyError as error:
        raise ValueError("V100 retained page differs") from error
    counts = np.asarray(
        [
            artifact.row_offsets[position + 1] - artifact.row_offsets[position]
            for position in positions
        ],
        dtype=np.int64,
    )
    maximum = int(counts.max(initial=0))
    if maximum <= 0:
        raise ValueError("V100 retained page differs")
    codes = np.zeros((len(retained_pages), maximum, PQ16X8.subspaces), dtype=np.uint8)
    for target, position in enumerate(positions):
        start = artifact.row_offsets[int(position)]
        stop = artifact.row_offsets[int(position) + 1]
        codes[target, : stop - start] = artifact.row_codes[start:stop]
    return rank_page_residuals(
        query,
        page_keys=retained_pages,
        page_means=np.ascontiguousarray(artifact.page_means[positions]),
        page_codes=codes,
        page_row_counts=counts,
        books=artifact.books,
    )


def select_page_residual_ranges(
    ranked_pages: Sequence[PageKey],
    pages: Mapping[PageKey, RoutedPage],
    *,
    max_gets: int,
    max_bytes: int,
) -> RangeSelection:
    """Apply the unchanged V99 adjacent-range planner to V100 page evidence."""

    return select_ranked_gap_ranges(
        ranked_pages, pages, max_gets=max_gets, max_bytes=max_bytes
    )


def project_v100_resident_bytes_100m(
    config: RankedGapConfig, *, dimensions: int
) -> V100Projection:
    """Project V100 memory without retaining decoded dense page means."""

    if type(dimensions) is not int or dimensions <= 0:
        raise ValueError("V100 projection dimensions differ")
    base = project_v98_resident_bytes_100m(PQ16X8, config)
    scratch = config.retained_pages * dimensions * np.dtype(np.float32).itemsize
    total = base.total_bytes + scratch
    return V100Projection(
        row_bytes=PQ16X8.row_bytes,
        v99_pq16=base,
        decoded_page_means_resident_bytes=0,
        decoded_page_mean_scratch_bytes=scratch,
        total_bytes=total,
        budget_bytes=base.budget_bytes,
        eligible=total < base.budget_bytes,
    )


def evaluate_v100(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: RankedGapConfig,
) -> V100Result:
    """Evaluate one fixed V100 hypothesis against its matched V99 PQ16 control."""

    queries = np.asarray(inputs.queries)
    truth_ids = np.asarray(inputs.truth_ids)
    vectors = np.asarray(inputs.vectors)
    source_ids = np.asarray(inputs.source_ids)
    if (
        queries.ndim != 2
        or queries.dtype != np.float32
        or not np.isfinite(queries).all()
        or truth_ids.shape != (queries.shape[0], inputs.neighbors)
        or authority.dimensions != vectors.shape[1]
        or authority.seed != inputs.seed
        or inputs.max_gets != config.maximum_gets
        or inputs.max_bytes != config.maximum_bytes
    ):
        raise ValueError("V100 input authority differs")
    hierarchy = build_hierarchy(inputs, config)
    fences = tuple(route_hierarchy(query, hierarchy, config) for query in queries)
    residual = build_page_residual_artifact(inputs, hierarchy)

    base_positions = np.asarray(
        [
            position
            for position, row_id in enumerate(source_ids)
            if inputs.page_by_id[int(row_id)].object_role == "base"
        ],
        dtype=np.int64,
    )
    control_books = fit_pq(
        np.ascontiguousarray(vectors[base_positions]),
        PQ16X8,
        seed=inputs.seed,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    control_codes = encode_pq(vectors, control_books, PQ16X8)
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    control_samples: list[RangeSample] = []
    residual_samples: list[RangeSample] = []
    for ordinal, query in enumerate(queries):
        fence = fences[ordinal]
        row_ids, positions = _retained_row_positions(fence, inputs, position_by_id)
        ranked_rows = score_retained_rows(
            query,
            row_ids,
            np.ascontiguousarray(control_codes[positions]),
            control_books,
            PQ16X8,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=min(config.shortlist_rows, row_ids.size),
        )
        control_selection = _selection_from_rows(ranked_rows, inputs, config)
        control_samples.append(
            _range_sample(ordinal, truth_ids[ordinal], control_selection, fence, inputs)
        )
        ranked_pages = _rank_retained_pages(query, fence.retained_pages, residual)
        residual_selection = select_page_residual_ranges(
            ranked_pages,
            inputs.pages,
            max_gets=config.maximum_gets,
            max_bytes=config.maximum_bytes,
        )
        residual_samples.append(
            _range_sample(
                ordinal, truth_ids[ordinal], residual_selection, fence, inputs
            )
        )
    control_tuple = tuple(control_samples)
    residual_tuple = tuple(residual_samples)
    control_aggregate = _aggregate_range_samples(control_tuple, config)
    residual_aggregate = _aggregate_range_samples(residual_tuple, config)
    matrix = _producer_bootstrap_matrix(
        queries.shape[0], seed=authority.seed, resamples=10_000
    )
    control10 = tuple(sample.recall10_ppm for sample in control_tuple)
    control100 = tuple(sample.recall100_ppm for sample in control_tuple)
    residual10 = tuple(sample.recall10_ppm for sample in residual_tuple)
    residual100 = tuple(sample.recall100_ppm for sample in residual_tuple)
    paired = PairedIntervalEvidence(
        name="page-residual-pq16x8",
        average_recall10_ppm=_producer_paired_interval(
            residual10, control10, matrix, statistic="mean"
        ),
        average_recall100_ppm=_producer_paired_interval(
            residual100, control100, matrix, statistic="mean"
        ),
        p05_recall100_ppm=_producer_paired_interval(
            residual100, control100, matrix, statistic="p05"
        ),
    )
    projection = project_v100_resident_bytes_100m(config, dimensions=vectors.shape[1])
    qualified = (
        residual_aggregate.quality_gate_passed
        and residual_aggregate.resource_gate_passed
        and projection.eligible
        and paired.average_recall100_ppm[0] >= 0
    )
    return V100Result(
        schema="borsuk-v100-page-residual-range-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        bootstrap_seed=authority.seed,
        bootstrap_resamples=10_000,
        bootstrap_matrix_sha256=hashlib.sha256(matrix.tobytes(order="C")).hexdigest(),
        classification=(
            "page-residual-qualified" if qualified else "page-residual-rejected"
        ),
        artifact_sha256=residual.digest(),
        page_means_identity=residual.page_means_identity,
        residual_books_identity=residual.books_identity,
        residual_codes_identity=residual.row_codes_identity,
        control_aggregate=control_aggregate,
        control_samples=control_tuple,
        residual_aggregate=residual_aggregate,
        residual_samples=residual_tuple,
        paired=paired,
        projection=projection,
    )


def canonical_v100_result_bytes(result: V100Result) -> bytes:
    """Serialize complete typed V100 evidence as canonical JSON."""

    if (
        not isinstance(result, V100Result)
        or result.schema != "borsuk-v100-page-residual-range-v1"
        or result.query_count != len(result.control_samples)
        or result.query_count != len(result.residual_samples)
        or result.bootstrap_resamples != 10_000
    ):
        raise ValueError("V100 result differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
