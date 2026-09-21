#!/usr/bin/env python3
"""Two-stage residual PQ at the same 16-byte row width as flat PQ16."""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import asdict, dataclass
from typing import Literal

import numpy as np

from scripts.v97_row_width_screen import (
    PQ16X8,
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

STAGES = 2
SUBSPACES = 8
CODEWORDS = 256
PQ8X8_STAGE = PqSpec("pq8x8-stage", SUBSPACES, 8, SUBSPACES)


@dataclass(frozen=True, slots=True)
class V101Projection:
    """Complete 100M resident worksheet delta over the PQ16 control."""

    row_bytes: int
    pq16_control: V98Projection
    second_codebooks_bytes: int
    cross_terms_bytes: int
    total_bytes: int
    budget_bytes: int
    eligible: bool


@dataclass(frozen=True, slots=True)
class V101Result:
    """Matched flat-PQ16 and two-stage residual-PQ evidence."""

    schema: str
    authority: ScreenAuthority
    config: RankedGapConfig
    query_count: int
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_matrix_sha256: str
    classification: Literal[
        "two-stage-residual-pq-rejected", "two-stage-residual-pq-qualified"
    ]
    artifact_sha256: str
    first_books_identity: ArrayIdentity
    second_books_identity: ArrayIdentity
    first_codes_identity: ArrayIdentity
    second_codes_identity: ArrayIdentity
    cross_terms_identity: ArrayIdentity
    control_aggregate: AggregateEvidence
    control_samples: tuple[RangeSample, ...]
    challenger_aggregate: AggregateEvidence
    challenger_samples: tuple[RangeSample, ...]
    paired: PairedIntervalEvidence
    projection: V101Projection


@dataclass(frozen=True, slots=True)
class TwoStageResidualPqArtifact:
    """Two additive PQ8 stages and their query-independent cross terms."""

    first_books: np.ndarray
    second_books: np.ndarray
    first_codes: np.ndarray
    second_codes: np.ndarray
    cross_terms: np.ndarray
    dimensions: int

    @classmethod
    def from_arrays(
        cls,
        *,
        first_books: np.ndarray,
        second_books: np.ndarray,
        first_codes: np.ndarray,
        second_codes: np.ndarray,
        dimensions: int | None = None,
    ) -> TwoStageResidualPqArtifact:
        """Validate canonical arrays and derive `2*c1·c2` tables."""

        books1 = np.ascontiguousarray(first_books)
        books2 = np.ascontiguousarray(second_books)
        codes1 = np.ascontiguousarray(first_codes)
        codes2 = np.ascontiguousarray(second_codes)
        registered_dimensions = (
            books1.shape[0] * books1.shape[2]
            if dimensions is None and books1.ndim == 3
            else dimensions
        )
        if (
            books1.ndim != 3
            or books1.dtype != np.float32
            or books1.shape[0] != SUBSPACES
            or books1.shape[1] != CODEWORDS
            or books2.shape != books1.shape
            or books2.dtype != np.float32
            or codes1.ndim != 2
            or codes1.dtype != np.uint8
            or codes1.shape[1] != SUBSPACES
            or codes2.shape != codes1.shape
            or codes2.dtype != np.uint8
            or not np.isfinite(books1).all()
            or not np.isfinite(books2).all()
            or type(registered_dimensions) is not int
            or registered_dimensions <= 0
            or registered_dimensions > SUBSPACES * books1.shape[2]
        ):
            raise ValueError("V101 artifact arrays differ")
        padded_dimensions = SUBSPACES * books1.shape[2]
        for coordinate in range(registered_dimensions, padded_dimensions):
            subspace, lane = divmod(coordinate, books1.shape[2])
            for books in (books1, books2):
                padding = books[subspace, :, lane]
                if np.any(padding != 0.0) or np.any(np.signbit(padding)):
                    raise ValueError("V101 artifact padding differs")
        cross = np.ascontiguousarray(
            np.float32(2.0)
            * np.einsum("siw,sjw->sij", books1, books2, dtype=np.float32)
        )
        return cls(
            books1,
            books2,
            codes1,
            codes2,
            cross,
            registered_dimensions,
        )

    @property
    def row_bytes(self) -> int:
        """Return the exact persistent code width per row."""

        return self.first_codes.shape[1] + self.second_codes.shape[1]

    def digest(self) -> str:
        """Bind the complete query-independent representation."""

        digest = hashlib.sha256()
        digest.update(struct.pack("<Q", self.dimensions))
        for value in (
            self.first_books,
            self.second_books,
            self.first_codes,
            self.second_codes,
            self.cross_terms,
        ):
            array = np.ascontiguousarray(value)
            digest.update(array.dtype.str.encode("ascii"))
            digest.update(struct.pack("<Q", array.ndim))
            for size in array.shape:
                digest.update(struct.pack("<Q", size))
            digest.update(array.tobytes(order="C"))
        return digest.hexdigest()


def two_stage_adc_scores(
    query: np.ndarray, artifact: TwoStageResidualPqArtifact
) -> np.ndarray:
    """Score reconstructed `c1+c2` rows under exact squared Euclidean ADC."""

    vector = np.asarray(query)
    width = artifact.first_books.shape[2]
    if (
        vector.ndim != 1
        or vector.dtype != np.float32
        or vector.size != artifact.dimensions
        or not np.isfinite(vector).all()
        or artifact.cross_terms.shape != (SUBSPACES, CODEWORDS, CODEWORDS)
        or artifact.cross_terms.dtype != np.float32
        or not np.isfinite(artifact.cross_terms).all()
        or artifact.first_codes.ndim != 2
        or artifact.first_codes.dtype != np.uint8
        or artifact.first_codes.shape[1] != SUBSPACES
        or artifact.second_codes.shape != artifact.first_codes.shape
        or artifact.second_codes.dtype != np.uint8
    ):
        raise ValueError("V101 ADC input differs")
    padded = np.zeros(SUBSPACES * width, dtype=np.float32)
    padded[: vector.size] = vector
    scores = np.zeros(artifact.first_codes.shape[0], dtype=np.float32)
    for subspace in range(SUBSPACES):
        start = subspace * width
        stop = start + width
        block = padded[start:stop]
        first = artifact.first_books[subspace]
        second = artifact.second_books[subspace]
        first_table = (
            np.einsum("i,i->", block, block, dtype=np.float32)
            + np.einsum("ij,ij->i", first, first, dtype=np.float32)
            - np.float32(2.0) * (first @ block)
        )
        second_table = (
            np.einsum("ij,ij->i", second, second, dtype=np.float32)
            - np.float32(2.0) * (second @ block)
        )
        first_code = artifact.first_codes[:, subspace]
        second_code = artifact.second_codes[:, subspace]
        scores += (
            first_table[first_code]
            + second_table[second_code]
            + artifact.cross_terms[subspace, first_code, second_code]
        )
    if not np.isfinite(scores).all():
        raise ValueError("V101 ADC score differs")
    return scores


def _zero_pad(values: np.ndarray, padded_dimensions: int) -> np.ndarray:
    array = np.asarray(values)
    if (
        array.ndim != 2
        or array.dtype != np.float32
        or array.shape[1] <= 0
        or array.shape[1] > padded_dimensions
        or not np.isfinite(array).all()
    ):
        raise ValueError("V101 training vectors differ")
    if array.shape[1] == padded_dimensions:
        return np.ascontiguousarray(array)
    padded = np.zeros((array.shape[0], padded_dimensions), dtype=np.float32)
    padded[:, : array.shape[1]] = array
    return padded


def _reconstruct(codes: np.ndarray, books: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(
        np.concatenate(
            [books[subspace][codes[:, subspace]] for subspace in range(SUBSPACES)],
            axis=1,
        )
    )


def build_two_stage_residual_pq(
    *,
    training_vectors: np.ndarray,
    vectors: np.ndarray,
    seed: int,
    sample_rows: int,
    iterations: int,
    block_rows: int = 8_192,
) -> TwoStageResidualPqArtifact:
    """Fit on one bounded sample and encode the corpus in bounded row blocks."""

    training = np.asarray(training_vectors)
    values = np.asarray(vectors)
    if (
        training.ndim != 2
        or values.ndim != 2
        or training.dtype != np.float32
        or values.dtype != np.float32
        or training.shape[1] != values.shape[1]
        or training.shape[0] < CODEWORDS
        or type(sample_rows) is not int
        or sample_rows < CODEWORDS
        or type(iterations) is not int
        or iterations <= 0
        or type(block_rows) is not int
        or block_rows <= 0
        or not np.isfinite(training).all()
        or not np.isfinite(values).all()
    ):
        raise ValueError("V101 build input differs")
    dimensions = values.shape[1]
    width = (dimensions + SUBSPACES - 1) // SUBSPACES
    padded_dimensions = width * SUBSPACES
    take = min(training.shape[0], sample_rows)
    generator = np.random.default_rng(seed ^ 0x56313031)
    sample_positions = generator.choice(training.shape[0], take, replace=False)
    sample = _zero_pad(np.ascontiguousarray(training[sample_positions]), padded_dimensions)
    first_books = fit_pq(
        sample,
        PQ8X8_STAGE,
        seed=seed ^ 0x31514750,
        sample_rows=take,
        iterations=iterations,
    )
    first_sample_codes = encode_pq(sample, first_books, PQ8X8_STAGE)
    residual_sample = np.ascontiguousarray(
        sample - _reconstruct(first_sample_codes, first_books)
    )
    second_books = fit_pq(
        residual_sample,
        PQ8X8_STAGE,
        seed=seed ^ 0x32514750,
        sample_rows=take,
        iterations=iterations,
    )
    first_codes = np.empty((values.shape[0], SUBSPACES), dtype=np.uint8)
    second_codes = np.empty_like(first_codes)
    for start in range(0, values.shape[0], block_rows):
        stop = min(start + block_rows, values.shape[0])
        block = _zero_pad(np.ascontiguousarray(values[start:stop]), padded_dimensions)
        encoded_first = encode_pq(block, first_books, PQ8X8_STAGE)
        residual = np.ascontiguousarray(block - _reconstruct(encoded_first, first_books))
        first_codes[start:stop] = encoded_first
        second_codes[start:stop] = encode_pq(
            residual, second_books, PQ8X8_STAGE
        )
    return TwoStageResidualPqArtifact.from_arrays(
        first_books=first_books,
        second_books=second_books,
        first_codes=first_codes,
        second_codes=second_codes,
        dimensions=dimensions,
    )


def score_two_stage_retained_rows(
    query: np.ndarray,
    *,
    row_ids: np.ndarray,
    row_positions: np.ndarray,
    artifact: TwoStageResidualPqArtifact,
    maximum_rows: int,
    shortlist_rows: int,
    block_rows: int = 8_192,
) -> tuple[int, ...]:
    """Return bounded best rows under exact `(ADC distance,row id)` order."""

    ids = np.asarray(row_ids)
    positions = np.asarray(row_positions)
    if (
        ids.ndim != 1
        or positions.ndim != 1
        or ids.shape != positions.shape
        or not np.issubdtype(ids.dtype, np.integer)
        or not np.issubdtype(positions.dtype, np.integer)
        or ids.size == 0
        or ids.size > maximum_rows
        or np.unique(ids).size != ids.size
        or np.any(positions < 0)
        or np.any(positions >= artifact.first_codes.shape[0])
        or type(shortlist_rows) is not int
        or not 0 < shortlist_rows <= ids.size
        or type(block_rows) is not int
        or block_rows <= 0
    ):
        raise ValueError("V101 retained row-score input differs")
    best_scores = np.empty(0, dtype=np.float32)
    best_ids = np.empty(0, dtype=ids.dtype)
    for start in range(0, ids.size, block_rows):
        stop = min(start + block_rows, ids.size)
        selected = positions[start:stop]
        block_artifact = TwoStageResidualPqArtifact(
            first_books=artifact.first_books,
            second_books=artifact.second_books,
            first_codes=np.ascontiguousarray(artifact.first_codes[selected]),
            second_codes=np.ascontiguousarray(artifact.second_codes[selected]),
            cross_terms=artifact.cross_terms,
            dimensions=artifact.dimensions,
        )
        scores = two_stage_adc_scores(query, block_artifact)
        candidate_scores = np.concatenate((best_scores, scores))
        candidate_ids = np.concatenate((best_ids, ids[start:stop]))
        order = np.lexsort((candidate_ids, candidate_scores))[:shortlist_rows]
        best_scores = np.ascontiguousarray(candidate_scores[order])
        best_ids = np.ascontiguousarray(candidate_ids[order])
    return tuple(int(row_id) for row_id in best_ids)


def project_v101_resident_bytes_100m(
    config: RankedGapConfig, *, dimensions: int
) -> V101Projection:
    """Add the second PQ8 books and resident pairwise cross terms to PQ16."""

    if type(dimensions) is not int or dimensions <= 0:
        raise ValueError("V101 projection dimensions differ")
    control = project_v98_resident_bytes_100m(PQ16X8, config)
    width = (dimensions + SUBSPACES - 1) // SUBSPACES
    second_books = SUBSPACES * CODEWORDS * width * np.dtype(np.float32).itemsize
    cross_terms = SUBSPACES * CODEWORDS * CODEWORDS * np.dtype(np.float32).itemsize
    total = control.total_bytes + second_books + cross_terms
    return V101Projection(
        row_bytes=STAGES * SUBSPACES,
        pq16_control=control,
        second_codebooks_bytes=second_books,
        cross_terms_bytes=cross_terms,
        total_bytes=total,
        budget_bytes=control.budget_bytes,
        eligible=total < control.budget_bytes,
    )


def evaluate_v101(
    inputs: ScreenInputs,
    authority: ScreenAuthority,
    config: RankedGapConfig,
) -> V101Result:
    """Evaluate one fixed two-stage representation against flat PQ16."""

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
        or source_ids.shape != (vectors.shape[0],)
        or authority.dimensions != vectors.shape[1]
        or authority.seed != inputs.seed
        or inputs.max_gets != config.maximum_gets
        or inputs.max_bytes != config.maximum_bytes
    ):
        raise ValueError("V101 input authority differs")
    hierarchy = build_hierarchy(inputs, config)
    fences = tuple(route_hierarchy(query, hierarchy, config) for query in queries)
    base_positions = np.asarray(
        [
            position
            for position, row_id in enumerate(source_ids)
            if inputs.page_by_id[int(row_id)].object_role == "base"
        ],
        dtype=np.int64,
    )
    training = np.ascontiguousarray(vectors[base_positions])
    challenger = build_two_stage_residual_pq(
        training_vectors=training,
        vectors=vectors,
        seed=inputs.seed,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    control_books = fit_pq(
        training,
        PQ16X8,
        seed=inputs.seed,
        sample_rows=inputs.training_rows,
        iterations=inputs.training_iterations,
    )
    control_codes = np.ascontiguousarray(
        encode_pq(vectors, control_books, PQ16X8)
    )
    position_by_id = {
        int(row_id): position for position, row_id in enumerate(source_ids)
    }
    control_samples: list[RangeSample] = []
    challenger_samples: list[RangeSample] = []
    for ordinal, query in enumerate(queries):
        fence = fences[ordinal]
        row_ids, positions = _retained_row_positions(fence, inputs, position_by_id)
        control_rows = score_retained_rows(
            query,
            row_ids,
            np.ascontiguousarray(control_codes[positions]),
            control_books,
            PQ16X8,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=min(config.shortlist_rows, row_ids.size),
        )
        challenger_rows = score_two_stage_retained_rows(
            query,
            row_ids=row_ids,
            row_positions=positions,
            artifact=challenger,
            maximum_rows=config.maximum_scanned_rows,
            shortlist_rows=min(config.shortlist_rows, row_ids.size),
        )
        control_selection = _selection_from_rows(control_rows, inputs, config)
        challenger_selection = _selection_from_rows(challenger_rows, inputs, config)
        control_samples.append(
            _range_sample(ordinal, truth_ids[ordinal], control_selection, fence, inputs)
        )
        challenger_samples.append(
            _range_sample(
                ordinal, truth_ids[ordinal], challenger_selection, fence, inputs
            )
        )
    control_tuple = tuple(control_samples)
    challenger_tuple = tuple(challenger_samples)
    control_aggregate = _aggregate_range_samples(control_tuple, config)
    challenger_aggregate = _aggregate_range_samples(challenger_tuple, config)
    matrix = _producer_bootstrap_matrix(
        queries.shape[0], seed=authority.seed, resamples=10_000
    )
    control10 = tuple(sample.recall10_ppm for sample in control_tuple)
    control100 = tuple(sample.recall100_ppm for sample in control_tuple)
    challenger10 = tuple(sample.recall10_ppm for sample in challenger_tuple)
    challenger100 = tuple(sample.recall100_ppm for sample in challenger_tuple)
    paired = PairedIntervalEvidence(
        name="two-stage-residual-pq8x8",
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
    projection = project_v101_resident_bytes_100m(
        config, dimensions=vectors.shape[1]
    )
    qualified = (
        challenger_aggregate.quality_gate_passed
        and challenger_aggregate.resource_gate_passed
        and projection.eligible
        and paired.average_recall100_ppm[0] >= 0
    )
    return V101Result(
        schema="borsuk-v101-two-stage-residual-pq-v1",
        authority=authority,
        config=config,
        query_count=queries.shape[0],
        bootstrap_seed=authority.seed,
        bootstrap_resamples=10_000,
        bootstrap_matrix_sha256=hashlib.sha256(
            matrix.tobytes(order="C")
        ).hexdigest(),
        classification=(
            "two-stage-residual-pq-qualified"
            if qualified
            else "two-stage-residual-pq-rejected"
        ),
        artifact_sha256=challenger.digest(),
        first_books_identity=_array_identity(challenger.first_books),
        second_books_identity=_array_identity(challenger.second_books),
        first_codes_identity=_array_identity(challenger.first_codes),
        second_codes_identity=_array_identity(challenger.second_codes),
        cross_terms_identity=_array_identity(challenger.cross_terms),
        control_aggregate=control_aggregate,
        control_samples=control_tuple,
        challenger_aggregate=challenger_aggregate,
        challenger_samples=challenger_tuple,
        paired=paired,
        projection=projection,
    )


def canonical_v101_result_bytes(result: V101Result) -> bytes:
    """Serialize complete typed V101 evidence as canonical JSON."""

    if (
        not isinstance(result, V101Result)
        or result.schema != "borsuk-v101-two-stage-residual-pq-v1"
        or result.query_count != len(result.control_samples)
        or result.query_count != len(result.challenger_samples)
        or result.bootstrap_resamples != 10_000
    ):
        raise ValueError("V101 result differs")
    return (
        json.dumps(
            asdict(result), allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )
