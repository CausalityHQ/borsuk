"""Read-free, GT-free nominee score-fidelity screen for V113.

This module contains query-side decisions. Corpus-only training and sealed
artifact publication happen in separate phases on a Causality Spot worker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from scripts.v113_resident_score import (
    Corrections, _decode_pq, _partition_padded,
    encode_corrections_from_codes, fit_residual_pq,
    score_encoded_nominees, score_nominees,
)


@dataclass(frozen=True, slots=True)
class QueryScreen:
    oracle_primary: np.ndarray
    resident_primary: np.ndarray
    scalar_primary: np.ndarray
    resident_overlap: int
    scalar_overlap: int
    oracle_fp_error_max: float
    resident_score_error_mean: float
    resident_score_error_max: float
    scalar_score_error_mean: float
    scalar_score_error_max: float


@dataclass(frozen=True, slots=True)
class BuiltArrays:
    ids: np.ndarray
    low: np.ndarray
    span_step: np.ndarray
    sq8_codes: np.ndarray
    sq8_norm: np.ndarray
    pq_books: np.ndarray
    pq_codes: np.ndarray
    residual_books: np.ndarray
    residual_codes: np.ndarray
    corrections: Corrections
    residual_maximum_error: np.ndarray


def build_arrays(
    vectors: np.ndarray, identifiers: np.ndarray,
    *, pq_seed: int, residual_seed: int, sample_rows: int,
    iterations: int,
) -> BuiltArrays:
    """Build all row codes from corpus rows without query or truth access."""
    from scripts.v97_row_width_screen import PqSpec, encode_pq, fit_pq

    source = np.asarray(vectors)
    ids = np.asarray(identifiers)
    if (
        source.ndim != 2 or source.dtype != np.float32
        or source.shape[0] < 256 or source.shape[1] < 64
        or ids.shape != (source.shape[0],)
        or not np.issubdtype(ids.dtype, np.integer)
        or np.unique(ids).size != ids.size
        or not np.isfinite(source).all()
    ):
        raise ValueError("V113 source-only input differs")
    low = source.min(axis=0).astype(np.float32)
    high = source.max(axis=0).astype(np.float32)
    span = np.maximum(high - low, np.float32(1e-12)).astype(np.float32)
    sq8_codes = np.clip(
        np.rint((source - low) / span * np.float32(255.0)), 0, 255,
    ).astype(np.uint8)
    step = (span / np.float32(255.0)).astype(np.float32)
    dequantized = low + sq8_codes.astype(np.float32) * step
    norm = np.einsum("ij,ij->i", dequantized, dequantized, dtype=np.float32)
    del dequantized
    padded = _partition_padded(source, 64)
    specification = PqSpec("v113-pq64", 64, 8, 64)
    pq_books = fit_pq(
        padded, specification, seed=pq_seed,
        sample_rows=sample_rows, iterations=iterations,
    )
    pq_codes = encode_pq(padded, pq_books, specification)
    del padded
    corrections = encode_corrections_from_codes(
        pq_books, pq_codes, sq8_codes, norm, low, step,
    )
    residual_books, residual_codes, _, maximum_error = fit_residual_pq(
        corrections.residual, seed=residual_seed,
        sample_rows=sample_rows, iterations=iterations,
    )
    return BuiltArrays(
        ids, low, step, sq8_codes, norm, pq_books, pq_codes,
        residual_books, residual_codes, corrections, maximum_error,
    )


def nominate_pq64(
    query: np.ndarray, books: np.ndarray, codes: np.ndarray, shortlist: int,
) -> np.ndarray:
    """Exhaustive PQ64 top rows under deterministic (score, ordinal) order."""
    q = np.asarray(query)
    book = np.asarray(books)
    row_codes = np.asarray(codes)
    if (
        q.ndim != 1 or q.dtype != np.float32 or q.size < 64
        or book.dtype != np.float32 or book.ndim != 3
        or book.shape != (64, 256, (q.size + 63) // 64)
        or row_codes.dtype != np.uint8 or row_codes.ndim != 2
        or row_codes.shape[1] != 64
        or not 1 <= shortlist <= row_codes.shape[0]
        or not np.isfinite(q).all() or not np.isfinite(book).all()
    ):
        raise ValueError("PQ64 nomination inputs differ")
    padded = _partition_padded(q.reshape(1, -1), 64).reshape(64, book.shape[2])
    delta = book - padded[:, None, :]
    table = np.einsum("ijk,ijk->ij", delta, delta, dtype=np.float32)
    score = np.zeros(row_codes.shape[0], dtype=np.float32)
    for subspace in range(64):
        score += table[subspace, row_codes[:, subspace]]
    if not np.isfinite(score).all():
        raise ValueError("nonfinite PQ64 nomination score")
    return np.lexsort((np.arange(score.size), score))[:shortlist].astype(np.int32)


def primary_rows(
    rows: np.ndarray, identifiers: np.ndarray, scores: np.ndarray,
    count: int,
) -> np.ndarray:
    """Select the precise top rows under V112's (score, ID) tie order."""
    ordinal = np.asarray(rows)
    ids = np.asarray(identifiers)
    value = np.asarray(scores)
    if (
        ordinal.ndim != 1
        or not np.issubdtype(ordinal.dtype, np.integer)
        or ids.shape != ordinal.shape
        or not np.issubdtype(ids.dtype, np.integer)
        or value.shape != ordinal.shape
        or not np.issubdtype(value.dtype, np.floating)
        or not 1 <= count <= ordinal.size
        or len(np.unique(ordinal)) != ordinal.size
        or len(np.unique(ids)) != ids.size
        or not np.isfinite(value).all()
    ):
        raise ValueError("primary nominee roster differs")
    return ordinal[np.lexsort((ids, value))[:count]]


def compare_one_query(
    query: np.ndarray,
    identifiers: np.ndarray,
    pq_books: np.ndarray,
    pq_codes: np.ndarray,
    residual_books: np.ndarray,
    residual_codes: np.ndarray,
    corrections: Corrections,
    sq8_codes: np.ndarray,
    sq8_norm: np.ndarray,
    low: np.ndarray,
    span_step: np.ndarray,
    *,
    shortlist: int = 512,
    primary_count: int = 100,
) -> QueryScreen:
    """Compare query-side primary sets after all corpus-only codes are fixed."""
    q = np.asarray(query)
    ids = np.asarray(identifiers)
    sq8 = np.asarray(sq8_codes)
    norm = np.asarray(sq8_norm)
    origin = np.asarray(low)
    step = np.asarray(span_step)
    rows = np.asarray(pq_codes).shape[0]
    if (
        q.ndim != 1 or q.dtype != np.float32
        or ids.shape != (rows,) or not np.issubdtype(ids.dtype, np.integer)
        or sq8.shape != (rows, q.size) or sq8.dtype != np.uint8
        or norm.shape != (rows,) or norm.dtype != np.float32
        or origin.shape != q.shape or origin.dtype != np.float32
        or step.shape != q.shape or step.dtype != np.float32
        or corrections.nu_codes.shape != (rows,)
        or corrections.alpha_codes.shape != (rows,)
        or not 1 <= primary_count <= shortlist <= rows
        or not np.isfinite(norm).all()
        or not np.isfinite(origin).all()
        or not np.isfinite(step).all()
    ):
        raise ValueError("V113 query screen cohort differs")
    nominated = nominate_pq64(q, pq_books, pq_codes, shortlist)
    nominated_ids = ids[nominated]
    weights = q * step
    shift = float(q @ origin - (q @ q) / 2.0)
    inner = sq8[nominated].astype(np.float32) @ weights
    oracle_scores = norm[nominated] - 2.0 * (inner + shift)
    oracle = primary_rows(nominated, nominated_ids, oracle_scores, primary_count)
    resident_scores = score_encoded_nominees(
        q, pq_books, pq_codes[nominated], residual_books,
        residual_codes[nominated], corrections.nu_scale,
        corrections.nu_codes[nominated], corrections.alpha_scale,
        corrections.alpha_codes[nominated],
    )
    resident = primary_rows(nominated, nominated_ids, resident_scores, primary_count)
    pq_reconstruction = _decode_pq(pq_books, pq_codes[nominated], q.size, 64)
    scalar_corrections = Corrections(
        corrections.nu_scale, corrections.alpha_scale,
        corrections.nu_codes[nominated], corrections.alpha_codes[nominated],
        np.zeros_like(pq_reconstruction),
    )
    scalar_scores = score_nominees(
        q, pq_reconstruction, scalar_corrections,
        np.zeros_like(pq_reconstruction),
    )
    scalar = primary_rows(nominated, nominated_ids, scalar_scores, primary_count)
    q64 = q.astype(np.float64)
    target = origin.astype(np.float64) + (
        sq8[nominated].astype(np.float64) * step.astype(np.float64)
    )
    exact = q64 @ q64 + norm[nominated].astype(np.float64) - 2.0 * (target @ q64)
    oracle_fp_error_max = float(np.max(np.abs(oracle_scores.astype(np.float64) - exact)))
    resident_error = np.abs(resident_scores - oracle_scores.astype(np.float64))
    scalar_error = np.abs(scalar_scores - oracle_scores.astype(np.float64))
    oracle_set = set(map(int, oracle))
    return QueryScreen(
        oracle, resident, scalar,
        len(oracle_set.intersection(map(int, resident))),
        len(oracle_set.intersection(map(int, scalar))),
        oracle_fp_error_max,
        float(np.mean(resident_error)), float(np.max(resident_error)),
        float(np.mean(scalar_error)), float(np.max(scalar_error)),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _fixed_vectors(path: Path, rows: int, dimensions: int) -> np.ndarray:
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["embedding"])
    values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    result = np.asarray(values, dtype=np.float32).reshape(rows, dimensions).copy()
    if not np.isfinite(result).all():
        raise ValueError("nonfinite V113 source/query embedding")
    return result


def build_from_source(
    source: Path, source_sha256: str, output: Path, *,
    rows: int = 100_000, dimensions: int = 768,
    sample_rows: int = 100_000, iterations: int = 10,
) -> None:
    """Source-only phase; query and GT paths are unavailable by signature."""
    import pyarrow.parquet as pq

    from scripts.v113_score_artifact import write_artifact

    if _sha256_file(source) != source_sha256:
        raise ValueError("V113 source hash differs")
    vectors = _fixed_vectors(source, rows, dimensions)
    ids = pq.read_table(source, columns=["feature_row_id"])[
        "feature_row_id"
    ].combine_chunks().to_numpy(zero_copy_only=False)
    if ids.shape != (rows,) or np.max(ids) > np.iinfo(np.int64).max:
        raise ValueError("V113 source feature IDs differ")
    built = build_arrays(
        vectors, ids.astype(np.int64), pq_seed=7301,
        residual_seed=113031, sample_rows=sample_rows, iterations=iterations,
    )
    write_artifact(
        output, built, source_sha256=source_sha256,
        pq_seed=7301, residual_seed=113031,
        sample_rows=sample_rows, iterations=iterations,
    )


def score_from_artifact(
    artifact: Path, source_sha256: str, queries: Path,
    query_sha256: str, output: Path, summary: Path,
    *, rows: int = 100_000, dimensions: int = 768,
    query_count: int = 1_000, shortlist: int = 512,
    primary_count: int = 100, sample_rows: int = 100_000,
    iterations: int = 10,
) -> None:
    """Query phase; consumes only authenticated resident/SQ8 row artifacts."""
    from scripts.v113_score_artifact import read_artifact

    if _sha256_file(queries) != query_sha256:
        raise ValueError("V113 development-query hash differs")
    built = read_artifact(
        artifact, expected_source_sha256=source_sha256,
        expected_sample_rows=sample_rows, expected_iterations=iterations,
    )
    if built.ids.size != rows or built.low.size != dimensions:
        raise ValueError("V113 100k score screen geometry differs")
    query_vectors = _fixed_vectors(queries, query_count, dimensions)
    temporary = output.with_suffix(output.suffix + ".tmp")
    resident_hits: list[int] = []
    scalar_hits: list[int] = []
    with temporary.open("w") as handle:
        header = {
            "schema": "borsuk-v113-100k-score-screen-v1",
            "source_sha256": source_sha256,
            "query_sha256": query_sha256,
            "artifact_seal_sha256": _sha256_file(artifact / "seal.json"),
            "split": "relaion-100k-development-1000" if query_count == 1_000 else "test-fixture",
            "nominees": shortlist,
            "primary_rows": primary_count,
            "ground_truth_used": False,
            "physical_reads_measured": False,
        }
        handle.write(json.dumps(header, sort_keys=True, separators=(",", ":")) + "\n")
        for ordinal, query in enumerate(query_vectors):
            result = compare_one_query(
                query, built.ids, built.pq_books, built.pq_codes,
                built.residual_books, built.residual_codes,
                built.corrections, built.sq8_codes, built.sq8_norm,
                built.low, built.span_step,
                shortlist=shortlist, primary_count=primary_count,
            )
            resident_hits.append(result.resident_overlap)
            scalar_hits.append(result.scalar_overlap)
            record = {
                "query_ordinal": ordinal,
                "oracle_primary": result.oracle_primary.tolist(),
                "resident_primary": result.resident_primary.tolist(),
                "scalar_primary": result.scalar_primary.tolist(),
                "resident_overlap": result.resident_overlap,
                "scalar_overlap": result.scalar_overlap,
                "oracle_fp_error_max": result.oracle_fp_error_max,
                "resident_score_error_mean": result.resident_score_error_mean,
                "resident_score_error_max": result.resident_score_error_max,
                "scalar_score_error_mean": result.scalar_score_error_mean,
                "scalar_score_error_max": result.scalar_score_error_max,
            }
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(output)
    resident_sorted = sorted(resident_hits)
    scalar_sorted = sorted(scalar_hits)
    tail_index = math.ceil(query_count * 0.05) - 1
    resident_mean = sum(resident_hits) / query_count
    scalar_mean = sum(scalar_hits) / query_count
    resident_pass = (
        resident_mean >= primary_count * 0.95
        and resident_sorted[tail_index] >= primary_count * 0.90
    )
    scalar_pass = (
        scalar_mean >= primary_count * 0.95
        and scalar_sorted[tail_index] >= primary_count * 0.90
    )
    decision = (
        "promote-scalar4" if scalar_pass else
        "promote-r16" if resident_pass else
        "one-r32-screen" if resident_mean - scalar_mean >= 2 else
        "redesign-representation"
    )
    reduction = {
        "schema": "borsuk-v113-100k-score-reduction-v1",
        "evidence_sha256": _sha256_file(output),
        "query_count": query_count,
        "resident_mean_primary_overlap": resident_mean,
        "resident_p05_primary_overlap": resident_sorted[tail_index],
        "scalar_mean_primary_overlap": scalar_mean,
        "scalar_p05_primary_overlap": scalar_sorted[tail_index],
        "decision": decision,
        "returned_recall_measured": False,
    }
    summary.with_suffix(summary.suffix + ".tmp").write_text(
        json.dumps(reduction, sort_keys=True, separators=(",", ":")) + "\n"
    )
    summary.with_suffix(summary.suffix + ".tmp").replace(summary)


def main() -> None:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    build = subcommands.add_parser("build")
    build.add_argument("--source", type=Path, required=True)
    build.add_argument("--source-sha256", required=True)
    build.add_argument("--artifact", type=Path, required=True)
    score = subcommands.add_parser("score")
    score.add_argument("--artifact", type=Path, required=True)
    score.add_argument("--source-sha256", required=True)
    score.add_argument("--queries", type=Path, required=True)
    score.add_argument("--query-sha256", required=True)
    score.add_argument("--output", type=Path, required=True)
    score.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "build":
        build_from_source(args.source, args.source_sha256, args.artifact)
    else:
        score_from_artifact(
            args.artifact, args.source_sha256, args.queries,
            args.query_sha256, args.output, args.summary,
        )


if __name__ == "__main__":
    main()
