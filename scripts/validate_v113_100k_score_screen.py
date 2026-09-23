"""Independent full-query check of the V113 score-fidelity evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np

from scripts.v113_score_artifact import read_artifact


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _queries(path: Path, count: int, dimensions: int) -> np.ndarray:
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["embedding"])
    values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    result = np.asarray(values, dtype=np.float32).reshape(count, dimensions)
    if not np.isfinite(result).all():
        raise ValueError("validator query roster differs")
    return result


def _decode(
    books: np.ndarray, codes: np.ndarray, dimensions: int,
) -> np.ndarray:
    subspaces = codes.shape[1]
    width = books.shape[2]
    result = np.empty((codes.shape[0], dimensions), dtype=np.float64)
    for subspace in range(subspaces):
        lo = subspace * dimensions // subspaces
        hi = (subspace + 1) * dimensions // subspaces
        result[:, lo:hi] = books[subspace, codes[:, subspace], :hi - lo]
    return result


def _validate_source(source: Path, source_sha256: str, built: object) -> None:
    """Check SQ8 targets, scalar codes and full-generation residual bounds."""
    import pyarrow.parquet as pq

    if _hash(source) != source_sha256:
        raise ValueError("validator source identity differs")
    table = pq.read_table(source, columns=["feature_row_id", "embedding"])
    rows, dimensions = built.sq8_codes.shape
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    vectors = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    if ids.shape != (rows,) or vectors.size != rows * dimensions:
        raise ValueError("validator source geometry differs")
    vectors = np.asarray(vectors, dtype=np.float32).reshape(rows, dimensions)
    if not np.isfinite(vectors).all() or not np.array_equal(ids, built.ids):
        raise ValueError("validator source rows differ")
    low = vectors.min(axis=0).astype(np.float32)
    high = vectors.max(axis=0).astype(np.float32)
    span = np.maximum(high - low, np.float32(1e-12)).astype(np.float32)
    step = (span / np.float32(255.0)).astype(np.float32)
    if not np.array_equal(low, built.low) or not np.array_equal(step, built.span_step):
        raise ValueError("validator SQ8 scale differs")
    nu = np.empty(rows, dtype=np.float64)
    alpha_codes = np.empty(rows, dtype=np.int16)
    maxima = np.zeros((12, 256), dtype=np.float64)
    cast_max = 0.0
    for start in range(0, rows, 1024):
        stop = min(start + 1024, rows)
        block = vectors[start:stop]
        sq8 = np.clip(
            np.rint((block - low) / span * np.float32(255.0)), 0, 255,
        ).astype(np.uint8)
        if not np.array_equal(sq8, built.sq8_codes[start:stop]):
            raise ValueError("validator SQ8 row codes differ")
        dequantized = low + sq8.astype(np.float32) * step
        norm = np.einsum("ij,ij->i", dequantized, dequantized, dtype=np.float32)
        if not np.array_equal(norm, built.sq8_norm[start:stop]):
            raise ValueError("validator SQ8 norms differ")
        p = _decode(built.pq_books, built.pq_codes[start:stop], dimensions)
        target = low.astype(np.float64) + sq8.astype(np.float64) * step.astype(np.float64)
        error = target - p
        squared = np.sum(p * p, axis=1)
        nu[start:stop] = norm.astype(np.float64) - squared
        alpha = np.divide(
            np.sum(error * p, axis=1), squared,
            out=np.zeros_like(squared), where=squared > 0,
        )
        alpha = np.clip(alpha, -1.0, 1.0)
        encoded_alpha = np.rint(alpha / built.corrections.alpha_scale).astype(np.int16)
        alpha_codes[start:stop] = encoded_alpha
        residual = error - (
            encoded_alpha.astype(np.float64) * built.corrections.alpha_scale
        )[:, None] * p
        cast = residual.astype(np.float32)
        cast_error = residual - cast.astype(np.float64)
        cast_max = max(cast_max, float(np.max(np.sqrt(np.sum(cast_error * cast_error, axis=1)))))
        for subspace in range(12):
            lo = subspace * dimensions // 12
            hi = (subspace + 1) * dimensions // 12
            codes = built.residual_codes[start:stop, subspace]
            centroid = built.residual_books[subspace, codes, :hi - lo]
            difference = cast[:, lo:hi].astype(np.float64) - centroid.astype(np.float64)
            errors = np.sqrt(np.sum(difference * difference, axis=1))
            np.maximum.at(maxima[subspace], codes, errors)
    scale = float(np.max(np.abs(nu))) / 32767.0 if np.any(nu) else 1.0
    encoded_nu = np.rint(nu / scale).astype(np.int16)
    if (
        scale != built.corrections.nu_scale
        or not np.array_equal(encoded_nu, built.corrections.nu_codes)
        or not np.array_equal(alpha_codes, built.corrections.alpha_codes)
        or np.nextafter(cast_max, np.inf) != built.corrections.residual_cast_error_max
        or not np.array_equal(np.nextafter(maxima, np.inf), built.residual_maximum_error)
    ):
        raise ValueError("validator scalar codes or residual bounds differ")


def _independent_primary(
    query: np.ndarray, built: object,
    shortlist: int, primary_count: int,
) -> tuple[list[int], list[int], list[int], int, int, float, float, float, float, float]:
    dimensions = query.size
    row_codes = built.pq_codes
    score = np.zeros(row_codes.shape[0], dtype=np.float32)
    for subspace in range(64):
        lo = subspace * dimensions // 64
        hi = (subspace + 1) * dimensions // 64
        delta = built.pq_books[subspace, :, :hi - lo] - query[lo:hi]
        lookup = np.sum(delta * delta, axis=1, dtype=np.float32)
        score += lookup[row_codes[:, subspace]]
    nominated = np.lexsort((np.arange(score.size), score))[:shortlist]
    ids = built.ids[nominated]
    weights = query * built.span_step
    shift = float(query @ built.low - (query @ query) / 2.0)
    oracle = built.sq8_norm[nominated] - 2.0 * (
        built.sq8_codes[nominated].astype(np.float32) @ weights + shift
    )
    p = _decode(built.pq_books, row_codes[nominated], dimensions)
    r = _decode(built.residual_books, built.residual_codes[nominated], dimensions)
    q64 = query.astype(np.float64)
    qnorm = q64 @ q64
    pnorm = np.sum(p * p, axis=1)
    nu = built.corrections.nu_codes[nominated].astype(np.float64) * built.corrections.nu_scale
    alpha = (
        built.corrections.alpha_codes[nominated].astype(np.float64)
        * built.corrections.alpha_scale
    )
    scalar = qnorm + pnorm + nu - 2.0 * (1.0 + alpha) * (p @ q64)
    resident = scalar - 2.0 * (r @ q64)
    def ranked(values: np.ndarray) -> list[int]:
        return nominated[np.lexsort((ids, values))[:primary_count]].tolist()
    true_primary = ranked(oracle)
    resident_primary = ranked(resident)
    scalar_primary = ranked(scalar)
    true_set = set(true_primary)
    exact_sq8 = (
        built.low.astype(np.float64)
        + built.sq8_codes[nominated].astype(np.float64)
        * built.span_step.astype(np.float64)
    )
    mathematical = (
        qnorm + built.sq8_norm[nominated].astype(np.float64)
        - 2.0 * (exact_sq8 @ q64)
    )
    return (
        true_primary, resident_primary, scalar_primary,
        len(true_set.intersection(resident_primary)),
        len(true_set.intersection(scalar_primary)),
        float(np.max(np.abs(oracle.astype(np.float64) - mathematical))),
        float(np.mean(np.abs(resident - oracle.astype(np.float64)))),
        float(np.max(np.abs(resident - oracle.astype(np.float64)))),
        float(np.mean(np.abs(scalar - oracle.astype(np.float64)))),
        float(np.max(np.abs(scalar - oracle.astype(np.float64)))),
    )


def validate_evidence(
    artifact: Path, source: Path, source_sha256: str, queries: Path,
    query_sha256: str, evidence: Path, summary: Path,
    *, rows: int = 100_000, dimensions: int = 768,
    query_count: int = 1_000, shortlist: int = 512,
    primary_count: int = 100, sample_rows: int = 100_000,
    iterations: int = 10,
) -> dict[str, object]:
    """Recompute all primary sets and independently reduce the sealed result."""
    if _hash(queries) != query_sha256:
        raise ValueError("validator query identity differs")
    built = read_artifact(
        artifact, expected_source_sha256=source_sha256,
        expected_sample_rows=sample_rows, expected_iterations=iterations,
    )
    if built.ids.size != rows or built.low.size != dimensions:
        raise ValueError("validator artifact geometry differs")
    _validate_source(source, source_sha256, built)
    query_vectors = _queries(queries, query_count, dimensions)
    records = [json.loads(line) for line in evidence.read_text().splitlines()]
    if len(records) != query_count + 1:
        raise ValueError("validator query evidence count differs")
    header = records[0]
    if (
        header.get("source_sha256") != source_sha256
        or header.get("query_sha256") != query_sha256
        or header.get("artifact_seal_sha256") != _hash(artifact / "seal.json")
        or header.get("schema") != "borsuk-v113-100k-score-screen-v1"
        or header.get("nominees") != shortlist
        or header.get("primary_rows") != primary_count
        or header.get("ground_truth_used") is not False
        or header.get("physical_reads_measured") is not False
    ):
        raise ValueError("validator evidence header differs")
    resident_hits: list[int] = []
    scalar_hits: list[int] = []
    for ordinal, query in enumerate(query_vectors):
        expected = _independent_primary(query, built, shortlist, primary_count)
        record = records[ordinal + 1]
        if (
            record.get("query_ordinal") != ordinal
            or record.get("oracle_primary") != expected[0]
            or record.get("resident_primary") != expected[1]
            or record.get("scalar_primary") != expected[2]
            or record.get("resident_overlap") != expected[3]
            or record.get("scalar_overlap") != expected[4]
            or not math.isclose(
                record.get("oracle_fp_error_max", float("nan")),
                expected[5], rel_tol=1e-9, abs_tol=1e-6,
            )
            or any(
                not math.isclose(
                    record.get(name, float("nan")), expected[index],
                    rel_tol=1e-8, abs_tol=1e-6,
                ) for index, name in enumerate((
                    "resident_score_error_mean", "resident_score_error_max",
                    "scalar_score_error_mean", "scalar_score_error_max",
                ), start=6)
            )
        ):
            raise ValueError(f"validator query {ordinal} primary sets differ")
        resident_hits.append(expected[3])
        scalar_hits.append(expected[4])
    reduction = json.loads(summary.read_bytes())
    tail_index = math.ceil(query_count * 0.05) - 1
    resident_mean = sum(resident_hits) / query_count
    scalar_mean = sum(scalar_hits) / query_count
    resident_pass = resident_mean >= primary_count * 0.95 and sorted(resident_hits)[tail_index] >= primary_count * 0.90
    scalar_pass = scalar_mean >= primary_count * 0.95 and sorted(scalar_hits)[tail_index] >= primary_count * 0.90
    decision = (
        "promote-scalar4" if scalar_pass else
        "promote-r16" if resident_pass else
        "one-r32-screen" if resident_mean - scalar_mean >= 2 else
        "redesign-representation"
    )
    if (
        reduction.get("schema") != "borsuk-v113-100k-score-reduction-v1"
        or
        reduction.get("evidence_sha256") != _hash(evidence)
        or reduction.get("query_count") != query_count
        or reduction.get("resident_mean_primary_overlap") != resident_mean
        or reduction.get("scalar_mean_primary_overlap") != scalar_mean
        or reduction.get("resident_p05_primary_overlap") != sorted(resident_hits)[tail_index]
        or reduction.get("scalar_p05_primary_overlap") != sorted(scalar_hits)[tail_index]
        or reduction.get("decision") != decision
        or reduction.get("returned_recall_measured") is not False
    ):
        raise ValueError("validator reduction differs")
    return {"validated_queries": query_count, "evidence_sha256": _hash(evidence)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--query-sha256", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    result = validate_evidence(
        args.artifact, args.source, args.source_sha256, args.queries,
        args.query_sha256, args.evidence, args.summary,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
