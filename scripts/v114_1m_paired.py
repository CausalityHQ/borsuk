"""Geometry-driven nomination for the paired V114 million-row gate.

The frozen ReLAION inputs are bound by the runner. This module keeps the
selection rule independent of their row count, dimension and page geometry.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v109_range_plan import admit_ranked_pages
from scripts.v109_capped_reader_replay import SQ8_DTYPE, load_manifest
from scripts.v114_exact_local_100k import (
    _canonical, _sha256_file, compare_records, route_reference, score_reference,
)

FROZEN_MANIFEST_SHA256 = "131a4cd80dfee8ef4d0486b2043349e01b6c230d702f8d17ad1224e3fbc6a874"
FROZEN_SQ8_SHA256 = "2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b"
FROZEN_SOURCE_SHA256 = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"


def nominate_region_pq64(
    query: np.ndarray,
    summaries: np.ndarray,
    books: np.ndarray,
    codes: np.ndarray,
    *,
    page_rows: int,
    blocks_per_page: int,
    regions: int,
    shortlist: int,
) -> tuple[list[int], list[int], np.ndarray]:
    """Select PQ64 rows from nearest summary regions, with stable ties.

    Returns pages ranked by their best nominated row score, those pages in
    physical order, and nominated row ordinals in score order. The score
    arithmetic deliberately matches the frozen V111/V109 replay for D=768.
    """
    query = np.asarray(query)
    summaries = np.asarray(summaries)
    books = np.asarray(books)
    codes = np.asarray(codes)
    if query.ndim != 1 or query.dtype != np.float32 or query.size == 0:
        raise ValueError("query must be a nonempty float32 vector")
    if summaries.ndim != 2 or summaries.shape[1] != query.size or summaries.dtype != np.float32:
        raise ValueError("summary shape or dtype differs from query")
    if books.ndim != 3 or books.shape[:2] != (64, 256) or books.dtype != np.float32:
        raise ValueError("books must have shape (64, 256, width) and float32 dtype")
    if books.shape[2] * 64 < query.size or books.shape[2] * 64 - query.size >= 64:
        raise ValueError("PQ width does not cover the query exactly")
    if codes.ndim != 2 or codes.shape[1] != 64 or codes.dtype != np.uint8 or codes.shape[0] == 0:
        raise ValueError("codes must have shape (rows, 64) and uint8 dtype")
    if page_rows <= 0 or blocks_per_page <= 0:
        raise ValueError("page and summary block geometry must be positive")
    pages = (codes.shape[0] + page_rows - 1) // page_rows
    if summaries.shape[0] != pages * blocks_per_page:
        raise ValueError("summary count does not match page geometry")
    if not 1 <= regions <= pages or not 1 <= shortlist <= min(regions * page_rows, codes.shape[0]):
        raise ValueError("region or shortlist count is outside geometry")
    if not (np.isfinite(query).all() and np.isfinite(summaries).all() and np.isfinite(books).all()):
        raise ValueError("query, summaries and books must be finite")

    norms = np.einsum("ij,ij->i", summaries, summaries)
    page_scores = (norms - 2.0 * (summaries @ query)).reshape(pages, blocks_per_page).min(axis=1)
    chosen = np.lexsort((np.arange(pages), page_scores))[:regions]
    chosen.sort()
    rows = np.concatenate([
        np.arange(page * page_rows, min((page + 1) * page_rows, codes.shape[0]), dtype=np.int32)
        for page in chosen
    ])
    if shortlist > rows.size:
        raise ValueError("shortlist exceeds rows in selected regions")
    padded_query = np.zeros(books.shape[2] * 64, dtype=np.float32)
    padded_query[:query.size] = query
    delta = books - padded_query.reshape(64, 1, books.shape[2])
    table = np.einsum("ijk,ijk->ij", delta, delta)
    scores = np.zeros(rows.size, dtype=np.float32)
    for subspace in range(64):
        scores += table[subspace, codes[rows, subspace]]
    best = np.lexsort((rows, scores))[:shortlist]
    best_scores: dict[int, float] = {}
    for index in best:
        page = int(rows[index]) // page_rows
        best_scores[page] = min(best_scores.get(page, float("inf")), float(scores[index]))
    ranked = sorted(best_scores, key=lambda page: (best_scores[page], page))
    return ranked, sorted(best_scores), rows[best]


def prepare_paired_query(
    query_ordinal: int,
    query: np.ndarray,
    summaries: np.ndarray,
    books: np.ndarray,
    pq_codes: np.ndarray,
    sq8: np.ndarray,
    low: np.ndarray,
    step: np.ndarray,
    *,
    page_rows: int,
    blocks_per_page: int,
    regions: int,
    shortlist: int,
    primary_count: int,
) -> tuple[dict[str, object], dict[str, object]]:
    """Fix nomination, primary and both routes without ground truth."""
    if query_ordinal < 0 or page_rows != 256 or sq8.shape[0] != pq_codes.shape[0]:
        raise ValueError("paired query identity or physical page geometry differs")
    ranked, historical, nominees = nominate_region_pq64(
        query, summaries, books, pq_codes,
        page_rows=page_rows, blocks_per_page=blocks_per_page,
        regions=regions, shortlist=shortlist,
    )
    primary, scores = score_reference(
        query=query, nominees=nominees,
        ids=sq8["id"], norms=sq8["norm"], codes=sq8["code"],
        low=low, step=step, primary_count=primary_count,
    )
    votes, ranges, byte_count, plan_score = route_reference(
        primary, nominees.astype(int).tolist(),
        rows=int(sq8.shape[0]), dimensions=int(query.size),
    )
    baseline = admit_ranked_pages(
        ranked, rows=int(sq8.shape[0]), page_rows=page_rows,
        row_bytes=int(sq8.dtype.itemsize), max_gets=32,
        max_bytes=16_777_216,
    )
    request: dict[str, object] = {
        "query_ordinal": query_ordinal,
        "query": query.astype(float).tolist(),
        "nominees": nominees.astype(int).tolist(),
        "primary_count": primary_count,
    }
    reference: dict[str, object] = {
        "query_ordinal": query_ordinal,
        "generation": 1,
        "ranked_pages": ranked,
        "historical_pages": historical,
        "primary": primary,
        "score_bits": scores.view(np.uint32).astype(int).tolist(),
        "page_votes": votes,
        "ranges": ranges,
        "plan_bytes": byte_count,
        "plan_score": plan_score,
        "baseline_ranges": [list(pair) for pair in baseline.plan.ranges],
        "baseline_gets": baseline.plan.gets,
        "baseline_bytes": baseline.plan.bytes,
    }
    return request, reference


def score_sq8_ranges(
    sq8: np.ndarray,
    query: np.ndarray,
    low: np.ndarray,
    step: np.ndarray,
    ranges: list[list[int]] | tuple[tuple[int, int], ...],
    *, top_k: int,
) -> list[int]:
    """Return ordered IDs for already fixed physical ranges.

    This mirrors the V109 SQ8 returned-result arithmetic, including its
    float32 matrix product and final (score, ID) tie rule.
    """
    if (
        query.ndim != 1 or query.dtype != np.float32
        or low.shape != query.shape or low.dtype != np.float32
        or step.shape != query.shape or step.dtype != np.float32
        or top_k <= 0 or sq8.ndim != 1
        or sq8.dtype.names != ("id", "norm", "code")
        or sq8["code"].shape != (sq8.size, query.size)
    ):
        raise ValueError("SQ8 returned-scoring geometry differs")
    row_bytes = sq8.dtype.itemsize
    previous_end = 0
    identifiers: list[np.ndarray] = []
    distances: list[np.ndarray] = []
    weights = query * step
    shift = float(query @ low - (query @ query) / 2.0)
    for start, end in ranges:
        if (
            start < previous_end or start % row_bytes or end % row_bytes
            or start >= end or end > sq8.nbytes
        ):
            raise ValueError("SQ8 returned range differs")
        rows = sq8[start // row_bytes:end // row_bytes]
        identifiers.append(rows["id"])
        inner = rows["code"].astype(np.float32) @ weights
        distances.append(rows["norm"] - 2.0 * (inner + shift))
        previous_end = end
    if not identifiers:
        raise ValueError("empty SQ8 returned plan")
    ids = np.concatenate(identifiers)
    scores = np.concatenate(distances)
    selected = np.lexsort((ids, scores))[:top_k]
    return [int(value) for value in ids[selected]]


def reduce_paired_query(
    reference: dict[str, object],
    actual: dict[str, object],
    sq8: np.ndarray,
    query: np.ndarray,
    low: np.ndarray,
    step: np.ndarray,
    truth: object,
    *, top_k: int,
) -> dict[str, object]:
    """Require exact Rust parity before reading GT or reporting quality."""
    mismatches = compare_records(reference, actual)
    if mismatches:
        raise ValueError("Rust exact parity differs: " + ", ".join(mismatches))
    ranges = reference["ranges"]
    baseline_ranges = reference["baseline_ranges"]
    if (
        len(ranges) > 32 or reference["plan_bytes"] > 16_777_216
        or len(baseline_ranges) != reference["baseline_gets"]
        or len(baseline_ranges) > 32
        or reference["baseline_bytes"] > 16_777_216
        or sum(end - start for start, end in ranges) != reference["plan_bytes"]
        or sum(end - start for start, end in baseline_ranges) != reference["baseline_bytes"]
    ):
        raise ValueError("paired physical GET or byte cap differs")
    baseline_ids = score_sq8_ranges(
        sq8, query, low, step, baseline_ranges, top_k=top_k,
    )
    exact_ids = score_sq8_ranges(
        sq8, query, low, step, ranges, top_k=top_k,
    )
    production_ids = score_sq8_ranges(
        sq8, query, low, step, actual["ranges"], top_k=top_k,
    )
    if exact_ids != production_ids:
        raise ValueError("exact and production returned IDs differ")
    truth_ids = set(map(int, truth))
    return {
        "query_ordinal": reference["query_ordinal"],
        "baseline_returned_ids": baseline_ids,
        "exact_returned_ids": exact_ids,
        "production_returned_ids": production_ids,
        "baseline_hits": len(truth_ids.intersection(baseline_ids)),
        "exact_hits": len(truth_ids.intersection(exact_ids)),
        "production_hits": len(truth_ids.intersection(production_ids)),
        "baseline_gets": reference["baseline_gets"],
        "baseline_bytes": reference["baseline_bytes"],
        "production_gets": len(ranges),
        "production_bytes": reference["plan_bytes"],
    }


def load_frozen_1m(
    manifest_path: Path, mirror: Path, sq8_path: Path,
) -> tuple[dict[str, object], np.memmap]:
    """Authenticate the frozen 1M corpus and bind router to mirror scales."""
    if _sha256_file(manifest_path) != FROZEN_MANIFEST_SHA256:
        raise ValueError("frozen V77 manifest SHA-256 differs")
    if sq8_path.stat().st_size != 780_000_000 or _sha256_file(sq8_path) != FROZEN_SQ8_SHA256:
        raise ValueError("frozen V70 SQ8 SHA-256 differs")
    source = json.loads((mirror / "source.json").read_text())
    mirror_manifest = json.loads((mirror / "manifest.json").read_text())
    if (
        source.get("schema") != "borsuk-v114-existing-sq8-mirror-v1"
        or source.get("source_sha256") != FROZEN_SOURCE_SHA256
        or source.get("sq8_sha256") != FROZEN_SQ8_SHA256
        or source.get("manifest_sha256") != _sha256_file(mirror / "manifest.json")
        or mirror_manifest.get("object_sha256") != FROZEN_SQ8_SHA256
        or mirror_manifest.get("geometry") != {"rows": 1_000_000, "dimensions": 768}
        or mirror_manifest.get("generation") != 1
        or mirror_manifest.get("max_nominees") != 512
        or mirror_manifest.get("block_digest_sha256") != _sha256_file(mirror / "blocks.sha256")
    ):
        raise ValueError("frozen V114 SQ8 mirror binding differs")
    manifest = load_manifest(manifest_path)
    if (
        not np.array_equal(np.asarray(mirror_manifest["low"], np.float32), manifest["low"])
        or not np.array_equal(np.asarray(mirror_manifest["step"], np.float32), manifest["span_step"])
    ):
        raise ValueError("source-derived SQ8 scales differ from frozen V77")
    sq8 = np.memmap(sq8_path, dtype=SQ8_DTYPE, mode="r", shape=(1_000_000,))
    return manifest, sq8


def qualifies_1m(
    *, total_hits: dict[str, int], p05_hits: dict[str, int],
    sub90_queries: dict[str, int],
) -> bool:
    """Apply the preregistered paired 1M development promotion gate."""
    return (
        total_hits["baseline"] == 98_803
        and total_hits["production"] == total_hits["exact"]
        and total_hits["production"] >= 99_000
        and p05_hits["production"] >= max(
            90, p05_hits["baseline"], p05_hits["exact"] - 1,
        )
        and sub90_queries["production"] <= sub90_queries["baseline"]
    )


def gate_summary(summary: dict[str, object], query_count: int) -> None:
    """Stop after independent validation unless preregistered gates pass."""
    if summary.get("query_count") != query_count or query_count not in (200, 1000):
        raise ValueError("paired gate query count differs")
    if summary.get("baseline_reproduced") is not True:
        raise ValueError("same-run V109 capped baseline differs")
    if query_count == 1000 and summary.get("qualifies_live_s3") is not True:
        raise ValueError("V114 paired 1M quality gate did not qualify")


def prepare_frozen(
    manifest_path: Path, mirror: Path, sq8_path: Path,
    requests_path: Path, reference_path: Path, *, query_count: int,
) -> None:
    """Write Rust requests and reference plans with no GT access."""
    if query_count not in (200, 1000):
        raise ValueError("paired frozen query count differs")
    manifest, sq8 = load_frozen_1m(manifest_path, mirror, sq8_path)
    request_tmp = requests_path.with_suffix(requests_path.suffix + ".tmp")
    reference_tmp = reference_path.with_suffix(reference_path.suffix + ".tmp")
    with request_tmp.open("w") as requests, reference_tmp.open("w") as references:
        for ordinal in range(query_count):
            request, reference = prepare_paired_query(
                ordinal, manifest["queries"][ordinal],
                manifest["summaries"], manifest["books"], manifest["codes"],
                sq8, manifest["low"], manifest["span_step"],
                page_rows=256, blocks_per_page=2, regions=1024,
                shortlist=512, primary_count=100,
            )
            requests.write(_canonical(request))
            references.write(_canonical(reference))
    request_tmp.replace(requests_path)
    reference_tmp.replace(reference_path)


def reduce_frozen(
    manifest_path: Path, mirror: Path, sq8_path: Path,
    requests_path: Path, reference_path: Path, rust_path: Path,
    evidence_path: Path, summary_path: Path,
    *, query_count: int,
) -> None:
    """Check all exact parity, then produce paired returned-recall evidence."""
    if query_count not in (200, 1000):
        raise ValueError("paired frozen query count differs")
    manifest, sq8 = load_frozen_1m(manifest_path, mirror, sq8_path)
    reference_lines = reference_path.read_text().splitlines()
    request_lines = requests_path.read_text().splitlines()
    rust_lines = rust_path.read_text().splitlines()
    if any(len(lines) != query_count
           for lines in (request_lines, reference_lines, rust_lines)):
        raise ValueError("paired result count differs")
    references = [json.loads(line) for line in reference_lines]
    actuals = [json.loads(line) for line in rust_lines]
    for ordinal, (reference, actual) in enumerate(zip(references, actuals)):
        if reference.get("query_ordinal") != ordinal or actual.get("query_ordinal") != ordinal:
            raise ValueError("paired ordered query IDs differ")
        mismatches = compare_records(reference, actual)
        if mismatches:
            raise ValueError(f"paired Rust parity query {ordinal}: {mismatches}")
    totals = {"baseline": 0, "exact": 0, "production": 0}
    hits: dict[str, list[int]] = {arm: [] for arm in totals}
    maximum_gets = maximum_bytes = 0
    evidence_tmp = evidence_path.with_suffix(evidence_path.suffix + ".tmp")
    with evidence_tmp.open("w") as evidence:
        for ordinal, (reference, actual) in enumerate(zip(references, actuals)):
            result = reduce_paired_query(
                reference, actual, sq8, manifest["queries"][ordinal],
                manifest["low"], manifest["span_step"],
                manifest["truth"][ordinal], top_k=100,
            )
            for arm in totals:
                value = result[f"{arm}_hits"]
                totals[arm] += value
                hits[arm].append(value)
            maximum_gets = max(maximum_gets, result["production_gets"])
            maximum_bytes = max(maximum_bytes, result["production_bytes"])
            evidence.write(_canonical(result))
    evidence_tmp.replace(evidence_path)
    summary: dict[str, object] = {
        "schema": "borsuk-v114-1m-paired-v1",
        "dataset": "ReLAION-1M",
        "split": f"development-{query_count}",
        "query_count": query_count,
        "manifest_sha256": FROZEN_MANIFEST_SHA256,
        "sq8_sha256": FROZEN_SQ8_SHA256,
        "requests_sha256": _sha256_file(requests_path),
        "reference_sha256": _sha256_file(reference_path),
        "rust_sha256": _sha256_file(rust_path),
        "evidence_sha256": _sha256_file(evidence_path),
        "exact_parity_queries": query_count,
        "total_hits": totals,
        "p05_hits": {arm: int(np.percentile(values, 5, method="lower"))
                     for arm, values in hits.items()},
        "sub90_queries": {arm: sum(value < 90 for value in values)
                          for arm, values in hits.items()},
        "maximum_gets": maximum_gets,
        "maximum_bytes": maximum_bytes,
        "live_s3_measured": False,
    }
    expected_baseline = 19_739 if query_count == 200 else 98_803
    summary["baseline_reproduced"] = totals["baseline"] == expected_baseline
    if query_count == 1000:
        summary["qualifies_live_s3"] = qualifies_1m(
            total_hits=totals,
            p05_hits=summary["p05_hits"],
            sub90_queries=summary["sub90_queries"],
        )
    summary_path.write_text(_canonical(summary))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "reduce", "gate"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--mirror", type=Path)
    parser.add_argument("--sq8", type=Path)
    parser.add_argument("--queries", required=True, type=int, choices=(200, 1000))
    parser.add_argument("--requests", type=Path)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--rust", type=Path)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    if args.phase == "gate":
        if args.summary is None:
            parser.error("gate requires --summary")
        gate_summary(json.loads(args.summary.read_text()), args.queries)
        return
    if args.manifest is None or args.mirror is None or args.sq8 is None:
        parser.error("prepare and reduce require --manifest, --mirror and --sq8")
    if args.reference is None:
        parser.error("prepare and reduce require --reference")
    if args.phase == "prepare":
        if args.requests is None:
            parser.error("prepare requires --requests")
        prepare_frozen(args.manifest, args.mirror, args.sq8,
                       args.requests, args.reference, query_count=args.queries)
    else:
        if (args.requests is None or args.rust is None
                or args.evidence is None or args.summary is None):
            parser.error("reduce requires --requests, --rust, --evidence and --summary")
        reduce_frozen(args.manifest, args.mirror, args.sq8, args.requests,
                      args.reference, args.rust, args.evidence, args.summary,
                      query_count=args.queries)


if __name__ == "__main__":
    main()
