"""Independent source and query validation for the frozen V114 1M cell."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from scripts.launch_v112_precise_nominee_spot import INPUTS
from scripts.v109_capped_reader_replay import score_ranges
from scripts.v109_range_plan import admit_ranked_pages
from scripts.v111_weighted_reader_replay import nominate_rows
from scripts.v114_1m_mirror import seal_existing_sq8
from scripts.v114_1m_paired import load_frozen_1m, qualifies_1m
from scripts.v114_exact_local_100k import (
    _canonical, _sha256_file, compare_records, route_reference,
)
from scripts.v77_export_manifest import fixed_list


def validate_block_sidecar(object_path: Path, sidecar_path: Path, object_bytes: int) -> None:
    """Check each 4-KiB object block against its independently read digest."""
    blocks = (object_bytes + 4095) // 4096
    if object_path.stat().st_size != object_bytes or sidecar_path.stat().st_size != blocks * 32:
        raise ValueError("SQ8 block sidecar length differs")
    with object_path.open("rb") as object_file, sidecar_path.open("rb") as sidecar:
        for ordinal in range(blocks):
            block = object_file.read(4096)
            expected = sidecar.read(32)
            if hashlib.sha256(block).digest() != expected:
                raise ValueError(f"SQ8 block digest differs at {ordinal}")
        if object_file.read(1) or sidecar.read(1):
            raise ValueError("SQ8 block sidecar has trailing bytes")


def _independent_nominee_scores(
    sq8: np.ndarray, nominees: np.ndarray, query: np.ndarray,
    low: np.ndarray, step: np.ndarray,
) -> tuple[list[int], list[int]]:
    """Recompute Rust f32 coordinate order without using preparation code."""
    shift = np.float32(0)
    qnorm = np.float32(0)
    for coordinate in range(query.size):
        shift += query[coordinate] * low[coordinate]
        qnorm += query[coordinate] * query[coordinate]
    shift -= qnorm / np.float32(2)
    weights = query * step
    inner = np.zeros(nominees.size, np.float32)
    for coordinate in range(query.size):
        inner += sq8["code"][nominees, coordinate].astype(np.float32) * weights[coordinate]
    scores = sq8["norm"][nominees] - np.float32(2) * (inner + shift)
    order = np.lexsort((sq8["id"][nominees], scores))[:100]
    return (nominees[order].astype(int).tolist(),
            scores.view(np.uint32).astype(int).tolist())


def validate(
    *, source: Path, queries: Path, truth: Path, layout: Path,
    sq8_path: Path, manifest_path: Path, mirror: Path,
    requests_path: Path, reference_path: Path, rust_path: Path,
    evidence_path: Path, summary_path: Path, output_path: Path,
    query_count: int,
) -> None:
    if query_count not in (200, 1000):
        raise ValueError("V114 frozen 1M query count differs")
    for role, path in (("SOURCE", source), ("QUERIES", queries),
                       ("TRUTH", truth), ("LAYOUT", layout), ("SQ8", sq8_path)):
        identity = INPUTS[role]
        if path.stat().st_size != identity.bytes or _sha256_file(path) != identity.sha256:
            raise ValueError(f"frozen {role} identity differs")
    manifest, sq8 = load_frozen_1m(manifest_path, mirror, sq8_path)
    query_vectors = fixed_list(pq.read_table(queries), "embedding", 768, 1000)
    truth_ids = np.asarray(
        pq.read_table(truth, columns=["feature_row_id"])["feature_row_id"]
        .combine_chunks().to_numpy(zero_copy_only=False), dtype=np.int64,
    ).reshape(1000, 100)
    if (not np.array_equal(query_vectors, manifest["queries"])
            or not np.array_equal(truth_ids, manifest["truth"])):
        raise ValueError("frozen query or GT manifest binding differs")
    validate_block_sidecar(sq8_path, mirror / "blocks.sha256", sq8.nbytes)
    with tempfile.TemporaryDirectory(prefix="v114-1m-independent-") as temporary:
        rebuilt = Path(temporary) / "mirror"
        seal_existing_sq8(
            source, sq8_path, rebuilt,
            expected_source_sha256=INPUTS["SOURCE"].sha256,
            expected_sq8_sha256=INPUTS["SQ8"].sha256,
            rows=1_000_000, dimensions=768, max_nominees=512,
        )
        for name in ("source.json", "manifest.json", "blocks.sha256"):
            if _sha256_file(rebuilt / name) != _sha256_file(mirror / name):
                raise ValueError(f"source-rebuilt mirror {name} differs")
    files = [requests_path, reference_path, rust_path, evidence_path]
    records = [[json.loads(line) for line in path.read_text().splitlines()] for path in files]
    if any(len(series) != query_count for series in records):
        raise ValueError("V114 paired evidence query count differs")
    requests, references, rust, evidence = records
    totals = {arm: 0 for arm in ("baseline", "exact", "production")}
    hits = {arm: [] for arm in totals}
    maximum_gets = maximum_bytes = 0
    for ordinal, (request, reference, actual, result) in enumerate(
        zip(requests, references, rust, evidence)
    ):
        if any(row.get("query_ordinal") != ordinal
               for row in (request, reference, actual, result)):
            raise ValueError(f"V114 ordered query ID differs at {ordinal}")
        if compare_records(reference, actual):
            raise ValueError(f"V114 Rust exact parity differs at {ordinal}")
        query = manifest["queries"][ordinal]
        if request["query"] != query.astype(float).tolist() or request["primary_count"] != 100:
            raise ValueError(f"V114 Rust query request differs at {ordinal}")
        ranked, historical, _, nominees = nominate_rows(
            query, manifest, regions=1024, shortlist=512,
        )
        if (request["nominees"] != nominees.astype(int).tolist()
                or reference["ranked_pages"] != ranked
                or reference["historical_pages"] != historical):
            raise ValueError(f"V114 independent PQ64 nomination differs at {ordinal}")
        primary, score_bits = _independent_nominee_scores(
            sq8, nominees, query, manifest["low"], manifest["span_step"],
        )
        votes, ranges, byte_count, plan_score = route_reference(
            primary, request["nominees"], rows=1_000_000, dimensions=768,
        )
        if (
            reference["primary"] != primary or reference["score_bits"] != score_bits
            or reference["page_votes"] != votes or reference["ranges"] != ranges
            or reference["plan_bytes"] != byte_count
            or reference["plan_score"] != plan_score
        ):
            raise ValueError(f"V114 independent exact plan differs at {ordinal}")
        baseline = admit_ranked_pages(
            ranked, rows=1_000_000, page_rows=256, row_bytes=780,
            max_gets=32, max_bytes=16_777_216,
        )
        if (
            reference["baseline_ranges"] != [list(pair) for pair in baseline.plan.ranges]
            or reference["baseline_gets"] != baseline.plan.gets
            or reference["baseline_bytes"] != baseline.plan.bytes
        ):
            raise ValueError(f"V114 independent V109 plan differs at {ordinal}")
        baseline_ids = score_ranges(sq8, query, manifest, baseline.plan.ranges)
        exact_ids = score_ranges(
            sq8, query, manifest, tuple(tuple(pair) for pair in ranges),
        )
        if (
            result["baseline_returned_ids"] != baseline_ids
            or result["exact_returned_ids"] != exact_ids
            or result["production_returned_ids"] != exact_ids
        ):
            raise ValueError(f"V114 independent returned IDs differ at {ordinal}")
        ground_truth = set(map(int, manifest["truth"][ordinal]))
        for arm, identifiers in (("baseline", baseline_ids), ("exact", exact_ids),
                                 ("production", exact_ids)):
            value = len(ground_truth.intersection(identifiers))
            if result[f"{arm}_hits"] != value:
                raise ValueError(f"V114 independent {arm} hits differ at {ordinal}")
            totals[arm] += value
            hits[arm].append(value)
        if (
            result["baseline_gets"] != baseline.plan.gets
            or result["baseline_bytes"] != baseline.plan.bytes
            or result["production_gets"] != len(ranges)
            or result["production_bytes"] != byte_count
            or len(ranges) > 32 or byte_count > 16_777_216
        ):
            raise ValueError(f"V114 physical cap differs at {ordinal}")
        maximum_gets = max(maximum_gets, len(ranges))
        maximum_bytes = max(maximum_bytes, byte_count)
    summary = json.loads(summary_path.read_text())
    expected_p05 = {arm: int(np.percentile(values, 5, method="lower"))
                    for arm, values in hits.items()}
    expected_sub90 = {arm: sum(value < 90 for value in values)
                      for arm, values in hits.items()}
    if (
        summary.get("query_count") != query_count
        or summary.get("manifest_sha256") != _sha256_file(manifest_path)
        or summary.get("sq8_sha256") != INPUTS["SQ8"].sha256
        or summary.get("exact_parity_queries") != query_count
        or summary.get("requests_sha256") != _sha256_file(requests_path)
        or summary.get("total_hits") != totals
        or summary.get("baseline_reproduced") != (
            totals["baseline"] == (19_739 if query_count == 200 else 98_803))
        or summary.get("p05_hits") != expected_p05
        or summary.get("sub90_queries") != expected_sub90
        or summary.get("maximum_gets") != maximum_gets
        or summary.get("maximum_bytes") != maximum_bytes
        or summary.get("reference_sha256") != _sha256_file(reference_path)
        or summary.get("rust_sha256") != _sha256_file(rust_path)
        or summary.get("evidence_sha256") != _sha256_file(evidence_path)
        or (query_count == 1000 and summary.get("qualifies_live_s3") != qualifies_1m(
            total_hits=totals, p05_hits=expected_p05, sub90_queries=expected_sub90))
    ):
        raise ValueError("V114 independent paired reduction differs")
    output_path.write_text(_canonical({
        "schema": "borsuk-v114-1m-independent-validation-v1",
        "query_count": query_count,
        "summary_sha256": _sha256_file(summary_path),
        "source_sha256": INPUTS["SOURCE"].sha256,
        "sq8_sha256": INPUTS["SQ8"].sha256,
        "exact_query_matches": query_count,
        "validated": True,
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("source", "queries", "truth", "layout", "sq8", "manifest",
                 "mirror", "requests", "reference", "rust", "evidence",
                 "summary", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--query-count", required=True, type=int)
    args = parser.parse_args()
    validate(
        source=args.source, queries=args.queries, truth=args.truth,
        layout=args.layout, sq8_path=args.sq8, manifest_path=args.manifest,
        mirror=args.mirror, requests_path=args.requests,
        reference_path=args.reference, rust_path=args.rust,
        evidence_path=args.evidence, summary_path=args.summary,
        output_path=args.output, query_count=args.query_count,
    )


if __name__ == "__main__":
    main()
