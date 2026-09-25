#!/usr/bin/env python3
"""Frozen source-only resident FP16 nomination diagnostic for Deep-Image."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np

from scripts.v206_deep_image_fp16 import (
    DIMENSIONS, QUERY_COUNT, ROWS, canonical, records, sha256, source_vectors,
)

SCHEMA = "borsuk-v207-resident-nominee-fp16-v1"
PLANE_BYTES = ROWS * DIMENSIONS * 2


def rank(ids: np.ndarray, vectors: np.ndarray, query: np.ndarray) -> list[int]:
    if (ids.shape != (512,) or np.unique(ids).size != 512
            or np.any(ids < 0) or np.any(ids >= ROWS)
            or query.shape != (DIMENSIONS,) or not np.isfinite(query).all()):
        raise ValueError("resident nominee geometry differs")
    payload = vectors[ids].astype(np.float64)
    query64 = query.astype(np.float64)
    query_norm = np.linalg.norm(query64)
    norms = np.linalg.norm(payload, axis=1)
    if (not np.isfinite(payload).all() or not np.isfinite(norms).all()
            or np.any(norms <= 0) or not np.isfinite(query_norm) or query_norm <= 0):
        raise ValueError("resident nominee vector differs")
    scores = (payload @ (query64 / query_norm)) / norms
    order = np.lexsort((ids, -scores))[:100]
    return [int(item) for item in ids[order]]


def returned(args: argparse.Namespace) -> None:
    if any(path.exists() for path in (args.plane, args.raw, args.seal)):
        raise ValueError("V207 return artifact exists")
    layout = np.load(args.layout, mmap_mode="r", allow_pickle=False)
    if (layout.shape != (ROWS,) or layout.dtype != np.dtype("int64")
            or not np.array_equal(np.sort(layout), np.arange(ROWS, dtype=np.int64))):
        raise ValueError("V207 physical layout differs")
    source = source_vectors(args.source)
    plane_build = np.memmap(args.plane, dtype="<f2", mode="w+",
                            shape=(ROWS, DIMENSIONS))
    for first in range(0, ROWS, 16_384):
        last = min(first + 16_384, ROWS)
        plane_build[first:last] = source[first:last].astype(np.float16)
    plane_build.flush()
    del plane_build
    if args.plane.stat().st_size != PLANE_BYTES:
        raise ValueError("V207 FP16 plane length differs")
    plane_sha = sha256(args.plane)
    plane = np.fromfile(args.plane, dtype="<f2").reshape(ROWS, DIMENSIONS)
    if (plane.nbytes != PLANE_BYTES or
            not np.array_equal(plane[:128], source[:128].astype(np.float16))):
        raise ValueError("V207 resident FP16 payload differs")
    queries, rosters, prior = (records(path) for path in
                                (args.queries, args.rosters, args.prior))
    if any(len(rows) != QUERY_COUNT for rows in (queries, rosters, prior)):
        raise ValueError("V207 paired query roster differs")
    with args.raw.open("x") as output:
        for ordinal, (request, roster, old) in enumerate(
                zip(queries, rosters, prior, strict=True)):
            if (request.get("query_ordinal") != ordinal
                    or roster.get("query_ordinal") != ordinal
                    or old.get("ordinal") != ordinal
                    or len(roster.get("nominees", [])) != 512):
                raise ValueError(f"V207 query identity differs: {ordinal}")
            physical = np.asarray(roster["nominees"], dtype=np.int64)
            if (np.unique(physical).size != 512 or np.any(physical < 0)
                    or np.any(physical >= ROWS)):
                raise ValueError(f"V207 physical nominees differ: {ordinal}")
            ids = np.asarray(layout[physical], dtype=np.int64)
            query = np.asarray(request["query"], dtype=np.float32)
            started = time.perf_counter_ns()
            fp16_ids = rank(ids, plane, query)
            fp16_ns = time.perf_counter_ns() - started
            f32_ids = rank(ids, source, query)
            if (len(old["fp16_ids"]) != 100
                    or len(set(old["fp16_ids"])) != 100):
                raise ValueError(f"V207 V206 baseline differs: {ordinal}")
            output.write(canonical({"ordinal": ordinal,
                                    "nominee_ids": ids.tolist(),
                                    "fp16_ids": fp16_ids, "f32_ids": f32_ids,
                                    "prior_fp16_ids": old["fp16_ids"],
                                    "fp16_score_ns": fp16_ns}))
    args.seal.write_text(canonical({
        "schema": SCHEMA + "-pretruth-seal",
        "source_truth_opened": False,
        "dataset": "deep-image-96-angular",
        "split": "publication-test-first-1000-already-used",
        "source_sha256": sha256(args.source),
        "layout_sha256": sha256(args.layout),
        "queries_sha256": sha256(args.queries),
        "rosters_sha256": sha256(args.rosters),
        "prior_sha256": sha256(args.prior),
        "plane_sha256": plane_sha, "plane_bytes": PLANE_BYTES,
        "raw_sha256": sha256(args.raw),
    }))


def score(args: argparse.Namespace) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if args.evidence.exists() or args.summary.exists():
        raise ValueError("V207 score artifact exists")
    seal = json.loads(args.seal.read_text())
    if (seal.get("schema") != SCHEMA + "-pretruth-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("raw_sha256") != sha256(args.raw)
            or seal.get("plane_bytes") != PLANE_BYTES):
        raise ValueError("V207 pretruth seal differs")
    rows = records(args.raw)
    table = pq.read_table(args.truth, columns=["neighbors_id"])
    kind = table.schema.field("neighbors_id").type
    if (table.num_rows < QUERY_COUNT or not (pa.types.is_list(kind)
                                           or pa.types.is_fixed_size_list(kind))
            or len(rows) != QUERY_COUNT):
        raise ValueError("V207 truth or return roster differs")
    truth = table["neighbors_id"].slice(0, QUERY_COUNT).to_pylist()
    hits = {arm: [] for arm in ("fp16", "f32", "prior_fp16")}
    times = []
    with args.evidence.open("x") as output:
        for ordinal, (row, neighbors) in enumerate(zip(rows, truth, strict=True)):
            if (row["ordinal"] != ordinal or len(neighbors) < 100
                    or len(set(neighbors[:100])) != 100
                    or len(row["nominee_ids"]) != 512
                    or len(set(row["nominee_ids"])) != 512):
                raise ValueError(f"V207 paired truth differs: {ordinal}")
            gold = set(neighbors[:100])
            record = {"ordinal": ordinal}
            for arm in hits:
                ids = row[f"{arm}_ids"]
                if (len(ids) != 100 or len(set(ids)) != 100
                        or (arm != "prior_fp16" and
                            not set(ids).issubset(row["nominee_ids"]))):
                    raise ValueError(f"V207 returned IDs differ: {ordinal} {arm}")
                count = len(gold.intersection(ids))
                hits[arm].append(count)
                record[f"{arm}_hits"] = count
            times.append(row["fp16_score_ns"])
            output.write(canonical(record))
    totals = {arm: sum(values) for arm, values in hits.items()}
    p05 = {arm: sorted(values)[49] for arm, values in hits.items()}
    sub90 = {arm: sum(value < 90 for value in values)
             for arm, values in hits.items()}
    fp16, prior = hits["fp16"], hits["prior_fp16"]
    ordered = sorted(times)
    verdict = (totals["fp16"] >= 99_900 and p05["fp16"] >= 99
               and sub90["fp16"] == 0 and totals["fp16"] > totals["prior_fp16"]
               and totals["prior_fp16"] == 99_547)
    args.summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "deep-image-96-angular",
        "split": "publication-test-first-1000-already-used",
        "queries": QUERY_COUNT, "hits": totals, "p05_hits": p05,
        "below_90": sub90,
        "fp16_vs_prior_wins": sum(a > b for a, b in zip(fp16, prior, strict=True)),
        "fp16_vs_prior_ties": sum(a == b for a, b in zip(fp16, prior, strict=True)),
        "fp16_vs_prior_losses": sum(a < b for a, b in zip(fp16, prior, strict=True)),
        "fp16_score_ns_p50": ordered[499],
        "fp16_score_ns_p95": ordered[949],
        "fp16_score_ns_p99": ordered[989],
        "plane_bytes": PLANE_BYTES,
        "raw_sha256": sha256(args.raw), "truth_sha256": sha256(args.truth),
        "passes_resident_screen": verdict,
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("return", "score"))
    for name in ("source", "layout", "queries", "rosters", "prior",
                 "plane", "raw", "seal", "truth", "evidence", "summary"):
        parser.add_argument(f"--{name}", type=Path)
    args = parser.parse_args()
    if args.phase == "return":
        returned(args)
    else:
        score(args)


if __name__ == "__main__":
    main()
