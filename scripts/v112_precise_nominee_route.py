#!/usr/bin/env python3
"""Offline SQ8-score ceiling for V77's fixed PQ64 top-512 nomination.

This diagnostic reads precise scores locally for nominated rows before it
plans ranges. Doing so from S3 would add a query wave, so it is not a
deployable one-wave route. Ground truth is used only after routing.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v109_capped_reader_replay import (
    MAX_BYTES, MAX_GETS, PAGE_ROWS, ROW_BYTES, ROWS, SQ8_DTYPE, SQ8_SHA256,
    load_manifest, nominate, score_ranges, sha256_file,
)
from scripts.v109_range_plan import admit_ranked_pages, plan_page_ranges
from scripts.v111_weighted_reader_replay import nominate_rows, weighted_plan

PRIMARY_FACTOR = 513
PRECISE_ROWS = 100


def precise_nominee_scores(
    sq8: np.memmap, rows: np.ndarray, query: np.ndarray,
    manifest: dict[str, object],
) -> tuple[np.ndarray, np.ndarray]:
    record = sq8[rows]
    weights = query * manifest["span_step"]
    shift = float(query @ manifest["low"] - (query @ query) / 2.0)
    inner = record["code"].astype(np.float32) @ weights
    scores = record["norm"] - 2.0 * (inner + shift)
    if not np.isfinite(scores).all():
        raise ValueError("SQ8 nominated score differs")
    return record["id"], scores


def precise_page_weights(
    rows: np.ndarray, identifiers: np.ndarray, scores: np.ndarray,
) -> dict[int, int]:
    if (rows.shape != (512,) or identifiers.shape != (512,) or scores.shape != (512,)
        or len(np.unique(rows)) != 512 or len(np.unique(identifiers)) != 512
        or not np.isfinite(scores).all()):
        raise ValueError("precise nominee roster differs")
    selected = np.lexsort((identifiers, scores))[:PRECISE_ROWS]
    primary = set(map(int, selected))
    primary_counts: dict[int, int] = {}
    secondary_counts: dict[int, int] = {}
    for ordinal, row in enumerate(rows):
        page = int(row) // PAGE_ROWS
        counts = primary_counts if ordinal in primary else secondary_counts
        counts[page] = counts.get(page, 0) + 1
    pages = primary_counts.keys() | secondary_counts.keys()
    weights = {page: PRIMARY_FACTOR * primary_counts.get(page, 0)
               + secondary_counts.get(page, 0) for page in pages}
    if (sum(primary_counts.values()) != PRECISE_ROWS
        or sum(secondary_counts.values()) != 512 - PRECISE_ROWS):
        raise AssertionError("precise vote accounting differs")
    return weights


def replay(manifest_path: Path, sq8_path: Path, output_path: Path, *,
           query_count: int, regions: int, shortlist: int) -> None:
    if not 1 <= query_count <= 1000 or regions != 1024 or shortlist != 512:
        raise ValueError("V112 operating point differs")
    if sq8_path.stat().st_size != ROWS * ROW_BYTES or sha256_file(sq8_path) != SQ8_SHA256:
        raise ValueError("SQ8 object identity differs")
    manifest = load_manifest(manifest_path)
    sq8 = np.memmap(sq8_path, dtype=SQ8_DTYPE, mode="r", shape=(ROWS,))
    header = {"schema": "borsuk-v112-precise-nominee-replay-v1",
              "manifest_sha256": sha256_file(manifest_path),
              "sq8_sha256": SQ8_SHA256, "query_count": query_count,
              "regions": regions, "shortlist": shortlist,
              "max_gets": MAX_GETS, "max_bytes": MAX_BYTES,
              "primary_rows": PRECISE_ROWS, "primary_factor": PRIMARY_FACTOR,
              "weight": "local-SQ8-top100-of-PQ64-top512-plus-secondary",
              "data_reads": "local-authenticated-object-no-query-S3-GET",
              "serving_eligible": False}
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    with temporary.open("w") as handle:
        handle.write(json.dumps(header, sort_keys=True, separators=(",", ":")) + "\n")
        for ordinal in range(query_count):
            query = manifest["queries"][ordinal]
            ranked, historical, _, nominated_rows = nominate_rows(
                query, manifest, regions=regions, shortlist=shortlist,
            )
            independent_ranked, independent_historical = nominate(
                query, manifest, regions=regions, shortlist=shortlist,
            )
            if ranked != independent_ranked or historical != independent_historical:
                raise AssertionError("V109 PQ64 nomination differs")
            ids, scores = precise_nominee_scores(sq8, nominated_rows, query, manifest)
            weights = precise_page_weights(nominated_rows, ids, scores)
            objective, precise = weighted_plan(weights)
            old = plan_page_ranges(historical, gap_pages=2, rows=ROWS,
                                   page_rows=PAGE_ROWS, row_bytes=ROW_BYTES)
            capped = admit_ranked_pages(
                ranked, rows=ROWS, page_rows=PAGE_ROWS, row_bytes=ROW_BYTES,
                max_gets=MAX_GETS, max_bytes=MAX_BYTES,
            )
            truth = set(map(int, manifest["truth"][ordinal]))
            old_ids = score_ranges(sq8, query, manifest, old.ranges)
            capped_ids = score_ranges(sq8, query, manifest, capped.plan.ranges)
            precise_ids = score_ranges(sq8, query, manifest, precise.ranges)
            record = {"query_ordinal": ordinal,
                      "ranked_pages": ranked, "historical_pages": historical,
                      "page_weights": weights, "precise_objective": objective,
                      "precise_ranges": precise.ranges,
                      "precise_gets": precise.gets, "precise_bytes": precise.bytes,
                      "precise_returned_ids": precise_ids,
                      "precise_hits": len(truth.intersection(precise_ids)),
                      "historical_gets": old.gets, "historical_bytes": old.bytes,
                      "historical_returned_ids": old_ids,
                      "historical_hits": len(truth.intersection(old_ids)),
                      "capped_gets": capped.plan.gets,
                      "capped_bytes": capped.plan.bytes,
                      "capped_nominated_pages": capped.nominated_pages,
                      "capped_rejected_pages": capped.rejected_pages,
                      "capped_returned_ids": capped_ids,
                      "capped_hits": len(truth.intersection(capped_ids))}
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(output_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--sq8", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--queries", type=int, required=True)
    parser.add_argument("--regions", type=int, required=True)
    args = parser.parse_args()
    replay(args.manifest, args.sq8, args.output,
           query_count=args.queries, regions=args.regions, shortlist=512)


if __name__ == "__main__":
    main()
