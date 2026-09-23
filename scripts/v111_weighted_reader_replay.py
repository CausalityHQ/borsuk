#!/usr/bin/env python3
"""Truth-free exact interval admission on V109's frozen PQ64 nomination.

This is an offline, read-free paired replay against the same V77 uncapped and
V109 capped controls. It never consults ground truth to select ranges.
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
from scripts.v109_range_plan import RangePlan, admit_ranked_pages, plan_page_ranges
from scripts.v111_weighted_interval_plan import optimal_weighted_intervals


def nominate_count_weights(
    query: np.ndarray, manifest: dict[str, object], *, regions: int, shortlist: int,
) -> tuple[list[int], list[int], dict[int, int]]:
    summaries = manifest["summaries"]
    books = manifest["books"]
    codes = manifest["codes"]
    pages = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS
    summary_norms = np.einsum("ij,ij->i", summaries, summaries)
    page_scores = (summary_norms - 2.0 * (summaries @ query)).reshape(pages, 2).min(axis=1)
    chosen = np.lexsort((np.arange(pages), page_scores))[:regions]
    chosen.sort()
    rows = np.concatenate([
        np.arange(page * PAGE_ROWS, min((page + 1) * PAGE_ROWS, ROWS), dtype=np.int32)
        for page in chosen
    ])
    delta = books - query.reshape(64, 1, 12)
    table = np.einsum("ijk,ijk->ij", delta, delta)
    scores = np.zeros(rows.size, dtype=np.float32)
    for subspace in range(64):
        scores += table[subspace, codes[rows, subspace]]
    best = np.lexsort((rows, scores))[:shortlist]
    best_scores: dict[int, float] = {}
    counts: dict[int, int] = {}
    for index in best:
        page = int(rows[index]) // PAGE_ROWS
        best_scores[page] = min(best_scores.get(page, float("inf")), float(scores[index]))
        counts[page] = counts.get(page, 0) + 1
    ranked = sorted(best_scores, key=lambda page: (best_scores[page], page))
    return ranked, sorted(best_scores), counts


def weighted_plan(weights: dict[int, int]) -> tuple[int, RangePlan]:
    page_count = (ROWS + PAGE_ROWS - 1) // PAGE_ROWS
    unit_rows = 64
    unit_bytes = unit_rows * ROW_BYTES
    score, intervals = optimal_weighted_intervals(
        weights, page_count=page_count, max_gets=MAX_GETS,
        max_units=MAX_BYTES // unit_bytes,
        full_page_units=PAGE_ROWS // unit_rows,
        last_page_units=(ROWS - (page_count - 1) * PAGE_ROWS) // unit_rows,
    )
    ranges = tuple((first * PAGE_ROWS * ROW_BYTES,
                    min((last + 1) * PAGE_ROWS, ROWS) * ROW_BYTES)
                   for first, last in intervals)
    plan = RangePlan(ranges, len(ranges), sum(end - start for start, end in ranges))
    if not plan.within(gets=MAX_GETS, bytes_limit=MAX_BYTES):
        raise AssertionError("weighted plan exceeded physical cap")
    return score, plan


def replay(manifest_path: Path, sq8_path: Path, output_path: Path, *,
           query_count: int, regions: int, shortlist: int) -> None:
    if not 1 <= query_count <= 1000 or regions != 1024 or shortlist != 512:
        raise ValueError("V111 operating point differs")
    if sq8_path.stat().st_size != ROWS * ROW_BYTES or sha256_file(sq8_path) != SQ8_SHA256:
        raise ValueError("SQ8 object identity differs")
    manifest = load_manifest(manifest_path)
    sq8 = np.memmap(sq8_path, dtype=SQ8_DTYPE, mode="r", shape=(ROWS,))
    header = {"schema": "borsuk-v111-weighted-replay-v1",
              "manifest_sha256": sha256_file(manifest_path),
              "sq8_sha256": SQ8_SHA256, "query_count": query_count,
              "regions": regions, "shortlist": shortlist,
              "max_gets": MAX_GETS, "max_bytes": MAX_BYTES,
              "weight": "top512-pq64-row-count-per-page",
              "data_reads": "local-authenticated-object-no-query-S3-GET"}
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    with temporary.open("w") as handle:
        handle.write(json.dumps(header, sort_keys=True, separators=(",", ":")) + "\n")
        for ordinal in range(query_count):
            query = manifest["queries"][ordinal]
            ranked, historical, weights = nominate_count_weights(
                query, manifest, regions=regions, shortlist=shortlist,
            )
            independent_ranked, independent_historical = nominate(
                query, manifest, regions=regions, shortlist=shortlist,
            )
            if ranked != independent_ranked or historical != independent_historical:
                raise AssertionError("V109 PQ64 nomination differs")
            old = plan_page_ranges(historical, gap_pages=2, rows=ROWS,
                                   page_rows=PAGE_ROWS, row_bytes=ROW_BYTES)
            capped = admit_ranked_pages(
                ranked, rows=ROWS, page_rows=PAGE_ROWS, row_bytes=ROW_BYTES,
                max_gets=MAX_GETS, max_bytes=MAX_BYTES,
            )
            weighted_score, weighted = weighted_plan(weights)
            truth = set(map(int, manifest["truth"][ordinal]))
            old_ids = score_ranges(sq8, query, manifest, old.ranges)
            capped_ids = score_ranges(sq8, query, manifest, capped.plan.ranges)
            weighted_ids = score_ranges(sq8, query, manifest, weighted.ranges)
            record = {"query_ordinal": ordinal,
                      "ranked_pages": ranked, "historical_pages": historical,
                      "page_weights": weights, "weighted_objective": weighted_score,
                      "weighted_ranges": weighted.ranges,
                      "weighted_gets": weighted.gets, "weighted_bytes": weighted.bytes,
                      "weighted_returned_ids": weighted_ids,
                      "weighted_hits": len(truth.intersection(weighted_ids)),
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
