#!/usr/bin/env python3
"""Recompute and reduce every V110 physical oracle cell from frozen inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v110_physical_interval_oracle import (
    FULL_PAGE_UNITS, LAST_PAGE_UNITS, MAX_BYTES, MAX_GETS, MAX_UNITS,
    ORDER_SHA256, PAGE_COUNT, SOURCE_SHA256, TRUTH_SHA256,
    optimal_truth_hits, sha256_file, truth_pages,
)


def nearest_rank(values: list[int], numerator: int, denominator: int) -> int:
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1,
                              (len(ordered) * numerator - 1) // denominator))]


def reduce_oracle(evidence: Path, source: Path, truth: Path,
                  layout: Path) -> dict[str, object]:
    for path, digest in ((source, SOURCE_SHA256), (truth, TRUTH_SHA256),
                         (layout, ORDER_SHA256)):
        if sha256_file(path) != digest:
            raise ValueError("oracle input identity differs")
    pages = truth_pages(source, truth, layout)
    hits = []
    with evidence.open() as handle:
        header = json.loads(handle.readline())
        if (header.get("schema") != "borsuk-v110-physical-oracle-v1"
            or header.get("source_sha256") != SOURCE_SHA256
            or header.get("truth_sha256") != TRUTH_SHA256
            or header.get("layout_sha256") != ORDER_SHA256
            or header.get("query_count") != 1000
            or header.get("max_gets") != MAX_GETS
            or header.get("max_bytes") != MAX_BYTES
            or header.get("max_units") != MAX_UNITS):
            raise ValueError("oracle header differs")
        for ordinal, page_row in enumerate(pages):
            record = json.loads(handle.readline())
            unique, counts = np.unique(page_row, return_counts=True)
            weights = dict(zip(map(int, unique), map(int, counts)))
            if (record.get("query_ordinal") != ordinal
                or {int(page): weight for page, weight in
                    record.get("truth_page_weights", {}).items()} != weights):
                raise ValueError("oracle page roster differs")
            best = optimal_truth_hits(
                weights, page_count=PAGE_COUNT, max_gets=MAX_GETS,
                max_units=MAX_UNITS, full_page_units=FULL_PAGE_UNITS,
                last_page_units=LAST_PAGE_UNITS,
            )
            if record.get("best_hits") != best:
                raise ValueError("oracle recurrence differs")
            hits.append(best)
        if handle.readline():
            raise ValueError("oracle evidence has extra rows")
    prefix = hits[:200]
    return {"schema": "borsuk-v110-physical-oracle-reduction-v1",
            "evidence_sha256": sha256_file(evidence), "query_count": 1000,
            "max_gets": MAX_GETS, "max_bytes": MAX_BYTES,
            "oracle_kind": "truth-aware-upper-bound-not-query-route",
            "all1000_hits": sum(hits),
            "all1000_recall100_ppm": sum(hits) * 10000 // 1000,
            "all1000_p05_hits": nearest_rank(hits, 5, 100),
            "all1000_sub90_queries": sum(hit < 90 for hit in hits),
            "prefix200_hits": sum(prefix),
            "prefix200_recall100_ppm": sum(prefix) * 10000 // 200,
            "prefix200_p05_hits": nearest_rank(prefix, 5, 100),
            "prefix200_sub90_queries": sum(hit < 90 for hit in prefix),
            "prefix200_ceiling_minus_v109_capped_hits": sum(prefix) - 19739,
            "prefix200_ceiling_minus_v109_uncapped_hits": sum(prefix) - 19832,
            "passes_necessary_prefix_quality": sum(prefix) >= 19800 and nearest_rank(prefix, 5, 100) >= 90,
            "passes_necessary_all1000_quality": sum(hits) >= 99000 and nearest_rank(hits, 5, 100) >= 90}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--truth", type=Path, required=True)
    parser.add_argument("--layout", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = reduce_oracle(args.evidence, args.source, args.truth, args.layout)
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
