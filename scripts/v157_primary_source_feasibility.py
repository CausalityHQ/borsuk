#!/usr/bin/env python3
"""GT-blind source-primary and minimum S3 nomination-cover arithmetic."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

CAP_BYTES = 16_777_216
MAX_GETS = 32
PAGE_ROWS = 256
QUERIES = 1000
INPUTS = {
    "deep-image-96-angular-random100k-publication-test-9000-9999": (
        100_000, 96, 6_183_526,
        "deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6",
    ),
    "relaion-1m-validation-1000": (
        1_000_000, 768, 13_455_525,
        "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960",
    ),
}


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def minimum_cover(
    pages: set[int], rows: int, row_bytes: int, max_gets: int = MAX_GETS,
) -> tuple[int, int, int]:
    """Return minimum bytes under 32 GETs, original runs and final GETs."""
    ordered = sorted(pages)
    if not ordered or max_gets <= 0 or ordered[-1] >= math.ceil(rows / PAGE_ROWS):
        raise ValueError("page geometry differs")
    runs: list[tuple[int, int]] = []
    for page in ordered:
        if runs and runs[-1][1] == page:
            runs[-1] = (runs[-1][0], page + 1)
        else:
            runs.append((page, page + 1))
    original_runs = len(runs)
    joins = max(0, original_runs - max_gets)
    gaps = sorted((runs[index + 1][0] - runs[index][1], index)
                  for index in range(len(runs) - 1))
    bridged = {index for _, index in gaps[:joins]}
    cover: list[tuple[int, int]] = []
    for index, run in enumerate(runs):
        if cover and index - 1 in bridged:
            cover[-1] = (cover[-1][0], run[1])
        else:
            cover.append(run)
    byte_count = sum((min(end * PAGE_ROWS, rows) - start * PAGE_ROWS) * row_bytes
                     for start, end in cover)
    if len(cover) > max_gets or byte_count <= 0:
        raise ValueError("cover construction differs")
    return byte_count, original_runs, len(cover)


def percentile(values: list[int], numerator: int) -> int:
    ordered = sorted(values)
    return ordered[(numerator * len(ordered) + 99) // 100 - 1]


def summarize(records: list[dict], cohort: str) -> dict:
    def spread(field: str) -> dict:
        values = [int(row[field]) for row in records]
        return {"min": min(values), "p05": percentile(values, 5),
                "p50": percentile(values, 50), "p95": percentile(values, 95),
                "p99": percentile(values, 99), "max": max(values),
                "sum": sum(values)}
    return {
        "schema": "borsuk-v157-primary-feasibility-summary-v1",
        "cohort": cohort, "queries": len(records),
        "cap_bytes": CAP_BYTES, "max_gets": MAX_GETS,
        "nomination_cover_within_cap": sum(row["nomination_cover_bytes"] <= CAP_BYTES
                                           for row in records),
        "pq_exact_row_overlap": spread("pq_exact_row_overlap"),
        "pq_exact_page_overlap": spread("pq_exact_page_overlap"),
        "exact_primary_pages": spread("exact_primary_pages"),
        "pq_primary_pages": spread("pq_primary_pages"),
        "nominee_pages": spread("nominee_pages"),
        "nomination_cover_bytes": spread("nomination_cover_bytes"),
        "unbridged_gets": spread("unbridged_gets"),
    }


def evaluate(input_path: Path, cohort: str, raw_path: Path, summary_path: Path) -> None:
    rows, dimensions, expected_bytes, expected_sha = INPUTS[cohort]
    if input_path.stat().st_size != expected_bytes or hash_file(input_path) != expected_sha:
        raise ValueError("frozen input identity differs")
    records = []
    with input_path.open() as source:
        for ordinal, line in enumerate(source):
            capture = json.loads(line)
            nominees = capture.get("nominees")
            exact = capture.get("primary")
            if (ordinal >= QUERIES or capture.get("query_ordinal") != ordinal
                    or not isinstance(nominees, list) or len(nominees) != 512
                    or not isinstance(exact, list) or len(exact) != 100
                    or any(type(value) is not int or value < 0 or value >= rows
                           for value in nominees + exact)
                    or len(set(nominees)) != 512 or len(set(exact)) != 100
                    or not set(exact).issubset(nominees)):
                raise ValueError(f"capture geometry differs at query {ordinal}")
            pq = nominees[:100]
            nominee_pages = {row // PAGE_ROWS for row in nominees}
            exact_pages = {row // PAGE_ROWS for row in exact}
            pq_pages = {row // PAGE_ROWS for row in pq}
            cover_bytes, runs, gets = minimum_cover(
                nominee_pages, rows, dimensions + 12,
            )
            records.append({
                "query_ordinal": ordinal, "pq_exact_row_overlap": len(set(pq) & set(exact)),
                "pq_exact_page_overlap": len(pq_pages & exact_pages),
                "exact_primary_pages": len(exact_pages),
                "pq_primary_pages": len(pq_pages),
                "nominee_pages": len(nominee_pages),
                "unbridged_gets": runs, "nomination_gets": gets,
                "nomination_cover_bytes": cover_bytes,
            })
    if len(records) != QUERIES:
        raise ValueError("query count differs")
    with raw_path.open("x") as target:
        for row in records:
            target.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    summary_path.write_text(json.dumps(summarize(records, cohort),
                                       sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--cohort", choices=sorted(INPUTS), required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    evaluate(args.input, args.cohort, args.raw, args.summary)
