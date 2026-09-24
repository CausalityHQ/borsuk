#!/usr/bin/env python3
"""Independent per-query recount of the V157 GT-blind arithmetic."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

SPECS = {
    "deep-image-96-angular-random100k-publication-test-9000-9999":
        (100_000, 108, 6_183_526,
         "deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6"),
    "relaion-1m-validation-1000":
        (1_000_000, 780, 13_455_525,
         "3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960"),
}


def verify(input_path: Path, raw_path: Path, summary_path: Path, cohort: str) -> None:
    rows, width, expected_bytes, expected_hash = SPECS[cohort]
    if input_path.stat().st_size != expected_bytes:
        raise ValueError("input byte length differs")
    digest = hashlib.sha256()
    with input_path.open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected_hash:
        raise ValueError("input SHA-256 differs")
    captures = input_path.open()
    raw = raw_path.open()
    checked = []
    try:
        for ordinal, (capture_line, raw_line) in enumerate(
            itertools.zip_longest(captures, raw)
        ):
            if capture_line is None or raw_line is None:
                raise ValueError("raw/input query count differs")
            source = json.loads(capture_line)
            recorded = json.loads(raw_line)
            if source["query_ordinal"] != ordinal or recorded["query_ordinal"] != ordinal:
                raise ValueError("query ordinal differs")
            nominees = source["nominees"]
            exact = source["primary"]
            if len(nominees) != 512 or len(exact) != 100:
                raise ValueError("roster size differs")
            pq = nominees[:100]
            pages = sorted({value // 256 for value in nominees})
            exact_pages = {value // 256 for value in exact}
            pq_pages = {value // 256 for value in pq}
            starts = [pages[0]]
            stops = []
            for left, right in zip(pages, pages[1:]):
                if right != left + 1:
                    stops.append(left + 1)
                    starts.append(right)
            stops.append(pages[-1] + 1)
            gaps = sorted(starts[index + 1] - stops[index]
                          for index in range(len(starts) - 1))
            bridges = max(0, len(starts) - 32)
            selected_bytes = sum(min(256, rows - page * 256) * width for page in pages)
            cover_bytes = selected_bytes + sum(gaps[:bridges]) * 256 * width
            expected = {
                "query_ordinal": ordinal,
                "pq_exact_row_overlap": len(set(pq).intersection(exact)),
                "pq_exact_page_overlap": len(pq_pages.intersection(exact_pages)),
                "exact_primary_pages": len(exact_pages),
                "pq_primary_pages": len(pq_pages),
                "nominee_pages": len(pages),
                "unbridged_gets": len(starts),
                "nomination_gets": min(len(starts), 32),
                "nomination_cover_bytes": cover_bytes,
            }
            if recorded != expected:
                raise ValueError(f"raw query {ordinal} differs")
            checked.append(expected)
    finally:
        captures.close()
        raw.close()
    if len(checked) != 1000:
        raise ValueError("raw query count differs")
    summary = json.loads(summary_path.read_text())
    if (summary["cohort"] != cohort or summary["queries"] != 1000
            or summary["cap_bytes"] != 16_777_216 or summary["max_gets"] != 32
            or summary["nomination_cover_within_cap"] != sum(
                row["nomination_cover_bytes"] <= 16_777_216 for row in checked)):
        raise ValueError("summary identity or cap count differs")
    for field in ("pq_exact_row_overlap", "pq_exact_page_overlap", "exact_primary_pages",
                  "pq_primary_pages", "nominee_pages", "nomination_cover_bytes",
                  "unbridged_gets"):
        ordered = sorted(row[field] for row in checked)
        wanted = {"min": ordered[0], "p05": ordered[49], "p50": ordered[499],
                  "p95": ordered[949], "p99": ordered[989], "max": ordered[-1],
                  "sum": sum(ordered)}
        if summary[field] != wanted:
            raise ValueError(f"summary {field} differs")
    print(json.dumps({"schema":"borsuk-v157-independent-check-v1",
                      "cohort":cohort,"queries":len(checked),"status":"pass"},
                     sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--cohort", choices=sorted(SPECS), required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    verify(args.input, args.raw, args.summary, args.cohort)
