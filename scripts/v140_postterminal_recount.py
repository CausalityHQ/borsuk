#!/usr/bin/env python3
"""Recount complete V140 raw rows and D96 physical GT coverage after terminal."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from v137_halo_ceiling import EVIDENCE_SHA, LAYOUT_SHA, inverse_layout

BETAS = ("1", "2", "4", "8")
MAX_BYTES = 16_777_216


def authenticated(path: Path, expected: str) -> bytes:
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError(f"artifact SHA-256 differs: {path}")
    return raw


def rows(path: Path, sha: str) -> list[dict]:
    values = [json.loads(line) for line in authenticated(path, sha).splitlines()]
    if len(values) != 1000 or [row["query_ordinal"] for row in values] != list(range(1000)):
        raise ValueError("V140 query identity differs")
    return values


def recount(raw: list[dict], rows_count: int, row_bytes: int) -> dict:
    report = {}
    for beta in BETAS:
        byte_values = []
        gets_values = []
        retained = 0
        primary_total = 0
        shortfalls = 0
        for row in raw:
            result = row["variants"][beta]
            spans = result["ranges"]
            if (not spans or len(spans) > 32
                    or any(not 0 <= first < last <= rows_count * row_bytes
                           for first, last in spans)
                    or any(spans[i][1] >= spans[i + 1][0]
                           for i in range(len(spans) - 1))):
                raise ValueError("V140 range schedule differs")
            charged = sum(last - first for first, last in spans)
            if (charged != result["planned_bytes"] or charged > MAX_BYTES
                    or len(spans) != result["gets"] or not result["fits_cap"]):
                raise ValueError("V140 byte or GET charge differs")
            selected = result["selected_pages"]
            if (len(selected) != len(set(selected)) or selected != sorted(selected)
                    or len(selected) != result["selected_page_count"]
                    or len(selected) + result["target_shortfall"] != result["target_pages"]
                    or result["primary_pages_retained"] > row["primary_page_count"]):
                raise ValueError("V140 selected page inventory differs")
            if any(not any(first <= page * 256 * row_bytes < last
                           for first, last in spans) for page in selected):
                raise ValueError("V140 selected page outside fetched ranges")
            byte_values.append(charged)
            gets_values.append(len(spans))
            retained += result["primary_pages_retained"]
            primary_total += row["primary_page_count"]
            shortfalls += result["target_shortfall"] > 0
        report[beta] = {
            "mean_bytes": sum(byte_values) / 1000,
            "p95_bytes": sorted(byte_values)[949],
            "mean_gets": sum(gets_values) / 1000,
            "primary_pages_retained": retained,
            "primary_pages_total": primary_total,
            "queries_with_target_shortfall": shortfalls,
        }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deep-raw", required=True, type=Path)
    parser.add_argument("--deep-sha256", required=True)
    parser.add_argument("--relaion-raw", required=True, type=Path)
    parser.add_argument("--relaion-sha256", required=True)
    parser.add_argument("--v122-evidence", required=True, type=Path)
    parser.add_argument("--v122-layout", required=True, type=Path)
    args = parser.parse_args()
    deep = rows(args.deep_raw, args.deep_sha256)
    relaion = rows(args.relaion_raw, args.relaion_sha256)
    report = {
        "schema": "borsuk-v140-postterminal-recount-v1",
        "deep": recount(deep, 100_000, 108),
        "relaion": recount(relaion, 1_000_000, 780),
    }
    inverse = inverse_layout(authenticated(args.v122_layout, LAYOUT_SHA))
    evidence = [json.loads(line) for line in
                authenticated(args.v122_evidence, EVIDENCE_SHA).splitlines()]
    if len(evidence) != 1000:
        raise ValueError("V122 evidence count differs")
    for beta in BETAS:
        hits = []
        baseline_hits = []
        for ordinal, (row, route) in enumerate(zip(evidence, deep)):
            if (row["query_ordinal"] != ordinal
                    or row["source_query_ordinal"] != 9000 + ordinal
                    or len(row["truth_ids"]) != 100):
                raise ValueError("V122 truth identity differs")
            spans = route["variants"][beta]["ranges"]
            hits.append(sum(any(first <= inverse[source_id] * 108 < last
                                for first, last in spans)
                            for source_id in row["truth_ids"]))
            baseline_hits.append(row["baseline_physical_coverage"])
        report["deep"][beta].update({
            "physical_gt_hits": sum(hits),
            "p05_physical_hits": sorted(hits)[49],
            "sub90_physical_queries": sum(value < 90 for value in hits),
            "v122_baseline_physical_hits": sum(baseline_hits),
            "paired_better_equal_worse": [
                sum(a > b for a, b in zip(hits, baseline_hits)),
                sum(a == b for a, b in zip(hits, baseline_hits)),
                sum(a < b for a, b in zip(hits, baseline_hits)),
            ],
        })
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
