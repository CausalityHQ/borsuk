#!/usr/bin/env python3
"""Separate V162 page membership and GT-hit recount from frozen inputs."""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

from scripts.v124_source_tier_precision import load_truth
from scripts.v155_relaion_returned_quality import QUERIES, SEALED_SHA, TRUTH_SHA, sha256
from scripts.v162_closed_route_loss import (
    CAP_BYTES, FIELDS, PAGE_ROWS, ROW_BYTES, SCHEMA,
    V155_EVIDENCE_SHA, V161_RAW_SHA, authenticated_maps, jsonl, minimum_cover,
    spread,
)


def check(membership: Path, layout: Path, layout_seal: Path, plan_seal: Path,
          plans: Path, sealed: Path, truth_path: Path, v161_raw: Path,
          v155_evidence: Path, raw_path: Path, summary_path: Path) -> dict:
    if (sha256(sealed) != SEALED_SHA or sha256(truth_path) != TRUTH_SHA
            or sha256(v161_raw) != V161_RAW_SHA
            or sha256(v155_evidence) != V155_EVIDENCE_SHA):
        raise ValueError("V162 independent frozen input differs")
    source_ids, physical_by_source, old_layout = authenticated_maps(
        membership, layout, layout_seal, plan_seal, plans,
    )
    source_by_id = {int(identifier): i for i, identifier in enumerate(source_ids)}
    if len(source_by_id) != len(source_ids):
        raise ValueError("V162 source ID mapping differs")
    gold = load_truth(truth_path, source_ids)
    metrics = {field: [] for field in FIELDS}
    count = 0
    for roster, plan, v161, v155, row in itertools.zip_longest(
            jsonl(sealed), jsonl(plans), jsonl(v161_raw),
            jsonl(v155_evidence), jsonl(raw_path)):
        if any(item is None or item.get("query_ordinal") != count
               for item in (roster, plan, v161, v155, row)):
            raise ValueError("V162 independent query order differs")
        nominee_source = {int(old_layout[value]) for value in roster["nominees"]}
        primary_source = {int(old_layout[value]) for value in roster["primary"]}
        nominated_pages = {int(physical_by_source[value]) // PAGE_ROWS
                           for value in nominee_source}
        primary_pages = {int(physical_by_source[value]) // PAGE_ROWS
                         for value in primary_source}
        truth_source = [source_by_id[int(identifier)] for identifier in gold[count][:100]]
        ranges = plan["ranges"]
        expected = {
            "nominee_rows": sum(value in nominee_source for value in truth_source),
            "nominee_pages": sum(int(physical_by_source[value]) // PAGE_ROWS
                                 in nominated_pages for value in truth_source),
            "primary_rows": sum(value in primary_source for value in truth_source),
            "primary_pages": sum(int(physical_by_source[value]) // PAGE_ROWS
                                 in primary_pages for value in truth_source),
            "final_plan": sum(any(start <= int(physical_by_source[value]) * ROW_BYTES < end
                                  for start, end in ranges) for value in truth_source),
            "v155_final_plan": v155["arms"]["sparse"]["physical_hits"],
            "primary_distinct_pages": len(primary_pages),
        }
        cover_bytes, runs, gets = minimum_cover(primary_pages)
        expected.update(primary_min_cover_bytes=cover_bytes,
                        primary_original_runs=runs, primary_cover_gets=gets)
        if (any(row.get(field) != expected[field] for field in FIELDS)
                or expected["final_plan"] != v161["candidate_physical_hits"]
                or expected["v155_final_plan"] != v161["control_physical_hits"]):
            raise ValueError(f"V162 independent stage recount differs at {count}")
        for field in FIELDS:
            metrics[field].append(expected[field])
        count += 1
    if count != QUERIES:
        raise ValueError("V162 independent query count differs")
    primary = metrics["primary_pages"]
    final = metrics["final_plan"]
    headroom = sum(primary) >= 99_400 and sorted(primary)[49] >= 97
    final_pass = sum(final) >= 99_400 and sorted(final)[49] >= 97
    classification = ("admission-loss" if headroom and not final_pass else
                      "layout-and-admission-loss" if not headroom and
                      sum(final) < sum(primary) else
                      "layout-headroom-loss" if not headroom else "no-transfer-loss")
    summary = json.loads(summary_path.read_text())
    if (summary.get("schema") != SCHEMA + "-summary"
            or summary.get("queries") != QUERIES
            or summary.get("classification") != classification
            or summary.get("primary_cover_within_cap") != sum(
                value <= CAP_BYTES for value in metrics["primary_min_cover_bytes"])
            or summary.get("metrics") != {field: spread(values)
                                           for field, values in metrics.items()}
            or summary.get("raw_sha256") != sha256(raw_path)):
        raise ValueError("V162 independent summary differs")
    return {"schema": SCHEMA + "-independent-check", "status": "pass",
            "queries": count, "classification": classification}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("membership", "layout", "layout_seal", "plan_seal", "plans",
                 "sealed", "truth", "v161_raw", "v155_evidence", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(
        args.membership, args.layout, args.layout_seal, args.plan_seal,
        args.plans, args.sealed, args.truth, args.v161_raw, args.v155_evidence,
        args.raw, args.summary,
    ), sort_keys=True))
