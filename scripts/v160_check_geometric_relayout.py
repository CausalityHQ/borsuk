#!/usr/bin/env python3
"""Independently recount V160 layout, plan, fetched IDs, hits and gate."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import (
    CAP_BYTES, CAP_GETS, DIMS, QUERIES, ROWS, authenticate, jsonl,
    load_truth, sha256, spread,
)
from scripts.v160_geometric_relayout_primary import (
    DTYPE, MEMBERSHIP_SHA, ROW_BYTES, SCHEMA, V158_RAW_SHA,
    V158_TERMINAL_SHA, membership_order, page_rows, route,
)


def check(old_sq8_path: Path, membership: Path, membership_seal: Path,
          new_sq8_path: Path, layout_seal: Path, requests: Path,
          reference: Path, plans: Path, plan_seal: Path, truth_path: Path,
          v158_terminal: Path, v158_raw: Path, raw: Path,
          summary_path: Path) -> dict:
    for role, path in (("sq8", old_sq8_path), ("requests", requests),
                       ("reference", reference), ("truth", truth_path)):
        authenticate(path, role)
    if (sha256(v158_terminal) != V158_TERMINAL_SHA
            or sha256(v158_raw) != V158_RAW_SHA
            or json.loads(v158_terminal.read_text()).get("status") != "complete"):
        raise ValueError("closed V158 control identity differs")
    old = np.memmap(old_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    new = np.memmap(new_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    order = membership_order(membership, membership_seal, old)
    if not np.array_equal(new, old[order]):
        raise ValueError("relayout changed an SQ8 row")
    layout = json.loads(layout_seal.read_text())
    if (layout.get("schema") != SCHEMA + "-layout-seal"
            or layout.get("gt_opened") is not False
            or layout.get("membership_sha256") != MEMBERSHIP_SHA
            or layout.get("new_sq8_sha256") != sha256(new_sq8_path)
            or layout.get("old_sq8_sha256") != sha256(old_sq8_path)
            or layout.get("order_sha256") != hashlib.sha256(
                order.astype("<i8").tobytes()).hexdigest()
            or layout.get("page_rows") != page_rows()):
        raise ValueError("layout seal differs")
    seal = json.loads(plan_seal.read_text())
    if (seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("gt_opened") is not False
            or seal.get("queries") != QUERIES
            or seal.get("layout_seal_sha256") != sha256(layout_seal)
            or seal.get("plans_sha256") != sha256(plans)):
        raise ValueError("plan seal differs")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    positions = {int(identifier): i for i, identifier in enumerate(new["id"])}
    if len(positions) != ROWS:
        raise ValueError("relayout stable IDs differ")
    gold = load_truth(truth_path)
    metrics = {key: [] for key in ("hits100", "hits10", "bytes", "gets",
                                   "distinct_primary_pages", "physical_coverage",
                                   "control_hits100")}
    wins = ties = losses = 0
    count = 0
    for request, reference_row, planned, control, result in itertools.zip_longest(
            jsonl(requests), jsonl(reference), jsonl(plans),
            jsonl(v158_raw), jsonl(raw)):
        if any(item is None or item.get("query_ordinal") != count
               for item in (request, reference_row, planned, control, result)):
            raise ValueError("row count/order differs")
        ranges, amount, distinct, score = route(
            reference_row["primary"], request["nominees"], inverse, page_rows(),
        )
        if (planned["ranges"] != ranges or planned["bytes"] != amount
                or planned["distinct_primary_pages"] != distinct
                or planned["plan_score"] != score
                or len(ranges) > CAP_GETS or amount > CAP_BYTES):
            raise ValueError(f"relayout plan differs at {count}")
        returned = result["returned_ids"]
        if (len(returned) != 100 or len(set(returned)) != 100
                or any(type(value) is not int or value not in positions
                       for value in returned)):
            raise ValueError(f"returned identifiers differ at {count}")
        def fetched(identifier: int) -> bool:
            offset = positions[identifier] * ROW_BYTES
            return any(start <= offset < end for start, end in ranges)
        if not all(fetched(value) for value in returned):
            raise ValueError(f"returned ID was not fetched at {count}")
        truth100 = set(map(int, gold[count]))
        truth10 = set(map(int, gold[count, :10]))
        baseline = control["arms"]["exact"]["hits"]
        expected = {
            "hits100": len(set(returned) & truth100),
            "hits10": len(set(returned[:10]) & truth10),
            "bytes": amount, "gets": len(ranges),
            "distinct_primary_pages": distinct,
            "physical_coverage": sum(fetched(value) for value in truth100),
            "control_hits100": baseline,
        }
        if expected["hits100"] > expected["physical_coverage"]:
            raise ValueError("returned hits exceed physical coverage")
        if any(result.get(key) != value for key, value in expected.items()):
            raise ValueError(f"outcome recount differs at {count}")
        for key, value in expected.items():
            metrics[key].append(value)
        wins += expected["hits100"] > baseline
        ties += expected["hits100"] == baseline
        losses += expected["hits100"] < baseline
        count += 1
    if count != QUERIES:
        raise ValueError("query count differs")
    summary = json.loads(summary_path.read_text())
    passed = (sum(metrics["hits100"]) >= 97_500
              and sorted(metrics["hits100"])[49] >= 90
              and sum(metrics["hits10"]) >= 9_600)
    if (summary.get("schema") != SCHEMA + "-summary"
            or summary.get("queries") != QUERIES
            or summary.get("decision") != ("candidate-advance" if passed else "killed")
            or summary.get("paired") != {"wins": wins, "ties": ties, "losses": losses}
            or summary.get("metrics") != {key: spread(values)
                                           for key, values in metrics.items()}
            or summary.get("raw_sha256") != sha256(raw)
            or summary.get("plan_seal_sha256") != sha256(plan_seal)):
        raise ValueError("summary does not match independent recount")
    return {"schema": SCHEMA + "-independent-check", "status": "pass",
            "queries": count, "decision": summary["decision"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("old_sq8", "membership", "membership_seal", "new_sq8",
                 "layout_seal", "requests", "reference", "plans", "plan_seal",
                 "truth", "v158_terminal", "v158_raw", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(
        args.old_sq8, args.membership, args.membership_seal,
        args.new_sq8, args.layout_seal, args.requests, args.reference,
        args.plans, args.plan_seal, args.truth, args.v158_terminal,
        args.v158_raw, args.raw, args.summary,
    ), sort_keys=True))
