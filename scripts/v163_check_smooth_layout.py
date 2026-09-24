#!/usr/bin/env python3
"""Rebuild V163 source order and recount all 100k returned outcomes."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v120_source_layout import _fit_order, cluster_count
from scripts.v158_pq_primary_returned import (
    CAP_BYTES, CAP_GETS, DIMS, QUERIES, ROWS, authenticate, jsonl,
    load_truth, sha256, spread,
)
from scripts.v160_geometric_relayout_primary import DTYPE, ROW_BYTES, page_rows, route
from scripts.v163_smooth_layout_100k import (
    SCHEMA, V160_RAW_SHA, V160_TERMINAL_SHA, source_vectors,
)


def check(source_path: Path, old_sq8_path: Path, old_manifest: Path,
          order_path: Path, new_sq8_path: Path, layout_seal: Path,
          requests: Path, reference: Path, plans_path: Path,
          plan_seal: Path, truth_path: Path, v160_terminal: Path,
          v160_raw: Path, raw_path: Path, summary_path: Path) -> dict:
    for role, path in (("sq8", old_sq8_path), ("manifest", old_manifest),
                       ("requests", requests), ("reference", reference),
                       ("truth", truth_path)):
        authenticate(path, role)
    if (sha256(v160_terminal) != V160_TERMINAL_SHA
            or sha256(v160_raw) != V160_RAW_SHA
            or json.loads(v160_terminal.read_text()).get("status") != "complete"):
        raise ValueError("closed V160 control differs")
    old = np.memmap(old_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    vectors = source_vectors(source_path, old)
    expected_order = _fit_order(vectors, cluster_count(ROWS))
    order = np.load(order_path, allow_pickle=False)
    if (order.shape != (ROWS,) or not np.array_equal(order, expected_order)
            or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("source-only k-means order differs")
    new = np.memmap(new_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    if not np.array_equal(new, old[order]):
        raise ValueError("V163 relayout changed SQ8 row bytes")
    layout = json.loads(layout_seal.read_text())
    plan_identity = json.loads(plan_seal.read_text())
    if (layout.get("schema") != SCHEMA + "-layout-seal"
            or layout.get("gt_opened") is not False
            or layout.get("order_file_sha256") != sha256(order_path)
            or layout.get("order_sha256") != hashlib.sha256(
                order.astype("<i8").tobytes()).hexdigest()
            or layout.get("new_sq8_sha256") != sha256(new_sq8_path)
            or layout.get("clusters") != cluster_count(ROWS)
            or plan_identity.get("schema") != SCHEMA + "-plan-seal"
            or plan_identity.get("gt_opened") is not False
            or plan_identity.get("layout_seal_sha256") != sha256(layout_seal)
            or plan_identity.get("plans_sha256") != sha256(plans_path)):
        raise ValueError("V163 GT-blind construction/plan seal differs")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    positions = {int(identifier): i for i, identifier in enumerate(new["id"])}
    if len(positions) != ROWS:
        raise ValueError("V163 relaid stable IDs differ")
    authority = json.loads(old_manifest.read_text())
    low = np.asarray(authority["low"], dtype=np.float32)
    step = np.asarray(authority["step"], dtype=np.float32)
    if low.shape != (DIMS,) or step.shape != (DIMS,):
        raise ValueError("V163 SQ8 quantizer geometry differs")
    truth = load_truth(truth_path)
    fields = ("hits100", "hits10", "physical_coverage", "bytes", "gets",
              "distinct_primary_pages", "control_hits100", "control_hits10",
              "control_bytes", "control_gets")
    metrics = {field: [] for field in fields}
    wins = ties = losses = 0
    count = 0
    for request, ref, planned, control, result in itertools.zip_longest(
            jsonl(requests), jsonl(reference), jsonl(plans_path),
            jsonl(v160_raw), jsonl(raw_path)):
        if any(row is None or row.get("query_ordinal") != count
               for row in (request, ref, planned, control, result)):
            raise ValueError("V163 replay row count/order differs")
        ranges, amount, distinct, score = route(
            ref["primary"], request["nominees"], inverse, page_rows(),
        )
        if (planned["ranges"] != ranges or planned["bytes"] != amount
                or planned["distinct_primary_pages"] != distinct
                or planned["plan_score"] != score
                or amount > CAP_BYTES or len(ranges) > CAP_GETS):
            raise ValueError(f"V163 physical plan differs at {count}")
        query = np.asarray(request["query"], dtype=np.float32)
        returned = score_sq8_ranges(new, query, low, step, ranges, top_k=100)
        truth100 = set(map(int, truth[count]))
        truth10 = set(map(int, truth[count, :10]))
        control_returned = control["returned_ids"]
        if (len(returned) != 100 or len(set(returned)) != 100
                or len(control_returned) != 100 or len(set(control_returned)) != 100
                or control["hits100"] != len(set(control_returned) & truth100)
                or control["hits10"] != len(set(control_returned[:10]) & truth10)):
            raise ValueError("V160/V163 returned ID geometry differs")
        expected = {
            "hits100": len(set(returned) & truth100),
            "hits10": len(set(returned[:10]) & truth10),
            "physical_coverage": sum(any(
                start <= positions[identifier] * ROW_BYTES < end
                for start, end in ranges) for identifier in truth100),
            "bytes": amount, "gets": len(ranges),
            "distinct_primary_pages": distinct,
            "control_hits100": control["hits100"],
            "control_hits10": control["hits10"],
            "control_bytes": control["bytes"],
            "control_gets": control["gets"],
        }
        if (result.get("returned_ids") != returned
                or any(result.get(field) != value for field, value in expected.items())
                or expected["hits100"] > expected["physical_coverage"]):
            raise ValueError(f"V163 independent outcome differs at {count}")
        for field, value in expected.items():
            metrics[field].append(value)
        wins += expected["hits100"] > control["hits100"]
        ties += expected["hits100"] == control["hits100"]
        losses += expected["hits100"] < control["hits100"]
        count += 1
    if count != QUERIES:
        raise ValueError("V163 independent query count differs")
    candidate = metrics["hits100"]
    passed = (sum(candidate) >= 97_500 and sorted(candidate)[49] >= 90
              and sum(metrics["hits10"]) >= 9_600)
    summary = json.loads(summary_path.read_text())
    if (summary.get("schema") != SCHEMA + "-summary"
            or summary.get("queries") != QUERIES
            or summary.get("decision") != ("candidate-advance" if passed else "killed")
            or summary.get("paired") != {"wins": wins, "ties": ties, "losses": losses}
            or summary.get("metrics") != {field: spread(values)
                                           for field, values in metrics.items()}
            or summary.get("raw_sha256") != sha256(raw_path)
            or summary.get("plan_seal_sha256") != sha256(plan_seal)):
        raise ValueError("V163 independent summary differs")
    return {"schema": SCHEMA + "-independent-check", "status": "pass",
            "queries": count, "decision": summary["decision"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("source", "old_sq8", "old_manifest", "order", "new_sq8",
                 "layout_seal", "requests", "reference", "plans", "plan_seal",
                 "truth", "v160_terminal", "v160_raw", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(
        args.source, args.old_sq8, args.old_manifest, args.order,
        args.new_sq8, args.layout_seal, args.requests, args.reference,
        args.plans, args.plan_seal, args.truth, args.v160_terminal,
        args.v160_raw, args.raw, args.summary,
    ), sort_keys=True))
