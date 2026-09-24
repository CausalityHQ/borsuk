#!/usr/bin/env python3
"""Independent closed-input recount for V161's 1M transfer decision."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v124_source_tier_precision import load_truth, rank, unit
from scripts.v155_relaion_returned_quality import (
    DIMS, QUERIES, REQUEST_SHA, SEALED_SHA, TRUTH_SHA, sha256,
)
from scripts.v161_geometric_relayout_1m import (
    CAP_BYTES, CAP_GETS, DTYPE, ROWS, ROW_BYTES, SCHEMA,
    V155_EVIDENCE_SHA, V155_REPLAY_SHA, V155_TERMINAL_SHA,
    distribution, jsonl, old_layout_and_sq8, page_rows, read_order,
    route, source_arrays,
)


def check(source_path: Path, old_layout_path: Path, old_sq8_path: Path,
          manifest_path: Path, membership_path: Path, new_sq8_path: Path,
          layout_seal: Path, requests_path: Path, sealed_path: Path,
          plans_path: Path, plan_seal: Path, scored_path: Path,
          score_seal: Path, truth_path: Path, v155_terminal: Path,
          v155_replay: Path, v155_evidence: Path, raw_path: Path,
          summary_path: Path) -> dict:
    if (sha256(requests_path) != REQUEST_SHA
            or sha256(sealed_path) != SEALED_SHA
            or sha256(truth_path) != TRUTH_SHA
            or sha256(v155_terminal) != V155_TERMINAL_SHA
            or sha256(v155_replay) != V155_REPLAY_SHA
            or sha256(v155_evidence) != V155_EVIDENCE_SHA):
        raise ValueError("V161 frozen identity differs")
    source_ids, vectors, _ = source_arrays(source_path)
    order, old_order, old_sq8 = read_order(
        membership_path, old_layout_path, old_sq8_path,
    )
    old_layout_and_sq8(old_layout_path, old_sq8_path, source_ids)
    new = np.memmap(new_sq8_path, dtype=DTYPE, mode="r", shape=(ROWS,))
    old_physical = np.empty(ROWS, dtype=np.int64)
    old_physical[old_order] = np.arange(ROWS, dtype=np.int64)
    for start in range(0, ROWS, 4096):
        stop = min(start + 4096, ROWS)
        if not np.array_equal(new[start:stop],
                              old_sq8[old_physical[order[start:stop]]]):
            raise ValueError("V161 relayout changed SQ8 row bytes")
    layout = json.loads(layout_seal.read_text())
    if (layout.get("schema") != SCHEMA + "-layout-seal"
            or layout.get("gt_opened") is not False
            or layout.get("membership_sha256") != sha256(membership_path)
            or layout.get("new_sq8_sha256") != sha256(new_sq8_path)
            or layout.get("order_sha256") != hashlib.sha256(
                order.astype("<i8").tobytes()).hexdigest()
            or layout.get("page_rows") != page_rows()):
        raise ValueError("V161 layout seal differs")
    plan_identity = json.loads(plan_seal.read_text())
    score_identity = json.loads(score_seal.read_text())
    if (plan_identity.get("schema") != SCHEMA + "-plan-seal"
            or plan_identity.get("gt_opened") is not False
            or plan_identity.get("layout_seal_sha256") != sha256(layout_seal)
            or plan_identity.get("plans_sha256") != sha256(plans_path)
            or score_identity.get("schema") != SCHEMA + "-score-seal"
            or score_identity.get("gt_opened") is not False
            or score_identity.get("plan_seal_sha256") != sha256(plan_seal)
            or score_identity.get("scored_sha256") != sha256(scored_path)):
        raise ValueError("V161 GT-blind plan/score seal differs")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    positions = {int(identifier): i for i, identifier in enumerate(new["id"])}
    id_to_source = {int(identifier): i for i, identifier in enumerate(source_ids)}
    if len(positions) != ROWS or len(id_to_source) != ROWS:
        raise ValueError("V161 IDs are not unique")
    manifest = json.loads(manifest_path.read_text())
    low = np.asarray(manifest["low"], dtype=np.float32)
    step = np.asarray(manifest["step"], dtype=np.float32)
    if low.shape != (DIMS,) or step.shape != (DIMS,):
        raise ValueError("V161 SQ8 quantizer geometry differs")
    truth = load_truth(truth_path, source_ids)
    fields = ("candidate_source_hits", "candidate_sq8_hits", "candidate_physical_hits",
              "control_source_hits", "control_sq8_hits", "control_physical_hits",
              "bytes", "gets", "distinct_primary_pages", "union_size")
    metrics = {name: [] for name in fields}
    wins = ties = losses = 0
    count = 0
    for request, sealed, planned, scored, control, prior, raw in itertools.zip_longest(
            jsonl(requests_path), jsonl(sealed_path), jsonl(plans_path),
            jsonl(scored_path), jsonl(v155_replay), jsonl(v155_evidence),
            jsonl(raw_path)):
        if any(row is None or row.get("query_ordinal") != count
               for row in (request, sealed, planned, scored, control, prior, raw)):
            raise ValueError("V161 recount query order differs")
        if request["nominees"] != sealed["nominees"]:
            raise ValueError("V116 nominees differ")
        ranges, amount, distinct, score = route(
            sealed["primary"], request["nominees"], old_order, inverse,
        )
        if (planned["ranges"] != ranges or planned["bytes"] != amount
                or planned["distinct_primary_pages"] != distinct
                or planned["plan_score"] != score
                or len(ranges) > CAP_GETS or amount > CAP_BYTES):
            raise ValueError(f"V161 plan differs at {count}")
        query_f32 = np.asarray(request["query"], dtype=np.float32)
        returned = score_sq8_ranges(new, query_f32, low, step, ranges, top_k=512)
        if (scored["sq8_top512_ids"] != returned
                or scored["sq8_top100_ids"] != returned[:100]):
            raise ValueError(f"V161 SQ8 returned IDs differ at {count}")
        nominee_ids = set(map(int, old_sq8["id"][request["nominees"]]))
        union = np.asarray(sorted(nominee_ids | set(returned)), dtype=np.int64)
        source_rows = np.fromiter((id_to_source[int(value)] for value in union),
                                  dtype=np.int64, count=union.size)
        query = query_f32.astype(np.float64)
        query /= np.linalg.norm(query)
        exact = rank(union, unit(vectors[source_rows].astype(np.float64)) @ query, 100)
        if (scored["source_top100_ids"] != exact.tolist()
                or scored["union_size"] != union.size):
            raise ValueError(f"V161 exact-source returned IDs differ at {count}")
        truth100 = set(map(int, truth[count][:100]))
        def fetched(identifier: int) -> bool:
            offset = positions[identifier] * ROW_BYTES
            return any(start <= offset < end for start, end in ranges)
        if not all(fetched(value) for value in returned):
            raise ValueError(f"V161 SQ8 result was not fetched at {count}")
        base = control["arms"]["sparse"]
        expected = {
            "candidate_source_hits": len(set(map(int, exact)) & truth100),
            "candidate_sq8_hits": len(set(returned[:100]) & truth100),
            "candidate_physical_hits": sum(fetched(value) for value in truth100),
            "control_source_hits": len(set(base["source_top100_ids"]) & truth100),
            "control_sq8_hits": len(set(base["sq8_top100_ids"]) & truth100),
            "control_physical_hits": prior["arms"]["sparse"]["physical_hits"],
            "bytes": amount, "gets": len(ranges),
            "distinct_primary_pages": distinct, "union_size": int(union.size),
        }
        if (prior["arms"]["sparse"]["source_hits"] != expected["control_source_hits"]
                or prior["arms"]["sparse"]["sq8_hits"] != expected["control_sq8_hits"]):
            raise ValueError("V155 baseline evidence differs")
        if expected["candidate_sq8_hits"] > expected["candidate_physical_hits"]:
            raise ValueError("SQ8 hits exceed physical coverage")
        if any(raw.get(name) != value for name, value in expected.items()):
            raise ValueError(f"V161 raw recount differs at {count}")
        for name, value in expected.items():
            metrics[name].append(value)
        wins += expected["candidate_source_hits"] > expected["control_source_hits"]
        ties += expected["candidate_source_hits"] == expected["control_source_hits"]
        losses += expected["candidate_source_hits"] < expected["control_source_hits"]
        count += 1
    if count != QUERIES:
        raise ValueError("V161 recount query count differs")
    candidate = metrics["candidate_source_hits"]
    if (sum(metrics["control_source_hits"]) != 99_567
            or sum(metrics["control_sq8_hits"]) != 99_222):
        raise ValueError("V155 baseline totals differ")
    transfer = sum(candidate) >= 99_400 and sorted(candidate)[49] >= 97
    competitive = (transfer and sum(candidate) >= 99_567
                   and sorted(candidate)[49] >= 98
                   and sum(metrics["bytes"]) <= 11_134_007_040
                   and sum(metrics["gets"]) <= 22_126)
    decision = ("baseline-competitive" if competitive else
                "transfer-pass" if transfer else "killed")
    summary = json.loads(summary_path.read_text())
    if (summary.get("schema") != SCHEMA + "-summary"
            or summary.get("queries") != QUERIES
            or summary.get("decision") != decision
            or summary.get("paired") != {"wins": wins, "ties": ties, "losses": losses}
            or summary.get("metrics") != {name: distribution(values)
                                           for name, values in metrics.items()}
            or summary.get("raw_sha256") != sha256(raw_path)
            or summary.get("score_seal_sha256") != sha256(score_seal)):
        raise ValueError("V161 summary recount differs")
    return {"schema": SCHEMA + "-independent-check", "status": "pass",
            "queries": count, "decision": decision}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("source", "old_layout", "old_sq8", "manifest", "membership",
                 "new_sq8", "layout_seal", "requests", "sealed", "plans",
                 "plan_seal", "scored", "score_seal", "truth", "v155_terminal",
                 "v155_replay", "v155_evidence", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(
        args.source, args.old_layout, args.old_sq8, args.manifest,
        args.membership, args.new_sq8, args.layout_seal, args.requests,
        args.sealed, args.plans, args.plan_seal, args.scored, args.score_seal,
        args.truth, args.v155_terminal, args.v155_replay, args.v155_evidence,
        args.raw, args.summary,
    ), sort_keys=True))
