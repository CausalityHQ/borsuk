#!/usr/bin/env python3
"""Independent V158 per-query outcome recount from frozen artifacts."""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import (
    DIMS, HASHES, QUERIES, ROWS, authenticate, jsonl, load_truth, sha256,
)
from scripts.v114_exact_local_100k import route_reference


def distribution(values: list[int]) -> dict:
    values = sorted(values)
    return {"min": values[0], "p05": values[49], "p50": values[499],
            "p95": values[949], "p99": values[989], "max": values[-1],
            "sum": sum(values)}


def verify(requests: Path, reference: Path, sq8_path: Path,
           truth_path: Path, plans: Path, seal: Path, raw: Path,
           summary_path: Path) -> dict:
    for role, path in (("requests", requests), ("reference", reference),
                       ("sq8", sq8_path), ("truth", truth_path)):
        authenticate(path, role)
    identity = json.loads(seal.read_text())
    if (identity["schema"] != "borsuk-v158-plan-seal-v1"
            or identity["plans_sha256"] != sha256(plans)
            or identity["gt_opened"] is not False):
        raise ValueError("plan seal differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"),
                      ("code", "u1", (DIMS,))])
    sq8 = np.memmap(sq8_path, mode="r", dtype=dtype, shape=(ROWS,))
    ids = np.asarray(sq8["id"], dtype=np.int64)
    if not np.array_equal(np.sort(ids), np.arange(ROWS)):
        raise ValueError("SQ8 ID permutation differs")
    physical = np.empty(ROWS, dtype=np.int64)
    physical[ids] = np.arange(ROWS)
    gold = load_truth(truth_path)
    metrics = {arm: {field: [] for field in ("hits", "physical_coverage", "bytes", "gets")}
               for arm in ("exact", "pq")}
    overlap = []
    wins = ties = losses = 0
    count = 0
    for request, exact, planned, result in itertools.zip_longest(
            jsonl(requests), jsonl(reference), jsonl(plans), jsonl(raw)):
        if any(item is None for item in (request, exact, planned, result)):
            raise ValueError("row count differs")
        if any(item["query_ordinal"] != count
               for item in (request, exact, planned, result)):
            raise ValueError("query ordinal differs")
        nominees, primary = request["nominees"], exact["primary"]
        expected_overlap = len(set(nominees[:100]) & set(primary))
        if (result["primary_overlap"] != expected_overlap
                or planned["primary_overlap"] != expected_overlap
                or planned["exact_ranges"] != exact["ranges"]
                or planned["exact_bytes"] != exact["plan_bytes"]):
            raise ValueError(f"plan/primary differs at {count}")
        _, pq_ranges, pq_bytes, _ = route_reference(
            nominees[:100], nominees, rows=ROWS, dimensions=DIMS,
        )
        if planned["pq_ranges"] != pq_ranges or planned["pq_bytes"] != pq_bytes:
            raise ValueError(f"PQ-primary route differs at {count}")
        overlap.append(expected_overlap)
        gold_ids = set(map(int, gold[count]))
        for arm in ("exact", "pq"):
            ranges = planned[f"{arm}_ranges"]
            found = result["arms"][arm]
            returned = found["returned_ids"]
            if (len(returned) != 100 or len(set(returned)) != 100
                    or any(type(value) is not int or not 0 <= value < ROWS
                           for value in returned)
                    or found["bytes"] != planned[f"{arm}_bytes"]
                    or found["bytes"] != sum(end - start for start, end in ranges)
                    or found["bytes"] > 16_777_216
                    or found["gets"] != len(ranges) or len(ranges) > 32):
                raise ValueError(f"result geometry differs at {count} {arm}")
            def fetched(identifier: int) -> bool:
                offset = int(physical[identifier]) * dtype.itemsize
                return any(start <= offset < end for start, end in ranges)
            if not all(fetched(value) for value in returned):
                raise ValueError(f"returned ID was not fetched at {count} {arm}")
            expected = {"hits": len(set(returned) & gold_ids),
                        "physical_coverage": sum(fetched(value) for value in gold_ids),
                        "bytes": found["bytes"], "gets": found["gets"]}
            if (found["hits"] != expected["hits"]
                    or found["physical_coverage"] != expected["physical_coverage"]
                    or expected["hits"] > expected["physical_coverage"]):
                raise ValueError(f"result recount differs at {count} {arm}")
            for field, value in expected.items():
                metrics[arm][field].append(value)
        pq_hits = metrics["pq"]["hits"][-1]
        exact_hits = metrics["exact"]["hits"][-1]
        wins += pq_hits > exact_hits
        ties += pq_hits == exact_hits
        losses += pq_hits < exact_hits
        count += 1
    if count != QUERIES:
        raise ValueError("query count differs")
    summary = json.loads(summary_path.read_text())
    if (summary["schema"] != "borsuk-v158-pq-primary-returned-summary-v1"
            or summary["queries"] != QUERIES
            or summary["truth_sha256"] != HASHES["truth"]
            or summary["plan_seal_sha256"] != sha256(seal)
            or summary["raw_sha256"] != sha256(raw)
            or summary["primary_overlap"] != distribution(overlap)
            or summary["paired"] != {"pq_wins": wins, "ties": ties,
                                     "pq_losses": losses}):
        raise ValueError("summary identity differs")
    for arm in metrics:
        if (summary["arms"][arm] != {field: distribution(values)
                                     for field, values in metrics[arm].items()}
                or summary["below_90"][arm] != sum(
                    value < 90 for value in metrics[arm]["hits"])):
            raise ValueError(f"summary {arm} differs")
    e = summary["arms"]["exact"]["hits"]
    p = summary["arms"]["pq"]["hits"]
    passes = (p["sum"] >= 99_000 and p["sum"] >= e["sum"] - 100
              and p["p05"] >= 98 and p["p05"] >= e["p05"] - 1
              and summary["below_90"]["pq"] <= summary["below_90"]["exact"])
    if summary["passes_100k_gate"] is not passes:
        raise ValueError("gate verdict differs")
    return {"schema": "borsuk-v158-independent-check-v1", "status": "pass",
            "queries": count, "passes_100k_gate": passes}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("requests", "reference", "sq8", "truth", "plans",
                 "seal", "raw", "summary"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.requests, args.reference, args.sq8,
                            args.truth, args.plans, args.seal, args.raw,
                            args.summary), sort_keys=True))
