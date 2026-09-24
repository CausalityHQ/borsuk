#!/usr/bin/env python3
"""Locate GT100 losses along frozen nomination and physical-plan stages."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import (
    DIMS, QUERIES, ROWS, authenticate, jsonl, load_truth,
)

V158_TERMINAL_SHA = "09306fa1aca94748635eca32ac9259e934249ee1fb620c3ec333977517a2dd35"
V158_PLAN_SHA = "5d89b1b5f4bcaf019bd4091e46291412fe56ce0e5bdb9cca0178e4162a6c383a"
V158_RAW_SHA = "12a77f91b6b0898ae2b9ed17f0450556eec7bf0a2bdc9d2f4130bd71193e00ba"
FIELDS = ("nominee_rows", "nominee_pages", "exact_primary_rows",
          "exact_primary_pages", "pq_primary_rows", "pq_primary_pages",
          "exact_plan", "pq_plan")


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def spread(values: list[int]) -> dict:
    ordered = sorted(values)
    return {"min": ordered[0], "p05": ordered[49], "p50": ordered[499],
            "p95": ordered[949], "p99": ordered[989], "max": ordered[-1],
            "sum": sum(ordered)}


def evaluate(requests: Path, reference: Path, sq8_path: Path, truth_path: Path,
             v158_terminal: Path, v158_plans: Path, v158_raw: Path,
             output: Path, summary: Path) -> None:
    for role, path in (("requests", requests), ("reference", reference),
                       ("sq8", sq8_path), ("truth", truth_path)):
        authenticate(path, role)
    if (hash_file(v158_terminal) != V158_TERMINAL_SHA
            or hash_file(v158_plans) != V158_PLAN_SHA
            or hash_file(v158_raw) != V158_RAW_SHA):
        raise ValueError("V158 closed input identity differs")
    terminal = json.loads(v158_terminal.read_text())
    if (terminal.get("status") != "complete"
            or terminal.get("artifacts", {}).get("plans.jsonl", {}).get("sha256")
                != V158_PLAN_SHA
            or terminal.get("artifacts", {}).get("raw.jsonl", {}).get("sha256")
                != V158_RAW_SHA):
        raise ValueError("V158 terminal binding differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"),
                      ("code", "u1", (DIMS,))])
    sq8 = np.memmap(sq8_path, mode="r", dtype=dtype, shape=(ROWS,))
    identifiers = np.asarray(sq8["id"], dtype=np.int64)
    physical = {int(identifier): position
                for position, identifier in enumerate(identifiers)}
    if len(physical) != ROWS:
        raise ValueError("SQ8 IDs are not unique")
    gold = load_truth(truth_path)
    if any(int(value) not in physical for row in gold for value in row):
        raise ValueError("GT ID absent from SQ8 source")
    observed = {field: [] for field in FIELDS}
    count = 0
    with output.open("x") as dest:
        for request, reference_row, planned, result in itertools.zip_longest(
                jsonl(requests), jsonl(reference), jsonl(v158_plans), jsonl(v158_raw)):
            if any(item is None for item in (request, reference_row, planned, result)):
                raise ValueError("frozen row count differs")
            if any(item["query_ordinal"] != count
                   for item in (request, reference_row, planned, result)):
                raise ValueError("frozen query ordinal differs")
            nominees = request["nominees"]
            exact = reference_row["primary"]
            pq = nominees[:100]
            if (len(nominees) != 512 or len(set(nominees)) != 512
                    or len(exact) != 100 or len(set(exact)) != 100
                    or not set(exact).issubset(nominees)
                    or any(type(row) is not int or not 0 <= row < ROWS
                           for row in nominees + exact)):
                raise ValueError("roster geometry differs")
            truth = {physical[int(identifier)] for identifier in gold[count]}
            def rows_covered(rows: list[int]) -> int:
                return len(truth & set(rows))
            def pages_covered(rows: list[int]) -> int:
                pages = {row // 256 for row in rows}
                return sum(row // 256 in pages for row in truth)
            def ranges_covered(ranges: list[list[int]]) -> int:
                return sum(any(start <= row * dtype.itemsize < end
                               for start, end in ranges) for row in truth)
            values = {"nominee_rows": rows_covered(nominees),
                      "nominee_pages": pages_covered(nominees),
                      "exact_primary_rows": rows_covered(exact),
                      "exact_primary_pages": pages_covered(exact),
                      "pq_primary_rows": rows_covered(pq),
                      "pq_primary_pages": pages_covered(pq),
                      "exact_plan": ranges_covered(planned["exact_ranges"]),
                      "pq_plan": ranges_covered(planned["pq_ranges"])}
            if (values["exact_plan"] != result["arms"]["exact"]["physical_coverage"]
                    or values["pq_plan"] != result["arms"]["pq"]["physical_coverage"]
                    or values["nominee_rows"] > values["nominee_pages"]
                    or values["exact_primary_rows"] > values["exact_primary_pages"]
                    or values["pq_primary_rows"] > values["pq_primary_pages"]):
                raise ValueError("coverage or V158 binding differs")
            dest.write(json.dumps({"query_ordinal": count, **values},
                                  sort_keys=True, separators=(",", ":")) + "\n")
            for field, value in values.items():
                observed[field].append(value)
            count += 1
    if count != QUERIES:
        raise ValueError("query count differs")
    summary.write_text(json.dumps({
        "schema": "borsuk-v159-route-loss-summary-v1", "queries": count,
        "dataset": "ReLAION-100k", "split": "development-1000-used",
        "v158_terminal_sha256": V158_TERMINAL_SHA,
        "raw_sha256": hash_file(output),
        "coverage": {field: spread(values) for field, values in observed.items()},
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("requests", "reference", "sq8", "truth", "v158_terminal",
                 "v158_plans", "v158_raw", "output", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    evaluate(args.requests, args.reference, args.sq8, args.truth,
             args.v158_terminal, args.v158_plans, args.v158_raw,
             args.output, args.summary)
