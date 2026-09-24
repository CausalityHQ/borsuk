#!/usr/bin/env python3
"""Recount every V159 coverage field against frozen inputs and raw rows."""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v158_pq_primary_returned import DIMS, ROWS, authenticate, jsonl, load_truth
from scripts.v159_route_loss_decomposition import (
    FIELDS, V158_PLAN_SHA, V158_RAW_SHA, hash_file,
)


def distribution(values: list[int]) -> dict:
    ordered = sorted(values)
    return {"min": ordered[0], "p05": ordered[49], "p50": ordered[499],
            "p95": ordered[949], "p99": ordered[989], "max": ordered[-1],
            "sum": sum(ordered)}


def verify(requests: Path, reference: Path, sq8_path: Path, truth_path: Path,
           plans: Path, raw: Path, output: Path, summary_path: Path) -> dict:
    for role, path in (("requests", requests), ("reference", reference),
                       ("sq8", sq8_path), ("truth", truth_path)):
        authenticate(path, role)
    if hash_file(plans) != V158_PLAN_SHA or hash_file(raw) != V158_RAW_SHA:
        raise ValueError("V158 input identity differs")
    dtype = np.dtype([("id", "<i8"), ("norm", "<f4"),
                      ("code", "u1", (DIMS,))])
    sq8 = np.memmap(sq8_path, dtype=dtype, mode="r", shape=(ROWS,))
    id_to_row = {int(value): row for row, value in enumerate(sq8["id"])}
    if len(id_to_row) != ROWS:
        raise ValueError("source IDs differ")
    truth = load_truth(truth_path)
    sums = {field: [] for field in FIELDS}
    count = 0
    for request, exact, plan, v158, measured in itertools.zip_longest(
            jsonl(requests), jsonl(reference), jsonl(plans), jsonl(raw), jsonl(output)):
        if any(item is None for item in (request, exact, plan, v158, measured)):
            raise ValueError("evidence row count differs")
        if any(item["query_ordinal"] != count
               for item in (request, exact, plan, v158, measured)):
            raise ValueError("query ordinal differs")
        gold = [id_to_row[int(value)] for value in truth[count]]
        source_rows = [request["nominees"], exact["primary"], request["nominees"][:100]]
        def count_rows(rows: list[int]) -> int:
            selected = set(rows)
            return sum(row in selected for row in gold)
        def count_pages(rows: list[int]) -> int:
            selected = {row // 256 for row in rows}
            return sum(row // 256 in selected for row in gold)
        expected = {}
        for prefix, rows in zip(("nominee", "exact_primary", "pq_primary"), source_rows):
            expected[prefix + "_rows"] = count_rows(rows)
            expected[prefix + "_pages"] = count_pages(rows)
        for arm in ("exact", "pq"):
            intervals = plan[arm + "_ranges"]
            expected[arm + "_plan"] = sum(any(
                start <= row * dtype.itemsize < end for start, end in intervals
            ) for row in gold)
            if expected[arm + "_plan"] != v158["arms"][arm]["physical_coverage"]:
                raise ValueError("V158 outcome differs")
        if measured != {"query_ordinal": count, **expected}:
            raise ValueError(f"raw coverage differs at {count}")
        for field, value in expected.items():
            sums[field].append(value)
        count += 1
    if count != 1000:
        raise ValueError("query count differs")
    summary = json.loads(summary_path.read_text())
    if (summary["schema"] != "borsuk-v159-route-loss-summary-v1"
            or summary["queries"] != count
            or summary["raw_sha256"] != hash_file(output)
            or summary["coverage"] != {field: distribution(values)
                                       for field, values in sums.items()}):
        raise ValueError("summary differs")
    return {"schema": "borsuk-v159-independent-check-v1",
            "queries": count, "status": "pass"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("requests", "reference", "sq8", "truth", "plans", "raw",
                 "output", "summary"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.requests, args.reference, args.sq8,
                            args.truth, args.plans, args.raw,
                            args.output, args.summary), sort_keys=True))
