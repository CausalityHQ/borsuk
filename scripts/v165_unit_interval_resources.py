#!/usr/bin/env python3
"""V165 GT-blind 32-row interval resource screen on the closed V164 order."""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np

from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v155_relaion_returned_quality import (
    LAYOUT_SHA, QUERIES, REQUEST_SHA, ROWS, SEALED_SHA, sha256,
)
from scripts.v164_smooth_layout_1m import CAP_BYTES, CAP_GETS, ROW_BYTES

SCHEMA = "borsuk-v165-unit-interval-resources-v1"
V164_TERMINAL_SHA = "daa4025093ddef883358a200751972b9d953cd53be80681b9055677c3c7793c7"
V164_SOURCE_COMMIT = "488fc4702f6fd408385e532f67d0a110daf1ba33"
V164_ORDER_SHA = "5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f"
UNIT_ROWS = 32
UNIT_BYTES = UNIT_ROWS * ROW_BYTES
BASELINE_BYTES = 11_134_007_040
BASELINE_GETS = 22_126


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def jsonl(path: Path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def spread(values: list[int]) -> dict:
    ordered = sorted(values)
    return {"min": ordered[0], "p05": ordered[49], "p50": ordered[499],
            "p95": ordered[949], "p99": ordered[989], "max": ordered[-1],
            "sum": sum(ordered)}


def load_orders(old_layout: Path, order_path: Path,
                v164_terminal: Path) -> tuple[np.ndarray, np.ndarray]:
    if (sha256(old_layout) != LAYOUT_SHA
            or sha256(order_path) != V164_ORDER_SHA
            or sha256(v164_terminal) != V164_TERMINAL_SHA):
        raise ValueError("V165 closed physical-order identity differs")
    terminal = json.loads(v164_terminal.read_text())
    if (terminal.get("status") != "complete"
            or terminal.get("source_commit") != V164_SOURCE_COMMIT
            or terminal.get("artifacts", {}).get("order.npy", {}).get("sha256")
                != V164_ORDER_SHA):
        raise ValueError("V164 complete terminal does not bind order")
    old = np.load(old_layout, allow_pickle=False)
    order = np.load(order_path, allow_pickle=False)
    if (old.shape != (ROWS,) or order.shape != (ROWS,)
            or order.dtype != np.int64
            or not np.array_equal(np.sort(old), np.arange(ROWS))
            or not np.array_equal(np.sort(order), np.arange(ROWS))):
        raise ValueError("V165 order is not a source-ordinal permutation")
    inverse = np.empty(ROWS, dtype=np.int64)
    inverse[order] = np.arange(ROWS, dtype=np.int64)
    return old, inverse


def route(primary: list[int], nominees: list[int], old: np.ndarray,
          inverse: np.ndarray) -> tuple[list[list[int]], int, int]:
    if (len(primary) != 100 or len(nominees) != 512
            or len(set(primary)) != 100 or len(set(nominees)) != 512
            or not set(primary).issubset(nominees)
            or any(type(value) is not int or not 0 <= value < ROWS
                   for value in primary + nominees)):
        raise ValueError("V165 frozen roster differs")
    primary_set = set(primary)
    votes: dict[int, int] = {}
    for physical in nominees:
        unit = int(inverse[old[physical]]) // UNIT_ROWS
        votes[unit] = votes.get(unit, 0) + (513 if physical in primary_set else 1)
    if ROWS % UNIT_ROWS != 0 or CAP_BYTES // UNIT_BYTES != 672:
        raise ValueError("V165 unit geometry differs")
    score, intervals = optimal_weighted_intervals(
        votes, page_count=ROWS // UNIT_ROWS, max_gets=CAP_GETS,
        max_units=CAP_BYTES // UNIT_BYTES,
        full_page_units=1, last_page_units=1,
    )
    ranges = [[start * UNIT_BYTES, (end + 1) * UNIT_BYTES]
              for start, end in intervals]
    amount = sum(end - start for start, end in ranges)
    if (not 1 <= len(ranges) <= CAP_GETS or amount > CAP_BYTES
            or any(left[1] >= right[0] for left, right in zip(ranges, ranges[1:]))):
        raise ValueError("V165 physical cap differs")
    return ranges, amount, score


def plan(requests: Path, sealed: Path, old_layout: Path, order_path: Path,
         v164_terminal: Path, plans: Path, plan_seal: Path,
         summary: Path) -> None:
    if sha256(requests) != REQUEST_SHA or sha256(sealed) != SEALED_SHA:
        raise ValueError("V165 frozen query roster identity differs")
    old, inverse = load_orders(old_layout, order_path, v164_terminal)
    bytes_values: list[int] = []
    gets_values: list[int] = []
    count = 0
    with plans.open("x") as output:
        for request, frozen in itertools.zip_longest(jsonl(requests), jsonl(sealed)):
            if (request is None or frozen is None
                    or request.get("query_ordinal") != count
                    or frozen.get("query_ordinal") != count
                    or request.get("nominees") != frozen.get("nominees")):
                raise ValueError("V165 roster sequence differs")
            ranges, amount, score = route(frozen["primary"],
                                          request["nominees"], old, inverse)
            output.write(canonical({"query_ordinal": count, "ranges": ranges,
                                    "bytes": amount, "gets": len(ranges),
                                    "plan_score": score}))
            bytes_values.append(amount)
            gets_values.append(len(ranges))
            count += 1
    if count != QUERIES:
        raise ValueError("V165 query count differs")
    plan_seal.write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "gt_opened": False,
        "queries": count, "requests_sha256": REQUEST_SHA,
        "sealed_sha256": SEALED_SHA, "old_layout_sha256": LAYOUT_SHA,
        "v164_terminal_sha256": V164_TERMINAL_SHA,
        "order_sha256": V164_ORDER_SHA, "plans_sha256": sha256(plans),
    }))
    passed = sum(bytes_values) <= BASELINE_BYTES and sum(gets_values) <= BASELINE_GETS
    summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-1M",
        "split": "validation-1000-used", "queries": count, "gt_opened": False,
        "decision": "resource-pass" if passed else "killed",
        "bytes": spread(bytes_values), "gets": spread(gets_values),
        "baseline_bytes": BASELINE_BYTES, "baseline_gets": BASELINE_GETS,
        "plan_seal_sha256": sha256(plan_seal),
    }))


def check(requests: Path, sealed: Path, old_layout: Path, order_path: Path,
          v164_terminal: Path, plans: Path, plan_seal: Path,
          summary: Path) -> dict:
    if sha256(requests) != REQUEST_SHA or sha256(sealed) != SEALED_SHA:
        raise ValueError("V165 checker roster identity differs")
    old, inverse = load_orders(old_layout, order_path, v164_terminal)
    seal = json.loads(plan_seal.read_text())
    if (seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("gt_opened") is not False
            or seal.get("queries") != QUERIES
            or seal.get("plans_sha256") != sha256(plans)
            or seal.get("order_sha256") != V164_ORDER_SHA
            or seal.get("old_layout_sha256") != LAYOUT_SHA
            or seal.get("v164_terminal_sha256") != V164_TERMINAL_SHA
            or seal.get("requests_sha256") != REQUEST_SHA
            or seal.get("sealed_sha256") != SEALED_SHA):
        raise ValueError("V165 plan seal differs")
    bytes_values: list[int] = []
    gets_values: list[int] = []
    count = 0
    for request, frozen, planned in itertools.zip_longest(
            jsonl(requests), jsonl(sealed), jsonl(plans)):
        if (request is None or frozen is None or planned is None
                or request.get("query_ordinal") != count
                or frozen.get("query_ordinal") != count
                or planned.get("query_ordinal") != count):
            raise ValueError("V165 checker query sequence differs")
        ranges, amount, score = route(frozen["primary"],
                                      request["nominees"], old, inverse)
        if (request["nominees"] != frozen["nominees"]
                or planned["ranges"] != ranges or planned["bytes"] != amount
                or planned["gets"] != len(ranges)
                or planned["plan_score"] != score):
            raise ValueError(f"V165 checker plan differs at {count}")
        bytes_values.append(amount)
        gets_values.append(len(ranges))
        count += 1
    if count != QUERIES:
        raise ValueError("V165 checker query count differs")
    passed = sum(bytes_values) <= BASELINE_BYTES and sum(gets_values) <= BASELINE_GETS
    decision = "resource-pass" if passed else "killed"
    declared = json.loads(summary.read_text())
    if (declared.get("schema") != SCHEMA + "-summary"
            or declared.get("dataset") != "ReLAION-1M"
            or declared.get("split") != "validation-1000-used"
            or declared.get("queries") != QUERIES
            or declared.get("gt_opened") is not False
            or declared.get("decision") != decision
            or declared.get("bytes") != spread(bytes_values)
            or declared.get("gets") != spread(gets_values)
            or declared.get("baseline_bytes") != BASELINE_BYTES
            or declared.get("baseline_gets") != BASELINE_GETS
            or declared.get("plan_seal_sha256") != sha256(plan_seal)):
        raise ValueError("V165 checker summary differs")
    return {"schema": SCHEMA + "-check", "status": "pass",
            "queries": count, "decision": decision}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("plan", "check"))
    for name in ("requests", "sealed", "old_layout", "order", "v164_terminal",
                 "plans", "plan_seal", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    common = (args.requests, args.sealed, args.old_layout, args.order,
              args.v164_terminal, args.plans, args.plan_seal, args.summary)
    if args.phase == "plan":
        plan(*common)
    else:
        print(json.dumps(check(*common), sort_keys=True))
