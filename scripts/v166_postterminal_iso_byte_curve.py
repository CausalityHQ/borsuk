#!/usr/bin/env python3
"""GT-blind V165 equal-vote capture curve on closed V166 pseudoqueries.

This is exploratory postterminal diagnosis, not a quality gate or a new
selected operating point. It uses SQ8 scores already sealed in V166 cases.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v114_weighted_interval_plan import optimal_weighted_intervals
from scripts.v155_relaion_returned_quality import LAYOUT_SHA, sha256
from scripts.v165_unit_interval_resources import UNIT_BYTES, V164_ORDER_SHA
from scripts.v166_postterminal_rank_diagnostic import CASES_SHA, PLANS_SHA

CAPS = (("6MiB", 6 * 1024 * 1024),
        ("8MiB", 8 * 1024 * 1024),
        ("V155-mean-B", 11_134_007),
        ("13.5MiB", 27 * 1024 * 1024 // 2),
        ("16MiB", 16 * 1024 * 1024))


def read_jsonl(path: Path) -> list[dict]:
    with path.open() as source:
        return [json.loads(line) for line in source]


def curve(cases_path: Path, plans_path: Path,
          old_path: Path, order_path: Path) -> dict:
    if (sha256(cases_path) != CASES_SHA or sha256(plans_path) != PLANS_SHA
            or sha256(old_path) != LAYOUT_SHA
            or sha256(order_path) != V164_ORDER_SHA):
        raise ValueError("V166 iso-byte input identity differs")
    cases = read_jsonl(cases_path)[128:]
    plans = read_jsonl(plans_path)
    if len(cases) != 128 or len(plans) != 128:
        raise ValueError("V166 iso-byte holdout count differs")
    old = np.load(old_path, allow_pickle=False)
    order = np.load(order_path, allow_pickle=False)
    if (old.shape != (1_000_000,) or order.shape != (1_000_000,)
            or not np.array_equal(np.sort(old), np.arange(1_000_000))
            or not np.array_equal(np.sort(order), np.arange(1_000_000))):
        raise ValueError("V166 iso-byte order differs")
    inverse = np.empty(1_000_000, dtype=np.int64)
    inverse[order] = np.arange(1_000_000)
    values = {label: {"captured": 0, "bytes": 0, "gets": 0,
                      "primary_complete_queries": 0}
              for label, _ in CAPS}
    for case, recorded in zip(cases, plans):
        if case["query_ordinal"] != recorded["query_ordinal"]:
            raise ValueError("V166 iso-byte query identity differs")
        primary = set(case["primary"])
        votes: dict[int, int] = {}
        for physical in case["nominees"]:
            unit = int(inverse[old[physical]]) // 32
            votes[unit] = votes.get(unit, 0) + (513 if physical in primary else 1)
        actual = {int(unit): int(count)
                  for unit, _, _, _, count, _ in case["unit_cases"]}
        for label, cap in CAPS:
            _, intervals = optimal_weighted_intervals(
                votes, page_count=31_250, max_gets=32,
                max_units=cap // UNIT_BYTES, full_page_units=1,
                last_page_units=1)
            if not intervals:
                raise ValueError("V166 iso-byte empty plan")
            if label == "16MiB" and [list(pair) for pair in intervals] != recorded["baseline_intervals"]:
                raise ValueError("V166 iso-byte 16MiB reproduction differs")
            covered = {unit for start, end in intervals
                       for unit in range(start, end + 1)}
            charged = len(covered) * UNIT_BYTES
            if charged > cap or len(intervals) > 32:
                raise ValueError("V166 iso-byte physical cap differs")
            entry = values[label]
            entry["captured"] += sum(count for unit, count in actual.items()
                                     if unit in covered)
            entry["bytes"] += charged
            entry["gets"] += len(intervals)
            entry["primary_complete_queries"] += set(case["primary_units"]).issubset(covered)
    return {"schema": "borsuk-v166-postterminal-iso-byte-v1", "gt_opened": False,
            "dataset": "ReLAION-1M", "split": "source-pseudoquery-holdout-128",
            "queries": 128, "candidate_universe_actual": sum(
                item[4] for case in cases for item in case["unit_cases"]),
            "caps": [{"label": label, "max_bytes_per_query": cap, **values[label]}
                     for label, cap in CAPS],
            "cases_sha256": CASES_SHA, "plans_sha256": PLANS_SHA,
            "old_layout_sha256": LAYOUT_SHA, "order_sha256": V164_ORDER_SHA}


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("cases", "plans", "old_layout", "order"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(curve(args.cases, args.plans, args.old_layout, args.order),
                     sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
