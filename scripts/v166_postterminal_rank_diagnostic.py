#!/usr/bin/env python3
"""Recount V166's closed GT-blind rank and physical-cover diagnostics."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

import numpy as np

from scripts.v155_relaion_returned_quality import LAYOUT_SHA, sha256
from scripts.v165_unit_interval_resources import UNIT_BYTES, V164_ORDER_SHA

CASES_SHA = "81c1c4c03c3d1eeadab68bd7c4e2fe44accfbc715aaf4f6c030d41486d2b45f9"
PLANS_SHA = "1da05d75c864adb3b5dbcaec77b20b669b56386bf866eec0119dda05f2a11d16"
LABELS = ("0-99", "100-199", "200-299", "300-399", "400-499",
          "500-511", "neighbor-only")


def minimum_cover_bytes(required_units: list[int], max_gets: int) -> int:
    """Merge cheapest gaps to cover required units in at most max_gets."""
    if (not required_units or max_gets <= 0
            or required_units != sorted(set(required_units))
            or required_units[0] < 0):
        raise ValueError("V166 required unit set or GET cap differs")
    gaps = sorted((right - left - 1 for left, right in zip(
        required_units, required_units[1:])), reverse=True)
    kept = sum(gaps[:max_gets - 1])
    return (required_units[-1] - required_units[0] + 1 - kept) * UNIT_BYTES


def read_jsonl(path: Path) -> list[dict]:
    with path.open() as source:
        return [json.loads(line) for line in source]


def recount(cases_path: Path, plans_path: Path,
            old_layout_path: Path, order_path: Path) -> dict:
    if (sha256(cases_path) != CASES_SHA or sha256(plans_path) != PLANS_SHA
            or sha256(old_layout_path) != LAYOUT_SHA
            or sha256(order_path) != V164_ORDER_SHA):
        raise ValueError("V166 closed diagnostic input identity differs")
    cases = read_jsonl(cases_path)[128:]
    plans = read_jsonl(plans_path)
    if len(cases) != 128 or len(plans) != 128:
        raise ValueError("V166 holdout length differs")
    old = np.load(old_layout_path, allow_pickle=False)
    order = np.load(order_path, allow_pickle=False)
    if (old.shape != (1_000_000,) or order.shape != (1_000_000,)
            or not np.array_equal(np.sort(old), np.arange(1_000_000))
            or not np.array_equal(np.sort(order), np.arange(1_000_000))):
        raise ValueError("V166 closed physical orders differ")
    inverse = np.empty(1_000_000, dtype=np.int64)
    inverse[order] = np.arange(1_000_000, dtype=np.int64)
    bins = [[0, 0, 0] for _ in LABELS]
    oracle_bytes = []
    feasible = 0
    for case, plan in zip(cases, plans):
        if case["query_ordinal"] != plan["query_ordinal"]:
            raise ValueError("V166 closed query order differs")
        best_rank: dict[int, int] = {}
        for rank, old_physical in enumerate(case["nominees"]):
            unit = int(inverse[old[old_physical]]) // 32
            best_rank[unit] = min(best_rank.get(unit, 999), rank)
        required = set(case["primary_units"])
        for unit, _, _, _, actual, _ in case["unit_cases"]:
            category = min(best_rank.get(unit, 999) // 100, 6)
            bins[category][0] += actual
            if any(left <= unit <= right for left, right in plan["baseline_intervals"]):
                bins[category][1] += actual
            if any(left <= unit <= right for left, right in plan["model_intervals"]):
                bins[category][2] += actual
            if actual > 0:
                required.add(unit)
        amount = minimum_cover_bytes(sorted(required), plan["baseline_gets"])
        oracle_bytes.append(amount)
        feasible += amount <= plan["baseline_bytes"]
    return {"schema": "borsuk-v166-postterminal-rank-v1", "gt_opened": False,
            "dataset": "ReLAION-1M", "split": "source-pseudoquery-holdout-128",
            "rank_bins": {label: dict(zip(("actual", "baseline", "surrogate"), values))
                          for label, values in zip(LABELS, bins)},
            "oracle_min_bytes_for_primary_plus_actual": sum(oracle_bytes),
            "oracle_median_bytes": statistics.median(oracle_bytes),
            "oracle_fits_baseline_cap_queries": feasible,
            "baseline_bytes": sum(row["baseline_bytes"] for row in plans),
            "cases_sha256": CASES_SHA, "plans_sha256": PLANS_SHA,
            "old_layout_sha256": LAYOUT_SHA, "order_sha256": V164_ORDER_SHA}


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("cases", "plans", "old_layout", "order"):
        parser.add_argument("--" + name.replace("_", "-"), required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(recount(args.cases, args.plans, args.old_layout, args.order),
                     sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
