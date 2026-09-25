#!/usr/bin/env python3
"""Sparse closed-fit physical ceiling under hard caps, never a serving arm."""

from __future__ import annotations

import argparse
import hashlib
import json
from math import ceil
from pathlib import Path

from scripts.hard_priced_interval import hard_priced_cover
from scripts.v192_optional_rank_fit_diagnostic import FEATURE_SHA, FIT_SHA

UNIT_COUNT, UNIT_BYTES = 31_250, 24_960


def _read(path: Path, expected: str) -> list[dict]:
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ValueError("V189 sealed fit input digest differs")
    return [json.loads(line) for line in path.read_text().splitlines()]


def _p05(values: list[int]) -> int:
    ordered = sorted(values)
    return ordered[ceil(.05 * len(ordered)) - 1]


def run(features_path: Path, labels_path: Path) -> dict:
    features = _read(features_path, FEATURE_SHA)[:128]
    labels = _read(labels_path, FIT_SHA)
    if (len(features) != 128 or len(labels) != 128
            or any(feature["ordinal"] != 2432 + i
                   or feature["ordinal"] != label["ordinal"]
                   or feature["source_id"] != label["source_id"]
                   for i, (feature, label) in
                   enumerate(zip(features, labels)))):
        raise ValueError("V189 fit identity differs")
    rows = []
    for feature, label in zip(features, labels):
        truth = {int(unit): int(hits)
                 for unit, hits in label["truth_by_unit"]}
        if sum(truth.values()) != 100:
            raise ValueError("V189 fit truth mass differs")
        candidate = set(feature["ranked_units"])
        mandatory = tuple(feature["mandatory_units"])
        candidate_truth = {unit: hits for unit, hits in truth.items()
                           if unit in candidate}
        plan = hard_priced_cover(
            candidate_truth, mandatory, page_count=UNIT_COUNT,
            max_gets=32, max_units=672, unit_price=0, get_price=0,
            max_trace_bytes=32 * 1024 * 1024)
        if (plan.units > 672 or plan.gets > 32
                or any(not any(a <= unit <= b for a, b in plan.intervals)
                       for unit in mandatory)):
            raise AssertionError("V192 truth oracle physical witness differs")
        full_hits = sum(
            hits for unit, hits in truth.items()
            if any(a <= unit <= b for a, b in plan.intervals))
        rows.append({
            "ordinal": feature["ordinal"], "source_id": feature["source_id"],
            "mandatory_hits": sum(truth.get(unit, 0) for unit in mandatory),
            "candidate_ceiling_hits": sum(candidate_truth.values()),
            "oracle_candidate_hits": plan.mass,
            "oracle_full_physical_hits": full_hits,
            "oracle_units": plan.units, "oracle_gets": plan.gets,
            "oracle_intervals": [list(pair) for pair in plan.intervals],
        })
    keys = ("mandatory_hits", "candidate_ceiling_hits",
            "oracle_candidate_hits", "oracle_full_physical_hits")
    return {
        "schema": "borsuk-v192-closed-fit-candidate-oracle-v1",
        "features_sha256": FEATURE_SHA, "fit_labels_sha256": FIT_SHA,
        "dataset": "ReLAION-1M D768",
        "split": "V189-closed-fit-source-pseudoquery-ordinals-2432-2559",
        "queries": len(rows),
        "metrics": {
            key: {"total": sum(row[key] for row in rows),
                  "p05_nearest_rank": _p05([row[key] for row in rows]),
                  "below_98_queries": sum(row[key] < 98 for row in rows)}
            for key in keys
        },
        "oracle_total_units": sum(row["oracle_units"] for row in rows),
        "oracle_planned_bytes": sum(row["oracle_units"] for row in rows)
                                * UNIT_BYTES,
        "oracle_total_gets": sum(row["oracle_gets"] for row in rows),
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--fit-labels", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(
        json.dumps(run(args.features, args.fit_labels), sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
