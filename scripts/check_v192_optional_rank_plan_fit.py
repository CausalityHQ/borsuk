#!/usr/bin/env python3
"""Independent completed-artifact replay of V192 fit-only physical plans."""

from __future__ import annotations

import argparse
import hashlib
import json
from math import ceil
from pathlib import Path

from scripts.v192_optional_rank_fit_diagnostic import FEATURE_SHA, FIT_SHA

RESULT_SHA = "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01"
SPLITS = {"price_fit": range(2496, 2528),
          "diagnostic": range(2528, 2560)}
ARMS = {"optional_risk", "full_rank_refit_price",
        "constant_risk", "greedy_control"}


def _read(path: Path, expected: str, *, lines: bool) -> object:
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected:
        raise ValueError("V192 replay input digest differs")
    if lines:
        return [json.loads(line) for line in content.splitlines()]
    return json.loads(content)


def check(result_path: Path, features_path: Path, labels_path: Path) -> dict:
    result = _read(result_path, RESULT_SHA, lines=False)
    features = _read(features_path, FEATURE_SHA, lines=True)[:128]
    labels = _read(labels_path, FIT_SHA, lines=True)
    if (result["schema"] != "borsuk-v192-optional-hard-plan-fit-v1"
            or result["status"] != "complete"
            or len(features) != 128 or len(labels) != 128):
        raise ValueError("V192 replay authority differs")
    cases = {}
    for feature, label in zip(features, labels):
        if (feature["ordinal"] != label["ordinal"]
                or feature["source_id"] != label["source_id"]):
            raise ValueError("V192 replay fit identity differs")
        truth = {int(unit): int(hits)
                 for unit, hits in label["truth_by_unit"]}
        if sum(truth.values()) != 100:
            raise ValueError("V192 replay GT100 mass differs")
        cases[feature["ordinal"]] = feature, truth
    checked_rows = 0
    for split, ordinals in SPLITS.items():
        arms = result["splits"][split]
        if set(arms) != ARMS:
            raise ValueError("V192 replay arm set differs")
        for arm in arms.values():
            rows = arm["queries"]
            if [row["ordinal"] for row in rows] != list(ordinals):
                raise ValueError("V192 replay ordinal range differs")
            hits = []
            units = gets = candidate = 0
            for row in rows:
                feature, truth = cases[row["ordinal"]]
                if row["source_id"] != feature["source_id"]:
                    raise ValueError("V192 replay source identity differs")
                intervals = [tuple(pair) for pair in row["intervals"]]
                if (not intervals
                        or any(not 0 <= start <= end < 31_250
                               for start, end in intervals)
                        or any(left[1] >= right[0]
                               for left, right in zip(intervals,
                                                      intervals[1:]))
                        or any(not any(start <= unit <= end
                                       for start, end in intervals)
                               for unit in feature["mandatory_units"])):
                    raise ValueError("V192 replay interval witness differs")
                used_units = sum(end - start + 1
                                 for start, end in intervals)
                used_gets = len(intervals)
                captured = sum(mass for unit, mass in truth.items()
                               if any(start <= unit <= end
                                      for start, end in intervals))
                ceiling = sum(truth.get(unit, 0)
                              for unit in feature["ranked_units"])
                if (used_units, used_gets, captured, ceiling) != (
                        row["units"], row["gets"], row["hits"],
                        row["candidate_ceiling_hits"]):
                    raise ValueError("V192 replay row accounting differs")
                if used_units > 672 or used_gets > 32:
                    raise ValueError("V192 replay hard cap exceeded")
                hits.append(captured)
                units += used_units
                gets += used_gets
                candidate += ceiling
                checked_rows += 1
            summary = arm["summary"]
            if (summary["queries"] != len(ordinals)
                    or summary["hits"] != sum(hits)
                    or summary["p05_nearest_rank_hits"]
                        != sorted(hits)[ceil(.05 * len(hits)) - 1]
                    or summary["units"] != units
                    or summary["planned_bytes"] != units * 24_960
                    or summary["gets"] != gets
                    or summary["candidate_ceiling_hits"] != candidate
                    or summary["over_aggregate_caps"]
                        != (units > 14_274 or gets > 708)):
                raise ValueError("V192 replay summary differs")
    if set(result["price_grids"]) != {"optional_risk", "full_rank"}:
        raise ValueError("V192 replay price arm set differs")
    for name, grid in result["price_grids"].items():
        feasible = [row for row in grid if row["feasible"]]
        if not feasible:
            raise ValueError("V192 replay price grid infeasible")
        winner = max(
            feasible,
            key=lambda row: (row["predicted_mass"], -row["units"],
                             -row["gets"], -row["unit_price"],
                             -row["get_price"]))
        if [winner["unit_price"], winner["get_price"]] != (
                result["selected_prices"][name]):
            raise ValueError("V192 replay price selection differs")
    return {"status": "pass", "checked_arm_query_rows": checked_rows,
            "selected_prices": result["selected_prices"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--fit-labels", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(args.result, args.features, args.fit_labels),
                     sort_keys=True))


if __name__ == "__main__":
    main()
