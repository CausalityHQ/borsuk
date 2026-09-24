#!/usr/bin/env python3
"""GT-blind source PQ rank admission into exact GET-limited covers."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from scripts.source_cover_frontier import CoverFrontier

SCHEMA = "borsuk-v183-source-physical-admission-v1"
V182_COMMIT = "34fd11f56559cc7d453f3bd04c92aba83d62d5fc"
V182_TERMINAL_SHA = "fa3de27a1d56747dbc4e2afd1af3a976e88ed5b90626d4f6dd8f525f426841cb"
ROWS = 1_000_000
UNIT_ROWS = 32
UNIT_BYTES = 24_960
FIRST = 1152
COUNT = 256
FIT = 128
HIT_THRESHOLD = 12745
V155_BYTES = 11_134_007_040
V155_GETS = 22_126
ARMS = {"v155_mean": (22, 446), "elastic_16m": (32, 672),
        "elastic_32m": (32, 1344)}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def _v182(root: Path, artifacts: tuple[str, ...]) -> dict:
    if sha256(root / "terminal.json") != V182_TERMINAL_SHA:
        raise ValueError("V183 V182 terminal identity differs")
    terminal = json.loads((root / "terminal.json").read_text())
    if (terminal.get("status") != "complete" or terminal.get("exit_code") != 0
            or terminal.get("source_commit") != V182_COMMIT):
        raise ValueError("V183 V182 terminal state differs")
    for name in artifacts:
        expected = terminal.get("artifacts", {}).get("out/" + name, {}).get("sha256")
        if not expected or sha256(root / name) != expected:
            raise ValueError(f"V183 V182 artifact identity differs: {name}")
    return terminal


def _features(root: Path) -> list[dict]:
    _v182(root, ("features.jsonl", "fit-seal.json"))
    rows = records(root / "features.jsonl")
    if (len(rows) != COUNT
            or [row.get("ordinal") for row in rows] != list(range(FIRST, FIRST + COUNT))
            or any(not isinstance(row.get("ranked_units"), list)
                   or not row["ranked_units"]
                   or len(set(row["ranked_units"])) != len(row["ranked_units"])
                   or any(type(unit) is not int or not 0 <= unit < ROWS // UNIT_ROWS
                          for unit in row["ranked_units"])
                   or row.get("mandatory_units")
                        != sorted(set(row.get("mandatory_units", [])))
                   or not set(row["mandatory_units"]).issubset(row["ranked_units"])
                   for row in rows)):
        raise ValueError("V183 V182 feature geometry differs")
    return rows


def plan(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V183 output already exists")
    features = _features(args.v182)
    args.output.mkdir(parents=True)
    with (args.output / "plans.jsonl").open("x") as output:
        for feature in features:
            mandatory = feature["mandatory_units"]
            plans = {}
            for arm, (gets, base_units) in ARMS.items():
                floor = CoverFrontier(ROWS // UNIT_ROWS, gets,
                                      ROWS // UNIT_ROWS, mandatory).minimum_units
                unit_cap = max(base_units, floor)
                frontier = CoverFrontier(ROWS // UNIT_ROWS, gets,
                                         unit_cap, mandatory)
                accepted = frontier.admit_ranked(feature["ranked_units"])
                intervals = frontier.intervals()
                if (frontier.minimum_units > unit_cap or len(intervals) > gets
                        or any(not any(start <= unit <= end for start, end in intervals)
                               for unit in mandatory)):
                    raise AssertionError("V183 primary cover plan differs")
                plans[arm] = {
                    "max_gets": gets, "base_units": base_units,
                    "mandatory_floor_units": floor, "unit_cap": unit_cap,
                    "accepted_optional_units": len(accepted),
                    "gets": len(intervals),
                    "units": frontier.minimum_units,
                    "bytes": frontier.minimum_units * UNIT_BYTES,
                    "intervals": [list(interval) for interval in intervals],
                }
            output.write(canonical({
                "ordinal": feature["ordinal"],
                "source_id": feature["source_id"], "arms": plans,
            }))
    (args.output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "truth_opened": False,
        "v182_terminal_sha256": V182_TERMINAL_SHA,
        "v182_features_sha256": sha256(args.v182 / "features.jsonl"),
        "v182_fit_seal_sha256": sha256(args.v182 / "fit-seal.json"),
        "plans_sha256": sha256(args.output / "plans.jsonl"),
        "arms": ARMS, "unit_bytes": UNIT_BYTES,
    }))


def _score(intervals: list[list[int]], masses: dict[int, int]) -> int:
    return sum(count for unit, count in masses.items()
               if any(start <= unit <= end for start, end in intervals))


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "plan-seal.json") != args.plan_sha256:
        raise ValueError("V183 externally sealed plan SHA differs")
    seal = json.loads((args.output / "plan-seal.json").read_text())
    features = _features(args.v182)
    _v182(args.v182, ("fit-labels.jsonl", "holdout-labels.jsonl"))
    plans = records(args.output / "plans.jsonl")
    labels = records(args.v182 / "fit-labels.jsonl") + records(
        args.v182 / "holdout-labels.jsonl")
    if (seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("truth_opened") is not False
            or seal.get("plans_sha256") != sha256(args.output / "plans.jsonl")
            or seal.get("v182_terminal_sha256") != V182_TERMINAL_SHA
            or seal.get("v182_features_sha256") != sha256(args.v182 / "features.jsonl")
            or seal.get("v182_fit_seal_sha256") != sha256(args.v182 / "fit-seal.json")
            or seal.get("arms") != {arm: list(value) for arm, value in ARMS.items()}
            or len(plans) != COUNT or len(labels) != COUNT):
        raise ValueError("V183 plan or label authority differs")
    metrics = {split: {arm: {"lower": [], "upper": [], "bytes": 0,
                             "gets": 0, "floors_above_base": 0}
                       for arm in ARMS}
               for split in ("fit", "holdout")}
    with (args.output / "scored-plans.jsonl").open("x") as output:
        for index, (feature, plan_row, label) in enumerate(
                zip(features, plans, labels, strict=True)):
            if (feature["ordinal"] != plan_row.get("ordinal")
                    or feature["ordinal"] != label.get("ordinal")
                    or feature["source_id"] != plan_row.get("source_id")
                    or feature["source_id"] != label.get("source_id")):
                raise ValueError("V183 row identity differs")
            masses = dict(label["candidate_truth_by_unit"])
            if (sum(masses.values()) != label["candidate_hits"]
                    or not set(masses).issubset(feature["ranked_units"])):
                raise ValueError("V183 candidate truth accounting differs")
            outside = 100 - label["candidate_hits"]
            split = "fit" if index < FIT else "holdout"
            scored = {}
            for arm, plan in plan_row["arms"].items():
                if arm not in ARMS:
                    raise ValueError("V183 unknown plan arm")
                intervals = plan["intervals"]
                if (plan["max_gets"] != ARMS[arm][0]
                        or plan["base_units"] != ARMS[arm][1]
                        or plan["unit_cap"] < plan["mandatory_floor_units"]
                        or plan["unit_cap"] != max(
                            plan["base_units"], plan["mandatory_floor_units"])
                        or len(intervals) != plan["gets"]
                        or len(intervals) > plan["max_gets"]
                        or any(not isinstance(interval, list)
                               or len(interval) != 2
                               or any(type(unit) is not int for unit in interval)
                               or not 0 <= interval[0] <= interval[1]
                                      < ROWS // UNIT_ROWS
                               for interval in intervals)
                        or any(left[1] >= right[0]
                               for left, right in zip(intervals, intervals[1:]))
                        or plan["units"] != sum(end - start + 1
                                                for start, end in intervals)
                        or plan["units"] > plan["unit_cap"]
                        or plan["bytes"] != plan["units"] * UNIT_BYTES
                        or any(not any(start <= unit <= end for start, end in intervals)
                               for unit in feature["mandatory_units"])):
                    raise ValueError("V183 physical plan witness differs")
                lower = _score(intervals, masses)
                upper = min(100, lower + outside)
                scored[arm] = {"lower": lower, "upper": upper}
                cell = metrics[split][arm]
                cell["lower"].append(lower)
                cell["upper"].append(upper)
                cell["bytes"] += plan["bytes"]
                cell["gets"] += plan["gets"]
                cell["floors_above_base"] += int(
                    plan["mandatory_floor_units"] > plan["base_units"])
            if set(scored) != set(ARMS):
                raise ValueError("V183 plan arm set differs")
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "split": split, "arms": scored}))
    summary = {}
    for split, arms in metrics.items():
        summary[split] = {}
        for arm, cell in arms.items():
            summary[split][arm] = {
                "lower_hits": sum(cell["lower"]),
                "upper_hits": sum(cell["upper"]),
                "lower_p05": sorted(cell["lower"])[(FIT * 5 + 99) // 100 - 1],
                "upper_p05": sorted(cell["upper"])[(FIT * 5 + 99) // 100 - 1],
                "bytes": cell["bytes"], "gets": cell["gets"],
                "floors_above_base": cell["floors_above_base"],
            }
    holdout = summary["holdout"]
    qualifies = {arm: cell["lower_hits"] >= HIT_THRESHOLD
                 and cell["lower_p05"] >= 98 for arm, cell in holdout.items()}
    baseline = holdout["v155_mean"]
    resources_ok = (baseline["bytes"] * 1000 <= V155_BYTES * FIT
                    and baseline["gets"] * 1000 <= V155_GETS * FIT)
    if qualifies["v155_mean"] and resources_ok:
        decision = "advance-to-independent-used-query-gate"
    elif any(qualifies.values()):
        decision = "quality-only-revise-resources"
    elif any(cell["upper_hits"] >= HIT_THRESHOLD and cell["upper_p05"] >= 98
             for cell in holdout.values()):
        decision = "inconclusive-outside-candidate-truth"
    else:
        decision = "revise-physical-layout-or-utility"
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "physical_interval_plan": True, "candidate_truth_lower_bound": True,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1153-1408",
        "query_count_per_split": FIT,
        "results": summary, "quality_qualifies": qualifies,
        "v155_mean_resources_ok": resources_ok,
        "v155_scaled_bytes_numerator": V155_BYTES * FIT,
        "v155_scaled_gets_numerator": V155_GETS * FIT,
        "v155_scaled_denominator": 1000,
        "decision": decision, "hit_threshold": HIT_THRESHOLD,
        "plan_seal_sha256": args.plan_sha256,
        "scored_plans_sha256": sha256(args.output / "scored-plans.jsonl"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("plan", "evaluate"))
    parser.add_argument("--v182", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plan-sha256", default="")
    args = parser.parse_args()
    if args.phase == "plan":
        plan(args)
    else:
        if len(args.plan_sha256) != 64:
            raise ValueError("V183 evaluation requires externally sealed plan SHA")
        evaluate(args)


if __name__ == "__main__":
    main()
