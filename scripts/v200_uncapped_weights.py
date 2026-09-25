#!/usr/bin/env python3
"""Seal V198 GT-blind optional weights for the Rust linear-cover preflight."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from scripts.optional_rank_utility import OptionalRankUtility
from scripts.source_rank_utility import RankUtility

FEATURE_SHA = "3b4fe2d7a83bd0af16b1ecf460052b78311c6526bd7263fb3e708b56dd26a08c"
PLAN_SHA = "0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00"
FIT_SHA = "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01"


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare(features_path: Path, plans_path: Path, fit_path: Path,
            output: Path) -> dict:
    for path, expected in ((features_path, FEATURE_SHA),
                           (plans_path, PLAN_SHA), (fit_path, FIT_SHA)):
        if sha(path) != expected:
            raise ValueError(f"V200 input identity differs: {path}")
    fit = json.loads(fit_path.read_text())
    if (fit.get("schema") != "borsuk-v192-optional-hard-plan-fit-v1"
            or fit.get("status") != "complete"
            or fit.get("selected_prices", {}).get("optional_risk") != [1000, 50000]):
        raise ValueError("V200 frozen optional model policy differs")
    encoded = fit["model"]
    curve = RankUtility(tuple(encoded["rank_curve"]["expected_hits"]),
                        tuple(encoded["rank_curve"]["samples"]))
    model = OptionalRankUtility(curve, tuple(encoded["risk_edges"]),
                                tuple(encoded["risk_expected_hits"]),
                                encoded["fit_queries"], encoded["result_count"])
    features = [json.loads(line) for line in features_path.read_text().splitlines()]
    plans = [json.loads(line) for line in plans_path.read_text().splitlines()]
    if len(features) != 1000 or len(plans) != 1000:
        raise ValueError("V200 cohort length differs")
    with output.open("x") as target:
        for index, (feature, plan) in enumerate(zip(features, plans, strict=True)):
            if feature["ordinal"] != index or plan["ordinal"] != index:
                raise ValueError("V200 ordinal differs")
            weights = model.weights(feature["ranked_units"],
                                    feature["mandatory_units"],
                                    units_per_hit=1_000_000)
            target.write(json.dumps({
                "ordinal": index,
                "mandatory": feature["mandatory_units"],
                "weights": sorted([unit, weight] for unit, weight in weights.items()),
            },sort_keys=True,separators=(",", ":")) + "\n")
    return {"schema":"borsuk-v200-uncapped-weights-v1",
            "features_sha256":FEATURE_SHA,"plans_sha256":PLAN_SHA,
            "fit_sha256":FIT_SHA,"weights_sha256":sha(output),"queries":1000,
            "source_truth_opened":False}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("features",type=Path)
    parser.add_argument("plans",type=Path)
    parser.add_argument("fit",type=Path)
    parser.add_argument("output",type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.features,args.plans,args.fit,args.output),sort_keys=True))


if __name__ == "__main__":
    main()
