#!/usr/bin/env python3
"""Closed V189 fit-only check for optional-unit utility calibration."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict
from pathlib import Path

from scripts.optional_rank_utility import fit_optional_rank_utility
from scripts.source_rank_utility import fit_rank_utility

FEATURE_SHA = "7eb4833c76675534cde41330c5939599ab70d7871ac27cb365d62cdb840c3fbe"
FIT_SHA = "4023ade93d32e4aa4377a3f56e7e9b5d469468396e96459715caa5f55394ebf4"
SCALE = 1_000_000


def _read(path: Path, digest: str) -> list[dict]:
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise ValueError("V189 sealed input digest differs")
    return [json.loads(line) for line in path.read_text().splitlines()]


def run(features_path: Path, fit_path: Path) -> dict:
    features = _read(features_path, FEATURE_SHA)[:128]
    labels = _read(fit_path, FIT_SHA)
    if (len(features) != 128 or len(labels) != 128
            or any(feature["ordinal"] != 2432 + index
                   or feature["ordinal"] != label["ordinal"]
                   or feature["source_id"] != label["source_id"]
                   for index, (feature, label) in enumerate(zip(features, labels)))):
        raise ValueError("V189 fit identity differs")
    examples = []
    for feature, label in zip(features, labels):
        truth = {int(unit): int(hits) for unit, hits in label["truth_by_unit"]}
        if sum(truth.values()) != 100:
            raise ValueError("V189 fit GT100 mass differs")
        examples.append((tuple(feature["ranked_units"]),
                         tuple(feature["mandatory_units"]), truth))
    train = examples[:64]
    optional_model = fit_optional_rank_utility(train, result_count=100)
    full_rank = fit_rank_utility(
        ({unit: float(index) for index, unit in enumerate(ranked)},
         {unit: hits for unit, hits in truth.items() if unit in set(ranked)})
        for ranked, _, truth in train)
    train_optional_hits = 0
    for ranked, mandatory, truth in train:
        required = set(mandatory)
        train_optional_hits += sum(
            truth.get(unit, 0) for unit in ranked if unit not in required)
    constant_risk = train_optional_hits / len(train)
    result = {
        "schema": "borsuk-v192-optional-rank-fit-diagnostic-v1",
        "features_sha256": FEATURE_SHA, "fit_labels_sha256": FIT_SHA,
        "train": "V189-fit-ordinals-2432-2495",
        "fit_validation": "V189-fit-ordinals-2496-2559",
        "model": asdict(optional_model),
        "splits": {},
    }
    for name, rows in (("train", train), ("fit_validation", examples[64:])):
        actual, old_predicted, new_predicted = [], [], []
        top_optional_200_hits = 0
        for ranked, mandatory, truth in rows:
            required = set(mandatory)
            optional = tuple(unit for unit in ranked if unit not in required)
            actual.append(sum(truth.get(unit, 0) for unit in optional))
            top_optional_200_hits += sum(
                truth.get(unit, 0) for unit in optional[:200])
            old_weights = full_rank.weights(ranked, units_per_hit=SCALE)
            old_predicted.append(
                sum(old_weights.get(unit, 0) for unit in optional) / SCALE)
            new_weights = optional_model.weights(
                ranked, mandatory, units_per_hit=SCALE)
            new_predicted.append(sum(new_weights.values()) / SCALE)
        result["splits"][name] = {
            "queries": len(rows),
            "actual_candidate_optional_hits": sum(actual),
            "old_full_rank_predicted_optional_hits": sum(old_predicted),
            "new_predicted_optional_hits": sum(new_predicted),
            "constant_predicted_optional_hits": constant_risk * len(rows),
            "top_optional_200_truth_hits": top_optional_200_hits,
            "old_mean_absolute_error_hits_per_query":
                sum(abs(predicted - truth) for predicted, truth in
                    zip(old_predicted, actual)) / len(rows),
            "new_mean_absolute_error_hits_per_query":
                sum(abs(predicted - truth) for predicted, truth in
                    zip(new_predicted, actual)) / len(rows),
            "constant_mean_absolute_error_hits_per_query":
                sum(abs(constant_risk - truth) for truth in actual) / len(rows),
            "zero_mean_absolute_error_hits_per_query":
                sum(actual) / len(rows),
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--fit-labels", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run(args.features, args.fit_labels)
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()
