#!/usr/bin/env python3
"""Frozen V194 planner and V196 resident precision on a disjoint 1M panel."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np

from scripts.hard_priced_interval import hard_priced_cover
from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_ranking_run import canonical, records
from scripts.v177_source_candidate_ceiling import _inputs
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v189_predicted_interval_source import validate_intervals
from scripts.v192_optional_rank_plan_fit import MAX_TRACE_BYTES
from scripts.v193_optional_100k_transfer import _models
from scripts.v195_used_1m_rerank_diagnostic import _floor, _rerank
import scripts.v194_fresh_1m_optional_source as v194

SCHEMA = "borsuk-v197-fresh-resident-fp16-source-v1"
FIRST, COUNT = 3456, 512
SPLIT = "source-pseudoquery-hash-ranks-3457-3968"
ARMS = ("optional_risk", "full_rank")
PRICES = {"optional_risk": (1000, 50000), "full_rank": (2000, 50000)}
TARGET_HITS = 50_979
SOURCE_SHA = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86"
PLANE_SHA = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47"


def _features(args: argparse.Namespace, expected_sha: str) -> tuple[dict, list[dict]]:
    seal_path = args.output / "prepare-seal.json"
    if sha256(seal_path) != expected_sha:
        raise ValueError("V197 prepare seal differs")
    seal = json.loads(seal_path.read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("split") != SPLIT
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or len(features) != COUNT
            or [item["ordinal"] for item in features] != list(range(FIRST, FIRST + COUNT))
            or [item["source_id"] for item in features] != seal["pseudo_ids"]):
        raise ValueError("V197 GT-blind feature authority differs")
    for row in features:
        ranked, mandatory = row["ranked_units"], row["mandatory_units"]
        if (not ranked or len(set(ranked)) != len(ranked)
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(ranked)):
            raise ValueError("V197 feature geometry differs")
    return seal, features


def prepare(args: argparse.Namespace) -> None:
    v194.FIRST = FIRST
    v194.SCHEMA = SCHEMA
    v194.prepare(args)
    path = args.output / "prepare-seal.json"
    seal = json.loads(path.read_text())
    if seal.get("split") != "source-pseudoquery-hash-ranks-2945-3456":
        raise ValueError("V194 preparation split literal changed")
    seal["split"] = SPLIT
    seal["resident_plane_sha256"] = PLANE_SHA
    path.write_text(canonical(seal))


def plan(args: argparse.Namespace) -> None:
    seal, features = _features(args, args.prepare_sha256)
    optional, full, _ = _models(args)
    with (args.output / "plans.jsonl").open("x") as output:
        for feature in features:
            ranked = tuple(feature["ranked_units"])
            mandatory = tuple(feature["mandatory_units"])
            floor = _floor(list(mandatory), v194.MAX_GETS)
            cap = max(v194.MAX_UNITS, floor)
            weights = {
                "optional_risk": optional.weights(
                    ranked, mandatory, units_per_hit=1_000_000),
                "full_rank": full.weights(ranked, units_per_hit=1_000_000),
            }
            arms = {}
            for name in ARMS:
                try:
                    cover = hard_priced_cover(
                        weights[name], mandatory, page_count=v194.UNIT_COUNT,
                        max_gets=v194.MAX_GETS, max_units=cap,
                        unit_price=PRICES[name][0], get_price=PRICES[name][1],
                        max_trace_bytes=MAX_TRACE_BYTES)
                    arms[name] = {"feasible": True,
                                  "intervals": [list(pair) for pair in cover.intervals],
                                  "units": cover.units,
                                  "bytes": cover.units * v194.UNIT_BYTES,
                                  "gets": cover.gets,
                                  "predicted_mass": cover.mass}
                except ValueError as error:
                    if str(error) not in {"mandatory cover infeasible within hard caps",
                                          "hard priced interval trace budget exceeded"}:
                        raise
                    arms[name] = {"feasible": False, "reason": str(error),
                                  "intervals": [], "units": 0,
                                  "bytes": 0, "gets": 0}
            output.write(canonical({
                "ordinal": feature["ordinal"], "source_id": feature["source_id"],
                "mandatory_floor": floor, "unit_cap": cap, "arms": arms,
            }))
    (args.output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "source_truth_opened": False,
        "split": SPLIT, "prepare_seal_sha256": args.prepare_sha256,
        "features_sha256": seal["features_sha256"],
        "plans_sha256": sha256(args.output / "plans.jsonl"),
        "arms": ARMS, "prices": PRICES,
        "base_unit_cap": v194.MAX_UNITS,
        "max_gets": v194.MAX_GETS,
        "max_trace_bytes": MAX_TRACE_BYTES,
        "shortlist": 128, "resident_plane_sha256": PLANE_SHA,
        "aggregate_caps": [v194.TOTAL_BYTES, v194.TOTAL_GETS],
    }))


def _cell(values: list[dict], name: str, field: str) -> dict:
    hits = [row["arms"][name][field + "_hits"]
            if row["arms"][name]["feasible"] else 0 for row in values]
    ordered = sorted(hits)
    result = {"hits": sum(hits), "p05_hits": ordered[25],
              "min_hits": ordered[0],
              "below_98": sum(hit < 98 for hit in hits),
              "below_98_wilson95": v194._wilson95(
                  sum(hit < 98 for hit in hits), COUNT)}
    if field == "fp16":
        result.update({"coverage": sum(row["arms"][name].get("coverage", 0)
                                       for row in values),
                       "bytes": sum(row["arms"][name].get("bytes", 0)
                                    for row in values),
                       "gets": sum(row["arms"][name].get("gets", 0)
                                   for row in values),
                       "infeasible": sum(not row["arms"][name]["feasible"]
                                         for row in values)})
    return result


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "plan-seal.json") != args.plan_sha256:
        raise ValueError("V197 external GT-blind plan seal differs")
    plan_seal = json.loads((args.output / "plan-seal.json").read_text())
    _, features = _features(args, plan_seal["prepare_seal_sha256"])
    plans = records(args.output / "plans.jsonl")
    if (plan_seal.get("schema") != SCHEMA + "-plan-seal"
            or plan_seal.get("source_truth_opened") is not False
            or plan_seal.get("split") != SPLIT
            or plan_seal.get("plans_sha256") != sha256(args.output / "plans.jsonl")
            or plan_seal.get("resident_plane_sha256") != PLANE_SHA
            or len(plans) != COUNT):
        raise ValueError("V197 GT-blind plan authority differs")
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    if len(set(source_ids)) != ROWS or sha256(args.plane) != PLANE_SHA:
        raise ValueError("V197 source or resident plane differs")
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS, dtype=np.int64)
    new_order = np.argsort(inverse_new).astype(np.int64)
    new_sq8 = np.memmap(args.output / "new-sq8.bin", dtype=sq8.dtype,
                        mode="w+", shape=(ROWS,))
    for start in range(0, ROWS, 8192):
        stop = min(start + 8192, ROWS)
        new_sq8[start:stop] = sq8[inverse_old[new_order[start:stop]]]
    new_sq8.flush()
    if not np.array_equal(new_sq8["id"], source_ids[new_order]):
        raise ValueError("V197 relaid SQ8 ID mapping differs")
    source = _normalized(vectors)
    low = np.asarray(planes["low"], dtype=np.float32)
    step = np.asarray(planes["step"], dtype=np.float32)
    positions = {int(identifier): row for row, identifier in enumerate(source_ids)}
    raw = []
    with ((args.output / "raw.jsonl").open("x") as output,
          (args.output / "cases.jsonl").open("x") as cases):
        for index, (feature, planned) in enumerate(zip(features, plans, strict=True)):
            ordinal = FIRST + index
            stable_id = feature["source_id"]
            source_row = feature["source_row"]
            if (planned["ordinal"] != ordinal or feature["ordinal"] != ordinal
                    or planned["source_id"] != stable_id
                    or int(source_ids[source_row]) != stable_id
                    or planned["mandatory_floor"] !=
                    _floor(feature["mandatory_units"], v194.MAX_GETS)
                    or planned["unit_cap"] != max(
                        v194.MAX_UNITS, planned["mandatory_floor"])):
                raise ValueError("V197 query/plan identity differs")
            gold_rows = _truth(source, source_ids, source_row)
            gold = set(map(int, source_ids[gold_rows]))
            masses = Counter(map(int, inverse_new[gold_rows] // v194.UNIT_ROWS))
            candidate = sum(masses.get(unit, 0) for unit in feature["ranked_units"])
            query = np.asarray(vectors[source_row], dtype=np.float32)
            row = {"ordinal": ordinal, "source_id": stable_id,
                   "gold_ids": sorted(gold), "candidate_ceiling": candidate,
                   "mandatory_floor": planned["mandatory_floor"],
                   "unit_cap": planned["unit_cap"], "arms": {}}
            for name in ARMS:
                arm = planned["arms"][name]
                if not arm["feasible"]:
                    row["arms"][name] = {"feasible": False,
                                         "reason": arm["reason"]}
                    continue
                intervals = tuple(tuple(pair) for pair in arm["intervals"])
                units, gets, bytes_read = validate_intervals(
                    intervals, tuple(feature["mandatory_units"]),
                    page_count=v194.UNIT_COUNT,
                    max_units=planned["unit_cap"], max_gets=v194.MAX_GETS)
                if (units, gets, bytes_read) != (arm["units"], arm["gets"], arm["bytes"]):
                    raise ValueError("V197 physical accounting differs")
                ranges = [[start * v194.UNIT_BYTES, (end + 1) * v194.UNIT_BYTES]
                          for start, end in intervals]
                sq8_ids = score_sq8_ranges(new_sq8, query, low, step,
                                           ranges, top_k=129)
                shortlist = [identifier for identifier in sq8_ids
                             if identifier != stable_id][:128]
                if len(shortlist) != 128 or len(set(shortlist)) != 128:
                    raise ValueError("V197 SQ8 shortlist differs")
                sq8_returned = shortlist[:100]
                fp16_returned = _rerank(shortlist, positions, vectors, source,
                                        source_row, fp16=True)
                coverage = sum(count for unit, count in masses.items()
                               if any(start <= unit <= end for start, end in intervals))
                fp16_hits = len(set(fp16_returned) & gold)
                sq8_hits = len(set(sq8_returned) & gold)
                if fp16_hits > coverage or sq8_hits > coverage:
                    raise ValueError("V197 returned GT100 exceeds physical coverage")
                row["arms"][name] = {
                    "feasible": True, "coverage": coverage,
                    "units": units, "bytes": bytes_read, "gets": gets,
                    "sq8_returned_ids": sq8_returned, "sq8_hits": sq8_hits,
                    "fp16_returned_ids": fp16_returned,
                    "fp16_hits": fp16_hits,
                    "shortlist_truth": len(set(shortlist) & gold),
                }
                if name == "optional_risk":
                    cases.write(canonical({
                        "ordinal": ordinal, "query": query.tolist(),
                        "candidates": [{
                            "ordinal": int(inverse_new[positions[identifier]]),
                            "source_id": int(identifier)} for identifier in shortlist],
                        "expected": fp16_returned,
                    }))
            output.write(canonical(row))
            raw.append(row)
    optional = _cell(raw, "optional_risk", "fp16")
    full = _cell(raw, "full_rank", "fp16")
    optional_sq8 = _cell(raw, "optional_risk", "sq8")
    qualifies = (optional["hits"] >= TARGET_HITS
                 and optional["p05_hits"] >= 98
                 and optional["infeasible"] == 0
                 and optional["bytes"] <= v194.TOTAL_BYTES
                 and optional["gets"] <= v194.TOTAL_GETS)
    summary = {"schema": SCHEMA + "-summary",
               "dataset": "ReLAION-1M D768", "split": SPLIT,
               "queries": COUNT,
               "candidate_ceiling_hits": sum(row["candidate_ceiling"] for row in raw),
               "optional_fp16": optional, "optional_sq8": optional_sq8,
               "full_rank_fp16": full,
               "paired_fp16_minus_sq8_hits": optional["hits"] - optional_sq8["hits"],
               "paired_optional_minus_full_hits": optional["hits"] - full["hits"],
               "decision": "advance-real-query-live-s3" if qualifies else
                           "revise-candidate-plan-or-precision",
               "raw_sha256": sha256(args.output / "raw.jsonl"),
               "cases_sha256": sha256(args.output / "cases.jsonl"),
               "resident_case_count": COUNT - optional["infeasible"],
               "plan_seal_sha256": args.plan_sha256,
               "resident_plane_sha256": PLANE_SHA}
    (args.output / "summary.json").write_text(canonical(summary))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "plan", "evaluate"))
    for name in ("output", "source", "old_layout", "old_sq8", "router",
                 "order", "v164_terminal", "v192_result", "v189_features",
                 "v189_fit_labels", "plane"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path,
                            required=True)
    parser.add_argument("--prepare-sha256", default="")
    parser.add_argument("--plan-sha256", default="")
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
    elif args.phase == "plan":
        plan(args)
    else:
        evaluate(args)


if __name__ == "__main__":
    main()
