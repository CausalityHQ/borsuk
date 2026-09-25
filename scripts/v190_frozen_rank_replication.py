#!/usr/bin/env python3
"""Untouched source replication of V189's frozen rank-price policy."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import numpy as np

from scripts.source_rank_utility import RankUtility, rank_units
from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_probe import select_pseudoqueries
from scripts.v166_surrogate_ranking_run import _score, canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v187_cosine_physical_plan import plan_arm, score_intervals
from scripts.v189_predicted_interval_source import (
    UNIT_BYTES, UNIT_COUNT, UNIT_ROWS, _priced_plan, rank_weights,
    validate_intervals,
)

SCHEMA = "borsuk-v190-frozen-rank-replication-v1"
FIRST, COUNT, WIDTH = 2688, 256, 32
MODEL_SHA = "cef01b55c5a196db187dc303dd002f33b00e75c6083465cf8ff746ea3d9beb5c"
PRICE = (6000, 50000)
ARMS = ("rank_priced", "greedy_control")
MAX_UNITS, MAX_GETS = 672, 32
TOTAL_BYTES, TOTAL_GETS = 2_850_305_802, 5664


def decide(rank: dict[str, int]) -> str:
    if (rank["hits"] >= 25490 and rank["p05"] >= 98
            and rank["infeasible"] == 0
            and rank["bytes"] <= TOTAL_BYTES
            and rank["gets"] <= TOTAL_GETS):
        return "advance-to-paired-used-validation"
    return "reject-frozen-rank-price"


def _panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def _features(args: argparse.Namespace, prepare_sha: str) -> tuple[dict, list[dict]]:
    if sha256(args.output / "prepare-seal.json") != prepare_sha:
        raise ValueError("V190 prepare seal hash differs")
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("frozen_model_sha256") != MODEL_SHA
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V190 sealed feature authority differs")
    for row in features:
        ranked, mandatory = row["ranked_units"], row["mandatory_units"]
        if (not ranked or len(ranked) != len(set(ranked))
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(ranked)
                or any(type(unit) is not int or not 0 <= unit < UNIT_COUNT
                       for unit in ranked)):
            raise ValueError("V190 candidate geometry differs")
    return seal, features


def _model(path: Path) -> RankUtility:
    if sha256(path) != MODEL_SHA:
        raise ValueError("V190 frozen V189 model SHA differs")
    raw = json.loads(path.read_text())
    if (raw.get("schema") != "borsuk-v189-predicted-interval-source-v1-model"
            or raw.get("chosen_prices", {}).get("rank_priced") != list(PRICE)):
        raise ValueError("V190 frozen V189 price/model differs")
    return RankUtility(**{key: tuple(value) for key, value in
                          raw["models"]["rank_priced"].items()})


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V190 output already exists")
    _model(args.frozen_model)
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    chosen = _panel(source_ids)
    id_to_source = {int(identifier): row for row, identifier in enumerate(source_ids)}
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS, dtype=np.int64)
    new_order = np.argsort(inverse_new).astype(np.int64)
    args.output.mkdir(parents=True)
    with (args.output / "features.jsonl").open("x") as output:
        for ordinal, stable_id in enumerate(chosen, start=FIRST):
            source_row = id_to_source[stable_id]
            own_old = int(inverse_old[source_row])
            query = np.asarray(vectors[source_row], dtype=np.float32)
            _, _, nominated = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=513)
            _, _, direct512 = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
            all_nominees = np.asarray(nominated, dtype=np.int64)
            if (all_nominees.shape != (513,)
                    or not np.array_equal(all_nominees[:512], direct512)
                    or len(np.unique(all_nominees)) != 513
                    or not np.any(all_nominees == own_old)):
                raise ValueError("V190 leave-one-out nominee roster differs")
            nominees = all_nominees[all_nominees != own_old][:512]
            _, sq8_scores = _score(sq8, query, nominees,
                                   planes["low"], planes["step"], 1)
            primary = nominees[np.lexsort((sq8["id"][nominees],
                                           sq8_scores))[:100]]
            base = sorted(set((inverse_new[old[nominees]] // UNIT_ROWS).tolist()))
            mandatory = sorted(set((inverse_new[old[primary]] // UNIT_ROWS).tolist()))
            candidate = expanded_units(base, WIDTH, UNIT_COUNT)
            field = score_neighbor_field(
                query, nominees=tuple(map(int, nominees)), old_order=old,
                inverse_old=inverse_old, new_order=new_order,
                inverse_new=inverse_new, books=planes["books"],
                codes=planes["codes"], unit_rows=UNIT_ROWS,
                radius=WIDTH, metric="cosine")
            if (field.units != candidate
                    or field.scores.size != len(candidate) * UNIT_ROWS):
                raise ValueError("V190 PQ candidate geometry differs")
            scores = field.scores.reshape(len(candidate), UNIT_ROWS).copy()
            own = np.flatnonzero(field.old_rows == own_old)
            if own.size > 1:
                raise ValueError("V190 source row appears twice")
            if own.size:
                scores.reshape(-1)[own[0]] = np.inf
            minima = scores.min(axis=1)
            ranked = rank_units(dict(zip(candidate, map(float, minima), strict=True)))
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row),
                "mandatory_units": mandatory, "ranked_units": ranked,
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-2689-2944",
        "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "frozen_model_sha256": MODEL_SHA,
        "frozen_price": PRICE, "candidate_width": WIDTH,
    }))


def plan(args: argparse.Namespace) -> None:
    seal, features = _features(args, args.prepare_sha256)
    model = _model(args.frozen_model)
    started_cpu, started_wall = time.process_time(), time.monotonic()
    with (args.output / "plans.jsonl").open("x") as output:
        for feature in features:
            weights = rank_weights(model, feature["ranked_units"])
            arms = {"rank_priced": _priced_plan(feature, weights, PRICE),
                    "greedy_control": plan_arm(
                        tuple(feature["mandatory_units"]),
                        tuple(feature["ranked_units"]),
                        max_gets=MAX_GETS, unit_cap=446,
                        floor_elastic=True)}
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "arms": arms}))
    (args.output / "planner-runtime.json").write_text(canonical({
        "schema": SCHEMA + "-planner-runtime",
        "plan_256_cpu_seconds": time.process_time() - started_cpu,
        "plan_256_wall_seconds": time.monotonic() - started_wall,
    }))
    (args.output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "source_truth_opened": False,
        "prepare_seal_sha256": args.prepare_sha256,
        "features_sha256": seal["features_sha256"],
        "frozen_model_sha256": MODEL_SHA, "frozen_price": PRICE,
        "plans_sha256": sha256(args.output / "plans.jsonl"),
        "planner_runtime_sha256": sha256(args.output / "planner-runtime.json"),
        "arms": ARMS, "per_query_caps": [MAX_GETS, MAX_UNITS],
        "aggregate_caps": [TOTAL_GETS, TOTAL_BYTES],
    }))


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "plan-seal.json") != args.plan_sha256:
        raise ValueError("V190 externally sealed plan SHA differs")
    plan_seal = json.loads((args.output / "plan-seal.json").read_text())
    prepare_seal, features = _features(args, plan_seal["prepare_seal_sha256"])
    model = _model(args.frozen_model)
    plans = records(args.output / "plans.jsonl")
    if (plan_seal.get("schema") != SCHEMA + "-plan-seal"
            or plan_seal.get("source_truth_opened") is not False
            or plan_seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or plan_seal.get("plans_sha256") != sha256(args.output / "plans.jsonl")
            or plan_seal.get("planner_runtime_sha256")
                != sha256(args.output / "planner-runtime.json")
            or plan_seal.get("frozen_model_sha256") != MODEL_SHA
            or plan_seal.get("frozen_price") != list(PRICE)
            or tuple(plan_seal.get("arms", ())) != ARMS
            or len(plans) != COUNT):
        raise ValueError("V190 sealed plan authority differs")
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != prepare_seal["pseudo_ids"]:
        raise ValueError("V190 source panel differs")
    source = _normalized(vectors)
    results = {arm: {"hits": [], "units": 0, "gets": 0,
                     "bytes": 0, "infeasible": 0,
                     "max_query_units": 0, "max_query_gets": 0}
               for arm in ARMS}
    candidate_hits = top446_hits = 0
    with (args.output / "source-labels.jsonl").open("x") as output:
        for index, (feature, plan_row) in enumerate(zip(features, plans,
                                                        strict=True)):
            if (plan_row.get("ordinal") != FIRST + index
                    or plan_row.get("source_id") != feature["source_id"]
                    or int(source_ids[feature["source_row"]]) != feature["source_id"]
                    or set(plan_row.get("arms", {})) != set(ARMS)):
                raise ValueError("V190 query/plan identity differs")
            weights = rank_weights(model, feature["ranked_units"])
            expected = {"rank_priced": _priced_plan(feature, weights, PRICE),
                        "greedy_control": plan_arm(
                            tuple(feature["mandatory_units"]),
                            tuple(feature["ranked_units"]),
                            max_gets=MAX_GETS, unit_cap=446,
                            floor_elastic=True)}
            if plan_row["arms"] != expected:
                raise ValueError("V190 sealed plan replay differs")
            truth = _truth(source, source_ids, feature["source_row"])
            masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
            if sum(masses.values()) != 100:
                raise ValueError("V190 GT100 mass differs")
            ranked = feature["ranked_units"]
            candidate_hits += sum(masses.get(unit, 0) for unit in ranked)
            top446_hits += sum(masses.get(unit, 0) for unit in ranked[:446])
            hits = {}
            for arm, item in plan_row["arms"].items():
                intervals = tuple(tuple(pair) for pair in item["intervals"])
                if intervals:
                    units, gets, bytes_read = validate_intervals(
                        intervals, tuple(feature["mandatory_units"]),
                        page_count=UNIT_COUNT, max_units=UNIT_COUNT,
                        max_gets=MAX_GETS)
                    if (units != item["units"] or gets != item["gets"]
                            or bytes_read != item["bytes"]
                            or item["feasible"] != (units <= MAX_UNITS)):
                        raise ValueError("V190 physical charge differs")
                elif item["feasible"] or item["units"] or item["gets"] or item["bytes"]:
                    raise ValueError("V190 empty plan accounting differs")
                cell = results[arm]
                value = score_intervals(masses, item["intervals"]) if item["feasible"] else 0
                hits[arm] = value
                cell["hits"].append(value)
                cell["units"] += item["units"]
                cell["gets"] += item["gets"]
                cell["bytes"] += item["bytes"]
                cell["infeasible"] += int(not item["feasible"])
                cell["max_query_units"] = max(cell["max_query_units"], item["units"])
                cell["max_query_gets"] = max(cell["max_query_gets"], item["gets"])
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "truth_by_unit": sorted(masses.items()),
                                    "hits": hits}))
    for cell in results.values():
        per_query = cell["hits"]
        cell["hits"] = sum(per_query)
        cell["p05"] = sorted(per_query)[(COUNT * 5 + 99) // 100 - 1]
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-2689-2944",
        "candidate_hits": candidate_hits, "top446_hits": top446_hits,
        "results": results, "decision": decide(results["rank_priced"]),
        "planner_runtime": json.loads((args.output / "planner-runtime.json").read_text()),
        "plan_seal_sha256": args.plan_sha256,
        "source_labels_sha256": sha256(args.output / "source-labels.jsonl"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "plan", "evaluate"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--old-sq8", type=Path, required=True)
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--v164-terminal", type=Path, required=True)
    parser.add_argument("--frozen-model", type=Path, required=True)
    parser.add_argument("--prepare-sha256", default="")
    parser.add_argument("--plan-sha256", default="")
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
    elif args.phase == "plan":
        if len(args.prepare_sha256) != 64:
            raise ValueError("V190 prepare SHA missing")
        plan(args)
    else:
        if len(args.plan_sha256) != 64:
            raise ValueError("V190 plan SHA missing")
        evaluate(args)


if __name__ == "__main__":
    main()
