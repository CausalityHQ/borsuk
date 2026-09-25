#!/usr/bin/env python3
"""Fresh sealed source screen for PQ utility and priced physical intervals."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np

from scripts.pq_cosine_margin_utility import (
    SCALE, MarginUtility, fit_margin_utility, normalized_pq_margins,
)
from scripts.predicted_interval_prices import QueryUtility, select_priced_plans
from scripts.priced_interval_oracle import priced_cover
from scripts.source_rank_utility import RankUtility, fit_rank_utility, rank_units
from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_probe import select_pseudoqueries
from scripts.v166_surrogate_ranking_run import _score, canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v187_cosine_physical_plan import plan_arm, score_intervals
from scripts.source_cover_frontier import CoverFrontier
from scripts.v165_unit_interval_resources import UNIT_ROWS

SCHEMA = "borsuk-v189-predicted-interval-source-v1"
FIRST, COUNT, FIT = 2432, 256, 128
WIDTH, UNIT_COUNT, UNIT_BYTES = 32, ROWS // UNIT_ROWS, 24_960
MAX_GETS, MAX_UNITS = 32, 672
TOTAL_GETS, TOTAL_UNITS = 2_832, 57_097
THRESHOLD = 12_745
PRICE_GRID = tuple((unit, get) for unit in
                   (1000, 2000, 3000, 4000, 6000, 10000, 20000)
                   for get in (0, 50000, 100000, 200000, 400000))
ARMS = ("margin_priced", "rank_priced", "greedy_control")


def validate_intervals(intervals: tuple[tuple[int, int], ...],
                       mandatory: tuple[int, ...], *, page_count: int,
                       max_units: int, max_gets: int) -> tuple[int, int, int]:
    """Independent whole-unit witness check, including bridge units."""
    if (not intervals or not mandatory or len(intervals) > max_gets
            or any(type(start) is not int or type(end) is not int
                   or not 0 <= start <= end < page_count
                   for start, end in intervals)
            or any(left[1] >= right[0] for left, right in
                   zip(intervals, intervals[1:]))
            or any(not any(start <= unit <= end for start, end in intervals)
                   for unit in mandatory)):
        raise ValueError("V189 interval/mandatory geometry differs")
    units = sum(end - start + 1 for start, end in intervals)
    if units > max_units:
        raise ValueError("V189 per-query unit cap exceeded")
    return units, len(intervals), units * UNIT_BYTES


def decide(holdout: dict[str, int]) -> str:
    if (holdout["hits"] >= THRESHOLD and holdout["p05"] >= 98
            and holdout["infeasible"] == 0
            and holdout["bytes"] <= 1_425_152_901
            and holdout["gets"] <= TOTAL_GETS):
        return "advance-to-replication"
    return "revise-utility-or-layout"


def _panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def _margins(feature: dict) -> dict[int, float]:
    return {int(unit): float(value) for unit, value in feature["margins"]}


def _rank_scores(feature: dict) -> dict[int, float]:
    return {unit: float(rank) for rank, unit in
            enumerate(feature["ranked_units"])}


def rank_weights(model: RankUtility, ranked_units: list[int]) -> dict[int, int]:
    """Use the same millionth-hit price scale as margin utility."""
    return model.weights(ranked_units, units_per_hit=SCALE)


def candidate_truth(truth_by_unit: Mapping[int, int],
                    ranked_units: Sequence[int]) -> dict[int, int]:
    """Fit rank utility only on scored units; account outside truth separately."""
    candidates = set(ranked_units)
    return {unit: int(hits) for unit, hits in truth_by_unit.items()
            if unit in candidates}


def _features(args: argparse.Namespace, prepare_sha: str) -> tuple[dict, list[dict]]:
    if sha256(args.output / "prepare-seal.json") != prepare_sha:
        raise ValueError("V189 prepare seal hash differs")
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("candidate_width") != WIDTH
            or seal.get("price_grid") != [list(pair) for pair in PRICE_GRID]
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V189 sealed feature authority differs")
    for row in features:
        ranked, mandatory, margins = (row["ranked_units"],
                                      row["mandatory_units"], _margins(row))
        if (not ranked or len(ranked) != len(set(ranked))
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(ranked)
                or set(margins) != set(ranked)
                or any(type(unit) is not int or not 0 <= unit < UNIT_COUNT
                       for unit in ranked)):
            raise ValueError("V189 candidate feature geometry differs")
    return seal, features


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V189 output already exists")
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
                raise ValueError("V189 leave-one-out nominee roster differs")
            nominees = all_nominees[all_nominees != own_old][:512]
            if nominees.shape != (512,):
                raise ValueError("V189 replacement nominee differs")
            _, sq8_scores = _score(sq8, query, nominees, planes["low"],
                                   planes["step"], 1)
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
                raise ValueError("V189 PQ candidate geometry differs")
            nominee_positions = {int(row): i for i, row in
                                 enumerate(map(int, field.old_rows))}
            if any(int(row) not in nominee_positions for row in nominees):
                raise ValueError("V189 nominee outside scored PQ field")
            nominee_scores = [float(field.scores[nominee_positions[int(row)]])
                              for row in nominees]
            scores = field.scores.reshape(len(candidate), UNIT_ROWS).copy()
            own = np.flatnonzero(field.old_rows == own_old)
            if own.size > 1:
                raise ValueError("V189 source row appears twice")
            if own.size:
                scores.reshape(-1)[own[0]] = np.inf
            minima = scores.min(axis=1)
            minimum_by_unit = dict(zip(candidate, map(float, minima), strict=True))
            margins = normalized_pq_margins(minimum_by_unit, nominee_scores)
            ranked = rank_units(minimum_by_unit)
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row),
                "mandatory_units": mandatory,
                "ranked_units": ranked,
                "margins": sorted(margins.items()),
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-2433-2688",
        "fit": "2433-2560", "holdout": "2561-2688",
        "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "candidate_width": WIDTH, "price_grid": PRICE_GRID,
        "nominee_shortlist": 512, "replacement_shortlist": 513,
        "feature_metric": "pq_reconstructed_cosine_only",
    }))


def _priced_plan(feature: dict, weights: dict[int, int],
                 price: tuple[int, int] | None) -> dict:
    if price is None:
        return {"feasible": False, "intervals": [], "units": 0,
                "gets": 0, "bytes": 0, "predicted_mass": 0}
    plan = priced_cover(weights, tuple(feature["mandatory_units"]),
                        page_count=UNIT_COUNT, max_gets=MAX_GETS,
                        hit_scale=1, unit_price=price[0], get_price=price[1])
    feasible = plan.units <= MAX_UNITS and plan.gets <= MAX_GETS
    return {"feasible": feasible,
            "intervals": [list(pair) for pair in plan.intervals],
            "units": plan.units, "gets": plan.gets,
            "bytes": plan.units * UNIT_BYTES,
            "predicted_mass": plan.hits}


def fit_plan(args: argparse.Namespace) -> None:
    seal, features = _features(args, args.prepare_sha256)
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != seal["pseudo_ids"]:
        raise ValueError("V189 source panel differs")
    source = _normalized(vectors)
    fit_masses = []
    with (args.output / "fit-labels.jsonl").open("x") as output:
        for feature in features[:FIT]:
            source_row = feature["source_row"]
            if int(source_ids[source_row]) != feature["source_id"]:
                raise ValueError("V189 fit source identity differs")
            truth = _truth(source, source_ids, source_row)
            masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
            if sum(masses.values()) != 100:
                raise ValueError("V189 fit GT100 differs")
            fit_masses.append(masses)
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "truth_by_unit": sorted(masses.items())}))
    del source
    planner_cpu_start = time.process_time()
    planner_wall_start = time.monotonic()
    margin_model = fit_margin_utility(
        (_margins(feature), masses)
        for feature, masses in zip(features[:FIT], fit_masses, strict=True))
    rank_model = fit_rank_utility(
        (_rank_scores(feature), candidate_truth(masses, feature["ranked_units"]))
        for feature, masses in zip(features[:FIT], fit_masses, strict=True))
    models = {"margin_priced": asdict(margin_model),
              "rank_priced": asdict(rank_model)}
    chosen: dict[str, tuple[int, int] | None] = {}
    for arm in ("margin_priced", "rank_priced"):
        weights = [(margin_model.weights(_margins(feature)) if arm == "margin_priced"
                    else rank_weights(rank_model, feature["ranked_units"]))
                   for feature in features[:FIT]]
        queries = tuple(QueryUtility(tuple(feature["mandatory_units"]),
                                     weight, UNIT_COUNT)
                        for feature, weight in zip(features[:FIT], weights,
                                                   strict=True))
        try:
            selection = select_priced_plans(
                queries, prices=PRICE_GRID, max_gets_per_query=MAX_GETS,
                max_units_per_query=MAX_UNITS,
                max_total_units=TOTAL_UNITS, max_total_gets=TOTAL_GETS)
            chosen[arm] = (selection.unit_price, selection.get_price)
        except ValueError as error:
            if str(error) != "no feasible price under physical caps":
                raise
            chosen[arm] = None
    (args.output / "model.json").write_text(canonical({
        "schema": SCHEMA + "-model", "models": models,
        "chosen_prices": chosen,
        "fit_labels_sha256": sha256(args.output / "fit-labels.jsonl"),
    }))
    with (args.output / "plans.jsonl").open("x") as output:
        for feature in features:
            arms = {}
            for arm in ("margin_priced", "rank_priced"):
                weights = (margin_model.weights(_margins(feature))
                           if arm == "margin_priced" else
                           rank_weights(rank_model, feature["ranked_units"]))
                arms[arm] = _priced_plan(feature, weights, chosen[arm])
            arms["greedy_control"] = plan_arm(
                tuple(feature["mandatory_units"]),
                tuple(feature["ranked_units"]),
                max_gets=MAX_GETS, unit_cap=446, floor_elastic=True)
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "arms": arms}))
    (args.output / "planner-runtime.json").write_text(canonical({
        "schema": SCHEMA + "-planner-runtime",
        "model_fit_and_256_plan_cpu_seconds":
            time.process_time() - planner_cpu_start,
        "model_fit_and_256_plan_wall_seconds":
            time.monotonic() - planner_wall_start,
        "source_truth_cpu_excluded": True,
    }))
    (args.output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "holdout_truth_opened": False,
        "prepare_seal_sha256": args.prepare_sha256,
        "features_sha256": sha256(args.output / "features.jsonl"),
        "fit_labels_sha256": sha256(args.output / "fit-labels.jsonl"),
        "model_sha256": sha256(args.output / "model.json"),
        "plans_sha256": sha256(args.output / "plans.jsonl"),
        "planner_runtime_sha256": sha256(args.output / "planner-runtime.json"),
        "arms": ARMS, "unit_bytes": UNIT_BYTES,
        "per_query_caps": [MAX_GETS, MAX_UNITS],
        "aggregate_caps": [TOTAL_GETS, TOTAL_UNITS],
    }))


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "plan-seal.json") != args.plan_sha256:
        raise ValueError("V189 externally sealed plan hash differs")
    plan_seal = json.loads((args.output / "plan-seal.json").read_text())
    prepare_sha = plan_seal["prepare_seal_sha256"]
    prepare_seal, features = _features(args, prepare_sha)
    plans = records(args.output / "plans.jsonl")
    model = json.loads((args.output / "model.json").read_text())
    if (plan_seal.get("schema") != SCHEMA + "-plan-seal"
            or plan_seal.get("holdout_truth_opened") is not False
            or plan_seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or plan_seal.get("fit_labels_sha256") != sha256(args.output / "fit-labels.jsonl")
            or plan_seal.get("model_sha256") != sha256(args.output / "model.json")
            or plan_seal.get("plans_sha256") != sha256(args.output / "plans.jsonl")
            or plan_seal.get("planner_runtime_sha256")
                != sha256(args.output / "planner-runtime.json")
            or tuple(plan_seal.get("arms", ())) != ARMS
            or len(plans) != COUNT
            or model.get("schema") != SCHEMA + "-model"):
        raise ValueError("V189 sealed plan authority differs")
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != prepare_seal["pseudo_ids"]:
        raise ValueError("V189 source panel differs")
    margin_model = MarginUtility(**{key: tuple(value) for key, value in
                                    model["models"]["margin_priced"].items()})
    rank_model = RankUtility(**{key: tuple(value) for key, value in
                               model["models"]["rank_priced"].items()})
    chosen = model["chosen_prices"]
    if set(chosen) != {"margin_priced", "rank_priced"}:
        raise ValueError("V189 chosen price arm set differs")
    fit_labels = records(args.output / "fit-labels.jsonl")
    if len(fit_labels) != FIT:
        raise ValueError("V189 fit label count differs")
    fit_masses = [dict(row["truth_by_unit"]) for row in fit_labels]
    refit_margin = fit_margin_utility(
        (_margins(feature), masses)
        for feature, masses in zip(features[:FIT], fit_masses, strict=True))
    refit_rank = fit_rank_utility(
        (_rank_scores(feature), candidate_truth(masses, feature["ranked_units"]))
        for feature, masses in zip(features[:FIT], fit_masses, strict=True))
    if refit_margin != margin_model or refit_rank != rank_model:
        raise ValueError("V189 fit model replay differs")
    source = _normalized(vectors)
    results = {split: {arm: {"hits": [], "bytes": 0, "gets": 0,
                            "units": 0, "infeasible": 0,
                            "max_query_units": 0, "max_query_gets": 0}
                       for arm in ARMS}
               for split in ("fit", "holdout")}
    candidate_hits = {"fit": 0, "holdout": 0}
    top446_hits = {"fit": 0, "holdout": 0}
    mandatory_floor = {"fit": {"units": 0, "max_units": 0},
                       "holdout": {"units": 0, "max_units": 0}}
    with (args.output / "source-labels.jsonl").open("x") as output:
        for index, (feature, plan_row) in enumerate(zip(features, plans,
                                                        strict=True)):
            if (plan_row.get("ordinal") != feature["ordinal"]
                    or plan_row.get("source_id") != feature["source_id"]
                    or int(source_ids[feature["source_row"]]) != feature["source_id"]
                    or set(plan_row.get("arms", {})) != set(ARMS)):
                raise ValueError("V189 query/plan identity differs")
            expected_arms = {}
            for arm in ("margin_priced", "rank_priced"):
                weights = (margin_model.weights(_margins(feature))
                           if arm == "margin_priced" else
                           rank_weights(rank_model, feature["ranked_units"]))
                price = chosen[arm]
                if price is not None and (len(price) != 2
                                          or tuple(price) not in PRICE_GRID):
                    raise ValueError("V189 chosen price differs")
                expected_arms[arm] = _priced_plan(
                    feature, weights, tuple(price) if price is not None else None)
            expected_arms["greedy_control"] = plan_arm(
                tuple(feature["mandatory_units"]),
                tuple(feature["ranked_units"]),
                max_gets=MAX_GETS, unit_cap=446, floor_elastic=True)
            if plan_row["arms"] != expected_arms:
                raise ValueError("V189 sealed plan replay differs")
            truth = _truth(source, source_ids, feature["source_row"])
            masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
            if sum(masses.values()) != 100:
                raise ValueError("V189 GT100 mass differs")
            if index < FIT and (fit_labels[index].get("ordinal") != feature["ordinal"]
                                or fit_labels[index].get("source_id") != feature["source_id"]
                                or fit_labels[index].get("truth_by_unit")
                                != [list(pair) for pair in sorted(masses.items())]):
                raise ValueError("V189 fit truth replay differs")
            split = "fit" if index < FIT else "holdout"
            ranked = feature["ranked_units"]
            floor = CoverFrontier(
                UNIT_COUNT, MAX_GETS, UNIT_COUNT,
                tuple(feature["mandatory_units"])).minimum_units
            mandatory_floor[split]["units"] += floor
            mandatory_floor[split]["max_units"] = max(
                mandatory_floor[split]["max_units"], floor)
            candidate_hits[split] += sum(masses.get(unit, 0) for unit in ranked)
            top446_hits[split] += sum(masses.get(unit, 0) for unit in ranked[:446])
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
                        raise ValueError("V189 sealed physical charges differ")
                elif item["feasible"] or item["units"] or item["gets"] or item["bytes"]:
                    raise ValueError("V189 empty plan accounting differs")
                cell = results[split][arm]
                value = score_intervals(masses, item["intervals"]) if item["feasible"] else 0
                hits[arm] = value
                cell["hits"].append(value)
                cell["bytes"] += item["bytes"]
                cell["gets"] += item["gets"]
                cell["units"] += item["units"]
                cell["infeasible"] += int(not item["feasible"])
                cell["max_query_units"] = max(cell["max_query_units"], item["units"])
                cell["max_query_gets"] = max(cell["max_query_gets"], item["gets"])
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "split": split,
                                    "truth_by_unit": sorted(masses.items()),
                                    "hits": hits}))
    for split in results:
        for cell in results[split].values():
            per_query = cell["hits"]
            cell["hits"] = sum(per_query)
            cell["p05"] = sorted(per_query)[(FIT * 5 + 99) // 100 - 1]
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-2433-2688",
        "fit": "2433-2560", "holdout": "2561-2688",
        "candidate_hits": candidate_hits, "top446_hits": top446_hits,
        "mandatory_floor": mandatory_floor,
        "planner_runtime": json.loads((args.output / "planner-runtime.json").read_text()),
        "results": results, "decision": decide(results["holdout"]["margin_priced"]),
        "plan_seal_sha256": args.plan_sha256,
        "source_labels_sha256": sha256(args.output / "source-labels.jsonl"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "fit-plan", "evaluate"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--old-layout", type=Path, required=True)
    parser.add_argument("--old-sq8", type=Path, required=True)
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--order", type=Path, required=True)
    parser.add_argument("--v164-terminal", type=Path, required=True)
    parser.add_argument("--prepare-sha256", default="")
    parser.add_argument("--plan-sha256", default="")
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
    elif args.phase == "fit-plan":
        if len(args.prepare_sha256) != 64:
            raise ValueError("V189 prepare SHA missing")
        fit_plan(args)
    else:
        if len(args.plan_sha256) != 64:
            raise ValueError("V189 plan SHA missing")
        evaluate(args)


if __name__ == "__main__":
    main()
