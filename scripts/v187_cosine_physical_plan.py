#!/usr/bin/env python3
"""GT-blind cosine-PQ physical interval plans and sealed source evaluation."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np

from scripts.source_rank_utility import rank_units
from scripts.source_cover_frontier import CoverFrontier
from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_probe import select_pseudoqueries
from scripts.v166_surrogate_ranking_run import _score, canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v165_unit_interval_resources import UNIT_ROWS

SCHEMA = "borsuk-v187-cosine-physical-plan-v1"
FIRST = 1920
COUNT = 256
FIT = 128
WIDTH = 32
UNIT_BYTES = 24_960
UNIT_COUNT = ROWS // UNIT_ROWS
ARMS = {"v155_mean_floor_elastic": (32, 446, True),
        "get32_units446": (32, 446, False),
        "elastic_16m": (32, 672, False)}
THRESHOLD = 12745
V155_BYTES = 11_134_007_040
V155_GETS = 22_126


def plan_arm(mandatory: tuple[int, ...], ranked: tuple[int, ...], *,
             max_gets: int, unit_cap: int,
             floor_elastic: bool = False) -> dict:
    floor = CoverFrontier(UNIT_COUNT, max_gets, UNIT_COUNT,
                          mandatory).minimum_units
    effective_cap = min(672, max(unit_cap, floor)) if floor_elastic else unit_cap
    result = {"max_gets": max_gets, "base_units": unit_cap,
              "unit_cap": effective_cap, "mandatory_floor_units": floor,
              "floors_above_base": floor > unit_cap,
              "feasible": floor <= effective_cap,
              "intervals": [], "units": 0, "bytes": 0, "gets": 0,
              "accepted_optional_units": 0}
    if floor > effective_cap:
        return result
    frontier = CoverFrontier(UNIT_COUNT, max_gets, effective_cap, mandatory)
    accepted = frontier.admit_ranked(ranked)
    intervals = frontier.intervals()
    result.update({"intervals": [list(interval) for interval in intervals],
                   "units": frontier.minimum_units,
                   "bytes": frontier.minimum_units * UNIT_BYTES,
                   "gets": len(intervals),
                   "accepted_optional_units": len(accepted)})
    return result


def decide(holdout: dict[str, dict[str, int]]) -> str:
    if set(holdout) != set(ARMS):
        raise ValueError("V187 arm set differs")
    qualifies = {arm: (cell["hits"] >= THRESHOLD and cell["p05"] >= 98
                       and cell["infeasible"] == 0)
                 for arm, cell in holdout.items()}
    baseline = holdout["v155_mean_floor_elastic"]
    baseline_resources = (baseline["bytes"] * 1000 <= V155_BYTES * FIT
                          and baseline["gets"] * 1000 <= V155_GETS * FIT)
    if qualifies["v155_mean_floor_elastic"] and baseline_resources:
        return "advance-to-replication"
    if qualifies["elastic_16m"]:
        return "elastic-physical-screen-pass"
    return "revise-utility-allocation-or-layout"


def score_intervals(masses: dict[int, int],
                    intervals: list[list[int]]) -> int:
    """Count full-source truth in all fetched units, including bridges."""
    return sum(count for unit, count in masses.items()
               if any(start <= unit <= end for start, end in intervals))


def _panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V187 output already exists")
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
            query = np.asarray(vectors[source_row], dtype=np.float32)
            _, _, nominated = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
            nominees = np.asarray(nominated, dtype=np.int64)
            if nominees.shape != (512,) or len(np.unique(nominees)) != 512:
                raise ValueError("V187 nominee roster differs")
            own_nominated = bool(np.any(nominees == inverse_old[source_row]))
            eligible = nominees[nominees != inverse_old[source_row]]
            _, sq8_scores = _score(sq8, query, eligible,
                                   planes["low"], planes["step"], 1)
            primary_rank = np.lexsort((sq8["id"][eligible], sq8_scores))[:100]
            primary = eligible[primary_rank]
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
                raise ValueError("V187 score field candidate geometry differs")
            scores = field.scores.reshape(len(candidate), UNIT_ROWS).copy()
            own = np.flatnonzero(field.old_rows == inverse_old[source_row])
            if own.size > 1:
                raise ValueError("V187 pseudoquery row appears twice")
            if own.size == 1:
                scores.reshape(-1)[own[0]] = np.inf
            minima = scores.min(axis=1)
            ranked = list(rank_units(dict(zip(
                candidate, map(float, minima), strict=True))))
            output.write(canonical({
                "ordinal": ordinal, "source_id": stable_id,
                "source_row": int(source_row),
                "own_nominated": own_nominated,
                "mandatory_units": mandatory,
                "ranked_units": ranked,
            }))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
        "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1921-2176",
        "fit": "1921-2048", "holdout": "2049-2176",
        "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "candidate_width": WIDTH, "arms": ARMS,
        "metric": "cosine", "threshold": THRESHOLD,
    }))


def plan(args: argparse.Namespace) -> None:
    if sha256(args.output / "prepare-seal.json") != args.prepare_sha256:
        raise ValueError("V187 externally sealed prepare SHA differs")
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("candidate_width") != WIDTH
            or seal.get("arms") != {arm: list(value) for arm, value in ARMS.items()}
            or seal.get("metric") != "cosine"
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V187 sealed feature authority differs")
    with (args.output / "plans.jsonl").open("x") as output:
        for feature in features:
            ranked = feature["ranked_units"]
            mandatory = feature["mandatory_units"]
            if (not isinstance(ranked, list) or not ranked
                    or len(set(ranked)) != len(ranked)
                    or any(type(unit) is not int or not 0 <= unit < UNIT_COUNT
                           for unit in ranked)
                    or mandatory != sorted(set(mandatory))
                    or not set(mandatory).issubset(ranked)):
                raise ValueError("V187 ranked candidate geometry differs")
            arms = {arm: plan_arm(tuple(mandatory), tuple(ranked),
                                  max_gets=gets, unit_cap=units,
                                  floor_elastic=elastic)
                    for arm, (gets, units, elastic) in ARMS.items()}
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "arms": arms}))
    (args.output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "source_truth_opened": False,
        "prepare_seal_sha256": args.prepare_sha256,
        "features_sha256": sha256(args.output / "features.jsonl"),
        "plans_sha256": sha256(args.output / "plans.jsonl"),
        "arms": ARMS, "unit_bytes": UNIT_BYTES,
    }))


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "plan-seal.json") != args.plan_sha256:
        raise ValueError("V187 externally sealed plan SHA differs")
    seal = json.loads((args.output / "plan-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    plans = records(args.output / "plans.jsonl")
    if (seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("prepare_seal_sha256")
                != sha256(args.output / "prepare-seal.json")
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("plans_sha256") != sha256(args.output / "plans.jsonl")
            or seal.get("arms") != {arm: list(value) for arm, value in ARMS.items()}
            or seal.get("unit_bytes") != UNIT_BYTES
            or len(features) != COUNT or len(plans) != COUNT):
        raise ValueError("V187 sealed plan authority differs")
    prepare_seal = json.loads((args.output / "prepare-seal.json").read_text())
    _, inverse_new, source_ids, vectors, _, _ = _inputs(args)
    if list(_panel(source_ids)) != prepare_seal["pseudo_ids"]:
        raise ValueError("V187 source panel differs")
    source = _normalized(vectors)
    cells = {split: {arm: {"hits": [], "bytes": 0, "gets": 0,
                           "units": 0, "infeasible": 0, "max_query_units": 0,
                           "max_query_gets": 0, "max_mandatory_floor": 0,
                           "floors_above_base": 0, "feasible_query_count": 0,
                           "feasible_query_hits": 0}
                     for arm in ARMS}
             for split in ("fit", "holdout")}
    candidate_totals = {"fit": 0, "holdout": 0}
    own_nominated = {"fit": 0, "holdout": 0}
    with (args.output / "source-labels.jsonl").open("x") as output:
        for index, (feature, plan_row) in enumerate(
                zip(features, plans, strict=True)):
            source_row = feature["source_row"]
            if (feature["ordinal"] != FIRST + index
                    or plan_row.get("ordinal") != feature["ordinal"]
                    or plan_row.get("source_id") != feature["source_id"]
                    or type(source_row) is not int or not 0 <= source_row < ROWS
                    or int(source_ids[source_row]) != feature["source_id"]
                    or type(feature.get("own_nominated")) is not bool
                    or set(plan_row.get("arms", {})) != set(ARMS)):
                raise ValueError("V187 query/plan identity differs")
            ranked = feature["ranked_units"]
            mandatory = feature["mandatory_units"]
            if (not isinstance(ranked, list) or len(ranked) != len(set(ranked))
                    or not set(mandatory).issubset(ranked)):
                raise ValueError("V187 ranked candidates differ")
            truth = _truth(source, source_ids, source_row)
            masses = Counter(map(int, inverse_new[truth] // UNIT_ROWS))
            if sum(masses.values()) != 100:
                raise AssertionError("V187 source truth mass differs")
            split = "fit" if index < FIT else "holdout"
            own_nominated[split] += int(feature["own_nominated"])
            candidate_hits = sum(masses.get(unit, 0) for unit in ranked)
            candidate_totals[split] += candidate_hits
            hits_by_arm = {}
            for arm, (max_gets, unit_cap, elastic) in ARMS.items():
                item = plan_row["arms"][arm]
                expected = plan_arm(tuple(mandatory), tuple(ranked),
                                    max_gets=max_gets, unit_cap=unit_cap,
                                    floor_elastic=elastic)
                if item != expected:
                    raise ValueError("V187 physical plan witness differs")
                intervals = item["intervals"]
                if item["feasible"]:
                    if (not intervals or len(intervals) > max_gets
                            or any(type(start) is not int or type(end) is not int
                                   or not 0 <= start <= end < UNIT_COUNT
                                   for start, end in intervals)
                            or any(left[1] >= right[0]
                                   for left, right in zip(intervals, intervals[1:]))
                            or item["units"] != sum(end - start + 1
                                                    for start, end in intervals)
                            or item["units"] > item["unit_cap"]
                            or item["unit_cap"] > 672
                            or item["bytes"] != item["units"] * UNIT_BYTES
                            or item["gets"] != len(intervals)
                            or any(not any(start <= unit <= end
                                           for start, end in intervals)
                                   for unit in mandatory)):
                        raise ValueError("V187 physical interval accounting differs")
                elif (intervals or item["units"] or item["bytes"] or item["gets"]
                      or item["mandatory_floor_units"] <= item["unit_cap"]):
                    raise ValueError("V187 infeasible arm accounting differs")
                hits = score_intervals(masses, intervals)
                if not item["feasible"] and hits != 0:
                    raise AssertionError("infeasible arm has fetched truth")
                hits_by_arm[arm] = hits
                cell = cells[split][arm]
                cell["hits"].append(hits)
                cell["bytes"] += item["bytes"]
                cell["gets"] += item["gets"]
                cell["units"] += item["units"]
                cell["infeasible"] += int(not item["feasible"])
                cell["floors_above_base"] += int(item["floors_above_base"])
                cell["max_mandatory_floor"] = max(
                    cell["max_mandatory_floor"], item["mandatory_floor_units"])
                if item["feasible"]:
                    cell["feasible_query_count"] += 1
                    cell["feasible_query_hits"] += hits
                cell["max_query_units"] = max(cell["max_query_units"], item["units"])
                cell["max_query_gets"] = max(cell["max_query_gets"], item["gets"])
            output.write(canonical({
                "ordinal": feature["ordinal"], "source_id": feature["source_id"],
                "split": split, "candidate_hits": candidate_hits,
                "truth_by_unit": sorted(masses.items()), "hits": hits_by_arm,
            }))
    results = {split: {arm: {
        "hits": sum(cell["hits"]),
        "p05": sorted(cell["hits"])[(FIT * 5 + 99) // 100 - 1],
        "bytes": cell["bytes"], "gets": cell["gets"], "units": cell["units"],
        "infeasible": cell["infeasible"],
        "floors_above_base": cell["floors_above_base"],
        "max_mandatory_floor": cell["max_mandatory_floor"],
        "feasible_query_count": cell["feasible_query_count"],
        "feasible_query_hits": cell["feasible_query_hits"],
        "quality_qualifies": sum(cell["hits"]) >= THRESHOLD
                             and sorted(cell["hits"])[(FIT * 5 + 99) // 100 - 1] >= 98
                             and cell["infeasible"] == 0,
        "max_query_units": cell["max_query_units"],
        "max_query_gets": cell["max_query_gets"],
    } for arm, cell in arms.items()} for split, arms in cells.items()}
    baseline = results["holdout"]["v155_mean_floor_elastic"]
    baseline_resources_ok = (
        baseline["bytes"] * 1000 <= V155_BYTES * FIT
        and baseline["gets"] * 1000 <= V155_GETS * FIT)
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "source_only": True,
        "physical_interval_plan": True, "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-1921-2176",
        "query_count_per_split": FIT, "candidate_hits": candidate_totals,
        "own_nominated": own_nominated,
        "results": results, "decision": decide(results["holdout"]),
        "v155_mean_resources_ok": baseline_resources_ok,
        "holdout_threshold": THRESHOLD,
        "v155_scaled_bytes_numerator": V155_BYTES * FIT,
        "v155_scaled_gets_numerator": V155_GETS * FIT,
        "v155_scaled_denominator": 1000,
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
    parser.add_argument("--prepare-sha256", default="")
    parser.add_argument("--plan-sha256", default="")
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
    elif args.phase == "plan":
        if len(args.prepare_sha256) != 64:
            raise ValueError("V187 plan requires externally sealed prepare SHA")
        plan(args)
    else:
        if len(args.plan_sha256) != 64:
            raise ValueError("V187 evaluate requires externally sealed plan SHA")
        evaluate(args)


if __name__ == "__main__":
    main()
