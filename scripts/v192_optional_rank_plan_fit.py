#!/usr/bin/env python3
"""Closed V189 fit-only hard-plan diagnostic; never a held-out result."""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import time
from dataclasses import asdict
from math import ceil
from pathlib import Path

from scripts.hard_priced_interval import HardPricedCover, hard_priced_cover
from scripts.optional_rank_utility import OptionalRankUtility, fit_optional_rank_utility
from scripts.source_cover_frontier import CoverFrontier
from scripts.source_rank_utility import fit_rank_utility
from scripts.v192_optional_rank_fit_diagnostic import FEATURE_SHA, FIT_SHA

SCALE = 1_000_000
UNIT_COUNT, UNIT_BYTES = 31_250, 24_960
MAX_GETS, MAX_UNITS = 32, 672
TOTAL_GETS, TOTAL_UNITS = 708, 14_274
MAX_TRACE_BYTES = 512 * 1024 * 1024
PRICE_GRID = tuple(
    (unit, get)
    for unit in (1000, 2000, 3000, 4000, 6000, 10000, 20000)
    for get in (0, 50000, 100000, 200000, 400000)
)


def _read(path: Path, expected: str) -> list[dict]:
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ValueError("V189 sealed fit input digest differs")
    return [json.loads(line) for line in path.read_text().splitlines()]


def _plan(weights: dict[int, int], mandatory: tuple[int, ...],
          price: tuple[int, int]) -> HardPricedCover:
    return hard_priced_cover(
        weights, mandatory, page_count=UNIT_COUNT, max_gets=MAX_GETS,
        max_units=MAX_UNITS, unit_price=price[0], get_price=price[1],
        max_trace_bytes=MAX_TRACE_BYTES)


def _hits(plan: HardPricedCover, truth: dict[int, int]) -> int:
    return sum(mass for unit, mass in truth.items()
               if any(start <= unit <= end
                      for start, end in plan.intervals))


def _summary(rows: list[dict]) -> dict:
    hits = sorted(row["hits"] for row in rows)
    return {
        "queries": len(rows),
        "hits": sum(hits),
        "p05_nearest_rank_hits": hits[ceil(len(hits) * .05) - 1],
        "candidate_ceiling_hits": sum(row["candidate_ceiling_hits"]
                                      for row in rows),
        "units": sum(row["units"] for row in rows),
        "planned_bytes": sum(row["units"] for row in rows) * UNIT_BYTES,
        "gets": sum(row["gets"] for row in rows),
        "max_units": max(row["units"] for row in rows),
        "max_gets": max(row["gets"] for row in rows),
        "over_aggregate_caps": (
            sum(row["units"] for row in rows) > TOTAL_UNITS
            or sum(row["gets"] for row in rows) > TOTAL_GETS),
    }


def _select_price(
    weights_and_mandatory: list[tuple[dict[int, int], tuple[int, ...]]],
) -> tuple[list[dict], tuple[int, int] | None]:
    grid = []
    winner: tuple[tuple[int, int, int, int, int], tuple[int, int]] | None = None
    for price in PRICE_GRID:
        plans = [_plan(weights, mandatory, price)
                 for weights, mandatory in weights_and_mandatory]
        units = sum(plan.units for plan in plans)
        gets = sum(plan.gets for plan in plans)
        mass = sum(plan.mass for plan in plans)
        feasible = units <= TOTAL_UNITS and gets <= TOTAL_GETS
        grid.append({"unit_price": price[0], "get_price": price[1],
                     "predicted_mass": mass, "units": units,
                     "gets": gets, "feasible": feasible})
        if feasible:
            key = (mass, -units, -gets, -price[0], -price[1])
            if winner is None or key > winner[0]:
                winner = (key, price)
    return grid, winner[1] if winner else None


def run(features_path: Path, labels_path: Path) -> dict:
    features = _read(features_path, FEATURE_SHA)[:128]
    labels = _read(labels_path, FIT_SHA)
    if (len(features) != 128 or len(labels) != 128
            or any(feature["ordinal"] != 2432 + index
                   or feature["ordinal"] != label["ordinal"]
                   or feature["source_id"] != label["source_id"]
                   for index, (feature, label) in
                   enumerate(zip(features, labels)))):
        raise ValueError("V189 fit identity differs")
    cases = []
    for feature, label in zip(features, labels):
        truth = {int(unit): int(hits)
                 for unit, hits in label["truth_by_unit"]}
        if sum(truth.values()) != 100:
            raise ValueError("V189 fit truth mass differs")
        ranked = tuple(feature["ranked_units"])
        mandatory = tuple(feature["mandatory_units"])
        cases.append((feature["ordinal"], feature["source_id"],
                      ranked, mandatory, truth))
    train = cases[:64]
    optional_model = fit_optional_rank_utility(
        ((ranked, mandatory, truth)
         for _, _, ranked, mandatory, truth in train),
        result_count=100)
    full_rank = fit_rank_utility(
        ({unit: float(index) for index, unit in enumerate(ranked)},
         {unit: truth[unit] for unit in ranked if unit in truth})
        for _, _, ranked, _, truth in train)
    constant_optional = sum(
        sum(truth.get(unit, 0) for unit in ranked
            if unit not in set(mandatory))
        for _, _, ranked, mandatory, truth in train) / len(train)
    constant_model = OptionalRankUtility(
        optional_model.rank_curve,
        (max(len(case[3]) for case in train),),
        (constant_optional,), len(train), 100)
    price_cases = cases[64:96]
    optional_price_inputs = [
        (optional_model.weights(ranked, mandatory, units_per_hit=SCALE),
         mandatory)
        for _, _, ranked, mandatory, _ in price_cases]
    full_rank_price_inputs = [
        (full_rank.weights(ranked, units_per_hit=SCALE), mandatory)
        for _, _, ranked, mandatory, _ in price_cases]
    optional_grid, selected_price = _select_price(optional_price_inputs)
    full_rank_grid, full_rank_price = _select_price(full_rank_price_inputs)
    report = {
        "schema": "borsuk-v192-optional-hard-plan-fit-v1",
        "status": "complete" if selected_price and full_rank_price
                  else "no-feasible-price",
        "features_sha256": FEATURE_SHA, "fit_labels_sha256": FIT_SHA,
        "model_fit_ordinals": [2432, 2495],
        "price_fit_ordinals": [2496, 2527],
        "diagnostic_ordinals": [2528, 2559],
        "per_query_caps": {"gets": MAX_GETS, "units": MAX_UNITS},
        "aggregate_caps": {"gets": TOTAL_GETS, "units": TOTAL_UNITS},
        "max_trace_bytes": MAX_TRACE_BYTES,
        "price_grids": {"optional_risk": optional_grid,
                        "full_rank": full_rank_grid},
        "selected_prices": {
            "optional_risk": list(selected_price) if selected_price else None,
            "full_rank": list(full_rank_price) if full_rank_price else None,
        },
        "model": asdict(optional_model),
        "splits": {},
    }
    if selected_price is None or full_rank_price is None:
        return report
    for split, subset in (("price_fit", cases[64:96]),
                          ("diagnostic", cases[96:128])):
        arms: dict[str, list[dict]] = {
            "optional_risk": [], "full_rank_refit_price": [],
            "constant_risk": [], "greedy_control": [],
        }
        for ordinal, source_id, ranked, mandatory, truth in subset:
            candidate = set(ranked)
            ceiling = sum(truth.get(unit, 0) for unit in candidate)
            models = (
                ("optional_risk",
                 optional_model.weights(ranked, mandatory,
                                        units_per_hit=SCALE),
                 selected_price),
                ("full_rank_refit_price",
                 full_rank.weights(ranked, units_per_hit=SCALE),
                 full_rank_price),
                ("constant_risk",
                 constant_model.weights(ranked, mandatory,
                                        units_per_hit=SCALE),
                 selected_price),
            )
            for name, weights, price in models:
                plan = _plan(weights, mandatory, price)
                if (plan.units > MAX_UNITS or plan.gets > MAX_GETS
                        or any(not any(a <= unit <= b
                                       for a, b in plan.intervals)
                               for unit in mandatory)):
                    raise AssertionError("V192 hard-plan witness differs")
                arms[name].append({
                    "ordinal": ordinal, "source_id": source_id,
                    "intervals": [list(pair) for pair in plan.intervals],
                    "units": plan.units, "gets": plan.gets,
                    "predicted_mass": plan.mass,
                    "hits": _hits(plan, truth),
                    "candidate_ceiling_hits": ceiling,
                })
            floor = CoverFrontier(UNIT_COUNT, MAX_GETS, UNIT_COUNT,
                                  mandatory).minimum_units
            greedy_cap = min(MAX_UNITS, max(446, floor))
            greedy = CoverFrontier(UNIT_COUNT, MAX_GETS, greedy_cap,
                                   mandatory)
            greedy.admit_ranked(ranked)
            greedy_intervals = greedy.intervals()
            greedy_units = greedy.minimum_units
            greedy_hits = sum(
                mass for unit, mass in truth.items()
                if any(start <= unit <= end
                       for start, end in greedy_intervals))
            if (greedy_units > MAX_UNITS or len(greedy_intervals) > MAX_GETS
                    or any(not any(a <= unit <= b
                                   for a, b in greedy_intervals)
                           for unit in mandatory)):
                raise AssertionError("V192 greedy control witness differs")
            arms["greedy_control"].append({
                "ordinal": ordinal, "source_id": source_id,
                "intervals": [list(pair) for pair in greedy_intervals],
                "units": greedy_units, "gets": len(greedy_intervals),
                "predicted_mass": None, "hits": greedy_hits,
                "candidate_ceiling_hits": ceiling,
            })
        report["splits"][split] = {
            name: {"summary": _summary(rows), "queries": rows}
            for name, rows in arms.items()}
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--fit-labels", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    cpu_started = time.process_time()
    report = run(args.features, args.fit_labels)
    report["offline_runtime"] = {
        "wall_seconds": time.monotonic() - started,
        "cpu_seconds": time.process_time() - cpu_started,
        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }
    args.output.write_text(json.dumps(report, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
