#!/usr/bin/env python3
"""Frozen V192 policy on a fresh 512-query 1M source panel."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from math import ceil, sqrt
from pathlib import Path

import numpy as np

from scripts.hard_priced_interval import hard_priced_cover
from scripts.v114_1m_paired import nominate_region_pq64, score_sq8_ranges
from scripts.v155_relaion_returned_quality import DIMS, ROWS, sha256
from scripts.v166_surrogate_probe import select_pseudoqueries
from scripts.v166_surrogate_ranking_run import _score, canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v182_wide_pq_rank import _normalized, _truth
from scripts.v187_cosine_physical_plan import plan_arm
from scripts.v189_predicted_interval_source import validate_intervals
from scripts.v192_optional_rank_plan_fit import MAX_TRACE_BYTES
from scripts.v193_optional_100k_transfer import _models
from scripts.source_rank_utility import rank_units

SCHEMA = "borsuk-v194-fresh-1m-optional-source-v1"
FIRST, COUNT, WIDTH = 2944, 512, 32
UNIT_ROWS, UNIT_BYTES, UNIT_COUNT = 32, 24_960, 31_250
MAX_UNITS, MAX_GETS = 672, 32
TOTAL_BYTES, TOTAL_GETS = 5_700_611_604, 11_328
ARMS = ("optional_risk", "full_rank", "constant_risk", "greedy_control")
PRICES = {"optional_risk": (1000, 50000),
          "full_rank": (2000, 50000), "constant_risk": (1000, 50000)}


def panel(source_ids: np.ndarray) -> tuple[int, ...]:
    return select_pseudoqueries(source_ids, FIRST + COUNT)[FIRST:]


def _features(args: argparse.Namespace, expected_seal: str) -> tuple[dict, list[dict]]:
    if sha256(args.output / "prepare-seal.json") != expected_seal:
        raise ValueError("V194 external prepare seal differs")
    seal = json.loads((args.output / "prepare-seal.json").read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("split") != "source-pseudoquery-hash-ranks-2945-3456"
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or seal.get("v192_result_sha256")
                != "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01"
            or len(features) != COUNT
            or [row.get("ordinal") for row in features]
                != list(range(FIRST, FIRST + COUNT))
            or [row.get("source_id") for row in features] != seal.get("pseudo_ids")):
        raise ValueError("V194 GT-blind features differ")
    for row in features:
        ranked, mandatory = row["ranked_units"], row["mandatory_units"]
        if (not ranked or len(ranked) != len(set(ranked))
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(ranked)
                or any(type(unit) is not int or not 0 <= unit < UNIT_COUNT
                       for unit in ranked)):
            raise ValueError("V194 candidate geometry differs")
    return seal, features


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V194 output already exists")
    _models(args)
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    chosen = panel(source_ids)
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
                raise ValueError("V194 leave-one-out nominee roster differs")
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
                raise ValueError("V194 PQ candidate geometry differs")
            scores = field.scores.reshape(len(candidate), UNIT_ROWS).copy()
            own = np.flatnonzero(field.old_rows == own_old)
            if own.size > 1:
                raise ValueError("V194 own row appears twice")
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
        "split": "source-pseudoquery-hash-ranks-2945-3456",
        "pseudo_ids": list(chosen),
        "features_sha256": sha256(args.output / "features.jsonl"),
        "v192_result_sha256":
            "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01",
        "candidate_radius": WIDTH, "unit_rows": UNIT_ROWS,
    }))


def _priced_arm(weights: dict[int, int], mandatory: tuple[int, ...],
                price: tuple[int, int]) -> dict:
    try:
        cover = hard_priced_cover(
            weights, mandatory, page_count=UNIT_COUNT,
            max_gets=MAX_GETS, max_units=MAX_UNITS,
            unit_price=price[0], get_price=price[1],
            max_trace_bytes=MAX_TRACE_BYTES)
    except ValueError as error:
        if str(error) not in {"mandatory cover infeasible within hard caps",
                              "hard priced interval trace budget exceeded"}:
            raise
        return {"feasible": False, "reason": str(error),
                "intervals": [], "units": 0, "bytes": 0, "gets": 0}
    return {"feasible": True,
            "intervals": [list(pair) for pair in cover.intervals],
            "units": cover.units, "bytes": cover.units * UNIT_BYTES,
            "gets": cover.gets, "predicted_mass": cover.mass}


def plan(args: argparse.Namespace) -> None:
    seal, features = _features(args, args.prepare_sha256)
    optional, full, constant = _models(args)
    started_cpu, started_wall = time.process_time(), time.monotonic()
    with (args.output / "plans.jsonl").open("x") as output:
        for feature in features:
            ranked = tuple(feature["ranked_units"])
            mandatory = tuple(feature["mandatory_units"])
            arms = {
                "optional_risk": _priced_arm(
                    optional.weights(ranked, mandatory, units_per_hit=1_000_000),
                    mandatory, PRICES["optional_risk"]),
                "full_rank": _priced_arm(
                    full.weights(ranked, units_per_hit=1_000_000), mandatory,
                    PRICES["full_rank"]),
                "constant_risk": _priced_arm(
                    constant.weights(ranked, mandatory, units_per_hit=1_000_000),
                    mandatory, PRICES["constant_risk"]),
                "greedy_control": plan_arm(mandatory, ranked,
                                           max_gets=MAX_GETS, unit_cap=446,
                                           floor_elastic=True),
            }
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "source_id": feature["source_id"],
                                    "arms": arms}))
    (args.output / "planner-runtime.json").write_text(canonical({
        "schema": SCHEMA + "-planner-runtime",
        "plan_512_cpu_seconds": time.process_time() - started_cpu,
        "plan_512_wall_seconds": time.monotonic() - started_wall,
    }))
    (args.output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "source_truth_opened": False,
        "prepare_seal_sha256": args.prepare_sha256,
        "features_sha256": seal["features_sha256"],
        "plans_sha256": sha256(args.output / "plans.jsonl"),
        "planner_runtime_sha256": sha256(args.output / "planner-runtime.json"),
        "arms": ARMS, "prices": PRICES,
        "per_query_caps": [MAX_GETS, MAX_UNITS],
        "aggregate_caps": [TOTAL_GETS, TOTAL_BYTES],
        "max_trace_bytes": MAX_TRACE_BYTES,
    }))


def _wilson95(failures: int, total: int) -> list[float]:
    z = 1.959963984540054
    p = failures / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    margin = z * sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return [centre - margin, centre + margin]


def _summary(values: dict[str, list[int]], infeasible: int) -> dict:
    hits = values["hits"]
    ordered = sorted(hits)
    return {"hits": sum(hits), "p05_hits": ordered[ceil(COUNT * .05) - 1],
            "min_hits": ordered[0], "below_98": sum(hit < 98 for hit in hits),
            "bottom_decile_mean": sum(ordered[:ceil(COUNT * .1)]) / ceil(COUNT * .1),
            "below_98_wilson95": _wilson95(sum(hit < 98 for hit in hits), COUNT),
            "coverage": sum(values["coverage"]),
            "bytes": sum(values["bytes"]), "gets": sum(values["gets"]),
            "infeasible": infeasible}


def _qualifies(cell: dict) -> bool:
    return (cell["hits"] >= 50_979 and cell["p05_hits"] >= 98
            and cell["coverage"] >= 50_979 and cell["infeasible"] == 0
            and cell["bytes"] <= TOTAL_BYTES and cell["gets"] <= TOTAL_GETS)


def decide(cells: dict[str, dict]) -> str:
    optional, full = cells["optional_risk"], cells["full_rank"]
    if _qualifies(optional) and _qualifies(full):
        if (optional["bytes"] <= full["bytes"]
                and optional["gets"] <= full["gets"]
                and full["hits"] - optional["hits"] <= 26):
            return "advance-optional-to-live-s3"
        return "advance-full-rank-to-live-s3"
    if _qualifies(optional):
        return "advance-optional-to-live-s3"
    if _qualifies(full):
        return "advance-full-rank-to-live-s3"
    return "revise-representation-allocation-or-serving"


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "plan-seal.json") != args.plan_sha256:
        raise ValueError("V194 external plan seal differs")
    plan_seal = json.loads((args.output / "plan-seal.json").read_text())
    prepare_seal, features = _features(args, plan_seal["prepare_seal_sha256"])
    plans = records(args.output / "plans.jsonl")
    if (plan_seal.get("schema") != SCHEMA + "-plan-seal"
            or plan_seal.get("source_truth_opened") is not False
            or plan_seal.get("features_sha256") != prepare_seal["features_sha256"]
            or plan_seal.get("plans_sha256") != sha256(args.output / "plans.jsonl")
            or plan_seal.get("planner_runtime_sha256")
                != sha256(args.output / "planner-runtime.json")
            or plan_seal.get("prices") != {key: list(value)
                                            for key, value in PRICES.items()}
            or tuple(plan_seal.get("arms", ())) != ARMS
            or plan_seal.get("per_query_caps") != [MAX_GETS, MAX_UNITS]
            or plan_seal.get("aggregate_caps") != [TOTAL_GETS, TOTAL_BYTES]
            or plan_seal.get("max_trace_bytes") != MAX_TRACE_BYTES
            or len(plans) != COUNT):
        raise ValueError("V194 sealed plan authority differs")
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    if list(panel(source_ids)) != prepare_seal["pseudo_ids"]:
        raise ValueError("V194 source panel differs")
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
        raise ValueError("V194 relaid SQ8 stable-ID map differs")
    source = _normalized(vectors)
    low = np.asarray(planes["low"], dtype=np.float32)
    step = np.asarray(planes["step"], dtype=np.float32)
    values = {name: {field: [] for field in ("hits", "coverage", "bytes", "gets")}
              for name in ARMS}
    infeasible = {name: 0 for name in ARMS}
    candidate_hits = 0
    with (args.output / "raw.jsonl").open("x") as output:
        for index, (feature, planned) in enumerate(zip(features, plans, strict=True)):
            ordinal = FIRST + index
            source_row = feature["source_row"]
            stable_id = feature["source_id"]
            if (planned["ordinal"] != ordinal or feature["ordinal"] != ordinal
                    or planned["source_id"] != stable_id
                    or int(source_ids[source_row]) != stable_id
                    or set(planned["arms"]) != set(ARMS)):
                raise ValueError("V194 query identity differs")
            truth_rows = _truth(source, source_ids, source_row)
            gold = set(map(int, source_ids[truth_rows]))
            masses = Counter(map(int, inverse_new[truth_rows] // UNIT_ROWS))
            if len(gold) != 100 or sum(masses.values()) != 100:
                raise ValueError("V194 exact-source GT100 mass differs")
            candidate = sum(masses.get(unit, 0) for unit in feature["ranked_units"])
            candidate_hits += candidate
            row = {"ordinal": ordinal, "source_id": stable_id,
                   "candidate_ceiling_hits": candidate,
                   "truth_by_unit": sorted(masses.items()), "arms": {}}
            query = np.asarray(vectors[source_row], dtype=np.float32)
            for name in ARMS:
                arm = planned["arms"][name]
                if not arm["feasible"]:
                    infeasible[name] += 1
                    row["arms"][name] = {"feasible": False,
                                         "reason": arm.get("reason", "mandatory-floor")}
                    for field in values[name]:
                        values[name][field].append(0)
                    continue
                intervals = tuple(tuple(pair) for pair in arm["intervals"])
                units, gets, bytes_read = validate_intervals(
                    intervals, tuple(feature["mandatory_units"]),
                    page_count=UNIT_COUNT, max_units=MAX_UNITS,
                    max_gets=MAX_GETS)
                if ((units, gets, bytes_read) !=
                        (arm["units"], arm["gets"], arm["bytes"])):
                    raise ValueError("V194 physical accounting differs")
                ranges = [[start * UNIT_BYTES, (end + 1) * UNIT_BYTES]
                          for start, end in intervals]
                returned = score_sq8_ranges(new_sq8, query, low, step,
                                            ranges, top_k=101)
                returned = [identifier for identifier in returned
                            if identifier != stable_id][:100]
                if len(returned) != 100 or len(set(returned)) != 100:
                    raise ValueError("V194 returned top100 identity differs")
                covered = sum(mass for unit, mass in masses.items()
                              if any(start <= unit <= end for start, end in intervals))
                hits = len(set(returned) & gold)
                if hits > covered:
                    raise AssertionError("V194 returned hits exceed coverage")
                row["arms"][name] = {"feasible": True,
                                     "returned_ids": returned,
                                     "hits": hits, "coverage": covered,
                                     "bytes": bytes_read, "gets": gets}
                for field, value in (("hits", hits), ("coverage", covered),
                                     ("bytes", bytes_read), ("gets", gets)):
                    values[name][field].append(value)
            output.write(canonical(row))
    cells = {name: _summary(values[name], infeasible[name]) for name in ARMS}
    paired = {}
    for name in ("full_rank", "constant_risk", "greedy_control"):
        delta = [left - right for left, right in zip(
            values["optional_risk"]["hits"], values[name]["hits"], strict=True)]
        paired[name] = {"wins": sum(x > 0 for x in delta),
                        "ties": sum(x == 0 for x in delta),
                        "losses": sum(x < 0 for x in delta),
                        "net_hits": sum(delta)}
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-1M D768",
        "split": "source-pseudoquery-hash-ranks-2945-3456", "queries": COUNT,
        "candidate_ceiling_hits": candidate_hits,
        "arms": cells, "paired_optional_risk": paired,
        "decision": decide(cells),
        "raw_sha256": sha256(args.output / "raw.jsonl"),
        "plan_seal_sha256": args.plan_sha256,
        "planner_runtime": json.loads((args.output / "planner-runtime.json").read_text()),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "plan", "evaluate"))
    for name in ("output", "source", "old_layout", "old_sq8", "router",
                 "order", "v164_terminal", "v192_result", "v189_features",
                 "v189_fit_labels"):
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
