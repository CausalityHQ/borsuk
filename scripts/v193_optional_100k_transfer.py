#!/usr/bin/env python3
"""GT-blind V192 policy transfer to V170's paired ReLAION-100k workload."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from collections import Counter
from dataclasses import asdict
from math import ceil
from pathlib import Path

import numpy as np

from scripts.hard_priced_interval import hard_priced_cover
from scripts.optional_rank_utility import OptionalRankUtility, fit_optional_rank_utility
from scripts.pq_cosine_margin_utility import SCALE
from scripts.source_rank_utility import RankUtility, fit_rank_utility, rank_units
from scripts.v114_1m_paired import score_sq8_ranges
from scripts.v158_pq_primary_returned import (
    DIMS, QUERIES, ROWS, authenticate, jsonl, load_truth, sha256,
)
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v170_pq_field_100k import _file, _load_physical
from scripts.v192_optional_rank_fit_diagnostic import FEATURE_SHA, FIT_SHA

SCHEMA = "borsuk-v193-optional-100k-transfer-v1"
V170_TERMINAL_SHA = "9d20e60e8b6ae115b51347e4360a613df0ff105070cdcc9b3eb07ed6734a81b1"
V170_PLAN_SHA = "6695ff985b555fac7865fb027a228ea7af3cbbfb372897887b29f09beeeb98ec"
V170_PLAN_SEAL_SHA = "d364520f598e9aeb8b4a794e6fe1c7cc29e5d37ae4d9688062b8191b18387848"
V170_RAW_SHA = "d8081a9feaec74d750393b675819985941a15f409890d788e00ad9b38e3b3894"
V192_RESULT_SHA = "b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01"
UNIT_ROWS, UNIT_BYTES = 32, 24_960
UNIT_COUNT = ROWS // UNIT_ROWS
TRACE_BYTES = 512 * 1024 * 1024
ARMS = ("optional_risk", "full_rank", "constant_risk")


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _read_fit(path: Path, expected: str) -> list[dict]:
    if sha256(path) != expected:
        raise ValueError("V193 sealed 1M fit input differs")
    return list(jsonl(path))


def _models(args: argparse.Namespace):
    _file(args.v192_result, 174_089, V192_RESULT_SHA)
    receipt = json.loads(args.v192_result.read_text())
    if (receipt.get("schema") != "borsuk-v192-optional-hard-plan-fit-v1"
            or receipt.get("status") != "complete"
            or receipt.get("selected_prices") != {
                "optional_risk": [1000, 50000],
                "full_rank": [2000, 50000]}):
        raise ValueError("V193 frozen V192 policy differs")
    features = _read_fit(args.v189_features, FEATURE_SHA)[:64]
    labels = _read_fit(args.v189_fit_labels, FIT_SHA)[:64]
    if (len(features) != 64 or len(labels) != 64
            or any(feature["ordinal"] != 2432 + index
                   or feature["ordinal"] != label["ordinal"]
                   or feature["source_id"] != label["source_id"]
                   for index, (feature, label) in enumerate(zip(features, labels)))):
        raise ValueError("V193 V189 model-fit identities differ")
    cases = []
    for feature, label in zip(features, labels):
        truth = {int(unit): int(hits)
                 for unit, hits in label["truth_by_unit"]}
        if sum(truth.values()) != 100:
            raise ValueError("V193 model-fit GT100 mass differs")
        cases.append((tuple(feature["ranked_units"]),
                      tuple(feature["mandatory_units"]), truth))
    optional = fit_optional_rank_utility(cases, result_count=100)
    if json.loads(canonical(asdict(optional))) != receipt["model"]:
        raise ValueError("V193 optional model replay differs")
    full = fit_rank_utility(
        ({unit: float(rank) for rank, unit in enumerate(ranked)},
         {unit: truth[unit] for unit in ranked if unit in truth})
        for ranked, _, truth in cases)
    constant = sum(
        sum(truth.get(unit, 0) for unit in ranked
            if unit not in set(mandatory))
        for ranked, mandatory, truth in cases) / len(cases)
    constant_model = OptionalRankUtility(
        optional.rank_curve,
        (max(len(mandatory) for _, mandatory, _ in cases),),
        (constant,), len(cases), 100)
    return optional, full, constant_model


def _v170_authority(args: argparse.Namespace) -> None:
    _file(args.v170_terminal, 1414, V170_TERMINAL_SHA)
    terminal = json.loads(args.v170_terminal.read_text())
    if (terminal.get("status") != "complete"
            or terminal.get("source_commit")
                != "85c73dde4964779336ad856f7170ffbaa020711d"
            or terminal.get("artifacts", {}).get("plans.jsonl", {}).get("sha256")
                != V170_PLAN_SHA
            or terminal.get("artifacts", {}).get("plan-seal.json", {}).get("sha256")
                != V170_PLAN_SEAL_SHA
            or terminal.get("artifacts", {}).get("raw.jsonl", {}).get("sha256")
                != V170_RAW_SHA):
        raise ValueError("V193 V170 complete terminal differs")
    _file(args.v170_plans, 767_470, V170_PLAN_SHA)
    _file(args.v170_plan_seal, 773, V170_PLAN_SEAL_SHA)
    seal = json.loads(args.v170_plan_seal.read_text())
    if (seal.get("gt_opened") is not False
            or seal.get("plans_sha256") != V170_PLAN_SHA):
        raise ValueError("V193 V170 GT-blind plan seal differs")


def _feature_plan(
    request: dict, reference: dict, control: dict,
    books: np.ndarray, codes: np.ndarray, order: np.ndarray,
    inverse: np.ndarray, identity: np.ndarray,
    models: tuple[OptionalRankUtility, RankUtility, OptionalRankUtility],
) -> dict:
    ordinal = request.get("query_ordinal")
    nominees = request.get("nominees")
    primary = reference.get("primary")
    query = np.asarray(request.get("query"), dtype=np.float32)
    if (type(ordinal) is not int or reference.get("query_ordinal") != ordinal
            or control.get("query_ordinal") != ordinal
            or not isinstance(nominees, list) or len(nominees) != 512
            or not isinstance(primary, list) or len(primary) != 100
            or len(set(nominees)) != 512 or len(set(primary)) != 100
            or not set(primary).issubset(nominees)
            or any(type(row) is not int or not 0 <= row < ROWS
                   for row in nominees + primary)
            or query.shape != (DIMS,) or not np.isfinite(query).all()
            or type(control.get("pq_bytes")) is not int
            or type(control.get("pq_gets")) is not int
            or control["pq_bytes"] % UNIT_BYTES
            or not 0 < control["pq_bytes"] <= 672 * UNIT_BYTES
            or not 0 < control["pq_gets"] <= 32):
        raise ValueError("V193 V170 paired request geometry differs")
    field = score_neighbor_field(
        query, nominees=tuple(nominees), old_order=identity,
        inverse_old=identity, new_order=order, inverse_new=inverse,
        books=books, codes=codes, unit_rows=UNIT_ROWS, radius=32,
        metric="cosine")
    if (field.scores.size != len(field.units) * UNIT_ROWS
            or not field.units):
        raise ValueError("V193 candidate score field differs")
    minima = field.scores.reshape(len(field.units), UNIT_ROWS).min(axis=1)
    scores = dict(zip(field.units, map(float, minima), strict=True))
    ranked = rank_units(scores)
    mandatory = tuple(sorted(set(
        int(inverse[row]) // UNIT_ROWS for row in primary)))
    if not set(mandatory).issubset(ranked):
        raise ValueError("V193 primary outside PQ candidate field")
    unit_cap = min(672, control["pq_bytes"] // UNIT_BYTES)
    get_cap = min(32, control["pq_gets"])
    optional, full, constant = models
    weights = {
        "optional_risk": optional.weights(
            ranked, mandatory, units_per_hit=SCALE),
        "full_rank": full.weights(ranked, units_per_hit=SCALE),
        "constant_risk": constant.weights(
            ranked, mandatory, units_per_hit=SCALE),
    }
    arms = {}
    for name in ARMS:
        price = ((1000, 50000) if name != "full_rank"
                 else (2000, 50000))
        try:
            cover = hard_priced_cover(
                weights[name], mandatory, page_count=UNIT_COUNT,
                max_gets=get_cap, max_units=unit_cap,
                unit_price=price[0], get_price=price[1],
                max_trace_bytes=TRACE_BYTES)
        except ValueError as error:
            if str(error) not in {
                    "mandatory cover infeasible within hard caps",
                    "hard priced interval trace budget exceeded"}:
                raise
            arms[name] = {"feasible": False, "reason": str(error)}
            continue
        ranges = [[start * UNIT_BYTES, (end + 1) * UNIT_BYTES]
                  for start, end in cover.intervals]
        if (cover.units > unit_cap or cover.gets > get_cap
                or sum(end - start for start, end in ranges)
                    != cover.units * UNIT_BYTES
                or any(not any(start <= unit <= end
                               for start, end in cover.intervals)
                       for unit in mandatory)):
            raise AssertionError("V193 hard interval witness differs")
        arms[name] = {
            "feasible": True,
            "intervals": [list(pair) for pair in cover.intervals],
            "ranges": ranges, "units": cover.units,
            "bytes": cover.units * UNIT_BYTES, "gets": cover.gets,
            "predicted_mass": cover.mass,
        }
    return {
        "query_ordinal": ordinal,
        "ranked_units": list(ranked), "mandatory_units": list(mandatory),
        "candidate_units": len(ranked),
        "paired_v170_bytes": control["pq_bytes"],
        "paired_v170_gets": control["pq_gets"],
        "arms": arms,
    }


def plan(args: argparse.Namespace) -> None:
    _v170_authority(args)
    models = _models(args)
    books, codes, order, inverse, identity, _ = _load_physical(args)
    count = 0
    with args.plans.open("x") as output:
        for request, reference, control in itertools.zip_longest(
                jsonl(args.requests), jsonl(args.reference),
                jsonl(args.v170_plans)):
            if any(row is None for row in (request, reference, control)):
                raise ValueError("V193 frozen 100k query count differs")
            row = _feature_plan(request, reference, control, books, codes,
                                order, inverse, identity, models)
            if row["query_ordinal"] != count:
                raise ValueError("V193 frozen 100k query order differs")
            output.write(canonical(row))
            count += 1
    if count != QUERIES:
        raise ValueError("V193 frozen 100k query count differs")
    args.plan_seal.write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "gt_opened": False,
        "queries": count,
        "requests_sha256": sha256(args.requests),
        "reference_sha256": sha256(args.reference),
        "v170_terminal_sha256": V170_TERMINAL_SHA,
        "v170_plans_sha256": V170_PLAN_SHA,
        "v192_result_sha256": V192_RESULT_SHA,
        "v189_features_sha256": FEATURE_SHA,
        "v189_fit_labels_sha256": FIT_SHA,
        "candidate_radius": 32, "metric": "cosine",
        "unit_bytes": UNIT_BYTES, "max_trace_bytes": TRACE_BYTES,
        "arms": ARMS, "plans_sha256": sha256(args.plans),
    }))


def _p05(values: list[int]) -> int:
    return sorted(values)[ceil(.05 * len(values)) - 1]


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.plan_seal) != args.plan_sha256:
        raise ValueError("V193 external GT-blind plan seal differs")
    seal = json.loads(args.plan_seal.read_text())
    if (seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("gt_opened") is not False
            or seal.get("queries") != QUERIES
            or seal.get("plans_sha256") != sha256(args.plans)
            or seal.get("requests_sha256") != sha256(args.requests)
            or seal.get("reference_sha256") != sha256(args.reference)
            or seal.get("v192_result_sha256") != V192_RESULT_SHA
            or seal.get("v170_terminal_sha256") != V170_TERMINAL_SHA
            or seal.get("v170_plans_sha256") != V170_PLAN_SHA
            or seal.get("v189_features_sha256") != FEATURE_SHA
            or seal.get("v189_fit_labels_sha256") != FIT_SHA
            or seal.get("candidate_radius") != 32
            or seal.get("metric") != "cosine"
            or seal.get("unit_bytes") != UNIT_BYTES
            or seal.get("max_trace_bytes") != TRACE_BYTES
            or tuple(seal.get("arms", ())) != ARMS):
        raise ValueError("V193 GT-blind plan authority differs")
    _v170_authority(args)
    _file(args.v170_raw, 2_300_269, V170_RAW_SHA)
    authenticate(args.truth, "truth")
    books, codes, order, inverse, identity, sq8 = _load_physical(args)
    del books, codes, order, inverse, identity
    authority = json.loads(args.manifest.read_text())
    low = np.asarray(authority["low"], dtype=np.float32)
    step = np.asarray(authority["step"], dtype=np.float32)
    if (low.shape != (DIMS,) or step.shape != (DIMS,)
            or not np.isfinite(low).all()
            or not np.isfinite(step).all()
            or (step <= 0).any()):
        raise ValueError("V193 SQ8 quantizer differs")
    truth = load_truth(args.truth)
    positions = {int(identifier): index
                 for index, identifier in enumerate(sq8["id"])}
    if len(positions) != ROWS:
        raise ValueError("V193 SQ8 stable ID map differs")
    values = {name: {"hits": [], "coverage": [], "bytes": [], "gets": [],
                     "infeasible": 0} for name in ARMS}
    controls = {"hits": [], "coverage": [], "bytes": [], "gets": []}
    count = 0
    with args.raw.open("x") as output:
        for request, reference, planned, control in itertools.zip_longest(
                jsonl(args.requests), jsonl(args.reference),
                jsonl(args.plans), jsonl(args.v170_raw)):
            if any(row is None for row in
                   (request, reference, planned, control)):
                raise ValueError("V193 paired 100k query count differs")
            if (any(row["query_ordinal"] != count for row in
                    (request, reference, planned, control))
                    or control["pq_bytes"] != planned["paired_v170_bytes"]
                    or control["pq_gets"] != planned["paired_v170_gets"]):
                raise ValueError("V193 paired V170 row differs")
            query = np.asarray(request["query"], dtype=np.float32)
            gold = set(map(int, truth[count]))
            if len(gold) != 100 or any(identifier not in positions
                                       for identifier in gold):
                raise ValueError("V193 GT100 stable IDs differ")
            candidate = set(planned["ranked_units"])
            candidate_hits = sum(
                positions[identifier] // UNIT_ROWS in candidate
                for identifier in gold)
            row = {
                "query_ordinal": count,
                "candidate_ceiling_hits": candidate_hits,
                "control_hits": control["pq_hits100"],
                "control_coverage": control["pq_coverage"],
                "control_bytes": control["pq_bytes"],
                "control_gets": control["pq_gets"],
                "arms": {},
            }
            for field, control_field in (
                    ("hits", "pq_hits100"),
                    ("coverage", "pq_coverage"),
                    ("bytes", "pq_bytes"), ("gets", "pq_gets")):
                controls[field].append(control[control_field])
            for name in ARMS:
                arm = planned["arms"][name]
                if not arm["feasible"]:
                    values[name]["infeasible"] += 1
                    row["arms"][name] = {"feasible": False,
                                         "reason": arm["reason"]}
                    continue
                ranges = arm["ranges"]
                if (arm["bytes"] > control["pq_bytes"]
                        or arm["gets"] > control["pq_gets"]
                        or arm["bytes"] != sum(end - start
                                               for start, end in ranges)
                        or arm["gets"] != len(ranges)
                        or any(not any(start <= unit * UNIT_BYTES < end
                                       for start, end in ranges)
                               for unit in planned["mandatory_units"])):
                    raise ValueError("V193 paired physical plan differs")
                returned = score_sq8_ranges(
                    sq8, query, low, step, ranges, top_k=100)
                hits = len(set(returned) & gold)
                coverage = sum(
                    any(start <= positions[identifier] * 780 < end
                        for start, end in ranges)
                    for identifier in gold)
                if hits > coverage:
                    raise AssertionError("V193 returned hits exceed coverage")
                row["arms"][name] = {
                    "feasible": True, "returned_ids": returned,
                    "hits": hits, "coverage": coverage,
                    "bytes": arm["bytes"], "gets": arm["gets"],
                }
                for field, amount in (("hits", hits), ("coverage", coverage),
                                      ("bytes", arm["bytes"]),
                                      ("gets", arm["gets"])):
                    values[name][field].append(amount)
            output.write(canonical(row))
            count += 1
    if count != QUERIES or sum(controls["hits"]) != 99_357:
        raise ValueError("V193 frozen V170 control differs")
    summaries = {}
    for name in ARMS:
        arm = values[name]
        if arm["infeasible"]:
            summaries[name] = {"infeasible": arm["infeasible"],
                               "complete_queries": len(arm["hits"])}
            continue
        summaries[name] = {
            "infeasible": 0, "hits": sum(arm["hits"]),
            "p05_hits": _p05(arm["hits"]),
            "coverage": sum(arm["coverage"]),
            "bytes": sum(arm["bytes"]), "gets": sum(arm["gets"]),
        }
    control_summary = {
        "hits": sum(controls["hits"]), "p05_hits": _p05(controls["hits"]),
        "coverage": sum(controls["coverage"]),
        "bytes": sum(controls["bytes"]), "gets": sum(controls["gets"]),
    }
    candidate_ceiling = sum(
        row["candidate_ceiling_hits"] for row in jsonl(args.raw))
    optional = summaries["optional_risk"]
    paired = {}
    if optional["infeasible"] == 0:
        for name, other in (
                ("v170_control", controls["hits"]),
                ("full_rank", values["full_rank"]["hits"])
                if summaries["full_rank"]["infeasible"] == 0
                else ("full_rank", None)):
            if other is None:
                continue
            differences = [left - right for left, right in
                           zip(values["optional_risk"]["hits"], other,
                               strict=True)]
            paired[name] = {
                "wins": sum(value > 0 for value in differences),
                "ties": sum(value == 0 for value in differences),
                "losses": sum(value < 0 for value in differences),
                "net_hits": sum(differences),
            }
    passed = (
        optional["infeasible"] == 0
        and optional["hits"] >= 99_357
        and optional["p05_hits"] >= 98
        and optional["coverage"] >= 99_747
        and optional["bytes"] <= control_summary["bytes"]
        and optional["gets"] <= control_summary["gets"])
    args.summary.write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-100k D768",
        "split": "development-1000-reused", "queries": count,
        "decision": "advance-to-fresh-1m" if passed else
                    "revise-feature-or-allocation",
        "candidate_ceiling_hits": candidate_ceiling,
        "arms": summaries, "v170_control": control_summary,
        "paired_optional_risk": paired,
        "raw_sha256": sha256(args.raw),
        "plan_seal_sha256": sha256(args.plan_seal),
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("plan", "evaluate"))
    for name in (
            "requests", "reference", "manifest",
            "v113_terminal", "v113_seal", "v113_ids",
            "v113_books", "v113_codes", "v163_terminal",
            "order", "sq8", "v163_plans", "v170_terminal",
            "v170_plans", "v170_plan_seal", "v192_result",
            "v189_features", "v189_fit_labels", "plans",
            "plan_seal", "truth", "v170_raw", "raw", "summary"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path,
                            required=True)
    parser.add_argument("--plan-sha256")
    args = parser.parse_args()
    if args.phase == "plan":
        plan(args)
    else:
        evaluate(args)


if __name__ == "__main__":
    main()
