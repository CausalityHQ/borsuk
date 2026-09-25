#!/usr/bin/env python3
"""Frozen optional-risk resident FP16 screen on used real D768 queries."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from math import ceil
from pathlib import Path

import numpy as np

from scripts.hard_priced_interval import hard_priced_cover
from scripts.v114_1m_paired import nominate_region_pq64, score_sq8_ranges
from scripts.v124_source_tier_precision import load_truth
from scripts.v155_relaion_returned_quality import ROWS, sha256
from scripts.v166_surrogate_ranking_run import _score, canonical, records
from scripts.v168_scored_neighbor_field import score_neighbor_field
from scripts.v177_source_candidate_ceiling import _inputs, expanded_units
from scripts.v189_predicted_interval_source import validate_intervals
from scripts.v192_optional_rank_plan_fit import MAX_TRACE_BYTES
from scripts.v193_optional_100k_transfer import _models
from scripts.v195_used_1m_rerank_diagnostic import _floor
from scripts.source_rank_utility import rank_units
import scripts.v194_fresh_1m_optional_source as v194

SCHEMA = "borsuk-v198-real-query-resident-fp16-v1"
SPLIT = "validation-1000-already-used"
COUNT = 1000
REQUEST_SHA = "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
TRUTH_SHA = "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871"
V155_EVIDENCE_SHA = "dd4d4a7c9448ce6833357e70bff5f72bd9a782afd8185114cb501b2a63a49a1a"
NEW_SQ8_SHA = "aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9"
PLANE_SHA = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47"
MAX_BYTES, MAX_GETS = 11_134_007_040, 22_126
TARGET_HITS = 99_567


def _requests(path: Path) -> list[dict]:
    if sha256(path) != REQUEST_SHA:
        raise ValueError("V198 V116 real query identity differs")
    result = records(path)
    if (len(result) != COUNT or
            any(row["query_ordinal"] != index or len(row["query"]) != 768
                or len(row["nominees"]) != 512
                for index, row in enumerate(result))):
        raise ValueError("V198 query roster differs")
    return result


def _features(args: argparse.Namespace, expected_sha: str) -> tuple[dict, list[dict]]:
    path = args.output / "prepare-seal.json"
    if sha256(path) != expected_sha:
        raise ValueError("V198 prepare seal differs")
    seal = json.loads(path.read_text())
    features = records(args.output / "features.jsonl")
    if (seal.get("schema") != SCHEMA + "-prepare-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("split") != SPLIT
            or seal.get("requests_sha256") != REQUEST_SHA
            or seal.get("features_sha256") != sha256(args.output / "features.jsonl")
            or len(features) != COUNT):
        raise ValueError("V198 GT-blind features differ")
    for index, feature in enumerate(features):
        ranked, mandatory = feature["ranked_units"], feature["mandatory_units"]
        if (feature["ordinal"] != index or not ranked
                or len(set(ranked)) != len(ranked)
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(ranked)):
            raise ValueError("V198 feature geometry differs")
    return seal, features


def prepare(args: argparse.Namespace) -> None:
    if args.output.exists():
        raise ValueError("V198 output already exists")
    requests = _requests(args.requests)
    _models(args)
    old, inverse_new, _, _, sq8, planes = _inputs(args)
    inverse_old = np.empty(ROWS, dtype=np.int64)
    inverse_old[old] = np.arange(ROWS, dtype=np.int64)
    new_order = np.argsort(inverse_new).astype(np.int64)
    args.output.mkdir(parents=True)
    with (args.output / "features.jsonl").open("x") as output:
        for index, request in enumerate(requests):
            query = np.asarray(request["query"], dtype=np.float32)
            if not np.isfinite(query).all() or np.linalg.norm(query) <= 0:
                raise ValueError("V198 real query vector differs")
            _, _, nominated = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512)
            nominees = np.asarray(nominated, dtype=np.int64)
            if (nominees.shape != (512,) or len(np.unique(nominees)) != 512
                    or set(map(int, nominees)) != set(request["nominees"])):
                raise ValueError(f"V198 V116 router parity differs at {index}")
            _, sq8_scores = _score(sq8, query, nominees,
                                   planes["low"], planes["step"], 1)
            primary = nominees[np.lexsort((sq8["id"][nominees], sq8_scores))[:100]]
            base = sorted(set((inverse_new[old[nominees]] // v194.UNIT_ROWS).tolist()))
            mandatory = sorted(set((inverse_new[old[primary]] // v194.UNIT_ROWS).tolist()))
            candidate = expanded_units(base, v194.WIDTH, v194.UNIT_COUNT)
            field = score_neighbor_field(
                query, nominees=tuple(map(int, nominees)), old_order=old,
                inverse_old=inverse_old, new_order=new_order,
                inverse_new=inverse_new, books=planes["books"],
                codes=planes["codes"], unit_rows=v194.UNIT_ROWS,
                radius=v194.WIDTH, metric="cosine")
            if field.units != candidate or field.scores.size != len(candidate) * 32:
                raise ValueError("V198 PQ candidate geometry differs")
            minima = field.scores.reshape(len(candidate), 32).min(axis=1)
            ranked = rank_units(dict(zip(candidate, map(float, minima), strict=True)))
            output.write(canonical({"ordinal": index,
                                    "mandatory_units": mandatory,
                                    "ranked_units": ranked}))
    (args.output / "prepare-seal.json").write_text(canonical({
        "schema": SCHEMA + "-prepare-seal", "source_truth_opened": False,
        "dataset": "ReLAION-1M D768", "split": SPLIT,
        "query_ordinals": list(range(COUNT)),
        "requests_sha256": REQUEST_SHA,
        "features_sha256": sha256(args.output / "features.jsonl"),
        "resident_plane_sha256": PLANE_SHA,
        "candidate_radius": 32, "unit_rows": 32,
    }))


def plan(args: argparse.Namespace) -> None:
    _, features = _features(args, args.prepare_sha256)
    optional, _, _ = _models(args)
    with (args.output / "plans.jsonl").open("x") as output:
        for feature in features:
            mandatory = tuple(feature["mandatory_units"])
            ranked = tuple(feature["ranked_units"])
            floor = _floor(list(mandatory), v194.MAX_GETS)
            cap = max(v194.MAX_UNITS, floor)
            weights = optional.weights(ranked, mandatory, units_per_hit=1_000_000)
            try:
                cover = hard_priced_cover(
                    weights, mandatory, page_count=v194.UNIT_COUNT,
                    max_gets=v194.MAX_GETS, max_units=cap,
                    unit_price=1000, get_price=50000,
                    max_trace_bytes=MAX_TRACE_BYTES)
                arm = {"feasible": True,
                       "intervals": [list(pair) for pair in cover.intervals],
                       "units": cover.units, "bytes": cover.units * v194.UNIT_BYTES,
                       "gets": cover.gets, "predicted_mass": cover.mass}
            except ValueError as error:
                if str(error) not in {"mandatory cover infeasible within hard caps",
                                      "hard priced interval trace budget exceeded"}:
                    raise
                arm = {"feasible": False, "reason": str(error),
                       "intervals": [], "units": 0, "bytes": 0, "gets": 0}
            output.write(canonical({"ordinal": feature["ordinal"],
                                    "mandatory_floor": floor,
                                    "unit_cap": cap, "optional_risk": arm}))
    (args.output / "plan-seal.json").write_text(canonical({
        "schema": SCHEMA + "-plan-seal", "source_truth_opened": False,
        "split": SPLIT, "prepare_seal_sha256": args.prepare_sha256,
        "plans_sha256": sha256(args.output / "plans.jsonl"),
        "model": "V194-optional-risk", "prices": [1000, 50000],
        "base_unit_cap": 672, "max_gets": 32,
        "max_trace_bytes": MAX_TRACE_BYTES,
        "shortlist": 128, "resident_plane_sha256": PLANE_SHA,
        "aggregate_caps": [MAX_BYTES, MAX_GETS],
    }))


def _rerank_real(shortlist: list[int], positions: dict[int, int],
                 vectors: np.ndarray, query: np.ndarray) -> list[int]:
    rows = np.asarray([positions[identifier] for identifier in shortlist], dtype=np.int64)
    payload = vectors[rows].astype(np.float16).astype(np.float64)
    norms = np.linalg.norm(payload, axis=1)
    query64 = query.astype(np.float64)
    query64 /= np.linalg.norm(query64)
    if not np.isfinite(norms).all() or (norms <= 0).any():
        raise ValueError("V198 FP16 candidate norm differs")
    scores = (payload @ query64) / norms
    stable = np.asarray(shortlist, dtype=np.int64)
    return [int(stable[index]) for index in np.lexsort((stable, -scores))[:100]]


def _wilson95(failures: int, total: int) -> list[float]:
    return v194._wilson95(failures, total)


def _cell(hits: list[int], coverages: list[int], bytes_read: list[int],
          gets: list[int], infeasible: int) -> dict:
    ordered = sorted(hits)
    failures = sum(hit < 98 for hit in hits)
    return {"hits": sum(hits), "p05_hits": ordered[ceil(COUNT * .05) - 1],
            "min_hits": ordered[0], "below_98": failures,
            "below_98_wilson95": _wilson95(failures, COUNT),
            "coverage": sum(coverages), "bytes": sum(bytes_read),
            "gets": sum(gets), "infeasible": infeasible}


def evaluate(args: argparse.Namespace) -> None:
    if sha256(args.output / "plan-seal.json") != args.plan_sha256:
        raise ValueError("V198 external GT-blind plan seal differs")
    plan_seal = json.loads((args.output / "plan-seal.json").read_text())
    _, features = _features(args, plan_seal["prepare_seal_sha256"])
    plans = records(args.output / "plans.jsonl")
    requests = _requests(args.requests)
    if (plan_seal.get("schema") != SCHEMA + "-plan-seal"
            or plan_seal.get("source_truth_opened") is not False
            or plan_seal.get("plans_sha256") != sha256(args.output / "plans.jsonl")
            or plan_seal.get("resident_plane_sha256") != PLANE_SHA
            or len(plans) != COUNT):
        raise ValueError("V198 plan authority differs")
    for path, expected in ((args.truth, TRUTH_SHA),
                           (args.v155_evidence, V155_EVIDENCE_SHA),
                           (args.new_sq8, NEW_SQ8_SHA), (args.plane, PLANE_SHA)):
        if sha256(path) != expected:
            raise ValueError("V198 source/GT/plane artifact differs")
    old, inverse_new, source_ids, vectors, sq8, planes = _inputs(args)
    new_order = np.argsort(inverse_new).astype(np.int64)
    new_sq8 = np.memmap(args.new_sq8, dtype=sq8.dtype, mode="r", shape=(ROWS,))
    if not np.array_equal(new_sq8["id"], source_ids[new_order]):
        raise ValueError("V198 physical SQ8/source map differs")
    truth = load_truth(args.truth, sq8["id"])
    baseline = records(args.v155_evidence)
    if len(truth) != COUNT or len(baseline) != COUNT:
        raise ValueError("V198 real-query truth or baseline length differs")
    if sum(row["arms"]["sparse"]["source_hits"] for row in baseline) != TARGET_HITS:
        raise ValueError("V198 V155 sparse returned baseline differs")
    positions = {int(identifier): row for row, identifier in enumerate(source_ids)}
    low = np.asarray(planes["low"], dtype=np.float32)
    step = np.asarray(planes["step"], dtype=np.float32)
    fp16_hits, sq8_hits, coverages, bytes_read, gets = [], [], [], [], []
    infeasible = 0
    wins = ties = losses = 0
    with ((args.output / "raw.jsonl").open("x") as output,
          (args.output / "cases.jsonl").open("x") as cases):
        for index, (feature, planned, request, gold_ids, reference) in enumerate(
                zip(features, plans, requests, truth, baseline, strict=True)):
            mandatory = feature["mandatory_units"]
            floor = _floor(mandatory, v194.MAX_GETS)
            if (feature["ordinal"] != index or planned["ordinal"] != index
                    or request["query_ordinal"] != index
                    or reference["query_ordinal"] != index
                    or planned["mandatory_floor"] != floor
                    or planned["unit_cap"] != max(672, floor)):
                raise ValueError("V198 real query/plan identity differs")
            gold = set(gold_ids[:100])
            masses = Counter(int(inverse_new[positions[identifier]] // 32)
                             for identifier in gold)
            candidate = sum(masses.get(unit, 0) for unit in feature["ranked_units"])
            row = {"ordinal": index, "gold_ids": sorted(gold),
                   "candidate_ceiling": candidate,
                   "mandatory_floor": floor, "unit_cap": planned["unit_cap"],
                   "v155_sparse_source_hits": reference["arms"]["sparse"]["source_hits"]}
            arm = planned["optional_risk"]
            if not arm["feasible"]:
                infeasible += 1
                row["optional_risk"] = {"feasible": False, "reason": arm["reason"]}
                fp16_hits.append(0); sq8_hits.append(0); coverages.append(0)
                bytes_read.append(0); gets.append(0)
                baseline_hit = row["v155_sparse_source_hits"]
                wins += 0 > baseline_hit
                ties += 0 == baseline_hit
                losses += 0 < baseline_hit
                output.write(canonical(row))
                continue
            intervals = tuple(tuple(pair) for pair in arm["intervals"])
            units, count, charged = validate_intervals(
                intervals, tuple(mandatory), page_count=v194.UNIT_COUNT,
                max_units=planned["unit_cap"], max_gets=v194.MAX_GETS)
            if (units, count, charged) != (arm["units"], arm["gets"], arm["bytes"]):
                raise ValueError("V198 interval charge differs")
            ranges = [[start * v194.UNIT_BYTES, (end + 1) * v194.UNIT_BYTES]
                      for start, end in intervals]
            query = np.asarray(request["query"], dtype=np.float32)
            shortlist = score_sq8_ranges(new_sq8, query, low, step,
                                         ranges, top_k=128)
            if len(shortlist) != 128 or len(set(shortlist)) != 128:
                raise ValueError("V198 SQ8 shortlist differs")
            sq8_returned = shortlist[:100]
            fp16_returned = _rerank_real(shortlist, positions, vectors, query)
            coverage = sum(count for unit, count in masses.items()
                           if any(start <= unit <= end for start, end in intervals))
            fp16_hit = len(set(fp16_returned) & gold)
            sq8_hit = len(set(sq8_returned) & gold)
            if max(fp16_hit, sq8_hit) > coverage:
                raise ValueError("V198 returned hits exceed fetched truth")
            baseline_hit = row["v155_sparse_source_hits"]
            wins += fp16_hit > baseline_hit
            ties += fp16_hit == baseline_hit
            losses += fp16_hit < baseline_hit
            fp16_hits.append(fp16_hit); sq8_hits.append(sq8_hit)
            coverages.append(coverage); bytes_read.append(charged); gets.append(count)
            row["optional_risk"] = {"feasible": True,
                                    "fp16_returned_ids": fp16_returned,
                                    "fp16_hits": fp16_hit,
                                    "sq8_returned_ids": sq8_returned,
                                    "sq8_hits": sq8_hit,
                                    "shortlist_truth": len(set(shortlist) & gold),
                                    "coverage": coverage,
                                    "units": units, "bytes": charged, "gets": count}
            cases.write(canonical({
                "ordinal": index, "query": query.tolist(),
                "candidates": [{"ordinal": int(inverse_new[positions[identifier]]),
                                "source_id": int(identifier)} for identifier in shortlist],
                "expected": fp16_returned,
            }))
            output.write(canonical(row))
    source = _cell(fp16_hits, coverages, bytes_read, gets, infeasible)
    sq8 = _cell(sq8_hits, coverages, bytes_read, gets, infeasible)
    qualifies = (source["hits"] >= TARGET_HITS and source["p05_hits"] >= 98
                 and source["infeasible"] == 0
                 and source["bytes"] <= MAX_BYTES and source["gets"] <= MAX_GETS)
    (args.output / "summary.json").write_text(canonical({
        "schema": SCHEMA + "-summary", "dataset": "ReLAION-1M D768",
        "split": SPLIT, "queries": COUNT,
        "candidate_ceiling_hits": sum(row["candidate_ceiling"]
                                      for row in records(args.output / "raw.jsonl")),
        "optional_fp16": source, "optional_sq8": sq8,
        "v155_sparse_source_hits": sum(row["arms"]["sparse"]["source_hits"]
                                        for row in baseline),
        "paired_optional_vs_v155_sparse": {"wins": wins, "ties": ties,
                                           "losses": losses,
                                           "net_hits": source["hits"] - TARGET_HITS},
        "decision": "advance-live-s3" if qualifies else
                    "revise-candidate-plan-or-precision",
        "resident_case_count": COUNT - infeasible,
        "raw_sha256": sha256(args.output / "raw.jsonl"),
        "cases_sha256": sha256(args.output / "cases.jsonl"),
        "plan_seal_sha256": args.plan_sha256,
        "resident_plane_sha256": PLANE_SHA,
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "plan", "evaluate"))
    for name in ("output", "requests", "truth", "v155_evidence",
                 "source", "old_layout", "old_sq8", "router", "order",
                 "v164_terminal", "v192_result", "v189_features",
                 "v189_fit_labels", "new_sq8", "plane"):
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
