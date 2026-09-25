#!/usr/bin/env python3
"""Independent closed-artifact replay of V197 geometry and decision."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

HASHES = {
    "terminal": "a54ca2acd78bdb2c6b5b689a1719d18efb28e102163661919fabb98582f3fef6",
    "features": "f0c67f1aa17256dfa6aa30d5c140971b045b0429de0929b930e84945b5953dfc",
    "prepare_seal": "7a78b9ef4ca9bf6aca1538ed4775b9a1aebd099cf5f4009684fb77a15a9fee6f",
    "plans": "814acbdbe8e1229d41356006fbc53fa571a0fbf1b46f31c5ee5f531a3c9fe4f0",
    "plan_seal": "a4d8cd1ae00fd4cee152721c336abc47ed3ba9506b2e715bf088fec9076424fb",
    "raw": "f5d7623825a5cd7a6c9e01130c8112e0097c3e4bb88f6a256886f5854431e324",
    "cases": "1e382a12637672a14d6e761b0f38137cb40f3c2246dfbc0f1c597cefcfa81c5f",
    "summary": "425d63818df4c40bb83daf1dc49a73fee406ce662ffb3e44337a4567b6e9b524",
    "bench": "2ca9be71a744a0da6416c9e25a7e570f45c84d206a59a18fd0189e6c4f51a5f4",
    "decision": "d44f7fcdc049967f778473d574a6ffc703dc599880973b7e10f0340bdb20a1c3",
    "v194_prepare_seal": "d9be646c330ed16aa1dfa78714a7fe4accc59dd966a327deae0ebe19c3ed5494",
}
FILES = {"features": "out/features.jsonl", "prepare_seal": "out/prepare-seal.json",
         "plans": "out/plans.jsonl", "plan_seal": "out/plan-seal.json",
         "raw": "out/raw.jsonl", "cases": "out/cases.jsonl",
         "summary": "out/summary.json", "bench": "bench.json",
         "decision": "decision.json"}
SCHEMA = "borsuk-v197-fresh-resident-fp16-source-v1"
SOURCE_COMMIT = "e3f6ee9b35923397f3ca037be4640bd831b363ea"
SOURCE_ARCHIVE_SHA = "46941744c455486f28b83de6c7ceb0e59062b386da0ed9dfbd4ac67934e1cfc7"
INSTANCE = "i-0e3d5da7030ae3bd4"
PLANE_SHA = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47"


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def floor(mandatory: list[int]) -> int:
    gaps = sorted(right - left - 1 for left, right in zip(mandatory, mandatory[1:])
                  if right > left + 1)
    return len(mandatory) + sum(gaps[:max(0, len(gaps) + 1 - 32)])


def wilson95(failures: int, total: int) -> list[float]:
    z = 1.959963984540054
    p = failures / total
    denominator = 1 + z * z / total
    center = p + z * z / (2 * total)
    margin = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))
    return [(center - margin) / denominator, (center + margin) / denominator]


def aggregate(raw: list[dict], arm: str, score: str) -> dict:
    hits = [row["arms"][arm][score + "_hits"] if row["arms"][arm]["feasible"] else 0
            for row in raw]
    ordered = sorted(hits)
    failures = sum(hit < 98 for hit in hits)
    result = {"hits": sum(hits), "p05_hits": ordered[25],
              "min_hits": ordered[0], "below_98": failures,
              "below_98_wilson95": wilson95(failures, 512)}
    if score == "fp16":
        result.update({"coverage": sum(row["arms"][arm].get("coverage", 0)
                                       for row in raw),
                       "bytes": sum(row["arms"][arm].get("bytes", 0)
                                    for row in raw),
                       "gets": sum(row["arms"][arm].get("gets", 0)
                                   for row in raw),
                       "infeasible": sum(not row["arms"][arm]["feasible"]
                                         for row in raw)})
    return result


def check(paths: dict[str, Path]) -> dict:
    for role, expected in HASHES.items():
        if digest(paths[role]) != expected:
            raise ValueError(f"V197 {role} identity differs")
    terminal = json.loads(paths["terminal"].read_text())
    prepare = json.loads(paths["prepare_seal"].read_text())
    plan_seal = json.loads(paths["plan_seal"].read_text())
    summary = json.loads(paths["summary"].read_text())
    bench = json.loads(paths["bench"].read_text())
    decision = json.loads(paths["decision"].read_text())
    previous = json.loads(paths["v194_prepare_seal"].read_text())
    if (terminal.get("schema") != SCHEMA.removesuffix("-v1") + "-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("instance_id") != INSTANCE
            or terminal.get("source_commit") != SOURCE_COMMIT
            or terminal.get("source_archive_sha256") != SOURCE_ARCHIVE_SHA
            or any(terminal["artifacts"][name]["sha256"] != HASHES[role]
                   or terminal["artifacts"][name]["bytes"] != paths[role].stat().st_size
                   for role, name in FILES.items())):
        raise ValueError("V197 complete terminal authority differs")
    features, plans, raw, cases = [lines(paths[name]) for name in
                                   ("features", "plans", "raw", "cases")]
    if any(len(rows) != 512 for rows in (features, plans, raw, cases)):
        raise ValueError("V197 cohort length differs")
    if (prepare.get("schema") != SCHEMA + "-prepare-seal"
            or prepare.get("source_truth_opened") is not False
            or prepare.get("split") != "source-pseudoquery-hash-ranks-3457-3968"
            or prepare.get("features_sha256") != HASHES["features"]
            or prepare.get("resident_plane_sha256") != PLANE_SHA
            or plan_seal.get("schema") != SCHEMA + "-plan-seal"
            or plan_seal.get("source_truth_opened") is not False
            or plan_seal.get("prepare_seal_sha256") != HASHES["prepare_seal"]
            or plan_seal.get("plans_sha256") != HASHES["plans"]
            or plan_seal.get("resident_plane_sha256") != PLANE_SHA
            or plan_seal.get("base_unit_cap") != 672
            or plan_seal.get("max_gets") != 32
            or plan_seal.get("aggregate_caps") != [5_700_611_604, 11_328]
            or set(prepare["pseudo_ids"]) & set(previous["pseudo_ids"])):
        raise ValueError("V197 seals, policy or split identity differs")
    for index, (feature, plan, row, case) in enumerate(
            zip(features, plans, raw, cases, strict=True)):
        ordinal = 3456 + index
        stable_id = feature["source_id"]
        mandatory = feature["mandatory_units"]
        minimum = floor(mandatory)
        if (any(item["ordinal"] != ordinal or item["source_id"] != stable_id
                for item in (feature, plan, row))
                or prepare["pseudo_ids"][index] != stable_id
                or mandatory != sorted(set(mandatory))
                or not set(mandatory).issubset(feature["ranked_units"])
                or minimum != plan["mandatory_floor"]
                or minimum != row["mandatory_floor"]
                or plan["unit_cap"] != max(672, minimum)
                or row["unit_cap"] != plan["unit_cap"]
                or len(row["gold_ids"]) != 100
                or len(set(row["gold_ids"])) != 100
                or stable_id in row["gold_ids"]
                or not 0 <= row["candidate_ceiling"] <= 100):
            raise ValueError(f"V197 identity or floor differs at {ordinal}")
        gold = set(row["gold_ids"])
        for arm in ("optional_risk", "full_rank"):
            chosen, measured = plan["arms"][arm], row["arms"][arm]
            if chosen["feasible"] != measured["feasible"]:
                raise ValueError("V197 plan feasibility differs")
            if not chosen["feasible"]:
                continue
            intervals = chosen["intervals"]
            units = sum(end - start + 1 for start, end in intervals)
            if (not 1 <= len(intervals) <= 32
                    or any(not 0 <= start <= end < 31_250 for start, end in intervals)
                    or any(left[1] >= right[0]
                           for left, right in zip(intervals, intervals[1:]))
                    or units > plan["unit_cap"]
                    or not all(any(start <= unit <= end for start, end in intervals)
                               for unit in mandatory)
                    or chosen["units"] != units or measured["units"] != units
                    or chosen["bytes"] != units * 24_960
                    or measured["bytes"] != units * 24_960
                    or chosen["gets"] != len(intervals)
                    or measured["gets"] != len(intervals)):
                raise ValueError("V197 physical interval witness differs")
            fp16, sq8 = measured["fp16_returned_ids"], measured["sq8_returned_ids"]
            if (len(fp16) != 100 or len(set(fp16)) != 100
                    or len(sq8) != 100 or len(set(sq8)) != 100
                    or stable_id in fp16 or stable_id in sq8
                    or measured["fp16_hits"] != len(set(fp16) & gold)
                    or measured["sq8_hits"] != len(set(sq8) & gold)
                    or not max(measured["fp16_hits"], measured["sq8_hits"]) <=
                           measured["shortlist_truth"] <= measured["coverage"] <= 100):
                raise ValueError("V197 returned GT100 intersections differ")
        optional = row["arms"]["optional_risk"]
        candidate_ids = [item["source_id"] for item in case["candidates"]]
        ordinals = [item["ordinal"] for item in case["candidates"]]
        if (case["ordinal"] != ordinal
                or len(case["query"]) != 768
                or not all(math.isfinite(value) for value in case["query"])
                or len(candidate_ids) != 128 or len(set(candidate_ids)) != 128
                or len(set(ordinals)) != 128
                or not all(0 <= value < 1_000_000 for value in ordinals)
                or stable_id in candidate_ids
                or case["expected"] != optional["fp16_returned_ids"]
                or not set(case["expected"]).issubset(candidate_ids)):
            raise ValueError("V197 resident case witness differs")
    optional = aggregate(raw, "optional_risk", "fp16")
    full = aggregate(raw, "full_rank", "fp16")
    sq8 = aggregate(raw, "optional_risk", "sq8")
    for name, recomputed in (("optional_fp16", optional),
                             ("full_rank_fp16", full), ("optional_sq8", sq8)):
        prior = summary[name]
        if prior.keys() != recomputed.keys() or any(
                not (all(math.isclose(x, y) for x, y in zip(prior[key], value))
                     if isinstance(value, list) else prior[key] == value)
                for key, value in recomputed.items()):
            raise ValueError(f"V197 {name} aggregate differs")
    qualifies = (optional["hits"] >= 50_979 and optional["p05_hits"] >= 98
                 and optional["infeasible"] == 0
                 and optional["bytes"] <= 5_700_611_604
                 and optional["gets"] <= 11_328)
    if (summary.get("schema") != SCHEMA + "-summary"
            or summary.get("raw_sha256") != HASHES["raw"]
            or summary.get("cases_sha256") != HASHES["cases"]
            or summary.get("plan_seal_sha256") != HASHES["plan_seal"]
            or summary.get("candidate_ceiling_hits") != sum(
                item["candidate_ceiling"] for item in raw)
            or summary.get("paired_fp16_minus_sq8_hits") != optional["hits"] - sq8["hits"]
            or summary.get("paired_optional_minus_full_hits") != optional["hits"] - full["hits"]
            or summary.get("decision") != (
                "advance-real-query-live-s3" if qualifies else
                "revise-candidate-plan-or-precision")):
        raise ValueError("V197 source decision arithmetic differs")
    reps = bench.get("repetitions", [])
    rust_pass = (bench.get("schema") == "borsuk-resident-fp16-preflight-v2"
                 and bench.get("cases") == 512
                 and bench.get("first_ordinal") == 3456
                 and bench.get("exact_returned_sets") == 5120
                 and bench.get("rows") == 1_000_000
                 and bench.get("dimensions") == 768
                 and bench.get("generation") == 196
                 and bench.get("charged_plane_bytes") == 1_544_000_000
                 and len(reps) == 10
                 and [item["repetition"] for item in reps] == list(range(1, 11))
                 and all(0 < item["p50_ns"] <= item["p95_ns"] <=
                         item["p99_ns"] <= item["max_ns"] for item in reps)
                 and max(item["p95_ns"] for item in reps) <= 2_000_000
                 and max(item["p99_ns"] for item in reps) <= 5_000_000
                 and bench.get("process_peak_rss_bytes", 2**64) <= 2 * 1024**3)
    if (decision.get("schema") != "borsuk-v197-fresh-resident-fp16-decision-v1"
            or decision.get("source_decision") != summary["decision"]
            or decision.get("rust_parity_and_resource_pass") != rust_pass
            or decision.get("decision") != (
                "advance-real-query-live-s3" if qualifies and rust_pass else
                "revise-candidate-plan-or-precision")):
        raise ValueError("V197 Rust parity or terminal decision differs")
    return {"status": "pass", "decision": decision["decision"],
            "optional_fp16": optional, "optional_sq8": sq8,
            "full_rank_fp16": full,
            "candidate_ceiling_hits": summary["candidate_ceiling_hits"],
            "worst_p95_ns": max(item["p95_ns"] for item in reps),
            "worst_p99_ns": max(item["p99_ns"] for item in reps),
            "process_peak_rss_bytes": bench["process_peak_rss_bytes"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    for role in HASHES:
        parser.add_argument("--" + role.replace("_", "-"), type=Path,
                            required=True)
    args = parser.parse_args()
    print(json.dumps(check(vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
