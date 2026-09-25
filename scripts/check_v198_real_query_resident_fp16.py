#!/usr/bin/env python3
"""Independent replay of the closed V198 real-query decision artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

ROOT = "artifacts/"
HASHES = {
    "terminal.json": "b3ae47adc77e155b12ddd23e2a061f90ca06415d828586bc4df36e51dd833994",
    "artifacts/out/features.jsonl": "3b4fe2d7a83bd0af16b1ecf460052b78311c6526bd7263fb3e708b56dd26a08c",
    "artifacts/out/prepare-seal.json": "f3254834d87ed68b215ab2716ca5d385cd9f81ca429e29b072fc98fc5176b66c",
    "artifacts/out/plans.jsonl": "0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00",
    "artifacts/out/plan-seal.json": "fb4328a41537db4ed0beb15fb85a914668afc60a072954194a39fe5bf7605fc2",
    "artifacts/out/raw.jsonl": "2b18321435642de3fad4df02b84abcc046fb6c9b17808e72d6a50eea54a9ec98",
    "artifacts/out/cases.jsonl": "e1ba95fbd0cb58e28f1601740edf8c8775230e2ab5c8954e7f11c522dd94cd39",
    "artifacts/out/summary.json": "69e30c056e744663e3d79d641d3e5412e9db69280092cd7c96f528e1dccb60c4",
    "artifacts/bench.json": "b1a8fdbfcae45bfc672b459c93d32ca9a2b0f073fa13a7b33ecbbb4cd1ad78ee",
    "artifacts/decision.json": "4527d16935bf77b7a4263543031341c9c972bfeea7a7511f009da6a3af624d52",
    "truth.parquet": "bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871",
    "v155-evidence.jsonl": "dd4d4a7c9448ce6833357e70bff5f72bd9a782afd8185114cb501b2a63a49a1a",
}
SCHEMA = "borsuk-v198-real-query-resident-fp16-v1"
PLANE_SHA = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47"


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def floor(mandatory: list[int]) -> int:
    gaps = sorted(right - left - 1 for left, right in zip(mandatory, mandatory[1:])
                  if right > left + 1)
    return len(mandatory) + sum(gaps[:max(0, len(gaps) + 1 - 32)])


def aggregate(rows: list[dict], score: str) -> dict:
    hits = [row["optional_risk"][score + "_hits"] for row in rows]
    ordered = sorted(hits)
    failures = sum(hit < 98 for hit in hits)
    z = 1.959963984540054
    p = failures / 1000
    denom = 1 + z * z / 1000
    center = p + z * z / 2000
    margin = z * math.sqrt(p * (1 - p) / 1000 + z * z / (4 * 1000**2))
    return {"hits": sum(hits), "p05_hits": ordered[49], "min_hits": ordered[0],
            "below_98": failures,
            "below_98_wilson95": [(center - margin) / denom, (center + margin) / denom],
            "coverage": sum(row["optional_risk"]["coverage"] for row in rows),
            "bytes": sum(row["optional_risk"]["bytes"] for row in rows),
            "gets": sum(row["optional_risk"]["gets"] for row in rows),
            "infeasible": sum(not row["optional_risk"]["feasible"] for row in rows)}


def same_cell(actual: dict, claimed: dict) -> bool:
    return (actual.keys() == claimed.keys()
            and all(all(math.isclose(a, b, rel_tol=0, abs_tol=1e-15)
                        for a, b in zip(value, claimed[key], strict=True))
                    if isinstance(value, list) else value == claimed[key]
                    for key, value in actual.items()))


def check(root: Path) -> dict:
    for name, expected in HASHES.items():
        if sha(root / name) != expected:
            raise ValueError(f"V198 {name} SHA-256 differs")
    terminal = json.loads((root / "terminal.json").read_text())
    prepare = json.loads((root / ROOT / "out/prepare-seal.json").read_text())
    seal = json.loads((root / ROOT / "out/plan-seal.json").read_text())
    summary = json.loads((root / ROOT / "out/summary.json").read_text())
    bench = json.loads((root / ROOT / "bench.json").read_text())
    decision = json.loads((root / ROOT / "decision.json").read_text())
    if (terminal.get("schema") != "borsuk-v198-real-query-resident-fp16-spot-v1"
            or terminal.get("status") != "complete" or terminal.get("exit_code") != 0
            or terminal.get("phase") != "complete"
            or terminal.get("instance_id") != "i-03e1b116ca4d427b7"
            or terminal.get("source_commit") != "fdc51a358be350678d9f6f279a2be95a57709c02"
            or terminal.get("source_archive_sha256") !=
                "19afd543e0685a425c7c7b3591c0f1d71059f224c0a0317c86a57c257eaa09af"
            or any(terminal["artifacts"][name.removeprefix(ROOT)]["sha256"] != digest
                   or terminal["artifacts"][name.removeprefix(ROOT)]["bytes"] !=
                   (root / name).stat().st_size
                   for name, digest in HASHES.items() if name.startswith(ROOT))):
        raise ValueError("V198 terminal authority differs")
    if (prepare.get("schema") != SCHEMA + "-prepare-seal"
            or prepare.get("source_truth_opened") is not False
            or prepare.get("split") != "validation-1000-already-used"
            or prepare.get("requests_sha256") !=
                "c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9"
            or prepare.get("features_sha256") != HASHES[ROOT + "out/features.jsonl"]
            or prepare.get("resident_plane_sha256") != PLANE_SHA
            or seal.get("schema") != SCHEMA + "-plan-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("prepare_seal_sha256") != HASHES[ROOT + "out/prepare-seal.json"]
            or seal.get("plans_sha256") != HASHES[ROOT + "out/plans.jsonl"]
            or seal.get("resident_plane_sha256") != PLANE_SHA
            or seal.get("base_unit_cap") != 672 or seal.get("max_gets") != 32
            or seal.get("aggregate_caps") != [11_134_007_040, 22_126]):
        raise ValueError("V198 GT-blind seal or policy differs")
    features, plans, raw, cases, baseline = [records(root / path) for path in (
        ROOT + "out/features.jsonl", ROOT + "out/plans.jsonl",
        ROOT + "out/raw.jsonl", ROOT + "out/cases.jsonl", "v155-evidence.jsonl")]
    if any(len(rows) != 1000 for rows in (features, plans, raw, cases, baseline)):
        raise ValueError("V198 paired cohort length differs")
    for i, (feature, plan, row, case, prior) in enumerate(
            zip(features, plans, raw, cases, baseline, strict=True)):
        mandatory = feature["mandatory_units"]
        ranked = feature["ranked_units"]
        minimum = floor(mandatory)
        if (any(item["ordinal"] != i for item in (feature, plan, row, case))
                or prior["query_ordinal"] != i
                or mandatory != sorted(set(mandatory))
                or len(set(ranked)) != len(ranked)
                or not set(mandatory).issubset(ranked)
                or not 0 <= row["candidate_ceiling"] <= 100
                or len(row["gold_ids"]) != 100 or len(set(row["gold_ids"])) != 100
                or any(item["mandatory_floor"] != minimum for item in (plan, row))
                or any(item["unit_cap"] != max(672, minimum) for item in (plan, row))
                or row["v155_sparse_source_hits"] != prior["arms"]["sparse"]["source_hits"]):
            raise ValueError(f"V198 paired identity/floor differs at {i}")
        chosen, measured = plan["optional_risk"], row["optional_risk"]
        if not chosen["feasible"] or not measured["feasible"]:
            raise ValueError(f"V198 infeasible plan at {i}")
        intervals = chosen["intervals"]
        units = sum(end - start + 1 for start, end in intervals)
        if (not 1 <= len(intervals) <= 32
                or any(not 0 <= start <= end < 31_250 for start, end in intervals)
                or any(left[1] >= right[0] for left, right in zip(intervals, intervals[1:]))
                or not all(any(start <= unit <= end for start, end in intervals)
                           for unit in mandatory)
                or units > plan["unit_cap"]
                or any(item["units"] != units or item["bytes"] != units * 24_960
                       or item["gets"] != len(intervals) for item in (chosen, measured))):
            raise ValueError(f"V198 physical witness differs at {i}")
        gold = set(row["gold_ids"])
        candidates = [item["source_id"] for item in case["candidates"]]
        if (len(case["query"]) != 768 or len(candidates) != 128
                or len(set(candidates)) != 128
                or any(not 0 <= item["ordinal"] < 1_000_000 for item in case["candidates"])
                or any(not math.isfinite(value) for value in case["query"])
                or case["expected"] != measured["fp16_returned_ids"]):
            raise ValueError(f"V198 resident case differs at {i}")
        for score in ("fp16", "sq8"):
            returned = measured[score + "_returned_ids"]
            if (len(returned) != 100 or len(set(returned)) != 100
                    or not set(returned).issubset(candidates)
                    or measured[score + "_hits"] != len(set(returned) & gold)):
                raise ValueError(f"V198 {score} returned intersection differs at {i}")
        if not (max(measured["fp16_hits"], measured["sq8_hits"])
                <= measured["shortlist_truth"] <= measured["coverage"] <= 100):
            raise ValueError(f"V198 coverage differs at {i}")
    fp16, sq8 = aggregate(raw, "fp16"), aggregate(raw, "sq8")
    if (summary.get("schema") != SCHEMA + "-summary"
            or summary.get("raw_sha256") != HASHES[ROOT + "out/raw.jsonl"]
            or summary.get("cases_sha256") != HASHES[ROOT + "out/cases.jsonl"]
            or summary.get("plan_seal_sha256") != HASHES[ROOT + "out/plan-seal.json"]
            or summary.get("candidate_ceiling_hits") != sum(r["candidate_ceiling"] for r in raw)
            or summary.get("v155_sparse_source_hits") != 99_567
            or not same_cell(fp16, summary.get("optional_fp16", {}))
            or not same_cell(sq8, summary.get("optional_sq8", {}))):
        raise ValueError("V198 summary arithmetic differs")
    paired = {"wins": 0, "ties": 0, "losses": 0, "net_hits": fp16["hits"] - 99_567}
    for row in raw:
        delta = row["optional_risk"]["fp16_hits"] - row["v155_sparse_source_hits"]
        paired["wins" if delta > 0 else "losses" if delta < 0 else "ties"] += 1
    qualifies = (fp16["hits"] >= 99_567 and fp16["p05_hits"] >= 98
                 and fp16["infeasible"] == 0 and fp16["bytes"] <= 11_134_007_040
                 and fp16["gets"] <= 22_126)
    if (summary["paired_optional_vs_v155_sparse"] != paired
            or summary["decision"] != ("advance-live-s3" if qualifies else
                                         "revise-candidate-plan-or-precision")):
        raise ValueError("V198 source decision differs")
    reps = bench.get("repetitions", [])
    rust_pass = (bench.get("schema") == "borsuk-resident-fp16-preflight-v3"
                 and bench.get("cases") == 1000
                 and bench.get("first_ordinal") == 0
                 and bench.get("exact_returned_sets") == 10000
                 and bench.get("charged_plane_bytes") == 1_544_000_000
                 and len(reps) == 10
                 and [r["repetition"] for r in reps] == list(range(1, 11))
                 and all(0 < r["p50_ns"] <= r["p95_ns"] <= r["p99_ns"] <= r["max_ns"]
                         for r in reps)
                 and max(r["p95_ns"] for r in reps) <= 2_000_000
                 and max(r["p99_ns"] for r in reps) <= 5_000_000
                 and bench.get("process_peak_rss_bytes", 2**64) <= 2 * 1024**3)
    if (decision.get("source_decision") != summary["decision"]
            or decision.get("rust_parity_and_resource_pass") != rust_pass
            or decision.get("decision") != ("advance-live-s3" if qualifies and rust_pass
                                            else "revise-candidate-plan-or-precision")):
        raise ValueError("V198 Rust/terminal decision differs")
    return {"status": "pass", "decision": decision["decision"],
            "optional_fp16": fp16, "optional_sq8": sq8,
            "candidate_ceiling_hits": summary["candidate_ceiling_hits"],
            "paired": paired, "worst_p95_ns": max(r["p95_ns"] for r in reps),
            "worst_p99_ns": max(r["p99_ns"] for r in reps),
            "process_peak_rss_bytes": bench["process_peak_rss_bytes"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    print(json.dumps(check(parser.parse_args().root), sort_keys=True))


if __name__ == "__main__":
    main()
