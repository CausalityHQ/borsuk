#!/usr/bin/env python3
"""Independent replay of the closed V203 exact-witness Spot cell."""

from __future__ import annotations

import hashlib
import json

import boto3

from scripts.launch_v203_hard_dp_loop_spot import BUCKET, INPUTS, REGION
from scripts.unconstrained_priced_interval import unconstrained_priced_cover

PREFIX = ("research/v203-hard-dp-loop/"
          "2cc92e1afc1d084f5979d6123eb5c092cb02bc19/runs/a0001/")
TERMINAL_SHA = "8c1eb0a49c5c430c056866e06742d68fbab5c3744f4f10c416d8703f907aa4d1"
SOURCE = "2cc92e1afc1d084f5979d6123eb5c092cb02bc19"
INSTANCE = "i-08e96436f3b39e304"


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def lines(raw: bytes) -> list[dict]:
    return [json.loads(line) for line in raw.splitlines() if line]


def percentile(values: list[int], point: int) -> int:
    ranked = sorted(values)
    return ranked[(len(ranked) * point + 99) // 100 - 1]


def check() -> dict:
    s3 = boto3.Session(profile_name="causality", region_name=REGION).client("s3")

    def read(key: str) -> bytes:
        return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()

    terminal_bytes = read(PREFIX + "terminal.json")
    terminal = json.loads(terminal_bytes)
    if (digest(terminal_bytes) != TERMINAL_SHA
            or terminal.get("schema") != "borsuk-v203-hard-dp-loop-spot-v1"
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("source_commit") != SOURCE
            or terminal.get("instance_id") != INSTANCE):
        raise ValueError("V203 terminal identity differs")
    artifacts = {}
    for name, identity in terminal["artifacts"].items():
        raw = read(PREFIX + "artifacts/" + name)
        if identity != {"bytes": len(raw), "sha256": digest(raw)}:
            raise ValueError(f"V203 closed artifact differs: {name}")
        artifacts[name] = raw
    if set(artifacts) != {"raw.jsonl", "bench.json", "bench-resources.txt",
                          "build.log", "test.log", "run-closed.log"}:
        raise ValueError("V203 artifact roster differs")
    test_log = artifacts["test.log"].decode(errors="replace")
    if "test result: ok. 2 passed; 0 failed" not in test_log:
        raise ValueError("V203 targeted Rust tests did not pass")

    frozen = {}
    for name, key, size, expected in INPUTS:
        raw = read(key)
        if len(raw) != size or digest(raw) != expected:
            raise ValueError(f"V203 frozen input differs: {name}")
        frozen[name] = lines(raw)
    weights, plans = frozen["weights.jsonl"], frozen["plans.jsonl"]
    rows = lines(artifacts["raw.jsonl"])
    bench = json.loads(artifacts["bench.json"])
    if len(rows) != len(weights) or len(rows) != len(plans) or len(rows) != 1000:
        raise ValueError("V203 cohort length differs")
    counts = {name: 0 for name in ("linear", "unit", "get", "hard")}
    linear_times, unit_times, get_times, hard_times, total_times = [], [], [], [], []
    for ordinal, (row, source, plan) in enumerate(zip(rows, weights, plans, strict=True)):
        if (row["ordinal"] != ordinal or source["ordinal"] != ordinal
                or plan["ordinal"] != ordinal):
            raise ValueError(f"V203 ordinal differs: {ordinal}")
        tier = row["tier"]
        if tier not in counts:
            raise ValueError(f"V203 tier differs: {ordinal}")
        counts[tier] += 1
        linear = unconstrained_priced_cover(
            dict(source["weights"]), source["mandatory"], page_count=31_250,
            unit_price=1000, get_price=50_000)
        linear_admitted = linear.units <= plan["unit_cap"] and linear.gets <= 32
        if (tier == "linear") != linear_admitted:
            raise ValueError(f"V203 linear admission differs: {ordinal}")
        if tier in ("linear", "unit", "get", "hard"):
            reference = plan["optional_risk"]
            if (not reference["feasible"] or row["intervals"] != reference["intervals"]
                    or row["mass"] != reference["predicted_mass"]
                    or row["units"] != reference["units"]
                    or row["gets"] != reference["gets"]
                    or row["units"] > plan["unit_cap"] or row["gets"] > 32):
                raise ValueError(f"V203 exact physical witness differs: {ordinal}")
            if tier == "linear" and (row["intervals"] != [list(pair) for pair in linear.intervals]
                                     or row["mass"] != linear.mass):
                raise ValueError(f"V203 Rust/Python linear cover differs: {ordinal}")
        linear_times.append(row["linear_ns"])
        total_times.append(row["total_ns"])
        if tier != "linear":
            unit_times.append(row["unit_ns"])
        if row["get_ns"]:
            get_times.append(row["get_ns"])
        if row["hard_ns"]:
            hard_times.append(row["hard_ns"])
    expected_counts = {"linear_exact": counts["linear"],
                       "unit_exact": counts["unit"], "get_exact": counts["get"],
                       "hard_exact": counts["hard"]}
    if any(bench[key] != value for key, value in expected_counts.items()):
        raise ValueError("V203 tier counts differ")
    for name, times in (("linear_ns", linear_times), ("unit_ns", unit_times),
                        ("get_ns", get_times), ("hard_ns", hard_times), ("all_total_ns", total_times)):
        expected = {f"p{point}": percentile(times, point) for point in (50, 95, 99)}
        if bench[name] != expected:
            raise ValueError(f"V203 percentile differs: {name}")
    if bench.get("peak_process_rss_bytes", 0) <= 0:
        raise ValueError("V203 RSS missing")
    return {"status": "pass", "source_commit": SOURCE,
            "terminal_sha256": TERMINAL_SHA, "tiers": expected_counts,
            "timings_ns": {name: bench[name] for name in
                           ("linear_ns", "unit_ns", "get_ns", "hard_ns", "all_total_ns")},
            "peak_process_rss_bytes": bench["peak_process_rss_bytes"]}


if __name__ == "__main__":
    print(json.dumps(check(), sort_keys=True))
