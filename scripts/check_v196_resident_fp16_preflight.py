#!/usr/bin/env python3
"""Replay completed V196 used-panel identity and local process gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

SCHEMA = "borsuk-v196-resident-fp16-preflight-spot-v1"
SOURCE_COMMIT = "c5fbb27ffba5fe5a78f15a4fb9e9c836a2afba04"
SOURCE_ARCHIVE_SHA = "1cbc37ef28e570f3439b565a4c31d9053db2a47279cfc149cf6a0715ccddede7"
INSTANCE = "i-057435f1b4c5b580d"
HASHES = {
    "terminal": "a73ce19c371751d391d9c35530e6529adbaca9e8c85b525d1d6028218c47a5f3",
    "case_summary": "7bbf7457573bc47c51f5d4e311b2633563e425db98bcf855e48954790de33eb3",
    "cases": "153813b1f1910145986ce340ddc55197e5549005ea3d600848af8e3b63c2e7ca",
    "bench": "6037dab430b27519fb43ce74b1bbca85a572c14dbd1a1780b3759e5ae83a86ab",
    "summary": "aa6cd7fc1057fc83f454a38194e78dc19cc600b3ce1b259835c4bbe4dbd34701",
    "v195_raw": "998cc015f670e470bbb435d47fc49d0aac5b22d0315a7d51eae326e4e48e18fa",
}
ARTIFACT_ROLES = {
    "case_summary": "case-summary.json", "cases": "cases.jsonl",
    "bench": "bench.json", "summary": "summary.json",
}


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def check(paths: dict[str, Path]) -> dict:
    for role, expected in HASHES.items():
        if digest(paths[role]) != expected:
            raise ValueError(f"V196 {role} identity differs")
    terminal = json.loads(paths["terminal"].read_text())
    case_summary = json.loads(paths["case_summary"].read_text())
    bench = json.loads(paths["bench"].read_text())
    summary = json.loads(paths["summary"].read_text())
    if (terminal.get("schema") != SCHEMA
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0
            or terminal.get("instance_id") != INSTANCE
            or terminal.get("source_commit") != SOURCE_COMMIT
            or terminal.get("source_archive_sha256") != SOURCE_ARCHIVE_SHA):
        raise ValueError("V196 completed terminal authority differs")
    for role, name in ARTIFACT_ROLES.items():
        if (terminal["artifacts"][name]["sha256"] != HASHES[role]
                or terminal["artifacts"][name]["bytes"] != paths[role].stat().st_size):
            raise ValueError(f"V196 {role} terminal artifact differs")
    if (terminal["artifacts"]["plane.bin"] != {
            "bytes": 1_544_000_064,
            "sha256": "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47"}
            or case_summary.get("schema") != "borsuk-v196-resident-fp16-cases-v1"
            or case_summary.get("plane_bytes") != terminal["artifacts"]["plane.bin"]["bytes"]
            or case_summary.get("plane_sha256") != terminal["artifacts"]["plane.bin"]["sha256"]
            or case_summary.get("cases_sha256") != HASHES["cases"]
            or case_summary.get("v195_raw_sha256") != HASHES["v195_raw"]
            or case_summary.get("rows") != 1_000_000
            or case_summary.get("dimensions") != 768
            or case_summary.get("generation") != 196):
        raise ValueError("V196 plane/case authority differs")
    cases, prior = lines(paths["cases"]), lines(paths["v195_raw"])
    if len(cases) != 512 or len(prior) != 512:
        raise ValueError("V196 used panel count differs")
    for index, (case, previous) in enumerate(zip(cases, prior, strict=True)):
        expected_ordinal = 2944 + index
        candidates = case["candidates"]
        ids = [entry["source_id"] for entry in candidates]
        ordinals = [entry["ordinal"] for entry in candidates]
        expected = case["expected"]
        if (case["ordinal"] != expected_ordinal
                or previous["ordinal"] != expected_ordinal
                or len(case["query"]) != 768
                or not all(math.isfinite(value) for value in case["query"])
                or len(candidates) != 128 or len(set(ids)) != 128
                or len(set(ordinals)) != 128
                or not all(0 <= ordinal < 1_000_000 for ordinal in ordinals)
                or previous["source_id"] in ids
                or len(expected) != 100 or len(set(expected)) != 100
                or not set(expected).issubset(ids)
                or expected != previous["k"]["128"]["fp16_returned_ids"]):
            raise ValueError(f"V196 case differs at ordinal {expected_ordinal}")
    reps = bench.get("repetitions", [])
    if (bench.get("schema") != "borsuk-v196-resident-fp16-preflight-v1"
            or bench.get("cases") != 512
            or bench.get("exact_returned_sets") != 5120
            or bench.get("rows") != 1_000_000
            or bench.get("dimensions") != 768
            or bench.get("generation") != 196
            or bench.get("charged_plane_bytes") != 1_544_000_000
            or len(reps) != 10
            or [row["repetition"] for row in reps] != list(range(1, 11))
            or any(not 0 < row["p50_ns"] <= row["p95_ns"] <= row["p99_ns"] <= row["max_ns"]
                   for row in reps)
            or summary.get("schema") != "borsuk-v196-resident-fp16-summary-v1"
            or summary.get("case_summary") != case_summary
            or summary.get("benchmark") != bench):
        raise ValueError("V196 process or summary authority differs")
    worst_p95 = max(row["p95_ns"] for row in reps)
    worst_p99 = max(row["p99_ns"] for row in reps)
    passed = (worst_p95 <= 2_000_000 and worst_p99 <= 5_000_000
              and bench["process_peak_rss_bytes"] <= 2 * 1024**3)
    if summary.get("decision") != (
            "advance-fresh-holdout" if passed else "revise-resident-tier"):
        raise ValueError("V196 decision arithmetic differs")
    return {"status": "pass", "decision": summary["decision"],
            "exact_returned_sets": 5120, "worst_p95_ns": worst_p95,
            "worst_p99_ns": worst_p99,
            "process_peak_rss_bytes": bench["process_peak_rss_bytes"],
            "hydration_ns": bench["hydration_ns"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    for role in HASHES:
        parser.add_argument("--" + role.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
