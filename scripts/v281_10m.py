#!/usr/bin/env python3
"""Freeze the CoHere10M reverse-edge transfer decision from sealed files."""

import argparse
import hashlib
import json
from pathlib import Path

V272_RAW_SHA = "62e8065f8cd7b1d06b23daef50519adfb0a64e6b65b8e1ed171f314d9686f3e3"
V272_ROOT_SHA = "948e8a5555f44011b22681ca2cd20edde93f6fec5e6261e20e3a7b799d467022"
TRUTH_SHA = "9d08b49fef274d5bee2572ed8ed186ff8f2063b759f556748fe83b6e1d21c4f1"
ROWS = 10_000_000


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def decide(baseline, candidate, old_root, new_root, resources):
    if resources["baseline_parity"] != 1000 or baseline["combined"]["hits"] != 99_299:
        return "invalid_cell"
    if candidate["combined"]["hits"] != sum(
        candidate[key]["hits"]
        for key in (
            "development_first256_prior_used",
            "validation_remaining744_prior_used",
        )
    ):
        return "invalid_cell"
    if any(
        old_root[name] != new_root[name]
        for name in (
            "source_sha256",
            "rows",
            "dimensions",
            "plane",
            "map",
            "books",
            "codes",
        )
    ):
        return "invalid_cell"
    if old_root["rows"] != ROWS or old_root["dimensions"] != 768:
        return "invalid_cell"
    old_misses = 100_000 - baseline["combined"]["hits"]
    new_misses = 100_000 - candidate["combined"]["hits"]
    quality = (
        all(
            candidate[key]["recall_at_100"] >= 0.995
            and candidate[key]["p05_hits"] >= 98
            for key in (
                "development_first256_prior_used",
                "validation_remaining744_prior_used",
            )
        )
        and new_misses * 5 <= old_misses * 4
    )
    resources_ok = (
        candidate["p95_ms"] <= min(150, 1.2 * baseline["p95_ms"])
        and candidate["p99_ms"] <= min(180, 1.2 * baseline["p99_ms"])
        and resources["search_rss_bytes"] <= 32 * 1024**3
        and resources["build_rss_bytes"] <= 8_000 * ROWS
        and resources["build_seconds"] <= 1.5 * 24_796
        and new_root["graph"]["bytes"] <= 1.15 * old_root["graph"]["bytes"]
        and resources["hydration_gets"] == 5
        and resources["query_gets"] == 0
    )
    return "go_http_10m" if quality and resources_ok else "no_go_10m"


def run(args):
    if digest(args.old_raw) != V272_RAW_SHA or digest(args.old_root) != V272_ROOT_SHA:
        raise ValueError("V272 baseline identity differs")
    if digest(args.truth) != TRUTH_SHA:
        raise ValueError("V272 exact truth differs")
    old = [json.loads(line)["ids"] for line in args.old_raw.read_text().splitlines()]
    current = [
        json.loads(line)["ids"] for line in args.baseline_raw.read_text().splitlines()
    ]
    if len(old) != 1000 or len(current) != 1000:
        raise ValueError("baseline query count differs")
    resources = json.loads(args.resources.read_text())
    resources["baseline_parity"] = sum(
        a == b for a, b in zip(old, current, strict=True)
    )
    baseline = json.loads(args.baseline_quality.read_text())
    candidate = json.loads(args.candidate_quality.read_text())
    old_root = json.loads(args.old_root.read_text())
    new_root = json.loads(args.candidate_root.read_text())
    result = {
        "decision": decide(baseline, candidate, old_root, new_root, resources),
        "baseline": baseline,
        "candidate": candidate,
        "resources": resources,
        "candidate_root_sha256": digest(args.candidate_root),
        "baseline_root_sha256": V272_ROOT_SHA,
        "truth_sha256": TRUTH_SHA,
    }
    args.output.write_text(
        json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in (
        "old-raw",
        "old-root",
        "baseline-raw",
        "baseline-quality",
        "candidate-quality",
        "candidate-root",
        "resources",
        "truth",
        "output",
    ):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
