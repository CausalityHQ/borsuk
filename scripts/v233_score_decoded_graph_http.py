#!/usr/bin/env python3
"""Score decoded-delta HTTP against the sealed V230 linear-delta pass."""

import argparse
import json
import math
from pathlib import Path

from scripts.v219_score_reachable_graph_1m import V198_SHA, digest, records

REFERENCE = {
    "first_pass": ("b4597049dd6959dbf5516342408a31e863fbe13505987f88811d58a4ccdd5d14",
                   38_788_652, 244.07717750715977),
    "immediate_repeat": ("e14e53a2601e0b6f0fe64bdf227b9c4a2431e7db21d93cd4db3acd1967dbf30c",
                         38_195_128, 247.92902013504562),
}


def run(args: argparse.Namespace) -> None:
    summary = json.loads(args.summary.read_text())
    label = summary["pass_label"]
    reference_sha, reference_p95, reference_qps = REFERENCE[label]
    if (digest(args.truth) != V198_SHA or digest(args.reference) != reference_sha
            or summary["schema"] != "borsuk-v222-graph-http-client-1m-v1"
            or summary["raw_sha256"] != digest(args.raw)
            or summary["queries"] != 1000 or summary["concurrency"] != 8
            or summary["transport"] != "persistent HTTP/1.1 VPC peer"
            or summary["vector_body_gets"] != 0):
        raise ValueError("decoded HTTP measurement identity differs")
    current, reference, truth = (records(path) for path in
                                 (args.raw, args.reference, args.truth))
    if any(len(rows) != 1000 for rows in (current, reference, truth)):
        raise ValueError("paired panel length differs")
    latencies, hits, matches = [], [], 0
    for ordinal, (row, old, gold) in enumerate(zip(current, reference, truth, strict=True)):
        ids = row["returned_ids"]
        if (row["ordinal"] != ordinal or old["ordinal"] != ordinal
                or gold["ordinal"] != ordinal or len(ids) != 100
                or len(set(ids)) != 100 or row["vector_body_gets"] != 0):
            raise ValueError(f"case geometry differs at {ordinal}")
        matches += ids == old["returned_ids"]
        hits.append(len(set(ids) & set(gold["gold_ids"])))
        latencies.append(row["whole_ns"])
    ordered = sorted(latencies)
    measured = {f"p{pct}_ns": ordered[math.ceil(1000 * pct / 100) - 1]
                for pct in (50, 90, 95, 99)}
    if (any(summary[key] != value for key, value in measured.items())
            or not math.isclose(summary["qps"], 1000 * 1e9 / summary["wall_ns"],
                                rel_tol=1e-12)
            or summary["request_bytes"] != sum(row["request_bytes"] for row in current)
            or summary["response_bytes"] != sum(row["response_bytes"] for row in current)):
        raise ValueError("summary differs from raw samples")
    quality = matches == 1000 and sum(hits[:256]) == 25_509 and sum(hits[256:]) == 74_159
    performance = (measured["p95_ns"] * 4 <= reference_p95 * 3
                   and summary["qps"] * 2 >= reference_qps * 3)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v233-decoded-graph-http-quality-v1",
        "dataset": "ReLAION-1M D768", "split": "validation-1000-prior-used",
        "k": 100, "pass_label": label, "exact_v230_id_lists": matches,
        "development_256_hits": sum(hits[:256]), "remaining_744_hits": sum(hits[256:]),
        "total_hits": sum(hits), "p05_hits_per_query": sorted(hits)[49],
        "latency": measured, "qps": summary["qps"],
        "reference_p95_ns": reference_p95, "reference_qps": reference_qps,
        "quality_pass": quality, "performance_pass": performance,
        "pass": quality and performance,
        "raw_sha256": digest(args.raw), "reference_sha256": reference_sha,
        "summary_sha256": digest(args.summary), "truth_sha256": V198_SHA,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "reference", "summary", "truth", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
