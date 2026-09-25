#!/usr/bin/env python3
"""Frozen paired 100k falsifier for FP16 plane as graph build source."""

import argparse
import json
from pathlib import Path

from scripts.v158_pq_primary_returned import jsonl, load_truth, sha256
from scripts.v210_resident_neighborhood_100k import canonical
from scripts.v218_score_reachable_graph_100k import hits

BASE_RAW_SHA = "900872f9572bff1a195c4e3ed595d3ee28a8aa6f6555f5cc082c1ada52882fa0"
BASE_HITS = (25537, 74234)
ARM = "2048-2048"


def run(args: argparse.Namespace) -> None:
    serving = json.loads(args.serving.read_text())
    build = json.loads(args.build.read_text())
    current = list(jsonl(args.raw))
    baseline = list(jsonl(args.baseline))
    truth = load_truth(args.truth)
    if (build.get("construction_source") != "authenticated-fp16-plane"
            or serving.get("schema") != "borsuk-v218-reachable-graph-100k-serving-v1"
            or serving.get("raw_sha256") != sha256(args.raw)
            or sha256(args.baseline) != BASE_RAW_SHA
            or len(current) != 1000 or len(baseline) != 1000 or len(truth) < 1000):
        raise ValueError("FP16 source or paired input identity differs")
    candidate_hits, base_hits, paired = [], [], []
    for ordinal, (new, old) in enumerate(zip(current, baseline, strict=True)):
        if new["ordinal"] != ordinal or old["ordinal"] != ordinal:
            raise ValueError("query ordinal differs")
        new_ids, old_ids = (row["arms"][ARM]["returned_ids"] for row in (new, old))
        candidate_hits.append(hits(new_ids, truth[ordinal]))
        base_hits.append(hits(old_ids, truth[ordinal]))
        paired.append(new_ids == old_ids)
    if (sum(base_hits[:256]), sum(base_hits[256:])) != BASE_HITS:
        raise ValueError("selected V218 paired hit count differs")

    def split(lo: int, hi: int) -> dict:
        return {"queries": hi - lo, "candidate_hits": sum(candidate_hits[lo:hi]),
                "v218_f32_hits": sum(base_hits[lo:hi]),
                "exact_id_ties": sum(paired[lo:hi]),
                "p05_hits_per_query": sorted(candidate_hits[lo:hi])[(hi - lo + 19) // 20 - 1]}

    development, heldout, combined = split(0, 256), split(256, 1000), split(0, 1000)
    loaded = serving["arms"][ARM]["loaded"]
    passed = (development["candidate_hits"] >= BASE_HITS[0] - 15
              and heldout["candidate_hits"] >= BASE_HITS[1] - 15
              and combined["candidate_hits"] >= sum(BASE_HITS) - 25
              and combined["p05_hits_per_query"] >= 98
              and loaded["p95_ns"] <= 8_298_000
              and serving["process_peak_rss_bytes"] <= 256 * 1024 * 1024
              and serving["vector_body_gets"] == 0)
    args.output.write_text(canonical({
        "schema": "borsuk-v226-fp16-graph-source-quality-v1",
        "dataset": "ReLAION-100k D768", "k": 100,
        "split": "development-256-plus-method-heldout-744-prior-used",
        "development": development, "method_heldout": heldout, "combined": combined,
        "pass": passed, "loaded_p95_ns": loaded["p95_ns"],
        "loaded_qps": loaded["qps"],
        "peak_rss_bytes": serving["process_peak_rss_bytes"],
        "candidate_raw_sha256": sha256(args.raw),
        "baseline_raw_sha256": BASE_RAW_SHA,
        "truth_sha256": sha256(args.truth),
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("build", "serving", "raw", "baseline", "truth", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    run(parser.parse_args())
