#!/usr/bin/env python3
"""Score a sealed external HTTP pass without discarding a negative result."""

import argparse
import json
from pathlib import Path

from scripts.v219_score_reachable_graph_1m import V198_SHA, digest, records


def run(args: argparse.Namespace) -> None:
    if digest(args.truth) != V198_SHA:
        raise ValueError("V198 GT witness differs")
    summary = json.loads(args.summary.read_text())
    if (summary["schema"] != "borsuk-v222-graph-http-client-1m-v1"
            or summary["raw_sha256"] != digest(args.raw)
            or summary["queries"] != 1000 or summary["concurrency"] != 8
            or summary["transport"] != "persistent HTTP/1.1 VPC peer"):
        raise ValueError("V222 HTTP measurement identity differs")
    current, previous, truth = records(args.raw), records(args.previous), records(args.truth)
    if len(current) != 1000 or len(previous) != 1000 or len(truth) != 1000:
        raise ValueError("paired panel length differs")
    hits = []
    exact = 0
    for index, (row, old, gold) in enumerate(zip(current, previous, truth, strict=True)):
        ids = row["returned_ids"]
        if (row["ordinal"] != index or old["ordinal"] != index
                or gold["ordinal"] != index or len(ids) != 100
                or len(set(ids)) != 100 or row["vector_body_gets"] != 0):
            raise ValueError(f"V222 case geometry differs at {index}")
        exact += ids == old["arms"]["4096-4096"]["returned_ids"]
        hits.append(len(set(ids) & set(gold["gold_ids"])))
    quality_pass = exact == 1000 and sum(hits) == 99664 and sorted(hits)[49] == 98
    perf_pass = (summary["p95_ns"] < 100_000_000
                 and summary["p99_ns"] < 150_000_000
                 and summary["qps"] >= 100)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v222-external-graph-http-quality-v1",
        "pass_label": summary["pass_label"], "queries": 1000,
        "exact_v219_id_lists": exact, "hits": sum(hits),
        "p05_hits": sorted(hits)[49], "quality_pass": quality_pass,
        "performance_pass": perf_pass, "pass": quality_pass and perf_pass,
        "raw_sha256": digest(args.raw), "summary_sha256": digest(args.summary),
        "previous_sha256": digest(args.previous), "truth_sha256": V198_SHA,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "summary", "previous", "truth", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
