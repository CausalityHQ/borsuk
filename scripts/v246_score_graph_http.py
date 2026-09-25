#!/usr/bin/env python3
"""Score sealed V246 HTTP IDs against V245 and exact GT100."""

import argparse
import json
from pathlib import Path

from scripts.v219_score_reachable_graph_1m import V198_SHA, digest, records


def run(args):
    if digest(args.truth) != V198_SHA:
        raise ValueError("GT100 witness differs")
    summary = json.loads(args.summary.read_text())
    if (summary["schema"] != "borsuk-v222-graph-http-client-1m-v1"
            or summary["raw_sha256"] != digest(args.raw)
            or summary["queries"] != 1000 or summary["concurrency"] != 8
            or summary["transport"] != "persistent HTTP/1.1 VPC peer"):
        raise ValueError("HTTP measurement identity differs")
    current, reference, truth = map(records, (args.raw, args.reference, args.truth))
    if len(current) != len(reference) or len(current) != len(truth) or len(current) != 1000:
        raise ValueError("panel length differs")
    hits = []
    parity = 0
    for i, (row, old, gold) in enumerate(zip(current, reference, truth, strict=True)):
        ids = row["returned_ids"]
        if (row["ordinal"] != old["ordinal"] or row["ordinal"] != gold["ordinal"]
                or row["ordinal"] != i or len(ids) != 100 or len(set(ids)) != 100
                or row["vector_body_gets"] != 0):
            raise ValueError(f"query geometry differs at {i}")
        parity += ids == old["arms"]["4096-4096"]["returned_ids"]
        hits.append(len(set(ids) & set(gold["gold_ids"])))
    result = {"schema": "borsuk-v246-graph-http-quality-v1",
              "queries": 1000, "exact_v245_id_lists": parity,
              "hits": sum(hits), "dev_hits": sum(hits[:256]),
              "remaining_hits": sum(hits[256:]), "p05_hits": sorted(hits)[49],
              "quality_pass": parity == 1000 and sum(hits) == 99662 and sorted(hits)[49] >= 98,
              "raw_sha256": digest(args.raw), "reference_sha256": digest(args.reference),
              "truth_sha256": V198_SHA, "summary_sha256": digest(args.summary)}
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("raw", "reference", "summary", "truth", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
