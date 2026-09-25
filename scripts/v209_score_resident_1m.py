#!/usr/bin/env python3
"""Independent closed-query quality and same-panel V199 comparison."""

import argparse
import hashlib
import json
from pathlib import Path

V198_SHA = "2b18321435642de3fad4df02b84abcc046fb6c9b17808e72d6a50eea54a9ec98"
COUNT = 1000


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def records(path: Path) -> list[dict]:
    with path.open() as source:
        return [json.loads(line) for line in source]


def run(args: argparse.Namespace) -> None:
    if digest(args.baseline) != V198_SHA:
        raise ValueError("V198 paired GT/quality authority differs")
    summary = json.loads(args.serving.read_text())
    rows, baseline = records(args.raw), records(args.baseline)
    if (summary.get("schema") != "borsuk-v209-resident-serving-v1"
            or summary.get("raw_sha256") != digest(args.raw)
            or len(rows) != COUNT or len(baseline) != COUNT):
        raise ValueError("V209 serving output differs")
    hits, prior, v155 = [], [], []
    for index, (row, old) in enumerate(zip(rows, baseline, strict=True)):
        ids, gold = row["returned_ids"], old["gold_ids"]
        if (row["ordinal"] != index or old["ordinal"] != index
                or len(ids) != 100 or len(set(ids)) != 100
                or len(gold) != 100 or len(set(gold)) != 100
                or row["vector_body_gets"] != 0 or row["vector_body_bytes"] != 0):
            raise ValueError(f"V209 case geometry differs at {index}")
        hits.append(len(set(ids) & set(gold)))
        prior.append(old["optional_risk"]["fp16_hits"])
        v155.append(old["v155_sparse_source_hits"])
    if sum(prior) != 99605 or sum(v155) != 99567:
        raise ValueError("paired prior baseline differs")
    ordered = sorted(hits)
    result = {
        "schema": "borsuk-v209-resident-1m-quality-v1",
        "dataset": "ReLAION-1M D768", "split": "validation-1000-already-used",
        "queries": COUNT, "hits": sum(hits), "p05_hits": ordered[49],
        "min_hits": ordered[0], "below_98": sum(hit < 98 for hit in hits),
        "v199_same_panel_hits": sum(prior), "v155_same_panel_hits": sum(v155),
        "v209_vs_v199_wins": sum(a > b for a, b in zip(hits, prior, strict=True)),
        "v209_vs_v199_ties": sum(a == b for a, b in zip(hits, prior, strict=True)),
        "v209_vs_v199_losses": sum(a < b for a, b in zip(hits, prior, strict=True)),
        "raw_sha256": digest(args.raw), "baseline_sha256": V198_SHA,
    }
    result["passes_internal_gate"] = (
        result["hits"] >= 99605 and result["p05_hits"] >= 98
        and summary["p95_ns"] < 92_230_000
        and summary["p99_ns"] < 137_870_000
        and summary["throughput_qps"] >= 100
        and summary["process_peak_rss_bytes"] <= 2 * 1024 ** 3
        and summary["vector_body_gets"] == 0
    )
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("baseline", "raw", "serving", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
