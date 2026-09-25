#!/usr/bin/env python3
"""Score sealed 1M graph returns against paired V199/V155 GT witness."""

import argparse
import hashlib
import json
from pathlib import Path

COUNT = 1000
ARMS = ((2048, 2048), (4096, 4096), (8192, 8192), (16384, 16384))
V198_SHA = "2b18321435642de3fad4df02b84abcc046fb6c9b17808e72d6a50eea54a9ec98"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def records(path: Path) -> list[dict]:
    with path.open() as source:
        return [json.loads(line) for line in source]


def run(args: argparse.Namespace) -> None:
    if digest(args.baseline) != V198_SHA:
        raise ValueError("paired V198/V199/V155 witness differs")
    serving = json.loads(args.serving.read_text())
    raw, baseline = records(args.raw), records(args.baseline)
    if (serving.get("schema") != "borsuk-v217-pq-cosine-graph-1m-serving-v1"
            or serving.get("raw_sha256") != digest(args.raw)
            or serving.get("queries") != COUNT or len(raw) != COUNT
            or len(baseline) != COUNT):
        raise ValueError("V217 serving/quality identity differs")
    v199 = [row["optional_risk"]["fp16_hits"] for row in baseline]
    v155 = [row["v155_sparse_source_hits"] for row in baseline]
    if sum(v199) != 99605 or sum(v155) != 99567:
        raise ValueError("paired BORSUK baselines differ")
    results = {}
    for ef, shortlist in ARMS:
        name = f"{ef}-{shortlist}"
        hits = []
        for ordinal, (row, old) in enumerate(zip(raw, baseline, strict=True)):
            got = row["arms"][name]["returned_ids"]
            gold = old["gold_ids"]
            if (row["ordinal"] != ordinal or old["ordinal"] != ordinal
                    or len(got) != 100 or len(set(got)) != 100
                    or len(gold) != 100 or len(set(gold)) != 100
                    or row["vector_body_gets"] != 0):
                raise ValueError(f"V217 case geometry differs at {ordinal}")
            hits.append(len(set(got) & set(gold)))
        ordered = sorted(hits)
        loaded = serving["arms"][name]["loaded"]
        results[name] = {
            "hits": sum(hits), "p05_hits": ordered[49], "min_hits": ordered[0],
            "below_98": sum(hit < 98 for hit in hits),
            "v199_wins": sum(a > b for a, b in zip(hits, v199, strict=True)),
            "v199_ties": sum(a == b for a, b in zip(hits, v199, strict=True)),
            "v199_losses": sum(a < b for a, b in zip(hits, v199, strict=True)),
            "passes_internal_gate": sum(hits) >= 99605 and ordered[49] >= 98
                and loaded["p95_ns"] < 92_230_000
                and loaded["p99_ns"] < 137_870_000
                and loaded["qps"] >= 100
                and serving["process_peak_rss_bytes"] <= 3 * 1024 ** 3
                and serving["vector_body_gets"] == 0,
        }
    winner = next((f"{ef}-{shortlist}" for ef, shortlist in ARMS
                   if results[f"{ef}-{shortlist}"]["passes_internal_gate"]), None)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v217-pq-cosine-graph-1m-quality-v1",
        "dataset": "ReLAION-1M D768", "split": "validation-1000-already-used",
        "queries": COUNT, "v199_same_panel_hits": sum(v199),
        "v155_same_panel_hits": sum(v155), "arms": results,
        "smallest_passing_arm": winner,
        "raw_sha256": digest(args.raw), "serving_sha256": digest(args.serving),
        "baseline_sha256": V198_SHA,
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("baseline", "raw", "serving", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())
