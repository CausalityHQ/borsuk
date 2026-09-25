#!/usr/bin/env python3
"""Score sealed indexed versus linear 10k-row mutation snapshots."""

import argparse
import hashlib
import json
from pathlib import Path

BASE_SHA = "900872f9572bff1a195c4e3ed595d3ee28a8aa6f6555f5cc082c1ada52882fa0"
BASE_SPLITS = (25_537, 74_234)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def jsonl(path: Path):
    with path.open() as source:
        for line in source:
            yield json.loads(line)


def load_truth(path: Path):
    from scripts.v158_pq_primary_returned import load_truth as read_truth
    return read_truth(path)


def hits(ids: list[int], truth) -> int:
    return len(set(ids) & set(truth))


def run(args: argparse.Namespace) -> None:
    linear, indexed = (json.loads(path.read_text()) for path in
                       (args.linear_serving, args.indexed_serving))
    if (sha256(args.baseline) != BASE_SHA
            or linear["schema"] != "borsuk-v231-indexed-delta-100k-serving-v1"
            or indexed["schema"] != "borsuk-v231-indexed-delta-100k-serving-v1"
            or linear["mode"] != "linear-10k" or indexed["mode"] != "indexed-10k"
            or linear["raw_sha256"] != sha256(args.linear_raw)
            or indexed["raw_sha256"] != sha256(args.indexed_raw)
            or any(row["upsert_rows"] != 10_000 or row["upsert_stride"] != 10
                   or row["vector_body_gets"] != 0 for row in (linear, indexed))
            or indexed["index_candidates"] != 256):
        raise ValueError("paired mutation identity differs")
    baseline = list(jsonl(args.baseline))
    linears = list(jsonl(args.linear_raw))
    indexes = list(jsonl(args.indexed_raw))
    truth = load_truth(args.truth)
    if any(len(rows) != 1000 for rows in (baseline, linears, indexes)) or len(truth) < 1000:
        raise ValueError("paired panel length differs")
    base_hits, linear_hits, index_hits, ties = [], [], [], 0
    for ordinal, (base, line, idx) in enumerate(zip(baseline, linears, indexes, strict=True)):
        if (base["ordinal"] != ordinal or line["ordinal"] != ordinal
                or idx["ordinal"] != ordinal or line["delta_rows_scanned"] != 10_000
                or idx["delta_rows_scanned"] > 256
                or line["vector_body_gets"] != 0 or idx["vector_body_gets"] != 0):
            raise ValueError(f"query identity differs at {ordinal}")
        base_ids = base["arms"]["2048-2048"]["returned_ids"]
        line_ids, idx_ids = line["returned_ids"], idx["returned_ids"]
        for ids in (line_ids, idx_ids):
            if len(ids) != 100 or len(set(ids)) != 100:
                raise ValueError(f"result geometry differs at {ordinal}")
        base_hits.append(hits(base_ids, truth[ordinal]))
        linear_hits.append(hits(line_ids, truth[ordinal]))
        index_hits.append(hits(idx_ids, truth[ordinal]))
        ties += line_ids == idx_ids
    if (sum(base_hits[:256]), sum(base_hits[256:])) != BASE_SPLITS:
        raise ValueError("V218 baseline hits differ")
    splits = [{"v218_hits": sum(base_hits[lo:hi]),
               "linear_hits": sum(linear_hits[lo:hi]),
               "indexed_hits": sum(index_hits[lo:hi])}
              for lo, hi in ((0, 256), (256, 1000))]
    quality = (all(row["indexed_hits"] >= max(row["v218_hits"], row["linear_hits"])
                   for row in splits) and sorted(index_hits)[49] >= 99)
    performance = (indexed["loaded"]["p95_ns"] * 4 <= linear["loaded"]["p95_ns"] * 3
                   and indexed["index_build_ns"] <= 60_000_000_000
                   and indexed["process_peak_rss_bytes"] <= 512 * 1024 * 1024
                   and indexed["delta_rows_scanned_total"] <= 256_000)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v231-indexed-delta-100k-quality-v1",
        "dataset": "ReLAION-100k D768", "split": "development-256-plus-method-heldout-744-prior-used",
        "k": 100, "development": splits[0], "method_heldout": splits[1],
        "linear_hits": sum(linear_hits), "indexed_hits": sum(index_hits),
        "indexed_p05_hits": sorted(index_hits)[49], "exact_linear_id_lists": ties,
        "linear_loaded": linear["loaded"], "indexed_loaded": indexed["loaded"],
        "index_build_ns": indexed["index_build_ns"],
        "linear_rss_bytes": linear["process_peak_rss_bytes"],
        "indexed_rss_bytes": indexed["process_peak_rss_bytes"],
        "indexed_overlay_bytes": indexed["overlay_resident_bytes"],
        "quality_pass": quality, "performance_pass": performance,
        "pass": quality and performance,
        "linear_raw_sha256": sha256(args.linear_raw),
        "indexed_raw_sha256": sha256(args.indexed_raw),
        "baseline_raw_sha256": BASE_SHA,
        "truth_sha256": sha256(args.truth),
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("linear_serving", "indexed_serving", "linear_raw", "indexed_raw",
                 "baseline", "truth", "output"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    run(parser.parse_args())
