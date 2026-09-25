#!/usr/bin/env python3
"""Score exact decoded delta parity and recompute loaded raw timings."""

import argparse
import hashlib
import json
import math
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


def percentiles(rows: list[dict]) -> dict:
    if len(rows) != 1000 or [row["ordinal"] for row in rows] != list(range(1000)):
        raise ValueError("loaded raw panel differs")
    values = sorted(row["whole_ns"] for row in rows)
    if values[0] <= 0:
        raise ValueError("loaded raw latency differs")
    return {f"p{pct}_ns": values[math.ceil(1000 * pct / 100) - 1]
            for pct in (50, 90, 95, 99)}


def run(args: argparse.Namespace) -> None:
    linear, decoded = (json.loads(path.read_text()) for path in
                       (args.linear_serving, args.decoded_serving))
    if (sha256(args.baseline) != BASE_SHA
            or any(row["schema"] != "borsuk-v232-decoded-delta-100k-serving-v1"
                   or row["upsert_rows"] != 10_000 or row["upsert_stride"] != 10
                   or row["vector_body_gets"] != 0
                   or row["delta_rows_scanned_total"] != 10_000_000
                   for row in (linear, decoded))
            or linear["mode"] != "linear-10k" or decoded["mode"] != "decoded-10k"
            or linear["raw_sha256"] != sha256(args.linear_raw)
            or decoded["raw_sha256"] != sha256(args.decoded_raw)
            or linear["loaded_raw_sha256"] != sha256(args.linear_loaded_raw)
            or decoded["loaded_raw_sha256"] != sha256(args.decoded_loaded_raw)):
        raise ValueError("paired decoded mutation identity differs")
    loaded = {}
    for mode, serving, path in (("linear", linear, args.linear_loaded_raw),
                                ("decoded", decoded, args.decoded_loaded_raw)):
        measured = percentiles(list(jsonl(path)))
        if any(serving["loaded"][name] != value for name, value in measured.items()):
            raise ValueError(f"{mode} loaded percentile differs from raw")
        if (serving["loaded_wall_ns"] <= 0 or not math.isclose(
                serving["loaded"]["qps"], 1000 * 1e9 / serving["loaded_wall_ns"],
                rel_tol=1e-12)):
            raise ValueError(f"{mode} throughput differs from wall time")
        loaded[mode] = serving["loaded"]
    baseline, linears, decodeds = (list(jsonl(path)) for path in
                                   (args.baseline, args.linear_raw, args.decoded_raw))
    truth = load_truth(args.truth)
    if any(len(rows) != 1000 for rows in (baseline, linears, decodeds)) or len(truth) < 1000:
        raise ValueError("paired panel length differs")
    base_hits, linear_hits, decoded_hits = [], [], []
    exact_lists = 0
    for ordinal, (base, line, decoded_row) in enumerate(
            zip(baseline, linears, decodeds, strict=True)):
        if (base["ordinal"] != ordinal or line["ordinal"] != ordinal
                or decoded_row["ordinal"] != ordinal
                or line["delta_rows_scanned"] != 10_000
                or decoded_row["delta_rows_scanned"] != 10_000
                or line["vector_body_gets"] != 0
                or decoded_row["vector_body_gets"] != 0):
            raise ValueError(f"query geometry differs at {ordinal}")
        ids = line["returned_ids"]
        other = decoded_row["returned_ids"]
        if len(ids) != 100 or len(set(ids)) != 100 or len(other) != 100 or len(set(other)) != 100:
            raise ValueError(f"result geometry differs at {ordinal}")
        exact_lists += ids == other
        gold = set(truth[ordinal])
        base_hits.append(len(set(base["arms"]["2048-2048"]["returned_ids"]) & gold))
        linear_hits.append(len(set(ids) & gold))
        decoded_hits.append(len(set(other) & gold))
    if (sum(base_hits[:256]), sum(base_hits[256:])) != BASE_SPLITS:
        raise ValueError("V218 baseline hits differ")
    splits = [{"v218_hits": sum(base_hits[lo:hi]),
               "linear_hits": sum(linear_hits[lo:hi]),
               "decoded_hits": sum(decoded_hits[lo:hi])}
              for lo, hi in ((0, 256), (256, 1000))]
    quality = (exact_lists == 1000
               and all(row["decoded_hits"] >= row["v218_hits"] for row in splits)
               and sorted(decoded_hits)[49] >= 99)
    performance = (decoded["loaded"]["p95_ns"] * 4 <= linear["loaded"]["p95_ns"] * 3
                   and decoded["loaded"]["qps"] * 2 >= linear["loaded"]["qps"] * 3
                   and decoded["decode_ns"] <= 1_000_000_000
                   and decoded["process_peak_rss_bytes"] <= 512 * 1024 * 1024
                   and decoded["overlay_resident_bytes"] <= 64 * 1024 * 1024)
    args.output.write_text(json.dumps({
        "schema": "borsuk-v232-decoded-delta-100k-quality-v1",
        "dataset": "ReLAION-100k D768", "split": "development-256-plus-method-heldout-744-prior-used",
        "k": 100, "development": splits[0], "method_heldout": splits[1],
        "linear_hits": sum(linear_hits), "decoded_hits": sum(decoded_hits),
        "decoded_p05_hits": sorted(decoded_hits)[49], "exact_linear_id_lists": exact_lists,
        "loaded": loaded, "decode_ns": decoded["decode_ns"],
        "linear_rss_bytes": linear["process_peak_rss_bytes"],
        "decoded_rss_bytes": decoded["process_peak_rss_bytes"],
        "decoded_overlay_bytes": decoded["overlay_resident_bytes"],
        "quality_pass": quality, "performance_pass": performance,
        "pass": quality and performance,
        "linear_raw_sha256": sha256(args.linear_raw),
        "decoded_raw_sha256": sha256(args.decoded_raw),
        "linear_loaded_raw_sha256": sha256(args.linear_loaded_raw),
        "decoded_loaded_raw_sha256": sha256(args.decoded_loaded_raw),
        "truth_sha256": sha256(args.truth),
    }, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("linear_serving", "decoded_serving", "linear_raw", "decoded_raw",
                 "linear_loaded_raw", "decoded_loaded_raw", "baseline", "truth", "output"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    run(parser.parse_args())
