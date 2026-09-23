"""Compare one frozen Rust returned-range replay with V114 paired evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.launch_v112_precise_nominee_spot import INPUTS
from scripts.v114_exact_local_100k import _canonical, _sha256_file
from scripts.validate_v115_router_parity import FROZEN_REQUESTS_SHA256

FROZEN_REFERENCE_SHA256 = "d805cf0fa734cae9009f565b4187deac0b7e4a9f0dc70b2e6b4084aa6493487b"
FROZEN_EVIDENCE_SHA256 = "786c69d8e833f56ab9b89de741ee4d56568f9b7f0a3ca2b6985861454e461ba9"
FROZEN_ROSTERS_SHA256 = "b1624b36ff4952d6268b46c8d1d11114448e567f8af7ea379fbb8f4e160e172d"


def compare_replay(
    requests: list[dict], rosters: list[dict], references: list[dict],
    evidence: list[dict], actual: list[dict], truth: np.ndarray,
) -> dict[str, object]:
    if (any(len(series) != 1000 for series in
            (requests, rosters, references, evidence, actual))
            or truth.shape != (1000, 100)):
        raise ValueError("frozen 1M replay query geometry differs")
    counts = {"set": 0, "primary": 0, "score_bits_by_row": 0,
              "votes": 0, "ranges": 0, "plan_bytes": 0,
              "plan_score": 0, "returned_ids": 0, "hits": 0}
    total_hits = 0
    per_query_hits = []
    for ordinal, (request, roster, reference, prior, replay, gold) in enumerate(
        zip(requests, rosters, references, evidence, actual, truth)
    ):
        if any(item.get("query_ordinal") != ordinal for item in
               (request, roster, reference, prior, replay)):
            raise ValueError(f"replay ordinal differs at {ordinal}")
        if replay["nominees"] != roster["nominees"]:
            raise ValueError(f"replay frozen Rust roster differs at {ordinal}")
        if set(replay["nominees"]) != set(request["nominees"]):
            counts["set"] += 1
        expected_bits = dict(zip(request["nominees"], reference["score_bits"]))
        actual_bits = dict(zip(replay["nominees"], replay["score_bits"]))
        if expected_bits != actual_bits:
            counts["score_bits_by_row"] += 1
        for field, key in (("primary", "primary"), ("page_votes", "votes"),
                           ("ranges", "ranges"), ("plan_bytes", "plan_bytes"),
                           ("plan_score", "plan_score")):
            if replay[field] != reference[field]:
                counts[key] += 1
        if replay["returned_ids"] != prior["production_returned_ids"]:
            counts["returned_ids"] += 1
        hit_count = len(set(map(int, gold)).intersection(replay["returned_ids"]))
        per_query_hits.append(hit_count)
        total_hits += hit_count
        if hit_count != prior["production_hits"]:
            counts["hits"] += 1
    return {
        "schema": "borsuk-v115-returned-replay-v1",
        "dataset": "ReLAION-1M", "split": "development-1000",
        "query_count": 1000, "mismatch_queries": counts,
        "rust_returned_hits": total_hits,
        "rust_recall_at_100": total_hits / 100_000,
        "rust_p05_hits": int(np.sort(per_query_hits)[49]),
        "rust_sub90_queries": sum(value < 90 for value in per_query_hits),
        "v114_returned_hits": sum(row["production_hits"] for row in evidence),
        "historical_same_run_v109_baseline_hits": sum(row["baseline_hits"] for row in evidence),
        "live_s3_measured": False,
    }


def validate(
    requests_path: Path, rosters_path: Path, reference_path: Path,
    evidence_path: Path, replay_path: Path, truth_path: Path, output_path: Path,
) -> None:
    for path, expected in ((requests_path, FROZEN_REQUESTS_SHA256),
                           (rosters_path, FROZEN_ROSTERS_SHA256),
                           (reference_path, FROZEN_REFERENCE_SHA256),
                           (evidence_path, FROZEN_EVIDENCE_SHA256),
                           (truth_path, INPUTS["TRUTH"].sha256)):
        if _sha256_file(path) != expected:
            raise ValueError(f"frozen input SHA-256 differs: {path.name}")
    import pyarrow.parquet as pq
    truth = np.asarray(pq.read_table(truth_path, columns=["feature_row_id"])[
        "feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False),
        dtype=np.int64).reshape(1000, 100)
    read = lambda path: [json.loads(line) for line in path.read_text().splitlines()]
    summary = compare_replay(read(requests_path), read(rosters_path),
                             read(reference_path), read(evidence_path),
                             read(replay_path), truth)
    summary["replay_sha256"] = _sha256_file(replay_path)
    output_path.write_text(_canonical(summary))
    if any(summary["mismatch_queries"].values()):
        raise ValueError("V115 composed returned replay differs from V114")


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("requests", "rosters", "reference", "evidence", "replay",
                 "truth", "output"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    args = parser.parse_args()
    validate(args.requests, args.rosters, args.reference, args.evidence,
             args.replay, args.truth, args.output)


if __name__ == "__main__":
    main()
