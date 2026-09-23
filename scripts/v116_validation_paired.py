"""Untouched paired quality gate for the source-only V115 router and Rust scorer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v109_range_plan import admit_ranked_pages
from scripts.v114_exact_local_100k import _canonical, _sha256_file
from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v115_source_router import load_source_router
from scripts.v77_export_manifest import fixed_list


def lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def prepare(queries: Path, output: Path) -> None:
    import pyarrow.parquet as pq

    vectors = fixed_list(pq.read_table(queries), "embedding", 768, 1000)
    with output.open("w") as dest:
        for ordinal, vector in enumerate(vectors):
            dest.write(_canonical({"query_ordinal": ordinal,
                                   "query": vector.astype(float).tolist()}))


def compose(router: Path, requests: Path, rosters: Path, output: Path) -> None:
    manifest, planes = load_source_router(router)
    geometry = manifest["geometry"]
    if geometry != {"rows": 1_000_000, "dimensions": 768, "page_rows": 256,
                    "blocks_per_page": 2, "subspaces": 64, "pq_width": 12}:
        raise ValueError("frozen validation router geometry differs")
    queries = lines(requests)
    nominees = lines(rosters)
    if len(queries) != 1000 or len(nominees) != 1000:
        raise ValueError("validation query or roster count differs")
    with output.open("w") as dest:
        for ordinal, (request, roster) in enumerate(zip(queries, nominees)):
            if request.get("query_ordinal") != ordinal or roster.get("query_ordinal") != ordinal:
                raise ValueError(f"validation query ordinal differs at {ordinal}")
            query = np.asarray(request["query"], np.float32)
            ranked, _, python_nominees = nominate_region_pq64(
                query, planes["summaries"], planes["books"], planes["codes"],
                page_rows=256, blocks_per_page=2, regions=1024, shortlist=512,
            )
            rust_nominees = roster["nominees"]
            if (len(rust_nominees) != 512 or len(set(rust_nominees)) != 512
                    or set(rust_nominees) != set(map(int, python_nominees))):
                raise ValueError(f"validation Rust/Python nominee set differs at {ordinal}")
            baseline = admit_ranked_pages(
                ranked, rows=1_000_000, page_rows=256, row_bytes=780,
                max_gets=32, max_bytes=16_777_216,
            )
            if baseline.plan.gets > 32 or baseline.plan.bytes > 16_777_216:
                raise ValueError(f"validation baseline cap differs at {ordinal}")
            dest.write(_canonical({**request, "nominees": rust_nominees,
                                   "primary_count": 100,
                                   "baseline_ranges": [list(pair) for pair in baseline.plan.ranges]}))


def reduce(requests: Path, replay: Path, truth: Path,
           evidence: Path, summary: Path) -> None:
    import pyarrow.parquet as pq

    prepared = lines(requests)
    actual = lines(replay)
    if len(prepared) != 1000 or len(actual) != 1000:
        raise ValueError("validation replay count differs")
    gold = np.asarray(pq.read_table(truth, columns=["feature_row_id"])[
        "feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False),
        dtype=np.int64).reshape(1000, 100)
    hits = {"candidate": [], "baseline": []}
    max_gets = {"candidate": 0, "baseline": 0}
    max_bytes = {"candidate": 0, "baseline": 0}
    with evidence.open("w") as dest:
        for ordinal, (request, result, truth_ids) in enumerate(zip(prepared, actual, gold)):
            if (request.get("query_ordinal") != ordinal
                    or result.get("query_ordinal") != ordinal
                    or result.get("nominees") != request["nominees"]
                    or result.get("baseline_ranges") != request["baseline_ranges"]):
                raise ValueError(f"validation replay identity differs at {ordinal}")
            truth_set = set(map(int, truth_ids))
            row = {"query_ordinal": ordinal}
            for arm, ranges_key, ids_key, bytes_key in (
                ("candidate", "ranges", "returned_ids", "plan_bytes"),
                ("baseline", "baseline_ranges", "baseline_returned_ids", "baseline_bytes"),
            ):
                ranges = result[ranges_key]
                ids = result[ids_key]
                nbytes = result[bytes_key]
                if (len(ids) != 100 or len(set(ids)) != 100
                        or not 1 <= len(ranges) <= 32
                        or nbytes != sum(end - start for start, end in ranges)
                        or nbytes > 16_777_216):
                    raise ValueError(f"validation {arm} physical or result cap differs at {ordinal}")
                hit = len(truth_set.intersection(ids))
                hits[arm].append(hit)
                max_gets[arm] = max(max_gets[arm], len(ranges))
                max_bytes[arm] = max(max_bytes[arm], nbytes)
                row[f"{arm}_hits"] = hit
                row[f"{arm}_returned_ids"] = ids
                row[f"{arm}_gets"] = len(ranges)
                row[f"{arm}_bytes"] = nbytes
            dest.write(_canonical(row))
    total = {arm: sum(values) for arm, values in hits.items()}
    p05 = {arm: sorted(values)[49] for arm, values in hits.items()}
    sub90 = {arm: sum(value < 90 for value in values) for arm, values in hits.items()}
    qualifies = (total["candidate"] >= 99_000
                 and total["candidate"] >= total["baseline"]
                 and p05["candidate"] >= 90
                 and p05["candidate"] >= p05["baseline"]
                 and sub90["candidate"] <= sub90["baseline"])
    summary.write_text(_canonical({
        "schema": "borsuk-v116-validation-paired-v1",
        "dataset": "ReLAION-1M", "split": "validation-1000",
        "query_count": 1000, "source_only_router": True,
        "rust_returned_scorer_both_arms": True,
        "total_hits": total,
        "recall_at_100": {arm: value / 100_000 for arm, value in total.items()},
        "p05_hits": p05, "sub90_queries": sub90,
        "max_gets": max_gets, "max_bytes": max_bytes,
        "paired_queries_candidate_better": sum(a > b for a, b in zip(hits["candidate"], hits["baseline"])),
        "paired_queries_equal": sum(a == b for a, b in zip(hits["candidate"], hits["baseline"])),
        "paired_queries_baseline_better": sum(a < b for a, b in zip(hits["candidate"], hits["baseline"])),
        "requests_sha256": _sha256_file(requests),
        "replay_sha256": _sha256_file(replay),
        "evidence_sha256": _sha256_file(evidence),
        "qualifies_validation_quality": qualifies,
        "live_s3_measured": False,
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "compose", "reduce"))
    for name in ("queries", "router", "requests", "rosters", "output", "replay", "truth",
                 "evidence", "summary"):
        parser.add_argument(f"--{name}", type=Path)
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args.queries, args.output)
    elif args.phase == "compose":
        compose(args.router, args.requests, args.rosters, args.output)
    else:
        reduce(args.requests, args.replay, args.truth, args.evidence, args.summary)


if __name__ == "__main__":
    main()
