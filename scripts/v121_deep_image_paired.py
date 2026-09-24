"""Query-blind plans and paired returned-quality reduction for deep-image-96."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from scripts.v109_range_plan import admit_ranked_pages
from scripts.v114_exact_local_100k import _canonical, _sha256_file
from scripts.v114_1m_paired import nominate_region_pq64
from scripts.v115_source_router import load_source_router

ROWS = 9_990_000
DIMENSIONS = 96
QUERY_COUNT = 1_000
PAGE_ROWS = 256
SHORTLIST = 512
GET_CAP = 32
BYTE_CAP = 16_777_216
REGIONS = (((ROWS + PAGE_ROWS - 1) // PAGE_ROWS) * 1024 + 3906) // 3907
PARITY_QUERIES = 16


def _lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def _balanced_query(query: np.ndarray, subspaces: int, width: int) -> np.ndarray:
    packed = np.zeros((subspaces, width), dtype=np.float32)
    for subspace in range(subspaces):
        first = subspace * query.size // subspaces
        last = (subspace + 1) * query.size // subspaces
        packed[subspace, :last - first] = query[first:last]
    return packed


def prepare(queries: Path, output: Path, *, query_count: int = QUERY_COUNT) -> None:
    """Fix the first query ordinals and normalize angular vectors before routing."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pq.read_schema(queries)
    if schema.names != ["emb"]:
        raise ValueError("deep-image query schema differs")
    field = schema.field("emb")
    if (not pa.types.is_fixed_size_list(field.type)
            or field.type.list_size != DIMENSIONS
            or field.type.value_type != pa.float32()):
        raise ValueError("deep-image query vector type differs")
    table = pq.read_table(queries, columns=["emb"])
    if (query_count <= 0 or table.num_rows < query_count):
        raise ValueError("deep-image query count differs")
    raw = table["emb"].combine_chunks().values.to_numpy(zero_copy_only=False)
    vectors = np.asarray(raw, dtype=np.float32).reshape(table.num_rows, DIMENSIONS)[:query_count]
    if not np.isfinite(vectors).all():
        raise ValueError("deep-image query contains nonfinite values")
    norms = np.linalg.norm(vectors, axis=1)
    if (norms <= 0).any():
        raise ValueError("deep-image query contains a zero vector")
    normalized = vectors / norms[:, None]
    with output.open("x") as dest:
        for ordinal, vector in enumerate(normalized):
            dest.write(_canonical({"query_ordinal": ordinal,
                                   "query": vector.astype(float).tolist()}))


def rank_nominee_pages(
    query: np.ndarray, books: np.ndarray, codes: np.ndarray,
    *, nominees: list[int], page_rows: int,
) -> list[int]:
    """Rank the same nominated pages by their best source-trained PQ64 score."""
    if (query.ndim != 1 or query.dtype != np.float32
            or books.shape != (64, 256, (query.size + 63) // 64)
            or books.dtype != np.float32
            or codes.ndim != 2 or codes.shape[1] != 64 or codes.dtype != np.uint8
            or page_rows <= 0 or len(nominees) == 0
            or len(set(nominees)) != len(nominees)
            or any(type(row) is not int or row < 0 or row >= codes.shape[0]
                   for row in nominees)
            or not np.isfinite(query).all() or not np.isfinite(books).all()):
        raise ValueError("PQ64 control nomination input differs")
    delta = books - _balanced_query(query, 64, books.shape[2])[:, None, :]
    table = np.einsum("ijk,ijk->ij", delta, delta, dtype=np.float32)
    rows = np.asarray(nominees, dtype=np.int64)
    scores = np.zeros(rows.size, dtype=np.float32)
    for subspace in range(64):
        scores += table[subspace, codes[rows, subspace]]
    best: dict[int, float] = {}
    for row, score in zip(rows, scores):
        page = int(row) // page_rows
        best[page] = min(best.get(page, float("inf")), float(score))
    return sorted(best, key=lambda page: (best[page], page))


def compose(router: Path, queries: Path, rosters: Path, output: Path) -> None:
    """Seal candidate rosters and same-roster capped control without GT."""
    manifest, planes = load_source_router(router)
    geometry = manifest["geometry"]
    expected = {"rows": ROWS, "dimensions": DIMENSIONS, "page_rows": PAGE_ROWS,
                "blocks_per_page": 2, "subspaces": 64, "pq_width": 2,
                "pq_partition": "balanced_floor_v1"}
    if geometry != expected:
        raise ValueError("deep-image router geometry differs")
    requests, nominees = _lines(queries), _lines(rosters)
    if len(requests) != QUERY_COUNT or len(nominees) != QUERY_COUNT:
        raise ValueError("deep-image query or roster count differs")
    with output.open("x") as dest:
        for ordinal, (request, roster) in enumerate(zip(requests, nominees)):
            if (request.get("query_ordinal") != ordinal
                    or roster.get("query_ordinal") != ordinal):
                raise ValueError(f"deep-image query ordinal differs at {ordinal}")
            rows = roster.get("nominees")
            if (type(rows) is not list or len(rows) != SHORTLIST
                    or len(set(rows)) != SHORTLIST):
                raise ValueError(f"deep-image Rust roster differs at {ordinal}")
            query = np.asarray(request["query"], np.float32)
            if ordinal < PARITY_QUERIES:
                _, _, independently_nominated = nominate_region_pq64(
                    query, planes["summaries"], planes["books"], planes["codes"],
                    page_rows=PAGE_ROWS, blocks_per_page=2,
                    regions=REGIONS, shortlist=SHORTLIST,
                )
                if set(rows) != set(map(int, independently_nominated)):
                    raise ValueError(f"deep-image Rust/Python nominee set differs at {ordinal}")
            ranked = rank_nominee_pages(
                query, planes["books"], planes["codes"],
                nominees=rows, page_rows=PAGE_ROWS,
            )
            baseline = admit_ranked_pages(
                ranked, rows=ROWS, page_rows=PAGE_ROWS, row_bytes=DIMENSIONS + 12,
                max_gets=GET_CAP, max_bytes=BYTE_CAP,
            )
            if baseline.plan.gets > GET_CAP or baseline.plan.bytes > BYTE_CAP:
                raise ValueError(f"deep-image baseline cap differs at {ordinal}")
            dest.write(_canonical({**request, "nominees": rows,
                                   "primary_count": 100,
                                   "baseline_ranges": [list(pair) for pair in baseline.plan.ranges]}))


def reduce(requests: Path, replay: Path, truth: Path,
           evidence: Path, summary: Path) -> None:
    """Use the untouched first thousand GT100 rows after both plans are sealed."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    prepared, actual = _lines(requests), _lines(replay)
    if len(prepared) != QUERY_COUNT or len(actual) != QUERY_COUNT:
        raise ValueError("deep-image replay count differs")
    table = pq.read_table(truth, columns=["neighbors_id"])
    field = table.schema.field("neighbors_id")
    if (table.num_rows < QUERY_COUNT
            or not (pa.types.is_list(field.type)
                    or pa.types.is_fixed_size_list(field.type))):
        raise ValueError("deep-image GT100 schema differs")
    gold = table["neighbors_id"].slice(0, QUERY_COUNT).to_pylist()
    if any(len(row) < 100 or len(set(row[:100])) != 100
           or any(type(item) is not int or item < 0 or item >= ROWS
                  for item in row[:100]) for row in gold):
        raise ValueError("deep-image GT100 identity differs")
    hits = {"candidate": [], "baseline": []}
    max_gets = {"candidate": 0, "baseline": 0}
    max_bytes = {"candidate": 0, "baseline": 0}
    with evidence.open("x") as dest:
        for ordinal, (request, result, truth_ids) in enumerate(zip(prepared, actual, gold)):
            if (request.get("query_ordinal") != ordinal
                    or result.get("query_ordinal") != ordinal
                    or result.get("nominees") != request["nominees"]
                    or result.get("baseline_ranges") != request["baseline_ranges"]):
                raise ValueError(f"deep-image replay identity differs at {ordinal}")
            truth_set = set(truth_ids[:100])
            row = {"query_ordinal": ordinal}
            for arm, ranges_key, ids_key, bytes_key in (
                ("candidate", "ranges", "returned_ids", "plan_bytes"),
                ("baseline", "baseline_ranges", "baseline_returned_ids", "baseline_bytes"),
            ):
                ranges, ids, nbytes = result[ranges_key], result[ids_key], result[bytes_key]
                if (len(ids) != 100 or len(set(ids)) != 100
                        or any(type(item) is not int or item < 0 or item >= ROWS for item in ids)
                        or not 1 <= len(ranges) <= GET_CAP
                        or nbytes != sum(end - start for start, end in ranges)
                        or nbytes > BYTE_CAP):
                    raise ValueError(f"deep-image {arm} physical or result cap differs at {ordinal}")
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
        "schema": "borsuk-v121-deep-image-paired-v1",
        "dataset": "deep-image-96-angular", "split": "test-first-1000",
        "query_count": QUERY_COUNT, "source_only_router": True,
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
        "qualifies_cross_corpus_quality": qualifies,
        "live_s3_measured": False,
    }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "compose", "reduce"))
    for name in ("queries", "router", "requests", "rosters", "output", "replay",
                 "truth", "evidence", "summary"):
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
